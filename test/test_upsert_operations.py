#!/usr/bin/env python3
"""
Comprehensive test suite for UPSERT operations in the NSPC ETL System.
This test suite verifies that the file-to-sql-loader.py script correctly handles
UPSERT operations when tables already exist in SQL Server database.

UPSERT operations should:
- UPDATE existing records (same primary key, different data)
- INSERT new records (new primary key)
- Handle mixed scenarios with both updates and inserts in the same batch
"""

import sys
import os
import pytest
import pandas as pd
import pyodbc
import uuid
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import tempfile
import shutil
import time

# Add the src directory to the path to import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Import with the correct module name (file-to-sql-loader.py)
import importlib.util

# Load the module dynamically
spec = importlib.util.spec_from_file_location("file_to_sql_loader", Path(__file__).parent.parent / 'src' / 'file-to-sql-loader.py')
file_to_sql_loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(file_to_sql_loader)

FileToSQLLoader = file_to_sql_loader.FileToSQLLoader
load_config = file_to_sql_loader.load_config


class TestUpsertOperations:
    """Test class for comprehensive UPSERT functionality testing."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class with SQL Server configuration and database connection."""
        # Load configuration for UPSERT tests
        config_path = Path(__file__).parent / 'test_upsert_config.yaml'
        if not config_path.exists():
            # Fallback to main config if UPSERT config doesn't exist
            config_path = Path(__file__).parent.parent / 'src' / 'loader_config.yaml'
        
        cls.config = load_config(str(config_path))
        
        # Override configuration for UPSERT testing with SQL Server
        cls.config['database']['type'] = 'sqlserver'  # Use SQL Server for UPSERT tests
        cls.config['loader']['table_mode'] = 'upsert'  # Assume UPSERT mode will be implemented
        cls.config['job_statistics']['enabled'] = True
        cls.config['error_logging']['enabled'] = True
        
        # Test data directory
        cls.test_data_dir = Path(__file__).parent / 'data' / 'upsert'
        cls.test_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a temporary directory for test processing
        cls.temp_dir = tempfile.mkdtemp()
        cls.processed_dir = Path(cls.temp_dir) / 'processed'
        cls.error_dir = Path(cls.temp_dir) / 'error'
        cls.logs_dir = Path(cls.temp_dir) / 'logs'
        
        for dir_path in [cls.processed_dir, cls.error_dir, cls.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def teardown_class(cls):
        """Clean up test class resources."""
        # Clean up temporary directory
        if hasattr(cls, 'temp_dir') and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)
    
    def setup_method(self, method):
        """Set up each test method."""
        # Create a unique job run ID for each test
        self.job_run_id = uuid.uuid4()
        self.config['job_run_id'] = self.job_run_id
        
        # Create loader instance
        self.loader = FileToSQLLoader(self.config)
        
        # Test SQL Server connection
        connection_success = self.loader.connect_to_database()
        if not connection_success:
            pytest.skip("SQL Server not available - skipping UPSERT tests")
        
        # Set up statistics and error logging tables
        self.loader.setup_statistics_table()
        self.loader.setup_error_log_table()
        
        # Clean up any existing test tables
        self.cleanup_test_tables()
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Clean up test tables
        self.cleanup_test_tables()
        
        if hasattr(self, 'loader') and self.loader.connection:
            self.loader.connection.close()
    
    def cleanup_test_tables(self):
        """Clean up test tables from previous runs."""
        if not hasattr(self, 'loader') or not self.loader.connection:
            return
            
        try:
            cursor = self.loader.connection.cursor()
            # Get list of test tables (tables that start with 'upsert_')
            cursor.execute("""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME LIKE 'upsert_%'
            """)
            test_tables = [row[0] for row in cursor.fetchall()]
            
            for table_name in test_tables:
                cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
            
            self.loader.connection.commit()
        except pyodbc.Error as e:
            print(f"Warning: Could not clean up test tables: {e}")
    
    def copy_test_file_to_temp(self, test_file: str) -> str:
        """Copy a test file to temporary directory for processing."""
        source = self.test_data_dir / test_file
        temp_file = Path(self.temp_dir) / test_file
        shutil.copy2(source, temp_file)
        return str(temp_file)
    
    def get_table_data(self, table_name: str) -> List[Dict]:
        """Get all data from a table as a list of dictionaries."""
        cursor = self.loader.connection.cursor()
        cursor.execute(f"SELECT * FROM [{table_name}]")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        cursor = self.loader.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        return cursor.fetchone()[0]
    
    def create_initial_test_data(self, table_name: str, data: List[Dict]) -> bool:
        """Create initial test data in a table for UPSERT testing."""
        try:
            if not data:
                return True  # No data to create

            # Create DataFrame to analyze structure
            df = pd.DataFrame(data)

            # Set up the loader's columns_info for this data
            self.loader.columns_info = {}
            for col in df.columns:
                self.loader.columns_info[col] = {
                    'max_length': 0,
                    'has_nulls': False,
                    'all_numeric': True,
                    'all_integer': True,
                    'all_decimal': True,
                    'all_datetime': True,
                    'sample_values': []
                }

            # Analyze the data structure
            self.loader.analyze_file_structure(df)

            # Generate SQL types
            sql_types = self.loader.infer_sql_types()

            # Create the table with the exact name we want
            ddl = self.loader.generate_ddl(table_name, sql_types)

            cursor = self.loader.connection.cursor()
            cursor.execute(ddl)
            self.loader.connection.commit()

            # Insert the initial data directly
            columns = list(df.columns)
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO [{table_name}] ([{'], ['.join(columns)}]) VALUES ({placeholders})"

            for _, row in df.iterrows():
                values = [row[col] for col in columns]
                converted_values = self.loader._convert_values(values, columns)
                cursor.execute(insert_sql, tuple(converted_values))

            self.loader.connection.commit()
            return True

        except Exception as e:
            print(f"Error creating initial test data: {e}")
            return False
    
    def verify_upsert_results(self, table_name: str, expected_data: List[Dict], 
                            primary_key_col: str = 'id') -> bool:
        """Verify that UPSERT operations produced expected results."""
        actual_data = self.get_table_data(table_name)
        
        # Sort both datasets by primary key for comparison
        expected_sorted = sorted(expected_data, key=lambda x: x[primary_key_col])
        actual_sorted = sorted(actual_data, key=lambda x: x[primary_key_col])
        
        if len(expected_sorted) != len(actual_sorted):
            print(f"Row count mismatch: expected {len(expected_sorted)}, got {len(actual_sorted)}")
            return False
        
        for expected_row, actual_row in zip(expected_sorted, actual_sorted):
            for key, expected_value in expected_row.items():
                actual_value = actual_row.get(key)
                if actual_value != expected_value:
                    print(f"Value mismatch for {key}: expected {expected_value}, got {actual_value}")
                    return False
        
        return True

    # Test Methods Start Here
    
    def test_basic_update_operations(self):
        """Test UPDATE operations for existing records."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 35}
        ]
        
        table_name = 'upsert_basic_update_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"
        
        # Create update data (same IDs, different values)
        update_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': 31},
            {'id': 2, 'name': 'Jane Updated', 'email': 'jane.updated@example.com', 'age': 26}
        ]
        
        # Create test file with update data
        df_update = pd.DataFrame(update_data)
        update_file = self.test_data_dir / 'upsert_basic_update.csv'
        df_update.to_csv(update_file, index=False)
        
        # Process the update file
        temp_file = self.copy_test_file_to_temp('upsert_basic_update.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process update file"
        
        # Verify results - should have updated records plus unchanged record
        expected_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': 31},
            {'id': 2, 'name': 'Jane Updated', 'email': 'jane.updated@example.com', 'age': 26},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 35}  # Unchanged
        ]
        
        assert self.verify_upsert_results(table_name, expected_data), "UPSERT update verification failed"
        assert self.get_table_row_count(table_name) == 3, "Row count should remain 3 after updates"

    def test_basic_insert_operations(self):
        """Test INSERT operations for new records."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25}
        ]
        
        table_name = 'upsert_basic_insert_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"
        
        # Create insert data (new IDs)
        insert_data = [
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 35},
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': 28}
        ]
        
        # Create test file with insert data
        df_insert = pd.DataFrame(insert_data)
        insert_file = self.test_data_dir / 'upsert_basic_insert.csv'
        df_insert.to_csv(insert_file, index=False)
        
        # Process the insert file
        temp_file = self.copy_test_file_to_temp('upsert_basic_insert.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process insert file"
        
        # Verify results - should have original records plus new records
        expected_data = initial_data + insert_data
        
        assert self.verify_upsert_results(table_name, expected_data), "UPSERT insert verification failed"
        assert self.get_table_row_count(table_name) == 4, "Row count should be 4 after inserts"

    def test_mixed_upsert_operations(self):
        """Test mixed scenarios with both updates and inserts in the same batch."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 35}
        ]

        table_name = 'upsert_mixed_operations_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create mixed data (updates for existing IDs, inserts for new IDs)
        mixed_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': 31},  # Update
            {'id': 3, 'name': 'Bob Updated', 'email': 'bob.updated@example.com', 'age': 36},   # Update
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': 28},         # Insert
            {'id': 5, 'name': 'Charlie Wilson', 'email': 'charlie@example.com', 'age': 32}     # Insert
        ]

        # Create test file with mixed data
        df_mixed = pd.DataFrame(mixed_data)
        mixed_file = self.test_data_dir / 'upsert_mixed_operations.csv'
        df_mixed.to_csv(mixed_file, index=False)

        # Process the mixed file
        temp_file = self.copy_test_file_to_temp('upsert_mixed_operations.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process mixed operations file"

        # Verify results
        expected_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': 31},  # Updated
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25},           # Unchanged
            {'id': 3, 'name': 'Bob Updated', 'email': 'bob.updated@example.com', 'age': 36},   # Updated
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': 28},         # Inserted
            {'id': 5, 'name': 'Charlie Wilson', 'email': 'charlie@example.com', 'age': 32}     # Inserted
        ]

        assert self.verify_upsert_results(table_name, expected_data), "Mixed UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 5, "Row count should be 5 after mixed operations"

    def test_empty_dataset_upsert(self):
        """Test UPSERT operations with empty datasets."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25}
        ]

        table_name = 'upsert_empty_dataset_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Process the empty file (already exists with just headers)
        temp_file = self.copy_test_file_to_temp('upsert_empty_dataset.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process empty dataset file"

        # Verify results - data should remain unchanged
        assert self.verify_upsert_results(table_name, initial_data), "Empty dataset UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 2, "Row count should remain 2 after empty dataset"

    def test_duplicate_primary_keys_in_file(self):
        """Test UPSERT operations with duplicate primary keys in the same file."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30}
        ]

        table_name = 'upsert_duplicate_keys_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create data with duplicate primary keys (should use last occurrence)
        duplicate_data = [
            {'id': 1, 'name': 'John First', 'email': 'john.first@example.com', 'age': 31},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25},
            {'id': 1, 'name': 'John Last', 'email': 'john.last@example.com', 'age': 32}  # Duplicate - should win
        ]

        # Create test file with duplicate keys
        df_duplicate = pd.DataFrame(duplicate_data)
        duplicate_file = self.test_data_dir / 'upsert_duplicate_keys.csv'
        df_duplicate.to_csv(duplicate_file, index=False)

        # Process the duplicate file
        temp_file = self.copy_test_file_to_temp('upsert_duplicate_keys.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process duplicate keys file"

        # Verify results - should use the last occurrence of duplicate key
        expected_data = [
            {'id': 1, 'name': 'John Last', 'email': 'john.last@example.com', 'age': 32},  # Last occurrence wins
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25}      # New insert
        ]

        assert self.verify_upsert_results(table_name, expected_data), "Duplicate keys UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 2, "Row count should be 2 after duplicate key resolution"

    def test_null_values_upsert(self):
        """Test UPSERT operations with NULL values in various columns."""
        # Create initial data with some NULL values
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'department': 'IT'},
            {'id': 2, 'name': 'Jane Smith', 'email': None, 'age': 25, 'department': 'HR'},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': None, 'department': None}
        ]

        table_name = 'upsert_null_values_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create update data with NULL values
        null_data = [
            {'id': 1, 'name': 'John Updated', 'email': None, 'age': 31, 'department': 'IT'},        # Set email to NULL
            {'id': 2, 'name': None, 'email': 'jane.updated@example.com', 'age': 26, 'department': 'HR'},  # Set name to NULL
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': None, 'department': None}  # New record with NULLs
        ]

        # Create test file with NULL values
        df_null = pd.DataFrame(null_data)
        null_file = self.test_data_dir / 'upsert_null_values.csv'
        df_null.to_csv(null_file, index=False)

        # Process the NULL values file
        temp_file = self.copy_test_file_to_temp('upsert_null_values.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process NULL values file"

        # Verify results
        expected_data = [
            {'id': 1, 'name': 'John Updated', 'email': None, 'age': 31, 'department': 'IT'},
            {'id': 2, 'name': None, 'email': 'jane.updated@example.com', 'age': 26, 'department': 'HR'},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': None, 'department': None},  # Unchanged
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': None, 'department': None}  # New
        ]

        assert self.verify_upsert_results(table_name, expected_data), "NULL values UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 4, "Row count should be 4 after NULL values operations"

    def test_unicode_and_special_characters_upsert(self):
        """Test UPSERT operations with special characters and Unicode data."""
        # Create initial data with Unicode and special characters
        initial_data = [
            {'id': 1, 'name': 'José García', 'email': 'jose@example.com', 'description': 'Regular user'},
            {'id': 2, 'name': '李小明', 'email': 'li@example.com', 'description': 'Chinese user'},
            {'id': 3, 'name': 'Müller', 'email': 'muller@example.com', 'description': 'German user'}
        ]

        table_name = 'upsert_unicode_data_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create update data with more Unicode and special characters
        unicode_data = [
            {'id': 1, 'name': 'José García-López', 'email': 'jose.updated@example.com', 'description': 'Updated: café & résumé'},
            {'id': 2, 'name': '李小明 (Updated)', 'email': 'li.updated@example.com', 'description': '更新的用户'},
            {'id': 4, 'name': 'Владимир', 'email': 'vladimir@example.com', 'description': 'Русский пользователь'},
            {'id': 5, 'name': 'Ahmed محمد', 'email': 'ahmed@example.com', 'description': 'مستخدم عربي'}
        ]

        # Create test file with Unicode data
        df_unicode = pd.DataFrame(unicode_data)
        unicode_file = self.test_data_dir / 'upsert_unicode_data.csv'
        df_unicode.to_csv(unicode_file, index=False, encoding='utf-8')

        # Process the Unicode file
        temp_file = self.copy_test_file_to_temp('upsert_unicode_data.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process Unicode data file"

        # Verify results
        expected_data = [
            {'id': 1, 'name': 'José García-López', 'email': 'jose.updated@example.com', 'description': 'Updated: café & résumé'},
            {'id': 2, 'name': '李小明 (Updated)', 'email': 'li.updated@example.com', 'description': '更新的用户'},
            {'id': 3, 'name': 'Müller', 'email': 'muller@example.com', 'description': 'German user'},  # Unchanged
            {'id': 4, 'name': 'Владимир', 'email': 'vladimir@example.com', 'description': 'Русский пользователь'},
            {'id': 5, 'name': 'Ahmed محمد', 'email': 'ahmed@example.com', 'description': 'مستخدم عربي'}
        ]

        assert self.verify_upsert_results(table_name, expected_data), "Unicode UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 5, "Row count should be 5 after Unicode operations"

    def test_large_dataset_upsert(self):
        """Test UPSERT operations with large datasets for performance testing."""
        # Create initial large dataset (1000 records)
        initial_data = []
        for i in range(1, 1001):
            initial_data.append({
                'id': i,
                'name': f'User {i}',
                'email': f'user{i}@example.com',
                'age': 20 + (i % 50),
                'department': f'Dept {i % 10}',
                'salary': 30000 + (i * 100)
            })

        table_name = 'upsert_large_dataset_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial large data"

        # Create update data (update first 500, insert 500 new)
        large_update_data = []
        # Updates for existing records
        for i in range(1, 501):
            large_update_data.append({
                'id': i,
                'name': f'Updated User {i}',
                'email': f'updated.user{i}@example.com',
                'age': 25 + (i % 50),
                'department': f'Updated Dept {i % 10}',
                'salary': 35000 + (i * 100)
            })
        # New records
        for i in range(1001, 1501):
            large_update_data.append({
                'id': i,
                'name': f'New User {i}',
                'email': f'new.user{i}@example.com',
                'age': 22 + (i % 50),
                'department': f'New Dept {i % 10}',
                'salary': 32000 + (i * 100)
            })

        # Create test file with large dataset
        df_large = pd.DataFrame(large_update_data)
        large_file = self.test_data_dir / 'upsert_large_dataset.csv'
        df_large.to_csv(large_file, index=False)

        # Process the large file and measure time
        start_time = time.time()
        temp_file = self.copy_test_file_to_temp('upsert_large_dataset.csv')
        success = self.loader.process_file(temp_file)
        end_time = time.time()

        assert success, "Failed to process large dataset file"

        # Verify results
        final_count = self.get_table_row_count(table_name)
        assert final_count == 1500, f"Expected 1500 rows after large UPSERT, got {final_count}"

        # Performance check - should complete within reasonable time (adjust as needed)
        processing_time = end_time - start_time
        print(f"Large dataset UPSERT processing time: {processing_time:.2f} seconds")
        assert processing_time < 300, f"Large dataset processing took too long: {processing_time:.2f} seconds"

    def test_data_type_boundary_conditions(self):
        """Test UPSERT operations with data type boundary conditions."""
        from decimal import Decimal
        from datetime import datetime, date

        # Create initial data with boundary values
        initial_data = [
            {
                'id': 1,
                'name': 'A' * 255,  # Max varchar length
                'age': 0,  # Min age
                'salary': 0.01,  # Min decimal
                'created_date': '1900-01-01',  # Min date
                'is_active': True
            },
            {
                'id': 2,
                'name': 'Short',
                'age': 150,  # Max reasonable age
                'salary': 999999.99,  # Max decimal
                'created_date': '2099-12-31',  # Max date
                'is_active': False
            }
        ]

        table_name = 'upsert_boundary_data_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial boundary data"

        # Create update data with different boundary values
        boundary_data = [
            {
                'id': 1,
                'name': 'B' * 254,  # Different max length
                'age': 1,  # Min + 1
                'salary': 0.02,  # Min + 0.01
                'created_date': '1900-01-02',  # Min + 1 day
                'is_active': False
            },
            {
                'id': 3,
                'name': '',  # Empty string
                'age': 149,  # Max - 1
                'salary': 999999.98,  # Max - 0.01
                'created_date': '2099-12-30',  # Max - 1 day
                'is_active': True
            }
        ]

        # Create test file with boundary data
        df_boundary = pd.DataFrame(boundary_data)
        boundary_file = self.test_data_dir / 'upsert_boundary_data.csv'
        df_boundary.to_csv(boundary_file, index=False)

        # Process the boundary file
        temp_file = self.copy_test_file_to_temp('upsert_boundary_data.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process boundary data file"

        # Verify results
        final_count = self.get_table_row_count(table_name)
        assert final_count == 3, f"Expected 3 rows after boundary UPSERT, got {final_count}"

    def test_constraint_violation_handling(self):
        """Test UPSERT operations with constraint violations and error handling."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25}
        ]

        table_name = 'upsert_constraint_test_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial constraint data"

        # Create data that might violate constraints
        constraint_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': -5},  # Invalid age
            {'id': 2, 'name': 'Jane Updated', 'email': 'jane.updated@example.com', 'age': 26},  # Valid update
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 200},  # Invalid age
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': 28}  # Valid insert
        ]

        # Create test file with constraint violations
        df_constraint = pd.DataFrame(constraint_data)
        constraint_file = self.test_data_dir / 'upsert_constraint_violations.csv'
        df_constraint.to_csv(constraint_file, index=False)

        # Process the constraint file (should handle errors gracefully)
        temp_file = self.copy_test_file_to_temp('upsert_constraint_violations.csv')
        success = self.loader.process_file(temp_file)

        # Depending on transaction mode, this might succeed with partial data or fail completely
        # The test should verify that error handling works correctly
        if success:
            # If tolerant mode, check that valid records were processed
            final_count = self.get_table_row_count(table_name)
            assert final_count >= 2, "At least original records should remain"
        else:
            # If strict mode, original data should be unchanged
            final_count = self.get_table_row_count(table_name)
            assert final_count == 2, "Original data should be unchanged after constraint violations"

    def test_different_file_formats_upsert(self):
        """Test UPSERT operations with different file formats (CSV, JSON, PSV)."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25}
        ]

        table_name = 'upsert_file_formats_test'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Set table name override for consistent UPSERT target
        self.config['loader']['override_table_name'] = table_name

        # Test JSON format UPSERT
        json_data = [
            {'id': 1, 'name': 'John JSON', 'email': 'john.json@example.com', 'age': 31},
            {'id': 3, 'name': 'Bob JSON', 'email': 'bob.json@example.com', 'age': 35}
        ]

        df_json = pd.DataFrame(json_data)
        json_file = self.test_data_dir / 'upsert_file_formats.json'
        df_json.to_json(json_file, orient='records', indent=2)

        # Create new loader instance with updated config
        json_loader = FileToSQLLoader(self.config)
        json_loader.connect_to_database()

        temp_json_file = self.copy_test_file_to_temp('upsert_file_formats.json')
        success = json_loader.process_file(temp_json_file)
        assert success, "Failed to process JSON UPSERT file"
        json_loader.connection.close()

        # Test PSV format UPSERT
        psv_data = [
            {'id': 2, 'name': 'Jane PSV', 'email': 'jane.psv@example.com', 'age': 26},
            {'id': 4, 'name': 'Alice PSV', 'email': 'alice.psv@example.com', 'age': 28}
        ]

        df_psv = pd.DataFrame(psv_data)
        psv_file = self.test_data_dir / 'upsert_file_formats.psv'
        df_psv.to_csv(psv_file, sep='|', index=False)

        # Create new loader instance with updated config
        psv_loader = FileToSQLLoader(self.config)
        psv_loader.connect_to_database()

        temp_psv_file = self.copy_test_file_to_temp('upsert_file_formats.psv')
        success = psv_loader.process_file(temp_psv_file)
        assert success, "Failed to process PSV UPSERT file"
        psv_loader.connection.close()

        # Verify final results
        final_count = self.get_table_row_count(table_name)
        assert final_count == 4, f"Expected 4 rows after multi-format UPSERT, got {final_count}"

    def test_transaction_mode_behavior(self):
        """Test UPSERT behavior in different transaction modes (strict vs tolerant)."""
        # Test strict transaction mode
        strict_config = self.config.copy()
        strict_config['loader']['transaction_mode'] = 'strict'

        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30}
        ]

        table_name = 'upsert_transaction_mode_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create data with one invalid record that should cause rollback in strict mode
        mixed_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': 31},  # Valid update
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 'invalid'}  # Invalid age
        ]

        df_mixed = pd.DataFrame(mixed_data)
        mixed_file = self.test_data_dir / 'upsert_transaction_mode.csv'
        df_mixed.to_csv(mixed_file, index=False)

        # Test with strict mode loader
        strict_loader = FileToSQLLoader(strict_config)
        assert strict_loader.connect_to_database(), "Failed to connect with strict config"

        temp_file = self.copy_test_file_to_temp('upsert_transaction_mode.csv')
        success = strict_loader.process_file(temp_file)

        # In strict mode, the entire transaction should fail due to invalid data
        # Original data should remain unchanged
        original_count = self.get_table_row_count(table_name)
        assert original_count == 1, "Strict mode should preserve original data on error"

        strict_loader.connection.close()

    def test_upsert_statistics_and_logging(self):
        """Test that UPSERT operations are properly logged and statistics are recorded."""
        # Create initial data
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25}
        ]

        table_name = 'upsert_statistics_test_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create mixed UPSERT data
        upsert_data = [
            {'id': 1, 'name': 'John Updated', 'email': 'john.updated@example.com', 'age': 31},  # Update
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 35}  # Insert
        ]

        df_upsert = pd.DataFrame(upsert_data)
        upsert_file = self.test_data_dir / 'upsert_statistics_test.csv'
        df_upsert.to_csv(upsert_file, index=False)

        # Process the UPSERT file
        temp_file = self.copy_test_file_to_temp('upsert_statistics_test.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process UPSERT statistics test file"

        # Check that statistics were recorded (look for any recent statistics for this table)
        cursor = self.loader.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM EtlJobStatistics
            WHERE TargetTable = ? AND JobEndTime >= DATEADD(minute, -5, GETDATE())
        """, table_name)
        stats_count = cursor.fetchone()[0]
        assert stats_count > 0, "UPSERT statistics should be recorded"

        # Check specific statistics values
        cursor.execute("""
            SELECT TOP 1 RowsInserted, RowsUpdated, RowsFailed
            FROM EtlJobStatistics
            WHERE TargetTable = ? AND JobEndTime >= DATEADD(minute, -5, GETDATE())
            ORDER BY JobEndTime DESC
        """, table_name)

        stats = cursor.fetchone()
        if stats:
            rows_inserted, rows_updated, rows_failed = stats
            # Verify that both inserts and updates were tracked
            assert rows_inserted >= 0, "Inserted rows should be tracked"
            assert rows_updated >= 0, "Updated rows should be tracked"
            assert rows_failed >= 0, "Failed rows should be tracked"
            # In UPSERT mode, we should have some updated rows
            assert rows_updated > 0, "Should have some updated rows in UPSERT mode"


# Integration test functions
def test_upsert_integration_with_sql_server():
    """Integration test for UPSERT operations with actual SQL Server."""
    config_path = Path(__file__).parent / 'test_upsert_config.yaml'
    if not config_path.exists():
        pytest.skip("UPSERT configuration file not found")

    config = load_config(str(config_path))
    loader = FileToSQLLoader(config)

    # Test SQL Server connection
    if not loader.connect_to_database():
        pytest.skip("SQL Server not available for integration testing")

    # Run a simple UPSERT integration test
    test_data = [
        {'id': 1, 'name': 'Integration Test', 'email': 'test@example.com', 'age': 30}
    ]

    df = pd.DataFrame(test_data)
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    df.to_csv(temp_file.name, index=False)

    try:
        success = loader.process_file(temp_file.name)
        assert success, "Integration test should succeed"
    finally:
        # File may have been moved to processed directory, so check if it still exists
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        loader.connection.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

    def test_null_values_upsert(self):
        """Test UPSERT operations with NULL values in various columns."""
        # Create initial data with some NULL values
        initial_data = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'department': 'IT'},
            {'id': 2, 'name': 'Jane Smith', 'email': None, 'age': 25, 'department': 'HR'},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': None, 'department': None}
        ]

        table_name = 'upsert_null_values_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create update data with NULL values
        null_data = [
            {'id': 1, 'name': 'John Updated', 'email': None, 'age': 31, 'department': 'IT'},        # Set email to NULL
            {'id': 2, 'name': None, 'email': 'jane.updated@example.com', 'age': 26, 'department': 'HR'},  # Set name to NULL
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': None, 'department': None}  # New record with NULLs
        ]

        # Create test file with NULL values
        df_null = pd.DataFrame(null_data)
        null_file = self.test_data_dir / 'upsert_null_values.csv'
        df_null.to_csv(null_file, index=False)

        # Process the NULL values file
        temp_file = self.copy_test_file_to_temp('upsert_null_values.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process NULL values file"

        # Verify results
        expected_data = [
            {'id': 1, 'name': 'John Updated', 'email': None, 'age': 31, 'department': 'IT'},
            {'id': 2, 'name': None, 'email': 'jane.updated@example.com', 'age': 26, 'department': 'HR'},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': None, 'department': None},  # Unchanged
            {'id': 4, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': None, 'department': None}  # New
        ]

        assert self.verify_upsert_results(table_name, expected_data), "NULL values UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 4, "Row count should be 4 after NULL values operations"

    def test_unicode_and_special_characters_upsert(self):
        """Test UPSERT operations with special characters and Unicode data."""
        # Create initial data with Unicode and special characters
        initial_data = [
            {'id': 1, 'name': 'José García', 'email': 'jose@example.com', 'description': 'Regular user'},
            {'id': 2, 'name': '李小明', 'email': 'li@example.com', 'description': 'Chinese user'},
            {'id': 3, 'name': 'Müller', 'email': 'muller@example.com', 'description': 'German user'}
        ]

        table_name = 'upsert_unicode_data_csv'
        assert self.create_initial_test_data(table_name, initial_data), "Failed to create initial data"

        # Create update data with more Unicode and special characters
        unicode_data = [
            {'id': 1, 'name': 'José García-López', 'email': 'jose.updated@example.com', 'description': 'Updated: café & résumé'},
            {'id': 2, 'name': '李小明 (Updated)', 'email': 'li.updated@example.com', 'description': '更新的用户'},
            {'id': 4, 'name': 'Владимир', 'email': 'vladimir@example.com', 'description': 'Русский пользователь'},
            {'id': 5, 'name': 'Ahmed محمد', 'email': 'ahmed@example.com', 'description': 'مستخدم عربي'}
        ]

        # Create test file with Unicode data
        df_unicode = pd.DataFrame(unicode_data)
        unicode_file = self.test_data_dir / 'upsert_unicode_data.csv'
        df_unicode.to_csv(unicode_file, index=False, encoding='utf-8')

        # Process the Unicode file
        temp_file = self.copy_test_file_to_temp('upsert_unicode_data.csv')
        success = self.loader.process_file(temp_file)
        assert success, "Failed to process Unicode data file"

        # Verify results
        expected_data = [
            {'id': 1, 'name': 'José García-López', 'email': 'jose.updated@example.com', 'description': 'Updated: café & résumé'},
            {'id': 2, 'name': '李小明 (Updated)', 'email': 'li.updated@example.com', 'description': '更新的用户'},
            {'id': 3, 'name': 'Müller', 'email': 'muller@example.com', 'description': 'German user'},  # Unchanged
            {'id': 4, 'name': 'Владимир', 'email': 'vladimir@example.com', 'description': 'Русский пользователь'},
            {'id': 5, 'name': 'Ahmed محمد', 'email': 'ahmed@example.com', 'description': 'مستخدم عربي'}
        ]

        assert self.verify_upsert_results(table_name, expected_data), "Unicode UPSERT verification failed"
        assert self.get_table_row_count(table_name) == 5, "Row count should be 5 after Unicode operations"
