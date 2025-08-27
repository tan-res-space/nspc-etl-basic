# UPSERT Operations Test Suite

This directory contains comprehensive test files for testing UPSERT (UPDATE/INSERT) operations in the file-to-sql-loader.py script when tables already exist in the SQL Server database.

## Overview

The UPSERT functionality allows the ETL system to:
- **UPDATE** existing records when a matching primary key is found
- **INSERT** new records when no matching primary key exists
- Handle mixed scenarios with both updates and inserts in the same batch
- Provide robust error handling and data validation

## Test Files

### Main Test File
- `test_upsert_operations.py` - Comprehensive test suite for UPSERT functionality

### Configuration Files
- `test_upsert_config.yaml` - SQL Server configuration for UPSERT tests
- `run_upsert_tests.py` - Test runner script with advanced options

### Test Data Files (in `data/upsert/` directory)
- `upsert_initial_data.csv` - Initial data to populate tables before UPSERT operations
- `upsert_update_data.csv` - Data with existing keys but updated values
- `upsert_insert_data.csv` - Data with new keys to be inserted
- `upsert_mixed_data.csv` - Mixed updates and inserts in the same batch
- `upsert_empty_dataset.csv` - Empty dataset for edge case testing
- `upsert_duplicate_keys.csv` - Duplicate primary keys within the same file
- `upsert_null_values.csv` - Various NULL value scenarios
- `upsert_unicode_data.csv` - Special characters and Unicode data
- `upsert_boundary_data.csv` - Data type boundary conditions
- `upsert_invalid_data.csv` - Invalid data for error handling tests
- `upsert_json_format.json` - JSON format test data
- `upsert_psv_format.psv` - Pipe-separated values test data

## Test Categories

### 1. Basic UPSERT Operations
- **UPDATE Operations**: Test updating existing records with same primary key
- **INSERT Operations**: Test inserting new records with new primary keys
- **Mixed Operations**: Test files containing both updates and inserts

### 2. Edge Cases
- **Empty Datasets**: Verify behavior with empty input files
- **Duplicate Primary Keys**: Handle duplicate keys within the same file
- **NULL Values**: Test NULL handling in various columns
- **Large Datasets**: Performance testing with 1000+ records
- **Unicode Data**: Special characters, international text, emojis
- **Boundary Conditions**: Min/max values for different data types

### 3. Error Handling
- **Invalid Data Types**: Test with incompatible data types
- **Constraint Violations**: Test age limits, email format validation
- **Transaction Modes**: Test both strict and tolerant transaction handling
- **Rollback Scenarios**: Verify proper rollback on errors

### 4. File Format Support
- **CSV Files**: Standard comma-separated values
- **JSON Files**: JavaScript Object Notation format
- **PSV Files**: Pipe-separated values format

### 5. Performance Testing
- **Large Dataset Processing**: Test with 1000+ records
- **Batch Processing**: Verify efficient batch operations
- **Memory Usage**: Monitor memory consumption during large operations
- **Processing Time**: Benchmark processing speed

## Prerequisites

### SQL Server Setup
1. **SQL Server Instance**: Running locally in Podman
   ```bash
   podman run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" \
      -p 1433:1433 --name sqlserver \
      -d mcr.microsoft.com/mssql/server:2019-latest
   ```

2. **Test Database**: Create a test database named 'TestDB'
   ```sql
   CREATE DATABASE TestDB;
   ```

3. **ODBC Driver**: Install ODBC Driver 17 for SQL Server

### Python Dependencies
Install required packages:
```bash
pip install -r requirements-test.txt
```

Required packages include:
- pytest
- pandas
- pyodbc
- pyyaml

## Running the Tests

### Quick Start
```bash
# Run all UPSERT tests
python test/run_upsert_tests.py

# Run with verbose output
python test/run_upsert_tests.py --verbose

# Run only SQL Server tests (no SQLite fallback)
python test/run_upsert_tests.py --sql-server-only
```

### Advanced Options
```bash
# Include performance benchmarking
python test/run_upsert_tests.py --performance

# Clean up test tables after running
python test/run_upsert_tests.py --cleanup

# Use custom configuration file
python test/run_upsert_tests.py --config custom_config.yaml

# Validate test environment only
python test/run_upsert_tests.py --validate-only
```

### Direct pytest Execution
```bash
# Run specific test class
pytest test/test_upsert_operations.py::TestUpsertOperations -v

# Run specific test method
pytest test/test_upsert_operations.py::TestUpsertOperations::test_basic_update_operations -v

# Run with custom markers
pytest test/test_upsert_operations.py -m "not performance" -v
```

## Test Configuration

### SQL Server Configuration
The `test_upsert_config.yaml` file contains:
- Database connection settings
- UPSERT-specific configuration
- Transaction mode settings
- Error handling options
- Performance optimization settings

### Key Configuration Options
```yaml
loader:
  table_mode: 'upsert'  # Enable UPSERT mode
  transaction_mode: 'tolerant'  # Allow partial success
  primary_key_columns: ['id']  # Define primary key columns

upsert:
  strategy: 'sql_merge'  # UPSERT implementation strategy
  conflict_resolution:
    duplicate_keys_in_source: 'last_wins'
    constraint_violations: 'skip_row'
```

## Expected Behavior

### UPSERT Logic
1. **Primary Key Match Found**: UPDATE the existing record
2. **No Primary Key Match**: INSERT as new record
3. **Duplicate Keys in Source**: Use last occurrence (configurable)
4. **Constraint Violations**: Skip row and log error (configurable)

### Transaction Handling
- **Strict Mode**: All-or-nothing transaction (rollback on any error)
- **Tolerant Mode**: Process valid rows, skip invalid ones

### Error Logging
- Constraint violations logged to `EtlJobError` table
- Processing statistics recorded in `EtlJobStatistics` table
- Failed rows written to log files

## Test Data Schema

All test data files use this schema:
```sql
CREATE TABLE test_table (
    id INT PRIMARY KEY,
    name NVARCHAR(255),
    email NVARCHAR(255),
    age INT CHECK (age >= 0 AND age <= 150),
    department NVARCHAR(100),
    salary DECIMAL(10,2),
    created_date DATE,
    is_active BIT,
    description NVARCHAR(MAX),
    long_text NVARCHAR(MAX)
);
```

## Troubleshooting

### Common Issues
1. **SQL Server Connection Failed**
   - Verify Podman container is running
   - Check connection string in configuration
   - Ensure ODBC driver is installed

2. **Test Data Files Missing**
   - Run `python test/run_upsert_tests.py --validate-only`
   - Check that all required files exist in `test/data/upsert/`

3. **Permission Errors**
   - Ensure SQL Server user has CREATE/DROP table permissions
   - Verify database 'TestDB' exists and is accessible

4. **Performance Test Timeouts**
   - Adjust timeout values in configuration
   - Consider hardware limitations for large dataset tests

### Debug Mode
Enable debug logging for detailed troubleshooting:
```bash
python test/run_upsert_tests.py --verbose
```

## Contributing

When adding new UPSERT tests:
1. Follow the existing naming convention
2. Add corresponding test data files
3. Update this README with new test descriptions
4. Ensure tests work with both strict and tolerant transaction modes
5. Include proper cleanup in test methods

## Notes

- These tests assume UPSERT functionality will be implemented in the main script
- Tests are designed to work specifically with SQL Server (not SQLite)
- All test tables are automatically cleaned up after test execution
- Performance benchmarks may vary based on hardware and SQL Server configuration
