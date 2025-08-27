#!/usr/bin/env python3
"""
Test runner for UPSERT operations in the NSPC ETL System.
This script runs comprehensive UPSERT tests against SQL Server.

Prerequisites:
- SQL Server running locally in Podman
- Test database 'TestDB' created
- ODBC Driver 17 for SQL Server installed
- Required Python packages installed (see requirements-test.txt)

Usage:
    python run_upsert_tests.py [options]

Options:
    --verbose, -v       Verbose output
    --sql-server-only   Run only SQL Server tests (skip SQLite fallback)
    --performance       Include performance benchmarking tests
    --cleanup           Clean up test tables after running
    --config CONFIG     Use specific configuration file
"""

import sys
import os
import argparse
import pytest
import logging
from pathlib import Path
from typing import List, Optional

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Import the loader module
import importlib.util
spec = importlib.util.spec_from_file_location("file_to_sql_loader", Path(__file__).parent.parent / 'src' / 'file-to-sql-loader.py')
file_to_sql_loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(file_to_sql_loader)

FileToSQLLoader = file_to_sql_loader.FileToSQLLoader
load_config = file_to_sql_loader.load_config


def setup_logging(verbose: bool = False):
    """Set up logging for the test runner."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path(__file__).parent / 'logs' / 'upsert_test_runner.log')
        ]
    )


def check_sql_server_connection(config_path: str) -> bool:
    """Check if SQL Server is available for testing."""
    try:
        config = load_config(config_path)
        loader = FileToSQLLoader(config)
        success = loader.connect_to_database()
        if success:
            loader.connection.close()
        return success
    except Exception as e:
        logging.error(f"SQL Server connection check failed: {e}")
        return False


def run_upsert_tests(args) -> int:
    """Run the UPSERT test suite."""
    # Set up logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    # Determine configuration file
    config_path = args.config or str(Path(__file__).parent / 'test_upsert_config.yaml')
    
    if not Path(config_path).exists():
        logger.error(f"Configuration file not found: {config_path}")
        return 1
    
    # Check SQL Server connection
    if not check_sql_server_connection(config_path):
        if args.sql_server_only:
            logger.error("SQL Server not available and --sql-server-only specified")
            return 1
        else:
            logger.warning("SQL Server not available, some tests may be skipped")
    
    # Prepare pytest arguments
    pytest_args = [
        str(Path(__file__).parent / 'test_upsert_operations.py'),
        '-v' if args.verbose else '-q',
        '--tb=short',
        f'--config-file={config_path}'
    ]
    
    # Add performance tests if requested
    if args.performance:
        pytest_args.extend(['-m', 'performance'])
    
    # Add cleanup marker if requested
    if args.cleanup:
        pytest_args.extend(['--cleanup'])
    
    # Run the tests
    logger.info("Starting UPSERT test suite...")
    logger.info(f"Configuration: {config_path}")
    logger.info(f"Test arguments: {' '.join(pytest_args)}")
    
    try:
        exit_code = pytest.main(pytest_args)
        
        if exit_code == 0:
            logger.info("All UPSERT tests passed successfully!")
        else:
            logger.error(f"UPSERT tests failed with exit code: {exit_code}")
        
        return exit_code
        
    except Exception as e:
        logger.error(f"Error running UPSERT tests: {e}")
        return 1


def validate_test_environment() -> List[str]:
    """Validate that the test environment is properly set up."""
    issues = []
    
    # Check required directories
    required_dirs = [
        Path(__file__).parent / 'data' / 'upsert',
        Path(__file__).parent / 'logs'
    ]
    
    for dir_path in required_dirs:
        if not dir_path.exists():
            issues.append(f"Required directory missing: {dir_path}")
    
    # Check required test data files
    required_files = [
        'upsert_initial_data.csv',
        'upsert_update_data.csv',
        'upsert_insert_data.csv',
        'upsert_mixed_data.csv',
        'upsert_null_values.csv',
        'upsert_duplicate_keys.csv',
        'upsert_unicode_data.csv',
        'upsert_boundary_data.csv',
        'upsert_invalid_data.csv',
        'upsert_json_format.json',
        'upsert_psv_format.psv',
        'upsert_empty_dataset.csv'
    ]
    
    data_dir = Path(__file__).parent / 'data' / 'upsert'
    for file_name in required_files:
        file_path = data_dir / file_name
        if not file_path.exists():
            issues.append(f"Required test data file missing: {file_path}")
    
    # Check configuration file
    config_path = Path(__file__).parent / 'test_upsert_config.yaml'
    if not config_path.exists():
        issues.append(f"UPSERT configuration file missing: {config_path}")
    
    return issues


def main():
    """Main entry point for the UPSERT test runner."""
    parser = argparse.ArgumentParser(
        description='Run comprehensive UPSERT tests for the NSPC ETL System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--sql-server-only',
        action='store_true',
        help='Run only SQL Server tests (skip SQLite fallback)'
    )
    
    parser.add_argument(
        '--performance',
        action='store_true',
        help='Include performance benchmarking tests'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up test tables after running'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file (default: test_upsert_config.yaml)'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate test environment, do not run tests'
    )
    
    args = parser.parse_args()
    
    # Validate test environment
    issues = validate_test_environment()
    if issues:
        print("Test environment validation failed:")
        for issue in issues:
            print(f"  - {issue}")
        
        if args.validate_only:
            return 1
        
        print("\nContinuing with tests despite validation issues...")
    elif args.validate_only:
        print("Test environment validation passed!")
        return 0
    
    # Run the tests
    return run_upsert_tests(args)


if __name__ == "__main__":
    sys.exit(main())
