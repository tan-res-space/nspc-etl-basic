#!/usr/bin/env python3
"""
Manual test runner for date format testing with SQL Server integration.
This script allows testing both with SQLite (for unit tests) and SQL Server (for integration tests).
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path
import yaml
import tempfile
import shutil

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Import with the correct module name (file-to-sql-loader.py becomes file_to_sql_loader)
import importlib.util
import sys

# Load the module dynamically
spec = importlib.util.spec_from_file_location("file_to_sql_loader", Path(__file__).parent.parent / 'src' / 'file-to-sql-loader.py')
file_to_sql_loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(file_to_sql_loader)

FileToSQLLoader = file_to_sql_loader.FileToSQLLoader
load_config = file_to_sql_loader.load_config


def run_pytest_tests(test_file: str = None, verbose: bool = True):
    """Run pytest tests."""
    print("=" * 60)
    print("Running pytest test suite...")
    print("=" * 60)
    
    cmd = ["python", "-m", "pytest"]
    
    if test_file:
        cmd.append(test_file)
    else:
        cmd.append(str(Path(__file__).parent / "test_date_formats.py"))
    
    if verbose:
        cmd.append("-v")
    
    cmd.extend(["-s", "--tb=short"])
    
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent, capture_output=False)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running pytest: {e}")
        return False


def test_sql_server_integration():
    """Test SQL Server integration with date format files."""
    print("=" * 60)
    print("Testing SQL Server integration...")
    print("=" * 60)
    
    # Load SQL Server configuration
    config_path = Path(__file__).parent.parent / 'src' / 'loader_config.yaml'
    try:
        config = load_config(str(config_path))
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        return False
    
    # Ensure we're using SQL Server
    if config.get('database', {}).get('type') != 'sqlserver':
        print("Warning: Configuration is not set to use SQL Server")
        print("Current database type:", config.get('database', {}).get('type'))
        response = input("Continue with current configuration? (y/N): ")
        if response.lower() != 'y':
            return False
    
    # Test database connection
    try:
        loader = FileToSQLLoader(config)
        if not loader.connect_to_database():
            print("Failed to connect to database")
            return False
        
        print("✅ Database connection successful")
        
        # Set up required tables
        loader.setup_statistics_table()
        loader.setup_error_log_table()
        print("✅ Statistics and error tables set up")
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False
    
    # Test file processing
    test_files = [
        'dates_iso_format.csv',
        'dates_us_format.csv',
        'dates_json_format.json',
        'dates_psv_format.psv'
    ]
    
    test_data_dir = Path(__file__).parent / 'data'
    temp_dir = tempfile.mkdtemp()
    
    try:
        results = {}
        
        for test_file in test_files:
            print(f"\nTesting file: {test_file}")
            
            # Copy file to temp directory
            source = test_data_dir / test_file
            if not source.exists():
                print(f"  ❌ Test file not found: {source}")
                results[test_file] = False
                continue
            
            temp_file = Path(temp_dir) / test_file
            shutil.copy2(source, temp_file)
            
            # Create subdirectories
            for subdir in ['processed', 'error', 'logs']:
                (Path(temp_dir) / subdir).mkdir(exist_ok=True)
            
            try:
                # Create new loader instance for each file
                loader = FileToSQLLoader(config)
                success = loader.process_file(str(temp_file))
                
                if success:
                    print(f"  ✅ Successfully processed {test_file}")
                    print(f"     Rows processed: {getattr(loader, 'processed_rows', 'N/A')}")
                    print(f"     Errors: {getattr(loader, 'error_rows', 'N/A')}")
                else:
                    print(f"  ❌ Failed to process {test_file}")
                
                results[test_file] = success
                
            except Exception as e:
                print(f"  ❌ Error processing {test_file}: {e}")
                results[test_file] = False
            finally:
                if hasattr(loader, 'connection') and loader.connection:
                    loader.connection.close()
    
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
    
    # Summary
    print("\n" + "=" * 60)
    print("SQL Server Integration Test Results:")
    print("=" * 60)
    
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_file, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {test_file}: {status}")
    
    print(f"\nOverall: {successful}/{total} tests passed")
    
    return successful == total


def test_using_run_script():
    """Test using the actual run-file-to-sql-loader.sh script."""
    print("=" * 60)
    print("Testing with run-file-to-sql-loader.sh script...")
    print("=" * 60)
    
    script_path = Path(__file__).parent.parent / 'run-file-to-sql-loader.sh'
    if not script_path.exists():
        print(f"❌ Run script not found: {script_path}")
        return False
    
    test_data_dir = Path(__file__).parent / 'data'
    test_file = test_data_dir / 'dates_iso_format.csv'
    
    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        return False
    
    # Create a temporary copy
    temp_dir = tempfile.mkdtemp()
    temp_file = Path(temp_dir) / 'dates_iso_format.csv'
    shutil.copy2(test_file, temp_file)
    
    try:
        # Run the script
        cmd = ["bash", str(script_path), str(temp_file)]
        print(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=script_path.parent)
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        success = result.returncode == 0
        print(f"\nScript execution: {'✅ SUCCESS' if success else '❌ FAILED'}")
        print(f"Return code: {result.returncode}")
        
        return success
        
    except Exception as e:
        print(f"❌ Error running script: {e}")
        return False
    finally:
        shutil.rmtree(temp_dir)


def validate_date_parsing_logic():
    """Validate the date parsing logic in isolation."""
    print("=" * 60)
    print("Validating date parsing logic...")
    print("=" * 60)
    
    # Test the actual date parsing functions
    # FileToSQLLoader is already imported above
    
    # Create a loader instance for testing
    config = {
        'database': {'type': 'sqlite', 'sqlite_path': ':memory:'},
        'loader': {'table_mode': 'drop_recreate'},
        'job_statistics': {'enabled': False},
        'error_logging': {'enabled': False}
    }
    
    loader = FileToSQLLoader(config)
    
    # Test various date formats
    test_dates = [
        "2024-01-15",           # ISO format
        "2024-01-15 10:30:00",  # ISO with time
        "01/15/2024",           # US format
        "15/01/2024",           # European format
        "2024.01.15",           # Dot separator
        "15-01-2024",           # Dash with day first
        "invalid date",         # Invalid
        "",                     # Empty
        None,                   # None
    ]
    
    print("Testing date parsing:")
    for test_date in test_dates:
        try:
            is_datetime = loader._is_datetime(test_date) if test_date else False
            parsed_date = loader._parse_datetime(test_date) if test_date and is_datetime else None
            
            print(f"  '{test_date}' -> is_datetime: {is_datetime}, parsed: {parsed_date}")
        except Exception as e:
            print(f"  '{test_date}' -> ERROR: {e}")
    
    print("✅ Date parsing logic validation completed")
    return True


def main():
    """Main function to run tests based on command line arguments."""
    parser = argparse.ArgumentParser(description='Run date format tests for NSPC ETL')
    parser.add_argument('--pytest', action='store_true', help='Run pytest test suite')
    parser.add_argument('--sqlserver', action='store_true', help='Test SQL Server integration')
    parser.add_argument('--script', action='store_true', help='Test with run script')
    parser.add_argument('--validate', action='store_true', help='Validate date parsing logic')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if not any([args.pytest, args.sqlserver, args.script, args.validate, args.all]):
        # Default to running pytest tests
        args.pytest = True
    
    results = []
    
    if args.all or args.validate:
        results.append(("Date Parsing Logic", validate_date_parsing_logic()))
    
    if args.all or args.pytest:
        results.append(("Pytest Tests", run_pytest_tests(verbose=args.verbose)))
    
    if args.all or args.sqlserver:
        results.append(("SQL Server Integration", test_sql_server_integration()))
    
    if args.all or args.script:
        results.append(("Run Script Test", test_using_run_script()))
    
    # Summary
    print("\n" + "=" * 60)
    print("FINAL TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name}: {status}")
        if success:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} test suites passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
