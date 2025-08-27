# NSPC ETL Testing Suite

This directory contains comprehensive tests for the NSPC ETL system, with a focus on date format handling and data processing validation.

## Directory Structure

```
test/
├── data/                           # Test data files
│   ├── dates_iso_format.csv        # ISO date format (YYYY-MM-DD)
│   ├── dates_us_format.csv         # US date format (MM/DD/YYYY)
│   ├── dates_european_format.csv   # European date format (DD/MM/YYYY)
│   ├── dates_mixed_separators.csv  # Mixed separators (dash, dot, slash)
│   ├── dates_edge_cases.csv        # Leap years, end of month, etc.
│   ├── dates_invalid_formats.csv   # Invalid date formats for error testing
│   ├── dates_json_format.json      # JSON file with date fields
│   ├── dates_psv_format.psv        # Pipe-separated values with dates
│   └── test_file*.csv              # Original test files
├── logs/                           # Test execution logs (created during tests)
├── test_date_formats.py            # Main pytest test suite
├── test_email_notifications.py     # Email notification tests
├── run_date_format_tests.py        # Manual test runner
├── test_config.yaml                # Test-specific configuration
├── requirements-test.txt           # Test dependencies
└── README.md                       # This file
```

## Test Categories

### 1. Date Format Tests (`test_date_formats.py`)

Comprehensive tests for date parsing and handling:

- **ISO Format Testing**: YYYY-MM-DD format validation
- **US Format Testing**: MM/DD/YYYY format validation
- **European Format Testing**: DD/MM/YYYY format validation
- **Mixed Separators**: Testing dash, dot, and slash separators
- **Edge Cases**: Leap years, end of month dates, boundary conditions
- **File Format Support**: CSV, JSON, PSV file testing
- **Invalid Date Handling**: Error handling for malformed dates
- **Data Integrity**: Ensuring consistent processing across formats

### 2. Integration Tests

- **SQL Server Integration**: Real database connection and processing
- **SQLite Testing**: In-memory database for unit tests
- **Statistics Tracking**: Job statistics and error logging validation
- **Error Handling**: Graceful handling of processing failures

### 3. End-to-End Tests

- **Script Integration**: Testing with `run-file-to-sql-loader.sh`
- **Batch Processing**: Directory-level processing tests
- **Configuration Validation**: Testing various configuration scenarios

## Running Tests

### Prerequisites

1. **Install Test Dependencies**:
   ```bash
   pip install -r test/requirements-test.txt
   ```

2. **Ensure Database Access**:
   - For SQL Server tests: Ensure container is running
   - For SQLite tests: No additional setup required

### Running Pytest Tests

```bash
# Run all pytest tests
cd nspc-etl
python -m pytest test/test_date_formats.py -v

# Run specific test
python -m pytest test/test_date_formats.py::TestDateFormats::test_iso_date_format -v

# Run with coverage
python -m pytest test/test_date_formats.py --cov=src --cov-report=html
```

### Using the Manual Test Runner

The `run_date_format_tests.py` script provides flexible testing options:

```bash
cd test

# Run all tests
python run_date_format_tests.py --all

# Run only pytest tests
python run_date_format_tests.py --pytest

# Test SQL Server integration
python run_date_format_tests.py --sqlserver

# Test with the run script
python run_date_format_tests.py --script

# Validate date parsing logic
python run_date_format_tests.py --validate

# Verbose output
python run_date_format_tests.py --all --verbose
```

### Integration Testing with SQL Server

```bash
# Ensure SQL Server is running
podman ps

# If not running, start it
bash setup-sqlserver-container.sh

# Run SQL Server integration tests
cd test
python run_date_format_tests.py --sqlserver
```

## Test Data Files

### Date Format Test Files

Each test file contains 10-20 records with various date scenarios:

1. **dates_iso_format.csv**: Standard ISO 8601 dates
2. **dates_us_format.csv**: US format dates with time components
3. **dates_european_format.csv**: European format dates
4. **dates_mixed_separators.csv**: Multiple separator styles
5. **dates_edge_cases.csv**: Leap years, month boundaries, historical dates
6. **dates_invalid_formats.csv**: Invalid dates for error handling
7. **dates_json_format.json**: JSON structure with date fields
8. **dates_psv_format.psv**: Pipe-separated format

### Data Scenarios Covered

- **Valid Date Formats**: ISO, US, European styles
- **Time Components**: Date-only and datetime combinations  
- **Separators**: Dash (-), slash (/), and dot (.) separators
- **Edge Cases**: 
  - Leap year dates (2024-02-29)
  - End of month dates (31st, 30th)
  - Historical dates (1900s, 2000s)
  - Future dates (2050s)
- **Invalid Cases**:
  - Invalid months (13th month)
  - Invalid days (February 30th)
  - Non-leap year February 29th
  - Malformed strings
  - Empty fields

## Configuration

### Test Configuration (`test_config.yaml`)

Optimized for testing with:
- SQLite database for speed
- Reduced retry attempts
- Debug-level logging
- Disabled email notifications

### Database Testing

- **SQLite**: Used for unit tests (fast, isolated)
- **SQL Server**: Used for integration tests (realistic environment)

## Expected Outcomes

### Successful Test Run Should Show:

1. **Date Parsing**: All valid date formats correctly identified and parsed
2. **Table Creation**: Proper DDL generation with DATETIME2 columns
3. **Data Insertion**: Successful loading with accurate row counts
4. **Error Handling**: Graceful handling of invalid dates without crashes
5. **Statistics**: Proper job statistics and error logging
6. **File Types**: Support for CSV, JSON, and PSV formats

### Key Metrics Tracked:

- **Processing Success Rate**: Percentage of files processed successfully
- **Date Parsing Accuracy**: Correct interpretation of date formats
- **Error Recovery**: Handling of invalid data without process failure
- **Performance**: Processing time for various file sizes
- **Data Integrity**: Consistency of data before and after processing

## Troubleshooting

### Common Issues:

1. **Database Connection Failures**:
   - Ensure SQL Server container is running
   - Check connection parameters in config
   - Verify network connectivity

2. **Import Errors**:
   - Ensure `src/` directory is in Python path
   - Check that all dependencies are installed

3. **Test Data Not Found**:
   - Verify test data files exist in `test/data/`
   - Check file permissions

4. **Date Parsing Failures**:
   - Review date format patterns in source code
   - Check for locale-specific date handling

### Debug Mode:

Enable debug logging in test configuration:
```yaml
logging:
  level: 'DEBUG'
```

## Contributing

When adding new tests:

1. **Create Test Data**: Add relevant CSV/JSON/PSV files in `test/data/`
2. **Add Test Cases**: Extend `TestDateFormats` class with new scenarios  
3. **Update Documentation**: Document new test scenarios in this README
4. **Validate**: Ensure tests pass with both SQLite and SQL Server

## Performance Benchmarks

Expected performance for test suite:
- **Unit Tests (SQLite)**: < 30 seconds
- **Integration Tests (SQL Server)**: < 2 minutes  
- **Full Test Suite**: < 5 minutes

## Security Considerations

Test data contains only synthetic information:
- No real personal data
- No production connection strings
- Isolated test database configurations
