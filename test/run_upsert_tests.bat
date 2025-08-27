@echo off
REM Batch script to run UPSERT tests on Windows
REM Prerequisites: Python 3.8+, SQL Server running in Podman, required packages installed

setlocal enabledelayedexpansion

REM Set script directory
set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

echo ========================================
echo NSPC ETL UPSERT Test Suite
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8 or later
    pause
    exit /b 1
)

REM Check if required packages are installed
echo Checking Python dependencies...
python -c "import pytest, pandas, pyodbc, yaml" >nul 2>&1
if errorlevel 1 (
    echo WARNING: Some required packages may be missing
    echo Installing requirements...
    pip install -r "%SCRIPT_DIR%requirements-upsert-test.txt"
    if errorlevel 1 (
        echo ERROR: Failed to install requirements
        pause
        exit /b 1
    )
)

REM Check if SQL Server is running (basic check)
echo Checking SQL Server connectivity...
python -c "import pyodbc; pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;UID=sa;PWD=YourStrong@Passw0rd;TrustServerCertificate=yes;', timeout=5)" >nul 2>&1
if errorlevel 1 (
    echo WARNING: Cannot connect to SQL Server
    echo Please ensure SQL Server is running in Podman:
    echo   podman run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" ^
    echo     -p 1433:1433 --name sqlserver ^
    echo     -d mcr.microsoft.com/mssql/server:2019-latest
    echo.
    echo Continue anyway? (Y/N)
    set /p CONTINUE=
    if /i not "!CONTINUE!"=="Y" (
        echo Test execution cancelled
        pause
        exit /b 1
    )
)

REM Create logs directory if it doesn't exist
if not exist "%SCRIPT_DIR%logs" (
    mkdir "%SCRIPT_DIR%logs"
)

REM Parse command line arguments
set VERBOSE=false
set SQL_SERVER_ONLY=false
set PERFORMANCE=false
set CLEANUP=false
set VALIDATE_ONLY=false
set CONFIG_FILE=

:parse_args
if "%1"=="" goto run_tests
if "%1"=="--verbose" set VERBOSE=true
if "%1"=="-v" set VERBOSE=true
if "%1"=="--sql-server-only" set SQL_SERVER_ONLY=true
if "%1"=="--performance" set PERFORMANCE=true
if "%1"=="--cleanup" set CLEANUP=true
if "%1"=="--validate-only" set VALIDATE_ONLY=true
if "%1"=="--config" (
    shift
    set CONFIG_FILE=%1
)
if "%1"=="--help" goto show_help
if "%1"=="-h" goto show_help
shift
goto parse_args

:run_tests
REM Build Python command arguments
set PYTHON_ARGS=

if "%VERBOSE%"=="true" set PYTHON_ARGS=!PYTHON_ARGS! --verbose
if "%SQL_SERVER_ONLY%"=="true" set PYTHON_ARGS=!PYTHON_ARGS! --sql-server-only
if "%PERFORMANCE%"=="true" set PYTHON_ARGS=!PYTHON_ARGS! --performance
if "%CLEANUP%"=="true" set PYTHON_ARGS=!PYTHON_ARGS! --cleanup
if "%VALIDATE_ONLY%"=="true" set PYTHON_ARGS=!PYTHON_ARGS! --validate-only
if not "%CONFIG_FILE%"=="" set PYTHON_ARGS=!PYTHON_ARGS! --config "%CONFIG_FILE%"

echo Running UPSERT tests with arguments: !PYTHON_ARGS!
echo.

REM Change to script directory and run tests
cd /d "%SCRIPT_DIR%"
python run_upsert_tests.py !PYTHON_ARGS!

set TEST_EXIT_CODE=%errorlevel%

echo.
if %TEST_EXIT_CODE%==0 (
    echo ========================================
    echo UPSERT TESTS COMPLETED SUCCESSFULLY
    echo ========================================
) else (
    echo ========================================
    echo UPSERT TESTS FAILED (Exit Code: %TEST_EXIT_CODE%^)
    echo ========================================
)

echo.
echo Test logs are available in: %SCRIPT_DIR%logs\
echo.

pause
exit /b %TEST_EXIT_CODE%

:show_help
echo.
echo Usage: run_upsert_tests.bat [options]
echo.
echo Options:
echo   --verbose, -v       Enable verbose output
echo   --sql-server-only   Run only SQL Server tests (skip SQLite fallback)
echo   --performance       Include performance benchmarking tests
echo   --cleanup           Clean up test tables after running
echo   --validate-only     Only validate test environment, do not run tests
echo   --config CONFIG     Use specific configuration file
echo   --help, -h          Show this help message
echo.
echo Examples:
echo   run_upsert_tests.bat
echo   run_upsert_tests.bat --verbose --cleanup
echo   run_upsert_tests.bat --sql-server-only --performance
echo   run_upsert_tests.bat --config custom_config.yaml
echo.
pause
exit /b 0
