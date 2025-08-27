@echo off
setlocal enabledelayedexpansion

REM File to SQL Server Loader - Setup and Run Script (Windows)
REM This script handles the complete setup and execution of the file-to-sql loader
REM Supports CSV, PSV, JSON files and directory batch processing
REM Uses YAML configuration for database connection and processing options
REM With Python Virtual Environment Management

REM Color codes for output (Windows)
set "RED=[91m"
set "GREEN=[92m"
set "YELLOW=[93m"
set "BLUE=[94m"
set "NC=[0m"

REM Script directory and file paths
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "PYTHON_SCRIPT=%SCRIPT_DIR%\src\file-to-sql-loader.py"
set "DEFAULT_CONFIG=%SCRIPT_DIR%\src\loader_config.yaml"
set "VENV_DIR=%SCRIPT_DIR%\venv"
set "REQUIREMENTS_FILE=%SCRIPT_DIR%\requirements.txt"

echo %BLUE%================================================%NC%
echo %BLUE%File to SQL Server Loader - Setup ^& Run Script%NC%
echo %BLUE%With Python Virtual Environment Management%NC%
echo %BLUE%================================================%NC%

REM Initialize variables
set "INPUT_PATH="
set "CONFIG_FILE=%DEFAULT_CONFIG%"
set "VERBOSE=false"
set "SETUP_ONLY=false"
set "CHECK_DEPS_ONLY=false"
set "FORCE_FILE_MODE=false"
set "FORCE_DIRECTORY_MODE=false"
set "PYTHON_CMD="

REM Parse command line arguments
:parse_args
if "%~1"=="" goto end_parse_args
if /i "%~1"=="-h" goto show_usage
if /i "%~1"=="--help" goto show_usage
if /i "%~1"=="-c" (
    set "CONFIG_FILE=%~2"
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--config" (
    set "CONFIG_FILE=%~2"
    shift
    shift
    goto parse_args
)
if /i "%~1"=="-f" (
    set "FORCE_FILE_MODE=true"
    shift
    goto parse_args
)
if /i "%~1"=="--file" (
    set "FORCE_FILE_MODE=true"
    shift
    goto parse_args
)
if /i "%~1"=="-d" (
    set "FORCE_DIRECTORY_MODE=true"
    shift
    goto parse_args
)
if /i "%~1"=="--directory" (
    set "FORCE_DIRECTORY_MODE=true"
    shift
    goto parse_args
)
if /i "%~1"=="-v" (
    set "VERBOSE=true"
    shift
    goto parse_args
)
if /i "%~1"=="--verbose" (
    set "VERBOSE=true"
    shift
    goto parse_args
)
if /i "%~1"=="--setup-only" (
    set "SETUP_ONLY=true"
    shift
    goto parse_args
)
if /i "%~1"=="--check-deps" (
    set "CHECK_DEPS_ONLY=true"
    shift
    goto parse_args
)
if "%~1" neq "" (
    if "!INPUT_PATH!"=="" (
        set "INPUT_PATH=%~1"
    ) else (
        call :print_error "Multiple input path arguments provided"
        goto show_usage
    )
)
shift
goto parse_args

:end_parse_args

REM Validate mutually exclusive options
if "%FORCE_FILE_MODE%"=="true" if "%FORCE_DIRECTORY_MODE%"=="true" (
    call :print_error "Cannot specify both --file and --directory options"
    goto show_usage
)

REM Dependency check mode
if "%CHECK_DEPS_ONLY%"=="true" (
    call :check_dependencies
    exit /b !errorlevel!
)

REM Setup phase - always check basic requirements
call :print_info "Starting setup and validation..."

REM Setup and activate virtual environment first
call :setup_virtual_environment
if !errorlevel! neq 0 (
    call :print_error "Failed to setup virtual environment"
    exit /b 1
)

REM Check Python installation (now in virtual environment)
call :check_python
if !errorlevel! neq 0 exit /b 1

REM Check Python packages (now in virtual environment)
call :check_python_packages
if !errorlevel! neq 0 (
    call :print_error "Required Python packages are missing. Please install them first."
    exit /b 1
)

REM Check if Python script exists
if not exist "%PYTHON_SCRIPT%" (
    call :print_error "Python script not found: %PYTHON_SCRIPT%"
    call :print_info "Please ensure the file-to-sql-loader.py script is in the src\ directory"
    exit /b 1
)

call :print_success "Basic setup validation completed successfully!"

REM Setup-only mode
if "%SETUP_ONLY%"=="true" (
    call :print_info "Setup-only mode. Performing full dependency check..."
    call :check_dependencies
    call :print_info "Setup completed. To run the loader, execute:"
    call :print_info "  %~nx0 <input_path> [--config config_file]"
    exit /b 0
)

REM Validate input path is provided
if "!INPUT_PATH!"=="" (
    call :print_error "Input path is required"
    call :print_info "Specify a file or directory to process"
    goto show_usage
)

REM Validate input path
call :validate_input_path "!INPUT_PATH!" "%FORCE_FILE_MODE%" "%FORCE_DIRECTORY_MODE%"
if !errorlevel! neq 0 exit /b 1

REM Validate configuration file
call :validate_config "%CONFIG_FILE%"
if !errorlevel! neq 0 exit /b 1

call :print_success "All validations passed. Starting file processing..."

REM Run the loader
call :run_file_loader "!INPUT_PATH!" "%CONFIG_FILE%" "%VERBOSE%" "%FORCE_FILE_MODE%" "%FORCE_DIRECTORY_MODE%"
if !errorlevel! neq 0 (
    call :print_error "File loader execution failed!"
    exit /b 1
)

call :print_success "File to SQL Server loader execution completed successfully!"
exit /b 0

REM ============================================================================
REM FUNCTIONS
REM ============================================================================

:show_usage
echo.
echo %YELLOW%Usage:%NC%
echo   %~nx0 [OPTIONS] ^<input_path^>
echo.
echo %YELLOW%Arguments:%NC%
echo   input_path              Path to file or directory to process
echo                          Supports: CSV, PSV, JSON files
echo                          For directories: processes all supported files
echo.
echo %YELLOW%Options:%NC%
echo   -h, --help              Show this help message
echo   -c, --config FILE       Configuration file path (default: loader_config.yaml)
echo   -f, --file              Force single file processing mode
echo   -d, --directory         Force directory batch processing mode
echo   -v, --verbose           Enable verbose output from this script
echo   --setup-only            Only run setup, don't execute the loader
echo   --check-deps            Check dependencies and configuration
echo.
echo %YELLOW%Configuration:%NC%
echo   Database connection and processing options are configured via YAML file.
echo   Default config: %DEFAULT_CONFIG%
echo   The config file contains database connection, logging, and processing settings.
echo.
echo %YELLOW%Virtual Environment:%NC%
echo   This script automatically manages a Python virtual environment:
echo   - Creates 'venv' directory on first run
echo   - Activates existing virtual environment on subsequent runs
echo   - Installs dependencies from requirements.txt if available
echo   - Ensures isolated Python environment for the application
echo.
echo %YELLOW%Examples:%NC%
echo   %~nx0 data.csv                                    # Process single CSV file (auto-detected)
echo   %~nx0 data.psv                                    # Process single PSV file (auto-detected)
echo   %~nx0 data.json                                   # Process single JSON file (auto-detected)
echo   %~nx0 C:\path\to\data\directory\                  # Process all files in directory (auto-detected)
echo   %~nx0 data.csv --file                             # Force single file processing mode
echo   %~nx0 C:\path\to\directory\ --directory           # Force directory batch processing mode
echo   %~nx0 data.csv --config custom_config.yaml       # Use custom configuration
echo   %~nx0 --setup-only                               # Just setup environment
echo   %~nx0 --check-deps                               # Check dependencies only
exit /b 0

:setup_virtual_environment
call :print_info "Setting up Python virtual environment..."

set "ACTIVATE_SCRIPT=%VENV_DIR%\Scripts\activate.bat"

REM Check if virtual environment already exists
if exist "%VENV_DIR%" (
    call :print_info "Virtual environment already exists at: %VENV_DIR%"
    
    REM Verify the virtual environment is valid
    if exist "%ACTIVATE_SCRIPT%" (
        call :print_success "Using existing virtual environment"
    ) else (
        call :print_warning "Virtual environment appears corrupted, recreating..."
        rmdir /s /q "%VENV_DIR%" 2>nul
    )
)

REM Create virtual environment if it doesn't exist or was corrupted
if not exist "%VENV_DIR%" (
    call :print_info "Creating new virtual environment..."
    
    REM First check if python is available
    python --version >nul 2>&1
    if !errorlevel! equ 0 (
        set "PYTHON_BASE_CMD=python"
    ) else (
        py --version >nul 2>&1
        if !errorlevel! equ 0 (
            set "PYTHON_BASE_CMD=py"
        ) else (
            call :print_error "Python is not installed or not in PATH"
            call :print_info "Please install Python 3.7 or higher"
            exit /b 1
        )
    )
    
    REM Create the virtual environment
    !PYTHON_BASE_CMD! -m venv "%VENV_DIR%"
    if !errorlevel! neq 0 (
        call :print_error "Failed to create virtual environment"
        call :print_info "Please ensure Python venv module is available"
        exit /b 1
    )
    
    call :print_success "Virtual environment created successfully"
)

REM Activate the virtual environment
call :print_info "Activating virtual environment..."

if not exist "%ACTIVATE_SCRIPT%" (
    call :print_error "Virtual environment activation script not found: %ACTIVATE_SCRIPT%"
    exit /b 1
)

REM Activate the virtual environment
call "%ACTIVATE_SCRIPT%"
if !errorlevel! neq 0 (
    call :print_error "Failed to activate virtual environment"
    exit /b 1
)

call :print_success "Virtual environment activated"

REM Verify we're in the virtual environment
if defined VIRTUAL_ENV (
    call :print_success "Confirmed running in virtual environment: %VIRTUAL_ENV%"
) else (
    call :print_warning "Virtual environment activation may not have worked correctly"
)

REM Install/upgrade pip in the virtual environment
call :print_info "Ensuring pip is up to date in virtual environment..."
python -m pip install --upgrade pip >nul 2>&1
if !errorlevel! neq 0 (
    call :print_warning "Failed to upgrade pip, continuing with existing version"
)

REM Install requirements if requirements.txt exists
if exist "%REQUIREMENTS_FILE%" (
    call :print_info "Installing dependencies from requirements.txt..."
    python -m pip install -r "%REQUIREMENTS_FILE%"
    if !errorlevel! neq 0 (
        call :print_error "Failed to install dependencies from requirements.txt"
        exit /b 1
    )
    call :print_success "Dependencies installed successfully"
) else (
    call :print_info "No requirements.txt found, skipping dependency installation"
)

exit /b 0

:check_python
call :print_info "Checking Python installation in virtual environment..."

REM At this point we should be in the virtual environment, so use 'python'
python --version >nul 2>&1
if !errorlevel! equ 0 (
    set "PYTHON_CMD=python"
    for /f "tokens=*" %%i in ('python --version 2^>^&1') do set "PYTHON_VERSION=%%i"
    call :print_success "Found !PYTHON_VERSION! in virtual environment"
) else (
    call :print_error "Python not found in virtual environment"
    exit /b 1
)
exit /b 0

:check_python_packages
call :print_info "Checking required Python packages in virtual environment..."

set "MISSING_PACKAGES="

REM Check sqlalchemy (from requirements.txt)
python -c "import sqlalchemy" >nul 2>&1
if !errorlevel! neq 0 set "MISSING_PACKAGES=!MISSING_PACKAGES! sqlalchemy"

REM Check pyodbc (from requirements.txt)
python -c "import pyodbc" >nul 2>&1
if !errorlevel! neq 0 set "MISSING_PACKAGES=!MISSING_PACKAGES! pyodbc"

REM Check pymssql (from requirements.txt)
python -c "import pymssql" >nul 2>&1
if !errorlevel! neq 0 set "MISSING_PACKAGES=!MISSING_PACKAGES! pymssql"

REM Check pyyaml (commonly needed for config files)
python -c "import yaml" >nul 2>&1
if !errorlevel! neq 0 set "MISSING_PACKAGES=!MISSING_PACKAGES! pyyaml"

REM Check pandas (commonly needed for data processing)
python -c "import pandas" >nul 2>&1
if !errorlevel! neq 0 set "MISSING_PACKAGES=!MISSING_PACKAGES! pandas"

if "!MISSING_PACKAGES!"=="" (
    call :print_success "All required Python packages are installed in virtual environment"
    exit /b 0
) else (
    call :print_error "Missing required Python packages:!MISSING_PACKAGES!"
    call :print_info "Install missing packages with:"
    call :print_info "  python -m pip install!MISSING_PACKAGES!"
    call :print_info "Or install from requirements file:"
    call :print_info "  python -m pip install -r %REQUIREMENTS_FILE%"
    exit /b 1
)

:validate_config
set "config_file=%~1"

call :print_info "Validating configuration file: %config_file%"

REM Check if config file exists
if not exist "%config_file%" (
    call :print_error "Configuration file not found: %config_file%"
    call :print_info "Please ensure the configuration file exists."
    call :print_info "Sample configuration available at: %DEFAULT_CONFIG%"
    exit /b 1
)

REM Check if config file is readable
if not exist "%config_file%" (
    call :print_error "Configuration file is not readable: %config_file%"
    exit /b 1
)

REM Basic YAML syntax validation using Python
python -c "import yaml; yaml.safe_load(open('%config_file%'))" >nul 2>&1
if !errorlevel! neq 0 (
    call :print_error "Configuration file has invalid YAML syntax: %config_file%"
    call :print_info "Please check the YAML syntax and try again."
    exit /b 1
)

call :print_success "Configuration file is valid"
exit /b 0

:validate_input_path
set "input_path=%~1"
set "force_file_mode=%~2"
set "force_directory_mode=%~3"

call :print_info "Validating input path: %input_path%"

REM Check if path exists
if not exist "%input_path%" (
    call :print_error "Input path does not exist: %input_path%"
    exit /b 1
)

REM Check if it's a file or directory and validate against force modes
if exist "%input_path%\*" (
    REM It's a directory
    if "%force_file_mode%"=="true" (
        call :print_error "File mode forced (--file) but input is a directory: %input_path%"
        exit /b 1
    )

    REM Count files in directory
    set "file_count=0"
    for %%f in ("%input_path%\*") do set /a file_count+=1

    if !file_count! equ 0 (
        call :print_warning "Directory contains no files: %input_path%"
        exit /b 1
    ) else (
        if "%force_directory_mode%"=="true" (
            call :print_success "Valid input directory: %input_path% (!file_count! files) [FORCED DIRECTORY MODE]"
        ) else (
            call :print_success "Valid input directory: %input_path% (!file_count! files) [AUTO-DETECTED]"
        )
        exit /b 0
    )
) else (
    REM It's a file
    if "%force_directory_mode%"=="true" (
        call :print_error "Directory mode forced (--directory) but input is a file: %input_path%"
        exit /b 1
    )

    REM Validate file extension for supported formats
    for %%f in ("%input_path%") do set "extension=%%~xf"
    set "extension=!extension:~1!"

    REM Convert to lowercase for comparison (Windows batch doesn't have a simple tr equivalent)
    REM Using case-insensitive comparison instead

    REM Check supported extensions (case-insensitive)
    if /i "!extension!"=="csv" goto valid_extension
    if /i "!extension!"=="psv" goto valid_extension
    if /i "!extension!"=="json" goto valid_extension
    if /i "!extension!"=="txt" goto valid_extension

    call :print_warning "File extension '.!extension!' may not be supported"
    call :print_info "Supported extensions: .csv, .psv, .json, .txt"
    call :print_info "The script will attempt to auto-detect the file format"
    if "%force_file_mode%"=="true" (
        call :print_info "Processing in FORCED FILE MODE"
    )
    exit /b 0

    :valid_extension
    if "%force_file_mode%"=="true" (
        call :print_success "Valid input file: %input_path% (.!extension!) [FORCED FILE MODE]"
    ) else (
        call :print_success "Valid input file: %input_path% (.!extension!) [AUTO-DETECTED]"
    )
    exit /b 0
)

:run_file_loader
set "input_path=%~1"
set "config_file=%~2"
set "verbose=%~3"
set "force_file_mode=%~4"
set "force_directory_mode=%~5"

call :print_info "Running File to SQL Server Loader..."

REM Build command arguments
set "cmd_args=%input_path% --config %config_file%"

REM Construct the full command
set "full_command=%PYTHON_CMD% %PYTHON_SCRIPT% %cmd_args%"

REM Show command being executed if verbose
if "%verbose%"=="true" (
    call :print_info "Executing: !full_command!"
)

REM Run the Python script
call :print_info "Processing input: %input_path%"
if exist "%input_path%\*" (
    if "%force_directory_mode%"=="true" (
        call :print_info "Mode: Directory batch processing [FORCED]"
    ) else (
        call :print_info "Mode: Directory batch processing [AUTO-DETECTED]"
    )
) else (
    if "%force_file_mode%"=="true" (
        call :print_info "Mode: Single file processing [FORCED]"
    ) else (
        call :print_info "Mode: Single file processing [AUTO-DETECTED]"
    )
)

REM Execute the command
%PYTHON_CMD% "%PYTHON_SCRIPT%" %input_path% --config "%config_file%"
set "exit_code=!errorlevel!"

if !exit_code! equ 0 (
    call :print_success "File loader execution completed successfully!"
) else (
    call :print_error "File loader execution failed with exit code: !exit_code!"
    exit /b !exit_code!
)
exit /b 0

:check_dependencies
call :print_info "Checking all dependencies and configuration..."

set "all_good=true"

REM Setup virtual environment first
call :setup_virtual_environment
if !errorlevel! neq 0 (
    call :print_error "Failed to setup virtual environment"
    set "all_good=false"
)

REM Check Python (in virtual environment)
call :check_python
if !errorlevel! neq 0 set "all_good=false"

REM Check Python packages (in virtual environment)
call :check_python_packages
if !errorlevel! neq 0 set "all_good=false"

REM Check Python script exists
if not exist "%PYTHON_SCRIPT%" (
    call :print_error "Python script not found: %PYTHON_SCRIPT%"
    set "all_good=false"
) else (
    call :print_success "Python script found: %PYTHON_SCRIPT%"
)

REM Check default configuration
if exist "%DEFAULT_CONFIG%" (
    call :validate_config "%DEFAULT_CONFIG%"
    if !errorlevel! equ 0 (
        call :print_success "Default configuration is valid: %DEFAULT_CONFIG%"
    ) else (
        set "all_good=false"
    )
) else (
    call :print_warning "Default configuration file not found: %DEFAULT_CONFIG%"
    call :print_info "You will need to specify a configuration file with --config option"
)

if "%all_good%"=="true" (
    call :print_success "All dependencies and configuration checks passed!"
    exit /b 0
) else (
    call :print_error "Some dependency or configuration checks failed"
    exit /b 1
)

REM ============================================================================
REM UTILITY FUNCTIONS
REM ============================================================================

:print_info
echo %BLUE%[INFO]%NC% %~1
exit /b 0

:print_success
echo %GREEN%[SUCCESS]%NC% %~1
exit /b 0

:print_warning
echo %YELLOW%[WARNING]%NC% %~1
exit /b 0

:print_error
echo %RED%[ERROR]%NC% %~1
exit /b 0
