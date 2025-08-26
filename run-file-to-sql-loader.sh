#!/bin/bash

# File to SQL Server Loader - Setup and Run Script
# This script handles the complete setup and execution of the file-to-sql loader
# Supports CSV, PSV, JSON files and directory batch processing
# Uses YAML configuration for database connection and processing options

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory and file paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/src/file-to-sql-loader.py"
DEFAULT_CONFIG="$SCRIPT_DIR/src/loader_config.yaml"

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}File to SQL Server Loader - Setup & Run Script${NC}"
echo -e "${BLUE}With Python Virtual Environment Management${NC}"
echo -e "${BLUE}================================================${NC}"

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo -e "\n${YELLOW}Usage:${NC}"
    echo "  $0 [OPTIONS] <input_path>"
    echo ""
    echo -e "${YELLOW}Arguments:${NC}"
    echo "  input_path              Path to file or directory to process"
    echo "                         Supports: CSV, PSV, JSON files"
    echo "                         For directories: processes all supported files"
    echo ""
    echo -e "${YELLOW}Options:${NC}"
    echo "  -h, --help              Show this help message"
    echo "  -c, --config FILE       Configuration file path (default: loader_config.yaml)"
    echo "  -f, --file              Force single file processing mode"
    echo "  -d, --directory         Force directory batch processing mode"
    echo "  -v, --verbose           Enable verbose output from this script"
    echo "  --setup-only            Only run setup, don't execute the loader"
    echo "  --check-deps            Check dependencies and configuration"
    echo ""
    echo -e "${YELLOW}Configuration:${NC}"
    echo "  Database connection and processing options are configured via YAML file."
    echo "  Default config: $DEFAULT_CONFIG"
    echo "  The config file contains database connection, logging, and processing settings."
    echo ""
    echo -e "${YELLOW}Virtual Environment:${NC}"
    echo "  This script automatically manages a Python virtual environment:"
    echo "  - Creates 'venv' directory on first run"
    echo "  - Activates existing virtual environment on subsequent runs"
    echo "  - Installs dependencies from requirements.txt if available"
    echo "  - Ensures isolated Python environment for the application"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  $0 data.csv                                    # Process single CSV file (auto-detected)"
    echo "  $0 data.psv                                    # Process single PSV file (auto-detected)"
    echo "  $0 data.json                                   # Process single JSON file (auto-detected)"
    echo "  $0 /path/to/data/directory/                    # Process all files in directory (auto-detected)"
    echo "  $0 data.csv --file                             # Force single file processing mode"
    echo "  $0 /path/to/directory/ --directory             # Force directory batch processing mode"
    echo "  $0 data.csv --config custom_config.yaml       # Use custom configuration"
    echo "  $0 --setup-only                               # Just setup environment"
    echo "  $0 --check-deps                               # Check dependencies only"
}

# Function to setup and activate Python virtual environment
setup_virtual_environment() {
    local venv_dir="$SCRIPT_DIR/venv"

    print_info "Setting up Python virtual environment..."

    # Detect OS for activation script path
    local activate_script
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
        activate_script="$venv_dir/Scripts/activate"
    else
        activate_script="$venv_dir/bin/activate"
    fi

    # Check if virtual environment already exists
    if [ -d "$venv_dir" ]; then
        print_info "Virtual environment already exists at: $venv_dir"

        # Verify the virtual environment is valid
        if [ -f "$activate_script" ]; then
            print_success "Using existing virtual environment"
        else
            print_warning "Virtual environment appears corrupted, recreating..."
            rm -rf "$venv_dir"
        fi
    fi

    # Create virtual environment if it doesn't exist or was corrupted
    if [ ! -d "$venv_dir" ]; then
        print_info "Creating new virtual environment..."

        # First check if python3 is available
        if command -v python3 >/dev/null 2>&1; then
            PYTHON_BASE_CMD="python3"
        elif command -v python >/dev/null 2>&1; then
            PYTHON_BASE_CMD="python"
        else
            print_error "Python is not installed"
            print_info "Please install Python 3.7 or higher"
            return 1
        fi

        # Create the virtual environment
        if ! $PYTHON_BASE_CMD -m venv "$venv_dir"; then
            print_error "Failed to create virtual environment"
            print_info "Please ensure Python venv module is available"
            return 1
        fi

        print_success "Virtual environment created successfully"
    fi

    # Activate the virtual environment
    print_info "Activating virtual environment..."

    if [ ! -f "$activate_script" ]; then
        print_error "Virtual environment activation script not found: $activate_script"
        return 1
    fi

    # Source the activation script
    if ! source "$activate_script"; then
        print_error "Failed to activate virtual environment"
        return 1
    fi

    print_success "Virtual environment activated"

    # Verify we're in the virtual environment
    if [[ "$VIRTUAL_ENV" == "$venv_dir" ]]; then
        print_success "Confirmed running in virtual environment: $VIRTUAL_ENV"
    else
        print_warning "Virtual environment activation may not have worked correctly"
    fi

    # Install/upgrade pip in the virtual environment
    print_info "Ensuring pip is up to date in virtual environment..."
    if ! python -m pip install --upgrade pip >/dev/null 2>&1; then
        print_warning "Failed to upgrade pip, continuing with existing version"
    fi

    # Install requirements if requirements.txt exists
    local requirements_file="$SCRIPT_DIR/requirements.txt"
    if [ -f "$requirements_file" ]; then
        print_info "Installing dependencies from requirements.txt..."
        if python -m pip install -r "$requirements_file"; then
            print_success "Dependencies installed successfully"
        else
            print_error "Failed to install dependencies from requirements.txt"
            return 1
        fi
    else
        print_info "No requirements.txt found, skipping dependency installation"
    fi

    return 0
}

# Function to check if Python is installed
check_python() {
    print_info "Checking Python installation in virtual environment..."

    # At this point we should be in the virtual environment, so use 'python'
    if command -v python >/dev/null 2>&1; then
        PYTHON_CMD="python"
        PYTHON_VERSION=$(python --version 2>&1)
        print_success "Found $PYTHON_VERSION in virtual environment"
    else
        print_error "Python not found in virtual environment"
        return 1
    fi
}

# Function to check required Python packages
check_python_packages() {
    print_info "Checking required Python packages in virtual environment..."

    local missing_packages=()

    # Check sqlalchemy (from requirements.txt)
    if ! $PYTHON_CMD -c "import sqlalchemy" >/dev/null 2>&1; then
        missing_packages+=("sqlalchemy")
    fi

    # Check pyodbc (from requirements.txt)
    if ! $PYTHON_CMD -c "import pyodbc" >/dev/null 2>&1; then
        missing_packages+=("pyodbc")
    fi

    # Check pymssql (from requirements.txt)
    if ! $PYTHON_CMD -c "import pymssql" >/dev/null 2>&1; then
        missing_packages+=("pymssql")
    fi

    # Check pyyaml (commonly needed for config files)
    if ! $PYTHON_CMD -c "import yaml" >/dev/null 2>&1; then
        missing_packages+=("pyyaml")
    fi

    # Check pandas (commonly needed for data processing)
    if ! $PYTHON_CMD -c "import pandas" >/dev/null 2>&1; then
        missing_packages+=("pandas")
    fi

    if [ ${#missing_packages[@]} -eq 0 ]; then
        print_success "All required Python packages are installed in virtual environment"
        return 0
    else
        print_error "Missing required Python packages: ${missing_packages[*]}"
        print_info "Install missing packages with:"
        print_info "  python -m pip install ${missing_packages[*]}"
        print_info "Or install from requirements file:"
        print_info "  python -m pip install -r $SCRIPT_DIR/requirements.txt"
        return 1
    fi
}

# Function to validate configuration file
validate_config() {
    local config_file="$1"

    print_info "Validating configuration file: $config_file"

    # Check if config file exists
    if [ ! -f "$config_file" ]; then
        print_error "Configuration file not found: $config_file"
        print_info "Please ensure the configuration file exists."
        print_info "Sample configuration available at: $DEFAULT_CONFIG"
        return 1
    fi

    # Check if config file is readable
    if [ ! -r "$config_file" ]; then
        print_error "Configuration file is not readable: $config_file"
        return 1
    fi

    # Basic YAML syntax validation using Python
    if ! $PYTHON_CMD -c "import yaml; yaml.safe_load(open('$config_file'))" >/dev/null 2>&1; then
        print_error "Configuration file has invalid YAML syntax: $config_file"
        print_info "Please check the YAML syntax and try again."
        return 1
    fi

    print_success "Configuration file is valid"
    return 0
}

# Function to validate input path
validate_input_path() {
    local input_path="$1"
    local force_file_mode="$2"
    local force_directory_mode="$3"

    print_info "Validating input path: $input_path"

    # Check if path exists
    if [ ! -e "$input_path" ]; then
        print_error "Input path does not exist: $input_path"
        return 1
    fi

    # Check if it's a file or directory and validate against force modes
    if [ -f "$input_path" ]; then
        # Check if directory mode is forced for a file
        if [ "$force_directory_mode" = "true" ]; then
            print_error "Directory mode forced (--directory) but input is a file: $input_path"
            return 1
        fi

        # Validate file extension for supported formats
        local extension="${input_path##*.}"
        local extension_lower=$(echo "$extension" | tr '[:upper:]' '[:lower:]')
        case "$extension_lower" in
            csv|psv|json|txt)
                if [ "$force_file_mode" = "true" ]; then
                    print_success "Valid input file: $input_path (.$extension) [FORCED FILE MODE]"
                else
                    print_success "Valid input file: $input_path (.$extension) [AUTO-DETECTED]"
                fi
                return 0
                ;;
            *)
                print_warning "File extension '.$extension' may not be supported"
                print_info "Supported extensions: .csv, .psv, .json, .txt"
                print_info "The script will attempt to auto-detect the file format"
                if [ "$force_file_mode" = "true" ]; then
                    print_info "Processing in FORCED FILE MODE"
                fi
                return 0
                ;;
        esac
    elif [ -d "$input_path" ]; then
        # Check if file mode is forced for a directory
        if [ "$force_file_mode" = "true" ]; then
            print_error "File mode forced (--file) but input is a directory: $input_path"
            return 1
        fi

        # Check if directory contains any files
        local file_count=$(find "$input_path" -maxdepth 1 -type f | wc -l)
        if [ "$file_count" -eq 0 ]; then
            print_warning "Directory contains no files: $input_path"
            return 1
        else
            if [ "$force_directory_mode" = "true" ]; then
                print_success "Valid input directory: $input_path ($file_count files) [FORCED DIRECTORY MODE]"
            else
                print_success "Valid input directory: $input_path ($file_count files) [AUTO-DETECTED]"
            fi
            return 0
        fi
    else
        print_error "Input path is neither a file nor a directory: $input_path"
        return 1
    fi
}

# Function to run the file loader
run_file_loader() {
    local input_path="$1"
    local config_file="$2"
    local verbose="$3"
    local force_file_mode="$4"
    local force_directory_mode="$5"

    print_info "Running File to SQL Server Loader..."

    # Build command arguments
    local cmd_args=()
    cmd_args+=("$input_path")

    # Always add config file (use full path)
    cmd_args+=("--config" "$config_file")

    # Construct the full command
    local full_command="$PYTHON_CMD $PYTHON_SCRIPT ${cmd_args[*]}"

    # Show command being executed if verbose
    if [ "$verbose" = "true" ]; then
        print_info "Executing: $full_command"
    fi

    # Run the Python script
    print_info "Processing input: $input_path"
    if [ -f "$input_path" ]; then
        if [ "$force_file_mode" = "true" ]; then
            print_info "Mode: Single file processing [FORCED]"
        else
            print_info "Mode: Single file processing [AUTO-DETECTED]"
        fi
    else
        if [ "$force_directory_mode" = "true" ]; then
            print_info "Mode: Directory batch processing [FORCED]"
        else
            print_info "Mode: Directory batch processing [AUTO-DETECTED]"
        fi
    fi

    # Execute the command
    $PYTHON_CMD "$PYTHON_SCRIPT" "${cmd_args[@]}"
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        print_success "File loader execution completed successfully!"
    else
        print_error "File loader execution failed with exit code: $exit_code"
        return $exit_code
    fi
}

# Function to check dependencies and configuration
check_dependencies() {
    print_info "Checking all dependencies and configuration..."

    local all_good=true

    # Setup virtual environment first
    if ! setup_virtual_environment; then
        print_error "Failed to setup virtual environment"
        all_good=false
    fi

    # Check Python (in virtual environment)
    if ! check_python; then
        all_good=false
    fi

    # Check Python packages (in virtual environment)
    if ! check_python_packages; then
        all_good=false
    fi

    # Check Python script exists
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        print_error "Python script not found: $PYTHON_SCRIPT"
        all_good=false
    else
        print_success "Python script found: $PYTHON_SCRIPT"
    fi

    # Check default configuration
    if [ -f "$DEFAULT_CONFIG" ]; then
        if validate_config "$DEFAULT_CONFIG"; then
            print_success "Default configuration is valid: $DEFAULT_CONFIG"
        else
            all_good=false
        fi
    else
        print_warning "Default configuration file not found: $DEFAULT_CONFIG"
        print_info "You will need to specify a configuration file with --config option"
    fi

    if [ "$all_good" = "true" ]; then
        print_success "All dependencies and configuration checks passed!"
        return 0
    else
        print_error "Some dependency or configuration checks failed"
        return 1
    fi
}

# Main execution
main() {
    local input_path=""
    local config_file="$DEFAULT_CONFIG"
    local verbose="false"
    local setup_only="false"
    local check_deps_only="false"
    local force_file_mode="false"
    local force_directory_mode="false"

    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -c|--config)
                config_file="$2"
                shift 2
                ;;
            -f|--file)
                force_file_mode="true"
                shift
                ;;
            -d|--directory)
                force_directory_mode="true"
                shift
                ;;
            -v|--verbose)
                verbose="true"
                shift
                ;;
            --setup-only)
                setup_only="true"
                shift
                ;;
            --check-deps)
                check_deps_only="true"
                shift
                ;;
            -*)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                if [ -z "$input_path" ]; then
                    input_path="$1"
                else
                    print_error "Multiple input path arguments provided"
                    show_usage
                    exit 1
                fi
                shift
                ;;
        esac
    done

    # Validate mutually exclusive options
    if [ "$force_file_mode" = "true" ] && [ "$force_directory_mode" = "true" ]; then
        print_error "Cannot specify both --file and --directory options"
        show_usage
        exit 1
    fi

    # Dependency check mode
    if [ "$check_deps_only" = "true" ]; then
        check_dependencies
        exit $?
    fi

    # Setup phase - always check basic requirements
    print_info "Starting setup and validation..."

    # Setup and activate virtual environment first
    if ! setup_virtual_environment; then
        print_error "Failed to setup virtual environment"
        exit 1
    fi

    # Check Python installation (now in virtual environment)
    if ! check_python; then
        exit 1
    fi

    # Check Python packages (now in virtual environment)
    if ! check_python_packages; then
        print_error "Required Python packages are missing. Please install them first."
        exit 1
    fi

    # Check if Python script exists
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        print_error "Python script not found: $PYTHON_SCRIPT"
        print_info "Please ensure the file-to-sql-loader.py script is in the src/ directory"
        exit 1
    fi

    print_success "Basic setup validation completed successfully!"

    # Setup-only mode
    if [ "$setup_only" = "true" ]; then
        print_info "Setup-only mode. Performing full dependency check..."
        check_dependencies
        print_info "Setup completed. To run the loader, execute:"
        print_info "  $0 <input_path> [--config config_file]"
        exit 0
    fi

    # Validate input path is provided
    if [ -z "$input_path" ]; then
        print_error "Input path is required"
        print_info "Specify a file or directory to process"
        show_usage
        exit 1
    fi

    # Validate input path
    if ! validate_input_path "$input_path" "$force_file_mode" "$force_directory_mode"; then
        exit 1
    fi

    # Validate configuration file
    if ! validate_config "$config_file"; then
        exit 1
    fi

    print_success "All validations passed. Starting file processing..."

    # Run the loader
    if ! run_file_loader "$input_path" "$config_file" "$verbose" "$force_file_mode" "$force_directory_mode"; then
        print_error "File loader execution failed!"
        exit 1
    fi

    print_success "File to SQL Server loader execution completed successfully!"
}

# Run main function with all arguments
main "$@"
