#!/bin/bash

# SQL Server Container Setup Script for NSPC ETL Project
# This script sets up a Microsoft SQL Server container using Podman
# Configured to match the project's loader_config.yaml requirements

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration variables (matching loader_config.yaml)
CONTAINER_NAME="nspc-sqlserver"
SA_PASSWORD="YourStrong@Passw0rd"
DATABASE_NAME="TestDB"
HOST_PORT="1433"
CONTAINER_PORT="1433"
VOLUME_NAME="nspc-sqlserver-data"

# Functions for colored output
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

# Function to check if container is running
check_container_status() {
    if podman ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        return 0  # Container is running
    else
        return 1  # Container is not running
    fi
}

# Function to check if container exists (running or stopped)
check_container_exists() {
    if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        return 0  # Container exists
    else
        return 1  # Container doesn't exist
    fi
}

# Function to create persistent volume
create_volume() {
    print_info "Creating persistent volume for SQL Server data..."
    
    if podman volume exists "$VOLUME_NAME" 2>/dev/null; then
        print_warning "Volume '$VOLUME_NAME' already exists. Using existing volume."
    else
        podman volume create "$VOLUME_NAME"
        print_success "Created volume '$VOLUME_NAME'"
    fi
}

# Function to start SQL Server container
start_sqlserver_container() {
    print_info "Starting Microsoft SQL Server container..."
    
    # Check if container already exists
    if check_container_exists; then
        print_warning "Container '$CONTAINER_NAME' already exists."
        
        if check_container_status; then
            print_success "Container is already running!"
            return 0
        else
            print_info "Starting existing container..."
            podman start "$CONTAINER_NAME"
            print_success "Container started successfully!"
            return 0
        fi
    fi
    
    # Create and start new container
    print_info "Creating new SQL Server container with the following configuration:"
    echo "  - Container Name: $CONTAINER_NAME"
    echo "  - SA Password: $SA_PASSWORD"
    echo "  - Host Port: $HOST_PORT"
    echo "  - Volume: $VOLUME_NAME"
    echo "  - Database: $DATABASE_NAME (will be created after startup)"
    
    podman run -d \
        --name "$CONTAINER_NAME" \
        --hostname sqlserver \
        -e "ACCEPT_EULA=Y" \
        -e "SA_PASSWORD=$SA_PASSWORD" \
        -e "MSSQL_PID=Express" \
        -p "$HOST_PORT:$CONTAINER_PORT" \
        -v "$VOLUME_NAME:/var/opt/mssql" \
        --restart unless-stopped \
        mcr.microsoft.com/mssql/server:2022-latest
    
    print_success "SQL Server container started successfully!"
}

# Function to wait for SQL Server to be ready
wait_for_sqlserver() {
    print_info "Waiting for SQL Server to be ready..."
    
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if podman exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P "$SA_PASSWORD" \
            -Q "SELECT 1" >/dev/null 2>&1; then
            print_success "SQL Server is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    print_error "SQL Server failed to start within expected time"
    return 1
}

# Function to create the TestDB database
create_testdb() {
    print_info "Creating '$DATABASE_NAME' database..."
    
    # Check if database already exists
    local db_exists=$(podman exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" \
        -Q "SELECT COUNT(*) FROM sys.databases WHERE name = '$DATABASE_NAME'" -h -1 2>/dev/null | tr -d ' \n\r')
    
    if [ "$db_exists" = "1" ]; then
        print_warning "Database '$DATABASE_NAME' already exists."
        return 0
    fi
    
    # Create the database
    podman exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" \
        -Q "CREATE DATABASE [$DATABASE_NAME]"
    
    if [ $? -eq 0 ]; then
        print_success "Database '$DATABASE_NAME' created successfully!"
    else
        print_error "Failed to create database '$DATABASE_NAME'"
        return 1
    fi
}

# Function to display connection information
show_connection_info() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}SQL Server Container Setup Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "${BLUE}Connection Details:${NC}"
    echo "  Server: localhost,$HOST_PORT"
    echo "  Database: $DATABASE_NAME"
    echo "  Username: sa"
    echo "  Password: $SA_PASSWORD"
    echo "  Driver: ODBC Driver 17 for SQL Server"
    echo ""
    echo -e "${BLUE}Container Management:${NC}"
    echo "  Container Name: $CONTAINER_NAME"
    echo "  Volume Name: $VOLUME_NAME"
    echo ""
    echo -e "${BLUE}Useful Commands:${NC}"
    echo "  Stop container:    podman stop $CONTAINER_NAME"
    echo "  Start container:   podman start $CONTAINER_NAME"
    echo "  Remove container:  podman rm $CONTAINER_NAME"
    echo "  Remove volume:     podman volume rm $VOLUME_NAME"
    echo "  View logs:         podman logs $CONTAINER_NAME"
    echo ""
    echo -e "${YELLOW}Your ETL project configuration in loader_config.yaml is already set up correctly!${NC}"
}

# Function to verify the setup
verify_setup() {
    print_info "Verifying SQL Server setup..."
    
    # Test connection
    if podman exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" -d "$DATABASE_NAME" \
        -Q "SELECT @@VERSION" >/dev/null 2>&1; then
        print_success "Connection to $DATABASE_NAME database verified!"
    else
        print_error "Failed to connect to $DATABASE_NAME database"
        return 1
    fi
    
    # Show SQL Server version
    local version=$(podman exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" \
        -Q "SELECT @@VERSION" -h -1 2>/dev/null | head -1)
    echo -e "${BLUE}SQL Server Version:${NC} $version"
}

# Main execution
main() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}SQL Server Container Setup for NSPC ETL Project${NC}"
    echo -e "${BLUE}================================================${NC}"
    echo ""
    
    # Create persistent volume
    create_volume
    
    # Start SQL Server container
    start_sqlserver_container
    
    # Wait for SQL Server to be ready
    wait_for_sqlserver
    
    # Create TestDB database
    create_testdb
    
    # Verify setup
    verify_setup
    
    # Show connection information
    show_connection_info
}

# Run main function
main "$@"
