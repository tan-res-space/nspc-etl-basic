# SQL Server Container Setup Guide

This guide provides instructions for setting up a Microsoft SQL Server instance using Podman containerization, specifically configured for the NSPC ETL project.

## Quick Start

### 1. Automated Setup (Recommended)

Run the provided setup script:

```bash
./setup-sqlserver-container.sh
```

This script will:
- Create a persistent volume for database files
- Start the SQL Server container with proper configuration
- Create the `TestDB` database required by your ETL project
- Verify the setup and display connection information

### 2. Manual Setup

If you prefer to run the commands manually:

```bash
# Create persistent volume
podman volume create nspc-sqlserver-data

# Start SQL Server container
podman run -d \
    --name nspc-sqlserver \
    --hostname sqlserver \
    -e "ACCEPT_EULA=Y" \
    -e "SA_PASSWORD=YourStrong@Passw0rd" \
    -e "MSSQL_PID=Express" \
    -p 1433:1433 \
    -v nspc-sqlserver-data:/var/opt/mssql \
    --restart unless-stopped \
    mcr.microsoft.com/mssql/server:2022-latest

# Wait for SQL Server to start (about 30-60 seconds)
sleep 60

# Create TestDB database
podman exec nspc-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong@Passw0rd" \
    -Q "CREATE DATABASE [TestDB]"
```

## Configuration Details

### Container Configuration
- **Image**: `mcr.microsoft.com/mssql/server:2022-latest`
- **Container Name**: `nspc-sqlserver`
- **SA Password**: `YourStrong@Passw0rd` (matches your loader_config.yaml)
- **Port Mapping**: `1433:1433` (host:container)
- **Volume**: `nspc-sqlserver-data` mounted to `/var/opt/mssql`
- **Edition**: SQL Server Express (free)

### Environment Variables
- `ACCEPT_EULA=Y`: Accepts the End User License Agreement
- `SA_PASSWORD=YourStrong@Passw0rd`: Sets the system administrator password
- `MSSQL_PID=Express`: Uses SQL Server Express edition (free)

### Database Configuration
- **Server**: `localhost`
- **Port**: `1433`
- **Database**: `TestDB`
- **Username**: `sa`
- **Password**: `YourStrong@Passw0rd`
- **Driver**: `ODBC Driver 17 for SQL Server`

## Verification

### 1. Check Container Status
```bash
podman ps
```

You should see the `nspc-sqlserver` container running.

### 2. Test Database Connection
```bash
podman exec nspc-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong@Passw0rd" -d TestDB \
    -Q "SELECT @@VERSION"
```

### 3. Test Your ETL Project
Your project's `loader_config.yaml` is already configured correctly:

```yaml
database:
  driver: 'ODBC Driver 17 for SQL Server'
  server: 'localhost'
  database: 'TestDB'
  username: 'sa'
  password: 'YourStrong@Passw0rd'
```

Test the connection by running:
```bash
./run-file-to-sql-loader.sh --check-deps
```

## Container Management

### Start/Stop Container
```bash
# Stop the container
podman stop nspc-sqlserver

# Start the container
podman start nspc-sqlserver

# Restart the container
podman restart nspc-sqlserver
```

### View Logs
```bash
podman logs nspc-sqlserver
```

### Access SQL Server Shell
```bash
podman exec -it nspc-sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong@Passw0rd"
```

### Remove Container and Data
```bash
# Stop and remove container
podman stop nspc-sqlserver
podman rm nspc-sqlserver

# Remove persistent volume (WARNING: This deletes all data!)
podman volume rm nspc-sqlserver-data
```

## External Connections

### From Host Applications
Your ETL project and other applications on the host can connect using:
- **Host**: `localhost` or `127.0.0.1`
- **Port**: `1433`
- **Database**: `TestDB`
- **Username**: `sa`
- **Password**: `YourStrong@Passw0rd`

### From Other Containers
Applications in other containers can connect using:
- **Host**: `nspc-sqlserver` (container name)
- **Port**: `1433`

### Connection String Examples

**ODBC Connection String:**
```
DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=TestDB;UID=sa;PWD=YourStrong@Passw0rd;TrustServerCertificate=yes;
```

**Python pyodbc:**
```python
import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=TestDB;"
    "UID=sa;"
    "PWD=YourStrong@Passw0rd;"
    "TrustServerCertificate=yes;"
)
connection = pyodbc.connect(conn_str)
```

**SQLAlchemy:**
```python
from sqlalchemy import create_engine

engine = create_engine(
    "mssql+pyodbc://sa:YourStrong@Passw0rd@localhost:1433/TestDB"
    "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
)
```

## Troubleshooting

### Container Won't Start
1. Check if port 1433 is already in use:
   ```bash
   lsof -i :1433
   ```

2. Check container logs:
   ```bash
   podman logs nspc-sqlserver
   ```

### Connection Issues
1. Ensure the container is running:
   ```bash
   podman ps
   ```

2. Test network connectivity:
   ```bash
   telnet localhost 1433
   ```

3. Verify SQL Server is accepting connections:
   ```bash
   podman exec nspc-sqlserver /opt/mssql-tools/bin/sqlcmd \
       -S localhost -U sa -P "YourStrong@Passw0rd" -Q "SELECT 1"
   ```

### Performance Considerations
- The container uses SQL Server Express, which has limitations:
  - Maximum database size: 10 GB
  - Maximum memory usage: 1410 MB
  - Maximum compute capacity: 1 socket or 4 cores

For production workloads, consider upgrading to SQL Server Standard or Enterprise editions.

## Security Notes

1. **Password Security**: The default password `YourStrong@Passw0rd` is used to match your existing configuration. In production, use a more secure password.

2. **Network Security**: The container exposes port 1433 to the host. Consider using firewall rules to restrict access.

3. **SSL/TLS**: The connection string includes `TrustServerCertificate=yes` for development. In production, configure proper SSL certificates.

## Next Steps

1. Run the setup script: `./setup-sqlserver-container.sh`
2. Verify the setup with your ETL project: `./run-file-to-sql-loader.sh --check-deps`
3. Test with sample data: `./run-file-to-sql-loader.sh your-data-file.csv`

Your SQL Server instance is now ready for use with the NSPC ETL project!
