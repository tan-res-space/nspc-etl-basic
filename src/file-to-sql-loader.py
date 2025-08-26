#!/usr/bin/env python3
"""
File to SQL Loader
A dynamic script to process various file formats (CSV, PSV, JSON) and load them into SQL Server.
Automatically infers schema, generates DDL, and handles data loading with error handling.
"""

import argparse
import json
import logging
import os
import re
import smtplib
import sys
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple

import pandas as pd
import pyodbc
import yaml

logger = logging.getLogger(__name__)


def setup_logging(config: Dict[str, Any], job_run_id: uuid.UUID):
    """
    Configures logging based on the provided configuration.
    """
    if config.get('logging', {}).get('enabled', False):
        log_config = config['logging']
        log_path = Path(log_config.get('path', 'logs'))
        log_path.mkdir(exist_ok=True)
        log_file = log_path / f"load_job_{job_run_id}.log"

        # Get the root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_config.get('level', 'INFO').upper())

        # Remove existing handlers to avoid duplication
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Create formatter
        formatter = logging.Formatter(log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s'))

        # Add file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        # Add stream handler for console output
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

        logger.info("Logging configured. Log file: %s", log_file)
    else:
        # Basic config if logging is disabled in config
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger.info("Logging not configured in YAML, using basic config.")


def detect_file_type(file_path: str) -> str:
    """
    Detects the file type (csv, psv, or json) by inspecting the first few lines.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [f.readline() for _ in range(10)]

        # Check for JSON
        first_line = lines[0].strip()
        if first_line.startswith('[') or first_line.startswith('{'):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    json.load(f)
                return 'json'
            except json.JSONDecodeError:
                pass  # Not a valid JSON file

        # Check for CSV/PSV
        pipe_counts = [line.count('|') for line in lines if line.strip()]
        comma_counts = [line.count(',') for line in lines if line.strip()]

        if pipe_counts and all(c == pipe_counts[0] for c in pipe_counts) and pipe_counts[0] > 0:
            return 'psv'
        if comma_counts and all(c == comma_counts[0] for c in comma_counts) and comma_counts[0] > 0:
            return 'csv'

    except Exception as e:
        raise ValueError(f"Could not determine file type for {file_path}: {e}")

    raise ValueError(f"Could not determine file type for {file_path}")


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads configuration from a YAML file.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info("Successfully loaded configuration from %s", config_path)
        return config
    except FileNotFoundError:
        logger.error("Configuration file not found at: %s", config_path)
        raise
    except yaml.YAMLError as e:
        logger.error("Error parsing YAML configuration file: %s", e)
        raise


class BatchJobManager:
    """Manages batch job lifecycle and checkpointing."""

    def __init__(self, config: Dict[str, Any], directory_path: str, connection):
        self.config = config
        self.directory_path = directory_path
        self.connection = connection

    def get_or_create_batch_job(self) -> Tuple[str, bool]:
        """
        Get existing incomplete batch job or create new one.
        Returns: (batch_job_id, is_resumed)
        """
        batch_config = self.config.get('batch_processing', {})

        if not batch_config.get('enable_checkpointing', True):
            # Checkpointing disabled, always create new batch
            return self._create_new_batch_job(), False

        if not batch_config.get('resume_incomplete_batches', True):
            # Resume disabled, always create new batch
            return self._create_new_batch_job(), False

        cursor = self.connection.cursor()

        # Check for incomplete batch jobs for this directory
        max_age_hours = batch_config.get('max_resume_age_hours', 24)
        cursor.execute("""
            SELECT BatchJobID, FilesProcessed, FilesFailed, TotalFiles, BatchStartTime
            FROM EtlBatchJobStatistics
            WHERE DirectoryPath = ?
            AND BatchStatus IN ('InProgress', 'Failed')
            AND BatchStartTime > DATEADD(hour, -?, GETUTCDATE())
            ORDER BY BatchStartTime DESC
        """, self.directory_path, max_age_hours)

        existing_batch = cursor.fetchone()

        if existing_batch and self._should_resume_batch(existing_batch):
            batch_job_id = existing_batch[0]
            logger.info("Resuming existing batch job: %s", batch_job_id)

            # Update batch status to indicate resumption
            cursor.execute("""
                UPDATE EtlBatchJobStatistics
                SET BatchStatus = 'InProgress', IsResumed = 1
                WHERE BatchJobID = ?
            """, batch_job_id)
            self.connection.commit()

            return batch_job_id, True
        else:
            # Create new batch job
            return self._create_new_batch_job(), False

    def _should_resume_batch(self, batch_record) -> bool:
        """Determine if batch should be resumed based on business rules."""
        files_processed, files_failed, total_files = batch_record[1:4]

        # Resume if there are still files to process
        remaining_files = total_files - files_processed - files_failed
        return remaining_files > 0

    def _create_new_batch_job(self) -> str:
        """Create a new batch job record."""
        batch_job_id = str(uuid.uuid4())
        logger.info("Creating new batch job: %s", batch_job_id)
        return batch_job_id

    def get_pending_files(self, all_files: List[str], batch_job_id: str) -> List[str]:
        """Get files that haven't been processed yet."""
        cursor = self.connection.cursor()

        # Get successfully processed files for this batch
        cursor.execute("""
            SELECT DISTINCT SourceFile
            FROM EtlJobStatistics
            WHERE BatchJobID = ?
            AND JobStatus = 'Completed'
        """, batch_job_id)

        processed_files = {row[0] for row in cursor.fetchall()}

        # Filter out processed files
        pending_files = [f for f in all_files if f not in processed_files]

        logger.info("Found %d pending files out of %d total files",
                    len(pending_files), len(all_files))

        return pending_files

    def setup_enhanced_batch_tables(self):
        """Setup enhanced batch statistics tables with checkpointing support."""
        try:
            cursor = self.connection.cursor()

            # Check if IsResumed column exists in EtlBatchJobStatistics
            cursor.execute("""
                SELECT COUNT(*) FROM sys.columns
                WHERE Name = N'IsResumed'
                AND Object_ID = Object_ID(N'EtlBatchJobStatistics')
            """)

            if cursor.fetchone()[0] == 0:
                logger.info("Adding IsResumed column to EtlBatchJobStatistics table")
                cursor.execute("""
                    ALTER TABLE EtlBatchJobStatistics
                    ADD IsResumed BIT DEFAULT 0
                """)

            # Check if OriginalBatchJobID column exists
            cursor.execute("""
                SELECT COUNT(*) FROM sys.columns
                WHERE Name = N'OriginalBatchJobID'
                AND Object_ID = Object_ID(N'EtlBatchJobStatistics')
            """)

            if cursor.fetchone()[0] == 0:
                logger.info("Adding OriginalBatchJobID column to EtlBatchJobStatistics table")
                cursor.execute("""
                    ALTER TABLE EtlBatchJobStatistics
                    ADD OriginalBatchJobID UNIQUEIDENTIFIER NULL
                """)

            self.connection.commit()
            logger.info("Enhanced batch statistics tables setup completed")

        except pyodbc.Error as e:
            logger.error("Error setting up enhanced batch statistics tables: %s", e)
            raise


class EmailNotificationManager:
    """Manages email notifications for ETL job completion status."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.email_config = config.get('email_notifications', {})
        self.enabled = self.email_config.get('enabled', False)

    def send_job_completion_email(self, job_summary: Dict[str, Any]) -> bool:
        """Send email notification for job completion."""
        if not self.enabled:
            logger.info("Email notifications are disabled")
            return True

        try:
            # Prepare email content
            subject = self._generate_subject(job_summary)
            body = self._generate_email_body(job_summary)

            # Send email
            return self._send_email(subject, body)

        except Exception as e:
            logger.error("Failed to send email notification: %s", str(e))
            return False

    def _generate_subject(self, job_summary: Dict[str, Any]) -> str:
        """Generate email subject based on job status."""
        job_type = job_summary.get('job_type', 'ETL Job')
        status = job_summary.get('status', 'Unknown')

        if status == 'Completed':
            return f"✅ {job_type} Completed Successfully"
        elif status == 'CompletedWithErrors':
            return f"⚠️ {job_type} Completed with Errors"
        elif status == 'Failed':
            return f"❌ {job_type} Failed"
        else:
            return f"📊 {job_type} Status Update"

    def _generate_email_body(self, job_summary: Dict[str, Any]) -> str:
        """Generate HTML email body with job summary."""
        job_type = job_summary.get('job_type', 'ETL Job')
        status = job_summary.get('status', 'Unknown')
        start_time = job_summary.get('start_time', 'N/A')
        end_time = job_summary.get('end_time', 'N/A')
        duration = job_summary.get('duration_seconds', 0)

        # Format duration
        duration_str = f"{duration // 3600:02d}:{(duration % 3600) // 60:02d}:{duration % 60:02d}"

        # Status color
        status_color = {
            'Completed': '#28a745',
            'CompletedWithErrors': '#ffc107',
            'Failed': '#dc3545'
        }.get(status, '#6c757d')

        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .status {{ color: {status_color}; font-weight: bold; font-size: 18px; }}
                .summary-table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                .summary-table th, .summary-table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                .summary-table th {{ background-color: #f2f2f2; }}
                .error {{ color: #dc3545; }}
                .success {{ color: #28a745; }}
                .warning {{ color: #ffc107; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>ETL Job Status Report</h2>
                <p class="status">Status: {status}</p>
            </div>

            <h3>Job Summary</h3>
            <table class="summary-table">
                <tr><th>Property</th><th>Value</th></tr>
                <tr><td>Job Type</td><td>{job_type}</td></tr>
                <tr><td>Status</td><td><span class="status">{status}</span></td></tr>
                <tr><td>Start Time</td><td>{start_time}</td></tr>
                <tr><td>End Time</td><td>{end_time}</td></tr>
                <tr><td>Duration</td><td>{duration_str}</td></tr>
        """

        # Add batch-specific information
        if job_summary.get('is_batch', False):
            html_body += f"""
                <tr><td>Directory Path</td><td>{job_summary.get('directory_path', 'N/A')}</td></tr>
                <tr><td>Total Files</td><td>{job_summary.get('total_files', 0)}</td></tr>
                <tr><td>Files Processed</td><td><span class="success">{job_summary.get('files_processed', 0)}</span></td></tr>
                <tr><td>Files Failed</td><td><span class="error">{job_summary.get('files_failed', 0)}</span></td></tr>
            """
        else:
            # Single file information
            html_body += f"""
                <tr><td>Source File</td><td>{job_summary.get('source_file', 'N/A')}</td></tr>
                <tr><td>Target Table</td><td>{job_summary.get('target_table', 'N/A')}</td></tr>
                <tr><td>Rows Read</td><td>{job_summary.get('rows_read', 0)}</td></tr>
                <tr><td>Rows Processed</td><td><span class="success">{job_summary.get('rows_processed', 0)}</span></td></tr>
                <tr><td>Rows Failed</td><td><span class="error">{job_summary.get('rows_failed', 0)}</span></td></tr>
            """

        html_body += """
            </table>
        """

        # Add error information if present
        if job_summary.get('error_message'):
            html_body += f"""
            <h3>Error Details</h3>
            <div class="error">
                <p><strong>Error Message:</strong></p>
                <pre>{job_summary.get('error_message')}</pre>
            </div>
            """

        # Add batch job ID if present
        if job_summary.get('batch_job_id'):
            html_body += f"""
            <h3>Job Identifiers</h3>
            <p><strong>Batch Job ID:</strong> {job_summary.get('batch_job_id')}</p>
            """

        html_body += """
            <hr>
            <p><em>This is an automated notification from the NSPC ETL System.</em></p>
        </body>
        </html>
        """

        return html_body

    def _send_email(self, subject: str, body: str) -> bool:
        """Send email using SMTP."""
        try:
            # Email configuration
            smtp_server = self.email_config.get('smtp_server')
            smtp_port = self.email_config.get('smtp_port', 587)
            username = self.email_config.get('username')
            password = self.email_config.get('password')
            from_email = self.email_config.get('from_email', username)
            to_emails = self.email_config.get('to_emails', [])
            use_tls = self.email_config.get('use_tls', True)

            if not all([smtp_server, username, password, to_emails]):
                logger.error("Email configuration is incomplete")
                return False

            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = from_email
            msg['To'] = ', '.join(to_emails)

            # Add HTML body
            html_part = MIMEText(body, 'html')
            msg.attach(html_part)

            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if use_tls:
                    server.starttls()
                server.login(username, password)
                server.send_message(msg)

            logger.info("Email notification sent successfully to: %s", ', '.join(to_emails))
            return True

        except Exception as e:
            logger.error("Failed to send email: %s", str(e))
            return False


class FileToSQLLoader:
    """Main class for processing files and loading into SQL Server."""

    def __init__(self, config: Dict[str, Any], batch_job_id: Optional[str] = None):
        """Initialize with configuration parameters."""
        self.config = config
        self.batch_job_id = batch_job_id
        self.connection = None
        self.table_name = None
        self.columns_info = {}
        self.total_rows = 0
        self.processed_rows = 0
        self.error_rows = 0
        if self.config.get('job_statistics', {}).get('enabled', False):
            if self.connect_to_database():
                self.setup_statistics_table()
                self.setup_error_log_table()
        self.max_row_errors = self.config.get('loader', {}).get('max-row-errors', 100)

    def _ensure_subdirectories(self, source_path: str):
        """Ensure 'error' and 'processed' subdirectories exist."""
        source_dir = Path(source_path).parent
        (source_dir / 'error').mkdir(exist_ok=True)
        (source_dir / 'processed').mkdir(exist_ok=True)
        (source_dir / 'logs').mkdir(exist_ok=True)

    def setup_statistics_table(self):
        """Creates the job statistics table if it doesn't exist."""
        stats_config = self.config['job_statistics']
        table_name = stats_config['table_name']
        try:
            cursor = self.connection.cursor()
            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'dbo'
            """, table_name)
            if cursor.fetchone()[0] == 0:
                logger.info("Statistics table '%s' not found. Creating it.", table_name)
                ddl = f"""
                CREATE TABLE [{table_name}] (
                    JobRunID UNIQUEIDENTIFIER PRIMARY KEY,
                    JobStartTime DATETIME2,
                    JobEndTime DATETIME2,
                    JobDurationSeconds INT,
                    JobStatus NVARCHAR(50),
                    SourceFile NVARCHAR(255),
                    TargetTable NVARCHAR(255),
                    RowsRead INT,
                    RowsInserted INT,
                    RowsUpdated INT,
                    RowsFailed INT,
                    ErrorMessage NVARCHAR(MAX)
                );
                """
                cursor.execute(ddl)
                self.connection.commit()
                logger.info("Statistics table '%s' created successfully.", table_name)
            else:
                logger.info("Statistics table '%s' already exists.", table_name)
        except pyodbc.Error as e:
            logger.error("Error setting up statistics table '%s': %s", table_name, e)
            raise

    def setup_error_log_table(self):
        """Creates the ETL job error table if it doesn't exist."""
        error_log_config = self.config.get('error_logging', {})
        table_name = error_log_config.get('table_name', 'EtlJobError')
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'dbo'
            """, table_name)
            if cursor.fetchone()[0] == 0:
                logger.info("Error log table '%s' not found. Creating it.", table_name)
                ddl = f"""
                CREATE TABLE [{table_name}] (
                    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
                    JobRunID UNIQUEIDENTIFIER,
                    TableName NVARCHAR(255),
                    ColumnName NVARCHAR(255),
                    ErrorType NVARCHAR(100),
                    ErrorMessage NVARCHAR(MAX),
                    ErrorTimestamp DATETIME2 DEFAULT GETUTCDATE()
                );
                """
                cursor.execute(ddl)
                self.connection.commit()
                logger.info("Error log table '%s' created successfully.", table_name)
            else:
                logger.info("Error log table '%s' already exists.", table_name)
        except pyodbc.Error as e:
            logger.error("Error setting up error log table '%s': %s", table_name, e)
            raise

    def setup_batch_statistics_table(self):
        """Creates the batch job statistics table and alters the job statistics table."""
        try:
            cursor = self.connection.cursor()
            # Create EtlBatchJobStatistics table
            batch_stats_table = 'EtlBatchJobStatistics'
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{batch_stats_table}')
                BEGIN
                    CREATE TABLE {batch_stats_table} (
                        BatchJobID UNIQUEIDENTIFIER PRIMARY KEY,
                        DirectoryPath NVARCHAR(MAX),
                        TotalFiles INT,
                        FilesProcessed INT,
                        FilesFailed INT,
                        BatchStartTime DATETIME2,
                        BatchEndTime DATETIME2,
                        BatchStatus NVARCHAR(50)
                    );
                END
            """)
            logger.info("Ensured EtlBatchJobStatistics table exists.")

            # Alter EtlJobStatistics table
            job_stats_table = self.config['job_statistics']['table_name']
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'BatchJobID' AND Object_ID = Object_ID(N'{job_stats_table}'))
                BEGIN
                    ALTER TABLE {job_stats_table} ADD BatchJobID UNIQUEIDENTIFIER;
                END
            """)
            logger.info("Ensured BatchJobID column exists in EtlJobStatistics table.")
            self.connection.commit()
        except pyodbc.Error as e:
            logger.error("Error setting up batch statistics tables: %s", e)
            raise

    def write_error_log(self, job_run_id: str, table_name: str, column_name: str, error_type: str, error_message: str):
        """Writes a specific error to the error log table."""
        error_log_config = self.config.get('error_logging', {})
        if not error_log_config.get('enabled', False):
            return

        log_table_name = error_log_config.get('table_name', 'EtlJobError')
        try:
            cursor = self.connection.cursor()
            insert_sql = f"""
            INSERT INTO [{log_table_name}] (JobRunID, TableName, ColumnName, ErrorType, ErrorMessage)
            VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(insert_sql, job_run_id, table_name, column_name, error_type, error_message)
            self.connection.commit()
            logger.info("Successfully logged error for column '%s' to '%s'.", column_name, log_table_name)
        except pyodbc.Error as e:
            logger.error("Failed to write to error log: %s", e)

    def write_statistics(self, stats: Dict[str, Any]):
        """Writes job statistics to the database."""
        stats_config = self.config.get('job_statistics', {})
        if not stats_config.get('enabled', False):
            return

        table_name = stats_config.get('table_name')
        if not table_name:
            logger.warning("Job statistics table name not configured. Skipping write.")
            return

        try:
            cursor = self.connection.cursor()
            columns = [
                "JobRunID", "JobStartTime", "JobEndTime", "JobDurationSeconds",
                "JobStatus", "SourceFile", "TargetTable", "RowsRead",
                "RowsInserted", "RowsUpdated", "RowsFailed", "ErrorMessage"
            ]
            if self.batch_job_id:
                columns.append("BatchJobID")

            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO [{table_name}] ({', '.join(f'[{col}]' for col in columns)}) VALUES ({placeholders})"

            # Ensure all keys are present
            stats_to_write = {key: stats.get(key) for key in columns if key != "BatchJobID"}
            
            values_to_insert = list(stats_to_write.values())
            if self.batch_job_id:
                values_to_insert.append(self.batch_job_id)

            cursor.execute(insert_sql, tuple(values_to_insert))
            self.connection.commit()
            logger.info("Successfully wrote job statistics to '%s'.", table_name)
        except pyodbc.Error as e:
            logger.error("Failed to write job statistics: %s", e)

    def connect_to_database(self) -> bool:
        """Establish connection to SQL Server."""
        try:
            db_config = self.config['database']
            conn_str = (
                f"DRIVER={{{db_config['driver']}}};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};"
                f"PWD={db_config['password']};"
                f"TrustServerCertificate=yes;"
            )

            self.connection = pyodbc.connect(conn_str)
            logger.info("Successfully connected to SQL Server database: %s",
                       db_config['database'])
            return True

        except pyodbc.Error as e:
            logger.error("Failed to connect to database: %s", str(e))
            return False

    def analyze_file_structure(self, df: pd.DataFrame) -> bool:
        """Analyze the DataFrame to understand its structure and infer schema."""
        try:
            logger.info("Analyzing DataFrame structure")
            headers = df.columns.tolist()
            logger.info("Found %d columns: %s", len(headers), headers)

            # Initialize column info
            for col in headers:
                self.columns_info[col] = {
                    'max_length': 0,
                    'has_nulls': False,
                    'all_numeric': True,
                    'all_integer': True,
                    'all_decimal': True,
                    'all_datetime': True,
                    'sample_values': []
                }

            # Efficiently calculate max lengths for object columns
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                # Ensure column is treated as string before using .str accessor
                max_len = df[col].astype(str).str.len().max()
                self.columns_info[col]['max_length'] = int(max_len) if pd.notna(max_len) else 0
                # Also, set numeric/datetime flags to False for string columns
                self.columns_info[col]['all_numeric'] = False
                self.columns_info[col]['all_integer'] = False
                self.columns_info[col]['all_decimal'] = False
                self.columns_info[col]['all_datetime'] = False


            # Sample rows to infer other types (numeric, datetime)
            sample_df = df.head(1000)
            for col in headers:
                self.columns_info[col]['has_nulls'] = df[col].isnull().any()
                sample_values = df[col].dropna().head(10).tolist()
                self.columns_info[col]['sample_values'] = [str(v) for v in sample_values]
                
                # Skip string columns for value-by-value analysis of other types
                if col not in string_columns:
                    for value in sample_df[col].dropna():
                        self._analyze_column_value(col, str(value))

            self.total_rows = len(df)
            logger.info("Analyzed %d sample rows out of %d total rows",
                       len(sample_df), self.total_rows)
            return True

        except Exception as e:
            logger.error("Error analyzing DataFrame: %s", str(e))
            return False

    def _analyze_column_value(self, column: str, value: str):
        """Analyze individual column value to infer data type."""
        col_info = self.columns_info[column]

        # Check for nulls/empty values
        if not value or value.strip() == '':
            col_info['has_nulls'] = True
            return

        # Test numeric types
        if col_info['all_numeric']:
            try:
                # Try integer
                int(value)
            except ValueError:
                col_info['all_integer'] = False

                # Try decimal
                try:
                    Decimal(value)
                except (ValueError, InvalidOperation):
                    col_info['all_numeric'] = False
                    col_info['all_decimal'] = False

        # Test datetime
        if col_info['all_datetime']:
            if not self._is_datetime(value):
                col_info['all_datetime'] = False

    def _is_datetime(self, value: str) -> bool:
        """Check if value can be parsed as datetime."""
        datetime_patterns = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y',
            '%d-%m-%Y %H:%M:%S',
            '%d-%m-%Y'
        ]

        for pattern in datetime_patterns:
            try:
                datetime.strptime(value, pattern)
                return True
            except ValueError:
                continue

        return False

    def _get_disputed_column_length(self, column_name: str) -> Optional[int]:
        """Check YAML config for a manual override for a disputed column's length."""
        tables_config = self.config.get('tables', {})
        table_specific_config = tables_config.get(self.table_name, {})
        disputed_columns = table_specific_config.get('disputed_columns', {})
        return disputed_columns.get(column_name, {}).get('max_length')

    def infer_sql_types(self) -> Dict[str, str]:
        """Infer SQL Server data types based on analyzed column data."""
        sql_types = {}

        for col, info in self.columns_info.items():
            if info['all_datetime']:
                sql_types[col] = 'DATETIME2'
            elif info['all_integer']:
                sql_types[col] = 'INT'
            elif info['all_decimal']:
                sql_types[col] = 'DECIMAL(18,4)'  # Default precision
            else:
                # String type - check for manual override first
                manual_length = self._get_disputed_column_length(col)
                if manual_length:
                    sql_length = manual_length
                    logger.info("Using manually configured max_length %d for column '%s'", sql_length, col)
                else:
                    # Auto-detect length
                    max_len = info['max_length']
                    if max_len == 0:
                        sql_length = 50  # Default for empty columns
                    elif max_len <= 50:
                        sql_length = 50
                    elif max_len <= 100:
                        sql_length = 100
                    elif max_len <= 255:
                        sql_length = 255
                    elif max_len <= 500:
                        sql_length = 500
                    else:
                        sql_length = max(1000, max_len + 100)  # Add buffer

                sql_types[col] = f'NVARCHAR({sql_length})'

        logger.info("Inferred SQL types:")
        for col, sql_type in sql_types.items():
            sample_vals = ', '.join(self.columns_info[col]['sample_values'][:3])
            logger.info("  %s: %s (samples: %s)", col, sql_type, sample_vals)

        return sql_types

    def generate_table_name(self, file_path: str) -> str:
        """Generate table name from file path."""
        filename = Path(file_path).name

        # Remove common suffixes like _000, _001, etc.
        table_name = re.sub(r'_\d{3,}$', '', filename)

        # Clean up the name for SQL Server
        table_name = re.sub(r'[^\w]', '_', table_name)
        table_name = re.sub(r'^[0-9]', 't_', table_name)  # Ensure doesn't start with number

        return table_name

    def generate_ddl(self, table_name: str, sql_types: Dict[str, str]) -> str:
        """Generate CREATE TABLE DDL statement."""
        columns_ddl = []
        not_null_columns = self.config.get('ddl', {}).get('not_null_columns', [])

        for col, sql_type in sql_types.items():
            null_clause = "NOT NULL" if col in not_null_columns else "NULL"
            columns_ddl.append(f"    [{col}] {sql_type} {null_clause}")

        ddl = f"""CREATE TABLE [{table_name}] (
{f',{chr(10)}'.join(columns_ddl)}
);"""

        logger.info("Generated DDL for table %s", table_name)
        return ddl

    def handle_existing_table(self, table_name: str) -> bool:
        """Handle existing table based on configuration."""
        try:
            cursor = self.connection.cursor()

            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'dbo'
            """, table_name)

            table_exists = cursor.fetchone()[0] > 0

            if table_exists:
                table_mode = self.config['loader']['table_mode']
                if table_mode == 'drop_recreate':
                    logger.info("Dropping existing table: %s", table_name)
                    # Use parameterized query to avoid SQL injection
                    drop_sql = f"DROP TABLE [{table_name}]"
                    cursor.execute(drop_sql)
                    self.connection.commit()
                    return False  # Table dropped, needs to be created
                if table_mode == 'append':
                    logger.info("Appending to existing table: %s", table_name)
                    return True  # Table exists, don't create
                logger.error("Table %s already exists and table_mode is 'fail'",
                           table_name)
                return False

            return False  # Table doesn't exist

        except pyodbc.Error as e:
            logger.error("Error checking existing table: %s", str(e))
            return False

    def create_table(self, ddl: str) -> bool:
        """Execute CREATE TABLE statement."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(ddl)
            self.connection.commit()
            logger.info("Table created successfully")
            return True

        except pyodbc.Error as e:
            logger.error("Error creating table: %s", str(e))
            return False

    def load_data(self, df: pd.DataFrame, table_name: str, file_path: str) -> bool:
        """Load data with configurable transaction semantics."""
        transaction_mode = self.config.get('loader', {}).get('transaction_mode', 'tolerant')

        if transaction_mode == 'strict':
            return self._load_data_strict_transaction(df, table_name, file_path)
        else:
            return self._load_data_tolerant_transaction(df, table_name, file_path)

    def _load_data_strict_transaction(self, df: pd.DataFrame, table_name: str, file_path: str) -> bool:
        """All-or-nothing transaction: complete success or complete rollback."""
        source_path = Path(file_path)

        try:
            self.connection.autocommit = False
            cursor = self.connection.cursor()

            logger.info("Loading data in STRICT transaction mode: %s", table_name)

            # Pre-validate all rows before any inserts
            validation_errors = self._validate_all_rows(df)
            if validation_errors:
                logger.error("Pre-validation failed for %d rows. Rejecting entire file.", len(validation_errors))
                self._log_validation_errors(validation_errors, source_path)
                os.rename(file_path, source_path.parent / 'error' / source_path.name)
                return False

            # If validation passes, insert all rows in single transaction
            columns = list(self.columns_info.keys())
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO [{table_name}] ([{'], ['.join(columns)}]) VALUES ({placeholders})"

            batch_data = []
            for _, row in df.iterrows():
                values = [row[col] for col in columns]
                converted_values = self._convert_values(values, columns)
                batch_data.append(tuple(converted_values))

            # Execute batch insert
            cursor.executemany(insert_sql, batch_data)
            self.connection.commit()

            # Move to processed directory
            os.rename(file_path, source_path.parent / 'processed' / source_path.name)
            self.processed_rows = len(df)
            self.error_rows = 0

            logger.info("Successfully loaded all %d rows in strict transaction mode", len(df))
            return True

        except Exception as e:
            logger.error("Strict transaction failed: %s", str(e))
            self.connection.rollback()
            os.rename(file_path, source_path.parent / 'error' / source_path.name)
            return False
        finally:
            self.connection.autocommit = True

    def _load_data_tolerant_transaction(self, df: pd.DataFrame, table_name: str, file_path: str) -> bool:
        """Tolerant transaction: allows partial success within error threshold."""
        successful_rows = 0
        failed_rows = 0
        failed_row_data = []
        source_path = Path(file_path)

        try:
            self.connection.autocommit = False
            cursor = self.connection.cursor()

            columns = list(self.columns_info.keys())
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = (f"INSERT INTO [{table_name}] "
                         f"([{'], ['.join(columns)}]) VALUES ({placeholders})")

            logger.info("Loading data in TOLERANT transaction mode: %s", table_name)

            for index, row in df.iterrows():
                try:
                    values = [row[col] for col in columns]
                    converted_values = self._convert_values(values, columns)
                    cursor.execute(insert_sql, tuple(converted_values))
                    successful_rows += 1
                except pyodbc.Error as e:
                    failed_rows += 1
                    failed_row_data.append(row.to_dict())
                    logger.warning("Error inserting row %d: %s", index, str(e))
                    if failed_rows > self.max_row_errors:
                        break

            if failed_rows > self.max_row_errors:
                self.connection.rollback()
                logger.critical("File %s rejected: Exceeded max-row-errors threshold (%d > %d).",
                                source_path.name, failed_rows, self.max_row_errors)
                os.rename(file_path, source_path.parent / 'error' / source_path.name)
                return False
            else:
                self.connection.commit()
                os.rename(file_path, source_path.parent / 'processed' / source_path.name)
                if failed_rows > 0:
                    log_file = source_path.parent / 'logs' / f"{source_path.stem}_{self.job_run_id}.txt"
                    with open(log_file, 'w') as f:
                        for item in failed_row_data:
                            f.write(f"{item}\n")
                self.processed_rows = successful_rows
                self.error_rows = failed_rows
                return True

        except pyodbc.Error as e:
            logger.error("Error loading data: %s", str(e))
            self.connection.rollback()
            return False
        finally:
            self.connection.autocommit = True

    def _validate_all_rows(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Pre-validate all rows before transaction. Returns list of validation errors."""
        validation_errors = []
        columns = list(self.columns_info.keys())

        for index, row in df.iterrows():
            try:
                values = [row[col] for col in columns]
                self._convert_values(values, columns)
            except Exception as e:
                validation_errors.append({
                    'row_index': index,
                    'error': str(e),
                    'row_data': row.to_dict()
                })

        return validation_errors

    def _log_validation_errors(self, validation_errors: List[Dict[str, Any]], source_path: Path):
        """Log validation errors to file for debugging."""
        if not validation_errors:
            return

        error_log_file = source_path.parent / 'logs' / f"{source_path.stem}_validation_errors_{self.job_run_id}.txt"

        try:
            with open(error_log_file, 'w') as f:
                f.write(f"Validation errors for file: {source_path.name}\n")
                f.write(f"Total errors: {len(validation_errors)}\n\n")

                for error in validation_errors:
                    f.write(f"Row {error['row_index']}: {error['error']}\n")
                    f.write(f"Data: {error['row_data']}\n\n")

            logger.info("Validation errors logged to: %s", error_log_file)
        except Exception as e:
            logger.error("Failed to write validation error log: %s", str(e))

    def _convert_values(self, values: List[str], columns: List[str]) -> List[Any]:
        """Convert string values to appropriate Python types."""
        converted = []

        for col, val in zip(columns, values):
            col_info = self.columns_info[col]

            # Handle empty/null values from pandas
            if pd.isna(val):
                converted.append(None)
                continue

            try:
                # Convert based on inferred type
                if col_info['all_datetime']:
                    # Try to parse datetime
                    dt_val = self._parse_datetime(val)
                    converted.append(dt_val)
                elif col_info['all_integer']:
                    converted.append(int(val))
                elif col_info['all_decimal']:
                    converted.append(Decimal(val))
                else:
                    converted.append(val)  # Keep as string

            except (ValueError, InvalidOperation):
                # If conversion fails, keep as string
                converted.append(val)

        return converted

    def _parse_datetime(self, value: str) -> Optional[datetime]:
        """Parse datetime string."""
        datetime_patterns = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y',
            '%d-%m-%Y %H:%M:%S',
            '%d-%m-%Y'
        ]

        for pattern in datetime_patterns:
            try:
                return datetime.strptime(value, pattern)
            except ValueError:
                continue

        return None

    def write_failed_statistics(self, file_path: str, error_message: str):
        """Writes a 'Failed' status to EtlJobStatistics for a file that could not be processed."""
        stats = {
            "JobRunID": str(uuid.uuid4()),
            "JobStartTime": datetime.utcnow(),
            "JobEndTime": datetime.utcnow(),
            "JobDurationSeconds": 0,
            "JobStatus": "Failed",
            "SourceFile": file_path,
            "TargetTable": self.generate_table_name(file_path),
            "RowsRead": 0,
            "RowsInserted": 0,
            "RowsUpdated": 0,
            "RowsFailed": 0,
            "ErrorMessage": error_message,
        }
        self.write_statistics(stats)

    def process_file(self, file_path: str, batch_job_id: Optional[str] = None) -> bool:
        """Main method to process a file."""
        self.job_run_id = self.config.get("job_run_id")
        self.batch_job_id = batch_job_id
        logger.info("Starting to process file: %s", file_path)

        if not os.path.exists(file_path):
            logger.error("File not found: %s", file_path)
            return False

        # Directory setup
        self._ensure_subdirectories(file_path)

        # Detect file type
        file_type = detect_file_type(file_path)
        logger.info("Detected file type: %s", file_type)

        # Read file into DataFrame
        if file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'psv':
            df = pd.read_csv(file_path, sep='|')
        else:
            logger.error("Unsupported file type: %s", file_type)
            return False

        # Connect to database
        if not self.connection and not self.connect_to_database():
            return False

        # Generate table name
        self.table_name = self.generate_table_name(file_path)
        logger.info("Target table: %s", self.table_name)

        # Analyze file structure
        if not self.analyze_file_structure(df):
            return False

        # Infer SQL types
        sql_types = self.infer_sql_types()

        # Handle existing table
        table_exists = self.handle_existing_table(self.table_name)

        # Create table if needed
        if not table_exists:
            ddl = self.generate_ddl(self.table_name, sql_types)
            logger.info("DDL Statement:\n%s", ddl)

            if not self.create_table(ddl):
                return False

        # Load data
        if not self.load_data(df, self.table_name, file_path):
            return False

        logger.info("Successfully processed file: %s", file_path)
        logger.info("Total rows: %d, Processed: %d, Errors: %d",
                   self.total_rows, self.processed_rows, self.error_rows)

        return True


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='File to SQL Server Loader')
    parser.add_argument('input_path', help='Path to the file or directory to process')
    parser.add_argument('--config', default='loader_config.yaml',
                        help='Path to the configuration file (default: loader_config.yaml)')
    args = parser.parse_args()

    config = load_config(args.config)
    job_run_id = uuid.uuid4()
    setup_logging(config, job_run_id)

    if os.path.isdir(args.input_path):
        # Directory processing logic with checkpointing
        logger.info("Starting batch processing for directory: %s", args.input_path)

        # Initial setup
        loader_for_setup = FileToSQLLoader(config)
        loader_for_setup.setup_batch_statistics_table()

        # Initialize batch job manager
        batch_manager = BatchJobManager(config, args.input_path, loader_for_setup.connection)
        batch_manager.setup_enhanced_batch_tables()

        # Get or resume batch job
        batch_job_id, is_resumed = batch_manager.get_or_create_batch_job()

        if is_resumed:
            logger.info("Resuming batch processing for directory: %s, BatchJobID: %s", args.input_path, batch_job_id)
        else:
            logger.info("Starting new batch processing for directory: %s, BatchJobID: %s", args.input_path, batch_job_id)

        # Get all files and filter out already processed ones
        all_files = [os.path.join(args.input_path, f) for f in os.listdir(args.input_path) if os.path.isfile(os.path.join(args.input_path, f))]
        files_to_process = batch_manager.get_pending_files(all_files, batch_job_id)
        total_files = len(all_files)

        # Handle batch statistics record creation or update
        if not is_resumed:
            # Create initial batch statistics record for new batch
            batch_stats = {
                'BatchJobID': batch_job_id,
                'DirectoryPath': args.input_path,
                'TotalFiles': total_files,
                'FilesProcessed': 0,
                'FilesFailed': 0,
                'BatchStartTime': datetime.utcnow(),
                'BatchEndTime': None,
                'BatchStatus': 'InProgress'
            }

            try:
                cursor = loader_for_setup.connection.cursor()
                cursor.execute("""
                    INSERT INTO EtlBatchJobStatistics (BatchJobID, DirectoryPath, TotalFiles, FilesProcessed, FilesFailed, BatchStartTime, BatchStatus, IsResumed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_job_id, args.input_path, total_files, 0, 0, batch_stats['BatchStartTime'], 'InProgress', 0)
                loader_for_setup.connection.commit()
            except pyodbc.Error as e:
                logger.error("Failed to insert initial batch statistics record: %s", e)
                sys.exit(1)
        else:
            # Get current batch statistics for resumed batch
            cursor = loader_for_setup.connection.cursor()
            cursor.execute("""
                SELECT FilesProcessed, FilesFailed FROM EtlBatchJobStatistics
                WHERE BatchJobID = ?
            """, batch_job_id)
            result = cursor.fetchone()
            if result:
                logger.info("Resuming batch with %d files already processed, %d failed", result[0], result[1])

        files_processed = 0
        files_failed = 0
        max_retries = config.get('loader', {}).get('max_retries', 1)

        if len(files_to_process) == 0:
            logger.info("No pending files to process. All files in directory have been processed.")
        else:
            logger.info("Processing %d pending files out of %d total files", len(files_to_process), total_files)

        for file_path in files_to_process:
            for attempt in range(max_retries + 1):
                try:
                    loader = FileToSQLLoader(config, batch_job_id=batch_job_id)
                    if loader.process_file(file_path, batch_job_id):
                        files_processed += 1
                        break  # Success, break retry loop
                except Exception as e:
                    logger.error("Attempt %d/%d failed for file %s: %s", attempt + 1, max_retries, file_path, e)
                    if attempt == max_retries:
                        files_failed += 1
                        logger.critical("All retries failed for file %s. Marking as failed.", file_path)
                        # This requires a method to write failure status without full processing
                        loader.write_failed_statistics(file_path, str(e))

        # Update final batch statistics
        batch_end_time = datetime.utcnow()

        # Get current counts from database for resumed batches
        cursor = loader_for_setup.connection.cursor()
        cursor.execute("""
            SELECT FilesProcessed, FilesFailed FROM EtlBatchJobStatistics
            WHERE BatchJobID = ?
        """, batch_job_id)
        current_stats = cursor.fetchone()

        if current_stats:
            # Add current run results to existing counts
            total_processed = current_stats[0] + files_processed
            total_failed = current_stats[1] + files_failed
        else:
            # New batch, use current run results
            total_processed = files_processed
            total_failed = files_failed

        # Determine final status
        if total_failed > 0:
            final_status = 'CompletedWithErrors'
        else:
            final_status = 'Completed'

        try:
            cursor.execute("""
                UPDATE EtlBatchJobStatistics
                SET FilesProcessed = ?, FilesFailed = ?, BatchEndTime = ?, BatchStatus = ?
                WHERE BatchJobID = ?
            """, total_processed, total_failed, batch_end_time, final_status, batch_job_id)
            loader_for_setup.connection.commit()

            logger.info("Batch processing completed. Total processed: %d, Total failed: %d",
                       total_processed, total_failed)
        except pyodbc.Error as e:
            logger.error("Failed to update final batch statistics: %s", e)

        # Send email notification for batch completion
        email_manager = EmailNotificationManager(config)
        batch_summary = {
            'job_type': 'Batch ETL Job',
            'status': final_status,
            'start_time': batch_stats.get('BatchStartTime', 'N/A') if 'batch_stats' in locals() else 'N/A',
            'end_time': batch_end_time.strftime('%Y-%m-%d %H:%M:%S') if batch_end_time else 'N/A',
            'duration_seconds': int((batch_end_time - batch_stats.get('BatchStartTime', batch_end_time)).total_seconds()) if 'batch_stats' in locals() and batch_stats.get('BatchStartTime') else 0,
            'is_batch': True,
            'directory_path': args.input_path,
            'total_files': total_files,
            'files_processed': total_processed,
            'files_failed': total_failed,
            'batch_job_id': batch_job_id,
            'error_message': None if final_status == 'Completed' else f"{total_failed} files failed processing"
        }
        email_manager.send_job_completion_email(batch_summary)

        if loader_for_setup.connection:
            loader_for_setup.connection.close()

    elif os.path.isfile(args.input_path):
        # Single file processing logic
        start_time = datetime.utcnow()
        loader = FileToSQLLoader(config)
        success = loader.process_file(args.input_path)
        end_time = datetime.utcnow()

        # Send email notification for single file completion
        email_manager = EmailNotificationManager(config)
        file_summary = {
            'job_type': 'Single File ETL Job',
            'status': 'Completed' if success else 'Failed',
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_seconds': int((end_time - start_time).total_seconds()),
            'is_batch': False,
            'source_file': args.input_path,
            'target_table': loader.table_name if hasattr(loader, 'table_name') else 'N/A',
            'rows_read': loader.total_rows if hasattr(loader, 'total_rows') else 0,
            'rows_processed': loader.processed_rows if hasattr(loader, 'processed_rows') else 0,
            'rows_failed': loader.error_rows if hasattr(loader, 'error_rows') else 0,
            'error_message': None if success else 'File processing failed - check logs for details'
        }
        email_manager.send_job_completion_email(file_summary)

        if not success:
            sys.exit(1)
    else:
        logger.error("Input path does not exist: %s", args.input_path)
        sys.exit(1)


if __name__ == "__main__":
    main()
