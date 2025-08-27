#!/usr/bin/env python3
"""
Test script for email notifications in the NSPC ETL System.
This script allows you to test email configuration without running a full ETL job.
"""

import sys
import yaml
from datetime import datetime
from pathlib import Path

# Add the src directory to the path to import the EmailNotificationManager
sys.path.append(str(Path(__file__).parent / 'src'))

try:
    from file_to_sql_loader import EmailNotificationManager
except ImportError as e:
    print(f"Error importing EmailNotificationManager: {e}")
    print("Make sure the src/file-to-sql-loader.py file exists and is accessible.")
    sys.exit(1)


def load_config(config_path: str = 'src/loader_config.yaml'):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def test_single_file_success():
    """Test email notification for successful single file processing."""
    return {
        'job_type': 'Single File ETL Job (TEST)',
        'status': 'Completed',
        'start_time': '2024-01-15 10:30:00',
        'end_time': '2024-01-15 10:32:15',
        'duration_seconds': 135,
        'is_batch': False,
        'source_file': '/test/data/sample_data.csv',
        'target_table': 'test_sample_data',
        'rows_read': 1000,
        'rows_processed': 1000,
        'rows_failed': 0,
        'error_message': None
    }


def test_single_file_failure():
    """Test email notification for failed single file processing."""
    return {
        'job_type': 'Single File ETL Job (TEST)',
        'status': 'Failed',
        'start_time': '2024-01-15 10:30:00',
        'end_time': '2024-01-15 10:30:45',
        'duration_seconds': 45,
        'is_batch': False,
        'source_file': '/test/data/corrupted_data.csv',
        'target_table': 'test_corrupted_data',
        'rows_read': 0,
        'rows_processed': 0,
        'rows_failed': 0,
        'error_message': 'File format not recognized or corrupted data structure'
    }


def test_batch_success():
    """Test email notification for successful batch processing."""
    return {
        'job_type': 'Batch ETL Job (TEST)',
        'status': 'Completed',
        'start_time': '2024-01-15 09:00:00',
        'end_time': '2024-01-15 09:25:30',
        'duration_seconds': 1530,
        'is_batch': True,
        'directory_path': '/test/data/batch_import/',
        'total_files': 15,
        'files_processed': 15,
        'files_failed': 0,
        'batch_job_id': '550e8400-e29b-41d4-a716-446655440000',
        'error_message': None
    }


def test_batch_with_errors():
    """Test email notification for batch processing with errors."""
    return {
        'job_type': 'Batch ETL Job (TEST)',
        'status': 'CompletedWithErrors',
        'start_time': '2024-01-15 09:00:00',
        'end_time': '2024-01-15 09:45:30',
        'duration_seconds': 2730,
        'is_batch': True,
        'directory_path': '/test/data/batch_import/',
        'total_files': 25,
        'files_processed': 22,
        'files_failed': 3,
        'batch_job_id': '550e8400-e29b-41d4-a716-446655440001',
        'error_message': '3 files failed processing due to data format issues'
    }


def main():
    """Main function to test email notifications."""
    print("NSPC ETL System - Email Notification Test")
    print("=" * 50)
    
    # Load configuration
    config = load_config()
    
    # Check if email notifications are enabled
    email_config = config.get('email_notifications', {})
    if not email_config.get('enabled', False):
        print("❌ Email notifications are disabled in configuration.")
        print("   Set 'email_notifications.enabled: true' in your config file.")
        return
    
    print("✅ Email notifications are enabled")
    print(f"📧 SMTP Server: {email_config.get('smtp_server', 'Not configured')}")
    print(f"📧 Recipients: {', '.join(email_config.get('to_emails', []))}")
    print()
    
    # Initialize email manager
    email_manager = EmailNotificationManager(config)
    
    # Test scenarios
    test_scenarios = [
        ("Single File Success", test_single_file_success()),
        ("Single File Failure", test_single_file_failure()),
        ("Batch Success", test_batch_success()),
        ("Batch with Errors", test_batch_with_errors())
    ]
    
    print("Available test scenarios:")
    for i, (name, _) in enumerate(test_scenarios, 1):
        print(f"  {i}. {name}")
    print("  5. Test all scenarios")
    print("  0. Exit")
    
    while True:
        try:
            choice = input("\nSelect a test scenario (0-5): ").strip()
            
            if choice == '0':
                print("Exiting...")
                break
            elif choice == '5':
                print("\n🚀 Testing all scenarios...")
                for name, scenario in test_scenarios:
                    print(f"\n📧 Sending test email: {name}")
                    success = email_manager.send_job_completion_email(scenario)
                    if success:
                        print(f"✅ {name} email sent successfully")
                    else:
                        print(f"❌ {name} email failed to send")
                break
            elif choice in ['1', '2', '3', '4']:
                idx = int(choice) - 1
                name, scenario = test_scenarios[idx]
                print(f"\n📧 Sending test email: {name}")
                success = email_manager.send_job_completion_email(scenario)
                if success:
                    print(f"✅ {name} email sent successfully")
                else:
                    print(f"❌ {name} email failed to send")
            else:
                print("Invalid choice. Please select 0-5.")
                
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except ValueError:
            print("Invalid input. Please enter a number.")
    
    print("\nTest completed!")


if __name__ == "__main__":
    main()
