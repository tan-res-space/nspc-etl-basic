# Email Notification Configuration Guide

## Overview

The NSPC ETL System includes comprehensive email notification functionality that automatically sends status updates upon completion of ETL jobs. This feature supports both single file processing and batch directory processing with detailed HTML-formatted reports.

## Configuration

### Basic Email Settings

Add the following configuration to your `loader_config.yaml` file:

```yaml
email_notifications:
  enabled: true  # Set to false to disable email notifications
  smtp_server: 'smtp.gmail.com'  # SMTP server address
  smtp_port: 587  # SMTP port (587 for TLS, 465 for SSL, 25 for non-encrypted)
  use_tls: true  # Use TLS encryption
  username: 'your-email@gmail.com'  # SMTP username (usually your email)
  password: 'your-app-password'  # SMTP password or app-specific password
  from_email: 'your-email@gmail.com'  # From email address (can be same as username)
  to_emails:  # List of recipient email addresses
    - 'admin@company.com'
    - 'data-team@company.com'
    - 'manager@company.com'
```

### Common SMTP Provider Settings

#### Gmail
```yaml
smtp_server: 'smtp.gmail.com'
smtp_port: 587
use_tls: true
```
**Note**: Use App Passwords for Gmail accounts with 2FA enabled.

#### Outlook/Hotmail
```yaml
smtp_server: 'smtp-mail.outlook.com'
smtp_port: 587
use_tls: true
```

#### Yahoo Mail
```yaml
smtp_server: 'smtp.mail.yahoo.com'
smtp_port: 587
use_tls: true
```

#### Custom SMTP Server
```yaml
smtp_server: 'your-smtp-server.com'
smtp_port: 587  # or 25, 465 depending on your server
use_tls: true   # or false for non-encrypted connections
```

## Email Content

### Subject Lines

The system automatically generates descriptive subject lines based on job status:

- ✅ **Success**: "✅ Batch ETL Job Completed Successfully"
- ⚠️ **Warnings**: "⚠️ Single File ETL Job Completed with Errors"
- ❌ **Failures**: "❌ Batch ETL Job Failed"

### Email Body Content

#### Single File Processing
- Job type and status
- Processing start and end times
- Duration in HH:MM:SS format
- Source file path
- Target table name
- Row counts (read, processed, failed)
- Error details (if applicable)

#### Batch Processing
- Job type and status
- Processing start and end times
- Duration in HH:MM:SS format
- Directory path
- File counts (total, processed, failed)
- Batch job identifier
- Error summary (if applicable)

### Sample Email Templates

#### Successful Single File Processing
```
Subject: ✅ Single File ETL Job Completed Successfully

ETL Job Status Report
Status: Completed

Job Summary:
- Job Type: Single File ETL Job
- Status: Completed
- Start Time: 2024-01-15 10:30:00
- End Time: 2024-01-15 10:32:15
- Duration: 00:02:15
- Source File: /data/sales_data.csv
- Target Table: sales_data
- Rows Read: 10,000
- Rows Processed: 10,000
- Rows Failed: 0
```

#### Batch Processing with Errors
```
Subject: ⚠️ Batch ETL Job Completed with Errors

ETL Job Status Report
Status: CompletedWithErrors

Job Summary:
- Job Type: Batch ETL Job
- Status: CompletedWithErrors
- Start Time: 2024-01-15 09:00:00
- End Time: 2024-01-15 09:45:30
- Duration: 00:45:30
- Directory Path: /data/batch_import/
- Total Files: 25
- Files Processed: 22
- Files Failed: 3
- Batch Job ID: 550e8400-e29b-41d4-a716-446655440000

Error Details:
3 files failed processing
```

## Security Considerations

### Email Credentials
- Use app-specific passwords instead of main account passwords
- Store sensitive credentials securely
- Consider using environment variables for production deployments
- Regularly rotate email passwords

### Network Security
- Always use TLS encryption when available
- Verify SMTP server certificates
- Use secure ports (587 for TLS, 465 for SSL)
- Avoid unencrypted connections (port 25) in production

## Troubleshooting

### Common Issues

#### Authentication Failures
- Verify username and password are correct
- Check if 2FA is enabled and app password is required
- Ensure account allows SMTP access

#### Connection Issues
- Verify SMTP server address and port
- Check firewall settings
- Confirm TLS/SSL settings match server requirements

#### Email Not Received
- Check spam/junk folders
- Verify recipient email addresses are correct
- Check email provider's delivery logs

### Debug Mode

To troubleshoot email issues, enable debug logging in your configuration:

```yaml
logging:
  enabled: true
  level: 'DEBUG'  # This will show detailed SMTP communication
```

## Best Practices

### Recipient Management
- Use distribution lists for team notifications
- Include both technical and business stakeholders
- Consider separate lists for different severity levels

### Content Customization
- Email templates are automatically generated
- HTML formatting provides clear, readable reports
- Status indicators use color coding for quick identification

### Performance Considerations
- Email sending is non-blocking and won't delay ETL processing
- Failed email delivery is logged but doesn't affect job status
- Consider email rate limits for high-frequency processing

## Integration Examples

### Cron Job Integration
```bash
# Daily batch processing with email notifications
0 2 * * * /path/to/run-file-to-sql-loader.sh /data/daily_batch/
```

### Monitoring Integration
- Email notifications complement existing logging
- Can be integrated with monitoring systems
- Provides immediate alerts for critical failures

## Customization

The email notification system is designed to be extensible. Future enhancements could include:
- Custom email templates
- Conditional notifications based on error thresholds
- Integration with ticketing systems
- Slack or Teams notifications
- SMS alerts for critical failures

For advanced customization, modify the `EmailNotificationManager` class in `src/file-to-sql-loader.py`.
