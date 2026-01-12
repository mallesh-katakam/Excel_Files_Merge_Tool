# Data Merge Automation - Production-Ready Tool

## Overview
Enterprise-grade data enrichment tool that automatically processes Excel/CSV files from a specified directory and enriches them with database information. Supports automated scheduling, SFTP file retrieval, email notifications, and advanced data processing features.

**IMPORTANT**: For production use with automatic execution, Windows Task Scheduler is required (see Setup Step 3). The Python scheduler mode only works while the terminal is running.

## Features

### Core Processing
- **Multi-file Processing**: Automatically discovers and processes all Excel/CSV files in the input directory
- **Database Enrichment**: Enriches data with information from MySQL database using flexible matching logic
- **Intelligent Matching**: Supports multiple reference combinations (PNR+Airline+Sector, PNR+Sector, Ticket+Sector)
- **FCM File Support**: Special parsing logic for Flight Centre Management (FCM) files
- **Invoice Deduplication**: Automatically identifies and marks duplicate invoice numbers
- **Data Splitting**: Separates output into Invoice, Credit Note, and Zero-amount files
- **GSTR Integration**: Merges GSTR 2B/3B filing status data

### Automation & Monitoring
- **Automated Scheduling**: Runs daily at configured time using Windows Task Scheduler
- **SFTP Integration**: Automatically downloads files from SFTP server (optional)
- **Email Notifications**: Sends processing reports via email (optional)
- **Enhanced Logging**: Detailed daily logs with timestamps for troubleshooting
- **Error Handling**: Robust error handling with automatic retry logic
- **Batch Processing**: Configurable batch size for optimal performance

### Configuration
- **Flexible Configuration**: All settings managed through `config.json`
- **Environment-specific Settings**: Easy to configure for different environments
- **Credential Management**: Centralized credential storage (consider encryption for production)

## Directory Structure
```
DATA_MERGE6\
├── config.json                 # Main configuration file
├── data_merge.py              # Main processing script
├── fcm_parser.py              # FCM file parsing module
├── gstr_2b_3b_merger.py       # GSTR filing status merger
├── invoice_deduplicator.py    # Invoice deduplication module
├── data_splitter.py           # Data splitting module
├── run_data_merge.bat         # Batch file for Task Scheduler
├── setup_scheduler.ps1        # PowerShell script for scheduler setup
├── requirements.txt           # Python dependencies
├── .gitignore                 # Git ignore file
├── README.md                  # This file
├── FCM_PARSING_GUIDE.md       # FCM parsing documentation
├── SECTOR_MATCHING_LOGIC.md   # Sector matching documentation
└── data_merge_YYYYMMDD.log    # Daily log files

Input Directory (configurable):
D:\Gst_Files\
├── file1.xlsx          # Input files (will be processed)
├── file2.csv           # Input files (will be processed)
└── GST_2b_3b\          # GSTR filing status files
    └── gstr_data.xlsx

Output Directory (configurable):
D:\Gst_Files\processed\
├── file1_enriched_20260105_120000.xlsx
├── file1_enriched_20260105_120000_Invoice.xlsx
├── file1_enriched_20260105_120000_credit_note.xlsx
└── file1_enriched_20260105_120000_zero.xlsx
```

## Usage Modes

### 1. Manual Processing (Default)
```bash
python data_merge.py
```
- Processes files in the input directory
- Shows available files and prompts for action

### 2. One-time Processing
```bash
python data_merge.py process
```
- Processes all files in the input directory once
- Moves processed files to avoid reprocessing

### 3. Automated/Scheduled Mode (Python - Requires Terminal Open)
```bash
python data_merge.py auto
```
- Starts the Python scheduler for daily execution
- **WARNING**: This mode only works while the terminal/editor is open
- If you close the editor, the scheduler stops
- **For automatic execution when editor is closed, use Windows Task Scheduler instead (see Setup Step 3)**

## Setup Instructions

### Step 1: Install Required Dependencies
```bash
pip install -r requirements.txt
```

Or install individually:
```bash
pip install pandas mysql-connector-python schedule openpyxl xlsxwriter paramiko pyxlsb
```

### Step 2: Configure Settings
Edit `config.json` to configure:
- **Directories**: Input/output directories, GSTR directory
- **Database**: Connection settings (host, database, user, password, port)
- **Processing**: Batch size, retry logic, timeout settings
- **Scheduling**: Enabled/disabled, execution time, timezone
- **SFTP** (optional): Server connection and file download settings
- **Email** (optional): SMTP settings for notifications
- **Column Mappings**: Map input columns to database columns
- **Debug**: Debug mode and specific ID filtering

### Step 3: Set Up Windows Task Scheduler (REQUIRED for Auto-Run When Editor is Closed)

**This is the ONLY way to make the script run automatically when you close the editor!**

The scheduler reads the time from `config.json` (`scheduling.time` setting).

#### Option A: Using PowerShell Script (Easiest - Recommended)
1. Right-click on `setup_scheduler.ps1` .\setup_scheduler.ps1
2. Select "Run with PowerShell" (or run as Administrator if needed)
3. The script will:
   - Read the schedule time from `config.json`
   - Create/update a Windows Task Scheduler task
   - Set it to run daily at the specified time
4. Follow the prompts - you can test run immediately

#### Option B: Manual Setup in Task Scheduler
1. Open Task Scheduler (`taskschd.msc` or search "Task Scheduler" in Start menu)
2. Click "Create Basic Task"
3. Name it: `DataMerge_Daily_Processing`
4. Set trigger to "Daily" and choose your time (from `config.json`)
5. Set action to "Start a program"
6. Program/script: `C:\Users\sharm\OneDrive\Desktop\DATA_MERGE6\run_data_merge.bat`
7. Start in: `C:\Users\sharm\OneDrive\Desktop\DATA_MERGE6`
8. Check "Run whether user is logged on or not" (optional - requires admin)
9. Finish

#### Verifying the Scheduled Task
- Open Task Scheduler
- Look for `DataMerge_Daily_Processing` in the Task Scheduler Library
- You can right-click and select "Run" to test it immediately
- Check "History" tab to see when it ran and if there were any errors

### Step 4: Test the Setup
```bash
# Test one-time processing
python data_merge.py process

# OR test using the batch file (same as Task Scheduler will use)
run_data_merge.bat

# Test Python scheduler mode (NOTE: only works while terminal is open)
python data_merge.py auto
```

**To test Windows Task Scheduler:**
- Open Task Scheduler
- Find `DataMerge_Daily_Processing` task
- Right-click → "Run" to execute immediately
- Check the log file to verify it ran successfully

## Configuration Details

### Directories (in `config.json`)
- **input_directory**: Source directory for Excel/CSV files
- **output_directory**: Destination for processed files
- **gstr_directory**: Location of GSTR 2B/3B filing status files
- Supported formats: `.xlsx`, `.xls`, `.xlsb`, `.csv`

### Database Configuration (in `config.json`)
The tool connects to a MySQL database to enrich data:
- Database connection settings in `config.json` → `database` section
- Table name: Configurable via `table_name` setting
- Column mappings: Flexible mapping in `column_mapping` section
- Reference combinations: Multiple matching strategies in `possible_reference_combinations`

### Processing Configuration
- **batch_size**: Number of rows to process in each batch (default: 100)
- **max_retries**: Maximum retry attempts for failed operations (default: 3)
- **connection_timeout**: Database connection timeout in seconds (default: 30)
- **query_timeout**: Query execution timeout in seconds (default: 10)

### Optional Features
- **SFTP**: Enable automatic file download from SFTP server
- **Email**: Enable email notifications with processing reports
- **Debug Mode**: Enable detailed logging for specific records

## Logging
- Log files created daily: `data_merge_YYYYMMDD.log`
- Console output for immediate feedback
- Detailed error logging for troubleshooting

## Troubleshooting

### Common Issues
1. **Database Connection Failed**
   - Check network connectivity
   - Verify database credentials
   - Ensure database server is running

2. **No Files Found**
   - Verify input directory path
   - Check file extensions (.xlsx, .xls, .csv)
   - Ensure files are not locked by other applications

3. **Permission Errors**
   - Run as Administrator for Task Scheduler setup
   - Check file/folder permissions
   - Ensure output directory is writable

### Log Analysis
Check the daily log file for:
- Processing status
- Error messages
- Performance metrics
- File processing results

## File Processing Flow
1. **SFTP Download** (optional): Download files from SFTP server to input directory
2. **Discovery**: Scan input directory for supported files (Excel/CSV)
3. **Validation**: Check file accessibility, format, and required columns
4. **Database Enrichment**: 
   - Connect to MySQL database
   - Match records using flexible reference combinations
   - Enrich with database information (ticket details, amounts, etc.)
5. **Invoice Deduplication**: Identify and mark duplicate invoice numbers
6. **GSTR Merging**: Merge GSTR 2B/3B filing status data
7. **Data Splitting**: Split into Invoice, Credit Note, and Zero-amount files
8. **Output**: Save enriched data with timestamp
9. **File Management**: Move original files to processed folder
10. **Email Notification** (optional): Send processing report via email

## Performance Optimization
- Batch processing (100 rows per batch)
- Connection pooling
- Retry logic for failed operations
- Efficient database queries

## Production Deployment Checklist

### Security
- [ ] Review and secure database credentials in `config.json`
- [ ] Consider encrypting `config.json` for production environments
- [ ] Ensure proper file permissions on directories and config files
- [ ] Review log files - they may contain sensitive data
- [ ] Secure SFTP credentials if using SFTP feature
- [ ] Secure email credentials if using email notifications
- [ ] Implement network security (firewall rules, VPN if required)

### Performance
- [ ] Adjust `batch_size` based on system resources and data volume
- [ ] Monitor database connection pool settings
- [ ] Review and optimize query timeout settings
- [ ] Monitor disk space for input/output directories and logs

### Monitoring
- [ ] Set up log rotation for `data_merge_YYYYMMDD.log` files
- [ ] Monitor Task Scheduler execution history
- [ ] Enable email notifications for production monitoring
- [ ] Set up alerts for processing failures
- [ ] Review logs regularly for errors and performance issues

### Maintenance
- [ ] Test the complete workflow in a test environment first
- [ ] Document any environment-specific configurations
- [ ] Create backup of `config.json` before changes
- [ ] Schedule regular testing of the Task Scheduler task
- [ ] Keep Python and dependencies updated

## Module Documentation

- **FCM_PARSING_GUIDE.md**: Detailed guide for Flight Centre Management file parsing
- **SECTOR_MATCHING_LOGIC.md**: Documentation on sector matching algorithms

## Support & Troubleshooting

### Log Files
Daily log files (`data_merge_YYYYMMDD.log`) contain:
- Processing status and statistics
- Error messages with stack traces
- Database query information
- File processing details
- Performance metrics

### Common Issues
1. **Database Connection Failed**
   - Check network connectivity to database server
   - Verify credentials in `config.json`
   - Ensure database server is running and accessible
   - Check firewall settings

2. **No Files Found**
   - Verify `input_directory` path in `config.json`
   - Check file extensions match `supported_extensions`
   - Ensure files are not locked by other applications
   - Check SFTP download if enabled

3. **Permission Errors**
   - Run Task Scheduler setup as Administrator
   - Check file/folder permissions on input/output directories
   - Ensure write permissions for output directory
   - Check log file write permissions

4. **Processing Errors**
   - Check log file for specific error details
   - Verify input file format and columns
   - Test database connectivity independently
   - Review column mappings in `config.json`

### Getting Help
For issues or questions:
1. Review the daily log file for error details
2. Verify all settings in `config.json`
3. Test database connectivity separately
4. Check file and directory permissions
5. Review the documentation files (FCM_PARSING_GUIDE.md, SECTOR_MATCHING_LOGIC.md)

## Version History
- **Production Release**: Code optimized and cleaned for production use
  - Removed test/debug code
  - Added comprehensive documentation
  - Enhanced security notes
  - Added deployment checklist
