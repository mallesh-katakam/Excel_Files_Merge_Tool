# Data Merge Automation - Setup and Usage Guide

## Overview
This enhanced data merge tool automatically processes Excel/CSV files from a specified directory and enriches them with database information. It can run manually or be scheduled for daily execution.

**IMPORTANT**: To run automatically even when you close the editor, you MUST use Windows Task Scheduler (see Setup Step 3). The Python scheduler mode (`python data_merge.py auto`) only works while the terminal/editor is open.

## Features
- **Multi-file Processing**: Automatically discovers and processes all Excel/CSV files in the input directory
- **Automated Scheduling**: Runs daily at scheduled time using Windows Task Scheduler (works even when editor is closed)
- **File Management**: Moves processed files to avoid reprocessing
- **Enhanced Logging**: Detailed logs with timestamps for troubleshooting
- **Error Handling**: Robust error handling with retry logic
- **Flexible Configuration**: Easy configuration through JSON file

## Directory Structure
```
C:\Users\sharm\Downloads\sftp_files\
├── file1.xlsx          # Input files (will be processed)
├── file2.csv           # Input files (will be processed)
├── processed\          # Directory for processed files
│   ├── file1_enriched_20241201_130000.xlsx
│   └── file2_enriched_20241201_130000.csv
└── processed\          # Directory for original files after processing
    ├── file1.xlsx
    └── file2.csv
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
pip install pandas mysql-connector-python schedule
```

### Step 2: Configure Settings
Edit `config.json` to modify:
- Input/output directories
- Database connection settings
- Column mappings
- Processing parameters

### Step 3: Set Up Windows Task Scheduler (REQUIRED for Auto-Run When Editor is Closed)

**This is the ONLY way to make the script run automatically when you close the editor!**

The scheduler reads the time from `config.json` (currently set to 18:56:00 = 6:56 PM).

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
4. Set trigger to "Daily" and choose your time
5. Set action to "Start a program"
6. Program/script: `C:\Users\sharm\OneDrive\Desktop\DATA_MERGE3\run_data_merge.bat`
7. Start in: `C:\Users\sharm\OneDrive\Desktop\DATA_MERGE3`
8. Check "Run whether user is logged on or not" (optional)
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

## Configuration

### Input Directory
- Place Excel/CSV files in: `C:\Users\sharm\Downloads\sftp_files`
- Supported formats: .xlsx, .xls, .csv

### Output Directory
- Processed files saved to: `C:\Users\sharm\Downloads\sftp_files\processed`
- Original files moved to: `C:\Users\sharm\Downloads\sftp_files\processed\processed`

### Database Configuration
- Host: 183.82.97.170
- Database: ats
- Table: PDF_Invoice_Details
- Authentication: ats/cbwu+v6zq-9

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
1. **Discovery**: Scan input directory for supported files
2. **Validation**: Check file accessibility and format
3. **Processing**: Enrich data with database information
4. **Output**: Save enriched data with timestamp
5. **Cleanup**: Move original files to processed folder

## Performance Optimization
- Batch processing (100 rows per batch)
- Connection pooling
- Retry logic for failed operations
- Efficient database queries

## Security Notes
- Database credentials stored in configuration
- Log files may contain sensitive data
- Ensure proper file permissions
- Consider encrypting configuration file for production use

## Support
For issues or questions:
1. Check log files for error details
2. Verify configuration settings
3. Test database connectivity
4. Ensure file permissions are correct
