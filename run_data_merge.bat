@echo off
REM Batch file to run the data merge script
REM This file can be used with Windows Task Scheduler
REM It will run automatically even when the editor/terminal is closed

REM Get the directory where this batch file is located
set "BATCH_DIR=%~dp0"
set "CONFIG_FILE=%BATCH_DIR%config.json"

REM Read working directory from config.json using PowerShell
for /f "usebackq tokens=*" %%i in (`powershell -Command "try { $config = Get-Content '%CONFIG_FILE%' -Raw | ConvertFrom-Json; if ($config.paths -and $config.paths.working_directory) { $config.paths.working_directory } else { '%BATCH_DIR%' } } catch { '%BATCH_DIR%' }"`) do set "WORKING_DIR=%%i"

REM Change to the working directory
cd /d "%WORKING_DIR%"

REM Run the script in process mode (one-time execution)
python data_merge.py process

REM Only pause if running interactively (not from Task Scheduler)
REM Check if stdout is a console (not redirected)
if "%1"=="interactive" (
    pause
)
