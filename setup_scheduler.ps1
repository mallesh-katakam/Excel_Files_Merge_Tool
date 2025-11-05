# PowerShell script to set up Windows Task Scheduler for daily execution
# Run this script as Administrator to create the scheduled task
# OR right-click and "Run with PowerShell" (may require elevation)

# Get the directory where this script is located
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConfigPath = "$ScriptDir\config.json"

# Read working directory from config.json
$WorkingDir = $ScriptDir  # Default fallback
try {
    if (Test-Path $ConfigPath) {
        $jsonContent = Get-Content $ConfigPath -Raw -Encoding UTF8
        $config = $jsonContent | ConvertFrom-Json
        
        if ($config.paths -and $config.paths.working_directory) {
            $WorkingDir = $config.paths.working_directory
            Write-Host "Working directory from config.json: $WorkingDir" -ForegroundColor Green
        } else {
            Write-Host "No working_directory found in config.json, using script directory: $WorkingDir" -ForegroundColor Yellow
        }
    } else {
        Write-Host "config.json not found, using script directory: $WorkingDir" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "Error reading working directory from config.json: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Using script directory: $WorkingDir" -ForegroundColor Yellow
}

$BatchPath = "$WorkingDir\run_data_merge.bat"

# Check if running as administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host ""
    Write-Host "WARNING: Not running as Administrator!" -ForegroundColor Yellow
    Write-Host "Some operations may require admin rights. Trying anyway..." -ForegroundColor Yellow
    Write-Host ""
}

# Read schedule time from config.json (using already loaded config)
$scheduleTimeFormatted = "1:00PM"  # Default fallback
try {
    if ($config -and $config.scheduling) {
        $scheduleTime = $config.scheduling.time
        $enabled = $config.scheduling.enabled
        
        if ($null -eq $enabled -or $enabled) {
            if ($scheduleTime) {
                # Convert time format from HH:MM:SS to HH:MM AM/PM for Windows Task Scheduler
                $timeParts = $scheduleTime -split ':'
                if ($timeParts.Length -ge 2) {
                    $hour = [int]$timeParts[0]
                    $minute = [int]$timeParts[1]
                    
                    if ($hour -eq 0) {
                        $hour = 12
                        $ampm = "AM"
                    } elseif ($hour -lt 12) {
                        $ampm = "AM"
                    } elseif ($hour -eq 12) {
                        $ampm = "PM"
                    } else {
                        $hour = $hour - 12
                        $ampm = "PM"
                    }
                    
                    $scheduleTimeFormatted = "$hour`:$minute$ampm"
                    Write-Host "Schedule time from config.json: $scheduleTime -> $scheduleTimeFormatted" -ForegroundColor Green
                }
            }
        } else {
            Write-Host "WARNING: Scheduling is disabled in config.json!" -ForegroundColor Yellow
            Write-Host "Please set 'scheduling.enabled' to 'true' in config.json" -ForegroundColor Yellow
        }
    } else {
        Write-Host "WARNING: No 'scheduling' section found in config.json, using default time 1:00 PM" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "Error reading schedule time from config: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Using default time 1:00 PM" -ForegroundColor Yellow
}

$TaskName = "DataMerge_Daily_Processing"

# Remove existing task if it exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task '$TaskName'..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the scheduled task
$Action = New-ScheduledTaskAction -Execute $BatchPath -WorkingDirectory $WorkingDir
$Trigger = New-ScheduledTaskTrigger -Daily -At $scheduleTimeFormatted
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2)

# Use Interactive logon type for user tasks (doesn't require admin, but user must be logged in)
# For tasks that run when user is NOT logged in, use LogonType Password (requires admin)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

# Try to register the task
try {
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description "Daily data merge processing (runs automatically even when editor is closed)" -ErrorAction Stop
    Write-Host ""
    Write-Host "="*60 -ForegroundColor Green
    Write-Host "Scheduled task '$TaskName' created successfully!" -ForegroundColor Green
    Write-Host "The task will run daily at $scheduleTimeFormatted" -ForegroundColor Green
    Write-Host ""
    Write-Host "IMPORTANT: This task will run automatically even when you close the editor!" -ForegroundColor Cyan
    $taskCreated = $true
}
catch {
    Write-Host ""
    Write-Host "="*60 -ForegroundColor Red
    Write-Host "ERROR: Failed to create scheduled task!" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    Write-Host "SOLUTION: Run PowerShell as Administrator:" -ForegroundColor Yellow
    Write-Host "  1. Right-click PowerShell in Start menu" -ForegroundColor Yellow
    Write-Host "  2. Select 'Run as Administrator'" -ForegroundColor Yellow
    Write-Host "  3. Navigate to: $WorkingDir" -ForegroundColor Yellow
    Write-Host "  4. Run: .\setup_scheduler.ps1" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Alternatively, create the task manually in Task Scheduler:" -ForegroundColor Yellow
    Write-Host "  1. Open Task Scheduler (taskschd.msc)" -ForegroundColor Yellow
    Write-Host "  2. Create Basic Task" -ForegroundColor Yellow
    Write-Host "  3. Program: $BatchPath" -ForegroundColor Yellow
    Write-Host "  4. Working Directory: $WorkingDir" -ForegroundColor Yellow
    Write-Host "  5. Daily trigger at: $scheduleTimeFormatted" -ForegroundColor Yellow
    Write-Host "="*60 -ForegroundColor Red
    $taskCreated = $false
}

if ($taskCreated) {
    Write-Host ""
    Write-Host "To view or modify the task:" -ForegroundColor Yellow
    Write-Host "  1. Open Task Scheduler (taskschd.msc)" -ForegroundColor Yellow
    Write-Host "  2. Look for '$TaskName' in the Task Scheduler Library" -ForegroundColor Yellow
    Write-Host "="*60 -ForegroundColor Green
    
    # Optional: Test the task immediately
    Write-Host ""
    $TestRun = Read-Host "Do you want to test run the task now? (y/n)"
    if ($TestRun -eq "y" -or $TestRun -eq "Y") {
        try {
            Start-ScheduledTask -TaskName $TaskName
            Write-Host "Task started. Check the log files for results." -ForegroundColor Green
        }
        catch {
            Write-Host "Could not start task: $($_.Exception.Message)" -ForegroundColor Yellow
            Write-Host "You can manually run it from Task Scheduler." -ForegroundColor Yellow
        }
    }
}


