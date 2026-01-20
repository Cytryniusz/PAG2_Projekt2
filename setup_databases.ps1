# Portable MongoDB and Redis - no admin rights needed
# Downloads and runs databases in project directory

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Portable MongoDB and Redis for Windows" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

$projectDir = $PSScriptRoot
$portableDir = Join-Path $projectDir "portable_databases"
$mongoDir = Join-Path $portableDir "mongodb"
$redisDir = Join-Path $portableDir "redis"
$dataDir = Join-Path $portableDir "data"
$mongoDataDir = Join-Path $dataDir "mongodb"
$redisDataDir = Join-Path $dataDir "redis"

# Create directories
Write-Host "Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path $portableDir | Out-Null
New-Item -ItemType Directory -Force -Path $mongoDataDir | Out-Null
New-Item -ItemType Directory -Force -Path $redisDataDir | Out-Null
Write-Host "[OK] Directories created" -ForegroundColor Green
Write-Host ""

# Download function
function Download-File {
    param($url, $output)
    Write-Host "  Downloading: $url" -ForegroundColor Gray
    try {
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $url -OutFile $output -UseBasicParsing
        Write-Host "  [OK] Downloaded" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "  [ERROR] Download failed: $_" -ForegroundColor Red
        return $false
    }
}

# MongoDB Portable
Write-Host "Checking MongoDB..." -ForegroundColor Yellow
$mongoExe = Join-Path $mongoDir "bin\mongod.exe"

if (-not (Test-Path $mongoExe)) {
    Write-Host "MongoDB not installed. Downloading..." -ForegroundColor Yellow

    # MongoDB Community Server (portable zip)
    $mongoUrl = "https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-7.0.5.zip"
    $mongoZip = Join-Path $portableDir "mongodb.zip"

    if (Download-File $mongoUrl $mongoZip) {
        Write-Host "  Extracting MongoDB..." -ForegroundColor Yellow
        Expand-Archive -Path $mongoZip -DestinationPath $portableDir -Force

        # Find extracted directory and rename
        $extractedDir = Get-ChildItem -Path $portableDir -Filter "mongodb-*" -Directory | Select-Object -First 1
        if ($extractedDir) {
            Move-Item -Path $extractedDir.FullName -Destination $mongoDir -Force
            Write-Host "  [OK] MongoDB installed" -ForegroundColor Green
        }

        Remove-Item $mongoZip -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "[WARNING] Failed to download MongoDB" -ForegroundColor Yellow
        Write-Host "You can download manually from: https://www.mongodb.com/try/download/community" -ForegroundColor Yellow
    }
} else {
    Write-Host "[OK] MongoDB already installed" -ForegroundColor Green
}
Write-Host ""

# Redis Portable
Write-Host "Checking Redis..." -ForegroundColor Yellow
$redisExe = Join-Path $redisDir "redis-server.exe"

if (-not (Test-Path $redisExe)) {
    Write-Host "Redis not installed. Downloading..." -ForegroundColor Yellow

    # Redis for Windows (unofficial port)
    $redisUrl = "https://github.com/tporadowski/redis/releases/download/v5.0.14.1/Redis-x64-5.0.14.1.zip"
    $redisZip = Join-Path $portableDir "redis.zip"

    if (Download-File $redisUrl $redisZip) {
        Write-Host "  Extracting Redis..." -ForegroundColor Yellow
        Expand-Archive -Path $redisZip -DestinationPath $redisDir -Force
        Write-Host "  [OK] Redis installed" -ForegroundColor Green
        Remove-Item $redisZip -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "[WARNING] Failed to download Redis" -ForegroundColor Yellow
        Write-Host "You can download manually from: https://github.com/tporadowski/redis/releases" -ForegroundColor Yellow
    }
} else {
    Write-Host "[OK] Redis already installed" -ForegroundColor Green
}
Write-Host ""

# Start MongoDB
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Starting databases..." -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

if (Test-Path $mongoExe) {
    Write-Host "Starting MongoDB..." -ForegroundColor Yellow

    # Check if already running
    try {
        $mongoProcess = Get-Process mongod -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*$mongoDir*" }
        if ($mongoProcess) {
            Write-Host "[INFO] MongoDB already running (PID: $($mongoProcess.Id))" -ForegroundColor Yellow
        } else {
            # Start MongoDB in background
            $mongoArgs = "--dbpath `"$mongoDataDir`" --port 27017"
            Start-Process -FilePath $mongoExe -ArgumentList $mongoArgs -WindowStyle Hidden
            Start-Sleep -Seconds 3
            Write-Host "[OK] MongoDB started (port 27017)" -ForegroundColor Green
        }
    } catch {
        Write-Host "[ERROR] Problem starting MongoDB: $_" -ForegroundColor Red
    }
} else {
    Write-Host "[ERROR] MongoDB not installed" -ForegroundColor Red
}

# Start Redis
if (Test-Path $redisExe) {
    Write-Host "Starting Redis..." -ForegroundColor Yellow

    # Check if already running
    try {
        $redisProcess = Get-Process redis-server -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*$redisDir*" }
        if ($redisProcess) {
            Write-Host "[INFO] Redis already running (PID: $($redisProcess.Id))" -ForegroundColor Yellow
        } else {
            # Start Redis in background
            $redisConfig = Join-Path $redisDir "redis.windows.conf"
            if (Test-Path $redisConfig) {
                Start-Process -FilePath $redisExe -ArgumentList $redisConfig -WindowStyle Hidden -WorkingDirectory $redisDir
            } else {
                Start-Process -FilePath $redisExe -WindowStyle Hidden -WorkingDirectory $redisDir
            }
            Start-Sleep -Seconds 2
            Write-Host "[OK] Redis started (port 6379)" -ForegroundColor Green
        }
    } catch {
        Write-Host "[ERROR] Problem starting Redis: $_" -ForegroundColor Red
    }
} else {
    Write-Host "[ERROR] Redis not installed" -ForegroundColor Red
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Done!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "MongoDB and Redis are running in background." -ForegroundColor White
Write-Host "Data is stored in:" -ForegroundColor White
Write-Host "  $dataDir" -ForegroundColor Gray
Write-Host ""
Write-Host "To run the application:" -ForegroundColor Cyan
Write-Host "   python main_gui.py" -ForegroundColor Gray
Write-Host ""
