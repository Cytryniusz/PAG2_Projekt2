# Stop MongoDB and Redis

Write-Host "Stopping databases..." -ForegroundColor Yellow

# Stop MongoDB
$mongoProcess = Get-Process mongod -ErrorAction SilentlyContinue
if ($mongoProcess) {
    Stop-Process -Name mongod -Force
    Write-Host "[OK] MongoDB stopped" -ForegroundColor Green
} else {
    Write-Host "[INFO] MongoDB was not running" -ForegroundColor Gray
}

# Stop Redis
$redisProcess = Get-Process redis-server -ErrorAction SilentlyContinue
if ($redisProcess) {
    Stop-Process -Name redis-server -Force
    Write-Host "[OK] Redis stopped" -ForegroundColor Green
} else {
    Write-Host "[INFO] Redis was not running" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Done!" -ForegroundColor Green
