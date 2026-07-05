# Emerging Edge watchdog — Windows equivalent of watchdog.sh
# Run with: powershell -ExecutionPolicy Bypass -File watchdog.ps1
#
# Mirrors the Mac watchdog.sh: health-check loop that restarts the
# server if it stops responding. Logs to watchdog.log next to this
# script (equivalent of /tmp/emerging-edge-watchdog.log on Mac).

$ErrorActionPreference = "SilentlyContinue"
$Port = 8878
$HealthUrl = "http://127.0.0.1:$Port/api/status"
$RepoDir = $PSScriptRoot
$Log = Join-Path $RepoDir "emerging-edge.log"
$WatchdogLog = Join-Path $RepoDir "watchdog.log"

Set-Location $RepoDir

# Load .env if present (simple KEY=VALUE lines).
$envFile = Join-Path $RepoDir ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match "^\s*([^#=]+)=(.*)$") {
            [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
        }
    }
}

function Write-Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $WatchdogLog -Value "[$ts] $msg"
}

function Test-Healthy {
    try {
        $resp = Invoke-WebRequest -Uri $HealthUrl -TimeoutSec 8 -UseBasicParsing
        return $resp.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Start-Server {
    # Kill any stale process already listening on the port.
    $conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        Write-Log "killing stale process on port $Port (pid $($c.OwningProcess))"
        Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
    }
    if ($conns) { Start-Sleep -Seconds 1 }

    Write-Log "starting server"
    Start-Process -FilePath "python" -ArgumentList "monitor.py", "serve" `
        -RedirectStandardOutput $Log -RedirectStandardError $Log `
        -WindowStyle Hidden

    for ($i = 1; $i -le 15; $i++) {
        Start-Sleep -Seconds 1
        if (Test-Healthy) {
            Write-Log "server ready after ${i}s"
            return $true
        }
    }
    Write-Log "server failed to become healthy after 15s"
    return $false
}

Write-Log "watchdog started (pid $PID)"
while ($true) {
    if (-not (Test-Healthy)) {
        Write-Log "unhealthy response, restarting server"
        if (-not (Start-Server)) {
            Write-Log "restart attempt failed"
        }
    }
    Start-Sleep -Seconds 60
}
