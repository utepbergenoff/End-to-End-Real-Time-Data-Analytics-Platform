$ErrorActionPreference = 'Continue'
$root = $PSScriptRoot
$pidsFile = Join-Path $root 'data\demo-pids.json'
if (Test-Path $pidsFile) {
    foreach ($job in @(Get-Content $pidsFile -Raw | ConvertFrom-Json)) {
        Stop-Process -Id $job.Id -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $pidsFile -ErrorAction SilentlyContinue
}
Push-Location $root
try {
    $docker = (Get-Command docker -ErrorAction SilentlyContinue).Source
    if (-not $docker) { $docker = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe' }
    if (Test-Path $docker) { & $docker compose down }
} finally { Pop-Location }
