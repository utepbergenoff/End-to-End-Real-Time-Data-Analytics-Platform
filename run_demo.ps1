$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$python = Join-Path $root '..\.venv\Scripts\python.exe'
if (-not (Test-Path $python)) { throw "Expected existing virtual environment: $python" }
$docker = (Get-Command docker -ErrorAction SilentlyContinue).Source
if (-not $docker) { $docker = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe' }
if (-not (Test-Path $docker)) { throw 'Install and start Docker Desktop first.' }
$env:PATH = "$(Split-Path $docker);$env:PATH"
New-Item -ItemType Directory -Force -Path (Join-Path $root 'data'),(Join-Path $root 'artifacts') | Out-Null
Push-Location $root
try {
    & $python app.py train
    & $python app.py evaluate
    & $docker compose up -d broker
    if ($LASTEXITCODE -ne 0) { throw 'Kafka container did not start.' }
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        try { & $python app.py setup-topics 2>$null; if ($LASTEXITCODE -eq 0) { $ready = $true; break } } catch { }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw 'Kafka did not become ready. Check docker compose logs broker.' }
    $jobs = @()
    foreach ($name in @('storage','processor','processor','processor','dashboard')) {
        $index = $jobs.Count
        $jobs += Start-Process -FilePath $python -ArgumentList @('app.py',$name) -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $root "data\$name-$index.out.log") -RedirectStandardError (Join-Path $root "data\$name-$index.err.log")
    }
    $jobs | Select-Object Id,ProcessName | ConvertTo-Json | Set-Content -Path (Join-Path $root 'data\demo-pids.json')
    & $docker compose up -d spark
    if ($LASTEXITCODE -ne 0) { throw 'Spark container did not start.' }
    Start-Sleep -Seconds 8
    & $python app.py produce --minutes 240 --rate 30
    Write-Host 'Dashboard: http://127.0.0.1:8000'
    Write-Host 'Run the benchmark in this terminal with: ..\.venv\Scripts\python.exe app.py benchmark'
} finally { Pop-Location }
