$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

& (Join-Path $ScriptRoot 'kafka.ps1')
& (Join-Path $ScriptRoot 'logging.ps1')
& (Join-Path $ScriptRoot 'metrics.ps1')
& (Join-Path $ScriptRoot 'dashboard.ps1')

Write-Host 'Full Kafka platform deployment completed.'
