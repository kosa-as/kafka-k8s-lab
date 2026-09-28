$ErrorActionPreference = 'Stop'

& (Join-Path (Split-Path -Parent $PSScriptRoot) 'deploy\kafka.ps1')
