$ErrorActionPreference = 'Stop'

& (Join-Path (Split-Path -Parent $PSScriptRoot) 'deploy\logging.ps1')
