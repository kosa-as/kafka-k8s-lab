$ErrorActionPreference = 'Stop'

# Run the real Helm checks against an installed release and an unreachable URL.
# A download failure must reach the existing-release fallback under PS 5.1.
$Source = Get-Content -Raw (Join-Path $PSScriptRoot '..\dashboard.ps1')
$ParseTokens = $null
$ParseErrors = $null
$null = [System.Management.Automation.Language.Parser]::ParseInput($Source, [ref]$ParseTokens, [ref]$ParseErrors)
if ($ParseErrors.Count -gt 0) {
    throw "Dashboard script failed syntax validation: $($ParseErrors.Message -join '; ')"
}
Write-Host 'PASS: the complete Dashboard script parses successfully.'
$Start = $Source.IndexOf('$HelmExe =')
if ($Start -lt 0) { throw 'Dashboard download block was not found' }
$End = $Source.IndexOf('if ($ChartAvailable) {', $Start)
if ($Start -lt 0 -or $End -lt 0) { throw 'Dashboard download block was not found' }
$DownloadBlock = [scriptblock]::Create($Source.Substring($Start, $End - $Start))
$ChartVersion = '7.14.0'
$ChartUrl = 'https://127.0.0.1:1/unavailable-chart.tgz'
$ChartDir = Join-Path ([IO.Path]::GetTempPath()) ('dashboard test ' + [guid]::NewGuid().ToString('N'))
$ChartPath = Join-Path $ChartDir 'kubernetes-dashboard'
New-Item -ItemType Directory -Path $ChartDir | Out-Null
try {
    New-Item -ItemType Directory -Path $ChartPath -Force | Out-Null
    New-Item -ItemType File -Path (Join-Path $ChartPath 'Chart.yaml') -Force | Out-Null
    $ChartUrl = 'https://127.0.0.1:1/unavailable-chart.tgz'
    . $DownloadBlock
    if (-not $ChartAvailable) { throw 'Existing chart cache was not reported as available' }
    Write-Host 'PASS: existing local chart cache avoids a Helm download.'

    Remove-Item -LiteralPath $ChartPath -Recurse -Force
    $ChartUrl = 'https://127.0.0.1:1/unavailable-chart.tgz'
    . $DownloadBlock
    if (-not $ReleaseExists) { throw 'Test requires the installed kubernetes-dashboard release' }
    if ($ChartAvailable) { throw 'Unreachable chart URL was reported as available' }
    Write-Host 'PASS: failed chart download reaches the installed-release fallback on PS 5.1.'
}
finally {
    Remove-Item -LiteralPath $ChartDir -Recurse -Force -ErrorAction SilentlyContinue
}
