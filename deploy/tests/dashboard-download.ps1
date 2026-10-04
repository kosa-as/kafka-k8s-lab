$ErrorActionPreference = 'Stop'

# Run the real Helm checks against an installed release and an unreachable URL.
# A download failure must reach the existing-release fallback under PS 5.1.
$Source = Get-Content -Raw (Join-Path $PSScriptRoot '..\dashboard.ps1')
$Start = $Source.IndexOf('try {')
if ($Start -lt 0) { throw 'Dashboard download block was not found' }
$Start += 'try {'.Length
$End = $Source.IndexOf('    if ($ChartAvailable)', $Start)
if ($Start -lt 0 -or $End -lt 0) { throw 'Dashboard download block was not found' }
$DownloadBlock = [scriptblock]::Create($Source.Substring($Start, $End - $Start))
$ChartVersion = '7.14.0'
$ChartUrl = 'https://127.0.0.1:1/unavailable-chart.tgz'
$ChartDir = Join-Path ([IO.Path]::GetTempPath()) ('dashboard test ' + [guid]::NewGuid().ToString('N'))
$ChartPath = Join-Path $ChartDir 'kubernetes-dashboard'
New-Item -ItemType Directory -Path $ChartDir | Out-Null
try {
    . $DownloadBlock
    if (-not $ReleaseExists) { throw 'Test requires the installed kubernetes-dashboard release' }
    if ($ChartAvailable) { throw 'Unreachable chart URL was reported as available' }
    Write-Host 'PASS: failed chart download reaches the installed-release fallback on PS 5.1.'
}
finally {
    Remove-Item -LiteralPath $ChartDir -Recurse -Force -ErrorAction SilentlyContinue
}
