param(
    [Parameter(Mandatory)][string]$BrokerLibraries,
    [Parameter(Mandatory)][string]$OutputJar,
    [string]$AsmJar = (Join-Path $env:USERPROFILE '.m2/repository/org/ow2/asm/asm/9.8/asm-9.8.jar')
)

$ErrorActionPreference = 'Stop'
$BrokerLibraries = (Resolve-Path -LiteralPath $BrokerLibraries).Path
$AsmJar = (Resolve-Path -LiteralPath $AsmJar).Path
$OutputJar = [IO.Path]::GetFullPath($OutputJar)
$stage = Join-Path (Split-Path -Parent $OutputJar) ('agent-build-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $stage | Out-Null
$sources = @(Get-ChildItem -LiteralPath $PSScriptRoot -Filter '*.java' | ForEach-Object FullName)
& javac --release 17 -cp "$BrokerLibraries/*;$AsmJar" -d $stage @sources
if ($LASTEXITCODE -ne 0) { throw 'Agent compilation failed.' }
Push-Location -LiteralPath $stage
try {
    & jar xf $AsmJar org/objectweb/asm
    if ($LASTEXITCODE -ne 0) { throw 'ASM extraction failed.' }
} finally {
    Pop-Location
}
$manifest = Join-Path $stage 'agent.mf'
[IO.File]::WriteAllText($manifest, "Manifest-Version: 1.0`nPremain-Class: lab.health.StallAgent`n`n")
& jar cfm $OutputJar $manifest -C $stage lab -C $stage org
if ($LASTEXITCODE -ne 0) { throw 'Agent packaging failed.' }
Get-FileHash -LiteralPath $OutputJar -Algorithm SHA256
