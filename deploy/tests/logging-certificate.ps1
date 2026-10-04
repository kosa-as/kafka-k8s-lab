$ErrorActionPreference = 'Stop'

# Execute the production certificate block without touching Docker or Kubernetes.
# Reverting to direct native stderr redirection must fail this test on PS 5.1.
$Tokens = $null
$ParseErrors = $null
$Ast = [Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $PSScriptRoot '..\logging.ps1'), [ref]$Tokens, [ref]$ParseErrors
)
if ($ParseErrors.Count -gt 0) { throw ($ParseErrors | Out-String) }
$Generation = $Ast.Find({ param($Node)
    $Node -is [Management.Automation.Language.IfStatementAst] -and
    $Node.Clauses[0].Item1.Extent.Text -eq '-not $ReusedCertificate'
}, $true)
if ($null -eq $Generation) { throw 'Certificate generation block was not found' }
$Commands = @()
foreach ($Statement in $Generation.Clauses[0].Item2.Statements) {
    if ($Statement.Extent.Text.StartsWith('kubectl ')) { break }
    $Commands += $Statement.Extent.Text
}
$CertificateBlock = [scriptblock]::Create($Commands -join "`n")
$OpensslExe = (Get-Command openssl -ErrorAction Stop).Source
$OpensslHome = Split-Path (Split-Path $OpensslExe -Parent) -Parent
$OpensslConfig = Join-Path $OpensslHome 'ssl\openssl.cnf'
$CertDir = Join-Path ([IO.Path]::GetTempPath()) ('certificate test ' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $CertDir | Out-Null
try {
    $CertPath = Join-Path $CertDir 'tls.crt'
    $KeyPath = Join-Path $CertDir 'tls.key'
    & $CertificateBlock
    $Certificate = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($CertPath)
    if ($Certificate.Subject -ne 'CN=kafka-log-sidecar-injector.kafka.svc') {
        throw "Unexpected certificate subject: $($Certificate.Subject)"
    }
    $San = $Certificate.Extensions | Where-Object { $_.Oid.Value -eq '2.5.29.17' }
    if ($San.Format($false) -notmatch 'kafka-log-sidecar-injector.kafka.svc.cluster.local') {
        throw 'Certificate is missing the service DNS SAN'
    }
    if ((Get-Item -LiteralPath $KeyPath).Length -eq 0) { throw 'Private key is empty' }
    $Certificate.Dispose()
    Write-Host 'PASS: successful OpenSSL stderr does not abort certificate generation (paths with spaces).'

    $KeyPath = Join-Path $CertDir 'missing directory\tls.key'
    $Failure = $null
    try { & $CertificateBlock } catch { $Failure = $_ }
    if ($null -eq $Failure -or $Failure.Exception.Message -notmatch 'openssl failed with exit code [1-9]') {
        throw "Expected a nonzero OpenSSL exit code, got: $Failure"
    }
    if ($Failure.Exception.Message -notmatch 'No such file|cannot find|BIO_new_file') {
        throw "OpenSSL failure lost its diagnostic output: $Failure"
    }
    Write-Host 'PASS: real OpenSSL failures stop deployment and retain diagnostic output.'
}
finally {
    Remove-Item -LiteralPath $CertDir -Recurse -Force -ErrorAction SilentlyContinue
}
