$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Commit = 'bb5e98a6e3c5ca196420d2414769b681c9284101'
$ChartDir = Join-Path $RepoRoot "bitnami\.helm-cache\kafka-32.5.0\$Commit\charts-$Commit\bitnami\kafka"
$Values = Join-Path $RepoRoot 'bitnami\helm-values.yaml'
if (-not (Test-Path -LiteralPath (Join-Path $ChartDir 'Chart.yaml'))) {
    throw 'Pinned chart cache is missing. Run deploy/kafka.ps1 once to download it.'
}

$Rendered = helm template bitnami-kafka $ChartDir --namespace kafka --values $Values
if ($LASTEXITCODE -ne 0) { throw 'Helm template failed.' }
$Text = $Rendered -join "`n"
$Required = @(
    'docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10',
    'docker.io/bitnamilegacy/jmx-exporter:1.4.0-debian-12-r0',
    'name: bitnami-kafka-controller-0-external',
    'name: bitnami-kafka-controller-1-external',
    'name: bitnami-kafka-controller-2-external',
    'nodePort: 32000',
    'nodePort: 32001',
    'nodePort: 32002',
    'nodePort: 32003',
    'name: log-agent',
    'name: health-sidecar',
    'name: bitnami-kafka-jmx',
    'name: bitnami-kafka-log4j2',
    'name: bitnami-kafka-kraft-static',
    'storage: "20Gi"',
    'storage: "2Gi"',
    'process.roles=controller,broker',
    'CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT'
)
foreach ($Needle in $Required) {
    if (-not $Text.Contains($Needle)) { throw "Rendered chart is missing: $Needle" }
}
Write-Host 'PASS: pinned Bitnami chart renders Kafka, metrics, sidecars, PVCs and NodePorts.'
