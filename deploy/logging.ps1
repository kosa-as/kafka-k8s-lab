$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Config = Join-Path $RepoRoot 'bitnami\10-log4j2-config.yaml'
$ApplyOutput = kubectl apply -f $Config
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply Bitnami Kafka Log4j2 configuration.' }
if ($ApplyOutput -match 'configured') {
    kubectl -n kafka rollout restart statefulset/bitnami-kafka-controller
    if ($LASTEXITCODE -ne 0) { throw 'Failed to restart Bitnami Kafka controllers.' }
}
kubectl -n kafka rollout status statefulset/bitnami-kafka-controller --timeout=15m
if ($LASTEXITCODE -ne 0) { throw 'Bitnami Kafka controllers did not become ready.' }
foreach ($Ordinal in 0..2) {
    $Pod = "bitnami-kafka-controller-$Ordinal"
    kubectl -n kafka exec $Pod -c kafka -- test -f /opt/bitnami/kafka/logs/server.log
    if ($LASTEXITCODE -ne 0) { throw "$Pod did not write server.log." }
    kubectl -n kafka logs $Pod -c log-agent --tail=2
    if ($LASTEXITCODE -ne 0) { throw "$Pod log-agent is not available." }
}
