$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$BitnamiRoot = Join-Path $RepoRoot 'bitnami'
$MetricRoot = Join-Path $RepoRoot 'metric'

$ApplyOutput = kubectl apply -f (Join-Path $BitnamiRoot '00-jmx-config.yaml')
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply Bitnami Kafka JMX configuration.' }
if ($ApplyOutput -match 'configured') {
    kubectl -n kafka rollout restart statefulset/bitnami-kafka-controller
    if ($LASTEXITCODE -ne 0) { throw 'Failed to restart Bitnami Kafka controllers.' }
}
kubectl -n kafka rollout status statefulset/bitnami-kafka-controller --timeout=15m
if ($LASTEXITCODE -ne 0) { throw 'Bitnami Kafka controllers did not become ready.' }

kubectl apply -f (Join-Path $MetricRoot '10-prometheus-config.yaml')
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply Prometheus configuration.' }
kubectl apply -f (Join-Path $MetricRoot '20-prometheus.yaml')
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply Prometheus Deployment.' }
kubectl -n kafka rollout restart deployment/prometheus
if ($LASTEXITCODE -ne 0) { throw 'Failed to restart Prometheus.' }
kubectl -n kafka rollout status deployment/prometheus --timeout=180s
if ($LASTEXITCODE -ne 0) { throw 'Prometheus did not become ready.' }

kubectl -n kafka get deployment,svc -l app=prometheus -o wide
