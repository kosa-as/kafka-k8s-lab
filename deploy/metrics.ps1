$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$StrimziRoot = Join-Path $RepoRoot 'strimzi'
$MetricRoot = Join-Path $RepoRoot 'metric'

kubectl apply -f (Join-Path $MetricRoot '00-kafka-metrics-config.yaml')
kubectl apply -f (Join-Path $StrimziRoot '20-kafka.yaml')
kubectl wait --for=condition=Ready kafka/kafka -n kafka --timeout=900s

kubectl apply -f (Join-Path $MetricRoot '10-prometheus-config.yaml')
kubectl apply -f (Join-Path $MetricRoot '20-prometheus.yaml')
kubectl rollout status deployment/prometheus -n kafka --timeout=180s
kubectl get deployment,svc -n kafka -l app=prometheus -o wide
