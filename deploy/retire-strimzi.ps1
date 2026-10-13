$ErrorActionPreference = 'Stop'

if ((kubectl config current-context) -ne 'docker-desktop') {
    throw 'Expected kubectl context docker-desktop.'
}
$Ready = kubectl -n kafka get statefulset bitnami-kafka-controller -o jsonpath='{.status.readyReplicas}'
if ($LASTEXITCODE -ne 0 -or $Ready -ne '3') {
    throw 'Bitnami Kafka must have three ready nodes before retiring Strimzi.'
}
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT `
  /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 `
  --describe --topic demo
if ($LASTEXITCODE -ne 0) { throw 'Bitnami demo topic is not ready.' }

# The old topic operator must remove its finalizer while Strimzi is running.
kubectl -n kafka delete kafkatopic demo --ignore-not-found --wait=true --timeout=120s
if ($LASTEXITCODE -ne 0) { throw 'Could not delete the old KafkaTopic CR.' }
kubectl -n kafka delete kafka kafka --ignore-not-found --wait=true --timeout=180s
if ($LASTEXITCODE -ne 0) { throw 'Could not delete the old Kafka CR.' }
kubectl -n kafka delete kafkanodepool kafka --ignore-not-found --wait=true --timeout=120s
if ($LASTEXITCODE -ne 0) { throw 'Could not delete the old KafkaNodePool CR.' }
kubectl -n kafka wait --for=delete pod `
  -l 'strimzi.io/cluster=kafka,strimzi.io/component-type=kafka' --timeout=300s
if ($LASTEXITCODE -ne 0) { throw 'Old Strimzi Kafka pods did not terminate.' }

helm uninstall strimzi-operator --namespace kafka --wait --timeout 5m
if ($LASTEXITCODE -ne 0) { throw 'Could not uninstall Strimzi Operator.' }

$RepoRoot = Split-Path -Parent $PSScriptRoot
kubectl delete -f (Join-Path $RepoRoot 'strimzi\60-kafka-log-sidecar-injector.yaml') --ignore-not-found
if ($LASTEXITCODE -ne 0) { throw 'Could not remove the old logging webhook.' }
kubectl -n kafka delete secret kafka-log-sidecar-injector-tls --ignore-not-found
if ($LASTEXITCODE -ne 0) { throw 'Could not remove the old webhook TLS secret.' }
kubectl -n kafka delete configmap kafka-metrics kafka-runtime-logging --ignore-not-found
if ($LASTEXITCODE -ne 0) { throw 'Could not remove old Strimzi configuration.' }

$OldClaims = @(
  'data-kafka-kafka-0', 'data-kafka-kafka-1', 'data-kafka-kafka-2',
  'kafka-runtime-logs-0', 'kafka-runtime-logs-1', 'kafka-runtime-logs-2'
)
kubectl -n kafka delete pvc $OldClaims --ignore-not-found --wait=true --timeout=180s
if ($LASTEXITCODE -ne 0) { throw 'Could not remove all old Strimzi PVCs.' }

Write-Host 'Strimzi Kafka and its disposable test PVCs have been retired.'
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
