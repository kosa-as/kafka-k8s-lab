$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$StrimziRoot = Join-Path $RepoRoot 'strimzi'
$MetricRoot = Join-Path $RepoRoot 'metric'
$Namespace = 'kafka'
$StrimziVersion = '1.2.0'
$KafkaImage = 'kafka-health:local'

kubectl apply -f (Join-Path $StrimziRoot '00-namespace.yaml')
kubectl apply -f (Join-Path $MetricRoot '00-kafka-metrics-config.yaml')
kubectl apply -f (Join-Path $StrimziRoot '40-kafka-runtime-log-pvcs.yaml')
kubectl apply -f (Join-Path $StrimziRoot '50-kafka-runtime-log-config.yaml')

Write-Host "Building the custom Kafka image $KafkaImage..."
docker build -t $KafkaImage (Join-Path $StrimziRoot 'kafka-image')
if ($LASTEXITCODE -ne 0) {
    throw "docker build failed with exit code $LASTEXITCODE"
}

helm repo add strimzi https://strimzi.io/charts/ --force-update
helm repo update
helm upgrade --install strimzi-operator strimzi/strimzi-kafka-operator `
  --namespace $Namespace `
  --create-namespace `
  --version $StrimziVersion `
  --values (Join-Path $StrimziRoot 'helm-values.yaml') `
  --wait `
  --timeout 10m

kubectl wait --for=condition=available `
  deployment/strimzi-cluster-operator `
  -n $Namespace `
  --timeout=300s

# Install and verify the mutating webhook before any Kafka Pod can be created.
& (Join-Path $ScriptRoot 'logging.ps1') -InstallOnly

kubectl apply -f (Join-Path $StrimziRoot '10-kafka-node-pool.yaml')
kubectl apply -f (Join-Path $StrimziRoot '20-kafka.yaml')
kubectl apply -f (Join-Path $StrimziRoot '30-topic.yaml')

kubectl wait --for=condition=Ready kafka/kafka -n $Namespace --timeout=900s
kubectl get kafka,kafkanodepool,kafkatopic,pods,pvc,svc -n $Namespace -o wide
