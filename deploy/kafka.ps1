$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$StrimziRoot = Join-Path $RepoRoot 'strimzi'
$MetricRoot = Join-Path $RepoRoot 'metric'
$Namespace = 'kafka'
$StrimziVersion = '1.2.0'

kubectl apply -f (Join-Path $StrimziRoot '00-namespace.yaml')
kubectl apply -f (Join-Path $MetricRoot '00-kafka-metrics-config.yaml')
kubectl apply -f (Join-Path $StrimziRoot '40-kafka-runtime-log-pvcs.yaml')
kubectl apply -f (Join-Path $StrimziRoot '50-kafka-runtime-log-config.yaml')

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

kubectl apply -f (Join-Path $StrimziRoot '10-kafka-node-pool.yaml')
kubectl apply -f (Join-Path $StrimziRoot '20-kafka.yaml')
kubectl apply -f (Join-Path $StrimziRoot '30-topic.yaml')

kubectl wait --for=condition=Ready kafka/kafka -n $Namespace --timeout=900s
kubectl get kafka,kafkanodepool,kafkatopic,pods,pvc,svc -n $Namespace -o wide
