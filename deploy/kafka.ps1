$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$BitnamiRoot = Join-Path $RepoRoot 'bitnami'
$ChartCommit = 'bb5e98a6e3c5ca196420d2414769b681c9284101'
$ChartCache = Join-Path $BitnamiRoot '.helm-cache\kafka-32.5.0'
$Archive = Join-Path $ChartCache "charts-$ChartCommit.zip"
$ExtractDir = Join-Path $ChartCache $ChartCommit
$ChartDir = Join-Path $ExtractDir "charts-$ChartCommit\bitnami\kafka"
$CommonSource = Join-Path $ExtractDir "charts-$ChartCommit\bitnami\common"
$CommonDependency = Join-Path $ChartDir 'charts\common'

if ((kubectl config current-context) -ne 'docker-desktop') {
    throw 'Expected kubectl context docker-desktop.'
}

if (-not (Test-Path -LiteralPath (Join-Path $ChartDir 'Chart.yaml'))) {
    New-Item -ItemType Directory -Path $ChartCache -Force | Out-Null
    $ChartUrl = "https://codeload.github.com/bitnami/charts/zip/$ChartCommit"
    Write-Host 'Downloading pinned Bitnami Kafka chart source...'
    Invoke-WebRequest -Uri $ChartUrl -OutFile $Archive -TimeoutSec 120
    Expand-Archive -LiteralPath $Archive -DestinationPath $ExtractDir -Force
}
if (-not (Test-Path -LiteralPath (Join-Path $ChartDir 'Chart.yaml'))) {
    throw "Bitnami Kafka chart 32.5.0 was not found in $ExtractDir"
}
if (-not (Test-Path -LiteralPath (Join-Path $CommonDependency 'Chart.yaml'))) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $CommonDependency) -Force | Out-Null
    Copy-Item -LiteralPath $CommonSource -Destination $CommonDependency -Recurse -Force
}

kubectl apply -f (Join-Path $BitnamiRoot '00-namespace.yaml')
if ($LASTEXITCODE -ne 0) { throw 'Failed to create kafka namespace.' }
kubectl apply -f (Join-Path $BitnamiRoot '00-jmx-config.yaml')
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply JMX configuration.' }
kubectl apply -f (Join-Path $BitnamiRoot '10-log4j2-config.yaml')
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply Log4j2 configuration.' }

$StaticSecretName = 'bitnami-kafka-kraft-static'
$StaticSecret = kubectl -n kafka get secret $StaticSecretName --ignore-not-found -o json
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect the KRaft Secret.' }
if ($StaticSecret) {
    $SecretData = ($StaticSecret | ConvertFrom-Json).data
}
else {
    # Reuse the first chart installation's cluster ID when adopting it.
    $PriorSecret = kubectl -n kafka get secret bitnami-kafka-kraft --ignore-not-found -o json
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect the existing KRaft Secret.' }
    if ($PriorSecret) {
        $SecretData = ($PriorSecret | ConvertFrom-Json).data
    }
    else {
        function New-KraftId {
            $Bytes = New-Object byte[] 16
            [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($Bytes)
            [Convert]::ToBase64String($Bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
        }
        $SecretData = @{}
        foreach ($Key in @('cluster-id', 'controller-0-id', 'controller-1-id', 'controller-2-id')) {
            $Value = New-KraftId
            $SecretData[$Key] = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))
        }
    }
}

# Kafka canonicalizes UUIDs when formatting storage. The chart's initially
# generated IDs can therefore differ from the IDs in meta.properties.
$Data = @{}
foreach ($Key in @('cluster-id', 'controller-0-id', 'controller-1-id', 'controller-2-id')) {
    $Data[$Key] = if ($SecretData -is [System.Collections.IDictionary]) {
        $SecretData[$Key]
    }
    else {
        $SecretData.$Key
    }
}
$ClusterId = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($Data['cluster-id']))
for ($i = 0; $i -lt 3; $i++) {
    $Pod = "bitnami-kafka-controller-$i"
    $Phase = kubectl -n kafka get pod $Pod --ignore-not-found -o jsonpath='{.status.phase}'
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect $Pod." }
    if ($Phase -ne 'Running') { continue }
    $Meta = (kubectl -n kafka exec $Pod -c kafka -- cat /bitnami/kafka/data/meta.properties) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw "Could not read storage metadata from $Pod." }
    if ($Meta -notmatch '(?m)^cluster\.id=(.+)$' -or $Matches[1].Trim() -ne $ClusterId) {
        throw "KRaft cluster ID in $Pod does not match the Secret."
    }
    if ($Meta -notmatch '(?m)^directory\.id=(.+)$') { throw "Missing directory ID in $Pod." }
    $Data["controller-$i-id"] = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Matches[1].Trim()))
}
$SecretManifest = @{
    apiVersion = 'v1'
    kind = 'Secret'
    metadata = @{ name = $StaticSecretName; namespace = 'kafka' }
    type = 'Opaque'
    data = $Data
}
$SecretManifest | ConvertTo-Json -Depth 8 | kubectl apply -f - | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply the stable KRaft Secret.' }

helm lint $ChartDir --values (Join-Path $BitnamiRoot 'helm-values.yaml') --quiet
if ($LASTEXITCODE -ne 0) { throw 'Bitnami chart lint failed.' }
helm upgrade --install bitnami-kafka $ChartDir `
  --namespace kafka `
  --values (Join-Path $BitnamiRoot 'helm-values.yaml') `
  --wait `
  --timeout 15m
if ($LASTEXITCODE -ne 0) { throw 'Bitnami Kafka Helm deployment failed.' }

kubectl -n kafka rollout status statefulset/bitnami-kafka-controller --timeout=15m
if ($LASTEXITCODE -ne 0) { throw 'Bitnami Kafka controllers did not become ready.' }

$KafkaCli = '/opt/bitnami/kafka/bin'
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT `
  "$KafkaCli/kafka-topics.sh" --bootstrap-server bitnami-kafka:9092 `
  --create --if-not-exists --topic demo --partitions 3 --replication-factor 3
if ($LASTEXITCODE -ne 0) { throw 'Failed to create demo topic.' }
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT `
  "$KafkaCli/kafka-configs.sh" --bootstrap-server bitnami-kafka:9092 `
  --entity-type topics --entity-name demo --alter `
  --add-config 'retention.ms=604800000,min.insync.replicas=2'
if ($LASTEXITCODE -ne 0) { throw 'Failed to configure demo topic.' }

kubectl -n kafka get pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka -o wide
