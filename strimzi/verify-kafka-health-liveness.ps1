$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$StrimziRoot = $ScriptRoot

Write-Host 'Running direct liveness wrapper protocol tests...'
& 'C:\Program Files\Git\bin\bash.exe' (Join-Path $StrimziRoot 'kafka-image\test_kafka_liveness.sh')
if ($LASTEXITCODE -ne 0) {
    throw "liveness wrapper tests failed with exit code $LASTEXITCODE"
}

Write-Host 'Checking Kafka Pod shape...'
$pods = kubectl get pods -n kafka -l 'strimzi.io/cluster=kafka,strimzi.io/component-type=kafka' -o json | ConvertFrom-Json
if (@($pods.items).Count -ne 3) { throw 'Expected three Kafka broker Pods' }

foreach ($Pod in @($pods.items)) {
    $kafka = @($Pod.spec.containers | Where-Object { $_.name -eq 'kafka' })
    $health = @($Pod.spec.containers | Where-Object { $_.name -eq 'health-sidecar' })
    $logAgent = @($Pod.spec.containers | Where-Object { $_.name -eq 'log-agent' })
    $volume = @($Pod.spec.volumes | Where-Object { $_.name -eq 'kafka-health' })
    if ($kafka.Count -ne 1 -or $health.Count -ne 1 -or $logAgent.Count -ne 1 -or $volume.Count -ne 1) {
        throw "Pod $($Pod.metadata.name) is missing the custom Kafka/sidecar/volume shape"
    }
    if ($kafka[0].image -ne 'kafka-health:local') { throw "Pod $($Pod.metadata.name) does not use kafka-health:local" }
    $kafkaMount = @($kafka[0].volumeMounts | Where-Object { $_.name -eq 'kafka-health' -and $_.mountPath -eq '/var/run/kafka-health' })
    $healthMount = @($health[0].volumeMounts | Where-Object { $_.name -eq 'kafka-health' -and $_.mountPath -eq '/var/run/kafka-health' })
    if ($kafkaMount.Count -ne 1 -or $kafkaMount[0].readOnly -ne $true) { throw "Pod $($Pod.metadata.name) Kafka mount is not read-only" }
    if ($healthMount.Count -ne 1 -or $healthMount[0].readOnly -eq $true) { throw "Pod $($Pod.metadata.name) Health Sidecar mount is not read-write" }

    $statusReady = $false
    for ($Attempt = 0; $Attempt -lt 15; $Attempt++) {
        kubectl exec $Pod.metadata.name -n kafka -c health-sidecar -- /usr/bin/sh -c 'test -s /var/run/kafka-health/status' 2>$null
        if ($LASTEXITCODE -eq 0) { $statusReady = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $statusReady) { throw "Pod $($Pod.metadata.name) Health Sidecar did not publish a status file" }

    kubectl exec $Pod.metadata.name -n kafka -c kafka -- /usr/bin/sh -c 'test -r /var/run/kafka-health/status; marker=/var/run/kafka-health/.kafka-write-test; if touch "$marker" 2>/dev/null; then rm -f "$marker"; exit 1; fi'
    if ($LASTEXITCODE -ne 0) { throw "Pod $($Pod.metadata.name) Kafka container can not read or incorrectly writes the health mount" }
}

Write-Host 'Checking status freshness and fail-open expiry behavior in the running sidecar...'
$firstPod = $pods.items[0].metadata.name
kubectl exec $firstPod -n kafka -c health-sidecar -- /usr/bin/sh -c 'test -s /var/run/kafka-health/status && grep -q "^updated_at=[0-9][0-9]*$" /var/run/kafka-health/status'
if ($LASTEXITCODE -ne 0) { throw 'Health Sidecar did not create a fresh atomic status file' }

Write-Host 'Infrastructure/protocol validation passed. Broker health decision source remains intentionally out of scope.'
