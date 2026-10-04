$ErrorActionPreference = 'Stop'

# Dry-run admission exercises the API server's CA trust and the running webhook.
# No Pod is persisted. A ready HTTPS health check alone cannot catch stale TLS.
$Probe = @{
    apiVersion = 'v1'
    kind = 'Pod'
    metadata = @{
        name = 'kafka-log-injection-test'
        namespace = 'kafka'
        labels = @{ 'strimzi.io/cluster' = 'kafka' }
        annotations = @{ 'kafka.strimzi.io/log-sidecar' = 'enabled' }
    }
    spec = @{
        containers = @(@{ name = 'kafka'; image = 'kafka-health:local' })
        volumes = @(@{ name = 'broker-runtime-logs'; emptyDir = @{} })
    }
}
$Result = $Probe | ConvertTo-Json -Depth 10 | kubectl create --dry-run=server -f - -o json
if ($LASTEXITCODE -ne 0) { throw 'Admission dry-run failed' }
$Pod = $Result | ConvertFrom-Json
if (@($Pod.spec.containers | Where-Object { $_.name -eq 'log-agent' }).Count -ne 1) {
    throw 'Ready webhook did not inject log-agent: check TLS certificate and API server CA trust'
}
$Health = @($Pod.spec.containers | Where-Object { $_.name -eq 'health-sidecar' })
$Kafka = @($Pod.spec.containers | Where-Object { $_.name -eq 'kafka' })
$Volume = @($Pod.spec.volumes | Where-Object { $_.name -eq 'kafka-health' })
if ($Health.Count -ne 1 -or $Volume.Count -ne 1 -or $null -eq $Volume[0].emptyDir) {
    throw 'Webhook did not inject health-sidecar and its emptyDir'
}
$KafkaMount = @($Kafka[0].volumeMounts | Where-Object { $_.name -eq 'kafka-health' })
$HealthMount = @($Health[0].volumeMounts | Where-Object { $_.name -eq 'kafka-health' })
if ($KafkaMount.Count -ne 1 -or $KafkaMount[0].readOnly -ne $true -or
    $HealthMount.Count -ne 1 -or $HealthMount[0].readOnly -eq $true) {
    throw 'Health directory mounts must be read-only for Kafka and writable for health-sidecar'
}
Write-Host 'PASS: API server trusts the webhook and injects both sidecars and the health mounts (no Pod created).'
