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
        containers = @(@{ name = 'kafka'; image = 'quay.io/strimzi/kafka:1.2.0-kafka-4.3.1' })
        volumes = @(@{ name = 'broker-runtime-logs'; emptyDir = @{} })
    }
}
$Result = $Probe | ConvertTo-Json -Depth 10 | kubectl create --dry-run=server -f - -o json
if ($LASTEXITCODE -ne 0) { throw 'Admission dry-run failed' }
$Pod = $Result | ConvertFrom-Json
if (@($Pod.spec.containers | Where-Object { $_.name -eq 'log-agent' }).Count -ne 1) {
    throw 'Ready webhook did not inject log-agent: check TLS certificate and API server CA trust'
}
Write-Host 'PASS: API server trusts the webhook and injects log-agent (server dry-run, no Pod created).'
