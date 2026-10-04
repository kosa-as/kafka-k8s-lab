$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$StrimziRoot = Join-Path $RepoRoot 'strimzi'
$CertDir = Join-Path ([System.IO.Path]::GetTempPath()) `
  ('kafka-log-sidecar-' + [guid]::NewGuid().ToString('N'))
$CertPath = Join-Path $CertDir 'tls.crt'
$KeyPath = Join-Path $CertDir 'tls.key'
$WebhookTemplate = Join-Path $StrimziRoot '60-kafka-log-sidecar-injector.yaml'
$WebhookManifest = Join-Path $CertDir '60-kafka-log-sidecar-injector.yaml'

New-Item -ItemType Directory -Path $CertDir | Out-Null
try {
    Write-Host 'Building the local injector image...'
    docker build -t kafka-log-sidecar-injector:local `
      (Join-Path $StrimziRoot 'log-sidecar-injector')

    Write-Host 'Creating the broker runtime log PVCs...'
    kubectl apply -f (Join-Path $StrimziRoot '40-kafka-runtime-log-pvcs.yaml')
    foreach ($NodeId in 0..2) {
        kubectl wait --for=jsonpath='{.status.phase}'=Bound `
          "pvc/kafka-runtime-logs-$NodeId" `
          -n kafka `
          --timeout=120s
    }

    Write-Host 'Creating the webhook certificate...'
    $OpensslExe = (Get-Command openssl -ErrorAction Stop).Source
    $OpensslHome = Split-Path (Split-Path $OpensslExe -Parent) -Parent
    $OpensslConfig = Join-Path $OpensslHome 'ssl\openssl.cnf'
    if (-not (Test-Path -LiteralPath $OpensslConfig)) {
        throw "OpenSSL configuration was not found at $OpensslConfig"
    }

    & openssl req -x509 -nodes -newkey rsa:2048 `
      -config $OpensslConfig `
      -keyout $KeyPath `
      -out $CertPath `
      -days 365 `
      -subj '/CN=kafka-log-sidecar-injector.kafka.svc' `
      -addext 'subjectAltName=DNS:kafka-log-sidecar-injector.kafka.svc,DNS:kafka-log-sidecar-injector.kafka.svc.cluster.local' `
      2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "openssl failed with exit code $LASTEXITCODE"
    }

    kubectl -n kafka create secret tls kafka-log-sidecar-injector-tls `
      --cert=$CertPath `
      --key=$KeyPath `
      --dry-run=client -o yaml | kubectl apply -f -

    $CaBundle = [Convert]::ToBase64String(
      [System.IO.File]::ReadAllBytes($CertPath)
    )
    (Get-Content -Raw $WebhookTemplate).Replace(
      'caBundle: ""',
      "caBundle: $CaBundle"
    ) | Set-Content -NoNewline $WebhookManifest

    kubectl apply -f $WebhookManifest
    kubectl -n kafka rollout status deployment/kafka-log-sidecar-injector --timeout=120s

    $EndpointReady = $false
    for ($Attempt = 0; $Attempt -lt 60; $Attempt++) {
        $Slices = kubectl get endpointslices -n kafka -l 'kubernetes.io/service-name=kafka-log-sidecar-injector' -o json | ConvertFrom-Json
        foreach ($Slice in @($Slices.items)) {
            $ReadyEndpoints = @($Slice.endpoints | Where-Object { $_.conditions.ready -eq $true -and @($_.addresses).Count -gt 0 })
            if ($ReadyEndpoints.Count -gt 0) { $EndpointReady = $true; break }
        }
        if ($EndpointReady) { break }
        Start-Sleep -Seconds 2
    }
    if (-not $EndpointReady) { throw 'kafka-log-sidecar-injector Service has no ready EndpointSlice address' }

    Write-Host 'Applying Kafka runtime logging configuration...'
    kubectl apply -f (Join-Path $StrimziRoot '50-kafka-runtime-log-config.yaml')
    kubectl apply -f (Join-Path $StrimziRoot '10-kafka-node-pool.yaml')
    kubectl apply -f (Join-Path $StrimziRoot '20-kafka.yaml')

    # The webhook only runs when a Pod is created. Recreate brokers that were
    # started before the webhook so they receive the log-agent sidecar.
    $KafkaPods = kubectl get pods -n kafka -l 'strimzi.io/cluster=kafka,strimzi.io/component-type=kafka' -o json | ConvertFrom-Json
    foreach ($Pod in $KafkaPods.items) {
        $HasLogAgent = @($Pod.spec.containers | Where-Object { $_.name -eq 'log-agent' }).Count -gt 0
        if (-not $HasLogAgent) {
            Write-Host "Recreating $($Pod.metadata.name) to inject log-agent..."
            kubectl delete pod $Pod.metadata.name -n kafka --wait=false
        }
    }

    Write-Host 'Waiting for Kafka reconciliation...'
    kubectl wait kafka/kafka -n kafka --for=condition=Ready --timeout=300s
    kubectl wait --for=condition=Ready pod `
      -l 'strimzi.io/cluster=kafka,strimzi.io/component-type=kafka' `
      -n kafka `
      --timeout=300s
    kubectl -n kafka get pods,pvc -o wide
}
finally {
    Remove-Item -LiteralPath $CertDir -Recurse -Force -ErrorAction SilentlyContinue
}
