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

    # OpenSSL writes key-generation progress to stderr even on success.
    # Direct stderr redirection can throw NativeCommandError on Windows PS 5.1.
    $OpensslArguments = @(
        'req', '-x509', '-nodes', '-newkey', 'rsa:2048',
        '-config', ('"{0}"' -f $OpensslConfig),
        '-keyout', ('"{0}"' -f $KeyPath),
        '-out', ('"{0}"' -f $CertPath),
        '-days', '365',
        '-subj', '/CN=kafka-log-sidecar-injector.kafka.svc',
        '-addext', 'subjectAltName=DNS:kafka-log-sidecar-injector.kafka.svc,DNS:kafka-log-sidecar-injector.kafka.svc.cluster.local'
    )
    $OpensslErrorPath = Join-Path $CertDir 'openssl.stderr.log'
    $OpensslProcess = Start-Process -FilePath $OpensslExe `
      -ArgumentList $OpensslArguments -NoNewWindow -Wait -PassThru `
      -RedirectStandardError $OpensslErrorPath
    if ($OpensslProcess.ExitCode -ne 0) {
        $OpensslError = Get-Content -Raw -LiteralPath $OpensslErrorPath
        throw "openssl failed with exit code $($OpensslProcess.ExitCode): $OpensslError"
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
    # The TLS context is loaded only at startup; updating the Secret alone does
    # not update the certificate served by an existing injector process.
    kubectl -n kafka rollout restart deployment/kafka-log-sidecar-injector
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

    # Prove that API server CA propagation and admission are working before
    # recreating any brokers. failurePolicy=Ignore otherwise hides TLS failures.
    Write-Host 'Verifying webhook admission with a server-side dry-run...'
    $Probe = @{
        apiVersion = 'v1'
        kind = 'Pod'
        metadata = @{
            name = 'kafka-log-injection-probe'
            namespace = 'kafka'
            labels = @{ 'strimzi.io/cluster' = 'kafka' }
            annotations = @{ 'kafka.strimzi.io/log-sidecar' = 'enabled' }
        }
        spec = @{
            containers = @(@{ name = 'kafka'; image = 'quay.io/strimzi/kafka:1.2.0-kafka-4.3.1' })
            volumes = @(@{ name = 'broker-runtime-logs'; emptyDir = @{} })
        }
    }
    $AdmissionReady = $false
    for ($Attempt = 0; $Attempt -lt 60; $Attempt++) {
        $Result = $Probe | ConvertTo-Json -Depth 10 | kubectl create --dry-run=server -f - -o json
        if ($LASTEXITCODE -ne 0) { throw 'Webhook admission dry-run failed' }
        $AdmittedPod = $Result | ConvertFrom-Json
        if (@($AdmittedPod.spec.containers | Where-Object { $_.name -eq 'log-agent' }).Count -eq 1) {
            $AdmissionReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $AdmissionReady) { throw 'Webhook did not inject log-agent; check its TLS certificate and API server CA trust' }

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
