param(
    [switch]$InstallOnly,
    [switch]$ReconcileOnly
)

$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$StrimziRoot = Join-Path $RepoRoot 'strimzi'
$CertDir = Join-Path ([System.IO.Path]::GetTempPath()) ('kafka-log-sidecar-' + [guid]::NewGuid().ToString('N'))
$CertPath = Join-Path $CertDir 'tls.crt'
$KeyPath = Join-Path $CertDir 'tls.key'
$WebhookTemplate = Join-Path $StrimziRoot '60-kafka-log-sidecar-injector.yaml'
$WebhookManifest = Join-Path $CertDir '60-kafka-log-sidecar-injector.yaml'
$WebhookTlsSecret = 'kafka-log-sidecar-injector-tls'

function Test-KafkaPodShape {
    param($Pod)
    $kafka = @($Pod.spec.containers | Where-Object { $_.name -eq 'kafka' })
    $logAgent = @($Pod.spec.containers | Where-Object { $_.name -eq 'log-agent' })
    $healthSidecar = @($Pod.spec.containers | Where-Object { $_.name -eq 'health-sidecar' })
    $healthVolume = @($Pod.spec.volumes | Where-Object { $_.name -eq 'kafka-health' })
    if ($kafka.Count -ne 1 -or $logAgent.Count -ne 1 -or $healthSidecar.Count -ne 1 -or $healthVolume.Count -ne 1) { return $false }
    $kafkaMount = @($kafka[0].volumeMounts | Where-Object { $_.name -eq 'kafka-health' -and $_.mountPath -eq '/var/run/kafka-health' })
    $healthMount = @($healthSidecar[0].volumeMounts | Where-Object { $_.name -eq 'kafka-health' -and $_.mountPath -eq '/var/run/kafka-health' })
    return ($kafka[0].image -eq 'kafka-health:local' -and $kafkaMount.Count -eq 1 -and $kafkaMount[0].readOnly -eq $true -and $healthMount.Count -eq 1 -and $healthMount[0].readOnly -ne $true -and $healthVolume[0].emptyDir -ne $null)
}

function Reconcile-KafkaPods {
    $pods = kubectl get pods -n kafka -l 'strimzi.io/cluster=kafka,strimzi.io/component-type=kafka' -o json | ConvertFrom-Json
    foreach ($Pod in @($pods.items)) {
        if (Test-KafkaPodShape $Pod) { continue }
        Write-Host "Recreating $($Pod.metadata.name) to apply the custom image and health injection..."
        kubectl delete pod $Pod.metadata.name -n kafka --wait=true
        kubectl wait --for=condition=Ready "pod/$($Pod.metadata.name)" -n kafka --timeout=600s
        $replacement = kubectl get pod $Pod.metadata.name -n kafka -o json | ConvertFrom-Json
        if (-not (Test-KafkaPodShape $replacement)) { throw "Pod $($Pod.metadata.name) is Ready but does not have the required Kafka health shape" }
    }
}

function Wait-WebhookAdmission {
    # Verify actual injection through the API server, including its TLS trust.
    # The dry-run creates no real Pod and includes the Kafka opt-in annotation.
    Write-Host 'Verifying webhook admission with a server-side dry-run...'
    $Probe = @{
        apiVersion = 'v1'
        kind = 'Pod'
        metadata = @{
            name = 'kafka-webhook-admission-probe'
            namespace = 'kafka'
            labels = @{ 'strimzi.io/cluster' = 'kafka' }
            annotations = @{ 'kafka.strimzi.io/log-sidecar' = 'enabled' }
        }
        spec = @{
            containers = @(@{ name = 'kafka'; image = 'kafka-health:local' })
            volumes = @(@{ name = 'broker-runtime-logs'; emptyDir = @{} })
        }
    }

    $LastDiagnostic = 'Webhook did not inject the required Kafka health shape'
    for ($Attempt = 0; $Attempt -lt 30; $Attempt++) {
        try {
            $Result = $Probe | ConvertTo-Json -Depth 10 | kubectl create --dry-run=server -f - -o json 2>&1
            if ($LASTEXITCODE -ne 0) { throw "Webhook admission dry-run failed: $Result" }
            $AdmittedPod = $Result | ConvertFrom-Json
            if (Test-KafkaPodShape $AdmittedPod) { return }
            $LastDiagnostic = 'Webhook did not inject the required Kafka health shape'
        } catch {
            # With failurePolicy=Fail, TLS/endpoint propagation failures are
            # nonzero exits (or NativeCommandError on Windows PowerShell 5.1).
            $LastDiagnostic = $_.Exception.Message
        }
        Start-Sleep -Seconds 2
    }
    throw "Webhook admission failed after 30 attempts; check its TLS certificate and API server CA trust. Last error: $LastDiagnostic"
}

if ($ReconcileOnly) { Reconcile-KafkaPods; exit 0 }

New-Item -ItemType Directory -Path $CertDir | Out-Null
try {
    Write-Host 'Building the local injector image...'
    docker build -t kafka-log-sidecar-injector:local (Join-Path $StrimziRoot 'log-sidecar-injector')
    if ($LASTEXITCODE -ne 0) { throw "docker build failed with exit code $LASTEXITCODE" }

    Write-Host 'Creating the broker runtime log PVCs...'
    kubectl apply -f (Join-Path $StrimziRoot '40-kafka-runtime-log-pvcs.yaml')
    foreach ($NodeId in 0..2) {
        kubectl wait --for=jsonpath='{.status.phase}'=Bound "pvc/kafka-runtime-logs-$NodeId" -n kafka --timeout=120s
    }

    Write-Host 'Preparing the webhook certificate...'
    $OpensslExe = (Get-Command openssl -ErrorAction Stop).Source
    $OpensslHome = Split-Path (Split-Path $OpensslExe -Parent) -Parent
    $OpensslConfig = Join-Path $OpensslHome 'ssl\openssl.cnf'
    if (-not (Test-Path -LiteralPath $OpensslConfig)) { throw "OpenSSL configuration was not found at $OpensslConfig" }
    $ExistingSecretJson = kubectl get secret $WebhookTlsSecret -n kafka -o json --ignore-not-found
    $ReusedCertificate = $false
    if ($LASTEXITCODE -eq 0 -and $ExistingSecretJson) {
        try {
            $ExistingSecret = $ExistingSecretJson | ConvertFrom-Json
            if ($ExistingSecret.data.'tls.crt' -and $ExistingSecret.data.'tls.key') {
                [System.IO.File]::WriteAllBytes($CertPath, [Convert]::FromBase64String($ExistingSecret.data.'tls.crt'))
                [System.IO.File]::WriteAllBytes($KeyPath, [Convert]::FromBase64String($ExistingSecret.data.'tls.key'))
                $ReusedCertificate = $true
                Write-Host 'Reusing the existing webhook certificate to avoid an unnecessary CA rotation.'
            }
        } catch {
            Write-Host 'Existing webhook TLS Secret was invalid; generating a replacement certificate.'
        }
    }
    if (-not $ReusedCertificate) {
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
        kubectl -n kafka create secret tls $WebhookTlsSecret --cert=$CertPath --key=$KeyPath --dry-run=client -o yaml | kubectl apply -f -
        if ($LASTEXITCODE -ne 0) { throw "kubectl failed to apply the webhook TLS Secret with exit code $LASTEXITCODE" }
    }

    $CaBundle = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($CertPath))
    (Get-Content -Raw $WebhookTemplate).Replace('caBundle: ""', "caBundle: $CaBundle") | Set-Content -NoNewline $WebhookManifest
    kubectl apply -f $WebhookManifest
    if ($LASTEXITCODE -ne 0) { throw "kubectl failed to apply the webhook configuration with exit code $LASTEXITCODE" }
    # The TLS context is loaded only at startup, so an existing process must
    # restart before it can serve an updated certificate (or rebuilt image).
    kubectl rollout restart deployment/kafka-log-sidecar-injector -n kafka
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
    Wait-WebhookAdmission
    if ($InstallOnly) { exit 0 }

    Write-Host 'Building the custom Kafka image...'
    docker build -t kafka-health:local (Join-Path $StrimziRoot 'kafka-image')
    if ($LASTEXITCODE -ne 0) { throw "docker build failed with exit code $LASTEXITCODE" }
    Write-Host 'Applying Kafka runtime logging and CR resources...'
    kubectl apply -f (Join-Path $StrimziRoot '50-kafka-runtime-log-config.yaml')
    kubectl apply -f (Join-Path $StrimziRoot '10-kafka-node-pool.yaml')
    kubectl apply -f (Join-Path $StrimziRoot '20-kafka.yaml')
    kubectl wait kafka/kafka -n kafka --for=condition=Ready --timeout=900s
    kubectl wait --for=condition=Ready pod -l 'strimzi.io/cluster=kafka,strimzi.io/component-type=kafka' -n kafka --timeout=900s
    Reconcile-KafkaPods
    kubectl -n kafka get pods,pvc -o wide
}
finally { Remove-Item -LiteralPath $CertDir -Recurse -Force -ErrorAction SilentlyContinue }
