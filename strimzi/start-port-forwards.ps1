$ErrorActionPreference = 'Stop'

$Namespace = 'kafka'
$Forwards = @(
    @{ Service = 'kafka-kafka-external-bootstrap'; LocalPort = 40000; RemotePort = 9094 },
    @{ Service = 'kafka-kafka-0'; LocalPort = 40001; RemotePort = 9094 },
    @{ Service = 'kafka-kafka-1'; LocalPort = 40002; RemotePort = 9094 },
    @{ Service = 'kafka-kafka-2'; LocalPort = 40003; RemotePort = 9094 }
)

$jobs = foreach ($forward in $Forwards) {
    Start-Job -ArgumentList $Namespace, $forward.Service, $forward.LocalPort, $forward.RemotePort -ScriptBlock {
        param($Namespace, $Service, $LocalPort, $RemotePort)
        kubectl port-forward --namespace $Namespace "service/$Service" "$LocalPort`:$RemotePort"
    }
}

try {
    Start-Sleep -Seconds 2
    $failed = @($jobs | Where-Object State -ne 'Running')
    if ($failed.Count -gt 0) {
        $failed | Receive-Job
        throw 'One or more Kafka port forwards failed to start.'
    }

    Write-Host 'Kafka port forwards are running. Press Ctrl+C to stop them.'
    while ($true) {
        Start-Sleep -Seconds 1
        $failed = @($jobs | Where-Object State -ne 'Running')
        if ($failed.Count -gt 0) {
            $failed | Receive-Job
            throw 'A Kafka port forward stopped unexpectedly.'
        }
    }
}
finally {
    $jobs | Stop-Job -ErrorAction SilentlyContinue
    $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
}
