param(
    [string]$BrokerLibraries,
    [Parameter(Mandatory)][string]$OutputJar,
    [string]$AsmJar = (Join-Path $env:USERPROFILE '.m2/repository/org/ow2/asm/asm/9.8/asm-9.8.jar')
)

$ErrorActionPreference = 'Stop'
$AsmJar = (Resolve-Path -LiteralPath $AsmJar).Path
$OutputJar = [IO.Path]::GetFullPath($OutputJar)
$stage = Join-Path (Split-Path -Parent $OutputJar) ('agent-build-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $stage | Out-Null
if (-not $BrokerLibraries) {
    $BrokerLibraries = Join-Path $stage 'broker-libraries'
    New-Item -ItemType Directory -Path $BrokerLibraries | Out-Null
    $ids = @(docker ps --filter 'label=io.kubernetes.container.name=kafka' --filter 'label=io.kubernetes.pod.namespace=kafka' --format '{{.ID}}')
    if (-not $ids) { throw 'No running Docker Desktop Kafka container was found.' }
    $id = $ids[0]
    $paths = @(docker exec $id sh -c 'ls /opt/bitnami/kafka/libs/kafka_2.13-4.0.0.jar /opt/bitnami/kafka/libs/kafka-clients-4.0.0.jar /opt/bitnami/kafka/libs/kafka-server-4.0.0.jar /opt/bitnami/kafka/libs/kafka-storage-4.0.0.jar /opt/bitnami/kafka/libs/scala-library-*.jar')
    if ($LASTEXITCODE -ne 0 -or $paths.Count -ne 5) { throw 'Missing Kafka 4.0.0 broker libraries.' }
    foreach ($path in $paths) {
        docker cp "${id}:$path" (Join-Path $BrokerLibraries (Split-Path -Leaf $path))
        if ($LASTEXITCODE -ne 0) { throw "Could not read $path from the running broker." }
    }
}
$BrokerLibraries = (Resolve-Path -LiteralPath $BrokerLibraries).Path
$sources = @(Get-ChildItem -LiteralPath $PSScriptRoot -Filter '*.java' | ForEach-Object FullName)
& javac --release 17 -cp "$BrokerLibraries/*;$AsmJar" -d $stage @sources
if ($LASTEXITCODE -ne 0) { throw 'Agent compilation failed.' }
Push-Location -LiteralPath $stage
try {
    & jar xf $AsmJar org/objectweb/asm
    if ($LASTEXITCODE -ne 0) { throw 'ASM extraction failed.' }
} finally {
    Pop-Location
}
$manifest = Join-Path $stage 'agent.mf'
[IO.File]::WriteAllText($manifest, "Manifest-Version: 1.0`nPremain-Class: lab.m1.ObserverAgent`n`n")
& jar cfm $OutputJar $manifest -C $stage lab -C $stage org
if ($LASTEXITCODE -ne 0) { throw 'Agent packaging failed.' }
& java -ea -cp $OutputJar lab.m1.DecisionTest
if ($LASTEXITCODE -ne 0) { throw 'M1 decision sequence did not pass.' }
Get-FileHash -LiteralPath $OutputJar -Algorithm SHA256
