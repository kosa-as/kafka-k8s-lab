$ErrorActionPreference = 'Stop'

# Execute the installed probe directly, as kubelet does. Git Bash protocol tests
# alone cannot detect a CRLF shebang inside the Linux image.
docker run --rm --entrypoint /opt/kafka/kafka_liveness.sh `
    -e KAFKA_HEALTH_ORIGINAL_CHECK=/bin/true `
    -e KAFKA_HEALTH_STATUS_FILE=/nonexistent-status kafka-health:local
if ($LASTEXITCODE -ne 0) { throw "Image liveness probe failed with exit code $LASTEXITCODE" }
Write-Host 'PASS: Linux image executes the liveness probe directly.'
