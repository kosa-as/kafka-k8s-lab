# Kafka PVC Runtime Logs Design

## Goal

Keep Kafka broker runtime logs in a per-broker PVC while continuing to emit the same logs to stdout, and inject a sidecar that can read the files from the same Pod.

## Current Context

- The cluster is a three-node Strimzi KRaft cluster in namespace `kafka`.
- Each broker is one Pod: `kafka-kafka-0`, `kafka-kafka-1`, and `kafka-kafka-2`.
- Kafka 4.3.1 runs in the Strimzi `1.2.0` image.
- The `hostpath` StorageClass is used for broker data.
- Kafka runtime logging currently uses only the generated Console appender.

## Design

1. Configure external Log4j2 logging through the Kafka custom resource.
2. Keep the `STDOUT` appender and add a `RollingFile` appender at `/mnt/kafka-runtime-logs/server.log`. Strimzi 1.2.0 requires additional volume mounts to use a path under `/mnt`.
3. Rotate at 100 MiB and keep 10 compressed archives per broker, for approximately 1 GiB of runtime log data per broker.
4. Create three independent 2 GiB PVCs named `kafka-runtime-logs-0`, `kafka-runtime-logs-1`, and `kafka-runtime-logs-2`. The extra capacity allows the active file, compressed archive, and filesystem overhead to coexist without truncating the configured log budget.
5. Use Strimzi templated PVC volumes so node ID `N` mounts `kafka-runtime-logs-N` into `/mnt/kafka-runtime-logs`.
6. Add a Mutating Admission Webhook that selects only Kafka Pods carrying the explicit sidecar annotation and injects a read-only `log-agent` sidecar. The sidecar tails `/mnt/kafka-runtime-logs/server.log` to its own stdout.
7. Build the injector image locally for Docker Desktop and use `IfNotPresent` so the local image is used by Kubernetes.
8. Increase the Kafka data volume request in the KafkaNodePool from 20 GiB to 40 GiB.

## Failure Handling

- The webhook is scoped to the `kafka` namespace and the explicit opt-in annotation.
- `failurePolicy: Ignore` keeps Kafka Pod creation available if the optional log injector is unavailable; the webhook deployment and rollout checks report whether injection occurred.
- The sidecar is read-only against the log PVC and does not write to Kafka data.
- Existing Kafka data PVCs must be expanded in place. If the `hostpath` provisioner does not support expansion, the desired CR size will be recorded but the current bound PVC capacity will remain unchanged until a storage migration is performed.

## Verification

- The KafkaNodePool reports the requested 40Gi data size, and the exact hostpath expansion limitation is reported if existing bound PVC capacity remains 20Gi.
- All three log PVCs are Bound and mounted at `/mnt/kafka-runtime-logs`.
- Each Kafka Pod has `kafka` and `log-agent` containers.
- Log4j2 contains both Console and RollingFile appenders.
- `/mnt/kafka-runtime-logs/server.log` contains broker log lines.
- `kubectl logs` works for both the Kafka and sidecar containers.
- Kafka remains Ready and its three broker endpoints remain scrapeable.
