# Bitnami Legacy Kafka on Docker Desktop

`helm-values.yaml` deploys three combined KRaft controller/broker nodes using the official archived `bitnamilegacy/kafka:4.0.0-debian-12-r10` image. The chart source is pinned to commit `bb5e98a6e3c5ca196420d2414769b681c9284101` (Kafka chart 32.5.0) and cached under `.helm-cache/` by `deploy/kafka.ps1`. The common subchart is copied from the same source commit. Bitnami Legacy images are archived and receive no updates.

`deploy/kafka.ps1` creates a stable `bitnami-kafka-kraft-static` Secret so repeated Helm upgrades retain the cluster and node IDs. Each node has a new 20Gi `hostpath` data PVC and 2Gi runtime log PVC. The release does not mount or migrate Strimzi PVCs. `demo` is recreated with three partitions, RF3, seven-day retention and min ISR2. Old messages and consumer offsets are intentionally not copied.

The Kafka container writes stdout and a rolling `server.log`. `log-agent` tails that file to its stdout. The existing lab `health-sidecar` status-file behavior is carried forward, while Kafka health is checked by the chart's own probes. The Strimzi admission webhook does not select Bitnami pods.

## Install

```powershell
.\deploy\kafka.ps1
.\deploy\metrics.ps1
```

For the complete platform, use `.\deploy\all.ps1`. The old Strimzi cluster can remain running during validation because the Helm release has different resource names. Run `deploy/retire-strimzi.ps1` only after the new cluster and clients have been verified; it removes the old Strimzi resources and old test PVCs.

## Endpoints

| Client location | Bootstrap address |
| --- | --- |
| Kubernetes | `bitnami-kafka.kafka.svc:9092` |
| Windows host | `localhost:32000` |

The three external broker addresses are `localhost:32001`, `localhost:32002`, `localhost:32003`. Docker Desktop must expose these NodePorts. Internal, controller, and inter-broker listeners are PLAINTEXT, matching this local lab's existing client behavior.

## Checks

```powershell
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 --describe --topic demo
kubectl -n kafka logs bitnami-kafka-controller-0 -c log-agent --tail=20
```

Prometheus continues on the existing `prometheus` Service. Its targets now point to the Bitnami controller headless DNS names on port 9404.
