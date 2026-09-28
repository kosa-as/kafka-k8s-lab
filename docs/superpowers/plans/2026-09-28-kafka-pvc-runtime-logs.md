# Kafka PVC Runtime Logs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist each broker's runtime log files on an independent PVC, retain stdout, inject a file-reading sidecar, and increase Kafka data PVCs to 40 GiB.

**Architecture:** Strimzi supplies the Kafka Log4j2 ConfigMap, per-node templated PVC mounts, and an opt-in Pod annotation. A small HTTPS Mutating Admission Webhook adds a read-only `log-agent` sidecar to matching Kafka Pods. The sidecar tails the shared log file to its own stdout.

**Tech Stack:** Kubernetes YAML, Strimzi Kafka/KafkaNodePool CRs, Python standard-library HTTPS webhook, Docker Desktop image, PowerShell verification.

**Spec:** `docs/superpowers/specs/2026-09-28-kafka-pvc-runtime-logs-design.md`

## Global Constraints

- Runtime log rotation is 100 MiB per active file with 10 compressed archives per broker.
- Kafka stdout logging remains enabled.
- Each broker gets an independent log PVC; no broker shares a log PVC with another broker.
- Kafka data PVC desired size is 40Gi and `deleteClaim: false` remains enabled.
- The webhook only targets explicitly annotated Kafka Pods in namespace `kafka`.

---

### Task 1: Add broker logging and PVC configuration

**Files:**
- Modify: `strimzi/10-kafka-node-pool.yaml`
- Modify: `strimzi/20-kafka.yaml`
- Create: `strimzi/40-kafka-runtime-log-pvcs.yaml`
- Create: `strimzi/50-kafka-runtime-log-config.yaml`

- [x] Change the KafkaNodePool data storage size from `20Gi` to `40Gi`.
- [x] Add the per-node log PVC volume template and Kafka container mount at `/mnt/kafka-runtime-logs`.
- [x] Add the opt-in annotation used by the injector.
- [x] Add three 2Gi `hostpath` PVCs with `ReadWriteOnce` and `deleteClaim` behavior represented by a retain policy in the manifest.
- [x] Add external Log4j2 properties with Console and RollingFile appenders, including 100MiB rotation and 10 compressed archives.
- [x] Reference the logging ConfigMap from the Kafka custom resource.

### Task 2: Implement the Mutating Admission Webhook

**Files:**
- Create: `strimzi/log-sidecar-injector/app.py`
- Create: `strimzi/log-sidecar-injector/Dockerfile`
- Create: `strimzi/60-kafka-log-sidecar-injector.yaml`

- [ ] Implement an HTTPS AdmissionReview v1 handler that only mutates Pods with the explicit annotation and Kafka cluster label.
- [x] Inject one `log-agent` container with a read-only `/mnt/kafka-runtime-logs` mount and a `tail -F` command.
- [x] Make the patch idempotent when the sidecar already exists.
- [x] Add a ServiceAccount, Deployment, Service, and MutatingWebhookConfiguration with a namespace selector for `kafka`.
- [x] Keep webhook failure policy `Ignore` because logging injection is optional and must not prevent Kafka recovery.

### Task 3: Add local deployment and certificate helper

**Files:**
- Create: `strimzi/install-runtime-log-sidecar.ps1`
- Modify: `strimzi/README.md`

- [x] Build the injector image in the Docker Desktop context.
- [x] Generate a self-signed certificate with the Service DNS name, create the TLS Secret, and apply the webhook manifests with a matching CA bundle.
- [x] Apply the PVC, logging, and Kafka resources in dependency order.
- [x] Document the install, rollout, log inspection, and cleanup commands.

### Task 4: Verify the live cluster

- [x] Validate all YAML documents with `kubectl apply --dry-run=server` where supported.
- [x] Apply the manifests and wait for Kafka readiness and Pod rollouts.
- [x] Confirm data PVC resize behavior and record the actual capacity.
- [x] Confirm three log PVCs are Bound, all broker Pods have the sidecar, and files are written.
- [x] Confirm both `kubectl logs -c kafka` and `kubectl logs -c log-agent` return runtime log lines.
- [x] Record any hostpath expansion limitation in `.planning/2026-09-28-kafka-pvc-runtime-logs/progress.md`.
