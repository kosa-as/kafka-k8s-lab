# Kafka Sidecar Liveness Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend Strimzi Kafka liveness checks with a time-bounded Health Sidecar failure signal delivered through a read-only Kafka mount of a shared `emptyDir`.

**Architecture:** Build a custom Kafka image that preserves the original Strimzi liveness script and wraps its exit code with a fresh-status-file check. Inject a shared `emptyDir` and the Health Sidecar into Kafka Pods using the repository's existing Mutating Admission Webhook; Kafka mounts the directory read-only, while the Sidecar writes atomic status updates.

**Health Sidecar image:** Reuse the cluster's existing `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` image. Supply a status-writer command through the Sidecar configuration; the upstream image alone does not implement the status protocol or Broker health calculation.

**Tech Stack:** Strimzi Kafka 1.2.0 / Kafka 4.3.1, Kubernetes Pod `emptyDir`, POSIX shell, Docker, PowerShell deployment scripts, Python `unittest` for injector tests.

**Spec:** `docs/superpowers/specs/2026-10-04-kafka-liveness-sidecar-design.md`

## Global Constraints

- Health status path is `/var/run/kafka-health/status`.
- A `failure` status is valid only when `updated_at` is a parseable Unix epoch timestamp no older than 30 seconds by default.
- `KAFKA_HEALTH_STATUS_TTL_SECONDS` overrides the default only when it is a positive integer.
- Missing, unreadable, malformed, non-failure, or expired Sidecar state falls back to the original Strimzi liveness exit code.
- A fresh Sidecar `failure` can force liveness failure but can never turn an original liveness failure into success.
- This is fail-open: absent/stopped Sidecar or expired state falls back to the original Java-process check, not a Broker business-health check. A sustained failure requires refreshes faster than the TTL (target at most 10 seconds).
- Kafka mounts the health volume read-only; the Health Sidecar mounts it read-write.
- The Broker health state machine is out of scope; its only integration contract is the status file protocol in the spec.

---

### Task 1: Add the liveness status contract and test fixtures

**Files:**
- Create: `strimzi/kafka-image/test_kafka_liveness.sh`
- Modify: `docs/superpowers/specs/2026-10-04-kafka-liveness-sidecar-design.md`
- Test: `strimzi/kafka-image/test_kafka_liveness.sh`

**Interfaces:**
- Consumes: `KAFKA_HEALTH_STATUS_FILE`, `KAFKA_HEALTH_STATUS_TTL_SECONDS`, and a replaceable original-check command from the future wrapper.
- Produces: executable shell fixtures that exercise fresh, stale, malformed, missing, healthy, and original-failure cases.

- [ ] **Step 1: Define the fixture contract**

  Make the test script create a temporary status directory and a fake original liveness command whose exit code is controlled by `ORIGINAL_RC`. Use `updated_at` values derived from `date +%s`, not hard-coded wall-clock dates.

- [ ] **Step 2: Add expected behavior cases**

  The fixture must assert these exact cases:

  ```text
  fresh failure + original success => exit 1
  stale failure + original success => exit 0
  malformed failure + original success => exit 0
  missing status + original success => exit 0
  healthy status + original success => exit 0
  fresh failure + original failure => exit 1
  healthy status + original failure => exit 1
  ```

- [ ] **Step 3: Run the fixture before implementation**

  Run: `bash strimzi/kafka-image/test_kafka_liveness.sh`

  Expected: FAIL because `strimzi/kafka-image/kafka_liveness.sh` does not exist yet.

### Task 2: Implement the custom Kafka liveness wrapper

**Files:**
- Create: `strimzi/kafka-image/kafka_liveness.sh`
- Modify: `strimzi/kafka-image/test_kafka_liveness.sh`

**Interfaces:**
- Consumes: `/opt/kafka/kafka_liveness_strimzi.sh`, `/var/run/kafka-health/status`, and optional `KAFKA_HEALTH_STATUS_TTL_SECONDS`.
- Produces: a probe-compatible executable that preserves the original liveness exit code unless a fresh `status=failure` is present.

- [ ] **Step 1: Implement original-check execution**

  Execute the preserved Strimzi script inside an `if` conditional so shell errexit behavior cannot terminate the wrapper before the status file is checked. Save its exact exit code in `original_rc` and preserve its stdout/stderr.

- [ ] **Step 2: Implement strict status parsing**

  Read only regular files. Extract exactly one `status=` value and one `updated_at=` value. Treat duplicate keys, missing keys, non-integer timestamps, non-positive TTL values, unreadable files, and unknown statuses as non-failure fallback cases.

- [ ] **Step 3: Implement freshness evaluation**

  Use `date +%s` for the current epoch. A failure is fresh only when `0 <= now-updated_at <= ttl`; future timestamps, negative ages, and ages above the TTL are stale and must fall back to `original_rc`. Use the default TTL of 30 seconds when the override is absent or invalid.

- [ ] **Step 4: Preserve final-result precedence**

  Emit a concise diagnostic to stderr when a fresh Sidecar failure overrides a successful original check, then return 1. Otherwise return `original_rc` unchanged.

- [ ] **Step 5: Run the fixture after implementation**

  Run: `bash strimzi/kafka-image/test_kafka_liveness.sh`

  Expected: PASS for all seven cases.

### Task 3: Package the wrapper into a custom Strimzi Kafka image

**Files:**
- Create: `strimzi/kafka-image/Dockerfile`
- Create: `strimzi/kafka-image/README.md`
- Modify: `deploy/kafka.ps1`
- Modify: `strimzi/20-kafka.yaml`

**Interfaces:**
- Consumes: the upstream image `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` and `strimzi/kafka-image/kafka_liveness.sh`.
- Produces: local image `kafka-health:local` with the original script preserved at `/opt/kafka/kafka_liveness_strimzi.sh` and the wrapper installed at `/opt/kafka/kafka_liveness.sh`.

- [ ] **Step 1: Write the Dockerfile**

  The Dockerfile must fail the build if the upstream liveness script is absent, copy it to `/opt/kafka/kafka_liveness_strimzi.sh`, copy the wrapper into `/opt/kafka/kafka_liveness.sh`, and set both scripts executable without changing the upstream entrypoint.

- [ ] **Step 2: Configure the Kafka image and TTL**

  Set the Kafka CR's image to `kafka-health:local`. Set `KAFKA_HEALTH_STATUS_TTL_SECONDS=30` through the Kafka container template so the runtime default is visible in the manifest and can be changed without rebuilding the image.

- [ ] **Step 3: Build before deployment**

  Update `deploy/kafka.ps1` to run `docker build -t kafka-health:local strimzi/kafka-image` before applying the Kafka CR. Keep the image name and upstream tag in one clearly named variable or documented constant.

- [ ] **Step 4: Verify the image layout**

  Run: `docker build -t kafka-health:local strimzi/kafka-image`

  Run: `docker run --rm --entrypoint /usr/bin/sh kafka-health:local -c 'test -x /opt/kafka/kafka_liveness.sh && test -x /opt/kafka/kafka_liveness_strimzi.sh'`

  Expected: both commands exit 0.

### Task 4: Inject the shared health volume and Sidecar mount

**Files:**
- Modify: `strimzi/log-sidecar-injector/app.py`
- Modify: `strimzi/log-sidecar-injector/test_app.py`
- Modify: `strimzi/60-kafka-log-sidecar-injector.yaml`
- Modify: `strimzi/10-kafka-node-pool.yaml`

**Interfaces:**
- Consumes: Kafka Pods selected by the existing annotation and the Health Sidecar image configured by the injector.
- Produces: one `emptyDir` named `kafka-health`, a Kafka read-only mount at `/var/run/kafka-health`, and a Health Sidecar read-write mount at the same path.

- [ ] **Step 1: Extend the admission patch model**

  Add the `kafka-health` volume only when it is absent. Add the Health Sidecar only when a container with the configured health-sidecar name is absent. Keep the existing log-agent injection idempotent.

- [ ] **Step 2: Define the Health Sidecar contract in the injected container**

  Use `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` for the Sidecar, with the status path `/var/run/kafka-health/status`, read-write mount, non-root execution, dropped capabilities, and resource requests/limits. Provide the status-writer command explicitly in the Sidecar configuration (or mount a script); do not assume the upstream image contains it. It must atomically write `status` and `updated_at` at least every 10 seconds, including while a failure persists and after recovery. Provide a controlled test input so one Broker can continuously publish `failure` during the restart test, then return to `healthy`. The source of a real Broker health decision is out of scope and must be connected before claiming functional Broker-health integration; an image name alone is not a producer.

- [ ] **Step 3: Add permission-sensitive tests**

  Extend `test_app.py` to assert the patch contains exactly one `kafka-health` `emptyDir`, the Kafka mount is read-only, the Health Sidecar mount is read-write, and reinvocation does not duplicate either volume or container.

- [ ] **Step 4: Set Pod security-group behavior**

  Configure the Kafka Pod template with a group that allows the non-root Health Sidecar to create and replace files in the `emptyDir`, while retaining Kafka's read-only mount. Avoid sharing the runtime-log PVC permissions with the health directory.

- [ ] **Step 5: Run injector tests**

  Run: `python -m unittest discover -s strimzi/log-sidecar-injector -p 'test_*.py' -v`

  Expected: all injector tests pass, including idempotency and mount-mode assertions.

### Task 5: Add deployment and operational verification

**Files:**
- Modify: `deploy/all.ps1`
- Modify: `deploy/kafka.ps1`
- Modify: `deploy/logging.ps1`
- Modify: `deploy/README.md`
- Modify: `strimzi/README.md`
- Create: `strimzi/verify-kafka-health-liveness.ps1`

**Interfaces:**
- Consumes: the custom image, injected Pod shape, and the Health Sidecar status protocol.
- Produces: repeatable local verification for image build, Pod mounts, status freshness behavior, and Kafka reconciliation.

- [ ] **Step 1: Make deployment ordering explicit**

  Split the existing deployment into prerequisites and Pod creation: create the namespace, ConfigMaps, log PVCs, and Strimzi Operator first; build the custom Kafka image and injector image; install the TLS secret and webhook, then wait for the injector Deployment to be Available and its Service endpoints to exist; only then apply KafkaNodePool and Kafka CR. Update `deploy/all.ps1` and standalone entry points so neither first-time path creates a Kafka Pod before the webhook is ready. Do not rely on `failurePolicy: Ignore` for successful injection.

  For existing Pods, update `deploy/logging.ps1` to detect the complete target shape (custom Kafka image, `log-agent`, Health Sidecar, `kafka-health` volume, and both mount modes), not just `log-agent`. After the webhook is ready and the CR has reconciled, replace missing/mismatched Pods one at a time. Wait for the replacement to become Ready and verify its full shape before replacing the next Pod; fail the deployment on a missed injection. Account for a Strimzi-triggered image rollout by checking the actual Pods again before deciding which ones still need recreation.

- [ ] **Step 2: Add verification commands**

  The verification script must check that each Kafka Pod has the custom Kafka image, the Health Sidecar, the `kafka-health` volume, and the expected read-only/read-write mount modes. It must also execute the Kafka container to confirm it can read but cannot create a marker file in `/var/run/kafka-health`.

- [ ] **Step 3: Document failure and expiry tests**

  Separate wrapper semantics from Kubelet restart behavior. For the former, execute the wrapper directly against a single fresh `failure`, stop refreshing, wait beyond the TTL, and confirm fallback to the original exit code. For the latter, use the controlled Sidecar test input on one Broker only to keep writing fresh `failure` at intervals no greater than 10 seconds; observe the 10-second liveness probe reach its current failure threshold of 3 and restart the Kafka container. A single write can expire before three failed probes and is not a valid restart test. Restore healthy status and confirm normal probing; never manipulate Kafka data volumes for cleanup. Document explicitly that a stopped Sidecar fails open to the original Java-process check.

- [ ] **Step 4: Run end-to-end validation**

  Run: `./deploy/all.ps1`

  Run: `./strimzi/verify-kafka-health-liveness.ps1`

  Expected: Kafka reaches Ready, all broker Pods have the required image, containers, volume, and mounts; direct wrapper tests demonstrate override-then-fallback, and the controlled continuously refreshed failure test demonstrates the Kubelet restart threshold. Without a connected Broker health decision source, report protocol/infrastructure validation only, not end-to-end Broker-health detection.

## Completion Review

- [ ] Run the shell fixture and Python injector tests.
- [ ] Build and inspect the custom Kafka image.
- [ ] Deploy and verify all broker Pods.
- [ ] Confirm the Health Sidecar refreshes `updated_at` atomically and clears stale failures by writing current state.
- [ ] Confirm Sidecar interruption/expiry falls back to the original check, while continuously refreshed failure crosses the Kubelet restart threshold on a single Broker.
- [ ] Update the design and plan only if the actual Strimzi image or Operator schema differs from the documented assumptions.
