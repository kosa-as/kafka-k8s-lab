"""Temporarily load the M0 lab agent on the highest ordinal, then restore the StatefulSet."""

import argparse
import json
from pathlib import Path
import time
import uuid

from validate_health import command, save


STS = "bitnami-kafka-controller"
POD = f"{STS}-2"
KUBE = ["kubectl", "--context", "docker-desktop", "-n", "kafka", "--request-timeout=8s"]
VOLUME = "m0-validation-agent"
MOUNT_PATH = "/opt/m0-validation"
AGENT_ENV = {"name": "KAFKA_OPTS", "value": f"-javaagent:{MOUNT_PATH}/agent.jar"}
STRATEGY = {"type": "RollingUpdate", "rollingUpdate": {"partition": 2}}


def kube(*parts):
    return command([*KUBE, *parts])


def get(kind, name):
    return json.loads(kube("get", kind, name, "-o", "json"))


def patch(folder, name, operations):
    path = folder / f"{name}.json"
    save(path, operations)
    kube("patch", "statefulset", STS, "--type=json", "--patch-file", str(path))


def container(pod):
    return next(c for c in pod["spec"]["containers"] if c["name"] == "kafka")


def ready_pod(old_uid, agent):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            pod = get("pod", POD)
            ready = any(c["type"] == "Ready" and c["status"] == "True"
                        for c in pod["status"].get("conditions", []))
            env = container(pod).get("env", [])
            if (pod["metadata"]["uid"] != old_uid and ready
                    and (AGENT_ENV in env) == agent):
                return pod
        except RuntimeError:
            pass  # The old Pod can disappear briefly during replacement.
        time.sleep(2)
    raise RuntimeError("Broker 2 did not finish the expected rolling replacement.")


def install(folder, agent):
    folder.mkdir(parents=True)
    sts = get("statefulset", STS)
    pod = get("pod", POD)
    if sts["spec"]["replicas"] != 3 or sts["status"].get("readyReplicas") != 3:
        raise RuntimeError("This laboratory rollout requires all three Kafka replicas Ready.")
    if sts["status"].get("currentRevision") != sts["status"].get("updateRevision"):
        raise RuntimeError("Kafka has a rollout in progress.")
    spec = sts["spec"]["template"]["spec"]
    index = next(i for i, c in enumerate(spec["containers"]) if c["name"] == "kafka")
    if any(e["name"] == AGENT_ENV["name"] for e in spec["containers"][index]["env"]):
        raise RuntimeError("Kafka already has KAFKA_OPTS, which requires a manual merge.")
    if any(v["name"] == VOLUME for v in spec["volumes"]):
        raise RuntimeError("A previous validation rollout must be restored first.")
    cm = "m0-validation-agent-" + uuid.uuid4().hex[:8]
    strategy = sts["spec"]["updateStrategy"]
    save(folder / "restore.json", {"resource": cm, "strategy": strategy, "uid": pod["metadata"]["uid"]})
    save(folder / "original-probes.json", {name: container(pod)[name]
                                         for name in ("readinessProbe", "livenessProbe", "startupProbe")})
    kube("create", "configmap", cm, f"--from-file=agent.jar={agent.resolve()}")
    prefix = f"/spec/template/spec/containers/{index}"
    patch(folder, "install", [
        {"op": "test", "path": "/metadata/resourceVersion", "value": sts["metadata"]["resourceVersion"]},
        {"op": "replace", "path": "/spec/updateStrategy", "value": STRATEGY},
        {"op": "add", "path": f"{prefix}/env/-", "value": AGENT_ENV},
        {"op": "add", "path": f"{prefix}/volumeMounts/-",
         "value": {"name": VOLUME, "mountPath": MOUNT_PATH, "readOnly": True}},
        {"op": "add", "path": "/spec/template/spec/volumes/-", "value": {"name": VOLUME, "configMap": {"name": cm}}},
    ])
    installed = ready_pod(pod["metadata"]["uid"], True)
    logs = kube("logs", POD, "-c", "kafka")
    if "HEALTH_STALL_AGENT_READY" not in logs:
        raise RuntimeError("The new Kafka JVM did not record the agent startup marker.")
    verify_probes(folder, installed)
    print(f"Agent loaded on {POD}. Broker image and probe definitions verified.", flush=True)


def verify_probes(folder, pod):
    original = json.loads((folder / "original-probes.json").read_text(encoding="utf-8"))
    if any(container(pod)[name] != value for name, value in original.items()):
        raise RuntimeError("The Kafka probe definitions differ from the native baseline.")


def restore(folder):
    saved = json.loads((folder / "restore.json").read_text(encoding="utf-8"))
    sts = get("statefulset", STS)
    spec = sts["spec"]["template"]["spec"]
    index = next(i for i, c in enumerate(spec["containers"]) if c["name"] == "kafka")
    kafka = spec["containers"][index]
    pod = get("pod", POD)
    if AGENT_ENV in kafka.get("env", []):
        prefix = f"/spec/template/spec/containers/{index}"
        mount_index = next(i for i, m in enumerate(kafka["volumeMounts"]) if m["name"] == VOLUME)
        volume_index = next(i for i, v in enumerate(spec["volumes"]) if v["name"] == VOLUME)
        patch(folder, "remove-agent", [
            {"op": "test", "path": "/metadata/resourceVersion", "value": sts["metadata"]["resourceVersion"]},
            {"op": "test", "path": f"/spec/template/spec/volumes/{volume_index}/configMap/name",
             "value": saved["resource"]},
            {"op": "remove", "path": f"{prefix}/env/{kafka['env'].index(AGENT_ENV)}"},
            {"op": "remove", "path": f"{prefix}/volumeMounts/{mount_index}"},
            {"op": "remove", "path": f"/spec/template/spec/volumes/{volume_index}"},
        ])
        pod = ready_pod(pod["metadata"]["uid"], False)
    elif AGENT_ENV in container(pod).get("env", []):
        pod = ready_pod(pod["metadata"]["uid"], False)
    verify_probes(folder, pod)
    sts = get("statefulset", STS)
    if sts["spec"]["updateStrategy"] != saved["strategy"]:
        patch(folder, "restore-strategy", [
            {"op": "test", "path": "/metadata/resourceVersion", "value": sts["metadata"]["resourceVersion"]},
            {"op": "replace", "path": "/spec/updateStrategy", "value": saved["strategy"]},
        ])
    kube("delete", "configmap", saved["resource"], "--ignore-not-found")
    print("Restored the original StatefulSet and removed the temporary agent ConfigMap.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "restore"))
    parser.add_argument("state", type=Path)
    parser.add_argument("--agent", type=Path)
    args = parser.parse_args()
    if args.action == "install":
        if args.agent is None or not args.agent.is_file():
            parser.error("Installation requires the packaged agent JAR.")
        if args.state.exists():
            parser.error("Use a fresh state directory for a new installation.")
        try:
            install(args.state, args.agent)
        except (Exception, KeyboardInterrupt):
            if (args.state / "restore.json").is_file():
                restore(args.state)
            raise
    else:
        restore(args.state)


if __name__ == "__main__":
    main()
