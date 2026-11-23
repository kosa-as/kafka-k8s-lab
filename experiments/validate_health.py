"""Collect and replay a Kafka health validation trial using the same inputs for M0 and M1."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from statistics import median
import subprocess
import sys
import threading
import time
import uuid

from replication_path import partition_resource, verify_pod_rules


ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "clients" / "producer" / "target"
JAVA_SOURCE = ROOT / "clients/producer/src/main/java/com/example/kafka/producer/ValidationClient.java"
PROBE_TYPES = ("Readiness", "Liveness")
STALL_SCENARIOS = {"execution-stall": "execution_enter", "storage-stall": "append_enter"}
PORT_SCOPED_ISR = "isr-below-threshold"
ISR_SCENARIOS = {"min-isr": 3, "isr-at-threshold": 2, PORT_SCOPED_ISR: 2}
FIELDS = {
    "partition": ("started_ms", "leader", "isr", "min_isr", "replicas"),
    "send": ("id",),
    "ack": ("id", "elapsed_us", "offset"),
    "send_error": ("id", "elapsed_us", "error"),
    "consume": ("id", "offset"),
    "metadata_error": ("error",),
    "consumer_error": ("error",),
    "control_error": ("error",),
    "producer_ready": (),
    "consumer_ready": (),
}


def now_ms():
    return time.time_ns() // 1_000_000


def command(args, timeout=12):
    result = subprocess.run(args, capture_output=True, encoding="utf-8", errors="replace", timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} exited {result.returncode}: {result.stderr.strip()[-1200:]}")
    return result.stdout


def kubectl(args, *parts):
    return command(["kubectl", "--context", args.context, "--request-timeout=5s",
                    "-n", args.namespace, *parts], timeout=8)


def kafka_command(args, operation, *extra):
    return ["java", "-cp", args.classpath, "com.example.kafka.producer.ValidationClient",
            operation, args.bootstrap, args.topic, str(args.partition), *map(str, extra)]


def decode_client(line):
    fields = line.strip().split("\t")
    if len(fields) < 2 or not fields[0].isdigit():
        return None
    timestamp, kind, *values = fields
    names = FIELDS[kind]
    if len(names) != len(values):
        raise ValueError(f"Malformed Kafka event: {line!r}")
    event = dict(zip(names, values))
    for name in ("started_ms", "leader", "min_isr", "replicas", "elapsed_us", "offset"):
        if name in event:
            event[name] = int(event[name])
    if "isr" in event:
        event["isr"] = [] if event["isr"] == "-" else list(map(int, event["isr"].split(",")))
    return dict(event, kind=kind, at_ms=int(timestamp))


class Trial:
    def __init__(self, folder, run_id):
        self.folder = folder
        self.run_id = run_id
        self.lock = threading.Lock()
        self.error = None
        self.events = []
        self.file = (folder / "events.jsonl").open("x", encoding="utf-8")

    def emit(self, kind, at_ms=None, **fields):
        with self.lock:
            event = {"at_ms": now_ms() if at_ms is None else at_ms, "run_id": self.run_id,
                     "kind": kind, **fields}
            self.file.write(json.dumps(event, ensure_ascii=False) + "\n")
            self.file.flush()
            self.events.append(event)

    def latest(self):
        with self.lock:
            return {event["kind"]: event for event in self.events}

    def worker(self, name, args):
        stderr = (self.folder / f"{name}.stderr.log").open("x", encoding="utf-8")
        try:
            process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr,
                                       text=True, encoding="utf-8", errors="replace")
        finally:
            stderr.close()

        def read():
            try:
                for line in process.stdout:
                    event = decode_client(line)
                    if event:
                        self.emit(**event)
            except Exception as failure:
                self.error = failure
                self.emit("collector_error", source=name, error=str(failure))

        reader = threading.Thread(target=read, name=name)
        reader.start()
        return process, reader


def pod_sample(args):
    started = now_ms()
    data = json.loads(kubectl(args, "get", "pod", args.pod, "-o", "json"))
    container = next(status for status in data["status"]["containerStatuses"] if status["name"] == "kafka")
    ready = next(item for item in data["status"]["conditions"] if item["type"] == "Ready")
    return dict(started_ms=started, at_ms=now_ms(), uid=data["metadata"]["uid"],
                node=data["spec"]["nodeName"], container_id=container["containerID"],
                restart_count=container["restartCount"], ready=ready["status"] == "True",
                ready_transition=ready["lastTransitionTime"])


def probe_sample(args):
    started = now_ms()
    lines = kubectl(args, "get", f"--raw=/api/v1/nodes/{args.node}/proxy/metrics/probes").splitlines()
    counts = {}
    for line in lines:
        if not line.startswith("prober_probe_total{"):
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', line))
        if labels.get("namespace") != args.namespace or labels.get("pod") != args.pod:
            continue
        if labels.get("container") != "kafka" or labels.get("pod_uid") != args.uid:
            continue
        counts.setdefault(labels["probe_type"], {})[labels["result"]] = int(line.rsplit(maxsplit=1)[1])
    if any(kind not in counts for kind in PROBE_TYPES):
        raise ValueError("Kubelet did not expose both Kafka running probe counters for the initial Pod UID.")
    return dict(started_ms=started, at_ms=now_ms(), uid=args.uid, counts=counts)


def resource_sample(args):
    data = json.loads(kubectl(args, "get", f"--raw=/api/v1/nodes/{args.node}/proxy/stats/summary"))
    pod = next(p for p in data["pods"] if p["podRef"]["uid"] == args.uid)
    return {"uid": args.uid, "containers": {
        c["name"]: {"cpu_at": c.get("cpu", {}).get("time"),
                    "cpu_cores": (value / 1e9 if (value := c.get("cpu", {}).get("usageNanoCores")) is not None else None),
                    "memory_at": c.get("memory", {}).get("time"),
                    "working_set_mib": (value / 2**20 if (value := c.get("memory", {}).get("workingSetBytes")) is not None else None)}
        for c in pod["containers"]}}


def record_clock(args, trial, phase):
    started = now_ms()
    try:
        broker = int(kubectl(args, "exec", args.pod, "-c", "kafka", "--", "date", "+%s%3N").strip())
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as failure:
        trial.emit("collector_error", source="broker_clock", error=str(failure))
        return
    finished = now_ms()
    trial.emit("broker_clock", phase=phase, host_bounds_ms=[started, finished], broker_ms=broker,
               offset_bounds_ms=[broker - finished, broker - started])


def collect_platform_events(args, trial):
    try:
        output = kubectl(args, "get", "events", "--field-selector", f"involvedObject.uid={args.uid}",
                         "-o", "json")
        save(trial.folder / "kube-events.json", json.loads(output))
    except (RuntimeError, ValueError, subprocess.TimeoutExpired) as failure:
        trial.emit("collector_error", source="kube_events", error=str(failure))


def collect_platform(args, trial, stop):
    def capture(kind, sampler):
        try:
            trial.emit(kind, **sampler(args))
        except Exception as failure:
            trial.emit("collector_error", source=kind, error=str(failure))

    next_resource = 0
    with ThreadPoolExecutor(max_workers=3) as pool:
        while not stop.is_set():
            start = time.monotonic()
            jobs = [pool.submit(capture, "pod", pod_sample), pool.submit(capture, "probe", probe_sample)]
            if args.resource_seconds and start >= next_resource:
                jobs.append(pool.submit(capture, "resource", resource_sample))
                next_resource = start + args.resource_seconds
            for job in jobs:
                job.result()
            stop.wait(max(0, args.poll_seconds - (time.monotonic() - start)))


def wait_for_sources(trial, workers, idle=False):
    deadline = time.monotonic() + 20
    required = {"partition", "pod", "probe", "consumer_ready"}
    if not idle:
        required.update(("ack", "producer_ready", "consume"))
    while time.monotonic() < deadline:
        check_workers(trial, workers)
        if required <= trial.latest().keys():
            return
        time.sleep(0.2)
    raise RuntimeError(f"Startup evidence missing: {sorted(required - trial.latest().keys())}")


def check_workers(trial, workers):
    if trial.error:
        raise trial.error
    if any(process.poll() is not None for process, _ in workers):
        raise RuntimeError("A Kafka worker exited early. See the trial's stderr logs.")


def wait_window(seconds, trial, workers):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        check_workers(trial, workers)
        time.sleep(min(0.2, max(0, deadline - time.monotonic())))


def stop_workers(workers):
    for process, _ in workers:
        if process.poll() is None:
            try:
                process.stdin.write("\n")
                process.stdin.flush()
            except BrokenPipeError:
                pass
    for process, reader in workers:
        try:
            process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        reader.join()
        process.stdin.close()
        process.stdout.close()


def single_follower_partition(args, name):
    leader_label = {"statefulset.kubernetes.io/pod-name": args.pod}
    follower_label = {"statefulset.kubernetes.io/pod-name": args.follower_pod}
    return {
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": "NetworkChaos",
        "metadata": {"name": name, "namespace": args.namespace},
        "spec": {
            "action": "partition", "mode": "one", "direction": "both",
            "duration": f"{args.fault_seconds + 20}s",
            "selector": {"namespaces": [args.namespace], "labelSelectors": follower_label},
            "target": {"mode": "one",
                       "selector": {"namespaces": [args.namespace], "labelSelectors": leader_label}},
        },
    }


def inject_partition(args, trial, name):
    if args.scenario == PORT_SCOPED_ISR:
        resource = partition_resource(name, args.namespace, args.replication_followers,
                                      args.replication_address, args.replication_port, args.fault_seconds + 20)
    else:
        resource = single_follower_partition(args, name)
    save(trial.folder / "injection.json", resource)
    trial.emit("fault_requested", follower_pod=args.follower_pod)
    kubectl(args, "create", "-f", str(trial.folder / "injection.json"))
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        state = json.loads(kubectl(args, "get", "networkchaos", name, "-o", "json"))
        conditions = state.get("status", {}).get("conditions", [])
        if any(condition["type"] == "AllInjected" and condition["status"] == "True"
               for condition in conditions):
            save(trial.folder / "injection-status.json", state["status"])
            if args.scenario == PORT_SCOPED_ISR:
                scopes = {}
                for pod in args.replication_followers:
                    scope = json.loads(kubectl(args, "get", "podnetworkchaos", pod, "-o", "json"))
                    verify_pod_rules(scope, resource)
                    scopes[pod] = scope["spec"]
                save(trial.folder / "applied-replication-rules.json", scopes)
            trial.emit("fault_acknowledged", follower_pod=args.follower_pod)
            return
        time.sleep(1)
    raise RuntimeError("Chaos Mesh did not report that the selected link was isolated.")


def restore_partition(args, trial, name):
    trial.emit("restore_requested")
    kubectl(args, "delete", "networkchaos", name, "--ignore-not-found", "--wait=false")
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        if not kubectl(args, "get", "networkchaos", name, "--ignore-not-found", "-o", "name").strip():
            trial.emit("restore_acknowledged")
            return
        time.sleep(1)
    raise RuntimeError("Chaos Mesh did not remove the experiment resource in time.")


def arm_stall(args, trial):
    stage = STALL_SCENARIOS[args.scenario]
    trial.emit("fault_requested", stage=stage)
    deadline = now_ms() + max(12_000, 4000 * (args.stall_records + 1))
    kubectl(args, "exec", args.pod, "-c", "kafka", "--", "sh", "-c",
            'umask 077; printf "%s %s %s %s %s\\n" "$1" "$2" "$3" "$4" "$5" > /tmp/health-validation-stall.new && '
            'mv /tmp/health-validation-stall.new /tmp/health-validation-stall',
            "sh", args.topic, stage, str(deadline), str(args.stall_seconds * 1000), str(args.stall_records))
    trial.emit("fault_acknowledged", stage=stage)


def disarm_stall(args, trial):
    trial.emit("restore_requested")
    kubectl(args, "exec", args.pod, "-c", "kafka", "--", "rm", "-f",
            "/tmp/health-validation-stall", "/tmp/health-validation-stall.new")
    trial.emit("restore_acknowledged")


def collect_broker_stages(args, trial):
    logs = kubectl(args, "logs", args.pod, "-c", "kafka")
    pods = [event for event in trial.events if event["kind"] == "pod"]
    if pods and pods[-1]["container_id"] != pods[0]["container_id"]:
        try:
            previous = kubectl(args, "logs", args.pod, "-c", "kafka", "--previous")
            logs = previous + "\n" + logs
        except RuntimeError as failure:
            trial.emit("collector_error", source="broker_stage", error=str(failure))
    selected = []
    for line in logs.splitlines():
        if line.startswith("HEALTH_STALL_HOOK_ERROR"):
            trial.emit("collector_error", source="broker_stage", error=line)
        if not line.startswith("HEALTH_STALL\t"):
            continue
        prefix, timestamp, topic, stage, message = line.split("\t")
        if topic == args.topic:
            selected.append(line)
            trial.emit("broker_stage", at_ms=int(timestamp), stage=stage, id=message)
    (trial.folder / "broker-stage.log").write_text("\n".join(selected) + "\n", encoding="utf-8")


def select_m1_target(args, topic):
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,249}", topic):
        raise ValueError("Invalid Kafka topic name.")
    kubectl(args, "exec", args.pod, "-c", "kafka", "--", "sh", "-c",
            'printf "%s" "$1" > /var/run/kafka-health/target.new && '
            'mv /var/run/kafka-health/target.new /var/run/kafka-health/target', "sh", topic)


def parse_m1_round(line):
    content = line.removeprefix("M1_ROUND {").removesuffix("}")
    # Role contains its own comma-separated fields and nested replica sets.
    role = re.search(r"metadata=(Role\[leader=.*?, min=\d+\])", content)
    if role:
        content = content[:role.start(1)] + "ROLE" + content[role.end(1):]
    fields = dict(pair.split("=", 1) for pair in content.split(", "))
    if role:
        fields["metadata"] = role[1]
    return fields


def collect_m1_rounds(args, trial):
    since = datetime.fromtimestamp(trial.latest()["window_start"]["at_ms"] / 1000 - 5, timezone.utc)
    logs = kubectl(args, "logs", args.pod, "-c", "health-sidecar",
                   "--since-time=" + since.isoformat(timespec="seconds"))
    selected = []
    for line in logs.splitlines():
        if not line.startswith("M1_ROUND "):
            continue
        fields = parse_m1_round(line)
        if fields.get("topic") == args.topic:
            selected.append(line)
            trial.emit("m1_round", at_ms=int(fields["time"]), snapshot=fields)
    (trial.folder / "m1-rounds.log").write_text("\n".join(selected) + "\n", encoding="utf-8")
    if not selected:
        raise RuntimeError("No M1 decision rounds for this trial.")


def broker_quorum(args, pod):
    output = kubectl(args, "exec", pod, "-c", "kafka", "--", "env", "-u", "JMX_PORT",
                     "-u", "KAFKA_JMX_OPTS", "/opt/bitnami/kafka/bin/kafka-metadata-quorum.sh",
                     "--bootstrap-server", "localhost:9092", "describe", "--status")
    match = re.search(r"(?m)^LeaderId:\s*(\d+)", output)
    if not match:
        raise RuntimeError("KRaft status did not contain a Controller Leader.")
    return int(match[1])


def record_quorum(args, trial, phase):
    observer = f"bitnami-kafka-controller-{args.observer_port - 32001}"
    try:
        trial.emit("quorum", phase=phase, controller=broker_quorum(args, observer), observer=observer)
    except (RuntimeError, subprocess.TimeoutExpired) as failure:
        trial.emit("collector_error", source="quorum", phase=phase, error=str(failure))


def prepare_replication_path(args, partition, kafka):
    args.replication_followers = [f"bitnami-kafka-controller-{broker}"
                                 for broker in sorted(set(partition["isr"]) - {args.broker})]
    listeners = {item["name"]: item["containerPort"] for item in kafka["ports"]}
    args.replication_port = listeners["interbroker"]
    if listeners["controller"] == args.replication_port or listeners["client"] == args.replication_port:
        raise RuntimeError("The replication listener must be separate from the Controller and client listeners.")
    pod = json.loads(kubectl(args, "get", "pod", args.pod, "-o", "json"))
    args.replication_address = pod["status"]["podIP"]
    properties = kubectl(args, "exec", args.pod, "-c", "kafka", "--", "grep",
                         "-E", "^(listeners|inter.broker.listener.name)=",
                         "/opt/bitnami/kafka/config/server.properties").splitlines()
    if "inter.broker.listener.name=INTERNAL" not in properties or not any(
        line.startswith("listeners=") and f"INTERNAL://:{args.replication_port}" in line.split("=", 1)[1].split(",")
        for line in properties
    ):
        raise RuntimeError("The configured interbroker listener does not match the target port.")


def prepare(args, trial):
    if args.topic is None:
        args.topic = "health-validation-" + trial.run_id
        fixed_broker = 2 if args.scenario in STALL_SCENARIOS or args.group == "M1" else args.broker_id
        placement = [fixed_broker] if fixed_broker is not None else []
        command(kafka_command(args, "create", ISR_SCENARIOS.get(args.scenario, 2), *placement))
        trial.emit("topic_created", topic=args.topic)
    deadline = time.monotonic() + 15
    while True:
        observed = command(kafka_command(args, "describe"))
        partition = next(event for line in observed.splitlines()
                         if (event := decode_client(line)) and event["kind"] == "partition")
        if partition["leader"] >= 0 and len(partition["isr"]) == partition["replicas"]:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("The initial partition did not have all replicas in ISR.")
        time.sleep(0.5)
    args.broker = partition["leader"]
    if not args.pod:
        args.pod = f"bitnami-kafka-controller-{args.broker}"
    if args.broker_id is not None and args.broker_id != args.broker:
        raise RuntimeError(f"Requested Broker {args.broker_id} is not the target Leader {args.broker}.")
    status = pod_sample(args)
    if not status["ready"]:
        raise RuntimeError("The target Kafka Pod must be Ready before starting the validation.")
    args.node, args.uid = status["node"], status["uid"]
    kafka = next(container for container in json.loads(kubectl(args, "get", "pod", args.pod, "-o", "json"))
                 ["spec"]["containers"] if container["name"] == "kafka")
    probes = {name: kafka[name] for name in ("startupProbe", "readinessProbe", "livenessProbe")}
    native = (probes["readinessProbe"].get("tcpSocket", {}).get("port") == "controller"
              and probes["livenessProbe"].get("exec", {}).get("command") == ["pgrep", "-f", "kafka"])
    if (args.group == "M0") != native:
        raise RuntimeError("The selected group does not match the Kafka container's probe configuration.")
    if args.group == "M1":
        mode = kubectl(args, "exec", args.pod, "-c", "kafka", "--", "cat", "/opt/m1/mode").strip()
        if mode != "enforce":
            raise RuntimeError(f"M1 comparison requires enforce mode, found {mode!r}.")
    if args.scenario in STALL_SCENARIOS:
        if args.broker != 2:
            raise RuntimeError("The unique experiment topic must be led by the instrumented Broker 2.")
        if "HEALTH_STALL_AGENT_READY" not in kubectl(args, "logs", args.pod, "-c", "kafka"):
            raise RuntimeError("Install the temporary experiment agent on Broker 2 first.")
    if args.group == "M1":
        if args.broker != 2:
            raise RuntimeError("M1 is installed only on Broker 2.")
        kubectl(args, "exec", args.pod, "-c", "kafka", "--", "sh", "-ec",
                'mkdir /var/run/kafka-health/trial-lock; printf "%s" "$1" > /var/run/kafka-health/trial-lock/owner',
                "sh", trial.run_id)
        args.owns_target = True
        args.previous_target = kubectl(args, "exec", args.pod, "-c", "kafka", "--", "sh", "-c",
                                      "cat /var/run/kafka-health/target 2>/dev/null || echo health-validation-m1-b2").strip()
        select_m1_target(args, args.topic)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            snapshot = kubectl(args, "exec", args.pod, "-c", "kafka", "--", "cat", "/var/run/kafka-health/decision")
            if "topic=" + args.topic in snapshot.splitlines() and "ready=true" in snapshot.splitlines():
                break
            time.sleep(1)
        else:
            raise RuntimeError("M1 did not establish healthy coverage for this target.")
    args.follower_pod = None
    args.replication_followers = []
    if args.scenario in ISR_SCENARIOS:
        if partition["min_isr"] != ISR_SCENARIOS[args.scenario] or partition["replicas"] != 3:
            raise RuntimeError("Unexpected minISR or replica count for the selected follower-loss scenario.")
        if kubectl(args, "get", "networkchaos", "-o", "name").strip():
            raise RuntimeError("Another NetworkChaos exists in the trial namespace.")
        if args.scenario == PORT_SCOPED_ISR:
            prepare_replication_path(args, partition, kafka)
        else:
            controller = broker_quorum(args, args.pod)
            candidates = set(partition["isr"]) - {args.broker, controller}
            if args.observer_port:
                candidates.discard(args.observer_port - 32001)
            if args.follower_id is not None:
                candidates &= {args.follower_id}
            if not candidates:
                raise RuntimeError("No requested follower can be isolated while keeping the observer and controller connected.")
            follower = min(candidates)
            args.follower_pod = f"bitnami-kafka-controller-{follower}"
            if not json.loads(kubectl(args, "get", "pod", args.follower_pod, "-o", "json"))["status"]["containerStatuses"]:
                raise RuntimeError("The selected follower does not have a running Kafka container.")
    return dict(run_id=trial.run_id, group=args.group, scenario=args.scenario, bootstrap=args.bootstrap,
                topic=args.topic, partition=args.partition, target_broker=args.broker, pod=args.pod,
                follower_pod=args.follower_pod,
                replication_followers=args.replication_followers, replication_port=getattr(args, "replication_port", None),
                initial=status, kafka_image=kafka["image"], probes=probes, original_min_isr=partition["min_isr"],
                initial_replicas=partition["replicas"], pair_id=args.pair_id, resource_seconds=args.resource_seconds,
                stall_seconds=args.stall_seconds if args.scenario in STALL_SCENARIOS else None,
                stall_records=args.stall_records if args.scenario in STALL_SCENARIOS else None,
                context=args.context, observer_port=args.observer_port, namespace=args.namespace, load_interval_ms=args.load_interval_ms, producer_retries=0 if args.scenario in STALL_SCENARIOS else 1,
                poll_seconds=args.poll_seconds,
                healthy_seconds=args.healthy_seconds, fault_seconds=args.fault_seconds,
                recovery_seconds=args.recovery_seconds, started_utc=datetime.now(timezone.utc).isoformat(),
                source_commit=command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip(),
                source_hashes={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in (Path(__file__), JAVA_SOURCE, Path(__file__).with_name("replication_path.py"))})


def run(args):
    stage_trial = args.scenario in STALL_SCENARIOS
    if stage_trial and (args.topic or args.partition != 0 or args.context != "docker-desktop"
                        or args.namespace != "kafka" or args.bootstrap != "localhost:32000" or args.pod
                        or args.fault_seconds < 24):
        raise ValueError("The stage pilot requires instrumented Broker 2, a fresh topic and at least 24s.")
    if stage_trial and args.fault_seconds < args.stall_seconds + 4 * (args.stall_records + 1):
        raise ValueError("The fault window must leave time for all selected records to complete their hold.")
    if args.stall_records != 1 and args.scenario != "execution-stall":
        raise ValueError("Multiple held records are supported for execution-stall only.")
    args.load_interval_ms = 4000 if stage_trial else round(1000 / args.rate) if args.rate else 0
    if args.scenario in ISR_SCENARIOS and (args.topic or args.partition != 0 or args.context != "docker-desktop"
                                       or args.namespace != "kafka" or args.bootstrap != "localhost:32000"
                                       or args.pod or args.fault_seconds > 120):
        raise ValueError("The min-isr pilot requires the default Docker Desktop topology, a fresh topic and a bounded fault window.")
    if args.pod and args.broker_id is None:
        raise ValueError("--pod requires --broker-id for independent Leader validation.")
    if args.scenario == PORT_SCOPED_ISR and (args.broker_id != 2 or args.observer_port not in (32001, 32002)
                                            or args.follower_id is not None):
        raise ValueError("The port-scoped trial requires Broker 2 and a separate independent observer.")
    if not args.topic and args.partition != 0:
        raise ValueError("A new experiment topic has only partition 0.")
    args.classpath = os.pathsep.join((str(TARGET / "classes"),
                                     (TARGET / "validation-classpath.txt").read_text(encoding="utf-8").strip()))
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8]
    folder = args.output / run_id
    folder.mkdir(parents=True)
    trial = Trial(folder, run_id)
    print(f"Trial: {folder}", flush=True)
    workers = []
    stop = threading.Event()
    platform = None
    restore_needed = False
    fault_name = "health-validation-" + run_id
    manifest = None
    try:
        manifest = prepare(args, trial)
        save(folder / "run.json", manifest)
        record_clock(args, trial, "before")
        observer = [args.observer_port] if args.observer_port else []
        workers.append(trial.worker("metadata", kafka_command(args, "observe", int(args.poll_seconds * 1000), *observer)))
        workers.append(trial.worker("load", kafka_command(args, "load", args.load_interval_ms, str(stage_trial).lower())))
        platform = threading.Thread(target=collect_platform, args=(args, trial, stop), name="platform")
        platform.start()
        wait_for_sources(trial, workers, idle=args.rate == 0)
        trial.emit("window_start")
        wait_window(args.healthy_seconds, trial, workers)
        if args.scenario in ISR_SCENARIOS:
            if args.scenario == PORT_SCOPED_ISR:
                record_quorum(args, trial, "before")
            restore_needed = True
            inject_partition(args, trial, fault_name)
            print("Follower-to-Leader link isolation confirmed by Chaos Mesh.", flush=True)
            if args.scenario == PORT_SCOPED_ISR:
                fault_deadline = time.monotonic() + args.fault_seconds
                wait_window(args.fault_seconds / 2, trial, workers)
                record_quorum(args, trial, "during")
                wait_window(max(0, fault_deadline - time.monotonic()), trial, workers)
            else:
                wait_window(args.fault_seconds, trial, workers)
            restore_partition(args, trial, fault_name)
            print("Experiment link isolation removed.", flush=True)
            wait_window(args.recovery_seconds, trial, workers)
            if args.scenario == PORT_SCOPED_ISR:
                record_quorum(args, trial, "after")
        if stage_trial:
            restore_needed = True
            arm_stall(args, trial)
            print(f"{args.scenario} armed for up to {args.stall_records} tagged Produce records.", flush=True)
            wait_window(args.fault_seconds, trial, workers)
            disarm_stall(args, trial)
            wait_window(args.recovery_seconds, trial, workers)
            collect_broker_stages(args, trial)
        if args.group == "M1":
            collect_m1_rounds(args, trial)
        trial.emit("window_end")
        wait_window(max(5.2, 2 * args.poll_seconds), trial, workers)
        record_clock(args, trial, "after")
        collect_platform_events(args, trial)
    finally:
        try:
            if restore_needed:
                # Roll back even if creation timed out after the resource was accepted.
                if "restore_acknowledged" not in trial.latest():
                    for attempt in range(2):
                        try:
                            if stage_trial:
                                disarm_stall(args, trial)
                            else:
                                restore_partition(args, trial, fault_name)
                            break
                        except Exception as failure:
                            trial.emit("collector_error", source="restore", error=str(failure))
                            if attempt == 1:
                                raise
        finally:
            stop.set()
            if platform is not None:
                platform.join()
            stop_workers(workers)
            if getattr(args, "owns_target", False):
                previous = getattr(args, "previous_target", "") or "health-validation-m1-b2"
                kubectl(args, "exec", args.pod, "-c", "kafka", "--", "sh", "-ec",
                        'root=/var/run/kafka-health; test "$(cat "$root/trial-lock/owner")" = "$1"; '
                        'printf "%s" "$2" > "$root/target.new"; mv "$root/target.new" "$root/target"; '
                        'rm "$root/trial-lock/owner"; rmdir "$root/trial-lock"', "sh", trial.run_id, previous)
            trial.file.close()
            if manifest:
                save(folder / "summary.json", summarize(manifest, trial.events))
    print(folder / "summary.json", flush=True)


def save(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def probe_result(events, manifest, kind, first, last):
    max_gap = max(5000, manifest["poll_seconds"] * 3000)
    samples = [event for event in events if event["kind"] == "probe" and kind in event["counts"]]
    before = [sample for sample in samples if sample["at_ms"] <= first]
    after = [sample for sample in samples if sample["at_ms"] >= last]
    if not before or not after:
        return {"classification": "indeterminate", "reason": "missing boundary samples"}
    active = [before[-1], *(sample for sample in samples if first < sample["at_ms"] < last), after[0]]
    expected = manifest["initial"]
    pod_samples = [e for e in events if e["kind"] == "pod"]
    overlaps_transition = False
    successes = 0
    missing = False
    boundary_start, boundary_end = active[0]["at_ms"], active[-1]["at_ms"]
    for event in events:
        within = boundary_start <= event["at_ms"] <= boundary_end
        if within and event["kind"] == "collector_error" and event["source"] in ("pod", "probe"):
            missing = True
    for pod in pod_samples:
        if boundary_start <= pod["at_ms"] <= boundary_end:
            if pod["uid"] != expected["uid"] or pod["container_id"] != expected["container_id"]:
                return {"classification": "indeterminate", "reason": "pod or container changed"}
    for old, new in zip(active, active[1:]):
        if old is new:
            continue
        old_counts, new_counts = old["counts"][kind], new["counts"][kind]
        keys = old_counts.keys() | new_counts.keys()
        delta = {key: new_counts.get(key, 0) - old_counts.get(key, 0) for key in keys}
        if (new["at_ms"] - old["at_ms"] > max_gap or
                min(delta.values(), default=0) < 0 or delta.get("unknown", 0)):
            missing = True
            continue
        successes += delta.get("successful", 0)
        if delta.get("failed", 0):
            lower = old["started_ms"]
            upper = new["at_ms"]
            if lower >= first and upper <= last:
                return {"classification": "detected", "first_observed_failure_bounds_ms": [lower, upper],
                        "first_failure_confirmed": not (missing or overlaps_transition)}
            overlaps_transition = True
    if missing or overlaps_transition or not successes:
        return {"classification": "indeterminate", "reason": "counter coverage or boundary ambiguity"}
    return {"classification": "no_failure_observed", "successful_count_in_bracketing_samples": successes}


def workload(events, start, end):
    attempts = {e["id"]: e for e in events if e["kind"] == "send" and start <= e["at_ms"] < end}
    outcomes = {e["id"]: e for e in events if e["kind"] in ("ack", "send_error") and e["id"] in attempts}
    errors = Counter(e["error"] for e in outcomes.values() if e["kind"] == "send_error")
    acknowledgments = sorted(e["elapsed_us"] for e in outcomes.values() if e["kind"] == "ack")
    ack_ids = {e["id"] for e in outcomes.values() if e["kind"] == "ack"}
    deliveries = {}
    observed = {}
    for event in events:
        if event["kind"] == "consume" and event["id"] in attempts:
            elapsed = event["at_ms"] - attempts[event["id"]]["at_ms"]
            if elapsed >= 0:
                observed.setdefault(event["id"], elapsed)
    deliveries = {message: elapsed for message, elapsed in observed.items()
                  if message in ack_ids and elapsed <= 5000}

    def p99(values):
        return sorted(values)[math.ceil(0.99 * len(values)) - 1] if values else None

    return dict(attempts=len(attempts), acknowledged=len(acknowledgments),
                acknowledgment_rate=len(acknowledgments) / len(attempts) if attempts else None,
                send_errors=dict(errors), unresolved=len(attempts) - len(outcomes),
                ack_p99_us=p99(acknowledgments), delivered_within_5s=len(deliveries),
                delivery_denominator=len(ack_ids),
                acknowledged_late=sum(observed[i] > 5000 for i in ack_ids & observed.keys()),
                acknowledged_not_observed=len(ack_ids - observed.keys()),
                unacknowledged_observed=len(observed.keys() - ack_ids),
                delivery_rate_within_5s=len(deliveries) / len(ack_ids) if ack_ids else None,
                delivery_p99_ms=p99(list(deliveries.values())))


def resources(events, begin, finish):
    values = {}
    for event in events:
        if event["kind"] != "resource" or not begin <= event["at_ms"] < finish:
            continue
        for name, container in event["containers"].items():
            series = values.setdefault(name, {"cpu_cores": {}, "working_set_mib": {}})
            for metric, timestamp in (("cpu_cores", "cpu_at"), ("working_set_mib", "memory_at")):
                if container[metric] is not None and container[timestamp]:
                    series[metric][container[timestamp]] = container[metric]
    return {name: {metric: {"samples": len(series), "median": median(series.values()) if series else None}
                   for metric, series in metrics.items()}
            for name, metrics in values.items()}


def effective_min_isr(metadata):
    return min(metadata["min_isr"], metadata["replicas"])


def assess_single_stall(events, manifest, selected_id=None):
    stages = [event for event in events if event["kind"] == "broker_stage"]
    if any(event["kind"] == "collector_error" and event["source"] == "broker_stage" for event in events):
        return {"classification": "indeterminate", "reason": "broker stage evidence failure"}
    gated = [event for event in stages if event["stage"] == "stall_enter"
             and (selected_id is None or event["id"] == selected_id)]
    if not gated:
        return {"classification": "injection_not_achieved" if stages else "indeterminate",
                "reason": "no targeted broker gate entry recorded"}
    entered = gated[0]
    related = [event for event in stages if event["id"] == entered["id"]]
    released = next((event for event in related if event["stage"] == "stall_release"), None)
    if not released:
        restarted = any(event["kind"] == "pod" and event["at_ms"] >= entered["at_ms"]
                        and event["container_id"] != manifest["initial"]["container_id"] for event in events)
        return {"classification": "indeterminate", "reason": "missing gate release",
                "record_id": entered["id"], "gate_entry_ms": entered["at_ms"],
                "container_changed_after_gate_entry": restarted}
    start, end = entered["at_ms"], released["at_ms"]
    append_begin = next((event for event in related if event["stage"] == "append_enter"), None)
    append_end = next((event for event in related if event["stage"] == "append_complete"), None)
    dispatched = next((event for event in related if event["stage"] == "execution_enter"), None)
    execution_fault = manifest["scenario"] == "execution-stall"
    correct_boundary = append_begin and append_end and dispatched and dispatched["at_ms"] <= start
    if correct_boundary:
        if execution_fault:
            correct_boundary = end <= append_begin["at_ms"] <= append_end["at_ms"]
        else:
            correct_boundary = append_begin["at_ms"] <= start < end <= append_end["at_ms"]
    attempts = [event for event in events if event.get("id") == entered["id"] and event["kind"] == "send"]
    results = [event for event in events if event.get("id") == entered["id"]
               and event["kind"] in ("ack", "send_error")]
    same_target = [event for event in events if event["kind"] == "partition" and start <= event["at_ms"] <= end]
    paths_healthy = len(same_target) >= 2 and all(
        event["leader"] == manifest["target_broker"] and len(event["isr"]) == event["replicas"]
        for event in same_target
    ) and not any(event["kind"] == "metadata_error" and start <= event["at_ms"] <= end for event in events)
    control_dispatches = sum(event["stage"] == "execution_enter" and event["id"] != entered["id"]
                             and start <= event["at_ms"] < end for event in stages)
    minimum_hold_ms = max(12_000, (manifest.get("stall_seconds") or 16) * 1000 - 1500)
    if not manifest.get("stall_seconds"):
        minimum_hold_ms = 12_000  # Previous manifests used a lower eligibility threshold.
    decision = (correct_boundary and attempts and results and end - start >= minimum_hold_ms and paths_healthy
                and (execution_fault or control_dispatches > 0))
    return {
        "classification": "achieved" if decision else "indeterminate",
        "scope": "one tagged Produce record",
        "record_id": entered["id"],
        "gate_bounds_ms": [start, end],
        "held_ms": end - start,
        "dispatch_ms": dispatched["at_ms"] if dispatched else None,
        "local_append_enter_ms": append_begin["at_ms"] if append_begin else None,
        "local_append_complete_ms": append_end["at_ms"] if append_end else None,
        "other_target_dispatches_during_gate": control_dispatches,
        "leader_and_isr_samples": len(same_target),
        "leader_and_isr_satisfy_precondition": bool(paths_healthy),
        "client_outcome": results[0] if results else None,
        "client_finished_while_held": bool(results and start <= results[0]["at_ms"] < end),
    }


def assess_stall(events, manifest):
    required = manifest.get("stall_records") or 1
    if required == 1:
        return assess_single_stall(events, manifest)
    selected = dict.fromkeys(event["id"] for event in events
                             if event["kind"] == "broker_stage" and event["stage"] == "stall_enter")
    if not selected:
        return assess_single_stall(events, manifest)
    records = [assess_single_stall(events, manifest, message) for message in selected]
    result = {"scope": "multiple tagged Produce records", "records": records, "classification": "indeterminate"}
    if len(records) == required and all(record["classification"] == "achieved" for record in records):
        start = max(record["gate_bounds_ms"][0] for record in records)
        end = min(record["gate_bounds_ms"][1] for record in records)
        if end > start:
            result.update(classification="achieved", gate_bounds_ms=[start, end])
    return result


def isr_target(scenario, metadata):
    size = len(metadata["isr"])
    threshold = effective_min_isr(metadata)
    if scenario == PORT_SCOPED_ISR:
        return size == 1 and threshold == 2
    return size < threshold if scenario == "min-isr" else size == threshold == 2


def summarize(manifest, data):
    events = sorted(data, key=lambda event: event["at_ms"])
    markers = {event["kind"]: event["at_ms"] for event in events}
    output = {"run_id": manifest["run_id"], "group": manifest["group"], "scenario": manifest["scenario"],
              "topic": manifest["topic"], "target_broker": manifest["target_broker"], "runs_to_completion":
              "window_end" in markers, "event_counts": dict(Counter(e["kind"] for e in events))}
    if "window_end" not in markers:
        return output
    output["broker_clock"] = [event for event in events if event["kind"] == "broker_clock"]
    start, end = markers["window_start"], markers["window_end"]
    first_control = markers.get("fault_requested", end)
    phases = {"healthy": [start, first_control]}
    metadata = [e for e in events if e["kind"] == "partition"]
    broker = manifest["target_broker"]
    scenario = manifest["scenario"]
    if scenario in ISR_SCENARIOS:
        if scenario == PORT_SCOPED_ISR:
            quorum = [e for e in events if e["kind"] == "quorum"]
            output["quorum"] = {"samples": quorum, "same_controller_observed":
                                len(quorum) == 3 and len({e["controller"] for e in quorum}) == 1}
        fault_end = markers["restore_requested"]
        affected = [e for e in metadata if e["started_ms"] >= first_control and e["at_ms"] < fault_end
                    and e["leader"] == broker and isr_target(scenario, e)]
        observed_restored = [e for e in metadata if e["started_ms"] >= fault_end
                             and e["at_ms"] <= end and e["leader"] == broker
                             and e["min_isr"] == manifest["original_min_isr"] and len(e["isr"]) == e["replicas"]]
        if observed_restored:
            phases["recovered"] = [observed_restored[0]["at_ms"], end]
        output["restored_on_target_leader"] = bool(observed_restored)
        if affected:
            first, last = affected[0], affected[-1]
            healthy_before = [e for e in metadata if e["at_ms"] <= first["started_ms"]
                              and e["leader"] == broker and len(e["isr"]) == e["replicas"]]
            previous = healthy_before[-1]["started_ms"] if healthy_before else None
            output["condition"] = {"classification": "achieved", "onset_bounds_ms": [previous, first["at_ms"]],
                                   "first_confirmed": first, "last_confirmed": last}
            if observed_restored:
                restored = observed_restored[0]
                before_recovery = max(
                    e["started_ms"] for e in metadata if first["at_ms"] <= e["at_ms"] <= restored["started_ms"]
                    and e["leader"] == broker and isr_target(scenario, e)
                )
                output["condition"]["recovery_bounds_ms"] = [before_recovery, restored["at_ms"]]
            intervening = [e for e in metadata if first["at_ms"] <= e["at_ms"] <= last["at_ms"]]
            if (len(intervening) == len(affected)
                    and not any(first["at_ms"] <= e["at_ms"] <= last["at_ms"]
                                for e in events if e["kind"] == "metadata_error")
                    and all(right["at_ms"] - left["at_ms"] <= max(5000, manifest["poll_seconds"] * 3000)
                            for left, right in zip(intervening, intervening[1:]))
                    and first["at_ms"] < last["started_ms"]):
                phase = "isr_at_threshold" if scenario == "isr-at-threshold" else "fault_confirmed"
                phases[phase] = [first["at_ms"], last["started_ms"]]
        else:
            observed = [e for e in metadata if first_control <= e["started_ms"] and e["at_ms"] <= fault_end]
            timestamps = [markers["fault_acknowledged"],
                          *(e["at_ms"] for e in observed if e["at_ms"] > markers["fault_acknowledged"]),
                          fault_end]
            has_missing = any(first_control <= e["at_ms"] <= fault_end
                              for e in events if e["kind"] == "metadata_error")
            covered = not has_missing and len(timestamps) > 2 and all(
                right - left <= max(5000, manifest["poll_seconds"] * 3000)
                for left, right in zip(timestamps, timestamps[1:]))
            output["condition"] = {"classification": "injection_not_achieved" if covered else "indeterminate"}

    if manifest["scenario"] in STALL_SCENARIOS:
        output["condition"] = assess_stall(events, manifest)
        if output["condition"]["classification"] == "achieved":
            phases["fault_confirmed"] = output["condition"]["gate_bounds_ms"]
        phases["recovered"] = [markers["restore_acknowledged"], end]

    output["windows"] = {}
    for phase, (begin, finish) in phases.items():
        pods = [e for e in events if e["kind"] == "pod" and begin <= e["at_ms"] < finish]
        output["windows"][phase] = {
            "bounds_ms": [begin, finish], "workload": workload(events, begin, finish),
            "resources": resources(events, begin, finish),
            "probes": {kind: probe_result(events, manifest, kind, begin, finish) for kind in PROBE_TYPES},
            "pod_samples": len(pods), "pod_not_ready_samples": sum(not e["ready"] for e in pods),
            "restart_count_range": [min(e["restart_count"] for e in pods), max(e["restart_count"] for e in pods)]
            if pods else None,
        }
    if manifest["group"] == "M1":
        rounds = [e for e in events if e["kind"] == "m1_round" and start <= e["at_ms"] <= end]
        output["m1"] = {"rounds": len(rounds), "confirmed": {}}
        for signal in ("exec", "disk", "isr"):
            confirmed = [e for e in rounds if e["snapshot"][signal + "Fault"] == "2"]
            later = [e for e in rounds if confirmed and e["at_ms"] > confirmed[-1]["at_ms"]
                     and e["snapshot"]["ready"] == "true"]
            output["m1"]["confirmed"][signal] = {
                "count": len(confirmed),
                "first_ms": confirmed[0]["at_ms"] if confirmed else None,
                "subsequent_ready_ms": later[0]["at_ms"] if later else None,
            }
    return output


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    actions = cli.add_subparsers(dest="action", required=True)
    live = actions.add_parser("run", help="collect a live trial")
    live.add_argument("--group", choices=("M0", "M1"), required=True)
    live.add_argument("--scenario", choices=("healthy", *ISR_SCENARIOS, *STALL_SCENARIOS), required=True)
    live.add_argument("--context", default="docker-desktop")
    live.add_argument("--namespace", default="kafka")
    live.add_argument("--bootstrap", default="localhost:32000")
    live.add_argument("--topic")
    live.add_argument("--partition", type=int, default=0)
    live.add_argument("--pod")
    live.add_argument("--broker-id", type=int, choices=(0, 1, 2))
    live.add_argument("--pair-id", help="Identifier shared by the matched M0 and M1 runs")
    live.add_argument("--follower-id", type=int, choices=(0, 1, 2))
    live.add_argument("--resource-seconds", type=int, default=0,
                      help="Sample kubelet container resources every N seconds (0 disables)")
    live.add_argument("--stall-seconds", type=int, choices=range(16, 91), default=16)
    live.add_argument("--stall-records", type=int, choices=(1, 2, 3), default=1)
    live.add_argument("--observer-port", type=int, choices=(32001,32002,32003),
                      help="Use a dedicated localhost Kafka peer for independent metadata sampling")
    live.add_argument("--rate", type=int, default=2, help="Business sends per second; 0 with healthy observes only.")
    live.add_argument("--poll-seconds", type=float, default=2)
    live.add_argument("--healthy-seconds", type=int, default=20)
    live.add_argument("--fault-seconds", type=int, default=65)
    live.add_argument("--recovery-seconds", type=int, default=20)
    live.add_argument("--output", type=Path, required=True)
    replay = actions.add_parser("summarize", help="recompute results from the saved manifest and events")
    replay.add_argument("folder", type=Path)
    args = cli.parse_args()
    if args.action == "run":
        valid_rate = 1 <= args.rate <= 100 or args.rate == 0 and args.scenario == "healthy"
        if not (1 <= args.poll_seconds <= 30 and valid_rate and args.partition >= 0
                and min(args.healthy_seconds, args.fault_seconds, args.recovery_seconds) >= 10
                and (args.resource_seconds == 0 or args.resource_seconds >= 5)):
            cli.error("Sampling, rate, partition or window is outside its supported range.")
        run(args)
    else:
        manifest = json.loads((args.folder / "run.json").read_text(encoding="utf-8"))
        events = [json.loads(line) for line in (args.folder / "events.jsonl").read_text(encoding="utf-8").splitlines()]
        if any(event["run_id"] != manifest["run_id"] for event in events):
            cli.error("Recorded events do not match the run manifest.")
        json.dump(summarize(manifest, events), sys.stdout, indent=2)
        print()


if __name__ == "__main__":
    main()
