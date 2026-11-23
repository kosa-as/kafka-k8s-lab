"""Observe one Kafka-container restart independently of Service endpoint readiness."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import socket
import struct
import subprocess
import time

from validate_health import now_ms, save


CONTEXT = ["kubectl", "--context", "docker-desktop", "-n", "kafka"]
POD = "bitnami-kafka-controller-2"
PORTS = (32992, 32993)


def pod_status():
    output = subprocess.check_output(
        [*CONTEXT, "--request-timeout=5s", "get", "pod", POD, "-o", "json"], timeout=8)
    pod = json.loads(output)
    container = next(c for c in pod["status"]["containerStatuses"] if c["name"] == "kafka")
    ready = next(c for c in pod["status"]["conditions"] if c["type"] == "Ready")
    return {"uid": pod["metadata"]["uid"], "container": container.get("containerID"),
            "started": container.get("started", False), "restarts": container["restartCount"],
            "started_at": container["state"].get("running", {}).get("startedAt"),
            "ready": ready["status"] == "True", "ready_transition": ready["lastTransitionTime"]}


def controller_available():
    # A local port-forward listener accepts TCP even if the destination is down.
    # Require a protocol reply from the Controller rather than that local handshake.
    request = struct.pack(">hhih", 18, 0, 4100, -1)  # ApiVersions v0
    with socket.create_connection(("127.0.0.1", PORTS[1]), timeout=0.4) as conn:
        conn.settimeout(0.4)
        conn.sendall(struct.pack(">i", len(request)) + request)
        with conn.makefile("rb") as stream:
            size, response_id, error = struct.unpack(">iih", stream.read(10))
            return size >= 6 and response_id == 4100 and error == 0


def produce_handler():
    # Kafka Produce v3 with an empty topic array exercises decoding and request dispatch
    # without creating records, consulting a Leader, or depending on an external Service.
    client_id = b"rq3-cold-start"
    correlation = 4101
    request = (struct.pack(">hhih", 0, 3, correlation, len(client_id)) + client_id
               + struct.pack(">hhii", -1, 1, 1000, 0))
    with socket.create_connection(("127.0.0.1", PORTS[0]), timeout=0.4) as conn:
        conn.settimeout(0.4)
        conn.sendall(struct.pack(">i", len(request)) + request)
        with conn.makefile("rb") as stream:
            size, = struct.unpack(">i", stream.read(4))
            if size != 12:
                return False
            response_id, topics, _ = struct.unpack(">iii", stream.read(size))
            return response_id == correlation and topics == 0


def capture(check):
    start = now_ms()
    try:
        value = check()
        return {"bounds_ms": [start, now_ms()], "value": value}
    except (OSError, struct.error, subprocess.SubprocessError, KeyError, StopIteration) as error:
        return {"bounds_ms": [start, now_ms()], "value": None, "error": type(error).__name__}


def identity(group):
    data = json.loads(subprocess.check_output([*CONTEXT, "get", "pod", POD, "-o", "json"], timeout=8))
    kafka = next(c for c in data["spec"]["containers"] if c["name"] == "kafka")
    probes = {name: kafka[name] for name in ("startupProbe", "readinessProbe", "livenessProbe")}
    native = "tcpSocket" in probes["readinessProbe"]
    if native != (group == "M0"):
        raise RuntimeError("Pod probes do not match the requested group.")
    if group == "M1":
        mode = subprocess.check_output(
            [*CONTEXT, "exec", POD, "-c", "kafka", "--", "cat", "/opt/m1/mode"], timeout=8)
        if mode.strip() != b"enforce":
            raise RuntimeError("M1 must be in enforce mode.")
    return {"image": kafka["image"], "probes": probes}


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {"pair_id": args.pair_id, "group": args.group, "initial": pod_status(),
                "configuration": identity(args.group)}
    before = metadata["initial"]
    if not before["ready"] or not before["started"]:
        raise RuntimeError("The target must initially be started and Ready.")
    save(args.output / "run.json", metadata)
    local_forward = [*CONTEXT, "port-forward", "pod/" + POD, "--address", "127.0.0.1",
                     f"{PORTS[0]}:9092", f"{PORTS[1]}:9093"]
    with (args.output / "forward.log").open("w") as log, (
            args.output / "events.jsonl").open("w", encoding="utf-8") as stream:
        forward = subprocess.Popen(local_forward, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and forward.poll() is None:
                if capture(produce_handler)["value"] is True:
                    break
                time.sleep(0.25)
            else:
                raise RuntimeError("The pre-restart direct Produce handler check failed.")
            restart_at = now_ms()
            command = [*CONTEXT, "exec", POD, "-c", "kafka", "--", "sh", "-c", "kill -TERM 1"]
            with (args.output / "restart.log").open("w") as restart_log, ThreadPoolExecutor(3) as pool:
                with subprocess.Popen(command, stdout=restart_log, stderr=subprocess.STDOUT) as restart:
                    result = observe(pool, stream, before, forward, restart_at, args.timeout)
                    result["command_exit_code"] = restart.wait(timeout=10)
            save(args.output / "summary.json", result)
        finally:
            forward.terminate()
            forward.wait(timeout=10)


def observe(pool, stream, before, forward, restart_at, timeout):
    deadline = time.monotonic() + timeout
    first = {}
    generation_seen = False
    unavailable = {}
    previous = {}
    while time.monotonic() < deadline and forward.poll() is None:
        checks = {"pod": pool.submit(capture, pod_status), "produce": pool.submit(capture, produce_handler),
                  "controller": pool.submit(capture, controller_available)}
        row = {key: future.result() for key, future in checks.items()}
        stream.write(json.dumps(row) + "\n")
        stream.flush()
        status = row["pod"]["value"]
        if status and status["uid"] != before["uid"]:
            raise RuntimeError("The Pod UID changed; this run does not isolate a Kafka container restart.")
        if status and status["container"] and status["container"] != before["container"]:
            generation_seen = True
        for key in ("produce", "controller"):
            if row[key]["value"] is not True:
                unavailable[key] = row[key]["bounds_ms"][0]
            elif generation_seen and key not in first:
                first[key] = [unavailable.get(key, restart_at), row[key]["bounds_ms"][1]]
        if generation_seen:
            if status and status["container"] != before["container"]:
                for key in ("started", "ready"):
                    if status[key] and key not in first:
                        first[key] = [max(restart_at, previous.get("pod", row["pod"])["bounds_ms"][0]),
                                      row["pod"]["bounds_ms"][1]]
            if all(key in first for key in ("produce", "controller", "started", "ready")):
                break
        previous = row
        time.sleep(0.25)
    produce, ready = first.get("produce"), first.get("ready")
    order = "indeterminate"
    if produce and ready:
        if produce[1] < ready[0]:
            order = "handler_before_ready"
        elif ready[1] < produce[0]:
            order = "ready_before_handler"
    return {"restart_request_ms": restart_at, "new_container_observed": generation_seen,
            "first_observed_ms": first, "produce_handler_vs_ready": order,
            "complete": generation_seen and len(first) == 4}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", choices=("M0", "M1"), required=True)
    parser.add_argument("--pair-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=120)
    run(parser.parse_args())
