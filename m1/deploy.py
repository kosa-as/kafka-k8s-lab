"""Manage the single-Broker Docker Desktop M1 overlay. The original Helm values stay available for M0."""
import argparse
import copy
import json
import re
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "experiments"))
from validate_health import command, save

K = ["kubectl", "--context", "docker-desktop", "-n", "kafka", "--request-timeout=8s"]
STS = "bitnami-kafka-controller"
POD = STS + "-2"
CM = "kafka-m1-observer"
MOUNT = {"name": "m1-program", "mountPath": "/opt/m1", "readOnly": True}


def get(kind, name):
    return json.loads(command([*K, "get", kind, name, "-o", "json"]))


def mutate(state, label, operations):
    path = state / (label + ".json")
    save(path, operations)
    command([*K, "patch", "sts", STS, "--type=json", "--patch-file", str(path)])


def spec_fields(sts):
    pod = sts["spec"]["template"]["spec"]
    return {"/spec/updateStrategy": sts["spec"]["updateStrategy"],
            "/spec/template/spec/volumes": pod["volumes"],
            "/spec/template/spec/containers": pod["containers"]}


def replace_fields(state, label, old, new):
    operations = [{"op": "test", "path": k, "value": v} for k, v in old.items()]
    operations.extend({"op": "replace", "path": k, "value": v} for k, v in new.items())
    mutate(state, label, operations)


def wait_pod(old_uid=None, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            pod = get("pod", POD)
            ready = any(item["type"] == "Ready" and item["status"] == "True"
                        for item in pod["status"].get("conditions", []))
            if ready and not pod["metadata"].get("deletionTimestamp") and pod["metadata"]["uid"] != old_uid:
                return
        except RuntimeError:
            pass
        time.sleep(2)
    raise RuntimeError("Target Pod did not reach Ready.")


def kafka_tool(tool, *args):
    return command([*K, "exec", STS+"-0", "-c", "kafka", "--", "env", "-u", "JMX_PORT",
                    "-u", "KAFKA_JMX_OPTS", "/opt/bitnami/kafka/bin/"+tool+".sh",
                    "--bootstrap-server", "localhost:9092", *args], timeout=20)


def default_target():
    topic = "health-validation-m1-b2"
    kafka_tool("kafka-topics", "--create", "--if-not-exists", "--topic", topic,
               "--replica-assignment", "2:0:1", "--config", "min.insync.replicas=2",
               "--config", "retention.ms=3600000")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        description = kafka_tool("kafka-topics", "--describe", "--topic", topic)
        match = re.search(r"Leader:\s*(\d+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]+)", description)
        if match and match[2] == "2,0,1" and len(match[3].split(",")) == 3:
            if match[1] == "2":
                return
            kafka_tool("kafka-leader-election", "--election-type", "preferred", "--topic", topic, "--partition", "0")
        time.sleep(1)
    raise RuntimeError("The dedicated M1 diagnostic partition is not fully replicated on Broker 2.")


def set_mode(state, mode):
    if mode == "enforce":
        default_target()
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            decision = command([*K, "exec", POD, "-c", "kafka", "--", "cat", "/var/run/kafka-health/decision"])
            if "ready=true" in decision.splitlines() and "topic=health-validation-m1-b2" in decision.splitlines():
                break
            time.sleep(1)
        else:
            raise RuntimeError("Missing healthy default-target shadow decisions.")
    patch = state / "mode-patch.json"
    save(patch, {"data":{"mode":mode}})
    command([*K, "patch","cm",CM,"--type=merge","--patch-file",str(patch)])
    command([*K, "annotate","pod",POD,f"m1-mode-refresh={time.time_ns()}","--overwrite"])
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if command([*K, "exec", POD, "-c", "kafka", "--", "cat", "/opt/m1/mode"]).strip() == mode:
            return
        time.sleep(1)
    raise RuntimeError("The projected probe mode did not refresh.")


def install(args):
    sts = get("sts", STS)
    if sts["spec"]["replicas"] != 3 or sts["status"].get("readyReplicas") != 3:
        raise ValueError("Expected three Ready Bitnami Kafka Pods.")
    original = spec_fields(sts)
    new = copy.deepcopy(original)
    new["/spec/updateStrategy"] = {"type": "RollingUpdate", "rollingUpdate": {"partition": 2}}
    volumes = new["/spec/template/spec/volumes"]
    volumes.append({"name": "m1-program", "configMap": {"name": CM, "defaultMode": 420}})
    containers = new["/spec/template/spec/containers"]
    broker = next(c for c in containers if c["name"] == "kafka")
    sidecar = next(c for c in containers if c["name"] == "health-sidecar")
    if any(item["name"] == "KAFKA_OPTS" for item in broker["env"]):
        raise ValueError("Existing KAFKA_OPTS requires an explicit merge.")
    opts = "-javaagent:/opt/m1/m1.jar"
    if args.fixture:
        opts = "-javaagent:/opt/m1/stall.jar " + opts
    broker["env"].append({"name": "KAFKA_OPTS", "value": opts})
    broker["volumeMounts"].extend([MOUNT, {"name":"kafka-health","mountPath":"/var/run/kafka-health"}])
    for probe, mode in (("readinessProbe","ready"),("livenessProbe","live")):
        definition = broker[probe]
        definition.pop("tcpSocket", None)
        definition["exec"] = {"command":["/bin/bash","/opt/m1/probe.sh",mode]}
    sidecar.update(image=broker["image"], command=["/opt/bitnami/java/bin/java"],
                   args=["-Xms24m","-Xmx80m","-cp","/opt/m1/m1.jar:/opt/bitnami/kafka/libs/*","lab.m1.Sidecar"],
                   env=[{"name":"M1_BROKER_ID","value":"2"}],
                   resources={"requests":{"cpu":"50m","memory":"96Mi"},"limits":{"cpu":"500m","memory":"256Mi"}})
    sidecar["volumeMounts"].append(MOUNT)
    args.state.mkdir(parents=True,exist_ok=False)
    save(args.state / "original.json", original)
    save(args.state / "overlay.json", new)
    unix_probe = args.state / "probe.sh"
    unix_probe.write_bytes(Path(__file__).with_name("probe.sh").read_text().encode("utf-8"))
    config = [*K,"create","configmap",CM,f"--from-file=m1.jar={args.jar}",
              f"--from-file=probe.sh={unix_probe}", "--from-literal=mode=shadow"]
    if args.fixture:
        config.append(f"--from-file=stall.jar={args.fixture}")
    command(config)
    try:
        initial_uid=get("pod",POD)["metadata"]["uid"]
        replace_fields(args.state,"install",original,new)
        wait_pod(initial_uid)
        default_target()
    except Exception:
        current = spec_fields(get("sts",STS))
        if current == original:
            command([*K,"delete","cm",CM])
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",choices=("install","enforce","shadow","add-fixture","remove-fixture","restore"))
    parser.add_argument("state",type=Path)
    parser.add_argument("--jar",type=Path)
    parser.add_argument("--fixture",type=Path)
    args=parser.parse_args()
    if args.action=="install":
        install(args)
        return
    original=json.loads((args.state/"original.json").read_text())
    expected=json.loads((args.state/"overlay.json").read_text())
    current=spec_fields(get("sts",STS))
    if args.action in ("enforce","shadow"):
        if current!=expected:
            raise ValueError("Unexpected StatefulSet template.")
        mode= "enforce" if args.action=="enforce" else "shadow"
        set_mode(args.state,mode)
    elif args.action in ("add-fixture","remove-fixture"):
        adding = args.action == "add-fixture"
        if adding:
            if args.fixture is None or not args.fixture.is_file():
                parser.error("--fixture requires the built M0 stall agent")
            import base64
            patch=args.state/"add-fixture-jar.json"
            save(patch, {"binaryData":{"stall.jar":base64.b64encode(args.fixture.read_bytes()).decode("ascii")}})
            command([*K,"patch","cm",CM,"--type=merge","--patch-file",str(patch)])
        set_mode(args.state,"shadow")
        old_uid=get("pod",POD)["metadata"]["uid"]
        clean=copy.deepcopy(expected)
        broker=next(c for c in clean["/spec/template/spec/containers"] if c["name"]=="kafka")
        entry=next(item for item in broker["env"] if item["name"]=="KAFKA_OPTS")
        entry["value"]=("-javaagent:/opt/m1/stall.jar " if adding else "") + "-javaagent:/opt/m1/m1.jar"
        replace_fields(args.state,args.action,expected,clean)
        save(args.state/"overlay.json",clean)
        if expected != clean:
            wait_pod(old_uid)
        set_mode(args.state,"enforce")
        if adding:
            return
        cleared = args.state/"clear-fixture.json"
        save(cleared, {"binaryData":{"stall.jar":None}})
        command([*K,"patch","cm",CM,"--type=merge","--patch-file",str(cleared)])
    else:
        if current!=original:
            previous_uid = get("pod", POD)["metadata"]["uid"]
            replace_fields(args.state,"restore",expected,original)
            wait_pod(previous_uid)
        command(["kubectl","--context","docker-desktop","-n","kafka","rollout","status",
                 "sts/"+STS,"--timeout=180s"],timeout=195)
        command([*K,"delete","cm",CM,"--ignore-not-found"])


if __name__ == "__main__":
    main()
