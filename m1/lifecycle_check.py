"""Check the enforced probes across a temporary sidecar freeze and a sidecar-only restart."""
from datetime import datetime
import argparse
import json
from pathlib import Path
import subprocess
import time

from deploy import K, POD, command, get, save


def snapshot():
    raw = command([*K,"exec",POD,"-c","kafka","--","cat","/var/run/kafka-health/decision"])
    return dict(line.split("=",1) for line in raw.splitlines() if line and not line.startswith("#"))


def probe(mode):
    return subprocess.run([*K,"exec",POD,"-c","kafka","--","bash","/opt/m1/probe.sh",mode],
                          stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10).returncode


def sidecar():
    pod = get("pod",POD)
    return next(c for c in pod["status"]["containerStatuses"] if c["name"]=="health-sidecar")


def recovered(before, restarted=False):
    deadline=time.monotonic()+70
    while time.monotonic()<deadline:
        current=snapshot()
        if (current.get("generation") == before["generation"] and int(current["round"])>int(before["round"])
                and time.time()*1000-int(current["time"])<3000 and current["ready"]=="true"):
            status=sidecar()
            launched=status.get("state",{}).get("running",{}).get("startedAt")
            if not restarted or (status["containerID"] != restarted and launched and
                    int(current["time"]) >= datetime.fromisoformat(launched.replace("Z","+00:00")).timestamp()*1000):
                return current
        time.sleep(1)
    raise RuntimeError("Sidecar did not resume healthy decisions in the same Kafka generation.")


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output",type=Path)
    args=parser.parse_args()
    start=snapshot()
    if (start["ready"]!="true" or start["topic"]!="health-validation-m1-b2"
            or command([*K,"exec",POD,"-c","kafka","--","cat","/opt/m1/mode"]).strip()!="enforce"):
        raise RuntimeError("This lifecycle trial requires the healthy idle M1 target in enforce mode.")
    status=sidecar()
    cid=status["containerID"].removeprefix("docker://")
    labels=json.loads(command(["docker","inspect","--format","{{json .Config.Labels}}",cid]))
    if (labels.get("io.kubernetes.pod.name")!=POD or labels.get("io.kubernetes.container.name")!="health-sidecar"
            or labels.get("io.kubernetes.pod.namespace")!="kafka"):
        raise RuntimeError("Docker and Kubernetes target identities disagree.")
    command(["docker","pause",cid])
    try:
        time.sleep(9.5)
        stale=snapshot()
        age=int(time.time()*1000)-int(stale["time"])
        ready,live=probe("ready"),probe("live")
        if age<=8000 or ready!=1 or live!=0:
            raise RuntimeError(f"Unexpected stale snapshot result: age={age}, ready={ready}, live={live}")
    finally:
        command(["docker","unpause",cid])
    resumed=recovered(stale)
    if probe("ready")!=0: raise RuntimeError("Readiness did not recover.")
    subprocess.run([*K,"exec",POD,"-c","health-sidecar","--","/bin/sh","-c","kill -TERM 1"],
                   capture_output=True,timeout=10)
    final=recovered(resumed,status["containerID"])
    if probe("ready") or probe("live"): raise RuntimeError("Final probes did not pass.")
    save(args.output,{"stale_age_ms":age,"stale_readiness_exit":ready,"stale_liveness_exit":live,
                      "generation":final["generation"],"rounds":[start["round"],stale["round"],resumed["round"],final["round"]],
                      "sidecar_restart_counts":[status["restartCount"],sidecar()["restartCount"]],
                      "final_ready":True})


if __name__ == "__main__":
    main()
