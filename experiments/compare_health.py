"""Recompute and compare one matched M0/M1 pair from raw trial directories."""

import argparse
import json
from pathlib import Path
import re

from validate_health import summarize


MATCHED = (
    "pair_id", "scenario", "target_broker", "pod", "follower_pod", "context", "namespace",
    "partition", "kafka_image", "original_min_isr", "initial_replicas", "bootstrap",
    "observer_port", "load_interval_ms", "producer_retries", "poll_seconds",
    "resource_seconds", "healthy_seconds", "fault_seconds", "recovery_seconds", "source_hashes",
)
PROBE_TIMING = ("initialDelaySeconds", "periodSeconds", "timeoutSeconds",
                "failureThreshold", "successThreshold")
DIAGNOSTIC_ID = re.compile(r"m[0-9a-f]{7}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def load(folder):
    manifest = json.loads((folder / "run.json").read_text(encoding="utf-8"))
    events = [json.loads(line) for line in (folder / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    if not events or any(event["run_id"] != manifest["run_id"] for event in events):
        raise ValueError(f"Events do not match {folder / 'run.json'}")
    result = summarize(manifest, events)
    for window in result.get("windows", {}).values():
        begin, end = window["bounds_ms"]
        window["observed_diagnostic_deliveries"] = len({
            event["id"] for event in events
            if event["kind"] == "consume" and begin <= event["at_ms"] < end
            and DIAGNOSTIC_ID.fullmatch(event["id"])
        })
    return manifest, result


def delta(left, right):
    return None if left is None or right is None else right - left


def delay_bounds(onset, event):
    if not onset or not event or any(point is None for point in (*onset, *event)):
        return None
    return [event[0] - onset[1], event[1] - onset[0]]


def compare(m0, m1):
    left, s0 = m0
    right, s1 = m1
    if [left["group"], right["group"]] != ["M0", "M1"]:
        raise ValueError("Supply the M0 trial first and the M1 trial second.")
    mismatches = [key for key in MATCHED if key not in left or key not in right or left[key] != right[key]]
    if not left.get("pair_id"):
        mismatches.append("missing pair_id")
    for probe, settings in left["probes"].items():
        if any(settings.get(key) != right["probes"].get(probe, {}).get(key) for key in PROBE_TIMING):
            mismatches.append(f"{probe} timing")
    result = {"pair_id": left.get("pair_id"), "scenario": left["scenario"],
              "comparable_config": not mismatches, "mismatches": mismatches,
              "runs": {"M0": s0, "M1": s1}}
    if mismatches or not s0["runs_to_completion"] or not s1["runs_to_completion"]:
        return result
    if left["scenario"] != "healthy":
        result["fault_achieved_both"] = all(
            s.get("condition", {}).get("classification") == "achieved" for s in (s0, s1))
    changes = {}
    for phase in s0["windows"].keys() & s1["windows"].keys():
        a, b = s0["windows"][phase], s1["windows"][phase]
        measures = ("acknowledgment_rate", "ack_p99_us", "delivery_rate_within_5s", "delivery_p99_ms")
        changes[phase] = {key: delta(a["workload"][key], b["workload"][key]) for key in measures}
        changes[phase]["duration_ms"] = [window["bounds_ms"][1] - window["bounds_ms"][0] for window in (a, b)]
        changes[phase]["probe_classification"] = {
            name: [window["probes"][name]["classification"] for window in (a, b)] for name in a["probes"]}
        if phase == "fault_confirmed":
            changes[phase]["onset_to_first_probe_failure_bounds_ms"] = {
                name: [delay_bounds(fault_onset(summary["condition"]),
                                    window["probes"][name].get("first_observed_failure_bounds_ms"))
                       for summary, window in ((s0, a), (s1, b))]
                for name in a["probes"]}
        changes[phase]["resource_median_delta"] = {
            name: {metric: delta(value["median"], b["resources"][name][metric]["median"])
                   for metric, value in a["resources"][name].items()}
            for name in a["resources"].keys() & b["resources"].keys()}
    result["m1_minus_m0"] = changes
    return result


def fault_onset(condition):
    if "gate_bounds_ms" in condition:
        start, _ = condition["gate_bounds_ms"]
        return [start, start]
    return condition.get("onset_bounds_ms")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("m0", type=Path)
    parser.add_argument("m1", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(load(args.m0), load(args.m1))
    with args.output.open("w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    if not result["comparable_config"]:
        parser.exit(2, "Pair configuration differs. See mismatch fields in the output.\n")


if __name__ == "__main__":
    main()
