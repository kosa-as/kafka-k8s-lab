import unittest

from compare_health import MATCHED, compare, stall_platform
from replication_path import partition_resource, verify_pod_rules
from validate_health import assess_stall, isr_target, resources, workload


class CalibrationScoringTest(unittest.TestCase):
    def test_late_and_unacknowledged_deliveries_are_visible(self):
        events = [
            {"kind": "send", "id": "late", "at_ms": 0},
            {"kind": "send", "id": "timeout", "at_ms": 0},
            {"kind": "ack", "id": "late", "at_ms": 1, "elapsed_us": 1000},
            {"kind": "send_error", "id": "timeout", "at_ms": 2, "error": "TimeoutException"},
            {"kind": "consume", "id": "late", "at_ms": 5001},
            {"kind": "consume", "id": "timeout", "at_ms": 6000},
        ]
        result = workload(events, 0, 100)
        self.assertEqual(1, result["acknowledged_late"])
        self.assertEqual(1, result["unacknowledged_observed"])
        self.assertEqual(0, result["delivered_within_5s"])

    def test_duplicate_kubelet_sample_is_counted_once(self):
        sample = {"cpu_at": "t1", "cpu_cores": 0.5,
                  "memory_at": "t0", "working_set_mib": 120}
        events = [{"kind": "resource", "at_ms": t, "containers": {"kafka": sample}} for t in (2, 3)]
        result = resources(events, 0, 5)
        self.assertEqual({"samples": 1, "median": 0.5}, result["kafka"]["cpu_cores"])

    def test_different_follower_disqualifies_pair(self):
        template = dict.fromkeys(MATCHED)
        template.update(pair_id="I3", scenario="min-isr", probes={}, follower_pod="a")
        result = compare((dict(template, group="M0"), {}), (dict(template, group="M1", follower_pod="b"), {}))
        self.assertEqual(["follower_pod"], result["mismatches"])
        self.assertNotIn("m1_minus_m0", result)

    def test_replica_loss_that_still_meets_threshold_is_separate(self):
        metadata = {"isr": [2, 1], "min_isr": 2, "replicas": 3}
        self.assertFalse(isr_target("min-isr", metadata))
        self.assertTrue(isr_target("isr-at-threshold", metadata))
        self.assertFalse(isr_target("isr-at-threshold", dict(metadata, isr=[2])))
        self.assertFalse(isr_target("isr-below-threshold", metadata))
        self.assertTrue(isr_target("isr-below-threshold", dict(metadata, isr=[2])))

    def test_one_held_record_does_not_qualify_as_multi_request_stall(self):
        events = [{"kind": "broker_stage", "stage": "stall_enter", "id": "first", "at_ms": 100}]
        manifest = {"stall_records": 3, "initial": {"container_id": "old"}}
        result = assess_stall(events, manifest)
        self.assertEqual("indeterminate", result["classification"])
        self.assertEqual("first", result["records"][0]["record_id"])

    def test_interrupted_hold_preserves_old_generation_probe_failure(self):
        identity = {"uid": "pod", "container_id": "old"}
        manifest = {"initial": identity, "poll_seconds": 1}
        events = [
            {"kind": "fault_requested", "at_ms": 1000},
            {"kind": "broker_stage", "stage": "stall_enter", "id": "one", "at_ms": 1400},
        ]
        for at_ms in range(0, 7000, 1000):
            pod = dict(identity, kind="pod", at_ms=at_ms, started_ms=at_ms - 100,
                       restart_count=0)
            probe = {"kind": "probe", "at_ms": at_ms, "started_ms": at_ms - 100,
                     "counts": {kind: {"successful": 1, "failed": int(at_ms >= 3000)}
                                for kind in ("Readiness", "Liveness")}}
            events.extend((pod, probe))
        events.append(dict(identity, kind="pod", container_id="new", restart_count=1,
                           at_ms=7000, started_ms=6900))
        result = stall_platform(manifest, events)
        self.assertTrue(result["container_changed"])
        self.assertEqual({}, result["observed_hold_ms"])
        self.assertEqual([5900, 7000], result["first_container_change_bounds_ms"])
        self.assertEqual("detected", result["pre_change_probes"]["Liveness"]["classification"])

    def test_replication_fixture_requires_port_scoped_egress(self):
        resource = partition_resource("calibration", "kafka", ["follower0", "follower1"],
                                      "10.1.2.170", 9094, 120)
        self.assertEqual({"kafka": ["follower0", "follower1"]}, resource["spec"]["selector"]["pods"])
        self.assertEqual(["10.1.2.170:9094"], resource["spec"]["externalTargets"])
        self.assertNotIn("target", resource["spec"])
        raw = {"spec": {
            "ipsets": [{"source": "kafka/calibration", "cidrAndPorts": [{"cidr": "10.1.2.170/32", "port": 9094}]}],
            "iptables": [{"source": "kafka/calibration", "direction": "output", "ipsets": ["scope"]}],
        }}
        verify_pod_rules(raw, resource)
        raw["spec"]["iptables"][0]["direction"] = "input"
        with self.assertRaises(ValueError):
            verify_pod_rules(raw, resource)
