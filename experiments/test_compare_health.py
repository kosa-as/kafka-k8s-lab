import unittest

from compare_health import MATCHED, compare
from validate_health import resources, workload


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
