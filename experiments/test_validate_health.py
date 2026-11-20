import unittest

from validate_health import assess_stall, effective_min_isr, probe_result, summarize


class ValidationScoringTest(unittest.TestCase):
    manifest = {
        "run_id": "example", "group": "M0", "scenario": "min-isr",
        "topic": "health-validation-example", "target_broker": 1,
        "poll_seconds": 1, "original_min_isr": 2,
        "initial": {"uid": "pod", "container_id": "container"},
    }

    def probes(self, failure=False):
        return [
            {"kind": "probe", "at_ms": timestamp, "started_ms": timestamp,
             "counts": {"Readiness": {"successful": index, "failed": int(failure and timestamp >= 3000)}}}
            for index, timestamp in enumerate(range(0, 7001, 1000))
        ]

    def score(self, events):
        return probe_result(events, self.manifest, "Readiness", 1000, 6000)

    def test_min_isr_is_capped_by_replica_count(self):
        self.assertEqual(3, effective_min_isr({"min_isr": 4, "replicas": 3}))

    def test_counter_coverage_with_no_failure(self):
        self.assertEqual("no_failure_observed", self.score(self.probes())["classification"])

    def test_counter_increment_is_interval(self):
        detected = self.score(self.probes(failure=True))
        self.assertEqual("detected", detected["classification"])
        self.assertEqual([2000, 3000], detected["first_observed_failure_bounds_ms"])

    def test_no_failure_cannot_be_inferred_from_missing_samples(self):
        self.assertEqual("indeterminate", self.score(self.probes()[2:])["classification"])
        error = {"kind": "collector_error", "at_ms": 3000, "source": "probe"}
        self.assertEqual("indeterminate", self.score(self.probes() + [error])["classification"])

    def test_probe_counts_do_not_span_container_generations(self):
        pod = {"kind": "pod", "at_ms": 3000, "uid": "pod", "container_id": "replacement"}
        self.assertEqual("indeterminate", self.score(self.probes() + [pod])["classification"])

    def test_injection_not_achieved_requires_independent_coverage(self):
        events = [
            {"kind": kind, "at_ms": time}
            for kind, time in (("window_start", 0), ("fault_requested", 1000),
                               ("fault_acknowledged", 1200), ("restore_requested", 9000),
                               ("restore_acknowledged", 10000), ("window_end", 14000))
        ]
        metadata = [
            {"kind": "partition", "at_ms": time, "started_ms": time - 100, "leader": 1,
             "isr": [0, 1, 2], "min_isr": 2, "replicas": 3}
            for time in (2000, 3000, 5000, 7000, 8000, 12000)
        ]
        self.assertEqual("injection_not_achieved",
                         summarize(self.manifest, events + metadata)["condition"]["classification"])
        self.assertEqual("indeterminate", summarize(self.manifest, events)["condition"]["classification"])


    def stage_scenario(self, scenario, stages, other_dispatch=False):
        events = [
            {"kind": "broker_stage", "id": "message", "stage": stage, "at_ms": time}
            for stage, time in stages
        ]
        if other_dispatch:
            events.append({"kind": "broker_stage", "id": "other", "stage": "execution_enter", "at_ms": 6000})
        events += [
            {"kind": "send", "id": "message", "at_ms": 1900},
            {"kind": "send_error", "id": "message", "error": "TimeoutException", "at_ms": 4100},
        ]
        events.extend({"kind": "partition", "leader": 1, "isr": [0, 1, 2],
                       "replicas": 3, "at_ms": timestamp} for timestamp in (3000, 5000, 7000))
        return assess_stall(sorted(events, key=lambda e: e["at_ms"]), {**self.manifest, "scenario": scenario})

    def test_execution_stall_stays_before_local_append(self):
        events = (("execution_enter", 2000), ("stall_enter", 2001), ("stall_release", 18001),
                  ("append_enter", 18002), ("append_complete", 18003))
        result = self.stage_scenario("execution-stall", events)
        self.assertEqual("achieved", result["classification"])
        self.assertTrue(result["client_finished_while_held"])
        self.assertEqual("indeterminate", self.stage_scenario("storage-stall", events, True)["classification"])

    def test_storage_requires_local_append_and_another_dispatch(self):
        events = (("execution_enter", 2000), ("append_enter", 2001), ("stall_enter", 2002),
                  ("stall_release", 18002), ("append_complete", 18003))
        self.assertEqual("achieved", self.stage_scenario("storage-stall", events, True)["classification"])
        self.assertEqual("indeterminate", self.stage_scenario("storage-stall", events)["classification"])
        self.assertEqual("indeterminate", self.stage_scenario("execution-stall", events)["classification"])


if __name__ == "__main__":
    unittest.main()
