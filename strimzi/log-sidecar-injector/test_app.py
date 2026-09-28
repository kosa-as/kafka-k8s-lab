import unittest

from app import SIDECAR_NAME, json_patch_for_pod


def kafka_pod(annotated=True):
    annotations = {"kafka.strimzi.io/log-sidecar": "enabled"} if annotated else {}
    return {
        "metadata": {
            "name": "kafka-kafka-0",
            "labels": {"strimzi.io/cluster": "kafka"},
            "annotations": annotations,
        },
        "spec": {
            "volumes": [{"name": "broker-runtime-logs"}],
            "containers": [{"name": "kafka"}],
        },
    }


class InjectorTests(unittest.TestCase):
    def test_injects_read_only_log_agent_for_opted_in_kafka_pod(self):
        patch = json_patch_for_pod(kafka_pod())

        self.assertEqual(len(patch), 1)
        self.assertEqual(patch[0]["op"], "add")
        sidecar = patch[0]["value"]
        self.assertEqual(sidecar["name"], SIDECAR_NAME)
        self.assertTrue(sidecar["volumeMounts"][0]["readOnly"])
        self.assertIn("/mnt/kafka-runtime-logs/server.log", sidecar["args"][0])

    def test_ignores_pod_without_opt_in_annotation(self):
        self.assertEqual(json_patch_for_pod(kafka_pod(annotated=False)), [])

    def test_does_not_duplicate_existing_sidecar(self):
        pod = kafka_pod()
        pod["spec"]["containers"].append({"name": SIDECAR_NAME})

        self.assertEqual(json_patch_for_pod(pod), [])


if __name__ == "__main__":
    unittest.main()
