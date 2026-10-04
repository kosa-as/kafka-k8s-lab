import unittest

from app import HEALTH_SIDECAR_NAME, HEALTH_VOLUME_NAME, SIDECAR_NAME, json_patch_for_pod


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
    def test_injects_health_volume_mounts_and_both_sidecars(self):
        patch = json_patch_for_pod(kafka_pod())

        volume_values = [item["value"] for item in patch if item["path"] == "/spec/volumes/-"]
        self.assertEqual([value["name"] for value in volume_values], [HEALTH_VOLUME_NAME])

        kafka_mount_patches = [
            item for item in patch if item["path"].startswith("/spec/containers/0/volumeMounts")
        ]
        self.assertEqual(len(kafka_mount_patches), 1)
        kafka_mount_value = kafka_mount_patches[0]["value"]
        kafka_mount = kafka_mount_value[0] if isinstance(kafka_mount_value, list) else kafka_mount_value
        self.assertEqual(kafka_mount["name"], HEALTH_VOLUME_NAME)
        self.assertTrue(kafka_mount["readOnly"])

        sidecars = [item["value"] for item in patch if item["path"] == "/spec/containers/-"]
        self.assertEqual({sidecar["name"] for sidecar in sidecars}, {SIDECAR_NAME, HEALTH_SIDECAR_NAME})

        log_agent = next(sidecar for sidecar in sidecars if sidecar["name"] == SIDECAR_NAME)
        self.assertTrue(log_agent["volumeMounts"][0]["readOnly"])
        self.assertIn("/mnt/kafka-runtime-logs/server.log", log_agent["args"][0])

        health_sidecar = next(sidecar for sidecar in sidecars if sidecar["name"] == HEALTH_SIDECAR_NAME)
        self.assertFalse(health_sidecar["volumeMounts"][0]["readOnly"])
        self.assertIn("mv", health_sidecar["args"][0])
        self.assertIn("sleep 5", health_sidecar["args"][0])

    def test_existing_log_agent_still_gets_health_injection(self):
        pod = kafka_pod()
        pod["spec"]["containers"].append({"name": SIDECAR_NAME})

        patch = json_patch_for_pod(pod)
        self.assertTrue(any(item["path"] == "/spec/volumes/-" for item in patch))
        self.assertTrue(any(item["path"] == "/spec/containers/-" and item["value"]["name"] == HEALTH_SIDECAR_NAME for item in patch))

    def test_health_test_annotation_controls_sidecar_fixture_state(self):
        pod = kafka_pod()
        pod["metadata"]["annotations"]["kafka.strimzi.io/health-test-status"] = "failure"

        patch = json_patch_for_pod(pod)
        health = next(item["value"] for item in patch if item["path"] == "/spec/containers/-" and item["value"]["name"] == HEALTH_SIDECAR_NAME)
        self.assertEqual(
            next(env["value"] for env in health["env"] if env["name"] == "KAFKA_HEALTH_TEST_STATUS"),
            "failure",
        )

    def test_ignores_pod_without_opt_in_annotation(self):
        self.assertEqual(json_patch_for_pod(kafka_pod(annotated=False)), [])

    def test_does_not_duplicate_existing_sidecar(self):
        pod = kafka_pod()
        pod["spec"]["containers"].append({"name": SIDECAR_NAME})
        pod["spec"]["containers"].append({"name": HEALTH_SIDECAR_NAME})
        pod["spec"]["volumes"].append({"name": HEALTH_VOLUME_NAME, "emptyDir": {}})
        pod["spec"]["containers"][0]["volumeMounts"] = [
            {"name": HEALTH_VOLUME_NAME, "mountPath": "/var/run/kafka-health", "readOnly": True}
        ]

        self.assertEqual(json_patch_for_pod(pod), [])


if __name__ == "__main__":
    unittest.main()
