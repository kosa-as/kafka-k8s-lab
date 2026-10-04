#!/usr/bin/env python3
import base64
import json
import logging
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


LOG = logging.getLogger("kafka-log-sidecar-injector")
SIDECAR_NAME = "log-agent"
LOG_VOLUME_NAME = "broker-runtime-logs"
HEALTH_SIDECAR_NAME = "health-sidecar"
HEALTH_VOLUME_NAME = "kafka-health"
HEALTH_MOUNT_PATH = "/var/run/kafka-health"
HEALTH_STATUS_PATH = f"{HEALTH_MOUNT_PATH}/status"


def _health_sidecar(test_status="healthy"):
    if test_status not in {"healthy", "failure"}:
        test_status = "healthy"
    return {
        "name": HEALTH_SIDECAR_NAME,
        "image": "quay.io/strimzi/kafka:1.2.0-kafka-4.3.1",
        "imagePullPolicy": "IfNotPresent",
        "command": ["/usr/bin/sh", "-c"],
        "args": [
            "status_file=\"${KAFKA_HEALTH_STATUS_FILE:-/var/run/kafka-health/status}\"; "
            "status=\"${KAFKA_HEALTH_TEST_STATUS:-healthy}\"; "
            "while :; do "
            "now=\"$(date +%s)\"; "
            "tmp=\"${status_file}.$$\"; "
            "{ printf 'status=%s\\n' \"$status\"; printf 'updated_at=%s\\n' \"$now\"; } > \"$tmp\" "
            "&& mv -f \"$tmp\" \"$status_file\"; "
            "sleep 5; "
            "done"
        ],
        "env": [
            {"name": "KAFKA_HEALTH_STATUS_FILE", "value": HEALTH_STATUS_PATH},
            {"name": "KAFKA_HEALTH_TEST_STATUS", "value": test_status},
        ],
        "resources": {
            "requests": {"cpu": "10m", "memory": "16Mi"},
            "limits": {"cpu": "100m", "memory": "64Mi"},
        },
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 1001,
            "runAsGroup": 1001,
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [
            {
                "name": HEALTH_VOLUME_NAME,
                "mountPath": HEALTH_MOUNT_PATH,
                "readOnly": False,
            }
        ],
    }


def json_patch_for_pod(pod):
    metadata = pod.get("metadata", {})
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})
    if annotations.get("kafka.strimzi.io/log-sidecar") != "enabled":
        return []
    if labels.get("strimzi.io/cluster") != "kafka":
        return []

    spec = pod.get("spec", {})
    containers = spec.get("containers", [])
    volumes = spec.get("volumes", [])
    patches = []

    kafka_index = next(
        (index for index, container in enumerate(containers) if container.get("name") == "kafka"),
        None,
    )
    if kafka_index is None:
        raise ValueError("annotated Kafka Pod is missing kafka container")

    if not any(volume.get("name") == HEALTH_VOLUME_NAME for volume in volumes):
        health_volume = {"name": HEALTH_VOLUME_NAME, "emptyDir": {}}
        if "volumes" in spec:
            patches.append({"op": "add", "path": "/spec/volumes/-", "value": health_volume})
        else:
            patches.append({"op": "add", "path": "/spec/volumes", "value": [health_volume]})

    kafka_mounts = containers[kafka_index].get("volumeMounts", [])
    health_mount_index = next(
        (index for index, mount in enumerate(kafka_mounts) if mount.get("name") == HEALTH_VOLUME_NAME),
        None,
    )
    kafka_mount = {
        "name": HEALTH_VOLUME_NAME,
        "mountPath": HEALTH_MOUNT_PATH,
        "readOnly": True,
    }
    mount_base = f"/spec/containers/{kafka_index}/volumeMounts"
    if health_mount_index is None:
        if "volumeMounts" in containers[kafka_index]:
            patches.append({"op": "add", "path": f"{mount_base}/-", "value": kafka_mount})
        else:
            patches.append({"op": "add", "path": mount_base, "value": [kafka_mount]})
    elif kafka_mounts[health_mount_index].get("readOnly") is not True:
        patches.append({
            "op": "replace",
            "path": f"{mount_base}/{health_mount_index}/readOnly",
            "value": True,
        })

    if not any(container.get("name") == SIDECAR_NAME for container in containers):
        if not any(volume.get("name") == LOG_VOLUME_NAME for volume in volumes):
            raise ValueError("annotated Kafka Pod is missing broker-runtime-logs volume")
        patches.append({
            "op": "add",
            "path": "/spec/containers/-",
            "value": {
                "name": SIDECAR_NAME,
                "image": "quay.io/strimzi/kafka:1.2.0-kafka-4.3.1",
                "imagePullPolicy": "IfNotPresent",
                "command": ["/usr/bin/sh", "-c"],
                "args": [
                    "until [ -f /mnt/kafka-runtime-logs/server.log ]; do sleep 1; done; "
                    "exec /usr/bin/tail -n+1 -F /mnt/kafka-runtime-logs/server.log"
                ],
                "resources": {
                    "requests": {"cpu": "10m", "memory": "16Mi"},
                    "limits": {"cpu": "100m", "memory": "64Mi"},
                },
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 1001,
                    "runAsGroup": 1001,
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "volumeMounts": [
                    {
                        "name": LOG_VOLUME_NAME,
                        "mountPath": "/mnt/kafka-runtime-logs",
                        "readOnly": True,
                    }
                ],
            },
        })

    if not any(container.get("name") == HEALTH_SIDECAR_NAME for container in containers):
        patches.append({
            "op": "add",
            "path": "/spec/containers/-",
            "value": _health_sidecar(annotations.get("kafka.strimzi.io/health-test-status", "healthy")),
        })

    return patches


class AdmissionHandler(BaseHTTPRequestHandler):
    server_version = "KafkaLogSidecarInjector/1.0"

    def do_GET(self):
        if self.path == "/healthz":
            self._write_json({"status": "ok"})
            return
        self.send_error(404)

    def do_POST(self):
        if urlsplit(self.path).path != "/mutate":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            review = json.loads(self.rfile.read(length))
            request = review.get("request", {})
            uid = request.get("uid", "")
            patches = json_patch_for_pod(request.get("object", {}))
            response = {
                "uid": uid,
                "allowed": True,
            }
            if patches:
                encoded = base64.b64encode(json.dumps(patches).encode()).decode()
                response["patchType"] = "JSONPatch"
                response["patch"] = encoded
                LOG.info("injected %s into Pod %s", SIDECAR_NAME, request.get("object", {}).get("metadata", {}).get("name"))
            body = {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "response": response}
            self._write_json(body)
        except ValueError as exc:
            self._write_json(
                {
                    "apiVersion": "admission.k8s.io/v1",
                    "kind": "AdmissionReview",
                    "response": {
                        "uid": locals().get("uid", ""),
                        "allowed": False,
                        "status": {"code": 400, "message": str(exc)},
                    },
                }
            )
        except Exception:
            LOG.exception("failed to process admission request")
            self.send_error(500)

    def _write_json(self, body):
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        LOG.info("%s - %s", self.address_string(), fmt % args)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
    server = ThreadingHTTPServer(("0.0.0.0", 8443), AdmissionHandler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    LOG.info("listening on https://0.0.0.0:8443/mutate")
    server.serve_forever()


if __name__ == "__main__":
    main()
