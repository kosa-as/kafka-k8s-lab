"""Build and verify the port-scoped follower-loss fixture for the Docker Desktop lab."""

from ipaddress import IPv4Address


def partition_resource(name, namespace, follower_pods, leader_ip, interbroker_port, seconds):
    if len(set(follower_pods)) != 2 or len(follower_pods) != 2:
        raise ValueError("The RF=3 threshold test requires exactly two distinct follower Pods.")
    address = str(IPv4Address(leader_ip))
    if interbroker_port != 9094 or not 1 <= seconds <= 140:
        raise ValueError("Expected the lab interbroker listener and a bounded duration.")
    return {
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": "NetworkChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "action": "partition",
            "mode": "all",
            "direction": "to",
            "duration": f"{seconds}s",
            "selector": {"pods": {namespace: list(follower_pods)}},
            "externalTargets": [f"{address}:{interbroker_port}"],
        },
    }


def verify_pod_rules(status, resource):
    """Reject any extra destinations or reverse-direction rules for this fixture."""
    spec = status["spec"]
    metadata = resource["metadata"]
    source = f'{metadata["namespace"]}/{metadata["name"]}'
    target, port = resource["spec"]["externalTargets"][0].rsplit(":", 1)
    sets = [rule for rule in spec.get("ipsets", []) if rule["source"] == source]
    chains = [rule for rule in spec.get("iptables", []) if rule["source"] == source]
    if (len(chains) != 1 or chains[0]["direction"].lower() != "output"
            or not chains[0].get("ipsets")):
        raise ValueError(f"Expected one destination-filtered egress rule for {source}.")
    cidrs = [cidr for rule in sets for cidr in rule.get("cidrs", [])]
    addresses = [point for rule in sets for point in rule.get("cidrAndPorts", [])]
    if cidrs or addresses != [{"cidr": f"{target}/32", "port": int(port)}]:
        raise ValueError(f"Unexpected destination in the applied replication rule for {source}.")
