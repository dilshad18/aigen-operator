import os
import logging
import kopf
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("aigen-operator")

CRD_GROUP = "infra.whiz.ai"
CRD_VERSION = "v1"
CRD_PLURAL = "aigens"
OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "whiz-operator")
CR_NAME = os.getenv("CR_NAME", "aigen")

try:
    config.load_incluster_config()
except:
    config.load_kube_config()

core_v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
custom_api = client.CustomObjectsApi()


def is_gpu_node(node):
    labels = node.metadata.labels or {}
    alloc = node.status.allocatable or {}

    # Case 1: GPU label present
    if labels.get("nvidia.com/gpu.present") == "true":
        return True

    # Case 2: Actual GPU allocatable resource detected
    gpu_qty = alloc.get("nvidia.com/gpu", "0")
    try:
        return int(gpu_qty) > 0
    except ValueError:
        return False



def get_node_counts():
    gpu = cpu = 0
    nodes = core_v1.list_node().items
    for node in nodes:
        if is_gpu_node(node):
            gpu += 1
        else:
            cpu += 1
    return gpu, cpu


def get_cr_spec():
    cr = custom_api.get_namespaced_custom_object(
        group=CRD_GROUP,
        version=CRD_VERSION,
        namespace=OPERATOR_NAMESPACE,
        plural=CRD_PLURAL,
        name=CR_NAME,
    )
    return cr.get("spec", {})


def scale_deployment(name, namespace, replicas):
    replicas = max(int(replicas), 0)
    body = {"spec": {"replicas": replicas}}
    try:
        apps_v1.patch_namespaced_deployment_scale(name, namespace, body)
        LOG.info(f"Scaled {name} → {replicas}")
    except ApiException as e:
        LOG.warning(f"Failed scaling {name}: {e}")


def update_status(active_deployment, target_ns, reason):
    now = datetime.now(timezone.utc).isoformat()
    status_body = {
        "status": {
            "lastSyncTime": now,
            "activeDeployment": active_deployment,
            "activeNamespace": target_ns,
            "reason": reason,
        }
    }
    try:
        custom_api.patch_namespaced_custom_object_status(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=OPERATOR_NAMESPACE,
            plural=CRD_PLURAL,
            name=CR_NAME,
            body=status_body,
        )
    except ApiException as e:
        LOG.warning(f"Failed to update status: {e}")


def reconcile():
    spec = get_cr_spec()
    target_ns = spec["targetNamespace"]
    cpu_name = spec["whizCpuDeployment"]
    gpu_name = spec["whizGpuDeployment"]

    gpu_nodes, cpu_nodes = get_node_counts()

    if gpu_nodes > 0:
        scale_deployment(gpu_name, target_ns, gpu_nodes)
        scale_deployment(cpu_name, target_ns, 0)
        update_status(gpu_name, target_ns, "GPU nodes detected")
    else:
        scale_deployment(gpu_name, target_ns, 0)
        scale_deployment(cpu_name, target_ns, cpu_nodes)
        update_status(cpu_name, target_ns, "No GPU nodes detected")


@kopf.on.startup()
def startup(**_):
    LOG.info("AIGen Operator Started")


# Watch for Node changes (core/v1/nodes)
@kopf.on.event('', 'v1', 'nodes')
def on_node_event(**_):
    reconcile()

# Periodic reconcile every 60 seconds
@kopf.timer('', 'v1', 'nodes', interval=60)
def periodic(**_):
    reconcile()



# Watch CR updates
@kopf.on.create(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
@kopf.on.update(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def on_cr_change(**_):
    reconcile()

