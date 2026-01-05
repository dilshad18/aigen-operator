import os
import logging
import kopf
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
LOG = logging.getLogger("aigen-operator")
LOG.setLevel(LOG_LEVEL)

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
CRD_GROUP = "infra.whiz.ai"
CRD_VERSION = "v1"
CRD_PLURAL = "aigens"

OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "default")
CR_NAME = os.getenv("CR_NAME", "aigen")

RECONCILE_INTERVAL = int(os.getenv("RECONCILE_INTERVAL", "60"))

# -------------------------------------------------------------------
# Kubernetes Client
# -------------------------------------------------------------------
try:
    config.load_incluster_config()
    LOG.info("Using in-cluster Kubernetes config")
except Exception:
    config.load_kube_config()
    LOG.info("Using local kubeconfig")

core_v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
custom_api = client.CustomObjectsApi()

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def is_gpu_node(node) -> bool:
    """Detect usable GPU nodes."""
    if node.spec.unschedulable:
        return False

    for taint in node.spec.taints or []:
        if taint.effect in ("NoSchedule", "NoExecute"):
            return False

    for cond in node.status.conditions or []:
        if cond.type == "Ready" and cond.status != "True":
            return False

    labels = node.metadata.labels or {}
    if labels.get("nvidia.com/gpu.present") == "true":
        return True

    alloc = node.status.allocatable or {}
    try:
        return int(alloc.get("nvidia.com/gpu", "0")) > 0
    except ValueError:
        return False


def get_cr_spec():
    cr = custom_api.get_namespaced_custom_object(
        CRD_GROUP,
        CRD_VERSION,
        OPERATOR_NAMESPACE,
        CRD_PLURAL,
        CR_NAME,
    )
    return cr.get("spec", {})


def scale_deployment(name, namespace, replicas):
    body = {"spec": {"replicas": max(int(replicas), 0)}}
    try:
        apps_v1.patch_namespaced_deployment_scale(name, namespace, body)
        LOG.info(f"Scaled {name} → {replicas}")
    except ApiException as e:
        LOG.warning(f"Failed scaling {name}: {e.reason}")


def update_status(active_dep, target_ns, replicas, gpu_count, cpu_count):
    now = datetime.now(timezone.utc).isoformat()

    if gpu_count > 0:
        message = (
            f"GPU nodes detected: {gpu_count}, "
            f"CPU nodes detected: {cpu_count}"
        )
    else:
        message = f"No GPU nodes detected, CPU nodes detected: {cpu_count}"

    status = {
        "lastSyncTime": now,
        "activeDeployment": active_dep,
        "activeNamespace": target_ns,
        "activeReplicas": replicas,
        "gpuNodeCount": gpu_count,
        "cpuNodeCount": cpu_count,
        "message": message,
    }

    try:
        custom_api.patch_namespaced_custom_object_status(
            CRD_GROUP,
            CRD_VERSION,
            OPERATOR_NAMESPACE,
            CRD_PLURAL,
            CR_NAME,
            {"status": status},
            field_manager="aigen-operator",
        )
        LOG.info(f"Status updated: {message}")
    except ApiException as e:
        LOG.warning(f"Failed to update status: {e.reason}")

# -------------------------------------------------------------------
# Reconcile Logic
# -------------------------------------------------------------------
def reconcile():
    spec = get_cr_spec()

    target_ns = spec["targetNamespace"]
    cpu_dep = spec["cpuDeployment"]
    gpu_dep = spec["gpuDeployment"]
    replicas = spec.get("replicas", 1)

    nodes = core_v1.list_node().items
    gpu_nodes = [n for n in nodes if is_gpu_node(n)]

    gpu_count = len(gpu_nodes)
    cpu_count = len(nodes) - gpu_count

    LOG.info(
        f"Cluster nodes: total={len(nodes)}, gpu={gpu_count}, cpu={cpu_count}"
    )

    if gpu_count > 0:
        scale_deployment(gpu_dep, target_ns, replicas)
        scale_deployment(cpu_dep, target_ns, 0)
        update_status(gpu_dep, target_ns, replicas, gpu_count, cpu_count)
    else:
        scale_deployment(gpu_dep, target_ns, 0)
        scale_deployment(cpu_dep, target_ns, replicas)
        update_status(cpu_dep, target_ns, replicas, 0, cpu_count)

# -------------------------------------------------------------------
# Kopf Handlers (NO FINALIZERS)
# -------------------------------------------------------------------
@kopf.on.startup()
def startup(**_):
    LOG.info("AIGen operator started")
    reconcile()


@kopf.on.create(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
@kopf.on.update(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def on_cr_change(**_):
    LOG.info("CR change detected")
    reconcile()


@kopf.on.event("", "v1", "nodes")
def on_node_event(type, name, **_):
    LOG.info(f"Node event: {type} {name}")
    reconcile()


@kopf.timer(
    CRD_GROUP,
    CRD_VERSION,
    CRD_PLURAL,
    interval=RECONCILE_INTERVAL,
)
def periodic(**_):
    LOG.debug("Periodic reconcile")
    reconcile()
