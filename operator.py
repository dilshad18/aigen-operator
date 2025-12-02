import os
import logging
import kopf
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.rest import ApiException


# ---------------- Logging Setup ----------------
log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig()
logging.getLogger().setLevel(log_level)

LOG = logging.getLogger("aigen-operator")
LOG.setLevel(log_level)
LOG.info(f"Logging initialized at level: {log_level}")

# Configurable reconcile interval (default: 60 seconds)
RECONCILE_INTERVAL = int(os.getenv("RECONCILE_INTERVAL", "60"))

# ---------------- CRD Info ----------------
CRD_GROUP = "infra.whiz.ai"
CRD_VERSION = "v1"
CRD_PLURAL = "aigens"
OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "whiz-operator")
CR_NAME = os.getenv("CR_NAME", "aigen")


# ---------------- Kubernetes Client ----------------
try:
    config.load_incluster_config()
except:
    config.load_kube_config()

core_v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
custom_api = client.CustomObjectsApi()


# ---------------- Helper Functions ----------------
def is_gpu_node(node):
    """Detect if a node is GPU-capable based on labels or allocatable resources."""
    labels = node.metadata.labels or {}
    alloc = node.status.allocatable or {}

    if labels.get("nvidia.com/gpu.present") == "true":
        return True

    gpu_qty = alloc.get("nvidia.com/gpu", "0")
    try:
        return int(gpu_qty) > 0
    except ValueError:
        return False


def get_cr_spec():
    """Fetch the CR spec for the configured CR name and namespace."""
    cr = custom_api.get_namespaced_custom_object(
        group=CRD_GROUP,
        version=CRD_VERSION,
        namespace=OPERATOR_NAMESPACE,
        plural=CRD_PLURAL,
        name=CR_NAME,
    )
    return cr.get("spec", {})


def scale_deployment(name, namespace, replicas):
    """Patch the deployment scale to the given replica count."""
    body = {"spec": {"replicas": max(int(replicas), 0)}}
    try:
        apps_v1.patch_namespaced_deployment_scale(name, namespace, body)
        LOG.info(f"Scaled {name} → {replicas}")
    except ApiException as e:
        LOG.warning(f"Failed scaling {name}: {e}")


def update_status(active_deployment, target_ns, reason, replicas):
    """Update CR status without overwriting Kopf-managed fields."""
    now = datetime.now(timezone.utc).isoformat()

    try:
        cr = custom_api.get_namespaced_custom_object_status(
            CRD_GROUP, CRD_VERSION, OPERATOR_NAMESPACE, CRD_PLURAL, CR_NAME
        )
        existing_status = cr.get("status", {}) or {}
    except ApiException:
        existing_status = {}

    existing_status.update({
        "lastSyncTime": now,
        "activeDeployment": active_deployment,
        "activeNamespace": target_ns,
        "reason": reason,
        "activeReplicas": replicas,
    })

    try:
        custom_api.patch_namespaced_custom_object_status(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=OPERATOR_NAMESPACE,
            plural=CRD_PLURAL,
            name=CR_NAME,
            body={"status": existing_status},
            field_manager="aigen-operator",
        )
        LOG.debug(f"Updated CR status: {existing_status}")
    except ApiException as e:
        LOG.warning(f"Failed to update status: {e}")


# ---------------- Reconciliation Logic ----------------
def reconcile():
    """Main reconciliation logic — decide which deployment to scale."""
    spec = get_cr_spec()
    target_ns = spec["targetNamespace"]
    cpu_name = spec["cpuDeployment"]
    gpu_name = spec["gpuDeployment"]
    replicas = spec.get("replicas", 1)

    nodes = core_v1.list_node().items
    gpu_exists = any(is_gpu_node(n) for n in nodes)

    if gpu_exists:
        LOG.info("GPU node detected — activating GPU deployment.")
        scale_deployment(gpu_name, target_ns, replicas)
        scale_deployment(cpu_name, target_ns, 0)
        update_status(gpu_name, target_ns, "GPU nodes detected", replicas)
    else:
        LOG.info("CPU node detected — activating CPU deployment.")
        scale_deployment(gpu_name, target_ns, 0)
        scale_deployment(cpu_name, target_ns, replicas)
        update_status(cpu_name, target_ns, "CPU nodes detected", replicas)


# ---------------- Kopf Event Hooks ----------------
@kopf.on.startup()
def startup(**_):
    LOG.info("AIGen Operator Started")


@kopf.on.event('', 'v1', 'nodes', use_finalizer=False)
def on_node_event(**_):
    """React immediately when nodes are added/removed."""
    LOG.debug("Node event detected — triggering reconciliation.")
    reconcile()


@kopf.timer('', 'v1', 'nodes', interval=RECONCILE_INTERVAL)
def periodic(**_):
    """Periodic sync in case of missed events or transient errors."""
    LOG.debug(f"Periodic reconciliation triggered (interval={RECONCILE_INTERVAL}s).")
    reconcile()

@kopf.on.create(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
@kopf.on.update(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def on_cr_change(**_):
    """Reconcile when CR changes."""
    LOG.info("CR created or updated — triggering reconciliation.")
    reconcile()
@kopf.on.delete('', 'v1', 'nodes')
def on_node_delete(meta, **kwargs):
    node_name = meta['name']
    LOG.info(f"Node {node_name} is being deleted — removing Kopf finalizer if present.")

    # Remove Kopf finalizer
    try:
        node = core_v1.read_node(node_name)
        finalizers = node.metadata.finalizers or []
        if 'kopf.zalando.org/KopfFinalizerMarker' in finalizers:
            finalizers.remove('kopf.zalando.org/KopfFinalizerMarker')
            core_v1.patch_node(node_name, {"metadata": {"finalizers": finalizers}})
            LOG.info(f"Removed Kopf finalizer from {node_name}")
    except Exception as e:
        LOG.warning(f"Failed to remove finalizer from {node_name}: {e}")

