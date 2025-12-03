import os
import logging
import kopf
import threading
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# ---------------- Logging Setup ----------------
log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
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

# ---------------- Reconciliation Lock ----------------
reconcile_lock = threading.Lock()


# ---------------- Kubernetes Client ----------------
def init_kubernetes_client():
    """Initialize Kubernetes client with proper error handling."""
    try:
        config.load_incluster_config()
        LOG.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException as e:
        try:
            config.load_kube_config()
            LOG.info("Loaded local kubeconfig")
        except config.ConfigException as ke:
            LOG.error(f"Failed to load any Kubernetes config: {ke}")
            raise RuntimeError("Could not initialize Kubernetes client") from ke


init_kubernetes_client()

core_v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
custom_api = client.CustomObjectsApi()


# ---------------- Validation Functions ----------------
def validate_cr_spec(spec):
    """Validate CR spec has required fields and valid values."""
    required_fields = ["targetNamespace", "cpuDeployment", "gpuDeployment"]
    
    for field in required_fields:
        if field not in spec or not spec[field]:
            raise ValueError(f"Missing or empty required field: {field}")
    
    replicas = spec.get("replicas", 1)
    if not isinstance(replicas, int) or replicas < 0:
        raise ValueError(f"Invalid replicas value: {replicas}. Must be non-negative integer.")
    
    LOG.debug(f"CR spec validated: {spec}")
    return True


def validate_deployment_exists(name, namespace):
    """Check if deployment exists before attempting to scale it."""
    try:
        apps_v1.read_namespaced_deployment(name, namespace)
        LOG.debug(f"Deployment {name} exists in namespace {namespace}")
        return True
    except ApiException as e:
        if e.status == 404:
            LOG.error(f"Deployment {name} not found in namespace {namespace}")
            return False
        LOG.error(f"Error checking deployment {name}: {e}")
        raise


# ---------------- Helper Functions ----------------
def is_gpu_node(node):
    """
    Detect if a node is GPU-capable and schedulable.
    Checks:
    - Node is not cordoned/unschedulable
    - Node has no blocking taints
    - Node is in Ready state
    - Node has GPU resources (labels or allocatable)
    """
    node_name = node.metadata.name
    
    # Check if node is schedulable
    if node.spec.unschedulable:
        LOG.debug(f"Node {node_name} is unschedulable (cordoned)")
        return False
    
    # Check for blocking taints
    taints = node.spec.taints or []
    for taint in taints:
        if taint.effect in ["NoSchedule", "NoExecute"]:
            LOG.debug(f"Node {node_name} has blocking taint: {taint.key}={taint.value}:{taint.effect}")
            return False
    
    # Check node conditions - must be Ready
    conditions = node.status.conditions or []
    ready = False
    for condition in conditions:
        if condition.type == "Ready":
            ready = condition.status == "True"
            break
    
    if not ready:
        LOG.debug(f"Node {node_name} is not in Ready state")
        return False
    
    # Check for GPU presence via labels
    labels = node.metadata.labels or {}
    if labels.get("nvidia.com/gpu.present") == "true":
        LOG.debug(f"Node {node_name} has GPU label")
        return True
    
    # Check for GPU allocatable resources
    allocatable = node.status.allocatable or {}
    gpu_qty = allocatable.get("nvidia.com/gpu", "0")
    
    try:
        gpu_count = int(gpu_qty)
        if gpu_count > 0:
            LOG.debug(f"Node {node_name} has {gpu_count} allocatable GPUs")
            return True
    except (ValueError, TypeError):
        LOG.warning(f"Invalid GPU quantity for node {node_name}: {gpu_qty}")
        return False
    
    return False


def get_cr_spec():
    """Fetch the CR spec for the configured CR name and namespace."""
    try:
        cr = custom_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=OPERATOR_NAMESPACE,
            plural=CRD_PLURAL,
            name=CR_NAME,
        )
        spec = cr.get("spec", {})
        
        if not spec:
            raise ValueError(f"CR {CR_NAME} has no spec defined")
        
        # Validate the spec
        validate_cr_spec(spec)
        
        return spec
    except ApiException as e:
        if e.status == 404:
            LOG.error(f"CR {CR_NAME} not found in namespace {OPERATOR_NAMESPACE}")
        else:
            LOG.error(f"Error fetching CR: {e}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(ApiException),
    reraise=True
)
def scale_deployment(name, namespace, replicas):
    """
    Patch the deployment scale to the given replica count.
    Includes retry logic for transient failures.
    """
    replicas = max(int(replicas), 0)
    
    try:
        # Get current scale to check if change is needed
        current_scale = apps_v1.read_namespaced_deployment_scale(name, namespace)
        current_replicas = current_scale.spec.replicas if current_scale.spec.replicas is not None else 0
        
        if current_replicas == replicas:
            LOG.debug(f"Deployment {name} already at {replicas} replicas, skipping scale")
            return
        
        # Perform the scale operation
        body = {"spec": {"replicas": replicas}}
        apps_v1.patch_namespaced_deployment_scale(name, namespace, body)
        LOG.info(f"Scaled {name} in namespace {namespace} → {replicas} replicas")
        
    except ApiException as e:
        if e.status == 404:
            LOG.error(f"Deployment {name} not found in namespace {namespace}")
        else:
            LOG.warning(f"Failed to scale deployment {name}: {e.reason} (status: {e.status})")
        raise


def should_update_status(existing_status, new_values):
    """
    Check if status actually changed to avoid unnecessary updates.
    Ignores lastSyncTime for comparison.
    """
    for key in ["activeDeployment", "activeNamespace", "message", "activeReplicas"]:
        if existing_status.get(key) != new_values.get(key):
            return True
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(ApiException),
    reraise=True
)
def update_status(active_deployment, target_ns, message, replicas):
    """Update CR status without overwriting Kopf-managed fields."""
    now = datetime.now(timezone.utc).isoformat()

    try:
        cr = custom_api.get_namespaced_custom_object_status(
            CRD_GROUP, CRD_VERSION, OPERATOR_NAMESPACE, CRD_PLURAL, CR_NAME
        )
        existing_status = cr.get("status", {}) or {}
    except ApiException as e:
        if e.status == 404:
            LOG.warning(f"CR {CR_NAME} not found when updating status")
            return
        existing_status = {}

    new_values = {
        "lastSyncTime": now,
        "activeDeployment": active_deployment,
        "activeNamespace": target_ns,
        "message": message,
        "activeReplicas": replicas,
    }
    
    # Check if update is actually needed
    if not should_update_status(existing_status, new_values):
        LOG.debug("Status unchanged, skipping update")
        return
    
    # Merge with existing status
    existing_status.update(new_values)

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
        LOG.info(f"Updated CR status: deployment={active_deployment}, replicas={replicas}, message={message}")
    except ApiException as e:
        LOG.warning(f"Failed to update CR status: {e.reason} (status: {e.status})")
        raise


def remove_node_finalizers():
    """Remove Kopf finalizers from all nodes to prevent finalizer buildup."""
    try:
        nodes = core_v1.list_node()
        removed_count = 0
        
        for node in nodes.items:
            finalizers = node.metadata.finalizers or []
            kopf_finalizer = 'kopf.zalando.org/KopfFinalizerMarker'
            
            if kopf_finalizer in finalizers:
                new_finalizers = [f for f in finalizers if f != kopf_finalizer]
                body = {"metadata": {"finalizers": new_finalizers if new_finalizers else None}}
                
                try:
                    core_v1.patch_node(node.metadata.name, body)
                    removed_count += 1
                    LOG.debug(f"Removed Kopf finalizer from node {node.metadata.name}")
                except ApiException as e:
                    LOG.warning(f"Failed to remove finalizer from node {node.metadata.name}: {e}")
        
        if removed_count > 0:
            LOG.info(f"Removed Kopf finalizers from {removed_count} node(s)")
            
    except Exception as e:
        LOG.warning(f"Failed to clean node finalizers: {e}")


# ---------------- Reconciliation Logic ----------------
def reconcile():
    """
    Main reconciliation logic with locking to prevent concurrent execution.
    Decides which deployment (CPU or GPU) should be active based on available nodes.
    """
    # Prevent concurrent reconciliations
    if not reconcile_lock.acquire(blocking=False):
        LOG.debug("Reconciliation already in progress, skipping")
        return
    
    try:
        LOG.debug("Starting reconciliation")
        
        # Fetch and validate CR spec
        spec = get_cr_spec()
        target_ns = spec["targetNamespace"]
        cpu_name = spec["cpuDeployment"]
        gpu_name = spec["gpuDeployment"]
        replicas = spec.get("replicas", 1)
        
        # Validate both deployments exist
        cpu_exists = validate_deployment_exists(cpu_name, target_ns)
        gpu_exists = validate_deployment_exists(gpu_name, target_ns)
        
        if not cpu_exists or not gpu_exists:
            error_msg = f"Required deployment(s) not found: cpu={cpu_exists}, gpu={gpu_exists}"
            LOG.error(error_msg)
            update_status("none", target_ns, error_msg, 0)
            return
        
        # Check for GPU and CPU nodes
        nodes = core_v1.list_node().items
        gpu_nodes = [n for n in nodes if is_gpu_node(n)]
        gpu_node_count = len(gpu_nodes)
        cpu_node_count = len(nodes) - gpu_node_count
        
        LOG.info(f"Cluster status: {len(nodes)} total nodes, {gpu_node_count} GPU-capable and schedulable, {cpu_node_count} CPU-only")
        
        # Decide which deployment to activate
        if gpu_node_count > 0:
            LOG.info(f"GPU nodes available ({gpu_node_count}), activating GPU deployment")
            scale_deployment(gpu_name, target_ns, replicas)
            scale_deployment(cpu_name, target_ns, 0)
            update_status(
                gpu_name,
                target_ns,
                f"GPU nodes detected: {gpu_node_count}",
                replicas
            )
        else:
            LOG.info(f"No GPU nodes available ({cpu_node_count} CPU nodes), activating CPU deployment")
            scale_deployment(gpu_name, target_ns, 0)
            scale_deployment(cpu_name, target_ns, replicas)
            update_status(
                cpu_name,
                target_ns,
                f"CPU nodes detected: {cpu_node_count}",
                replicas
            )
        
        LOG.debug("Reconciliation completed successfully")
        
    except ValueError as e:
        LOG.error(f"Validation error during reconciliation: {e}")
        try:
            update_status("error", "", str(e), 0)
        except:
            pass
    except ApiException as e:
        LOG.error(f"Kubernetes API error during reconciliation: {e.reason} (status: {e.status})")
    except Exception as e:
        LOG.error(f"Unexpected error during reconciliation: {e}", exc_info=True)
    finally:
        reconcile_lock.release()


# ---------------- Kopf Event Hooks ----------------
@kopf.on.startup()
def startup(**_):
    """Operator startup handler."""
    LOG.info("=" * 60)
    LOG.info("AIGen Operator Starting")
    LOG.info(f"Operator Namespace: {OPERATOR_NAMESPACE}")
    LOG.info(f"CR Name: {CR_NAME}")
    LOG.info(f"Reconcile Interval: {RECONCILE_INTERVAL}s")
    LOG.info("=" * 60)
    
    # Clean up any lingering finalizers from previous runs
    remove_node_finalizers()
    
    # Perform initial reconciliation
    try:
        reconcile()
    except Exception as e:
        LOG.error(f"Initial reconciliation failed: {e}")


@kopf.on.cleanup()
def cleanup(**_):
    """Operator cleanup handler."""
    LOG.info("=" * 60)
    LOG.info("AIGen Operator Shutting Down")
    LOG.info("=" * 60)


@kopf.on.event('', 'v1', 'nodes')
def on_node_event(type, name, **_):
    """
    React immediately when nodes are added/removed/modified.
    This ensures quick response to infrastructure changes.
    """
    LOG.info(f"Node event detected: {type} on node '{name}' - triggering reconciliation")
    reconcile()
    
    # Clean up any finalizers that may have been added
    remove_node_finalizers()


@kopf.timer(CRD_GROUP, CRD_VERSION, CRD_PLURAL, interval=RECONCILE_INTERVAL, idle=RECONCILE_INTERVAL)
def periodic_on_cr(spec, **_):
    """
    Periodic reconciliation based on CR timer.
    Ensures system converges to desired state even if events are missed.
    """
    LOG.debug(f"Periodic reconciliation triggered (interval={RECONCILE_INTERVAL}s)")
    reconcile()


@kopf.on.create(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
@kopf.on.update(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def on_cr_change(spec, old, new, **_):
    """Reconcile when CR is created or updated."""
    if old is None:
        LOG.info(f"CR {CR_NAME} created - triggering reconciliation")
    else:
        # Log what changed
        changed_fields = []
        for key in spec.keys():
            if old.get(key) != new.get(key):
                changed_fields.append(key)
        
        if changed_fields:
            LOG.info(f"CR {CR_NAME} updated (changed: {', '.join(changed_fields)}) - triggering reconciliation")
        else:
            LOG.debug(f"CR {CR_NAME} updated but spec unchanged")
    
    reconcile()


@kopf.on.delete(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def on_cr_delete(**_):
    """Handle CR deletion."""
    LOG.info(f"CR {CR_NAME} deleted - operator will stop managing deployments")