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

# Suppress noisy kopf internal loggers -- they log "Handler succeeded" for every object event
logging.getLogger("kopf.objects").setLevel(logging.WARNING)
logging.getLogger("kopf.activities").setLevel(logging.WARNING)

LOG.info(f"Logging initialized at level: {log_level}")

# Configurable reconcile interval (default: 60 seconds)
RECONCILE_INTERVAL = int(os.getenv("RECONCILE_INTERVAL", "60"))

# ---------------- ANSI Colors for Terminal Output ----------------
# These make critical scaling events impossible to miss in kubectl logs
class Colors:
    RESET      = "\033[0m"
    BOLD       = "\033[1m"
    DIM        = "\033[2m"
    # Foreground
    RED        = "\033[91m"
    GREEN      = "\033[92m"
    YELLOW     = "\033[93m"
    BLUE       = "\033[94m"
    MAGENTA    = "\033[95m"
    CYAN       = "\033[96m"
    WHITE      = "\033[97m"
    # Background
    BG_GREEN   = "\033[42m"
    BG_RED     = "\033[41m"
    BG_YELLOW  = "\033[43m"
    BG_BLUE    = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN    = "\033[46m"

# Disable colors if NO_COLOR env var is set (https://no-color.org/)
if os.getenv("NO_COLOR"):
    for attr in vars(Colors):
        if not attr.startswith('_'):
            setattr(Colors, attr, "")

C = Colors

# ---------------- CRD Info ----------------
CRD_GROUP = "infra.whiz.ai"
CRD_VERSION = "v1"
CRD_PLURAL = "aigens"
OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "whiz-operator")
CR_NAME = os.getenv("CR_NAME", "aigen")
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "aigen-operator"

# ---------------- Reconciliation Lock ----------------
reconcile_lock = threading.Lock()

# ---------------- GPU Node State Tracking ----------------
# Tracks which nodes are currently "effective GPU nodes" so we can detect
# transitions (e.g. node gains GPU label/resources) and react immediately.
_known_gpu_nodes = set()
_gpu_tracking_lock = threading.Lock()


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
def _validate_gpu_memory_field(value, field_path):
    """Validate a requiredGpuMemoryGB field."""
    if value is None:
        raise ValueError(f"Missing required field: {field_path}")
    if not isinstance(value, (int, float)) or value < 0:
        raise ValueError(
            f"Invalid {field_path} value: {value}. "
            "Must be a non-negative number (in GB)."
        )


def validate_cr_spec(spec):
    """Validate CR spec has required fields and valid values."""
    # Top-level required fields
    if "targetNamespace" not in spec or not spec["targetNamespace"]:
        raise ValueError("Missing or empty required field: targetNamespace")

    # Validate llmDeployment
    llm = spec.get("llmDeployment")
    if not llm or not isinstance(llm, dict):
        raise ValueError("Missing or invalid required field: llmDeployment")
    if not llm.get("name"):
        raise ValueError("Missing or empty required field: llmDeployment.name")
    llm_replicas = llm.get("replicas", 1)
    if not isinstance(llm_replicas, int) or llm_replicas < 0:
        raise ValueError(f"Invalid llmDeployment.replicas value: {llm_replicas}. Must be non-negative integer.")
    _validate_gpu_memory_field(llm.get("requiredGpuMemoryGB"), "llmDeployment.requiredGpuMemoryGB")

    # Validate textProcessingDeployment
    tp = spec.get("textProcessingDeployment")
    if not tp or not isinstance(tp, dict):
        raise ValueError("Missing or invalid required field: textProcessingDeployment")
    if not tp.get("cpuDeployment"):
        raise ValueError("Missing or empty required field: textProcessingDeployment.cpuDeployment")
    if not tp.get("gpuDeployment"):
        raise ValueError("Missing or empty required field: textProcessingDeployment.gpuDeployment")
    tp_replicas = tp.get("replicas", 1)
    if not isinstance(tp_replicas, int) or tp_replicas < 0:
        raise ValueError(f"Invalid textProcessingDeployment.replicas value: {tp_replicas}. Must be non-negative integer.")
    _validate_gpu_memory_field(tp.get("requiredGpuMemoryGB"), "textProcessingDeployment.requiredGpuMemoryGB")

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


# ---------------- GPU Memory Helper Functions ----------------
def get_node_gpu_memory_mb(node):
    """
    Read the nvidia.com/gpu.memory label from a node.
    The label value is in MB.
    Returns the value as a float, or 0.0 if the label is missing/invalid.
    """
    labels = node.metadata.labels or {}
    gpu_memory_str = labels.get("nvidia.com/gpu.memory", "0")

    try:
        gpu_memory_mb = float(gpu_memory_str)
        if gpu_memory_mb < 0:
            LOG.warning(
                f"Node {node.metadata.name} has negative gpu.memory label: {gpu_memory_str} MB, treating as 0"
            )
            return 0.0
        return gpu_memory_mb
    except (ValueError, TypeError):
        LOG.warning(
            f"Node {node.metadata.name} has invalid nvidia.com/gpu.memory label: '{gpu_memory_str}'"
        )
        return 0.0


def get_node_gpu_count(node):
    """
    Get the number of GPUs on a node from allocatable resources or labels.
    Returns an integer count.
    """
    allocatable = node.status.allocatable or {}
    gpu_qty = allocatable.get("nvidia.com/gpu", "0")
    try:
        return max(int(gpu_qty), 0)
    except (ValueError, TypeError):
        return 0


def calculate_total_gpu_memory_gb(gpu_nodes):
    """
    Calculate the total GPU memory and GPU count across all given GPU nodes.
    Each node's nvidia.com/gpu.memory label reports per-GPU memory in MB.
    Total = sum(per_gpu_memory_mb * gpu_count) for each node, converted to GB.

    Returns:
        (total_gb, total_gpu_count, details) where details is a list of dicts with per-node info.
    """
    total_memory_mb = 0.0
    total_gpu_count = 0
    node_details = []

    for node in gpu_nodes:
        node_name = node.metadata.name
        per_gpu_memory_mb = get_node_gpu_memory_mb(node)
        gpu_count = get_node_gpu_count(node)
        node_total_mb = per_gpu_memory_mb * gpu_count

        total_memory_mb += node_total_mb
        total_gpu_count += gpu_count

        node_details.append({
            "name": node_name,
            "gpu_count": gpu_count,
            "per_gpu_memory_mb": per_gpu_memory_mb,
            "total_memory_mb": node_total_mb,
            "total_memory_gb": round(node_total_mb / 1024, 2),
        })

        LOG.debug(
            f"Node {node_name}: {gpu_count} GPU(s) × {per_gpu_memory_mb:.0f} MB = "
            f"{node_total_mb:.0f} MB ({node_total_mb / 1024:.2f} GB)"
        )

    total_gb = round(total_memory_mb / 1024, 2)
    return total_gb, total_gpu_count, node_details


# ---------------- Helper Functions ----------------
def is_gpu_node(node):
    """
    Detect if a node is GPU-capable and schedulable.
    Checks:
    - Node is not cordoned/unschedulable
    - Node has no blocking taints
    - Node is in Ready state
    - Node has nvidia.com/gpu.memory label with a value > 0
    """
    node_name = node.metadata.name

    # Check if node is schedulable
    if node.spec.unschedulable:
        LOG.debug(f"Node {node_name} is unschedulable (cordoned)")
        return False

    # Check for blocking taints (allow GPU-specific taints)
    GPU_TAINT_KEYS = {"nvidia.com/gpu", "nvidia.com/gpu.present", "nvidia.com/gpu.memory"}
    taints = node.spec.taints or []
    for taint in taints:
        if taint.effect in ["NoSchedule", "NoExecute"] and taint.key not in GPU_TAINT_KEYS:
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

    # Check for GPU via nvidia.com/gpu.memory label (source of truth)
    labels = node.metadata.labels or {}
    gpu_memory_str = labels.get("nvidia.com/gpu.memory", "0")
    try:
        if float(gpu_memory_str) > 0:
            LOG.debug(f"Node {node_name} has GPU memory label: {gpu_memory_str} MB")
            return True
    except (ValueError, TypeError):
        pass

    return False


def _is_effective_gpu_node_from_body(body):
    """
    Determine if a node is an effective GPU node from a kopf event body dict.
    This mirrors the logic of is_gpu_node() but works with the raw dict
    provided by kopf event handlers, enabling GPU state change detection
    without additional API calls.
    """
    node_name = body.get('metadata', {}).get('name', 'unknown')
    spec = body.get('spec', {})
    status = body.get('status', {})

    # Check if node is schedulable
    if spec.get('unschedulable'):
        LOG.debug(f"[gpu-track] Node {node_name} is unschedulable")
        return False

    # Check for blocking taints (allow GPU-specific taints)
    GPU_TAINT_KEYS = {"nvidia.com/gpu", "nvidia.com/gpu.present", "nvidia.com/gpu.memory"}
    taints = spec.get('taints') or []
    for taint in taints:
        if isinstance(taint, dict):
            effect = taint.get('effect', '')
            key = taint.get('key', '')
        else:
            effect = getattr(taint, 'effect', '')
            key = getattr(taint, 'key', '')
        if effect in ['NoSchedule', 'NoExecute'] and key not in GPU_TAINT_KEYS:
            LOG.debug(f"[gpu-track] Node {node_name} has blocking taint: {key}")
            return False

    # Check Ready condition
    conditions = status.get('conditions') or []
    ready = False
    for cond in conditions:
        cond_type = cond.get('type', '') if isinstance(cond, dict) else getattr(cond, 'type', '')
        cond_status = cond.get('status', '') if isinstance(cond, dict) else getattr(cond, 'status', '')
        if cond_type == 'Ready':
            ready = cond_status == 'True'
            break

    if not ready:
        LOG.debug(f"[gpu-track] Node {node_name} is not Ready")
        return False

    # Check for GPU via nvidia.com/gpu.memory label (source of truth)
    labels = body.get('metadata', {}).get('labels') or {}
    gpu_memory_str = labels.get('nvidia.com/gpu.memory', '0')
    try:
        if float(gpu_memory_str) > 0:
            LOG.debug(f"[gpu-track] Node {node_name} has GPU memory label: {gpu_memory_str} MB")
            return True
    except (ValueError, TypeError):
        pass

    return False


def _update_gpu_tracking(name, body, event_type):
    """
    Update GPU node tracking and return whether the GPU state changed.
    Returns:
        (bool) True if the node's effective GPU status changed, False otherwise.
    """
    with _gpu_tracking_lock:
        was_gpu = name in _known_gpu_nodes

        if event_type == 'DELETED':
            changed = was_gpu
            _known_gpu_nodes.discard(name)
            if changed:
                LOG.info(f"{C.RED}{C.BOLD}  ✗ [gpu-track] GPU node removed: {name}{C.RESET} (tracked GPU nodes: {len(_known_gpu_nodes)})")
            return changed

        is_gpu_now = _is_effective_gpu_node_from_body(body)

        if is_gpu_now and not was_gpu:
            _known_gpu_nodes.add(name)
            LOG.info(f"{C.GREEN}{C.BOLD}  ✓ [gpu-track] New GPU node detected: {name}{C.RESET} (tracked GPU nodes: {len(_known_gpu_nodes)})")
            return True
        elif not is_gpu_now and was_gpu:
            _known_gpu_nodes.discard(name)
            LOG.info(f"{C.YELLOW}{C.BOLD}  ⚠ [gpu-track] GPU node lost: {name}{C.RESET} (tracked GPU nodes: {len(_known_gpu_nodes)})")
            return True
        else:
            # Update tracking (no change)
            if is_gpu_now:
                _known_gpu_nodes.add(name)
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


def label_managed_deployment(name, namespace):
    """Inject the managed-by label onto a deployment so the drift watcher can filter on it."""
    try:
        body = {"metadata": {"labels": {MANAGED_BY_LABEL: MANAGED_BY_VALUE}}}
        apps_v1.patch_namespaced_deployment(name, namespace, body)
        LOG.debug(f"Labeled deployment {name} with {MANAGED_BY_LABEL}={MANAGED_BY_VALUE}")
    except ApiException as e:
        if e.status == 404:
            LOG.debug(f"Deployment {name} not found, skipping label injection")
        else:
            LOG.warning(f"Failed to label deployment {name}: {e.reason}")


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
            with _expected_replicas_lock:
                _expected_replicas[name] = replicas
            return

        # Record expected state before scaling to avoid drift false positives
        with _expected_replicas_lock:
            _expected_replicas[name] = replicas

        body = {"spec": {"replicas": replicas}}
        apps_v1.patch_namespaced_deployment_scale(name, namespace, body)

        # Colorful scaling log — stands out in kubectl logs
        if replicas > 0 and current_replicas == 0:
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_GREEN}{C.WHITE}"
                f"  ▲ SCALE UP ▲  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}  "
                f"{C.RESET}"
            )
        elif replicas == 0 and current_replicas > 0:
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_RED}{C.WHITE}"
                f"  ▼ SCALE DOWN ▼  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}  "
                f"{C.RESET}"
            )
        elif replicas > current_replicas:
            LOG.info(
                f"{C.GREEN}{C.BOLD}  ▲ SCALED  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}{C.RESET}"
            )
        elif replicas < current_replicas:
            LOG.info(
                f"{C.YELLOW}{C.BOLD}  ▼ SCALED  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}{C.RESET}"
            )

    except ApiException as e:
        if e.status == 404:
            LOG.error(f"{C.RED}{C.BOLD}  ✗ SCALE FAILED  {name}  │  Deployment not found in ns: {namespace}{C.RESET}")
        else:
            LOG.warning(f"{C.RED}  ✗ SCALE FAILED  {name}  │  {e.reason} (status: {e.status}){C.RESET}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(ApiException),
    reraise=True
)
def update_status(target_ns, message, deployments,
                  total_gpu_memory_gb=None, total_gpu_count=None,
                  gpu_node_count=None, cpu_node_count=None):
    """
    Update CR status with a deployments list and cluster info.

    Args:
        deployments: list of dicts, each with keys:
            workload, name, mode, replicas, requiredGpuMemoryGB, gpuMemoryMet
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

    # Build summary for the printer column using full deployment names
    summary_parts = []
    for d in deployments:
        summary_parts.append(f"{d['name']}({d['mode']}/{d['replicas']})")
    deployment_summary = "  ".join(summary_parts)

    new_values = {
        "lastSyncTime": now,
        "namespace": target_ns,
        "message": message,
        "deploymentSummary": deployment_summary,
        "cluster": {
            "totalGpuCount": total_gpu_count or 0,
            "totalGpuMemoryGB": total_gpu_memory_gb or 0,
            "gpuNodeCount": gpu_node_count or 0,
            "cpuNodeCount": cpu_node_count or 0,
            "gpuMemoryDisplay": f"{total_gpu_memory_gb or 0:.2f} GB",
        },
        "deployments": deployments,
    }

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
        LOG.info(f"Updated CR status: {deployment_summary}")
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


def _update_error_status(error_msg):
    """Write an error status to the CR. Used by reconcile exception handlers."""
    try:
        update_status(OPERATOR_NAMESPACE, error_msg, [
            {"workload": "llm", "name": "", "mode": "error", "replicas": 0,
             "requiredGpuMemoryGB": 0, "gpuMemoryMet": False},
            {"workload": "textProcessing", "name": "", "mode": "error", "replicas": 0,
             "requiredGpuMemoryGB": 0, "gpuMemoryMet": False},
        ])
    except Exception:
        pass


# ---------------- Reconciliation Logic ----------------
def reconcile():
    """
    Main reconciliation logic with locking to prevent concurrent execution.

    Manages two workloads, each with its own requiredGpuMemoryGB threshold:

      1. ai-llm (LLM):  GPU-only.
         - If total GPU memory >= llmDeployment.requiredGpuMemoryGB → scale UP.
         - Otherwise → scale to 0.

      2. ai-text-processing: Has a CPU and GPU variant.
         - If total GPU memory >= textProcessingDeployment.requiredGpuMemoryGB
           → GPU variant UP, CPU variant DOWN.
         - Otherwise → CPU variant UP, GPU variant DOWN.

    Each deployment is evaluated independently, so it's possible for the LLM
    to be off while text-processing runs on GPU (or vice-versa).
    """
    if not reconcile_lock.acquire(blocking=False):
        LOG.debug("Reconciliation already in progress, skipping")
        return

    try:
        LOG.debug("Starting reconciliation")

        # ---- Fetch and validate CR spec ----
        spec = get_cr_spec()
        target_ns = spec["targetNamespace"]

        llm_cfg = spec["llmDeployment"]
        llm_name = llm_cfg["name"]
        llm_replicas = llm_cfg.get("replicas", 1)
        llm_required_gb = llm_cfg["requiredGpuMemoryGB"]

        tp_cfg = spec["textProcessingDeployment"]
        tp_cpu_name = tp_cfg["cpuDeployment"]
        tp_gpu_name = tp_cfg["gpuDeployment"]
        tp_replicas = tp_cfg.get("replicas", 1)
        tp_required_gb = tp_cfg["requiredGpuMemoryGB"]

        # ---- Validate which deployments exist ----
        llm_exists = validate_deployment_exists(llm_name, target_ns)
        tp_cpu_exists = validate_deployment_exists(tp_cpu_name, target_ns)
        tp_gpu_exists = validate_deployment_exists(tp_gpu_name, target_ns)

        missing = []
        if not llm_exists:
            missing.append(f"llm (GPU)={llm_name}")
        if not tp_cpu_exists:
            missing.append(f"textProcessing (CPU)={tp_cpu_name}")
        if not tp_gpu_exists:
            missing.append(f"textProcessing (GPU)={tp_gpu_name}")

        if missing:
            LOG.warning(f"Missing deployment(s): {', '.join(missing)}")

        # ---- Inject managed-by labels onto existing deployments ----
        managed_names = {llm_name, tp_cpu_name, tp_gpu_name}
        for dep_name, exists in [(llm_name, llm_exists), (tp_cpu_name, tp_cpu_exists), (tp_gpu_name, tp_gpu_exists)]:
            if exists:
                label_managed_deployment(dep_name, target_ns)

        # ---- Clean stale entries from expected replicas tracker ----
        with _expected_replicas_lock:
            stale = [k for k in _expected_replicas if k not in managed_names]
            for k in stale:
                del _expected_replicas[k]

        # ---- Discover GPU nodes ----
        nodes = core_v1.list_node().items
        gpu_nodes = [n for n in nodes if is_gpu_node(n)]
        gpu_node_count = len(gpu_nodes)
        cpu_node_count = len(nodes) - gpu_node_count

        # ---- Calculate total GPU memory ----
        total_gpu_memory_gb, total_gpu_count, gpu_node_details = calculate_total_gpu_memory_gb(gpu_nodes)

        # ================================================================
        # SCALING DECISIONS (priority-based: LLM gets GPU first)
        # ================================================================

        # ---- LLM Decision (highest priority) ----
        if not llm_exists:
            llm_mode = "error"
            llm_target_replicas = 0
            llm_gpu_reserved = 0
            llm_gpu_met = False
            llm_message = f"Deployment {llm_name} not found"
        elif gpu_node_count > 0 and total_gpu_memory_gb >= llm_required_gb:
            llm_mode = "gpu"
            llm_target_replicas = llm_replicas
            llm_gpu_reserved = llm_required_gb
            llm_gpu_met = True
            llm_message = f"GPU memory met ({total_gpu_memory_gb:.2f}>={llm_required_gb:.2f} GB), scaled to {llm_replicas}"
        else:
            llm_mode = "off"
            llm_target_replicas = 0
            llm_gpu_reserved = 0
            llm_gpu_met = False
            if gpu_node_count > 0:
                llm_message = f"GPU memory insufficient ({total_gpu_memory_gb:.2f}<{llm_required_gb:.2f} GB), scaled to 0"
            else:
                llm_message = f"No GPU nodes available, scaled to 0"

        # ---- Text-Processing Decision (uses remaining GPU after LLM) ----
        remaining_gpu_gb = total_gpu_memory_gb - llm_gpu_reserved
        tp_gpu_met = (gpu_node_count > 0 and remaining_gpu_gb >= tp_required_gb)

        if not tp_cpu_exists and not tp_gpu_exists:
            tp_mode = "error"
            tp_active_name = tp_cpu_name
            tp_inactive_name = tp_gpu_name
            tp_target_replicas = 0
            tp_gpu_met = False
            tp_message = f"Deployments {tp_cpu_name} and {tp_gpu_name} not found"
        elif tp_gpu_met and tp_gpu_exists:
            tp_mode = "gpu"
            tp_active_name = tp_gpu_name
            tp_inactive_name = tp_cpu_name
            tp_target_replicas = tp_replicas
            tp_message = (
                f"Remaining GPU memory met ({remaining_gpu_gb:.2f}>={tp_required_gb:.2f} GB "
                f"after LLM reserved {llm_gpu_reserved:.2f} GB), using GPU variant"
            )
        elif tp_cpu_exists:
            tp_mode = "cpu"
            tp_active_name = tp_cpu_name
            tp_inactive_name = tp_gpu_name
            tp_target_replicas = tp_replicas
            if not tp_gpu_exists:
                tp_message = f"GPU deployment {tp_gpu_name} not found, using CPU variant"
            elif gpu_node_count > 0:
                tp_message = (
                    f"Remaining GPU memory insufficient ({remaining_gpu_gb:.2f}<{tp_required_gb:.2f} GB "
                    f"after LLM reserved {llm_gpu_reserved:.2f} GB), using CPU variant"
                )
            else:
                tp_message = f"No GPU nodes available, using CPU variant"
        else:
            tp_mode = "error"
            tp_active_name = tp_gpu_name
            tp_inactive_name = tp_cpu_name
            tp_target_replicas = 0
            tp_message = f"CPU deployment {tp_cpu_name} not found and GPU threshold not met"

        # ---- Reconciliation plan banner (deployments + thresholds only) ----
        B = f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
        R = C.RESET
        LOG.info(
            f"\n"
            f"{B}  ╔══════════════════════════════════════════════════════════════════════════╗  {R}\n"
            f"{B}  ║                       RECONCILIATION STATUS                               ║  {R}\n"
            f"{B}  ╠══════════════════════════════════════════════════════════════════════════╣  {R}\n"
            f"{B}  ║  WORKLOAD            │ DEPLOYMENT                │ REPLICAS │ MODE      ║  {R}\n"
            f"{B}  ║  ai-llm              │ {llm_name:<25} │ {llm_target_replicas:<8} │ {llm_mode:<9} ║  {R}\n"
            f"{B}  ║  ai-text-processing  │ {tp_active_name:<25} │ {tp_target_replicas:<8} │ {tp_mode:<9} ║  {R}\n"
            f"{B}  ║  ai-text-processing  │ {tp_inactive_name:<25} │ {'0':<8} │ {'off':<9} ║  {R}\n"
            f"{B}  ╠══════════════════════════════════════════════════════════════════════════╣  {R}\n"
            f"{B}  ║  Priority : LLM={llm_required_gb:.0f}GB ({'MET' if llm_gpu_met else 'NOT MET'})"
            f"  │  Remaining: {remaining_gpu_gb:.2f}GB"
            f"  │  TP={tp_required_gb:.0f}GB ({'MET' if tp_gpu_met else 'NOT MET'})"
            f"{' ' * max(0, 3 - len(f'{llm_required_gb:.0f}') - len(f'{tp_required_gb:.0f}'))}║  {R}\n"
            f"{B}  ╠══════════════════════════════════════════════════════════════════════════╣  {R}\n"
            f"{B}  ║  CLUSTER STATUS                                                          ║  {R}\n"
            f"{B}  ║  Nodes: {len(nodes):<5} │ GPU Nodes: {gpu_node_count:<5} │ CPU Nodes: {cpu_node_count:<5} │ GPUs: {total_gpu_count:<5} │ Mem: {total_gpu_memory_gb:.2f} GB  ║  {R}\n"
            f"{B}  ╚══════════════════════════════════════════════════════════════════════════╝  {R}"
        )
        if gpu_node_details:
            for detail in gpu_node_details:
                LOG.info(
                    f"{C.DIM}    └─ {detail['name']}: "
                    f"{detail['gpu_count']} GPU(s) × {detail['per_gpu_memory_mb']:.0f} MB = "
                    f"{detail['total_memory_gb']:.2f} GB{C.RESET}"
                )

        # ---- Execute scaling (per-deployment, skip missing, track failures) ----
        scale_errors = []
        deployments_to_scale = []
        if llm_exists:
            deployments_to_scale.append((llm_name, llm_target_replicas, "llm"))
        if tp_active_name == tp_gpu_name and tp_gpu_exists:
            deployments_to_scale.append((tp_active_name, tp_target_replicas, f"textProcessing({tp_mode})"))
        elif tp_active_name == tp_cpu_name and tp_cpu_exists:
            deployments_to_scale.append((tp_active_name, tp_target_replicas, f"textProcessing({tp_mode})"))
        if tp_inactive_name == tp_gpu_name and tp_gpu_exists:
            deployments_to_scale.append((tp_inactive_name, 0, "textProcessing(off)"))
        elif tp_inactive_name == tp_cpu_name and tp_cpu_exists:
            deployments_to_scale.append((tp_inactive_name, 0, "textProcessing(off)"))

        for dep_name, dep_replicas, dep_label in deployments_to_scale:
            try:
                scale_deployment(dep_name, target_ns, dep_replicas)
            except Exception as e:
                err = f"{dep_label}={dep_name}: {e}"
                scale_errors.append(err)
                LOG.error(f"{C.RED}{C.BOLD}  ✗ Scale failed for {dep_name}: {e}{C.RESET}")

        # ---- Build status message ----
        if missing and scale_errors:
            msg = f"Sync partial: missing={', '.join(missing)}; errors={'; '.join(scale_errors)}"
        elif missing:
            msg = f"Sync partial: missing={', '.join(missing)}"
        elif scale_errors:
            msg = f"Sync partial: {'; '.join(scale_errors)}"
        else:
            msg = "Sync successful"

        update_status(
            target_ns, msg,
            [
                {"workload": "llm", "name": llm_name, "mode": llm_mode,
                 "replicas": llm_target_replicas, "requiredGpuMemoryGB": llm_required_gb,
                 "gpuMemoryMet": llm_gpu_met, "message": llm_message},
                {"workload": "textProcessing", "name": tp_active_name, "mode": tp_mode,
                 "replicas": tp_target_replicas, "requiredGpuMemoryGB": tp_required_gb,
                 "gpuMemoryMet": tp_gpu_met, "message": tp_message},
            ],
            total_gpu_memory_gb=total_gpu_memory_gb,
            total_gpu_count=total_gpu_count,
            gpu_node_count=gpu_node_count,
            cpu_node_count=cpu_node_count,
        )

        if scale_errors:
            LOG.warning(f"{C.YELLOW}{C.BOLD}  ⚠ Reconciliation completed with errors{C.RESET}")
        else:
            LOG.info(f"{C.GREEN}{C.BOLD}  ✓ Reconciliation completed successfully{C.RESET}")

    except ValueError as e:
        LOG.error(f"{C.RED}{C.BOLD}  ✗ RECONCILE FAILED  │  Validation error: {e}{C.RESET}")
        _update_error_status(str(e))
    except ApiException as e:
        error_msg = f"K8s API error: {e.reason} (status: {e.status})"
        LOG.error(f"{C.RED}{C.BOLD}  ✗ RECONCILE FAILED  │  {error_msg}{C.RESET}")
        _update_error_status(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        LOG.error(f"{C.RED}{C.BOLD}  ✗ RECONCILE FAILED  │  {error_msg}{C.RESET}", exc_info=True)
        _update_error_status(error_msg)
    finally:
        reconcile_lock.release()


# ---------------- Kopf Event Hooks ----------------
@kopf.on.startup()
def startup(settings: kopf.OperatorSettings, **_):
    """Configure kopf settings and perform initial startup tasks."""
    settings.posting.level = logging.WARNING
    settings.watching.server_timeout = 270
    settings.watching.client_timeout = 300
    settings.execution.max_workers = 2
    settings.peering.standalone = True
    LOG.info(
        f"\n"
        f"{C.BOLD}{C.BG_CYAN}{C.WHITE}"
        f"  ╔══════════════════════════════════════════════════════════╗  {C.RESET}\n"
        f"{C.BOLD}{C.BG_CYAN}{C.WHITE}"
        f"  ║              AIGen Operator Starting                    ║  {C.RESET}\n"
        f"{C.BOLD}{C.BG_CYAN}{C.WHITE}"
        f"  ║  Namespace : {OPERATOR_NAMESPACE:<43}  ║  {C.RESET}\n"
        f"{C.BOLD}{C.BG_CYAN}{C.WHITE}"
        f"  ║  CR Name   : {CR_NAME:<43}  ║  {C.RESET}\n"
        f"{C.BOLD}{C.BG_CYAN}{C.WHITE}"
        f"  ║  Reconcile : {str(RECONCILE_INTERVAL) + 's':<43}  ║  {C.RESET}\n"
        f"{C.BOLD}{C.BG_CYAN}{C.WHITE}"
        f"  ╚══════════════════════════════════════════════════════════╝  {C.RESET}"
    )

    # Clean up any lingering finalizers from previous runs
    remove_node_finalizers()

    # Build initial GPU node tracking state
    try:
        nodes = core_v1.list_node().items
        with _gpu_tracking_lock:
            for node in nodes:
                if is_gpu_node(node):
                    _known_gpu_nodes.add(node.metadata.name)
        LOG.info(f"Initial GPU node tracking: {len(_known_gpu_nodes)} GPU node(s) detected: {_known_gpu_nodes or '{none}'}")

        # Log initial GPU memory info
        gpu_nodes = [n for n in nodes if is_gpu_node(n)]
        if gpu_nodes:
            total_gb, total_count, details = calculate_total_gpu_memory_gb(gpu_nodes)
            LOG.info(f"Initial total GPU memory: {total_gb:.2f} GB ({total_count} GPUs across {len(gpu_nodes)} node(s))")
            for d in details:
                LOG.info(
                    f"  └─ {d['name']}: {d['gpu_count']} GPU(s) × "
                    f"{d['per_gpu_memory_mb']:.0f} MB = {d['total_memory_gb']:.2f} GB"
                )
    except Exception as e:
        LOG.warning(f"Failed to initialize GPU node tracking: {e}")

    # Perform initial reconciliation
    try:
        reconcile()
    except Exception as e:
        LOG.error(f"Initial reconciliation failed: {e}")


@kopf.on.cleanup()
def cleanup(**_):
    """Operator cleanup handler."""
    LOG.info(
        f"\n"
        f"{C.BOLD}{C.BG_RED}{C.WHITE}"
        f"  ╔══════════════════════════════════════════════════════════╗  {C.RESET}\n"
        f"{C.BOLD}{C.BG_RED}{C.WHITE}"
        f"  ║              AIGen Operator Shutting Down                ║  {C.RESET}\n"
        f"{C.BOLD}{C.BG_RED}{C.WHITE}"
        f"  ╚══════════════════════════════════════════════════════════╝  {C.RESET}"
    )


@kopf.on.event('', 'v1', 'nodes')
def on_node_event(type, body, name, **_):
    """
    React to node events with GPU-aware debouncing.

    Only reacts to:
    - ADDED/DELETED: Always trigger immediate reconciliation.
    - MODIFIED with GPU state change: Trigger immediate reconciliation.
    All other MODIFIED events are ignored to avoid API server pressure.
    """
    if type is None:
        return

    gpu_state_changed = _update_gpu_tracking(name, body, type)

    if type in ['ADDED', 'DELETED']:
        color = C.GREEN if type == 'ADDED' else C.RED
        LOG.info(f"{color}{C.BOLD}  ● NODE {type}: {name}{C.RESET} — triggering immediate reconciliation")
        reconcile()
        remove_node_finalizers()

    elif type == 'MODIFIED' and gpu_state_changed:
        LOG.info(
            f"\n"
            f"{C.BOLD}{C.BG_YELLOW}{C.WHITE}"
            f"  ⚡ GPU STATE CHANGE DETECTED  │  Node: {name}  │  Reconciling NOW  "
            f"{C.RESET}"
        )
        reconcile()
        remove_node_finalizers()
    else:
        LOG.debug(f"Node event {type}: {name} — no GPU change, skipping")


# Track expected replica state to avoid reconcile loops from our own scaling
_expected_replicas = {}
_expected_replicas_lock = threading.Lock()


@kopf.on.event('apps', 'v1', 'deployments')
def on_deployment_event(name, namespace, body, type, **_):
    """Detect manual scaling drift on managed deployments and correct immediately."""
    if type is None:
        return

    labels = body.get("metadata", {}).get("labels", {})
    if labels.get(MANAGED_BY_LABEL) != MANAGED_BY_VALUE:
        return

    current_replicas = body.get("spec", {}).get("replicas")
    with _expected_replicas_lock:
        expected = _expected_replicas.get(name)

    if expected is not None and current_replicas == expected:
        return

    LOG.info(
        f"{C.YELLOW}{C.BOLD}  ⚠ DRIFT DETECTED  │  {name}  │  "
        f"expected={expected}  actual={current_replicas}  │  Reconciling{C.RESET}"
    )
    reconcile()


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
        LOG.info(f"CR {CR_NAME} created — triggering reconciliation")
    else:
        changed_fields = []
        for key in spec.keys():
            if old.get(key) != new.get(key):
                changed_fields.append(key)

        if changed_fields:
            LOG.info(f"CR {CR_NAME} updated (changed: {', '.join(changed_fields)}) — triggering reconciliation")
        else:
            LOG.debug(f"CR {CR_NAME} updated but spec unchanged")

    reconcile()


@kopf.on.delete(CRD_GROUP, CRD_VERSION, CRD_PLURAL)
def on_cr_delete(**_):
    """Handle CR deletion."""
    LOG.info(f"CR {CR_NAME} deleted — operator will stop managing deployments")
