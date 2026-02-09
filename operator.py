import os
import logging
import kopf
import time
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

# ---------------- Reconciliation Lock ----------------
reconcile_lock = threading.Lock()

# ---------------- Node Event Debouncing ----------------
last_reconcile_time = 0
MIN_RECONCILE_INTERVAL = 30  # Minimum seconds between non-GPU-change node-triggered reconciles

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

    # Check for blocking taints
    taints = spec.get('taints') or []
    for taint in taints:
        if isinstance(taint, dict):
            effect = taint.get('effect', '')
        else:
            effect = getattr(taint, 'effect', '')
        if effect in ['NoSchedule', 'NoExecute']:
            LOG.debug(f"[gpu-track] Node {node_name} has blocking taint")
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

    # Check GPU label
    labels = body.get('metadata', {}).get('labels') or {}
    if labels.get('nvidia.com/gpu.present') == 'true':
        LOG.debug(f"[gpu-track] Node {node_name} has GPU label")
        return True

    # Check GPU allocatable resources
    allocatable = status.get('allocatable') or {}
    gpu_qty = allocatable.get('nvidia.com/gpu', '0')
    try:
        if int(gpu_qty) > 0:
            LOG.debug(f"[gpu-track] Node {node_name} has allocatable GPUs")
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

        # Colorful scaling log — stands out in kubectl logs
        if replicas > 0 and current_replicas == 0:
            # Scale UP from zero — big green banner
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_GREEN}{C.WHITE}"
                f"  ▲ SCALE UP ▲  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}  "
                f"{C.RESET}"
            )
        elif replicas == 0 and current_replicas > 0:
            # Scale DOWN to zero — big red banner
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_RED}{C.WHITE}"
                f"  ▼ SCALE DOWN ▼  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}  "
                f"{C.RESET}"
            )
        elif replicas > current_replicas:
            # Scale UP (partial) — green text
            LOG.info(
                f"{C.GREEN}{C.BOLD}  ▲ SCALED  {name}  │  {current_replicas} → {replicas} replicas  │  ns: {namespace}{C.RESET}"
            )
        elif replicas < current_replicas:
            # Scale DOWN (partial) — yellow text
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

    # Always update to reflect latest sync time
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

        # ---- Colorful cluster status ----
        LOG.info(
            f"{C.CYAN}{C.BOLD}  CLUSTER STATUS  "
            f"{C.RESET}{C.CYAN}│  "
            f"Total: {len(nodes)}  │  "
            f"{C.GREEN}GPU: {gpu_node_count}{C.CYAN}  │  "
            f"{C.BLUE}CPU: {cpu_node_count}"
            f"{C.RESET}"
        )

        # Decide which deployment to activate
        if gpu_node_count > 0:
            # ---- GPU MODE banner ----
            gpu_node_names = [n.metadata.name for n in gpu_nodes]
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
                f"  ╔══════════════════════════════════════════════════════════╗  {C.RESET}\n"
                f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
                f"  ║         SWITCHING TO GPU MODE                           ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
                f"  ║  Active : {gpu_name:<46}  ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
                f"  ║  Replicas: {replicas:<45}  ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
                f"  ║  GPU Nodes: {', '.join(gpu_node_names):<44}  ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_MAGENTA}{C.WHITE}"
                f"  ╚══════════════════════════════════════════════════════════╝  {C.RESET}"
            )
            scale_deployment(gpu_name, target_ns, replicas)
            scale_deployment(cpu_name, target_ns, 0)
            update_status(
                gpu_name,
                target_ns,
                f"GPU nodes: {gpu_node_count}, CPU nodes: {cpu_node_count}",
                replicas
            )
        else:
            # ---- CPU MODE banner ----
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_BLUE}{C.WHITE}"
                f"  ╔══════════════════════════════════════════════════════════╗  {C.RESET}\n"
                f"{C.BOLD}{C.BG_BLUE}{C.WHITE}"
                f"  ║         SWITCHING TO CPU MODE                           ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_BLUE}{C.WHITE}"
                f"  ║  Active : {cpu_name:<46}  ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_BLUE}{C.WHITE}"
                f"  ║  Replicas: {replicas:<45}  ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_BLUE}{C.WHITE}"
                f"  ║  GPU Nodes: {'0 (none available)':<44}  ║  {C.RESET}\n"
                f"{C.BOLD}{C.BG_BLUE}{C.WHITE}"
                f"  ╚══════════════════════════════════════════════════════════╝  {C.RESET}"
            )
            scale_deployment(gpu_name, target_ns, 0)
            scale_deployment(cpu_name, target_ns, replicas)
            update_status(
                cpu_name,
                target_ns,
                f"GPU nodes: {gpu_node_count}, CPU nodes: {cpu_node_count}",
                replicas
            )

        LOG.info(f"{C.GREEN}{C.BOLD}  ✓ Reconciliation completed successfully{C.RESET}")

    except ValueError as e:
        LOG.error(f"{C.RED}{C.BOLD}  ✗ RECONCILE FAILED  │  Validation error: {e}{C.RESET}")
        try:
            update_status("error", "", str(e), 0)
        except Exception:
            pass
    except ApiException as e:
        LOG.error(f"{C.RED}{C.BOLD}  ✗ RECONCILE FAILED  │  K8s API: {e.reason} (status: {e.status}){C.RESET}")
    except Exception as e:
        LOG.error(f"{C.RED}{C.BOLD}  ✗ RECONCILE FAILED  │  Unexpected: {e}{C.RESET}", exc_info=True)
    finally:
        reconcile_lock.release()


# ---------------- Kopf Event Hooks ----------------
@kopf.on.startup()
def startup(**_):
    """Operator startup handler."""
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

    Strategy:
    - ADDED/DELETED: Always trigger immediate reconciliation.
    - MODIFIED with GPU state change: Trigger immediate reconciliation
      (bypasses debounce). This is the key fix — when NVIDIA device plugin
      labels a node with GPU resources, we detect the transition and react
      within seconds instead of waiting for the periodic timer.
    - MODIFIED (no GPU change): Debounce with MIN_RECONCILE_INTERVAL.
    """
    global last_reconcile_time

    current_time = time.time()
    time_since_last = current_time - last_reconcile_time

    # Update GPU tracking and detect state changes
    gpu_state_changed = _update_gpu_tracking(name, body, type)

    if type in ['ADDED', 'DELETED']:
        # Always reconcile immediately for node additions/deletions
        color = C.GREEN if type == 'ADDED' else C.RED
        LOG.info(f"{color}{C.BOLD}  ● NODE {type}: {name}{C.RESET} — triggering immediate reconciliation")
        reconcile()
        remove_node_finalizers()
        last_reconcile_time = time.time()

    elif type == 'MODIFIED':
        if gpu_state_changed:
            # GPU state changed — bypass debounce, reconcile NOW
            LOG.info(
                f"\n"
                f"{C.BOLD}{C.BG_YELLOW}{C.WHITE}"
                f"  ⚡ GPU STATE CHANGE DETECTED  │  Node: {name}  │  Bypassing debounce — reconciling NOW  "
                f"{C.RESET}"
            )
            reconcile()
            remove_node_finalizers()
            last_reconcile_time = time.time()
        elif time_since_last >= MIN_RECONCILE_INTERVAL:
            # Enough time has passed, reconcile for non-GPU changes
            LOG.info(f"{C.CYAN}  ● Node MODIFIED: {name}{C.RESET} — triggering reconciliation ({time_since_last:.1f}s since last)")
            reconcile()
            remove_node_finalizers()
            last_reconcile_time = time.time()
        else:
            # Too soon, skip this event
            LOG.debug(
                f"Node MODIFIED: {name} — skipping reconciliation "
                f"(last reconcile {time_since_last:.1f}s ago, min interval: {MIN_RECONCILE_INTERVAL}s)"
            )
    else:
        LOG.debug(f"Node event {type}: {name} — ignoring unknown event type")


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
        # Log what changed
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
