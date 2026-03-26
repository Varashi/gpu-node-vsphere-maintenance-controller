#!/usr/bin/env python3
"""
VSphere Maintenance Mode Controller

Watches ESXi hosts for maintenance mode transitions and automatically:
- Cordon + drain + power off GPU worker nodes when their host enters maintenance
- If a free GPU-capable host is available, migrate the VM there and power it on:
    * DRS full automation: power on directly, DRS selects placement host
    * No DRS: cold migrate (RelocateVM) to a free GPU host, then power on
- Otherwise wait for the host to exit maintenance, then power on + uncordon

GPU nodes are identified by label: intel.feature.node.kubernetes.io/gpu=true
VM name in vSphere matches K8s node name exactly.

State is persisted as node annotations so the controller survives pod restarts.
"""

import logging
import os
import ssl
import sys
import time
from datetime import datetime, timezone

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException
from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim

# ── Configuration ────────────────────────────────────────────────────────────

VCENTER_HOST = os.environ["VCENTER_HOST"]
VCENTER_USER = os.environ["VCENTER_USER"]
VCENTER_PASSWORD = os.environ["VCENTER_PASSWORD"]

GPU_NODE_LABEL = os.environ.get(
    "GPU_NODE_LABEL", "intel.feature.node.kubernetes.io/gpu=true"
)
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
DRAIN_TIMEOUT = int(os.environ.get("DRAIN_TIMEOUT_SECONDS", "600"))
POWER_ON_TIMEOUT = int(os.environ.get("POWER_ON_TIMEOUT_SECONDS", "300"))
MAX_CONCURRENT_DRAINS = int(os.environ.get("MAX_CONCURRENT_DRAINS", "1"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

ANNOTATION_STATE = "vsphere-maintenance.boeye.net/state"
ANNOTATION_HOST = "vsphere-maintenance.boeye.net/host"
ANNOTATION_TIME = "vsphere-maintenance.boeye.net/transition-time"
ANNOTATION_MIGRATED_HOST = "vsphere-maintenance.boeye.net/migrated-to-host"

STATE_DRAINING = "draining"
STATE_POWERED_OFF = "powered-off"
STATE_MIGRATED = "migrated"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ── vSphere client ────────────────────────────────────────────────────────────

class VSphereClient:
    def __init__(self):
        self.si = None
        self._connect()

    def _connect(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.si = SmartConnect(
            host=VCENTER_HOST,
            user=VCENTER_USER,
            pwd=VCENTER_PASSWORD,
            sslContext=ctx,
        )
        log.info(f"Connected to vCenter {VCENTER_HOST}")

    def _ensure_connected(self):
        try:
            self.si.content.sessionManager.currentSession
        except Exception:
            log.warning("vCenter session lost, reconnecting...")
            self._connect()

    def _container(self, obj_type):
        content = self.si.content
        return content.viewManager.CreateContainerView(
            content.rootFolder, [obj_type], True
        )

    def get_hosts_state(self):
        """
        Returns {host_name: {"in_maintenance": bool, "entering_maintenance": bool}}

        "entering_maintenance" is True when a HostSystem.enterMaintenanceMode task
        is actively running — this fires before inMaintenanceMode flips to True,
        which is critical for VMs with PCI passthrough that block vMotion.
        """
        self._ensure_connected()
        view = self._container(vim.HostSystem)
        result = {}
        for host in view.view:
            entering = any(
                t.info.descriptionId == "HostSystem.enterMaintenanceMode"
                and t.info.state == "running"
                for t in host.recentTask
            )
            result[host.name] = {
                "in_maintenance": host.runtime.inMaintenanceMode,
                "entering_maintenance": entering,
            }
        view.Destroy()
        return result

    def get_vm_names_on_host(self, host_name):
        """Returns list of VM names currently on the given ESXi host."""
        self._ensure_connected()
        view = self._container(vim.HostSystem)
        for host in view.view:
            if host.name == host_name:
                names = [vm.name for vm in host.vm]
                view.Destroy()
                return names
        view.Destroy()
        return []

    def is_drs_fully_automated(self, host_name: str) -> bool:
        """
        Returns True if the host belongs to a vSphere cluster with DRS enabled
        in fully automated mode. Returns False for standalone hosts or manual/partial DRS.
        When True, PowerOn requests are handled by DRS for initial placement,
        so explicit relocation before power-on is unnecessary.
        """
        self._ensure_connected()
        view = self._container(vim.HostSystem)
        result = False
        for host in view.view:
            if host.name == host_name:
                if isinstance(host.parent, vim.ClusterComputeResource):
                    drs = host.parent.configuration.drsConfig
                    result = (
                        drs.enabled
                        and drs.defaultVmBehavior
                        == vim.cluster.DrsConfigInfo.DrsBehavior.fullyAutomated
                    )
                break
        view.Destroy()
        return result

    def find_free_gpu_host(
        self, gpu_node_names: set, host_states: dict, exclude_host: str
    ):
        """
        Find an ESXi host that:
        - Has PCI passthrough enabled (indicates GPU-capable hardware)
        - Is not in or entering maintenance mode
        - Is not the excluded host
        - Has none of the known GPU worker VMs on it (free GPU slot)

        Used as fallback when DRS is not in full automation mode.
        Returns host name, or None if no suitable host is found.
        """
        self._ensure_connected()
        view = self._container(vim.HostSystem)
        result = None
        for host in view.view:
            if host.name == exclude_host:
                continue
            h = host_states.get(host.name, {})
            if h.get("in_maintenance") or h.get("entering_maintenance"):
                continue
            # Only consider hosts with PCI passthrough capability (GPU hosts)
            has_passthrough = (
                host.config
                and host.config.pciPassthruInfo
                and any(p.passthruEnabled for p in host.config.pciPassthruInfo)
            )
            if not has_passthrough:
                continue
            # Skip hosts that already have a GPU worker VM
            vms_on_host = {vm.name for vm in host.vm}
            if vms_on_host.isdisjoint(gpu_node_names):
                result = host.name
                break
        view.Destroy()
        return result

    def get_vm_host(self, vm_name: str):
        """Returns the name of the ESXi host the VM is currently on, or None."""
        self._ensure_connected()
        vm = self._find_vm(vm_name)
        if vm and vm.runtime.host:
            return vm.runtime.host.name
        return None

    def relocate_vm(self, vm_name: str, target_host_name: str):
        """Cold migrate a powered-off VM to the target ESXi host."""
        self._ensure_connected()
        vm = self._find_vm(vm_name)
        if vm is None:
            raise RuntimeError(f"VM {vm_name} not found")

        view = self._container(vim.HostSystem)
        target_host = None
        for host in view.view:
            if host.name == target_host_name:
                target_host = host
                break
        view.Destroy()

        if target_host is None:
            raise RuntimeError(f"Target host {target_host_name} not found")

        spec = vim.vm.RelocateSpec()
        spec.host = target_host
        spec.pool = target_host.parent.resourcePool

        log.info(f"Cold migrating {vm_name} to {target_host_name}")
        self._wait_task(vm.Relocate(spec), timeout=600)
        log.info(f"Cold migration of {vm_name} to {target_host_name} complete")

    def power_off_vm(self, vm_name):
        self._ensure_connected()
        vm = self._find_vm(vm_name)
        if vm is None:
            log.error(f"VM {vm_name} not found in vCenter")
            return
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
            log.info(f"VM {vm_name} is already powered off")
            return
        if DRY_RUN:
            log.info(f"[DRY RUN] Would power off VM {vm_name}")
            return
        log.info(f"Powering off VM {vm_name}")
        self._wait_task(vm.PowerOff())

    def power_on_vm(self, vm_name):
        self._ensure_connected()
        vm = self._find_vm(vm_name)
        if vm is None:
            log.error(f"VM {vm_name} not found in vCenter")
            return
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
            log.info(f"VM {vm_name} is already powered on")
            return
        if DRY_RUN:
            log.info(f"[DRY RUN] Would power on VM {vm_name}")
            return
        log.info(f"Powering on VM {vm_name}")
        try:
            self._wait_task(vm.PowerOn())
        except RuntimeError as e:
            if "Powered on" not in str(e):
                raise
            # VM was powered on concurrently (e.g., by DRS or Rancher).
            # Verify it landed on a host that is not in/entering maintenance.
            current_host = vm.runtime.host
            if current_host is None:
                raise RuntimeError(f"VM {vm_name} is powered on but host is unknown")
            host_entering = any(
                t.info.descriptionId == "HostSystem.enterMaintenanceMode"
                and t.info.state == "running"
                for t in current_host.recentTask
            )
            if current_host.runtime.inMaintenanceMode or host_entering:
                raise RuntimeError(
                    f"VM {vm_name} is already powered on but on a host in/entering "
                    f"maintenance mode ({current_host.name}) — cannot use"
                )
            log.info(
                f"VM {vm_name} already powered on on {current_host.name} "
                f"(concurrent power-on) — treating as success"
            )

    def _find_vm(self, vm_name):
        view = self._container(vim.VirtualMachine)
        for vm in view.view:
            if vm.name == vm_name:
                view.Destroy()
                return vm
        view.Destroy()
        return None

    def _wait_task(self, task, timeout=120):
        deadline = time.time() + timeout
        while task.info.state not in (
            vim.TaskInfo.State.success,
            vim.TaskInfo.State.error,
        ):
            if time.time() > deadline:
                raise TimeoutError(f"vSphere task timed out after {timeout}s")
            time.sleep(2)
        if task.info.state == vim.TaskInfo.State.error:
            raise RuntimeError(f"vSphere task failed: {task.info.error.msg}")


# ── Kubernetes client ─────────────────────────────────────────────────────────

class K8sClient:
    def __init__(self):
        try:
            k8s_config.load_incluster_config()
            log.info("Loaded in-cluster kubeconfig")
        except k8s_config.ConfigException:
            k8s_config.load_kube_config(context="k8s")
            log.info("Loaded local kubeconfig (k8s context)")
        self.core = k8s_client.CoreV1Api()

    def get_gpu_nodes(self):
        return self.core.list_node(label_selector=GPU_NODE_LABEL).items

    def get_node(self, node_name):
        return self.core.read_node(node_name)

    def get_annotation(self, node_name, key):
        node = self.get_node(node_name)
        return (node.metadata.annotations or {}).get(key)

    def patch_node_annotations(self, node_name, annotations):
        """Set annotations on a node. Pass None as value to remove an annotation."""
        if DRY_RUN:
            log.info(f"[DRY RUN] Would annotate {node_name}: {annotations}")
            return
        self.core.patch_node(node_name, {"metadata": {"annotations": annotations}})

    def cordon(self, node_name):
        if DRY_RUN:
            log.info(f"[DRY RUN] Would cordon {node_name}")
            return
        log.info(f"Cordoning {node_name}")
        self.core.patch_node(node_name, {"spec": {"unschedulable": True}})

    def uncordon(self, node_name):
        if DRY_RUN:
            log.info(f"[DRY RUN] Would uncordon {node_name}")
            return
        log.info(f"Uncordoning {node_name}")
        self.core.patch_node(node_name, {"spec": {"unschedulable": False}})

    def is_ready(self, node_name):
        node = self.get_node(node_name)
        for condition in node.status.conditions:
            if condition.type == "Ready":
                return condition.status == "True"
        return False

    def get_evictable_pods(self, node_name):
        """Non-DaemonSet, non-mirror pods on the node."""
        pods = self.core.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name}"
        ).items
        result = []
        for pod in pods:
            annotations = pod.metadata.annotations or {}
            if "kubernetes.io/config.mirror" in annotations:
                continue
            owners = pod.metadata.owner_references or []
            if any(ref.kind == "DaemonSet" for ref in owners):
                continue
            result.append(pod)
        return result

    def evict_pod(self, name, namespace):
        body = k8s_client.V1Eviction(
            metadata=k8s_client.V1ObjectMeta(name=name, namespace=namespace)
        )
        try:
            self.core.create_namespaced_pod_eviction(name, namespace, body)
        except ApiException as e:
            if e.status == 404:
                pass  # already gone
            elif e.status == 429:
                log.warning(f"Eviction of {namespace}/{name} blocked by PDB, will retry")
            else:
                raise


# ── Controller ────────────────────────────────────────────────────────────────

class MaintenanceController:
    def __init__(self):
        self.vsphere = VSphereClient()
        self.k8s = K8sClient()
        # Tracks previous {host: {"in_maintenance": bool, "entering_maintenance": bool}}
        self.last_host_state: dict[str, dict] = {}
        self.drain_started_at: dict[str, float] = {}

        if DRY_RUN:
            log.warning("*** DRY RUN MODE — no changes will be made ***")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def gpu_node_names(self) -> set[str]:
        return {n.metadata.name for n in self.k8s.get_gpu_nodes()}

    def nodes_in_progress(self) -> int:
        count = 0
        for node in self.k8s.get_gpu_nodes():
            state = (node.metadata.annotations or {}).get(ANNOTATION_STATE)
            if state in (STATE_DRAINING, STATE_POWERED_OFF, STATE_MIGRATED):
                count += 1
        return count

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Startup reconciliation ────────────────────────────────────────────────

    def startup_reconcile(self):
        """
        On startup, compare node annotations against live vSphere state.
        Handles the case where the controller pod was restarted mid-cycle.
        """
        log.info("Running startup reconciliation...")
        host_states = self.vsphere.get_hosts_state()

        # Resume any in-progress cycles from before the controller restarted
        for node in self.k8s.get_gpu_nodes():
            name = node.metadata.name
            annotations = node.metadata.annotations or {}
            state = annotations.get(ANNOTATION_STATE)
            host = annotations.get(ANNOTATION_HOST)

            if state == STATE_DRAINING:
                # Restarted mid-drain — resume timeout tracking
                log.info(f"Resuming drain watch for {name} (host={host})")
                self.drain_started_at[name] = time.time()

            elif state == STATE_POWERED_OFF and host:
                h = host_states.get(host, {})
                if not h.get("in_maintenance") and not h.get("entering_maintenance"):
                    # Host already exited maintenance while we were down — power on
                    log.info(
                        f"Host {host} already exited maintenance, "
                        f"triggering power-on for {name}"
                    )
                    self.vsphere.power_on_vm(name)

            elif state == STATE_MIGRATED:
                migrated_to = annotations.get(ANNOTATION_MIGRATED_HOST, "unknown")
                log.info(
                    f"Resuming wait for {name} to be Ready "
                    f"(migrated to {migrated_to})"
                )
                # reconcile_migrated will handle uncordoning once Ready

        # Start fresh drains for hosts already entering/in maintenance with no annotation yet
        gpu_nodes = self.gpu_node_names()
        for host_name, state in host_states.items():
            if not (state["in_maintenance"] or state["entering_maintenance"]):
                continue
            vms_on_host = self.vsphere.get_vm_names_on_host(host_name)
            for vm in vms_on_host:
                if vm not in gpu_nodes:
                    continue
                existing_state = self.k8s.get_annotation(vm, ANNOTATION_STATE)
                if existing_state is None:
                    log.info(
                        f"Host {host_name} already entering/in maintenance at startup "
                        f"and {vm} has no annotation — starting drain"
                    )
                    self.on_host_entered_maintenance(host_name)
                    break  # on_host_entered_maintenance handles all VMs on this host

        log.info("Startup reconciliation complete")

    # ── Maintenance transitions ───────────────────────────────────────────────

    def on_host_entered_maintenance(self, host_name: str):
        log.info(f"Host {host_name} entered maintenance mode")

        in_progress = self.nodes_in_progress()
        if in_progress >= MAX_CONCURRENT_DRAINS:
            log.warning(
                f"{in_progress} GPU node(s) already in a maintenance cycle "
                f"(max={MAX_CONCURRENT_DRAINS}) — skipping host {host_name}"
            )
            return

        vms_on_host = self.vsphere.get_vm_names_on_host(host_name)
        gpu_nodes = self.gpu_node_names()
        targets = [vm for vm in vms_on_host if vm in gpu_nodes]

        if not targets:
            log.info(f"No GPU worker nodes on host {host_name}, nothing to do")
            return

        for node_name in targets:
            log.info(f"Starting drain for GPU node {node_name} on host {host_name}")
            self.k8s.patch_node_annotations(node_name, {
                ANNOTATION_STATE: STATE_DRAINING,
                ANNOTATION_HOST: host_name,
                ANNOTATION_TIME: self.now_iso(),
            })
            self.k8s.cordon(node_name)
            # Issue initial evictions
            for pod in self.k8s.get_evictable_pods(node_name):
                if not DRY_RUN:
                    self.k8s.evict_pod(pod.metadata.name, pod.metadata.namespace)
                else:
                    log.info(
                        f"[DRY RUN] Would evict "
                        f"{pod.metadata.namespace}/{pod.metadata.name}"
                    )
            self.drain_started_at[node_name] = time.time()

    def on_host_exited_maintenance(self, host_name: str):
        log.info(f"Host {host_name} exited maintenance mode")

        for node in self.k8s.get_gpu_nodes():
            name = node.metadata.name
            annotations = node.metadata.annotations or {}
            state = annotations.get(ANNOTATION_STATE)

            if state == STATE_POWERED_OFF and annotations.get(ANNOTATION_HOST) == host_name:
                log.info(f"Powering on VM {name}")
                self.vsphere.power_on_vm(name)
            # STATE_MIGRATED nodes are already powered on elsewhere — nothing to do

    # ── Migration ─────────────────────────────────────────────────────────────

    def _try_migrate(self, node_name: str, original_host: str, host_states: dict):
        """
        After a VM is powered off, attempt to start it on a different host so it
        becomes available without waiting for the original host to exit maintenance.

        Strategy:
        1. DRS fully automated: power on directly — DRS selects a compatible host
           (respects maintenance mode and PCI passthrough hardware requirements).
        2. No DRS: find a free GPU-capable host manually and cold migrate there first.

        Returns the host name the VM was migrated to, or None if migration was not
        possible or not attempted (caller keeps state as STATE_POWERED_OFF).
        """
        if DRY_RUN:
            if self.vsphere.is_drs_fully_automated(original_host):
                log.info(
                    f"[DRY RUN] DRS fully automated — would power on {node_name} "
                    f"and let DRS select placement host"
                )
            else:
                gpu_nodes = self.gpu_node_names()
                free_host = self.vsphere.find_free_gpu_host(
                    gpu_nodes, host_states, original_host
                )
                if free_host:
                    log.info(
                        f"[DRY RUN] Would cold migrate {node_name} to {free_host} "
                        f"and power on"
                    )
                else:
                    log.info(
                        f"[DRY RUN] No free GPU host available for {node_name} — "
                        f"would wait for {original_host} to exit maintenance"
                    )
            return None

        # DRS path: let vSphere handle initial placement
        if self.vsphere.is_drs_fully_automated(original_host):
            log.info(
                f"Cluster DRS is fully automated — powering on {node_name}, "
                f"DRS will select placement host"
            )
            try:
                self.vsphere.power_on_vm(node_name)
                actual_host = self.vsphere.get_vm_host(node_name) or "drs-managed"
                log.info(f"DRS placed {node_name} on {actual_host}")
                return actual_host
            except Exception:
                log.exception(
                    f"DRS-managed power-on of {node_name} failed — "
                    f"will wait for {original_host} to exit maintenance"
                )
                return None

        # Non-DRS path: find a free GPU host and cold migrate
        gpu_nodes = self.gpu_node_names()
        free_host = self.vsphere.find_free_gpu_host(gpu_nodes, host_states, original_host)

        if free_host is None:
            log.info(
                f"No free GPU host available for {node_name} — "
                f"will wait for {original_host} to exit maintenance"
            )
            return None

        log.info(f"Free GPU host found: {free_host} — cold migrating {node_name}")
        try:
            self.vsphere.relocate_vm(node_name, free_host)
            self.vsphere.power_on_vm(node_name)
            return free_host
        except Exception:
            log.exception(
                f"Migration of {node_name} to {free_host} failed — "
                f"will wait for {original_host} to exit maintenance"
            )
            return None

    # ── Ongoing reconciliation ────────────────────────────────────────────────

    def reconcile_draining(self, host_states: dict):
        """Advance draining nodes: evict remaining pods, power off when clear."""
        for node in self.k8s.get_gpu_nodes():
            name = node.metadata.name
            annotations = node.metadata.annotations or {}
            if annotations.get(ANNOTATION_STATE) != STATE_DRAINING:
                continue

            remaining = self.k8s.get_evictable_pods(name)
            elapsed = time.time() - self.drain_started_at.get(name, time.time())

            if not remaining or elapsed > DRAIN_TIMEOUT:
                if not remaining:
                    log.info(
                        f"Node {name} fully drained after {int(elapsed)}s — powering off VM"
                    )
                else:
                    log.warning(
                        f"Drain timeout ({DRAIN_TIMEOUT}s) exceeded for {name} — "
                        f"forcing power off with {len(remaining)} pod(s) remaining"
                    )
                self.vsphere.power_off_vm(name)
                self.drain_started_at.pop(name, None)

                original_host = annotations.get(ANNOTATION_HOST)
                migrated_to = self._try_migrate(name, original_host, host_states)

                if migrated_to:
                    self.k8s.patch_node_annotations(name, {
                        ANNOTATION_STATE: STATE_MIGRATED,
                        ANNOTATION_HOST: original_host,
                        ANNOTATION_MIGRATED_HOST: migrated_to,
                        ANNOTATION_TIME: self.now_iso(),
                    })
                else:
                    self.k8s.patch_node_annotations(name, {
                        ANNOTATION_STATE: STATE_POWERED_OFF,
                        ANNOTATION_HOST: original_host,
                        ANNOTATION_TIME: self.now_iso(),
                    })

            else:
                # Re-evict pods that haven't terminated yet (handles PDB retries)
                for pod in remaining:
                    if not DRY_RUN:
                        self.k8s.evict_pod(pod.metadata.name, pod.metadata.namespace)
                log.info(
                    f"Node {name} draining: {len(remaining)} pod(s) remaining "
                    f"({int(elapsed)}s / {DRAIN_TIMEOUT}s)"
                )

    def reconcile_powered_off(self, host_states: dict):
        """Uncordon powered-off nodes once their host has exited maintenance and they're back Ready."""
        for node in self.k8s.get_gpu_nodes():
            name = node.metadata.name
            annotations = node.metadata.annotations or {}
            if annotations.get(ANNOTATION_STATE) != STATE_POWERED_OFF:
                continue

            host = annotations.get(ANNOTATION_HOST)
            h = host_states.get(host, {})

            if h.get("in_maintenance") or h.get("entering_maintenance"):
                # Host still in/entering maintenance — check if VM was already placed
                # on a different host (e.g., by DRS or Rancher in a previous cycle)
                actual_host = self.vsphere.get_vm_host(name)
                ah = host_states.get(actual_host, {}) if actual_host else {}
                if (actual_host and actual_host != host
                        and not ah.get("in_maintenance")
                        and not ah.get("entering_maintenance")):
                    log.info(
                        f"VM {name} already running on {actual_host} — "
                        f"transitioning to migrated state"
                    )
                    self.k8s.patch_node_annotations(name, {
                        ANNOTATION_STATE: STATE_MIGRATED,
                        ANNOTATION_HOST: host,
                        ANNOTATION_MIGRATED_HOST: actual_host,
                        ANNOTATION_TIME: self.now_iso(),
                    })
                else:
                    log.info(f"Node {name} powered off, host {host} still in maintenance")
                continue

            if self.k8s.is_ready(name):
                log.info(f"Node {name} is Ready — uncordoning")
                self.k8s.uncordon(name)
                self.k8s.patch_node_annotations(name, {
                    ANNOTATION_STATE: None,
                    ANNOTATION_HOST: None,
                    ANNOTATION_TIME: None,
                })
            else:
                log.info(f"Node {name} not yet Ready, waiting...")

    def reconcile_migrated(self):
        """Uncordon migrated nodes once they're back Ready on their new host."""
        for node in self.k8s.get_gpu_nodes():
            name = node.metadata.name
            annotations = node.metadata.annotations or {}
            if annotations.get(ANNOTATION_STATE) != STATE_MIGRATED:
                continue

            migrated_to = annotations.get(ANNOTATION_MIGRATED_HOST, "unknown")

            if self.k8s.is_ready(name):
                log.info(f"Node {name} is Ready on {migrated_to} — uncordoning")
                self.k8s.uncordon(name)
                self.k8s.patch_node_annotations(name, {
                    ANNOTATION_STATE: None,
                    ANNOTATION_HOST: None,
                    ANNOTATION_MIGRATED_HOST: None,
                    ANNOTATION_TIME: None,
                })
            else:
                log.info(
                    f"Node {name} migrated to {migrated_to}, not yet Ready, waiting..."
                )

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self):
        self.startup_reconcile()

        log.info(
            f"Controller started — poll={POLL_INTERVAL}s, "
            f"drain_timeout={DRAIN_TIMEOUT}s, "
            f"max_concurrent_drains={MAX_CONCURRENT_DRAINS}, "
            f"dry_run={DRY_RUN}"
        )

        while True:
            try:
                host_states = self.vsphere.get_hosts_state()

                for host_name, state in host_states.items():
                    in_maintenance = state["in_maintenance"]
                    entering = state["entering_maintenance"]
                    prev = self.last_host_state.get(host_name)

                    if prev is None:
                        # First poll — record state, don't act on it
                        self.last_host_state[host_name] = state
                        if in_maintenance or entering:
                            log.info(
                                f"Host {host_name} already in/entering maintenance "
                                f"at startup (handled by startup reconciliation)"
                            )
                        else:
                            log.info(f"Host {host_name}: normal")
                        continue

                    prev_active = prev["in_maintenance"] or prev["entering_maintenance"]
                    now_active = in_maintenance or entering
                    now_idle = not in_maintenance and not entering

                    if not prev_active and now_active:
                        # Triggered as soon as EnterMaintenanceMode task starts —
                        # well before inMaintenanceMode flips to True
                        self.on_host_entered_maintenance(host_name)
                    elif prev_active and now_idle:
                        self.on_host_exited_maintenance(host_name)

                    self.last_host_state[host_name] = state

                self.reconcile_draining(host_states)
                self.reconcile_powered_off(host_states)
                self.reconcile_migrated()

            except Exception:
                log.exception("Unhandled error in reconcile loop")

            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    MaintenanceController().run()
