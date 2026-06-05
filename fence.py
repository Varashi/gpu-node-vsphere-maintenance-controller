#!/usr/bin/env python3
"""
GPU-node crash fence controller (sibling of the maintenance controller).

Automates non-graceful node shutdown for passthrough-GPU workers that can't be
vSphere-HA-restarted during a host crash. When a host crashes, the node's RWO
volume stays attached to the dead node and k8s won't auto-detach it (it can't
tell a crash from a network partition) — so a rescheduled stateful pod hangs on
`Multi-Attach`. The fix is the `node.kubernetes.io/out-of-service` taint, which
force-detaches volumes + force-deletes pods. This controller applies it only on
a node confirmed dead by BOTH k8s and vCenter, and removes it on recovery.

Two-gate fence condition (both required, sustained for FENCE_GRACE_SECONDS):
  1. k8s:     node NotReady
  2. vCenter: that node's VM runtime.connectionState is disconnected/inaccessible
              (a crash — a clean maintenance power-off keeps it 'connected', so
               this never collides with the maintenance controller)

Un-fence when the node recovers: VM 'connected' AND node Ready.

Deliberately scoped to taint/un-taint only:
  * Power-on is owned by vSphere HA (it restarts passthrough VMs on the original
    host once it returns).
  * Graceful maintenance drains are owned by the maintenance controller
    (controller.py), which keys off vCenter maintenance-mode tasks — a disjoint
    signal, so the two never clash.

Runs as its own Deployment with its own RBAC + kill switch (Values.fence.*).
"""

import os
import time

from kubernetes.client.rest import ApiException

from controller import (
    VM_DEAD_CONNECTION_STATES,
    DRY_RUN,
    K8sClient,
    VSphereClient,
    _is_transient_k8s_error,
    log,
)

FENCE_POLL_SECONDS = int(os.environ.get("FENCE_POLL_SECONDS", "20"))
# How long both gates must hold before we fence — guards against transient
# blips (kubelet restart, brief vCenter/host comms loss) and lets vCenter's
# host-down detection settle (it lags node-NotReady by tens of seconds).
FENCE_GRACE_SECONDS = int(os.environ.get("FENCE_GRACE_SECONDS", "60"))


class FenceController:
    def __init__(self):
        self.vsphere = VSphereClient()
        self.k8s = K8sClient()
        # node -> monotonic time the two-gate condition first became true
        self.gate_since: dict[str, float] = {}

    def reconcile(self):
        for node in self.k8s.get_gpu_nodes():
            name = node.metadata.name
            ready = self.k8s.is_ready(name)
            vm_state = self.vsphere.get_vm_connection_state(name)
            tainted = self.k8s.has_out_of_service_taint(name)
            dead_vm = vm_state in VM_DEAD_CONNECTION_STATES

            # ── FENCE gate: NotReady AND vCenter says the VM is gone ──
            if not ready and dead_vm:
                first = self.gate_since.setdefault(name, time.monotonic())
                elapsed = time.monotonic() - first
                if tainted:
                    continue  # already fenced
                if elapsed >= FENCE_GRACE_SECONDS:
                    log.warning(
                        f"{name}: NotReady + vm={vm_state} for {elapsed:.0f}s "
                        f"(>= {FENCE_GRACE_SECONDS}s grace) — fencing"
                    )
                    self.k8s.apply_out_of_service_taint(name)
                else:
                    log.info(
                        f"{name}: fence-gate pending {elapsed:.0f}/"
                        f"{FENCE_GRACE_SECONDS}s (NotReady + vm={vm_state})"
                    )
                continue

            # ── Gates not both true: reset timer, and un-fence on recovery ──
            self.gate_since.pop(name, None)
            if tainted and ready and vm_state == "connected":
                log.info(f"{name}: recovered (Ready + vm connected) — un-fencing")
                self.k8s.remove_out_of_service_taint(name)

    def run(self):
        log.info(
            f"Fence controller started — poll={FENCE_POLL_SECONDS}s, "
            f"grace={FENCE_GRACE_SECONDS}s, dry_run={DRY_RUN}"
        )
        while True:
            try:
                self.reconcile()
            except Exception as e:
                if _is_transient_k8s_error(e):
                    reason = getattr(e, "reason", type(e).__name__)
                    status = getattr(e, "status", None)
                    detail = f" status={status}" if status else ""
                    log.warning(
                        f"Transient k8s/transport error in fence loop: "
                        f"{reason}{detail} — retrying next poll"
                    )
                else:
                    log.exception("Unhandled error in fence loop")
            time.sleep(FENCE_POLL_SECONDS)


if __name__ == "__main__":
    FenceController().run()
