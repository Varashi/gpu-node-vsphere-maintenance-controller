#!/usr/bin/env python3
"""Unit test for the fence controller's two-gate logic — no cluster/vCenter
needed. Stubs the heavy imports so `import controller`/`fence` works, then
drives FenceController.reconcile() through the scenarios that matter for the
out-of-service taint (the most destructive action in the cluster).

Run: python3 test_fence.py
"""
import os
import sys
import types

# ── stub heavy deps so `import controller` succeeds ──────────────────────────
os.environ.setdefault("VCENTER_HOST", "x")
os.environ.setdefault("VCENTER_USER", "x")
os.environ.setdefault("VCENTER_PASSWORD", "x")


def _mod(name):
    m = types.ModuleType(name)
    sys.modules[name] = m
    return m


class _ApiException(Exception):
    def __init__(self, status=None):
        self.status = status


_u = _mod("urllib3")
_ue = _mod("urllib3.exceptions")
_u.exceptions = _ue
for _n in ("ProtocolError", "ReadTimeoutError", "MaxRetryError", "ConnectionError"):
    setattr(_ue, _n, type(_n, (Exception,), {}))

_k = _mod("kubernetes")
_kc = _mod("kubernetes.client")
_kcr = _mod("kubernetes.client.rest")
_kcfg = _mod("kubernetes.config")
_k.client = _kc
_k.config = _kcfg
_kc.rest = _kcr
_kcr.ApiException = _ApiException
_kc.V1Taint = lambda **kw: types.SimpleNamespace(**kw)
_kc.CoreV1Api = object
_kc.V1Eviction = object
_kc.V1ObjectMeta = object


class _ConfigException(Exception):
    pass


_kcfg.ConfigException = _ConfigException
_kcfg.load_incluster_config = lambda: None
_kcfg.load_kube_config = lambda **kw: None

_pv = _mod("pyVim")
_pvc = _mod("pyVim.connect")
_pv.connect = _pvc
_pvc.SmartConnect = lambda **kw: None
_pvm = _mod("pyVmomi")
_pvm.vim = types.SimpleNamespace()
_pvm.vmodl = types.SimpleNamespace()

import fence  # noqa: E402


# ── fakes ────────────────────────────────────────────────────────────────────
class FakeK8s:
    def __init__(self, nodes):
        # nodes: name -> {"ready": bool, "tainted": bool}
        self.nodes = nodes
        self.applied = []
        self.removed = []

    def get_gpu_nodes(self):
        return [types.SimpleNamespace(metadata=types.SimpleNamespace(name=n))
                for n in self.nodes]

    def is_ready(self, name):
        return self.nodes[name]["ready"]

    def has_out_of_service_taint(self, name):
        return self.nodes[name]["tainted"]

    def apply_out_of_service_taint(self, name):
        self.nodes[name]["tainted"] = True
        self.applied.append(name)

    def remove_out_of_service_taint(self, name):
        self.nodes[name]["tainted"] = False
        self.removed.append(name)


class FakeVS:
    def __init__(self, states):
        self.states = states  # name -> connectionState

    def get_vm_connection_state(self, name):
        return self.states[name]


def make(nodes, states, grace=0):
    fc = fence.FenceController.__new__(fence.FenceController)
    fc.k8s = FakeK8s(nodes)
    fc.vsphere = FakeVS(states)
    fc.gate_since = {}
    fence.FENCE_GRACE_SECONDS = grace
    return fc


results = []


def check(desc, cond):
    results.append((desc, cond))
    print(("PASS " if cond else "FAIL ") + desc)


# 1. healthy node — never fenced
fc = make({"n": {"ready": True, "tainted": False}}, {"n": "connected"})
fc.reconcile()
check("healthy (Ready+connected) -> no fence", not fc.k8s.applied)

# 2. NotReady but VM still connected (vCenter lag) — must NOT fence (one gate)
fc = make({"n": {"ready": False, "tainted": False}}, {"n": "connected"})
fc.reconcile()
check("NotReady + vm connected -> no fence (one gate only)", not fc.k8s.applied)

# 3. both gates but within grace — no fence yet
fc = make({"n": {"ready": False, "tainted": False}}, {"n": "disconnected"}, grace=9999)
fc.reconcile()
check("NotReady + disconnected, within grace -> no fence yet", not fc.k8s.applied)

# 4. both gates, grace elapsed -> fence
fc = make({"n": {"ready": False, "tainted": False}}, {"n": "disconnected"}, grace=0)
fc.reconcile()
check("NotReady + disconnected, grace met -> FENCE", fc.k8s.applied == ["n"])

# 5. already tainted -> no double apply
fc = make({"n": {"ready": False, "tainted": True}}, {"n": "disconnected"}, grace=0)
fc.reconcile()
check("already tainted -> no re-apply", not fc.k8s.applied)

# 6. recovery: tainted + Ready + connected -> un-fence
fc = make({"n": {"ready": True, "tainted": True}}, {"n": "connected"})
fc.reconcile()
check("recovered (Ready+connected) while tainted -> un-fence", fc.k8s.removed == ["n"])

# 7. tainted, node back Ready but vm still disconnected -> do NOT un-fence yet
fc = make({"n": {"ready": True, "tainted": True}}, {"n": "disconnected"})
fc.reconcile()
check("tainted, Ready but vm disconnected -> stay fenced", not fc.k8s.removed)

# 8. 'notfound' VM is not a dead-state -> no fence
fc = make({"n": {"ready": False, "tainted": False}}, {"n": "notfound"}, grace=0)
fc.reconcile()
check("NotReady + vm notfound -> no fence", not fc.k8s.applied)

print()
failed = [d for d, c in results if not c]
if failed:
    print(f"{len(failed)} FAILED")
    sys.exit(1)
print(f"all {len(results)} passed")
