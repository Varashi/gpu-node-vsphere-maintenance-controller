# gpu-node-vsphere-maintenance-controller

A Kubernetes controller that safely handles ESXi maintenance mode transitions
for worker nodes that use **PCI passthrough** (Intel ARC / NVIDIA / any
passthrough device). Without this controller, entering maintenance on a host
with a passthrough-attached VM hangs indefinitely: vMotion is blocked, and
`HostSystem.inMaintenanceMode` never flips to `true`.

The controller detects the operator clicking *Enter Maintenance Mode* in
vCenter, drains the affected Kubernetes node, powers the VM off, and — if
possible — migrates it (cold) to another GPU-capable host and brings it back
online. When the original host exits maintenance, a powered-off node is
returned to service automatically.

Image: `ghcr.io/varashi/gpu-node-vsphere-maintenance-controller` (public).

## Why this exists

`HostSystem.inMaintenanceMode` only transitions to `true` once every VM on
the host is either migrated (vMotion) or powered off. PCI passthrough
disables vMotion. Result: maintenance mode hangs until an operator manually
powers off each passthrough VM. In a cluster with several GPU workers, that
manual dance is error-prone and blocks patching, firmware updates, and
hardware swaps.

This controller automates the full lifecycle:

1. Detect the `enterMaintenanceMode` task the moment it fires.
2. Cordon and drain the K8s node that maps to the VM on that host.
3. Power off the VM (allowing the maintenance task to complete).
4. Migrate the VM to a free GPU-capable host if one exists, or wait for the
   original host to leave maintenance.
5. Power on, wait for Node *Ready*, uncordon.

State is persisted as Node annotations so a controller restart resumes
cleanly.

## How detection works

The key trick: maintenance-mode *intent* is visible via
`HostSystem.recentTask` with `info.name == "EnterMaintenanceMode_Task"` and
`state == "running"`. The task never completes for passthrough VMs, but it
appears the instant the operator triggers it. The controller polls for this
task (default every 30s).

## GPU node identification

Nodes are discovered by label. Default:

```
intel.feature.node.kubernetes.io/gpu=true
```

(emitted by the [Intel Device
Plugin](https://github.com/intel/intel-device-plugins-for-kubernetes) / NFD).
Configurable via `GPU_NODE_LABEL`.

The VM name in vSphere **must match the Kubernetes Node name exactly**.

## State machine

Annotations on the Node (all prefixed `vsphere-maintenance.boeye.net/`):

| Key                  | Values                                           |
|----------------------|--------------------------------------------------|
| `state`              | `draining`, `powered-off`, `migrated`            |
| `host`               | ESXi host that triggered maintenance             |
| `migrated-to-host`   | Host VM moved to (when `state=migrated`)         |
| `transition-time`    | ISO8601 of last transition                       |

Flow on *enter maintenance*:

1. **draining** — cordon, evict pods (retries every poll for PDBs).
2. When drained (or `DRAIN_TIMEOUT_SECONDS` elapses) → power off VM → try
   to migrate:
   - *DRS fully automated*: call `PowerOn()` and let DRS pick a host.
   - *No DRS*: find a free GPU-capable host (`pciPassthruEnabled=true`,
     no existing GPU worker, not in maintenance), `RelocateVM` cold, then
     `PowerOn()`.
   - Success → **migrated**. Failure / no host → **powered-off**.
3. **migrated** — poll until Node *Ready* → uncordon → clear annotations.
4. **powered-off** — wait for original host to leave maintenance →
   `PowerOn()` → wait for *Ready* → uncordon → clear annotations.

Flow on *exit maintenance*: any Node in `powered-off` state referencing that
host is powered back on. `migrated` nodes are already running elsewhere and
are ignored.

Recovery: if a `powered-off` VM ends up on a different host (DRS race,
operator intervention), the controller notices on the next poll and
transitions it to `migrated`.

## Requirements

- Kubernetes 1.26+ (eviction API, server-side apply)
- vSphere 7+ (tested on 8.0)
- Workers running as vSphere VMs, with a 1:1 `VM name == Node name` mapping
- vCenter user with: `Virtual Machine → Power Off/On`, `Virtual Machine →
  Migrate`, `Host → Inventory → Read`, and task/view privileges
- GPU workers labelled so they can be discovered

## Deployment

Minimal manifests (adjust namespace / credentials source as needed):

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: gpu-node-vsphere-maintenance
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gpu-node-vsphere-maintenance
  namespace: gpu-node-vsphere-maintenance
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: gpu-node-vsphere-maintenance
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch", "patch", "update"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: gpu-node-vsphere-maintenance
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: gpu-node-vsphere-maintenance
subjects:
  - kind: ServiceAccount
    name: gpu-node-vsphere-maintenance
    namespace: gpu-node-vsphere-maintenance
---
apiVersion: v1
kind: Secret
metadata:
  name: vsphere-credentials
  namespace: gpu-node-vsphere-maintenance
type: Opaque
stringData:
  VCENTER_HOST: vcenter.example.com
  VCENTER_USER: maintenance-controller@vsphere.local
  VCENTER_PASSWORD: replace-me
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: controller-config
  namespace: gpu-node-vsphere-maintenance
data:
  POLL_INTERVAL_SECONDS: "30"
  DRAIN_TIMEOUT_SECONDS: "600"
  POWER_ON_TIMEOUT_SECONDS: "300"
  MAX_CONCURRENT_DRAINS: "1"
  GPU_NODE_LABEL: "intel.feature.node.kubernetes.io/gpu=true"
  DRY_RUN: "false"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpu-node-vsphere-maintenance
  namespace: gpu-node-vsphere-maintenance
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gpu-node-vsphere-maintenance
  template:
    metadata:
      labels:
        app: gpu-node-vsphere-maintenance
    spec:
      serviceAccountName: gpu-node-vsphere-maintenance
      containers:
        - name: controller
          image: ghcr.io/varashi/gpu-node-vsphere-maintenance-controller:v0.2.1
          envFrom:
            - secretRef:
                name: vsphere-credentials
            - configMapRef:
                name: controller-config
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              memory: 128Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            capabilities:
              drop: ["ALL"]
            seccompProfile:
              type: RuntimeDefault
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: intel.feature.node.kubernetes.io/gpu
                    operator: DoesNotExist
```

The node anti-affinity keeps the controller off GPU workers — otherwise it
would drain itself.

### Secrets via External Secrets Operator

If you use ESO, replace the plain Secret above with an `ExternalSecret`
pointing at your secret backend. An example against Bitwarden Secrets Manager:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: vsphere-credentials
  namespace: gpu-node-vsphere-maintenance
spec:
  refreshInterval: 1h
  secretStoreRef:
    kind: ClusterSecretStore
    name: bitwarden-secretsmanager
  target:
    name: vsphere-credentials
    template:
      type: Opaque
      data:
        VCENTER_HOST: "vcenter.example.com"
        VCENTER_USER: "{{ .username }}"
        VCENTER_PASSWORD: "{{ .password }}"
  data:
    - secretKey: username
      remoteRef: { key: VSPHERE_USERNAME }
    - secretKey: password
      remoteRef: { key: VSPHERE_PASSWORD }
```

## Configuration reference

| Variable                    | Default                                     | Description                                                                   |
|-----------------------------|---------------------------------------------|-------------------------------------------------------------------------------|
| `VCENTER_HOST`              | *(required)*                                | vCenter FQDN or IP                                                            |
| `VCENTER_USER`              | *(required)*                                | vCenter username                                                              |
| `VCENTER_PASSWORD`          | *(required)*                                | vCenter password                                                              |
| `GPU_NODE_LABEL`            | `intel.feature.node.kubernetes.io/gpu=true` | Node label selector (`key=value`) identifying GPU workers                     |
| `POLL_INTERVAL_SECONDS`     | `30`                                        | How often to poll vSphere for host state changes                              |
| `DRAIN_TIMEOUT_SECONDS`     | `600`                                       | Max time to wait for a drain to finish before forcing power-off               |
| `POWER_ON_TIMEOUT_SECONDS`  | `300`                                       | Max time to wait for a powered-on VM's Node to become *Ready*                 |
| `MAX_CONCURRENT_DRAINS`     | `1`                                         | Upper bound on simultaneous drain operations                                  |
| `DRY_RUN`                   | `false`                                     | If `true`, log actions without executing vSphere / Kubernetes mutations       |

TLS certificate verification against vCenter is currently disabled
(homelab-style). If that matters to you, patch `VSphereClient._connect()`.

## Building from source

```bash
docker build -t ghcr.io/you/gpu-node-vsphere-maintenance-controller:dev .
docker push  ghcr.io/you/gpu-node-vsphere-maintenance-controller:dev
```

Source layout is deliberately tiny — a single `controller.py` plus a
minimal Python 3.14 Dockerfile. Dependencies: `pyVmomi` and the official
Kubernetes Python client.

## Race conditions handled

- **DRS vs. controller power-on**: if DRS full-automation powers a VM on
  concurrently, the `PowerOn()` call returns an "already powered on" error.
  The controller verifies the VM is on a healthy (non-maintenance) host; if
  it landed on a maintenance host, it re-raises.
- **Stale `powered-off` annotation**: if a VM is already running elsewhere
  when the controller inspects it, state is advanced to `migrated` without
  waiting for the original host to leave maintenance.
- **Node never becomes Ready after power-on**: `POWER_ON_TIMEOUT_SECONDS`
  bounds the wait; after that the controller logs and moves on. The Node
  stays cordoned until reconciled on a subsequent poll.

## Limitations

- One VM per GPU host is assumed for migration target selection.
- Cluster-level PDBs can prevent draining; the controller does not force
  evictions.
- No leader election — run `replicas: 1`. A brief double-run during rollout
  is harmless (all operations are idempotent against the state machine).
- Only *enter* maintenance mode is detected via `recentTask`; *exit* is
  detected by watching `inMaintenanceMode` transition from `true` back to
  `false`, so a missed task during a controller restart is picked up on
  the next poll.

## Version history

- **v0.2.1** — concurrent-power-on race handled; stale `powered-off`
  recovery.
- **v0.2.0** — cold-migrate to a free GPU host after power-off (DRS auto
  or manual relocation).
- **v0.1.1** — fix: `reconcile_powered_off` checked host state before
  uncordoning.
- **v0.1.0** — initial: drain → power-off → wait-for-exit → power-on →
  uncordon.

## License

MIT. See `LICENSE` (add one if distributing — public use is welcome).
