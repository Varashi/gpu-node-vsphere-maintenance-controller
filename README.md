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

### Helm (OCI chart — recommended)

The chart is published as an OCI artifact alongside the image:

```bash
helm upgrade --install gpu-node-vsphere-maintenance \
  oci://ghcr.io/varashi/charts/gpu-node-vsphere-maintenance-controller \
  --version 0.4.2 \
  --namespace gpu-node-vsphere-maintenance --create-namespace \
  --set vcenter.host=vcenter.example.com \
  --set vcenter.user=maintenance-controller@vsphere.local \
  --set vcenter.password='replace-me'
```

To pair with External Secrets Operator, render a Secret containing
`VCENTER_HOST` / `VCENTER_USER` / `VCENTER_PASSWORD` yourself and pass
`--set vcenter.existingSecret=<name>` instead. To enable TLS verification,
create a ConfigMap containing your CA bundle and pass
`--set vcenter.caBundle.configMapName=<name>`.

A Flux `HelmRelease` example:

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: gpu-node-vsphere-maintenance-controller
  namespace: gpu-node-vsphere-maintenance
spec:
  interval: 1h
  url: oci://ghcr.io/varashi/charts/gpu-node-vsphere-maintenance-controller
  ref:
    tag: 0.4.2
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: gpu-node-vsphere-maintenance-controller
  namespace: gpu-node-vsphere-maintenance
spec:
  interval: 1h
  chartRef:
    kind: OCIRepository
    name: gpu-node-vsphere-maintenance-controller
  values:
    vcenter:
      existingSecret: vsphere-credentials
      caBundle:
        configMapName: vcenter-ca
```

See `chart/values.yaml` in this repo for the full value surface.

### Raw manifests

If you would rather skip Helm, the equivalent manifests (adjust namespace
and credentials source as needed):

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
  GUEST_SHUTDOWN_TIMEOUT_SECONDS: "120"
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
  strategy:
    type: Recreate
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
          image: ghcr.io/varashi/gpu-node-vsphere-maintenance-controller:v0.3.0
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
| `VCENTER_CA_BUNDLE`         | *(unset)*                                   | Path to CA bundle (PEM). When set, enables TLS verification against this CA. Covers self-signed and private/self-hosted CAs. |
| `VCENTER_TLS_VERIFY`        | `false`                                     | When `true` and `VCENTER_CA_BUNDLE` is unset, verify vCenter against the container's system trust store (publicly-signed certs). |
| `GPU_NODE_LABEL`            | `intel.feature.node.kubernetes.io/gpu=true` | Node label selector (`key=value`) identifying GPU workers                     |
| `POLL_INTERVAL_SECONDS`     | `30`                                        | How often to poll vSphere for host state changes                              |
| `DRAIN_TIMEOUT_SECONDS`     | `600`                                       | Max time to wait for a drain to finish before forcing power-off               |
| `GUEST_SHUTDOWN_TIMEOUT_SECONDS` | `120`                                  | Max time to wait for guest OS shutdown (via VMware Tools) before hard power-off |
| `POWER_ON_TIMEOUT_SECONDS`  | `300`                                       | Max time to wait for a powered-on VM's Node to become *Ready*                 |
| `MAX_CONCURRENT_DRAINS`     | `1`                                         | Upper bound on simultaneous drain operations                                  |
| `DRY_RUN`                   | `false`                                     | If `true`, log actions without executing vSphere / Kubernetes mutations       |

### TLS verification modes

Certificate verification against vCenter is opt-in. Default is unverified
(homelab-style). Three supported modes:

- **Self-signed certificate** (default vCenter out of the box): export the
  certificate from vCenter, mount it as a `ConfigMap` into the pod, and set
  `VCENTER_CA_BUNDLE` to the mounted path (e.g. `/etc/ssl/vcenter-ca/ca.pem`).
  Use the chart's `vcenter.caBundle.configMapName` to wire this up.
- **Private / self-hosted CA** (e.g. an internal AD CS issuing the vCenter
  cert): identical to the self-signed case — mount the issuing CA's bundle
  and set `VCENTER_CA_BUNDLE` to that path. Intermediate certs concatenated
  into the same PEM work.
- **Public CA** (Let's Encrypt, DigiCert, etc.): set `VCENTER_TLS_VERIFY=true`
  and leave `VCENTER_CA_BUNDLE` unset. The controller uses Python's
  `ssl.create_default_context()` with no cafile, which falls back to
  OpenSSL's system trust store (the CA certs shipped in the container
  image). In the chart, set `vcenter.tlsVerify: true`.

`VCENTER_CA_BUNDLE` takes precedence over `VCENTER_TLS_VERIFY` when both
are set.

## Building from source

```bash
docker build -t ghcr.io/you/gpu-node-vsphere-maintenance-controller:dev .
docker push  ghcr.io/you/gpu-node-vsphere-maintenance-controller:dev
```

Source layout is deliberately tiny — a single `controller.py` plus a
minimal Python 3.13 Dockerfile. Dependencies: `pyVmomi` and the official
Kubernetes Python client.

## Race conditions handled

- **DRS vs. controller power-on**: if DRS full-automation powers a VM on
  concurrently, the `PowerOn()` call returns an "already powered on" error.
  The controller verifies the VM is on a healthy (non-maintenance) host; if
  it landed on a maintenance host, it re-raises.
- **Concurrent power-off**: symmetric to the power-on race. If a `PowerOff()`
  call lands on a VM that is already off, the controller treats the
  "Powered off" error as success.
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
- No leader election — run `replicas: 1` with `strategy.type: Recreate`
  (shown in the example Deployment). `Recreate` guarantees the old pod has
  fully terminated before the new one starts, avoiding any double-run race
  during rollouts. All operations are also idempotent against the state
  machine as a defence in depth.
- Only *enter* maintenance mode is detected via `recentTask`; *exit* is
  detected by watching `inMaintenanceMode` transition from `true` back to
  `false`, so a missed task during a controller restart is picked up on
  the next poll.

## Release process

Releases are cut by pushing a SemVer tag `vX.Y.Z` to `main`:

```bash
git tag v0.3.1
git push origin v0.3.1
```

The `release.yaml` GitHub Actions workflow then:

1. Builds and pushes the controller image to
   `ghcr.io/varashi/gpu-node-vsphere-maintenance-controller`, multi-arch
   (`linux/amd64`, `linux/arm64`), with cosign keyless signatures (GitHub
   OIDC), an SPDX SBOM, and a build-provenance attestation.
2. Packages the Helm chart in `chart/` with `version` and `appVersion`
   matching the tag and pushes it to
   `oci://ghcr.io/varashi/charts/gpu-node-vsphere-maintenance-controller`.
3. Creates a GitHub Release whose body is extracted from the matching
   section of [`CHANGELOG.md`](./CHANGELOG.md) and attaches the SBOM and
   the packaged chart `.tgz`.

### Verifying a release

Every release is cosign-keyless-signed (GitHub OIDC), carries a SLSA build
provenance attestation pushed to the registry, and has the SPDX SBOM
attached as a cosign attestation. Verify any of these before deploying:

```bash
# 1. Image signature.
cosign verify \
  --certificate-identity-regexp 'https://github\.com/Varashi/gpu-node-vsphere-maintenance-controller/\.github/workflows/release\.yaml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/varashi/gpu-node-vsphere-maintenance-controller:<tag>

# 2. SBOM attestation (SPDX).
cosign verify-attestation --type spdxjson \
  --certificate-identity-regexp 'https://github\.com/Varashi/gpu-node-vsphere-maintenance-controller/\.github/workflows/release\.yaml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/varashi/gpu-node-vsphere-maintenance-controller:<tag>

# 3. SLSA build provenance (GitHub Attestations).
gh attestation verify \
  oci://ghcr.io/varashi/gpu-node-vsphere-maintenance-controller:<tag> \
  --owner Varashi
```

Verify the Helm chart the same way — the release workflow cosign-signs
chart digests too:

```bash
cosign verify \
  --certificate-identity-regexp 'https://github\.com/Varashi/gpu-node-vsphere-maintenance-controller/\.github/workflows/release\.yaml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/varashi/charts/gpu-node-vsphere-maintenance-controller:<tag>
```

`helm pull --verify` is *not* supported against this chart: `--verify`
looks for a PGP `.prov` file (produced by `helm package --sign`), which
is a separate signing mechanism from cosign keyless. Use `cosign verify`
above instead.

## Version history

See [`CHANGELOG.md`](./CHANGELOG.md) for the full history. Released tags
are also listed on the [GitHub Releases](https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/releases)
page with signed assets and SBOMs.

## License

MIT. See `LICENSE` (add one if distributing — public use is welcome).
