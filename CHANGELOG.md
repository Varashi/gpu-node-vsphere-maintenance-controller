# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- README "Verifying a release" section with cosign verify, cosign
  verify-attestation, `gh attestation verify`, and `helm pull --verify`
  snippets for the published image and chart.
- Release workflow now cosign-keyless-signs the published Helm chart OCI
  artifact as well as the image, against the digest returned by
  `helm push`.
- CI `chart` job runs `helm/chart-testing` `ct lint` in addition to
  `helm lint` and `helm template`, catching SemVer and metadata drift
  that plain `helm lint` misses.

### Changed
- Release workflow signs the image digest once rather than once per tag —
  all tags resolve to the same digest, so per-tag signing only recorded
  duplicate signatures against the same subject.
- Release workflow disables the buildx-embedded SBOM (`sbom: false` on
  `docker/build-push-action`). `anchore/sbom-action` remains the single
  source of the SPDX SBOM and the only input to the cosign SBOM
  attestation, so image consumers no longer see two SBOMs referencing
  the same digest.
- Dockerfile `pip install` now passes `--disable-pip-version-check` to
  pre-empt hadolint `DL3042` and trim startup noise.

## [0.4.1] — 2026-04-21

### Added
- `VCENTER_TLS_VERIFY` (bool, default `false`) for vCenter certificates
  issued by a public CA. When `true` and `VCENTER_CA_BUNDLE` is unset,
  the controller uses `ssl.create_default_context()` with no `cafile`,
  which falls back to OpenSSL's system trust store shipped in the
  container image. Chart exposes it as `vcenter.tlsVerify`.
- README "TLS verification modes" section covering the three supported
  cases: self-signed, private/self-hosted CA, public CA.

## [0.4.0] — 2026-04-21

### Added
- Optional TLS verification against vCenter via `VCENTER_CA_BUNDLE`.
  When set, uses `ssl.create_default_context(cafile=...)`; otherwise falls
  back to the previous unverified behaviour and logs a warning.
- `reconcile_pending_drains(host_states)` runs every poll. Picks up GPU
  nodes on in/entering-maintenance hosts that still have no state
  annotation — covers the case where `MAX_CONCURRENT_DRAINS` throttled the
  edge-trigger and the skipped host would otherwise never be retried.
- `get_inventory_snapshot()` emits both `host_states` and a
  `vm_host_map` from a single `HostSystem` view walk. `reconcile_powered_off`
  now consults the map instead of making a per-node `get_vm_host` round-trip
  to vCenter on every poll.
- Minimal Helm chart under `chart/`, published as OCI to
  `ghcr.io/varashi/charts/gpu-node-vsphere-maintenance-controller`.
- GitHub Actions: `ci.yaml` (ruff, hadolint, helm lint, buildx smoke build)
  on pull requests; `release.yaml` on `v*.*.*` tag push builds multi-arch
  images (amd64, arm64), cosign-signs keyless via OIDC, attaches SBOM and
  build-provenance attestations, packages and pushes the Helm chart, and
  creates a GitHub Release with the body extracted from this file.
- This `CHANGELOG.md`, seeded from the previous README "Version history".

### Changed
- Dockerfile pinned to `python:3.13-slim`. `pyVmomi==8.0.3.0.1` predates
  Python 3.14 and has not been tested against it upstream.
- `startup_reconcile` delegates its "host already in maintenance at boot"
  branch to `reconcile_pending_drains` so the two paths share one
  implementation.
- Example Deployment in the README sets `strategy.type: Recreate`. With
  `replicas: 1` this closes the brief double-run window that was previously
  only partially mitigated by idempotency at the state-machine level.

### Fixed
- Concurrent power-off race: a `PowerOff()` landing on an already-off VM
  previously bubbled an `InvalidPowerState` error through the generic
  exception catch and aborted the cycle mid-transition. Now treated as
  success, symmetric to the existing power-on handling.

### Removed
- `policy/poddisruptionbudgets` verb from the example ClusterRole. The
  controller never calls the PDB API — PDB-blocked evictions are handled
  via the 429 response on `pods/eviction`.

## [0.3.0] — 2026-04-19

### Added
- `GUEST_SHUTDOWN_TIMEOUT_SECONDS` (default `120`). `power_off_vm` now
  requests a graceful guest shutdown via VMware Tools and falls back to
  a hard `PowerOff()` on `ToolsUnavailable`, on any `VimFault` rejecting
  the shutdown, or after the timeout elapses.

### Changed
- `_try_migrate` distinguishes expected vSphere failures from bugs:
  `RuntimeError` (surfaced from `_wait_task`) and `vim.fault.VimFault`
  (e.g. `NoCompatibleHost`, `InsufficientResources`) log as one-line
  WARNINGs. Unexpected exceptions still log with a full traceback.

## [0.2.3] — 2026-04

### Changed
- Reconcile loop classifies transient kube-apiserver errors (408, 429,
  5xx) and urllib3 transport blips as WARNING instead of ERROR +
  traceback. Genuine exceptions still log with the full traceback.

## [0.2.2]

### Added
- OCI image labels. No code change; extracted to a dedicated GitHub repo.

## [0.2.1]

### Fixed
- Concurrent power-on race with DRS handled explicitly.
- Stale `powered-off` annotation: a VM already running elsewhere now
  transitions to `migrated` on the next poll.

## [0.2.0]

### Added
- Cold-migrate to a free GPU-capable host after power-off, either via
  DRS full-automation `PowerOn()` or manual `RelocateVM`.

## [0.1.1]

### Fixed
- `reconcile_powered_off` checked host state before uncordoning.

## [0.1.0]

### Added
- Initial release: drain → power-off → wait-for-exit → power-on →
  uncordon, driven by edge-triggered `HostSystem.recentTask` polling.

[Unreleased]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/releases/tag/v0.1.0
