# Query regression self-hosted runners

The `Query Regression` workflow runs on **Aliyun ECS ephemeral runners**
(`aliyun-ecs`, the default and only automated path): a `provision` job on
`ubuntu-latest` creates one pay-as-you-go ECS instance per run, attaches the
retained ESSD build-cache disk, and the instance registers itself as an
ephemeral GitHub runner with a per-run label. A `teardown` job
(`if: always()`) deletes the instance; `query-regression-janitor.yml` sweeps
tagged leftovers older than 4 hours daily.

Dispatching with any other `runner` value uses it as a literal self-hosted
runner label, which is how a manually prepared host (see
`ecs-image/bootstrap-runner-host.sh`) runs the workflow. For PR labels, set
the repository variable `QUERY_REGRESSION_PR_RUNNER` to such a label to
redirect PR runs away from ECS.

The office ARC scale set `perf-regression-8-cores` that previously ran this
workflow is retired; see git history for its values files and pause/deploy
procedures. Tearing down the office cluster (helm release, runner namespace,
and the `query-regression-build-cache` PVC) is a manual operator action
outside this repository. This directory keeps its historical
`runner-scale-sets` name for path stability.

## Aliyun ECS path

Configuration lives in repository variables/secrets:

| Kind | Name | Purpose |
| --- | --- | --- |
| secret | `ALICLOUD_ECS_ACCESS_KEY_ID` / `ALICLOUD_ECS_ACCESS_KEY_SECRET` | RAM user scoped to ECS RunInstances/DeleteInstances/AttachDisk/Describe*/CreateImage/RunCommand. Used only by provision/teardown jobs on `ubuntu-latest`; never reaches the ECS instance. |
| secret | `GH_PERSONAL_ACCESS_TOKEN` | Creates the short-lived runner registration token (shared with the jsonbench EC2 path). |
| vars | `ALIYUN_ECS_REGION_ID` / `ALIYUN_ECS_VSWITCH_ID` / `ALIYUN_ECS_SECURITY_GROUP_ID` | Network placement. The security group should allow egress only; no inbound rules are needed. The vSwitch pins the zone, which must match the cache disk's zone. |
| vars | `ALIYUN_ECS_INSTANCE_TYPE` | Dedicated (non-burstable, non-shared) instance family, e.g. `ecs.g8i.2xlarge`. Both base and candidate clusters run on the same machine, so noisy neighbors break thresholds. |
| vars | `QUERY_REGRESSION_ECS_IMAGE_ID` | Custom image built by `ecs-image/build-ecs-image.py`. |
| vars | `QUERY_REGRESSION_CACHE_DISK_ID` | Retained ESSD data disk (>=600Gi) holding the build caches. |

The retained cache disk holds top-level directories `cargo-registry-v1`,
`cargo-git-v1`, `query-regression-target-v1`, `meta-v1`, and `sccache-v1`,
which cloud-init bind-mounts to the `/home/runner/...` paths the workflow
uses. The instance is created with `DeleteWithInstance=false` for the data
disk, so deleting the instance detaches and retains it. To rebuild the cache
from scratch, delete or reformat the disk; the next provision formats an
unformatted disk automatically.

Trust model on the ECS path: the instance receives only the one-hour runner
registration token via user data and holds no cloud credentials; the Aliyun
AK/SK exist only in the control-plane jobs. Instance tags
(`managed-by=query-regression-ci`, `query-regression-run-id`) feed the janitor
sweep.

### Building and updating the ECS image

The runner `Dockerfile` in the parent directory stays the single source of the
tool contract. Build a new ECS image from it:

```bash
ALIBABA_CLOUD_ACCESS_KEY_ID=... ALIBABA_CLOUD_ACCESS_KEY_SECRET=... \
uv run .github/runner-scale-sets/query-regression/ecs-image/build-ecs-image.py \
  --region-id <region> --vswitch-id <vsw-...> --security-group-id <sg-...> \
  --base-image-id <ubuntu-24.04-image-id>
```

The script boots a temporary builder instance, `docker build`s the runner
image, materializes `/opt/rustup`, `/opt/cargo`, `/usr/local/bin` tools, and
`/home/runner` (actions-runner) onto the host, installs the ephemeral-runner
systemd unit from `ecs-image/`, and snapshots a custom image. It prints the
image id; set it as `QUERY_REGRESSION_ECS_IMAGE_ID`, and bump
`RUNNER_IMAGE_EPOCH` in `query-regression.yml` at the same time so the target
cache invalidates. Builder sentinel polling requires the Cloud Assistant
agent, which Aliyun public Ubuntu images include.

### Cache disk recovery

The cache is disposable. For disk loss or corruption, create a fresh ESSD disk
in the same zone, update `QUERY_REGRESSION_CACHE_DISK_ID`, and let the next
run cold-fill it (expect the first build to take the full cold-build time).

## Trust admission for PR runs

A maintainer applying the `query-regression` or `heavy-regression` label is
**trust admission for that exact PR revision**. `query-regression` runs the
six routine default cases; `heavy-regression` runs only the high-cardinality
`prom_remote_write_7913` remote-write case. The admitted job may use the
dedicated, writable persistent cache. `pull_request: labeled` is the only PR
trigger: the label event snapshots its merge, head, and base SHAs. A queued
job fetches that immutable event merge SHA directly, verifies it is a
two-parent merge whose parents include the snapshotted head exactly once, and
uses its other parent as the actual base build revision. The snapshotted
event base is retained for audit only, so a difference from the merge's
non-head parent is not a failure. The job never follows a newer mutable PR
merge ref. An unavailable event merge, or one that does not contain exactly
one snapshotted head parent, fails closed. A later PR head change does not
retarget an already queued run: it may execute only its previously trusted
event revision if that revision remains fetchable. To run the new revision,
the maintainer must review it, remove the label, and re-add the desired
regression label; cancel the old run if it is no longer wanted. An existing
label does not automatically rerun the benchmark.

Admission does not relax runner hardening or GitHub permissions. Keep the ECS
instance free of cloud credentials and long-lived tokens, keep the security
group egress-only, keep GitHub tokens least-privilege, and review workflow
changes before admission.

## Runner image and workflow tools

The runner `Dockerfile` builds `otelgen` from
[`WenyXu/otelgen`](https://github.com/WenyXu/otelgen) commit
[`863a3f395d062c7322cc1de08a38774b7fdaa6c8`](https://github.com/WenyXu/otelgen/commit/863a3f395d062c7322cc1de08a38774b7fdaa6c8)
so trace cases do not download or compile tools during a benchmark run. It is
built only as a toolchain factory: `build-ecs-image.py` and
`bootstrap-runner-host.sh` both materialize its contents onto a host, and the
benchmark itself runs host-native.

Before builds, the workflow asserts the runner UID/GID (1001 in the ECS image;
overridable via `QUERY_REGRESSION_RUNNER_UID`/`QUERY_REGRESSION_RUNNER_GID`)
and exact tool versions: `libprotoc 3.21.12`, `uv 0.11.26`, `mold 2.30.0`,
`Python 3.12.3`, `sccache 0.16.0`, `otelgen` commit
`863a3f395d062c7322cc1de08a38774b7fdaa6c8`, root-owned `rustup 1.29.0`, and
the image-baked `nightly-2026-03-21` Rust toolchain. Rustup, Cargo, and Rustc
must resolve from `/opt/cargo/bin`; the runner cannot write `/opt/rustup` or
`/opt/cargo/bin`. Protobuf well-known includes, including
`google/protobuf/any.proto` and `google/protobuf/empty.proto`, are an image
contract and must compile with `protoc`.
`actions-rust-lang/setup-rust-toolchain@v1` is intentionally removed. The
workflow sets its warning-denying mold `RUSTFLAGS` directly, disables
automatic Rustup installation, and performs no runtime toolchain downloads.

The workflow no longer uses GitHub `rust-cache`, `setup-protoc`, `setup-uv`,
or runtime Rust setup: the image establishes immutable executable state and
the cache disk supplies only reusable Cargo data. Do not reintroduce those
actions unless the corresponding cache or image contract changes.

## Capacity and persistent cache

The job uses group `query-regression-persistent-cache-v1`, `queue: max`, and
`cancel-in-progress: false`; admitted jobs queue rather than replacing older
pending jobs. The cache disk layout is:

| Persistent state | Disk directory | Runner mount |
| --- | --- | --- |
| Ephemeral Cargo home | system disk | `/home/runner/.cargo` |
| Cargo registry data | `cargo-registry-v1` | `/home/runner/.cargo/registry` |
| Cargo Git data | `cargo-git-v1` | `/home/runner/.cargo/git` |
| Cargo target | `query-regression-target-v1` | `/home/runner/query-regression-target` |
| Cache metadata | `meta-v1` | `/home/runner/query-regression-cache-meta` |
| sccache local disk cache | `sccache-v1` | `/home/runner/.cache/sccache` |
| Immutable Rust toolchain | image-owned | `/opt/rustup`, `/opt/cargo/bin` |

`CARGO_HOME` lives on the per-instance system disk; only its nested `registry`
and `git` mounts are persistent. `RUSTUP_HOME=/opt/rustup` and
`/opt/cargo/bin` are image-owned immutable paths, while `CARGO_TARGET_DIR`,
cache metadata, and `SCCACHE_DIR` are persistent absolute paths. The runner
sets `RUSTC_WRAPPER=/usr/local/bin/sccache`,
`SCCACHE_DIR=/home/runner/.cache/sccache`, `SCCACHE_CACHE_SIZE=40G`, and
`CARGO_INCREMENTAL=0`. sccache uses its local disk backend and self-evicts at
40G; do not add runtime downloads, object storage, or a shared backend.

The repository's `.cargo/config.toml` remains a trusted per-revision build
input. In contrast, `$CARGO_HOME/config*`, credentials, installed bins, and
Cargo metadata outside the persistent `registry` and `git` data mounts are
ephemeral and cannot survive to another instance.

The local disk backend has a one-server constraint. The single cache disk and
the unchanged `query-regression-persistent-cache-v1` workflow concurrency
group serialize runs; do not provision parallel runners or relax that
serialization while this backend is in use. Base and candidate builds share
the target; Cargo fingerprints invalidate source and dependency changes. The
workflow records the sccache version and relevant environment in the target
ABI marker, starts and zeros sccache after cache and toolchain checks, shows
initial/base/candidate statistics, and resets statistics between base and
candidate builds.

### Disk cleanup contract

The workflow reports `du`, `df -P`, human-readable free space, and inode
availability before builds and in an always-run report. Its cleanup is narrow
and non-destructive:

- warn at target size 400GiB; at 450GiB clear only the complete target root;
- warn at Cargo registry-plus-Git data size 60GiB; at 80GiB remove only
  `registry/src` and `git/checkouts`, then abort if that persistent data
  remains at least 80GiB;
- below 300GiB backing free space, clear the complete target root first,
  remeasure, then remove only those Cargo extracted trees and checkouts; abort
  if free space is still below 300GiB;
- never automatically remove Cargo registry cache/index, Git database, the
  image-owned Cargo bin or Rustup toolchain, cache metadata, the self-evicting
  sccache directory, or the cache disk.

The target clear uses fixed absolute roots and removes all entries, including
dotfiles.

## Future optional phases

The current phase uses a materialized runner toolchain with sccache 0.16.0 and
no shared cache service. Optional follow-ups are an image additionally seeded
with `cargo fetch --locked` results, or an internal read/write sccache
backend or Cargo/Git mirror. Evaluate them only if persistent disk reuse is
insufficient.
