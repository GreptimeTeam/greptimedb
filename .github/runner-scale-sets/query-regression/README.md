# Query regression self-hosted runners

The `Query Regression` workflow runs on **Aliyun ECS ephemeral runners**
(`aliyun-ecs`, the default and only automated path): a `provision` job on
`ubuntu-latest` creates one pay-as-you-go ECS instance per run, and the
instance registers itself as an ephemeral GitHub runner with a per-run
label. A `teardown` job (`if: always()`) deletes the instance;
`query-regression-janitor.yml` sweeps tagged leftovers older than 4 hours
daily.

Build caches live on the instance's system disk, so **every run compiles
cold**; only the within-run reuse (base build warms the candidate build
through the shared target dir and sccache) applies. There is no retained
data disk.

Dispatching with any other `runner` value uses it as a literal self-hosted
runner label, which is how a manually prepared host (see
`ecs-image/bootstrap-runner-host.sh`) runs the workflow. For PR comment
admission, set the repository variable `QUERY_REGRESSION_PR_RUNNER` to such
a label to redirect those runs away from ECS.

The office ARC scale set `perf-regression-8-cores` that previously ran this
workflow is retired; see git history for its values files and pause/deploy
procedures. Tearing down the office cluster (helm release, runner namespace,
and the `query-regression-build-cache` PVC) is a manual operator action
outside this repository. This directory keeps its historical
`runner-scale-sets` name for path stability.

## Nightly vs previous nightly

`query-regression-nightly.yml` runs after a successful `GreptimeDB Nightly
Build` (`workflow_run`). It resolves that run's `head_sha` as the candidate
and the previous successful nightly (same branch, typically Friday when
Monday's nightly fires) as the base, then calls `query-regression.yml` with
those immutable SHAs. Both binaries are still compiled on the ECS runner;
this is not an artifact-download path. `workflow_dispatch` can pass explicit
`base_ref` / `candidate_ref` or a nightly run id. If there is no previous
nightly, or both nightlies built the same commit, the comparison is skipped.

## Aliyun ECS path

Configuration lives in repository variables/secrets:

| Kind | Name | Purpose |
| --- | --- | --- |
| secret | `ALICLOUD_ECS_ACCESS_KEY_ID` / `ALICLOUD_ECS_ACCESS_KEY_SECRET` | RAM user scoped to ECS RunInstances/DeleteInstances/Describe*/CreateImage/RunCommand. Used only by provision/teardown jobs on `ubuntu-latest`; never reaches the ECS instance. |
| secret | `GH_PERSONAL_ACCESS_TOKEN` | Creates the short-lived runner registration token (shared with the jsonbench EC2 path) and `repository_dispatch` events for slash-command-dispatch (`repo` scope; `GITHUB_TOKEN` cannot dispatch). |
| vars | `ALIYUN_ECS_REGION_ID` / `ALIYUN_ECS_VSWITCH_ID` / `ALIYUN_ECS_SECURITY_GROUP_ID` | Network placement. The security group should allow egress only; no inbound rules are needed. The vSwitch pins the zone. |
| vars | `ALIYUN_ECS_INSTANCE_TYPE` | Dedicated (non-burstable, non-shared) instance family. Prefer 32 GiB (e.g. `ecs.g8i.2xlarge`); `ecs.c9i.2xlarge` is 8c16g and nightly thin-LTO of greptime peaks above that. Both base and candidate clusters run on the same machine, so noisy neighbors break thresholds. |
| vars | `QUERY_REGRESSION_ECS_IMAGE_ID` | Custom image built by `ecs-image/build-ecs-image.py`. |
| vars | `QUERY_REGRESSION_COMMENT_ALLOWLIST` | Comma/whitespace-separated GitHub logins allowed to comment `/query-regression` on a PR. Each login must also have repository `admin` permission. Empty denies all comment commands. |

The system disk is 40 GiB, which covers the image, a 16 GiB swapfile, the
checkout, and cold build caches (target dir, cargo registry, sccache). ENOSPC
stops the runner itself from writing logs, which GitHub reports as `The
operation was canceled` with no telemetry, indistinguishable from a
platform-side cancellation.
cloud-init masks `systemd-oomd`, disables `unattended-upgrades` /
`apt-daily-upgrade`, creates `/swapfile`, and sets `OOMPolicy=continue`
on the runner unit. Ubuntu 24.04 defaults to `DefaultOOMPolicy=stop`,
which SIGTERM-s the whole unit when rustc is OOM-killed and GitHub
reports `The operation was canceled` with no telemetry. Unattended
upgrades can do the same via `systemctl restart` of the runner after a
library update; GitHub then records `UserCancelled` even though the job
was still valid. Swap is a safety net for 16 GiB types, not a substitute
for 32 GiB; linking on swap is slow.

When a run dies with an unexplained `The operation was canceled` (no
"Canceled by" banner, healthy machine), re-dispatch with `keep_instance`
checked: the teardown job is skipped and the instance survives for
post-mortem inspection. The security group is egress-only, so inspect via
Cloud Assistant (`RunCommand`) or VNC: the runner's `_diag` logs under the
runner home record reconnects and worker crashes, `journalctl -u
ephemeral-github-runner.service` mirrors the console stream, `dmesg -T`
shows kernel OOM kills, and `machine-telemetry.log` in the job workspace
has the 30 s sampler history. The janitor sweep still deletes the instance
after its TTL, so finish the inspection within that window.

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

## Trust admission for PR runs

An allowlisted repository admin commenting `/query-regression` on the PR is
**trust admission for that exact PR revision**. `/query-regression` runs the
six routine default cases; `/query-regression heavy` runs only the
high-cardinality `prom_remote_write_7913` remote-write case.
`slash-command-dispatch.yml` (`issue_comment` on the default branch) parses
the command and `repository_dispatch`es; `query-regression-slash.yml` admits
the revision so secrets work for fork PRs. It snapshots merge, head, and base
SHAs at admission. A queued job fetches that immutable event merge SHA
directly, verifies it is a two-parent merge whose parents include the
snapshotted head exactly once, and uses its other parent as the actual base
build revision. The snapshotted event base is retained for audit only, so a
difference from the merge's non-head parent is not a failure. The job never
follows a newer mutable PR merge ref. An unavailable event merge, or one that
does not contain exactly one snapshotted head parent, fails closed. A later
PR head change does not retarget an already queued run: it may execute only
its previously trusted event revision if that revision remains fetchable. To
run the new revision, the maintainer must review it and comment
`/query-regression` again; cancel the old run if it is no longer wanted.

The commenter must be in `QUERY_REGRESSION_COMMENT_ALLOWLIST` and have
repository `admin` permission.

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
and exact tool versions: `libprotoc 3.21.12`, `uv 0.11.26`, `mold 2.40.4`,
`Python 3.14.4`, `sccache 0.16.0`, `otelgen` commit
`863a3f395d062c7322cc1de08a38774b7fdaa6c8`, root-owned `rustup 1.29.0`, and
the image-baked `nightly-2026-03-21` Rust toolchain. `mold` and `python3`
come from apt at image-build time (not Ubuntu 24.04's default 3.12); bump
the Verify pins together with `QUERY_REGRESSION_ECS_IMAGE_ID` when the
image is rebuilt. Rustup, Cargo, and Rustc
must resolve from `/opt/cargo/bin`; the runner cannot write `/opt/rustup` or
`/opt/cargo/bin`. Protobuf well-known includes, including
`google/protobuf/any.proto` and `google/protobuf/empty.proto`, are an image
contract and must compile with `protoc`.
`actions-rust-lang/setup-rust-toolchain@v1` is intentionally removed. The
workflow sets its warning-denying mold `RUSTFLAGS` directly, disables
automatic Rustup installation, and performs no runtime toolchain downloads.

The workflow no longer uses GitHub `rust-cache`, `setup-protoc`, `setup-uv`,
or runtime Rust setup: the image establishes immutable executable state and
each instance's system disk holds only that run's Cargo data. Do not
reintroduce those actions unless the image contract changes.

## Capacity

Each run provisions its own ECS instance, so overlapping dispatches proceed
in parallel. There is no workflow `concurrency` group. All Cargo state
(`CARGO_HOME` including registry/git, `CARGO_TARGET_DIR`, sccache, cache
metadata) lives on the 40 GiB system disk and is discarded with the VM.
`RUSTUP_HOME=/opt/rustup` and `/opt/cargo/bin` are image-owned. The runner
sets `RUSTC_WRAPPER=/usr/local/bin/sccache`,
`SCCACHE_DIR=/home/runner/.cache/sccache`, `SCCACHE_CACHE_SIZE=10G`, and
`CARGO_INCREMENTAL=0`. sccache uses its local disk backend and self-evicts
at 10G. Base and candidate builds share the target on that disk; Cargo
fingerprints invalidate source and dependency changes.

The workflow reports `du` / `df` in telemetry. It does not try to reclaim
space across runs: a cold 40 GiB disk that fills up fails the build.

## Future optional phases

The current phase uses a materialized runner toolchain with sccache 0.16.0 and
no shared cache service. Optional follow-ups are an image additionally seeded
with `cargo fetch --locked` results, or an internal read/write sccache
backend or Cargo/Git mirror. Evaluate them only if cold compile time on the
system disk becomes the bottleneck.
