# Agent observability benchmark

Run [agent-observability.yml](../../.github/workflows/agent-observability.yml)
manually from GitHub Actions. It creates one Aliyun ECS runner, generates a
dataset once and benchmarks the selected databases sequentially. Results include
query timings and correctness checks; the workflow does not decide whether a
performance regression occurred.

## Setup

Reuse the Query Regression credentials and ECS configuration:

- Secrets: `ALICLOUD_ECS_ACCESS_KEY_ID`, `ALICLOUD_ECS_ACCESS_KEY_SECRET`,
  `GH_PERSONAL_ACCESS_TOKEN`.
- Variables: `ALIYUN_ECS_REGION_ID`, `ALIYUN_ECS_VSWITCH_ID`,
  `ALIYUN_ECS_SECURITY_GROUP_ID`, `QUERY_REGRESSION_ECS_IMAGE_ID`.
- Optional variables: `ALIYUN_ECS_RESOURCE_GROUP_ID`,
  `QUERY_REGRESSION_RUNNER_UID`, `QUERY_REGRESSION_RUNNER_GID`.

Use the prepared Linux x86-64 runner image, which includes Docker CE and jq.
The create action starts Docker and grants the runner Docker group access before
starting the job. The workflow does not need sudo or install packages. The host
needs access to GitHub, Docker Hub and Aliyun registry.

Publish a publicly pullable `o11ybench-runtime` image to Aliyun registry and set
`O11YBENCH_RUNTIME_IMAGE` to its digest reference. Build it with `VCS_REF` set to
the exact o11ybench commit containing the Docker stage runner and M support.
The workflow checks out that commit from the image's
`org.opencontainers.image.revision` label; it does not build the runtime image.

## Run

Once setup is complete, all dispatch inputs can use their defaults. Start with
`dataset=S` and one target.

| Input | Default | Options |
| --- | --- | --- |
| `targets` | `greptimedb` | `greptimedb`, `clickhouse`, `victorialogs`, a comma-separated subset, or `all` |
| `dataset` | `S` | S: 5K rows; P: 10M; M: 100M |
| `greptimedb_tag` | `latest` | Docker Hub tag or tag@digest |
| `clickhouse_tag` | `26.6.1.1193` | Docker Hub tag or tag@digest |
| `victorialogs_tag` | `v1.52.0`, pinned by digest in the workflow | Docker Hub tag or tag@digest |
| `ecs_instance_type` | `ecs.c9i.2xlarge` | ECS instance type |
| `system_disk_gib` | 500 | 20–2048 GiB, subject to the image/provider minimum |
| `db_cpus` / `db_memory` | `4` / `8g` | Per-database container limits |
| `benchmark_timeout_minutes` | 360 | 1–360 minutes, including setup and artifact upload |
| `janitor_ttl_hours` | 8 | 1–168 hours from instance creation |

`runtime_image` overrides the repository variable. `o11ybench_ref` optionally
specifies a full commit SHA and must match the runtime image's revision.
The dataset seed is fixed at `20260710`; data and query windows are fixed too.

Allow memory for the DB container, the runtime's 2 GiB limit and the OS. The
provisioner also configures 16 GiB of swap. A 100M dataset alone takes about
90 GB; reserve space for database files, images and artifacts as well. The
500 GiB default is not an available-space check.

## Execution and results

The workflow pulls the runtime from Aliyun and selected DB images from Docker
Hub, then records their immutable image IDs. YAML calls o11ybench's
`generate`, `target` and `cleanup` stages:

1. Generate the dataset under `$GITHUB_WORKSPACE/benchmark-data/<run>-<attempt>`.
2. Start each selected DB, load the data, probe capabilities, check the common
   16 queries, run the benchmark and remove the container.
3. Compare results if all three targets succeed. A target failure still allows
   the remaining targets to run, but fails the job.
4. Attempt cleanup and artifact upload, then delete ECS in a separate job.

The workflow passes fixed ports to the runner and uses the workspace disk rather
than `/tmp`. It does not add repeated dataset SHA-256 scans. Queries run after
loading without conditioning restarts or SWCS; this is not the full controlled
100M comparison protocol, and results remain non-publishable.

Artifacts include query results, latency summaries (P95/P99), image and source
versions, logs and stage timings. The dataset JSONL is excluded. Single-target
runs produce their own summaries without requiring the other databases.

## Cleanup

Normal teardown deletes the instance and unregisters its runner. The scheduled
janitor handles leftovers. Set its TTL to at least the benchmark timeout plus
two hours for provisioning, queueing and teardown; allow more for long queues.
TTL expiry makes an instance eligible for cleanup at the next janitor run.

Deploy the TTL-aware janitor to the default branch before launching long runs.
The old janitor uses four hours regardless of the instance's TTL tag. If a TTL
tag is malformed, the updated janitor logs the problem and skips the instance;
fix the tag or delete the instance manually.
