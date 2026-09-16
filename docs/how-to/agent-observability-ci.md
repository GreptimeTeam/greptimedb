# Manual agent-observability benchmark

Run `.github/workflows/agent-observability.yml` using `workflow_dispatch`.
Version one records benchmark timings and correctness, not a performance
regression verdict or publishable cross-database benchmark.

## Prerequisites

- Configure the same Aliyun secrets and repository variables as Query Regression:
  `ALICLOUD_ECS_ACCESS_KEY_ID`, `ALICLOUD_ECS_ACCESS_KEY_SECRET`,
  `GH_PERSONAL_ACCESS_TOKEN`; `ALIYUN_ECS_REGION_ID`, `ALIYUN_ECS_VSWITCH_ID`,
  `ALIYUN_ECS_SECURITY_GROUP_ID`, and
  `QUERY_REGRESSION_ECS_IMAGE_ID`. Optional resource group and runner UID/GID
  inputs also use the existing variables.
- The prepared Linux x86-64 ECS runner image must allow passwordless sudo and
  package installation. The workflow installs Docker, jq and ACL tools. Size
  the instance for the DB limit plus the runtime's 2 CPUs / 2 GiB and OS overhead.
- Verify the configured region and network can reach GitHub, Docker Hub, the
  package mirror and Aliyun registry. Overseas placement is configuration, not
  enforced by the workflow.
- Publish an anonymously pullable `o11ybench-runtime` image to Aliyun registry
  with `VCS_REF` set to the exact o11ybench commit. That commit must contain the
  Docker stage runner. Configure the published digest once in the repository
  variable `O11YBENCH_RUNTIME_IMAGE`. Runtime publishing is separate; this workflow does not
  build images or require registry push credentials.

## Inputs and execution

`targets` accepts `greptimedb`, `clickhouse`, `victorialogs`, a comma-separated
subset, or `all`. `dataset` is `S` (5K), `P` (10M), or `M` (100M); arbitrary row counts are not
supported. All three profiles retain the canonical seed `20260710` and fixed
data/query windows; dispatch does not expose a seed override.
Database tags and DB CPU/memory limits have defaults. `runtime_image` is an
optional override of `O11YBENCH_RUNTIME_IMAGE`; `o11ybench_ref` is an optional
full-SHA override. By default, the workflow checks out the runtime image's
`org.opencontainers.image.revision`, so users do not need to supply a matching
image/ref pair. An explicit ref must still match the runtime revision.

Tags (including `latest`) or `tag@sha256:<digest>` references are pulled once and resolved to immutable local
image IDs. The manifest retains requested tags and registry digests. Runtime
revision mismatch fails before generating data. Only selected DB images are
pulled. DB images come from Docker Hub; runtime comes from Aliyun.

The workflow creates one ECS runner and checks out `greptimedb/` and
`o11ybench/`. YAML invokes the existing o11ybench generate/target/cleanup stages:

1. Generate one dataset under `$GITHUB_WORKSPACE/benchmark-data/<run>-<attempt>`.
2. For each selected DB, sequentially start, load, probe capabilities, validate
   common16 queries, benchmark and remove its container. A DB failure does not
   prevent trying the remaining selected DBs; the job still fails.
3. Retain each selected target's runtime-generated summaries. Compare cross-target
   results only when all three targets complete successfully. Runtime exit codes
   determine success; this workflow does not revalidate benchmark results.
4. Always attempt container cleanup and artifact upload, then delete ECS and
   unregister its runner from a separate GitHub-hosted job. The existing janitor
   is the fallback if normal teardown cannot run.

Ports are explicit workflow environment values passed to the stage script.
Dataset and results stay on the ECS workspace disk, not `/tmp`. Artifacts retain
query results, latencies (including P95/P99), provenance, logs and stage timing
files, but exclude `corpus/agent_observations.jsonl`. Results from a single DB do
not require artifacts from the other DBs. No repeated dataset SHA256 scan is added.

Offline validation:

```sh
python3 tests/perf/test_aliyun_ecs_runner_scripts.py
actionlint .github/workflows/agent-observability.yml
```

Cloud execution costs money and is not part of these offline tests. After merge,
start with S and one target before trying all three or P.

M uses the existing 100M profile and its eligibility gates. It does not by itself
reproduce every conditioning/resource step of the formal 100M handoff. The handoff
corpus alone is approximately 90 GB; provision disk space for both corpus and DB
state. Resource and time budgets below are configurable; they do not establish
that a given machine can complete a 100M run.
Use an o11ybench commit/runtime containing Docker M support. No 100M cloud run
has been validated by the offline tests.

VictoriaLogs defaults to the previously validated `v1.52.0` and its fixed digest,
not `latest`. An explicit dispatch override is still supported. Query Regression
now uses the same create/delete actions without changing its workload or cleanup
conditions; its nightly, slash-command and release callers inherit this reuse.

## ECS resource and lifetime overrides

All of these dispatch inputs have defaults:

| Input | Default | Meaning |
| --- | --- | --- |
| `ecs_instance_type` | `ecs.c9i.2xlarge` (explicit dispatch default) | Whole-machine ECS type; independent of DB container limits |
| `system_disk_gib` | 500 | System disk in GiB, including corpus, DB, images, OS and existing 16 GiB swap |
| `benchmark_timeout_minutes` | 360 | Whole benchmark job budget, including setup and artifact upload (1..360) |
| `janitor_ttl_hours` | 8 | Per-instance lifetime before janitor cleanup eligibility (1..168) |

TTL must cover the benchmark timeout plus two hours: 45 minutes for provision,
15 minutes for teardown, and 60 minutes of queue headroom. Longer queue delays
consume this margin; increase TTL if necessary. The janitor runs on its existing
schedule, not exactly at the expiry instant. Normal teardown still deletes the
instance immediately after the run, regardless of TTL.

The create action retains an 80 GiB disk default for existing callers. With no
`ttl-hours` override it adds no TTL tag, preserving the janitor's existing
four-hour fallback. The observability workflow explicitly passes its own
settings. Instances with malformed TTL tags are reported and skipped rather
than falling back to a shorter deadline; repair their tags or delete them
manually. Deploy the updated janitor to the default branch before launching
long runs: the old janitor does not understand per-instance TTL tags.

The system disk accepts 20..2048 GiB, subject to the ECS image/provider minimum.
The default 500 GiB is a capacity budget, not a measured guarantee for every
workload. The provisioner still enables its existing 16 GiB swap; this change
does not tune swap or the runtime's fixed 2 CPU / 2 GiB limits. In particular,
an 8 GiB ECS host cannot safely budget the default 8 GiB DB limit plus runtime
and OS overhead: lower the DB limit or select a larger host.
