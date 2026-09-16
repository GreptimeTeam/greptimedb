# Manual agent-observability benchmark

Run `.github/workflows/agent-observability.yml` using `workflow_dispatch`.
Version one records benchmark timings and correctness, not a performance
regression verdict or publishable cross-database benchmark.

## Prerequisites

- Configure the same Aliyun secrets and repository variables as Query Regression:
  `ALICLOUD_ECS_ACCESS_KEY_ID`, `ALICLOUD_ECS_ACCESS_KEY_SECRET`,
  `GH_PERSONAL_ACCESS_TOKEN`; `ALIYUN_ECS_REGION_ID`, `ALIYUN_ECS_VSWITCH_ID`,
  `ALIYUN_ECS_SECURITY_GROUP_ID`, `ALIYUN_ECS_INSTANCE_TYPE`, and
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
  Docker stage runner. Runtime publishing is separate; this workflow does not
  build images or require registry push credentials.

## Inputs and execution

`targets` accepts `greptimedb`, `clickhouse`, `victorialogs`, a comma-separated
subset, or `all`. `dataset` is `S` (5K), `P` (10M), or `M` (100M); arbitrary row counts are not
supported. All three profiles retain the canonical seed `20260710` and fixed
data/query windows; dispatch does not expose a seed override.
Set the three database tags, DB CPU/memory limits, full
`o11ybench_ref` commit SHA, and Aliyun `runtime_image` reference.

Tags (including `latest`) are pulled once and resolved to immutable local
image IDs. The manifest retains requested tags and registry digests. Runtime
revision mismatch fails before generating data. Only selected DB images are
pulled. DB images come from Docker Hub; runtime comes from Aliyun.

The workflow creates one ECS runner and checks out `greptimedb/` and
`o11ybench/`. YAML invokes the existing o11ybench generate/target/cleanup stages:

1. Generate one dataset under `$GITHUB_WORKSPACE/benchmark-data/<run>-<attempt>`.
2. For each selected DB, sequentially start, load, probe capabilities, validate
   common16 queries, benchmark and remove its container. A DB failure does not
   prevent trying the remaining selected DBs; the job still fails.
3. Summarize selected targets. Compare cross-target results only when all three
   targets complete successfully.
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
python3 tests/perf/test_o11ybench_ci.py
python3 tests/perf/test_aliyun_ecs_runner_scripts.py
actionlint .github/workflows/agent-observability.yml
```

Cloud execution costs money and is not part of these offline tests. After merge,
start with S and one target before trying all three or P.

M uses the existing 100M profile and its eligibility gates. It does not by itself
reproduce every conditioning/resource step of the formal 100M handoff. The handoff
corpus alone is approximately 90 GB; provision disk space for both corpus and DB
state. The 120-minute job limit and existing four-hour ECS janitor still apply.
Use an o11ybench commit/runtime containing Docker M support. No 100M cloud run
has been validated by the offline tests.
