---
name: greptimedb-fuzz-ci-failure-investigation
description: Investigate a failed GreptimeDB fuzz CI target link by downloading GitHub Actions job logs plus fuzz artifacts such as kind logs, monitor dumps, and CSV dumps, then correlate the failure with local GreptimeDB source code. Use when the user provides a failed fuzz CI target/job URL or asks to diagnose GreptimeDB fuzz CI failures.
---

# GreptimeDB Fuzz CI Failure Investigation

Investigate failed fuzz-test jobs for **`GreptimeTeam/greptimedb`** from a local
checkout. The purpose is to download the failed job's CI output and fuzz artifacts,
then explain the likely cause by correlating the evidence with GreptimeDB source.

Always pass `--repo GreptimeTeam/greptimedb` to `gh`; local remotes may point to
forks. Keep the workflow read-only: do not rerun jobs, cancel workflows, push,
comment on PRs, or delete artifacts unless the user explicitly asks.

## Scope

Use this skill for fuzz CI only. In `.github/workflows/develop.yml`, fuzz failures
are the CI jobs that use `.github/actions/fuzz-test`:

- `fuzztest` — standalone fuzz targets.
- `unstable-fuzztest` — unstable standalone fuzz target.
- `distributed-fuzztest` — distributed cluster fuzz targets.
- `distributed-fuzztest-with-chaos` — distributed fuzz targets with Chaos Mesh.

Standalone fuzz jobs usually only provide the GitHub Actions job log. Distributed
fuzz jobs additionally export cluster artifacts on failure.

The reusable action `.github/actions/fuzz-test/action.yaml` runs:

```bash
cargo fuzz run <target> --fuzz-dir tests-fuzz -D -s none ... -- -max_total_time=<seconds>
```

Distributed fuzz jobs are the ones that export cluster artifacts. In the current
workflow they upload these names on failure:

- `fuzz-tests-kind-logs-${{ matrix.mode.name }}-${{ matrix.target }}` from
  `/tmp/kind` after `kind export logs /tmp/kind`.
- `fuzz-tests-monitor-dumps-${{ matrix.mode.name }}-${{ matrix.target }}` from
  `/tmp/fuzz-monitor-dumps`, collected by
  `.github/scripts/collect-fuzz-monitor-artifacts.sh`.
- `fuzz-tests-csv-dumps-${{ matrix.mode.name }}-${{ matrix.target }}` from
  `/tmp/greptime-fuzz-dumps` for the non-chaos distributed fuzz job.

Example: for run `29000666156`, job `86062894741`, target `fuzz_alter_table`, and
mode `Remote WAL`, the kind logs artifact is:

```text
fuzz-tests-kind-logs-Remote WAL-fuzz_alter_table
```

## Inputs

The normal user input is a **CI target link**: a GitHub Actions job URL for one
failed matrix target. Treat that URL as the primary source of truth. Example:

```text
https://github.com/GreptimeTeam/greptimedb/actions/runs/29000666156/job/86062894741
```

Also accept these fallback inputs:

- A workflow run URL, e.g.
  `https://github.com/GreptimeTeam/greptimedb/actions/runs/29000666156`.
- A run id plus a fuzz target, mode name, artifact name, PR, branch, or commit SHA.

Parse ids from URLs:

```bash
REPO=GreptimeTeam/greptimedb
RUN_ID=29000666156
JOB_ID=86062894741
```

When the input is a job URL, do not ask for the target name first. Use the job id
to fetch metadata, then derive the target and mode from the job name. A distributed
fuzz job name usually contains the CI target tuple:

```text
Fuzz Test (Distributed, <mode>, <target>)
Fuzz Test with Chaos (Distributed, <mode>, <target>)
```

If only a run URL is provided, list failed jobs and choose the fuzz job matching the
target/mode. Ask only when multiple fuzz jobs are plausible and there is no target,
mode, artifact, or job-url clue.

## Prerequisites

- `gh` must be installed and authenticated (`gh auth status`). Actions logs and
  artifacts commonly require authentication even for a public repo.
- `jq`, `unzip`, and standard shell tools are useful for reducing logs.
- Use the local GreptimeDB checkout for source analysis. If the run's head SHA is
  not the current checkout, inspect the code at the run's SHA before making claims.
- Use a scratch directory outside the source tree, such as `/tmp/greptimedb-fuzz-ci`.

## 1. Capture run and job metadata

Create a scratch directory:

```bash
mkdir -p /tmp/greptimedb-fuzz-ci/$RUN_ID
cd /tmp/greptimedb-fuzz-ci/$RUN_ID
```

Fetch run metadata:

```bash
gh run view "$RUN_ID" --repo "$REPO" \
  --json databaseId,name,displayTitle,event,headBranch,headSha,status,conclusion,createdAt,updatedAt,url,workflowName \
  > run.json
```

Fetch all jobs through the REST API:

```bash
gh api "repos/$REPO/actions/runs/$RUN_ID/jobs?per_page=100" \
  --paginate --jq '.jobs[]' \
  | jq -s '{jobs: .}' > jobs.json
```

List failed fuzz-like jobs:

```bash
jq -r '
  .jobs[]
  | select(.conclusion != null and .conclusion != "success")
  | select(.name | test("Fuzz Test|fuzz"; "i"))
  | [.id, .name, .status, .conclusion, .html_url] | @tsv
' jobs.json
```

For a known `JOB_ID`, capture job name and step outcomes:

```bash
jq -r --argjson id "$JOB_ID" '
  .jobs[] | select(.id == $id) |
  "job=\(.name) conclusion=\(.conclusion) url=\(.html_url)",
  (.steps[]? | [.number, .name, .status, .conclusion, .started_at, .completed_at] | @tsv)
' jobs.json > job-summary.txt
```

Record the fuzz job kind, matrix mode, target, failed step, event type, branch, and
head SHA. Do not assume `main` for PR or merge-queue runs.

For the common job-link input, parse `mode` and `target` from the captured job name
and use them to construct artifact names. If parsing is inconvenient, keep the raw
job name and match artifacts by the target substring first, then by mode substring.

## 2. Download CI output for the failed job

Download the selected job's log; this is the authoritative target CI output for the
`cargo fuzz run` command:

```bash
gh run view "$RUN_ID" --repo "$REPO" --job "$JOB_ID" --log > job-$JOB_ID.log
```

Download the full workflow log only when setup/build context matters:

```bash
gh run view "$RUN_ID" --repo "$REPO" --log > run-$RUN_ID.log
```

Fallback if `gh run view --log` is incomplete:

```bash
gh api "repos/$REPO/actions/jobs/$JOB_ID/logs" > job-$JOB_ID.log
gh api "repos/$REPO/actions/runs/$RUN_ID/logs" > run-$RUN_ID-logs.zip
unzip -oq run-$RUN_ID-logs.zip -d run-logs
```

In the job log, inspect the `Run Fuzz Test` step first. Capture:

- exact target name;
- `max_total_time`;
- panic/assertion/backtrace or libFuzzer crash output;
- generated crash/reproducer path if printed;
- timeout or resource symptoms.

## 3. Find and download fuzz artifacts

List artifacts:

```bash
gh api "repos/$REPO/actions/runs/$RUN_ID/artifacts?per_page=100" \
  --paginate --jq '.artifacts[]' \
  | jq -s '{artifacts: .}' > artifacts.json
jq -r '.artifacts[] | [.id, .name, .expired, .size_in_bytes] | @tsv' artifacts.json
```

Derive the expected artifact names from the failed job name:

- Job name shape: `Fuzz Test (Distributed, <mode>, <target>)` or
  `Fuzz Test with Chaos (Distributed, <mode>, <target>)`.
- Expected kind artifact: `fuzz-tests-kind-logs-<mode>-<target>`.
- Expected monitor artifact: `fuzz-tests-monitor-dumps-<mode>-<target>`.
- Expected CSV artifact: `fuzz-tests-csv-dumps-<mode>-<target>` when present.

For a CI target/job link, this derivation is mandatory: the job URL gives `JOB_ID`,
`jobs.json` gives the full matrix job name, and the matrix job name gives
`<mode>`/`<target>`. Do not rely on the user to provide the artifact name.

Download exact artifacts when possible:

```bash
ARTIFACT='fuzz-tests-kind-logs-Remote WAL-fuzz_alter_table'
gh run download "$RUN_ID" --repo "$REPO" --name "$ARTIFACT" --dir artifacts/"$ARTIFACT"
```

Fallback by artifact id:

```bash
ARTIFACT_ID=$(jq -r --arg name "$ARTIFACT" '.artifacts[] | select(.name == $name and (.expired | not)) | .id' artifacts.json | head -n 1)
if [ -z "$ARTIFACT_ID" ]; then
  echo "Artifact not found or expired: $ARTIFACT"
  exit 1
fi
gh api "repos/$REPO/actions/artifacts/$ARTIFACT_ID/zip" > artifact-$ARTIFACT_ID.zip
mkdir -p artifacts/"$ARTIFACT"
unzip -oq artifact-$ARTIFACT_ID.zip -d artifacts/"$ARTIFACT"
```

For distributed fuzz failures, download at least the matching kind logs. Also
download matching monitor dumps and CSV dumps when available. Note expired or
missing artifacts explicitly; retention is currently short (`retention-days: 3`).

## 4. Understand artifact contents

Kind logs come from `kind export logs /tmp/kind` and usually include Kubernetes
state plus container logs. Prioritize:

- GreptimeDB component logs: frontend, datanode, metasrv, flownode, monitor.
- Kubernetes pod descriptions/events: restarts, OOMKilled, scheduling, image pull,
  volume, DNS, and network issues.
- External service logs: etcd, Kafka, Minio, Chaos Mesh, Kafka WAL helper.

Monitor dumps are collected by `.github/scripts/collect-fuzz-monitor-artifacts.sh`.
They may include:

- `state.log`, `sql.log`, `copy.log`, `port-forward.log`;
- `*.show_create_table.sql` for `_gt_logs` and OpenTelemetry trace tables;
- parquet dumps for `_gt_logs`, `opentelemetry_traces`,
  `opentelemetry_traces_operations`, and `opentelemetry_traces_services`.

CSV dumps come from fuzz targets under `/tmp/greptime-fuzz-dumps` and can contain
target-specific generated operations or state snapshots.

## 5. Build a layered diagnosis model

Before drilling into a specific error string, build a small macro-level model of
the failure. This prevents overfitting on the loudest symptom, such as
`libFuzzer: deadly signal`, `Internal error`, or a cleanup warning.

### 5.1 Create a failure timeline

Record the key timestamps in order:

- job start and setup steps;
- external dependencies ready, such as etcd, Kafka, Minio, and Chaos Mesh;
- GreptimeDB cluster ready;
- fuzz binary starts;
- first generated operation or SQL visible in the target log;
- first suspicious component log line;
- user-visible fuzz failure or panic;
- artifact collection and cleanup.

Use the timeline to classify the failure phase: setup, workload execution,
storage/WAL/write path, query path, cleanup, or runner infrastructure.

### 5.2 Trace the error propagation chain

For distributed fuzz failures, identify the chain from outer symptom to inner
cause candidate. Prefer this shape:

```text
fuzz panic/assertion
  -> client-visible SQL/gRPC/HTTP error
  -> frontend or monitor error
  -> meta/datanode/flownode component error
  -> storage/WAL/object-store/Kafka/etcd/k8s dependency error
```

At each boundary, decide whether that layer produced the error or merely wrapped
and forwarded a downstream error. Do not stop at wrapper errors unless there is no
deeper evidence.

### 5.3 Investigate by component boundary

For distributed targets, scan evidence in this order unless the timeline points
elsewhere:

1. fuzz target and generated operations;
2. frontend request handling and SQL/error response;
3. metasrv metadata/procedure/routing;
4. datanode/mito2/metric-engine region execution;
5. WAL, Kafka, object store, Minio, etcd, or other dependencies;
6. Kubernetes pod state, restarts, scheduling, DNS, network, and runner resource
   signals.

For each layer, answer two questions: "is this the first causal error?" and "is
this layer producing or propagating the failure?"

### 5.4 Verify intended config against runtime behavior

When a failure appears configuration-related, compare all three levels before
claiming a config did or did not take effect:

1. the workflow/action/Helm/YAML config at the tested SHA;
2. the actual pod/container runtime config or startup logs, if available;
3. the source path that consumes the config, including hard-coded defaults and
   library retry/backoff settings.

If a logged value differs from the expected config, explicitly state whether the
config was not applied, was overridden by another layer, or refers to a different
kind of timeout/retry/deadline.

### 5.5 Keep competing hypotheses until evidence rules them out

Before final classification, list at least two plausible causes and the evidence
that supports or weakens each, for example:

- product bug;
- fuzz/test invalid assumption or missing wait;
- config not applied or overridden;
- dependency/infra flake;
- timeout, retry, or resource pressure.

Then choose the most likely cause and keep the confidence scoped. It is often
valid to have high confidence in the observed error chain but only medium
confidence in the ultimate cause category.

### 5.6 State what would disprove the diagnosis

Include one or two falsifiable checks, such as a same-SHA rerun, a fixed
reproducer, a cross-target pattern in the same dependency mode, or one missing
artifact/log that would change the conclusion.

## 6. Reduce evidence before reading source

Extract high-signal lines from job logs and artifacts:

```bash
grep -RInE '(^|[^[:alpha:]])(ERROR|Error|WARN|panic|panicked|FAILED|failure|timeout|Timeout|assert|backtrace|SIG[A-Z]+|OOMKilled|CrashLoopBackOff|ImagePullBackOff|libFuzzer|AddressSanitizer|UndefinedBehaviorSanitizer)' \
  job-$JOB_ID.log run-$RUN_ID.log job-logs run-logs artifacts 2>/dev/null > signals.txt
```

Then identify the first causal failure, not the last cleanup error. A typical order:

1. `cargo fuzz run` output in `job-$JOB_ID.log`.
2. GreptimeDB component logs around the same timestamp.
3. Kubernetes events/restarts/OOMs.
4. Monitor/CSV dumps that show the generated SQL or cluster state.

Avoid common mistakes:

- Do not blame cleanup errors after the fuzz failure.
- Do not treat expected retry warnings as root cause unless they align with the
  assertion, crash, or timeout.
- Do not assume an artifact is relevant only because it contains `fuzz`; match mode
  and target.
- Do not diagnose from current local `main` if the run used a different SHA.

## 7. Inspect the tested source revision

Read the tested SHA:

```bash
HEAD_SHA=$(jq -r '.headSha // empty' run.json)
if [ -z "$HEAD_SHA" ]; then
  echo "Error: HEAD_SHA is empty or null"
  exit 1
fi

UPSTREAM_REMOTE=$(git remote -v \
  | awk '/GreptimeTeam\/greptimedb(\.git)?[[:space:]]/ { print $1; exit }')
UPSTREAM_REMOTE=${UPSTREAM_REMOTE:-origin}

git rev-parse --verify "$HEAD_SHA^{commit}" >/dev/null 2>&1 \
  || git fetch "$UPSTREAM_REMOTE" "$HEAD_SHA"
```

If local `HEAD` differs, inspect files at that SHA or create a temporary worktree:

```bash
git worktree add --detach /tmp/greptimedb-fuzz-ci/$RUN_ID/source "$HEAD_SHA"
```

Remove temporary worktrees after investigation. Never overwrite the user's current
branch.

## 8. Map logs to GreptimeDB code

Start from concrete strings: fuzz target name, panic text, assertion message,
error variant, SQL statement, or log message. Search/read the matching source at
the run SHA.

Useful entry points:

- `tests-fuzz/` — fuzz targets, operation generators, checkers, dump paths.
- `.github/actions/fuzz-test/action.yaml` — exact `cargo fuzz` invocation.
- `.github/workflows/develop.yml` — fuzz matrix and artifact upload names.
- `.github/scripts/collect-fuzz-monitor-artifacts.sh` — monitor dump contents.
- `src/mito2/AGENTS.md` — storage/WAL/region failures.
- `src/metric-engine/AGENTS.md` — metric/logical table failures.
- `src/frontend/AGENTS.md` — SQL/protocol/query orchestration failures.
- `src/meta-srv/AGENTS.md` — metadata/procedure/routing failures.
- `src/flow/AGENTS.md` — flow failures.

When reporting code evidence, cite the file/function/line and quote the log line
that reaches it. Separate confirmed facts from hypotheses.

## 9. Classify the cause

Use this taxonomy:

- **Product bug**: GreptimeDB panics, returns wrong results, loses data, or violates
  invariants under generated fuzz operations.
- **Fuzz/test bug**: the fuzz target/generator/checker makes an invalid assumption
  or fails to wait for an asynchronous condition it requires.
- **Flake/race**: timing/order/resource issue, usually supported by timeouts,
  retries, pod restarts, or prior similar failures.
- **Environment/infra**: runner disk/OOM, Docker/kind, network, dependency install,
  Kafka/Minio/etcd startup, or GitHub service issue.
- **Unknown**: logs/artifacts are expired or evidence conflicts.

## 10. Final response format

Use this structure:

```markdown
## Summary
- Likely cause: <one sentence>
- Confidence: high|medium|low
- Classification: product bug|fuzz/test bug|flake/race|environment/infra|unknown
- Confidence basis: <which parts are confirmed vs inferred>

## Evidence
- `<job log or artifact path>`: quoted line(s)
- `<source-file:line>`: relevant code path

## Reasoning
<short chain from fuzz target -> CI output -> artifact logs -> source code>

## Alternative hypotheses
- <hypothesis>: <supporting and weakening evidence>
- <hypothesis>: <supporting and weakening evidence>

## Next steps
1. <minimal fix, reproduction command, or verification command>
2. <rerun command or extra log needed, if any>

## What would disprove this
- <specific rerun, artifact, log line, or reproducer result that would change the diagnosis>
```

If logs or artifacts cannot be downloaded because `gh` is not authenticated or the
artifacts expired, say so directly and include the exact command/error.
