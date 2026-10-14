# Metric snapshot benchmark on AWS S3

The manual `Metric Snapshot S3 Benchmark` workflow tests packed Metric export and
restore against real S3 in `ap-southeast-1`. It must first exist on the repository's
default branch for GitHub to register `workflow_dispatch`. AWS measurements have
not yet been collected for this harness.

## Run

Use the repository's existing EC2 runner configuration and CI S3 credentials.
The workflow passes credentials only through CI; no local AWS credentials are
needed. Source, target and CLI use the same immutable image. This image's source
SHA and required CLI capabilities have been checked:

```bash
gh workflow run metric-snapshot-s3-bench.yml \
  --repo GreptimeTeam/greptimedb --ref main \
  -f greptimedb_image=greptime/greptimedb-nightly@sha256:20a1182bed0f452e6d573ca110b313e0b5cb76de7aedfa41a0c339963d4683d3 \
  -f expected_commit=f690bbb2040865c0d22b0b57d51612b60ae27cd0 \
  -f logical_tables_per_group=10000 \
  -f benchmark_timeout_minutes=240 \
  -f prior_spend_usd=0
```

Start a new dispatch for each attempt and update `prior_spend_usd` to include all
earlier cloud attempts in the campaign, including failed setup and cleanup.
Do not reset the estimate to zero when retrying. The workflow accepts 50–10,000
logical tables per group and a 30–240 minute benchmark job timeout.

The runner is fixed at `c6id.4xlarge` (16 vCPU, 32 GiB RAM). At the recorded
Singapore Linux on-demand estimate of $0.9408/hour, the default attempt reserves
$6.4392: four benchmark hours, 45 minutes of provisioning, 10 minutes for queueing,
20 minutes of cleanup, and $1.50 for S3, disk and IPv4 costs. Preflight rejects prior spend at or above
$15, or an attempt whose reserve would exceed the cumulative $20 budget.
These estimates and checks are not an AWS hard spending cap or an AWS bill.

## Workload and validation

- A 10-physical-table × 50-logical-table smoke runs first. Its 500 logical tables
  cover all 298 string tag columns before the larger workload starts.
- The default full run uses 10 physical tables and 100,000 logical tables, with
  32 rows per logical table: 3.2 million rows. Each logical schema has 10
  overlapping tags, a timestamp and a DOUBLE; schemas vary between tables.
- Source and target use local File storage on the temporary NVMe disk. Only
  snapshots use S3. Source validation finishes before the source process stops;
  restoration then starts a separate target process.
- Export uses `--pack-metric-data`, parallelism 4, chunk parallelism 1, and one
  complete time chunk. Import uses task parallelism 1 and automatic batch DDL.
- Every source and restored logical table is checked for membership, schema,
  DDL and typed values including NULLs. Target placeholders force table ID
  remapping; successful batch DDL and removal of import resume state are checked.

## Results and cleanup

The job summary reports complete export, structural verify and import durations;
data generation and full data validation are timed separately. Evidence artifacts
are credential-scanned and retained for 14 days. Keep conclusions limited to
this version's absolute AWS results; this workflow does not compare old versions.

Artifacts include sampled CPU/RSS, host memory, swap/OOM counters, server metrics,
CLI logs and S3 object/byte inventories. Configured swap is not RAM. A phase with
new swap-out or OOM fails and retains its evidence. OpenDAL operation counters
are not complete AWS HTTP request counts: multipart calls, retries and CLI traffic
are not fully covered. Object inventory bytes are not transferred bytes.

A separate GitHub-hosted teardown job starts when provisioning finishes and polls
the benchmark job. Its deadline includes queue time: allocation start plus 45
minutes, the benchmark timeout and 10 minutes. Completion, cancellation, an API
error or the deadline leads to cleanup. Failed provisioning skips the wait.
It terminates the EC2 instance and its disks, unregisters the runner, aborts
unfinished multipart uploads, deletes all object versions under the run's unique
prefix and verifies that prefix is empty. Check teardown completion before another
attempt. A deadline may leave the GitHub benchmark job queued after its resources
are removed; cancel that workflow manually. If cleanup fails, rerun the teardown
job only; it keeps the original attempt's resource identity and deadline. A new benchmark needs a new dispatch because teardown
removes its runner. The daily EC2 janitor is only a fallback and does not clean S3.
