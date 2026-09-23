# CI runners

The agent observability and long-range benchmark workflows select `Aliyun` or
`AWS` through the `provider` dispatch input. `instance_type: auto` selects
`ecs.c9i.2xlarge` or `c7i.2xlarge`, respectively (8 vCPU, 16 GiB).
Existing build/release and JSONBench workflows keep their current runner actions.

Aliyun uses the existing ECS actions and image. Its provision and teardown
scripts share only GitHub API and output helpers from `runner_utils.py` with
the new AWS scripts.

AWS uses `aws-ec2-runner-provision.py` and `aws-ec2-runner-teardown.py` directly.
Repository variables supply `EC2_RUNNER_REGION`, `EC2_RUNNER_SUBNET_ID`, and
`EC2_RUNNER_SECURITY_GROUP_ID`; credentials use `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `GH_PERSONAL_ACCESS_TOKEN` secrets.
The current region is Singapore (`ap-southeast-1`). By default the script selects
the newest available Canonical Ubuntu 24.04 amd64 server AMI in that region
(owner `099720109477`) and logs its ID. Set `BENCHMARK_EC2_IMAGE_ID` to pin a
prepared Ubuntu AMI. Existing build CI's `EC2_RUNNER_LINUX_AMD64_IMAGE_ID` is
unaffected. Both workflows default `system_disk_gib` to `auto` (also accepting
`0`): agent observability uses S=80, P=100, M=500 GiB, and long-range uses 80 GiB.
These defaults are the same for both providers. Explicit values from 20 to 2048
GiB override them and must be at least the selected AMI's root snapshot size.
The official 24.04 image does not have the old custom AMI's 500 GiB minimum.

AWS user data registers a fresh ephemeral runner. Git, curl, tar, and sudo are
expected in the Ubuntu server image. Docker group membership is set before the
runner service starts, and this dedicated VM grants the runner passwordless sudo
so its job can install dependencies. Like ECS, the host creates 16 GiB swap with
swappiness 10, disables systemd-oomd and background automatic updates, and sets
`OOMPolicy=continue` on the runner service. Explicit setup package installation
remains enabled.

Both benchmark jobs call `.github/actions/setup-benchmark` after checkout. It
reuses existing tools, installs missing curl/jq/python3/rsync, installs Docker CE
if absent, and checks Docker access. Missing packages require passwordless sudo;
Aliyun's prepared image normally provides the tools and Docker already. Existing
Docker installations are preserved, so exact versions need not match between
providers. Provisioning must establish Docker permissions before starting the
runner; adding a group inside the running job would not update its groups.

The subnet must provide outbound access to Ubuntu and Docker packages, GitHub,
and the configured container registries. The IAM identity needs EC2
image/instance-type discovery, instance and volume tagging, launch, describe,
console output, and termination permissions. GitHub credentials need repository
runner administration access.

Creation publishes instance identity before waiting for registration. Teardown
can recover missing outputs using repository and workflow-attempt tags. The
daily AWS janitor (03:23 UTC) removes expired instances tagged `managed-by=query-regression-ci`
and unregisters their runners. Both providers use the existing
`managed-by=query-regression-ci`, `query-regression-run-id`, and
`runner-ttl-hours` tags. AWS retains a run-attempt suffix in its run identity to
isolate retries, plus repository and runner-name tags to restrict cleanup.
Legacy build instances do not match these ownership filters.
Both janitors use a 4-hour fallback when the TTL tag is absent and skip invalid
TTL tags. Benchmark jobs explicitly tag their configured TTL (default 8 hours).
Cleanup does not check whether the GitHub job is still running. Expired instances
are removed on the next daily sweep; normal workflow teardown runs immediately.
TTL is measured from instance launch, so it must cover provisioning and the
benchmark. The scheduled janitor becomes active after landing on the default
branch.

Local validation:

```sh
python3 tests/perf/test_ci_runner_scripts.py
python3 tests/perf/test_aliyun_ecs_runner_scripts.py
actionlint .github/workflows/agent-observability.yml .github/workflows/vmbench-long-range.yml .github/workflows/aws-ec2-runner-janitor.yml
```

A real AWS dispatch is still needed to verify AMI boot, package
installation, registry connectivity, and end-to-end benchmark execution.
