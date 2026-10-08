# Aliyun ECS runners

Use `aliyun-ecs-create` to create an ECS runner and `aliyun-ecs-delete` to
remove the instance and unregister the runner. Both actions run on GitHub-hosted
Linux runners after checkout. Pass credentials through action inputs.

Split the workflow into three jobs:

1. Create the runner from a prepared ECS image.
2. Run the workload with `runs-on: ${{ needs.create.outputs.label }}`.
3. Delete the runner in a job with `if: always()` and dependencies on both jobs.
   Pass `instance_id` and `runner_name` from the create job's outputs, even if
   provisioning fails after creating the instance.

See [agent-observability.yml](../../workflows/agent-observability.yml) for a caller
and [action.yml](action.yml) for all inputs and outputs.

`system-disk-gib` defaults to 80. Optional `ttl-hours` sets when the janitor may
remove the instance, measured from ECS creation. Without it, the janitor uses
its four-hour fallback. Allow time for provisioning, queueing, the workload and
teardown. Normal teardown does not wait for the TTL.

Deploy the TTL-aware janitor to the default branch before using a TTL above
four hours; the old janitor ignores this setting. The actions retain the existing
query-regression ownership tags and runner naming.

Set `enable-docker: 'true'` for container workloads. Root cloud-init starts the
image-provided Docker CE service and adds the runner to the Docker group before
starting the runner. Docker group access is root-equivalent. This option defaults
to `false`, so existing query-regression jobs keep their current permissions.
