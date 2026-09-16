# Ephemeral Aliyun ECS runners

`aliyun-ecs-create` and `../aliyun-ecs-delete` wrap the existing provisioning
and teardown scripts. They must run on GitHub-hosted Linux runners, with the
repository checked out. Pass credentials explicitly; composite actions do not
read repository secrets themselves.

Use three jobs: create, workload (`runs-on: needs.create.outputs.label`), and
delete. The delete job must use `always()` and depend on both earlier jobs.
Pass its `instance-id` and `runner-name` from create's outputs, including when
provisioning fails after creating an instance. See `agent-observability.yml`.

The prepared ECS image, UID/GID, repository variables and secrets are the same
as query regression. Existing ownership tags and runner naming are retained. `system-disk-gib`
defaults to 80; optional `ttl-hours` sets a per-instance janitor deadline relative
to ECS creation time. Without that tag, the existing four-hour fallback applies.
Budget for provisioning, queueing, workload and teardown before the TTL. The
updated janitor must be deployed before tagged long-lived runs are launched. No general-purpose ECS image or Kubernetes cluster is provisioned.
