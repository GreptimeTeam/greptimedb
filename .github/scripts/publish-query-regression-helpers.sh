#!/usr/bin/env bash
set -euo pipefail
repository=greptime-registry.cn-hangzhou.cr.aliyuncs.com/tools/query-regression-helpers
revision="$(cat helpers-image/source-revision.txt)"
[[ "$revision" == "$GITHUB_SHA" ]]
tag="${repository}:${revision}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
# Match release-dev-builder-images.yaml: publish ACR images with Skopeo from
# a hosted runner. A failed publish can reuse the completed build artifact.
mkdir -p helpers-validation
docker run --rm \
  -v "${DOCKER_CONFIG:-$HOME/.docker}:/root/.docker:ro" \
  -v "$PWD/helpers-image:/image:ro" \
  -v "$PWD/helpers-validation:/result" \
  -e REGISTRY_AUTH_FILE=/root/.docker/config.json \
  quay.io/skopeo/stable:latest --command-timeout 20m copy --retry-times 3 \
  --digestfile /result/digest.txt \
  docker-archive:/image/image.tar "docker://$tag"
digest="$(cat helpers-validation/digest.txt)"
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]
image="$repository@$digest"
# The ECS build tested these exact bytes. Verify them again from the registry;
# hosted runners need not share the ECS runtime library versions.
python3 .github/scripts/query-regression-helpers.py "$image" helpers-validation/extracted \
  --checksums helpers-image/SHA256SUMS
jq -e --arg revision "$revision" '.source_revision == $revision' \
  helpers-validation/extracted/helpers-image.json >/dev/null
printf 'image=%s\n' "$image" >> "$GITHUB_OUTPUT"
printf 'Published helpers: `%s` (source `%s`)\n' "$image" "$revision" >> "$GITHUB_STEP_SUMMARY"
