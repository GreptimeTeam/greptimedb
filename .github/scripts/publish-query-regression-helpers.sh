#!/usr/bin/env bash
set -euo pipefail
: "${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be configured by the runner}"
revision="$(git rev-parse HEAD)"
repository=greptime-registry.cn-hangzhou.cr.aliyuncs.com/tools/query-regression-helpers
# A rerun gets a distinct tag; consumers always use the registry digest.
tag="${repository}:${revision}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
staging="$(mktemp -d)"
trap 'rm -rf -- "$staging"' EXIT
cargo build --profile nightly -p cmd --features dev-tools \
  --bin query_perf_fixture --bin query_regression_runner
for binary in query_perf_fixture query_regression_runner; do
  "$CARGO_TARGET_DIR/nightly/$binary" --help >/dev/null
  cp "$CARGO_TARGET_DIR/nightly/$binary" "$staging/$binary"
done
docker build --build-arg "SOURCE_REVISION=$revision" \
  --file docker/query-regression-helpers/Dockerfile --tag "$tag" "$staging"
# ACR token exchange can time out even after docker login succeeds. Retry
# publication without rebuilding the binaries or changing the image tag.
for attempt in 1 2 3; do
  if timeout 600 docker push "$tag"; then
    break
  fi
  if [[ "$attempt" == 3 ]]; then
    echo "Helper image push failed after $attempt attempts." >&2
    exit 1
  fi
  echo "Helper image push failed (attempt $attempt/3); retrying in 15 seconds." >&2
  sleep 15
done
image="$(docker image inspect "$tag" --format '{{index .RepoDigests 0}}')"
[[ "$image" == "$repository@sha256:"* ]]
# Verify the published artifact through the same extraction path as consumers.
python3 .github/scripts/query-regression-helpers.py "$image" "$staging/extracted"
for binary in query_perf_fixture query_regression_runner; do
  cmp "$staging/$binary" "$staging/extracted/$binary"
done
# Hold the revision and cases constant: compare source-built versus extracted
# fixture normalization twice. Keep the raw outputs as workflow artifacts.
mkdir -p helpers-validation
printf '%s\n' "$revision" > helpers-validation/source-revision.txt
printf '%s\n' "$image" > helpers-validation/image.txt
for iteration in 1 2; do
  while IFS= read -r case; do
    name="$(basename "$(dirname "$case")")"
    for mode in source image; do
      binary="$staging/query_perf_fixture"
      [[ "$mode" != image ]] || binary="$staging/extracted/query_perf_fixture"
      "$binary" plan --case "$case" > "helpers-validation/${name}-${iteration}-${mode}.json"
    done
    cmp "helpers-validation/${name}-${iteration}-source.json" \
        "helpers-validation/${name}-${iteration}-image.json"
  done < <(find tests/perf/query_cases -name case.toml -type f | sort)
done
printf 'image=%s\n' "$image" >> "$GITHUB_OUTPUT"
printf 'Published helpers: `%s` (source `%s`)\n' "$image" "$revision" >> "$GITHUB_STEP_SUMMARY"
