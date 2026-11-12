#!/usr/bin/env bash
set -euo pipefail
: "${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be configured by the runner}"
revision="$(git rev-parse HEAD)"
tag="query-regression-helpers:${revision}"
staging="$(mktemp -d)"
trap 'rm -rf -- "$staging"' EXIT
cargo build --profile nightly -p cmd --features dev-tools \
  --bin query_perf_fixture --bin query_regression_runner
for binary in query_perf_fixture query_regression_runner; do
  "$CARGO_TARGET_DIR/nightly/$binary" --help >/dev/null
  cp "$CARGO_TARGET_DIR/nightly/$binary" "$staging/$binary"
done
mkdir -p helpers-image helpers-validation
docker build --provenance=false --build-arg "SOURCE_REVISION=$revision" \
  --file docker/query-regression-helpers/Dockerfile --tag "$tag" "$staging"
container="$(docker create "$tag" /query_perf_fixture --help)"
trap 'docker rm --force --volumes "$container"; rm -rf -- "$staging"' EXIT
mkdir -p "$staging/extracted"
for binary in query_perf_fixture query_regression_runner; do
  docker cp "$container:/$binary" "$staging/extracted/$binary"
  chmod +x "$staging/extracted/$binary"
  "$staging/extracted/$binary" --help >/dev/null
  cmp "$staging/$binary" "$staging/extracted/$binary"
done
# Hold the revision and cases constant: compare source-built versus extracted
# fixture normalization twice. Keep the raw outputs as workflow artifacts.
printf '%s\n' "$revision" > helpers-validation/source-revision.txt
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
(cd "$staging" && sha256sum query_perf_fixture query_regression_runner) > helpers-image/SHA256SUMS
printf '%s\n' "$revision" > helpers-image/source-revision.txt
docker save "$tag" --output helpers-image/image.tar
