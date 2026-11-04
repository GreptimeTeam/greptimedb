#!/usr/bin/env bash

set -euo pipefail

: "${FUZZ_TARGETS:?FUZZ_TARGETS is required}"
: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"

targets=()
while IFS= read -r target; do
  target="${target%$'\r'}"
  [[ -z "${target}" ]] && continue
  if [[ ! "${target}" =~ ^[a-zA-Z0-9_]+$ ]]; then
    printf 'Invalid fuzz target: %s\n' "${target}" >&2
    exit 2
  fi
  targets+=("${target}")
done <<<"${FUZZ_TARGETS}"

if [[ "${#targets[@]}" -eq 0 ]]; then
  printf 'FUZZ_TARGETS contains no targets\n' >&2
  exit 2
fi

# Corpus identity depends on the targets, not the source revision or their order.
target_set="$(printf '%s\n' "${targets[@]}" | LC_ALL=C sort -u)"
target_hash="$(printf '%s\n' "${target_set}" | sha256sum | cut -d ' ' -f 1)"
corpus_root="${GITHUB_WORKSPACE}/tests-fuzz/corpus"

{
  printf 'key-prefix=fuzz-corpus-v1-%s-\n' "${target_hash}"
  printf 'paths<<FUZZ_CORPUS_PATHS\n'
  while IFS= read -r target; do
    mkdir -p "${corpus_root}/${target}"
    printf '%s\n' "${corpus_root}/${target}"
  done <<<"${target_set}"
  printf 'FUZZ_CORPUS_PATHS\n'
} >>"${GITHUB_OUTPUT}"
