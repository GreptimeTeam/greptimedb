#!/usr/bin/env bash

set -euo pipefail

# Publishing keeps the existing full-build behavior, regardless of checkboxes.
build_all=false
if [[ "${PUBLISH_GITHUB_RELEASE:-false}" == true || "${RELEASE_IMAGES:-false}" == true ]]; then
    build_all=true
fi
case "${GITHUB_EVENT_NAME}" in
    push|schedule) build_all=true ;;
    workflow_dispatch) ;;
    *) echo "Unsupported release event: ${GITHUB_EVENT_NAME}" >&2; exit 1 ;;
esac

for arch in amd64 arm64 riscv64; do
    input="BUILD_LINUX_${arch^^}_ARTIFACTS"
    selected=false
    if [[ "$build_all" == true || "${!input:-false}" == true ]]; then
        selected=true
    fi
    printf '%s=%s\n' "$arch" "$selected"
done
