#!/usr/bin/env bash

set -euo pipefail

# Resolve build targets before runner allocation. Explicit manual selections are
# never expanded by publishing flags; incompatible selections fail early.
selection="${LINUX_ARCHITECTURES:-auto}"
case "${GITHUB_EVENT_NAME}" in
    push|schedule) selection=all ;;
    workflow_dispatch)
        if [[ "$selection" == auto ]]; then
            selection=none
            if [[ "${PUBLISH_GITHUB_RELEASE:-false}" == true || "${RELEASE_IMAGES:-false}" == true ]]; then
                selection=all
            fi
        fi
        ;;
    *) echo "Unsupported release event: ${GITHUB_EVENT_NAME}" >&2; exit 1 ;;
esac

case "$selection" in
    all) selection=amd64,arm64,riscv64 ;;
    none|amd64|arm64|riscv64|amd64,arm64|amd64,riscv64|arm64,riscv64) ;;
    *) echo "Invalid Linux architecture selection: $selection" >&2; exit 1 ;;
esac

amd64=false
arm64=false
riscv64=false
[[ ",$selection," != *,amd64,* ]] || amd64=true
[[ ",$selection," != *,arm64,* ]] || arm64=true
[[ ",$selection," != *,riscv64,* ]] || riscv64=true

if [[ "${PUBLISH_GITHUB_RELEASE:-false}" == true && "$amd64,$arm64,$riscv64" != true,true,true ]]; then
    echo "GitHub release publishing requires all Linux architectures; select all or auto." >&2
    exit 1
fi
if [[ "${RELEASE_IMAGES:-false}" == true && "$amd64,$arm64" != true,true ]]; then
    echo "Image publishing requires amd64 and arm64; select amd64,arm64, all, or auto." >&2
    exit 1
fi

printf 'amd64=%s\narm64=%s\nriscv64=%s\n' "$amd64" "$arm64" "$riscv64"
