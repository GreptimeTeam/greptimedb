#!/usr/bin/env bash

set -euo pipefail

usage() {
    printf 'Usage: %s --context PATH --image REFERENCE --platform PLATFORM --mode local|push [--dockerfile PATH]\n' "$0" >&2
}

context=""
image=""
platform=""
mode=""
dockerfile=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --context|--image|--platform|--mode|--dockerfile)
            if [[ $# -lt 2 ]]; then
                usage
                exit 2
            fi
            case "$1" in
                --context) context="$2" ;;
                --image) image="$2" ;;
                --platform) platform="$2" ;;
                --mode) mode="$2" ;;
                --dockerfile) dockerfile="$2" ;;
            esac
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$context" || -z "$image" || -z "$platform" || -z "$mode" ]]; then
    usage
    exit 2
fi

if [[ "$mode" != "local" && "$mode" != "push" ]]; then
    printf 'error: --mode must be local or push\n' >&2
    exit 2
fi

dockerfile="${dockerfile:-$context/Dockerfile}"
if [[ ! -f "$dockerfile" ]]; then
    printf 'error: Dockerfile does not exist: %s\n' "$dockerfile" >&2
    exit 1
fi

if [[ "$platform" == *,* ]]; then
    printf 'error: only one target platform is supported per image build\n' >&2
    exit 2
fi

case "$platform" in
    linux/amd64|linux/arm64) ;;
    *)
        printf 'error: unsupported platform: %s\n' "$platform" >&2
        exit 2
        ;;
esac

if [[ ! -f "$context/greptime" ]]; then
    printf 'error: expected executable is missing: %s/greptime\n' "$context" >&2
    exit 1
fi

if command -v docker >/dev/null 2>&1; then
    server_platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"
    if [[ "$platform" == "$server_platform" ]]; then
        docker build --platform "$platform" -f "$dockerfile" -t "$image" "$context"
        if [[ "$mode" == "push" ]]; then
            exec docker push "$image"
        fi
        exit 0
    fi

    if [[ "$mode" == "push" ]]; then
        exec docker buildx build --platform "$platform" --push -f "$dockerfile" -t "$image" "$context"
    fi
    exec docker buildx build --platform "$platform" --load -f "$dockerfile" -t "$image" "$context"
fi

if command -v podman >/dev/null 2>&1; then
    host_arch="$(uname -m)"
    case "$host_arch" in
        x86_64) native_platform="linux/amd64" ;;
        aarch64|arm64) native_platform="linux/arm64" ;;
        *)
            printf 'error: unsupported host architecture for Podman fallback: %s\n' "$host_arch" >&2
            exit 1
            ;;
    esac
    if [[ "$platform" != "$native_platform" ]]; then
        printf 'error: Docker Buildx is required for non-native platform %s\n' "$platform" >&2
        exit 1
    fi
    podman build -f "$dockerfile" -t "$image" "$context"
    if [[ "$mode" == "push" ]]; then
        exec podman push "$image"
    fi
    exit 0
fi

printf 'error: Docker or Podman is required\n' >&2
exit 1
