#!/usr/bin/env bash

set -euo pipefail

usage() {
    printf 'Usage: %s --context PATH --image REFERENCE --platform PLATFORM --mode local|push [--dockerfile PATH] [--engine auto|docker|podman] [--allow-local-tag-overwrite]\n' "$0" >&2
}

context=""
image=""
platform=""
mode=""
dockerfile=""
engine="auto"
allow_local_tag_overwrite=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --context|--image|--platform|--mode|--dockerfile|--engine)
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
                --engine) engine="$2" ;;
            esac
            shift 2
            ;;
        --allow-local-tag-overwrite)
            allow_local_tag_overwrite=true
            shift
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

if [[ "$engine" != "auto" && "$engine" != "docker" && "$engine" != "podman" ]]; then
    printf 'error: --engine must be auto, docker, or podman\n' >&2
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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$script_dir/binary_platform.py" --binary "$context/greptime" --platform "$platform"

check_collision() {
    local runtime="$1"
    if [[ "$allow_local_tag_overwrite" == true ]]; then
        return 0
    fi
    if [[ "$runtime" == docker ]] && docker image inspect "$image" >/dev/null 2>&1; then
        printf 'error: local image tag exists: %s (pass --allow-local-tag-overwrite to replace it)\n' "$image" >&2
        exit 3
    fi
    if [[ "$runtime" == podman ]] && podman image exists "$image"; then
        printf 'error: local image tag exists: %s (pass --allow-local-tag-overwrite to replace it)\n' "$image" >&2
        exit 3
    fi
}

if [[ "$engine" != podman ]] && command -v docker >/dev/null 2>&1 && server_platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null)"; then
    if [[ "$platform" == "$server_platform" ]]; then
        check_collision docker
        docker build --platform "$platform" -f "$dockerfile" -t "$image" "$context"
        if [[ "$mode" == "push" ]]; then
            exec docker push "$image"
        fi
        exit 0
    fi

    if [[ "$mode" == "push" ]]; then
        exec docker buildx build --platform "$platform" --push -f "$dockerfile" -t "$image" "$context"
    fi
    check_collision docker
    exec docker buildx build --platform "$platform" --load -f "$dockerfile" -t "$image" "$context"
fi

if [[ "$engine" != docker ]] && command -v podman >/dev/null 2>&1; then
    native_platform="$(podman info --format '{{.Host.Os}}/{{.Host.Arch}}' 2>/dev/null || true)"
    case "$native_platform" in
        linux/amd64|linux/arm64) ;;
        *)
            printf 'error: Podman is unavailable or has unsupported platform: %s\n' "$native_platform" >&2
            exit 1
            ;;
    esac
    if [[ "$platform" != "$native_platform" ]]; then
        printf 'error: Docker Buildx is required for non-native platform %s\n' "$platform" >&2
        exit 1
    fi
    check_collision podman
    podman build -f "$dockerfile" -t "$image" "$context"
    if [[ "$mode" == "push" ]]; then
        exec podman push "$image"
    fi
    exit 0
fi

printf 'error: Docker or Podman is required\n' >&2
exit 1
