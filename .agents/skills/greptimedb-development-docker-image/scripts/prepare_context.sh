#!/usr/bin/env bash

set -euo pipefail

usage() {
    printf 'Usage: %s --context PATH --binary PATH --dockerfile PATH --platform PLATFORM\n' "$0" >&2
}

context=""
binary=""
dockerfile=""
platform=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --context)
            context="$2"
            shift 2
            ;;
        --binary)
            binary="$2"
            shift 2
            ;;
        --dockerfile)
            dockerfile="$2"
            shift 2
            ;;
        --platform)
            platform="$2"
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$context" || -z "$binary" || -z "$dockerfile" || -z "$platform" ]]; then
    usage
    exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$script_dir/binary_platform.py" --binary "$binary" --platform "$platform"

if [[ ! -f "$dockerfile" ]]; then
    printf 'error: Dockerfile template does not exist: %s\n' "$dockerfile" >&2
    exit 1
fi

if [[ -e "$context" && ( -L "$context" || -n "$(ls -A "$context")" ) ]]; then
    printf 'error: build context must be a new or empty directory: %s\n' "$context" >&2
    exit 1
fi
mkdir -p "$context"
if [[ -e "$context/greptime" || -e "$context/Dockerfile" || -L "$context/greptime" || -L "$context/Dockerfile" ]]; then
    printf 'error: build context already contains output files: %s\n' "$context" >&2
    exit 1
fi
cp "$binary" "$context/greptime"
cp "$dockerfile" "$context/Dockerfile"
printf 'created Dockerfile: %s\n' "$context/Dockerfile"

file "$context/greptime"
