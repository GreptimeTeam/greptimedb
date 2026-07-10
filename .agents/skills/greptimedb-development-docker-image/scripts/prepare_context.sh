#!/usr/bin/env bash

set -euo pipefail

usage() {
    printf 'Usage: %s --context PATH --binary PATH --dockerfile PATH\n' "$0" >&2
}

context=""
binary=""
dockerfile=""

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
        *)
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$context" || -z "$binary" || -z "$dockerfile" ]]; then
    usage
    exit 2
fi

if [[ ! -f "$binary" ]]; then
    printf 'error: binary does not exist: %s\n' "$binary" >&2
    exit 1
fi

if [[ ! -f "$dockerfile" ]]; then
    printf 'error: Dockerfile template does not exist: %s\n' "$dockerfile" >&2
    exit 1
fi

mkdir -p "$context"
cp "$binary" "$context/greptime"

if [[ ! -e "$context/Dockerfile" ]]; then
    cp "$dockerfile" "$context/Dockerfile"
    printf 'created Dockerfile: %s\n' "$context/Dockerfile"
else
    printf 'kept existing Dockerfile: %s\n' "$context/Dockerfile"
fi

file "$context/greptime"
