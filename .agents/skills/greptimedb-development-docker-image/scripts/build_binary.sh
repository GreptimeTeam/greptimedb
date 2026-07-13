#!/usr/bin/env bash

set -euo pipefail

usage() {
    printf 'Usage: %s --source PATH --bin NAME --profile PROFILE [--target RUST_TARGET]\n' "$0" >&2
}

source_dir=""
binary_name=""
profile=""
target=""
package=""
features=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source|--bin|--profile|--target|--package|--features)
            if [[ $# -lt 2 ]]; then
                usage
                exit 2
            fi
            case "$1" in
                --source) source_dir="$2" ;;
                --bin) binary_name="$2" ;;
                --profile) profile="$2" ;;
                --target) target="$2" ;;
                --package) package="$2" ;;
                --features) features="$2" ;;
            esac
            shift 2
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

if [[ -z "$source_dir" || -z "$binary_name" || -z "$profile" ]]; then
    usage
    exit 2
fi

if [[ ! -f "$source_dir/Cargo.toml" ]]; then
    printf 'error: not a Cargo workspace: %s\n' "$source_dir" >&2
    exit 1
fi

command=(cargo build --locked --bin "$binary_name" --profile "$profile")
if [[ -n "$package" ]]; then
    command+=(--package "$package")
fi
if [[ -n "$features" ]]; then
    command+=(--features "$features")
fi
if [[ -n "$target" ]]; then
    command+=(--target "$target")
fi

printf 'building %s with profile %s\n' "$binary_name" "$profile"
(
    cd "$source_dir"
    exec "${command[@]}"
)
