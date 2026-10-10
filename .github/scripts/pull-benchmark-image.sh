#!/usr/bin/env bash
# Pull only: never retry database writes, benchmarks, or cloud provisioning.
set -euo pipefail

if [[ $# != 1 || -z "$1" || "$1" == -* ]]; then
    echo "Usage: $0 IMAGE" >&2
    exit 2
fi
image=$1
attempts=3
for ((attempt=1; attempt<=attempts; attempt++)); do
    echo "Pulling $image (attempt $attempt/$attempts)" >&2
    if timeout --kill-after=10s 180s docker pull "$image"; then
        exit 0
    else
        status=$?
    fi
    if ((attempt == attempts)); then
        echo "Failed to pull $image after $attempts attempts (exit $status)" >&2
        exit "$status"
    fi
    delay=$((attempt * 10))
    echo "Pull failed (exit $status); retrying in ${delay}s" >&2
    sleep "$delay"
done
