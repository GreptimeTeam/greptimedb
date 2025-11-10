#!/bin/bash

# This script is used to pull the test dependency images that are stored in public ECR one by one to avoid rate limiting.

set -e

MAX_RETRIES=3

IMAGES=(
  "greptime/zookeeper:3.7"
  "greptime/kafka:3.9.0-debian-12-r1"
  "greptime/etcd:3.6.1-debian-12-r3"
  "greptime/minio:2024"
  "greptime/mysql:5.7"
)

for image in "${IMAGES[@]}"; do
  for ((attempt=1; attempt<=MAX_RETRIES; attempt++)); do
    if docker pull "$image"; then
      # Successfully pulled the image.
      break
    else
      # Use some simple exponential backoff to avoid rate limiting.
      if [ $attempt -lt $MAX_RETRIES ]; then
        sleep_seconds=$((attempt * 5))
        echo "Attempt $attempt failed for $image, waiting $sleep_seconds seconds"
        sleep $sleep_seconds  # 5s, 10s delays
      else
        echo "Failed to pull $image after $MAX_RETRIES attempts"
        exit 1
      fi
    fi
  done
done
