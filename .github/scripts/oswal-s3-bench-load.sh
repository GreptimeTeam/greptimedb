#!/usr/bin/env bash
# Runs inside the o11ybench runtime image: the load of scripts/agent-observability/greptimedb.sh
# with the same arguments plus the loader's --skip-create-table, for a table the cell created.
set -euo pipefail

mkdir "/results/$RESULT_NAME"
mkdir -p /results/corpus
cp /work/corpus/summary.json /work/corpus/evidence.json /results/corpus/
w="$PWD/workloads/logbench-agent-observability"
r="/results/$RESULT_NAME"
prepare_workers=${PREPARE_WORKERS:-$(python3 -c 'import sys; sys.path.insert(0, "scripts"); from benchmark_workers import effective_cpus; print(effective_cpus())')}
o11ybench-greptimedb-loader --workload logbench-agent-observability --prepare-workers "$prepare_workers" \
    --skip-create-table \
    --grpc-endpoint "$GRPC_ENDPOINT" --sql-endpoint "$ENDPOINT" \
    --database public --table agent_observations --batch-size "${GREPTIMEDB_BATCH_SIZE:-10000}" --write-mode stream --max-in-flight "${MAX_IN_FLIGHT:-1}" \
    --flush-table --request-timeout-seconds 900 \
    --schema "$w/schema.json" --physical-index-profiles "$w/physical-index-profiles.json" \
    --data /work/corpus/agent_observations.jsonl \
    --profile-summary /work/corpus/summary.json --evidence /work/corpus/evidence.json \
    --transcript-output "$r/load-transcript.jsonl" \
    --target-metadata-output "$r/target-metadata.json" \
    --greptimedb-image "$TARGET_IMAGE" --greptimedb-version "$TARGET_VERSION" \
    --greptimedb-digest "$TARGET_DIGEST" \
    --startup-input=standalone --startup-input=start \
    --startup-input=--http-addr=0.0.0.0:4000 \
    --startup-input=--grpc-bind-addr=0.0.0.0:4001 \
    > "$r/load-result.json"
