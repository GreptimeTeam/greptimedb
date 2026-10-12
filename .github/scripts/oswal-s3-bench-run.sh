#!/usr/bin/env bash
# Host side of the object store WAL S3 benchmark (oswal-s3-bench.yml).
#   wal-disk DIR  mount the instance store at DIR when the host has one
#   generate      generate the dataset once with the o11ybench runtime
#   cells         run every cell of $PLAN, each on a fresh GreptimeDB container
set -euo pipefail

: "${PLAN:?}" "${RESULTS:?}" "${CORPUS_DIR:?}" "${OWNER:?}" "${RUNTIME_IMAGE:?}"
# Same isolation as the o11ybench runtime's own docker run.
runtime=(docker run --rm --user "$(id -u):$(id -g)" --label "o11ybench.owner=$OWNER"
    --security-opt no-new-privileges --cap-drop ALL)

wal_disk() {
    local root=$1 device
    device=$(lsblk -dnpo NAME,MODEL | awk '/Instance Storage/ {print $1; exit}')
    sudo -n mkdir -p "$root"
    if [[ -n "$device" ]]; then
        sudo -n umount "$device" 2>/dev/null || true
        sudo -n mkfs.ext4 -q -F "$device"
        sudo -n mount -o noatime "$device" "$root"
    fi
    sudo -n chown "$(id -u):$(id -g)" "$root"
    findmnt -no SOURCE,FSTYPE -T "$root"
}

generate() {
    local args
    mapfile -t args < <(jq -r '.generator_args[]' "$PLAN")
    mkdir -p "$CORPUS_DIR"
    "${runtime[@]}" --name "$OWNER-generator" \
        --mount "type=bind,src=$CORPUS_DIR,dst=/work/corpus" \
        "$RUNTIME_IMAGE" scripts/generate_agent_observations.py "${args[@]}" \
        --output /work/corpus/agent_observations.jsonl \
        --summary-output /work/corpus/summary.json --evidence-output /work/corpus/evidence.json \
        > "$RESULTS/generator.log" 2>&1
    jq -e --argjson rows "$(jq .rows "$PLAN")" '.line_count == $rows' "$CORPUS_DIR/summary.json" >/dev/null
}

# One cell in its own process, so errexit holds inside it.
cell() {
    : "${GREPTIMEDB_IMAGE:?}" "${RUN_PREFIX:?}" "${WAL_ROOT:?}" "${S3_BUCKET:?}" "${S3_REGION:?}"
    : "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
    # Globals, not locals: the EXIT trap runs after this function returns.
    spec=$1 cid= wal_dir= version= load_seconds=
    local concurrency config name
    id=$(jq -r .id <<< "$spec"); concurrency=$(jq -r .concurrency <<< "$spec"); config=$(jq -r .config <<< "$spec")
    dir="$RESULTS/cells/$id"; name="$OWNER-$id"
    mkdir -p "$dir"
    # The prefix holds data/ and the WAL's default prefix wal/ side by side.
    local settings=(STORAGE__TYPE=S3 "STORAGE__BUCKET=$S3_BUCKET" "STORAGE__REGION=$S3_REGION"
        "STORAGE__ROOT=$RUN_PREFIX/$id")
    [[ -z "${S3_CONTAINER_ENDPOINT:-}" ]] || settings+=("STORAGE__ENDPOINT=$S3_CONTAINER_ENDPOINT")
    mapfile -t -O "${#settings[@]}" settings < <(jq -r '.wal | to_entries[] | "WAL__\(.key)=\(.value)"' <<< "$spec")
    local docker_env=() mounts=() setting
    for setting in "${settings[@]}"; do
        docker_env+=(-e "GREPTIMEDB_STANDALONE__$setting")
        printf 'GREPTIMEDB_STANDALONE__%s\n' "$setting" >> "$dir/config.env"
    done
    # Pass the two keys by name: their values never reach argv, a file or the artifact.
    export GREPTIMEDB_STANDALONE__STORAGE__ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
    export GREPTIMEDB_STANDALONE__STORAGE__SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
    docker_env+=(-e GREPTIMEDB_STANDALONE__STORAGE__ACCESS_KEY_ID -e GREPTIMEDB_STANDALONE__STORAGE__SECRET_ACCESS_KEY)
    printf 'GREPTIMEDB_STANDALONE__STORAGE__%s=<redacted>\n' ACCESS_KEY_ID SECRET_ACCESS_KEY >> "$dir/config.env"
    if jq -e '.wal.DIR' <<< "$spec" >/dev/null; then
        wal_dir="$WAL_ROOT/$id"; mkdir -p "$wal_dir"
        mounts=(--mount "type=bind,src=$wal_dir,dst=$(jq -r .wal.DIR <<< "$spec")")
        wal_device=$(findmnt -no SOURCE -T "$wal_dir")
    else
        wal_device="s3://$S3_BUCKET/$RUN_PREFIX/$id/wal"
    fi
    finish() {
        local rc=$?
        trap - EXIT
        set +e
        if [[ -n "$cid" ]]; then
            docker stop -t 120 "$cid" >/dev/null
            docker logs "$cid" > "$dir/server.log" 2>&1
            docker rm -fv "$cid" >/dev/null
        fi
        [[ -z "$wal_dir" ]] || sudo -n rm -rf "$wal_dir"
        jq -n --argjson spec "$spec" --arg device "$wal_device" --arg version "$version" \
            --arg load_seconds "$load_seconds" --argjson exit_code "$rc" \
            '$spec + {wal_device: $device, version: $version, exit_code: $exit_code,
              load_wall_seconds: ($load_seconds | tonumber? // null)}' > "$dir/cell.json"
        exit "$rc"
    }
    trap finish EXIT
    cid=$(docker run -d --init --name "$name" --label "o11ybench.owner=$OWNER" \
        --add-host host.docker.internal:host-gateway \
        -p 127.0.0.1:4000:4000 -p 127.0.0.1:4001:4001 "${docker_env[@]}" "${mounts[@]}" \
        "$GREPTIMEDB_IMAGE" standalone start --http-addr=0.0.0.0:4000 --grpc-bind-addr=0.0.0.0:4001)
    local deadline=$((SECONDS + 300))
    until curl --noproxy '*' -fsS --max-time 5 http://127.0.0.1:4000/health >/dev/null 2>&1; do
        [[ $(docker inspect --format '{{.State.Running}}' "$cid") == true ]] || { echo "$id: server exited" >&2; exit 1; }
        ((SECONDS < deadline)) || { echo "$id: server not healthy in 300s" >&2; exit 1; }
        sleep 2
    done
    version=$(docker exec "$cid" greptime --version | sed -n 's/^version: //p')
    [[ -n "$version" ]]
    local started ended
    started=$(date +%s.%N)
    # Only the load phase, run exactly as run-agent-observability.sh runs it.
    "${runtime[@]}" --name "$name-load" --network host \
        --mount "type=bind,src=$CORPUS_DIR,dst=/work/corpus,readonly" --mount "type=bind,src=$dir,dst=/results" \
        -e GRPC_ENDPOINT=127.0.0.1:4001 -e "DATA_PROFILE=$(jq -r .data_profile "$PLAN")" -e RESULT_NAME=greptimedb-1 \
        -e "ENDPOINT=http://127.0.0.1:4000/v1/sql?db=public&format=greptimedb_v1" \
        -e "TARGET_IMAGE=$GREPTIMEDB_IMAGE" -e "TARGET_VERSION=$version" -e "TARGET_DIGEST=${GREPTIMEDB_IMAGE##*@}" \
        -e "TARGET_ID=$name" -e TARGET_HOST=127.0.0.1 -e PREPARE_WORKERS= -e "MAX_IN_FLIGHT=$concurrency" \
        -e "GREPTIMEDB_BATCH_SIZE=$(jq -r .batch_rows "$PLAN")" --entrypoint /bin/bash \
        "$RUNTIME_IMAGE" scripts/agent-observability/greptimedb.sh load > "$dir/load.log" 2>&1
    ended=$(date +%s.%N)
    load_seconds=$(awk -v a="$started" -v b="$ended" 'BEGIN {printf "%.3f", b - a}')
    jq -e --argjson rows "$(jq .rows "$PLAN")" '.count_before == 0 and .count_after == $rows' \
        "$dir/greptimedb-1/load-result.json" >/dev/null
    curl --noproxy '*' -fsS --max-time 30 http://127.0.0.1:4000/metrics > "$dir/metrics.all"
    grep -E '^(# (HELP|TYPE) )?greptime_logstore_object_store_wal_' "$dir/metrics.all" > "$dir/metrics.prom" || true
    rm "$dir/metrics.all"
}

case "${1:-}" in
    wal-disk) wal_disk "${2:?}" ;;
    generate) generate ;;
    cell) cell "${2:?}" ;;
    cells)
        mapfile -t specs < <(jq -c '.cells[]' "$PLAN")
        failed=0
        for spec in "${specs[@]}"; do
            echo "::group::cell $(jq -r .id <<< "$spec")"
            bash "${BASH_SOURCE[0]}" cell "$spec" || { echo "::error::cell $(jq -r .id <<< "$spec") failed"; failed=1; }
            echo '::endgroup::'
        done
        exit "$failed" ;;
    *) echo "usage: $0 wal-disk DIR | generate | cells" >&2; exit 2 ;;
esac
