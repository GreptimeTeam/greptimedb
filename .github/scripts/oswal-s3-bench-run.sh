#!/usr/bin/env bash
# Host side of oswal-s3-bench.yml: one subcommand per step; each cell of $PLAN gets a fresh GreptimeDB.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Same isolation as the o11ybench runtime's own docker run.
runtime() {
    docker run --rm --user "$(id -u):$(id -g)" --label "o11ybench.owner=$OWNER" \
        --security-opt no-new-privileges --cap-drop ALL "$@"
}

# Pin the runtime image to a digest without pulling it: ECR Public's anonymous data limit can refuse a
# hosted runner even a manifest read. A reference that names its digest is pinned as given; only a tag
# is resolved from the registry's manifest. The benchmark host pulls the image and checks the contents.
check_runtime() {
    local image=$1 repository digest attempt
    [[ "$image" == public.ecr.aws/* ]] || { echo 'The runtime image must come from AWS ECR Public' >&2; return 1; }
    if [[ "$image" == *@* ]]; then
        digest=${image##*@}
    else
        for attempt in 1 2 3; do
            digest=$(docker buildx imagetools inspect --format '{{json .Manifest}}' "$image" | jq -er .digest) && break
            ((attempt < 3)) || { echo "Cannot read the manifest of $image" >&2; return 1; }
            sleep $((attempt * 10))
        done
    fi
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "$image has no sha256 digest" >&2; return 1; }
    repository=${image%@*}
    [[ "${repository##*/}" != *:* ]] || repository=${repository%:*}
    printf 'runtime_image=%s@%s\n' "$repository" "$digest" >> "${GITHUB_OUTPUT:-/dev/stdout}"
}

# Pull every GreptimeDB image of the run, pin it to its digest and record the version and
# commit it reports, before any instance is allocated.
check_images() {
    local entry image resolved info version commit checked=()
    while IFS= read -r entry; do
        image=$(jq -r .image <<< "$entry")
        bash "$here/pull-benchmark-image.sh" "$image"
        resolved=$(docker image inspect "$image" | jq -er '[.[0].RepoDigests[]? | select(test("@sha256:[0-9a-f]{64}$"))][0]') \
            || { echo "$image has no sha256 repository digest" >&2; return 1; }
        info=$(docker run --rm --entrypoint greptime "$resolved" --version)
        version=$(sed -n 's/^version: //p' <<< "$info") commit=$(sed -n 's/^commit: //p' <<< "$info")
        [[ -n "$version" && -n "$commit" ]] || { echo "$image reports no version or commit" >&2; return 1; }
        echo "${image}: $resolved, version $version, commit $commit"
        checked+=("$(jq -c --arg image "$resolved" --arg version "$version" --arg commit "$commit" \
            '{label, image: $image, requested: .image, version: $version, commit: $commit}' <<< "$entry")")
    done < <(jq -c '.[]' <<< "$1")
    ((${#checked[@]})) || { echo 'No GreptimeDB image to check' >&2; return 1; }
    printf 'greptimedb_images=%s\n' "$(jq -sc . <<< "${checked[*]}")" >> "${GITHUB_OUTPUT:-/dev/stdout}"
}

pull_images() {
    : "${RESULTS:?}" "${RUNTIME_IMAGE:?}" "${GREPTIMEDB_IMAGES:?}"
    local image images=()
    mkdir -p "$RESULTS/images"
    bash "$here/pull-benchmark-image.sh" "$RUNTIME_IMAGE"
    docker run --rm --entrypoint sh "$RUNTIME_IMAGE" -c \
        'test -f scripts/agent-observability/greptimedb.sh && test -f scripts/generate_agent_observations.py' \
        || { echo 'The runtime image lacks the agent observability loader or generator' >&2; return 1; }
    while IFS= read -r image; do
        bash "$here/pull-benchmark-image.sh" "$image"
        images+=("$image")
    done < <(jq -r '.[].image' <<< "$GREPTIMEDB_IMAGES")
    docker image inspect "$RUNTIME_IMAGE" > "$RESULTS/images/runtime.json"
    docker image inspect "${images[@]}" > "$RESULTS/images/greptimedb.json"
}

wal_disk() {
    : "${RESULTS:?}"
    local root=$1 device
    device=$(lsblk -dnpo NAME,MODEL | awk '/Instance Storage/ {print $1; exit}')
    # Only a smoke run may keep the Raft Engine WAL on the root volume.
    [[ -n "$device" || "${SMOKE:-}" == true ]] || { echo 'No local NVMe instance store; choose an instance type with one' >&2; return 1; }
    sudo -n mkdir -p "$root"
    if [[ -n "$device" ]]; then
        sudo -n umount "$device" 2>/dev/null || true
        sudo -n mkfs.ext4 -q -F "$device"
        sudo -n mount -o noatime "$device" "$root"
    fi
    sudo -n chown "$(id -u):$(id -g)" "$root"
    findmnt -no SOURCE,FSTYPE -T "$root" | tee "$RESULTS/wal-disk.txt"
}

# One corpus per row count the load cells use; crash cells write their own rows.
generate() {
    : "${PLAN:?}" "${RESULTS:?}" "${CORPUS_DIR:?}" "${OWNER:?}" "${RUNTIME_IMAGE:?}"
    local corpus args arg dir rows
    while IFS= read -r corpus; do
        args=() dir="$CORPUS_DIR/$(jq -r .dir <<< "$corpus")" rows=$(jq .rows <<< "$corpus")
        while IFS= read -r arg; do args+=("$arg"); done < <(jq -r '.generator_args[]' <<< "$corpus")
        mkdir -p "$dir"
        runtime --name "$OWNER-generator-$rows" --mount "type=bind,src=$dir,dst=/work/corpus" \
            "$RUNTIME_IMAGE" scripts/generate_agent_observations.py "${args[@]}" \
            --output /work/corpus/agent_observations.jsonl \
            --summary-output /work/corpus/summary.json --evidence-output /work/corpus/evidence.json \
            >> "$RESULTS/generator.log" 2>&1
        jq -e --argjson rows "$rows" '.line_count == $rows' "$dir/summary.json" >/dev/null
    done < <(jq -c '.corpora[]' "$PLAN")
}

# Scrape once per server, after the load succeeded or failed, while it still runs:
# the loaded server into metrics.prom, a restarted one into metrics-restart.prom.
scrape_metrics() {
    curl --noproxy '*' -fsS --max-time 30 http://127.0.0.1:4000/metrics > "$dir/metrics.all" || { rm -f "$dir/metrics.all"; return 1; }
    grep -E '^(# (HELP|TYPE) )?(greptime_logstore_object_store_wal_|opendal_operation_(duration_seconds_count|errors_total))' \
        "$dir/metrics.all" > "$dir/metrics$phase.prom" || true
    rm -f "$dir/metrics.all"
    case "$phase" in "") metrics_scraped=true ;; -restart) restart_scraped=true ;; esac
}

# A crash cycle cannot repeat a scrape once its server is killed, so it retries.
# The named WAL sample must be in the scrape, or it does not count.
scrape_retry() {
    local required=$1 attempt
    for attempt in 1 2 3; do
        scrape_metrics && grep -q "^greptime_logstore_object_store_wal_$required " "$dir/metrics$phase.prom" && return 0
        sleep 1
    done
    return 1
}

sql() { curl --noproxy '*' -fsS --max-time 30 --data-urlencode "sql=$1" 'http://127.0.0.1:4000/v1/sql?db=public'; }
now_ns() { python3 -c 'import time; print(time.time_ns())'; }

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$(now)" 'BEGIN {printf "%.3f", b - a}'; }
# CPU seconds the server's cgroup used, and busy and total jiffies of the host.
server_cpu() { docker exec "$cid" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/^usage_usec / {printf "%.3f", $2 / 1e6}' || true; }
host_cpu() { awk '/^cpu / {print $2 + $3 + $4 + $7 + $8 + $9, $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9}' /proc/stat 2>/dev/null || true; }
# The host's 1, 5 and 15 minute load averages as a JSON list.
host_load() { awk '{printf "[%s, %s, %s]", $1, $2, $3}' "${OSWAL_LOADAVG:-/proc/loadavg}" 2>/dev/null || true; }

# A server's log is named after the phase it started in, whatever phase stops it.
start_server() {
    server_phase=$phase
    cid=$(docker run -d --init --name "$name$phase" --label "o11ybench.owner=$OWNER" \
        --add-host host.docker.internal:host-gateway \
        -p 127.0.0.1:4000:4000 -p 127.0.0.1:4001:4001 "${docker_env[@]}" ${mounts[@]+"${mounts[@]}"} \
        "$GREPTIMEDB_IMAGE" standalone start --http-addr=0.0.0.0:4000 --grpc-bind-addr=0.0.0.0:4001)
    local deadline=$((SECONDS + 300))
    until curl --noproxy '*' -fsS --max-time 5 http://127.0.0.1:4000/health >/dev/null 2>&1; do
        [[ $(docker inspect --format '{{.State.Running}}' "$cid") == true ]] || { echo "$id: server$phase exited" >&2; exit 1; }
        ((SECONDS < deadline)) || { echo "$id: server$phase not healthy in 300s" >&2; exit 1; }
        sleep 0.2
    done
}

# kill sends SIGKILL, killed collects a server the caller already killed, and
# no argument stops it gracefully.
stop_server() {
    case "${1:-}" in
        kill) docker kill --signal KILL "$cid" >/dev/null ;;
        killed) docker wait "$cid" >/dev/null ;;
        *) docker stop -t 120 "$cid" >/dev/null ;;
    esac
    docker logs "$cid" > "$dir/server$server_phase.log" 2>&1
    docker rm -fv "$cid" >/dev/null
    cid=
}

# Kill the loaded server, start the same configuration on the same prefix and
# data home, and time it until /health and until the loaded table answers.
restart() {
    local table count started deadline
    table=$(jq -er .table "$dir/greptimedb-1/load-result.json")
    scrape_metrics || { echo "$id: metrics scrape failed" >&2; exit 4; }
    # The objects recovery has to read are those still indexed at the kill.
    indexed_before_kill=$(awk '/^greptime_logstore_object_store_wal_indexed_objects / {print $2}' "$dir/metrics.prom")
    stop_server kill
    phase=-restart started=$(now)
    start_server
    restart_seconds=$(elapsed "$started")
    deadline=$((SECONDS + 300))
    # Regions may still be opening after /health.
    until count=$(curl --noproxy '*' -fsS --max-time 30 --data-urlencode "sql=SELECT count(*) FROM $table" \
        'http://127.0.0.1:4000/v1/sql?db=public' | jq -er '.output[0].records.rows[0][0]'); do
        ((SECONDS < deadline)) || { echo "$id: $table not readable in 300s after restart" >&2; exit 5; }
        sleep 0.2
    done
    restart_query_seconds=$(elapsed "$started") restart_count=$count
    [[ "$count" == "$(jq .rows <<< "$spec")" ]] || { echo "$id: $count rows after restart" >&2; exit 5; }
    # The restart was measured, but on fewer objects than the cell asked for.
    awk -v n="${indexed_before_kill:-0}" -v min="$(jq -r '.min_objects // 0' <<< "$spec")" 'BEGIN {exit !(n >= min)}' \
        || { echo "$id: ${indexed_before_kill:-no} objects indexed at the kill, fewer than min_objects" >&2; exit 7; }
}

# Pins the WAL with one row in a second table before the writers start, if asked,
# and samples the object counts every 30 s while they run.
sample_objects() {
    set +e
    local body
    while sleep "${OSWAL_SAMPLE_SECONDS:-30}"; do
        # A failed scrape is a missing sample, never zero objects.
        if ! body=$(curl --noproxy '*' -fsS --max-time 10 http://127.0.0.1:4000/metrics 2>/dev/null) \
            || ! grep -q '^greptime_logstore_object_store_wal_indexed_objects ' <<< "$body"; then
            echo "$(date +%s) missing" >> "$dir/objects.tsv"
            continue
        fi
        awk -v t="$(date +%s)" '
            /^greptime_logstore_object_store_wal_indexed_objects / {i = $2}
            /^greptime_logstore_object_store_wal_deleted_objects_total / {d = $2}
            /^greptime_logstore_object_store_wal_created_objects_total / {c = $2}
            END {print t, i + 0, d + 0, c + 0}' <<< "$body" >> "$dir/objects.tsv"
    done
}

# Writers insert striped batches until a SIGKILL at a random point stops the
# server; it restarts on the same prefix and data home and the table is checked
# against what was acknowledged. Nothing is cleaned between cycles.
crash_cycles() {
    local writers rows cycles kill_min kill_max cycle cdir wpid spid= delay before after started restart_s query_s next=0 pin_count pin_args
    local load_start complete pin_left pin_base deleted sql_url=${OSWAL_SQL_URL:-http://127.0.0.1:4000/v1/sql?db=public}
    writers=$(jq -r .concurrency <<< "$spec") rows=$(jq -r .batch_rows "$PLAN") cycles=$(jq -r .cycles <<< "$spec")
    kill_min=$(jq -r '.kill_after[0]' <<< "$spec") kill_max=$(jq -r '.kill_after[1]' <<< "$spec")
    if jq -e .pin <<< "$spec" >/dev/null; then
        # Only the objects indexed before the pin row lie below it, such as the empty start object that
        # the row's write lets the store collect at any later point; the servers may collect no more.
        phase=-pin; scrape_retry indexed_objects || { echo "$id: metrics scrape failed" >&2; exit 4; }
        read -r pin_left pin_base < <(awk '/^greptime_logstore_object_store_wal_indexed_objects / {i = $2}
            /^greptime_logstore_object_store_wal_deleted_objects_total / {d = $2} END {printf "%d %d\n", i, d}' "$dir/metrics-pin.prom")
        # The pin table's own auto flush outlasts every cycle a crash group allows, so its row holds the WAL.
        sql "CREATE TABLE pin (v BIGINT, ts TIMESTAMP TIME INDEX) WITH ('auto_flush_interval' = '1d')" >/dev/null
        sql "INSERT INTO pin VALUES (1, 0)" >/dev/null
    fi
    sql "CREATE TABLE crash (seq BIGINT, ts TIMESTAMP TIME INDEX) WITH (append_mode = 'true')" >/dev/null
    for ((cycle = 1; cycle <= cycles; cycle++)); do
        cdir="$dir/cycle-$cycle" complete=true; mkdir -p "$cdir"
        # The counters of this server process before its writers start are the cycle's baseline.
        phase=-start-$cycle; scrape_retry indexed_objects || complete=false
        load_start=$(now_ns)
        python3 "$here/oswal-s3-bench-crash.py" write --dir "$cdir" --url "$sql_url" \
            --writers "$writers" --first-seq "$next" --rows "$rows" &
        wpid=$!
        if jq -e .pin <<< "$spec" >/dev/null; then sample_objects & spid=$!; fi
        delay=$(awk -v a="$kill_min" -v b="$kill_max" 'BEGIN {srand(); printf "%.3f", a + rand() * (b - a)}')
        sleep "$delay"
        phase=-cycle-$cycle; scrape_retry indexed_objects || complete=false
        before=$(now_ns); docker kill --signal KILL "$cid" >/dev/null; after=$(now_ns)
        wait "$wpid"
        if [[ -n "$spid" ]]; then kill "$spid" 2>/dev/null || true; wait "$spid" 2>/dev/null || true; spid=; fi
        stop_server killed
        phase=-restart-$cycle started=$(now)
        start_server
        restart_s=$(elapsed "$started")
        until sql "SELECT seq, count(*) FROM crash GROUP BY seq" > "$cdir/found.json" 2>/dev/null; do
            (($(elapsed "$started" | cut -d. -f1) < 300)) || { echo "$id: crash not readable after restart $cycle" >&2; exit 5; }
            sleep 0.2
        done
        query_s=$(elapsed "$started")
        scrape_retry recovery_seconds_count || complete=false
        pin_count=null pin_args=()
        if jq -e .pin <<< "$spec" >/dev/null; then
            pin_count=$(sql "SELECT count(*) FROM pin" | jq -er '.output[0].records.rows[0][0]') pin_args=(--pin-rows "$pin_count")
        fi
        local verdict=0
        python3 "$here/oswal-s3-bench-crash.py" check --run-dir "$dir" --cycle "$cycle" --found "$cdir/found.json" \
            --mode "$(jq -r .wal.ACK_MODE <<< "$spec")" --writers "$writers" --rows "$rows" \
            --before-ns "$before" --after-ns "$after" --window-max-ms 2000 ${pin_args[@]+"${pin_args[@]}"} \
            > "$cdir/check.json" || verdict=$?
        jq -c --arg delay "$delay" --arg restart "$restart_s" --arg query "$query_s" --argjson pin "$pin_count" \
            --argjson load "$(awk -v a="$load_start" -v b="$before" 'BEGIN {printf "%.3f", (b - a) / 1e9}')" \
            --argjson complete "$complete" \
            '. + {kill_after_seconds: ($delay | tonumber), load_seconds: $load, restart_seconds: ($restart | tonumber),
                  restart_query_seconds: ($query | tonumber), pin_rows: $pin, metrics_complete: $complete}' \
            "$cdir/check.json" >> "$dir/cycles.jsonl"
        ((verdict == 0)) || { echo "$id: cycle $cycle failed its check" >&2; exit 6; }
        # A cycle without its scrapes is not a measurement, whatever the table held.
        [[ "$complete" == true ]] || { echo "$id: cycle $cycle lost a metrics scrape" >&2; exit 4; }
        # Each server counts its collections from its own start: the first one from its pin scrape,
        # every restarted one from zero, and an absent counter has not moved.
        if jq -e .pin <<< "$spec" >/dev/null; then
            deleted=$(awk '/^greptime_logstore_object_store_wal_deleted_objects_total / {d = $2} END {printf "%d", d}' \
                "$dir/metrics-cycle-$cycle.prom")
            pin_left=$((pin_left - (deleted - pin_base))) pin_base=0
            ((pin_left >= 0)) || { echo "$id: cycle $cycle collected WAL objects the pin held" >&2; exit 11; }
        fi
        next=$(cat "$cdir"/attempted-*.log | sort -n | tail -1); next=$((next + 1))
    done
}

# Writes the table `dropped` for write_seconds, drops it and kills the server right
# away, while most of its objects are still indexed; restarts on the same prefix and
# data home, then writes and flushes the table `kept` and samples the indexed objects
# until the dropped table's objects are collected.
drop_restart() {
    local writers rows seconds started deadline indexed recovered sql_url=${OSWAL_SQL_URL:-http://127.0.0.1:4000/v1/sql?db=public}
    writers=$(jq -r .concurrency <<< "$spec") rows=$(jq -r .batch_rows "$PLAN") seconds=$(jq -r .write_seconds <<< "$spec")
    sql "CREATE TABLE dropped (seq BIGINT, ts TIMESTAMP TIME INDEX) WITH (append_mode = 'true')" >/dev/null
    sql "CREATE TABLE kept (seq BIGINT, ts TIMESTAMP TIME INDEX) WITH (append_mode = 'true')" >/dev/null
    python3 "$here/oswal-s3-bench-crash.py" write --dir "$dir/dropped" --url "$sql_url" --table dropped \
        --writers "$writers" --first-seq 0 --rows "$rows" --seconds "$seconds" \
        || { echo "$id: the writers of dropped stopped before $seconds s" >&2; exit 10; }
    phase=-written; scrape_retry indexed_objects || { echo "$id: metrics scrape failed" >&2; exit 4; }
    sql "DROP TABLE dropped" >/dev/null
    stop_server kill
    phase=-restart started=$(now)
    start_server
    restart_seconds=$(elapsed "$started")
    scrape_retry recovery_seconds_count || { echo "$id: metrics scrape failed" >&2; exit 4; }
    recovered=$(awk '/^greptime_logstore_object_store_wal_recovered_objects_total / {print $2}' "$dir/metrics-restart.prom")
    python3 "$here/oswal-s3-bench-crash.py" write --dir "$dir/kept" --url "$sql_url" --table kept \
        --writers 1 --first-seq 0 --rows "$rows" --seconds 2 \
        || { echo "$id: the writer of kept stopped before 2 s" >&2; exit 10; }
    sql "ADMIN FLUSH_TABLE('kept')" >/dev/null
    # The flushed watermark becomes durable with the next object, so every sample writes one row.
    deadline=$((SECONDS + ${OSWAL_COLLECT_SECONDS:-120}))
    while :; do
        sql "INSERT INTO kept VALUES (-1, $SECONDS)" >/dev/null
        # The samples have their own file; the final scrape keeps the restart's name.
        phase=-collected
        scrape_retry indexed_objects || { phase=-restart; echo "$id: metrics scrape failed" >&2; exit 4; }
        phase=-restart
        indexed=$(awk '/^greptime_logstore_object_store_wal_indexed_objects / {print $2}' "$dir/metrics-collected.prom")
        printf '%s %s\n' "$(date +%s)" "$indexed" >> "$dir/collected.tsv"
        # A start object, the newest object and kept's own may stay; the dropped table's go.
        awk -v i="$indexed" -v r="${recovered:-0}" 'BEGIN {exit !(i <= 3 || 2 * i < r)}' && break
        ((SECONDS < deadline)) || { echo "$id: $indexed of ${recovered:-?} recovered objects still indexed" >&2; exit 9; }
        sleep 1
    done
}

finish() {
    local rc=$?
    trap - EXIT
    set +e
    if [[ -n "$cid" ]]; then
        # A load that succeeded without its metrics is not a measurement.
        scrape_metrics || { echo "$id: metrics scrape failed" >&2; [[ $rc != 0 ]] || rc=4; }
        stop_server
    fi
    [[ -z "$wal_dir" ]] || sudo -n rm -rf "$wal_dir"
    [[ -z "$home_dir" ]] || sudo -n rm -rf "$home_dir"
    jq -n --argjson spec "$spec" --arg device "$wal_device" --arg version "$version" \
        --arg host_load_start "$host_load_start" --arg host_load_end "$(host_load)" \
        --arg load_seconds "$load_seconds" --argjson exit_code "$rc" --argjson scraped "$metrics_scraped" \
        --argjson restart_scraped "$restart_scraped" --arg restart_seconds "$restart_seconds" \
        --arg restart_query_seconds "$restart_query_seconds" --arg restart_count "$restart_count" \
        --arg indexed_before_kill "$indexed_before_kill" \
        --arg server_cpu_seconds "$server_cpu_seconds" --arg host_cpu_busy "$host_cpu_busy" --arg host_cpus "$(nproc 2>/dev/null)" \
        '$spec + {wal_device: $device, version: $version, exit_code: $exit_code, metrics_scraped: $scraped,
          load_wall_seconds: ($load_seconds | tonumber? // null),
          server_cpu_seconds: ($server_cpu_seconds | tonumber? // null),
          host_cpu_busy: ($host_cpu_busy | tonumber? // null), host_cpus: ($host_cpus | tonumber? // null),
          restart_scraped: $restart_scraped, restart_seconds: ($restart_seconds | tonumber? // null),
          restart_query_seconds: ($restart_query_seconds | tonumber? // null),
          restart_count: ($restart_count | tonumber? // null),
          indexed_before_kill: ($indexed_before_kill | tonumber? // null),
          host_load_start: ($host_load_start | fromjson? // null), host_load_end: ($host_load_end | fromjson? // null)}' \
        > "$dir/cell.json"
    exit "$rc"
}

# One cell in its own process, so errexit holds inside it. The cell's state is
# global because the EXIT trap runs after this function returns.
cell() {
    : "${PLAN:?}" "${RESULTS:?}" "${CORPUS_DIR:?}" "${OWNER:?}" "${RUNTIME_IMAGE:?}" "${GREPTIMEDB_IMAGES:?}"
    : "${RUN_PREFIX:?}" "${WAL_ROOT:?}" "${S3_BUCKET:?}" "${S3_REGION:?}" "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
    spec=$1 cid= wal_dir= home_dir= version= load_seconds= metrics_scraped=false id= phase= server_phase=
    restart_scraped=false restart_seconds= restart_query_seconds= restart_count= server_cpu_seconds= host_cpu_busy=
    indexed_before_kill= host_load_start=$(host_load)
    local concurrency setting started cpu_before host_before host_after corpus partitions load script label
    id=$(jq -r .id <<< "$spec"); concurrency=$(jq -r .concurrency <<< "$spec") label=$(jq -r .image <<< "$spec")
    dir="$RESULTS/cells/$id" name="$OWNER-$id"
    # The digest the image of the cell's label was pinned to before the run.
    GREPTIMEDB_IMAGE=$(jq -er --arg label "$label" '.[] | select(.label == $label) | .image' <<< "$GREPTIMEDB_IMAGES") \
        || { echo "$id: no image labelled '$label'" >&2; exit 1; }
    mkdir -p "$dir"
    # The root holds data/ and the WAL's default prefix wal/ side by side.
    local settings=(STORAGE__TYPE=S3 "STORAGE__BUCKET=$S3_BUCKET" "STORAGE__REGION=$S3_REGION"
        "STORAGE__ROOT=$RUN_PREFIX/$id")
    [[ -z "${S3_CONTAINER_ENDPOINT:-}" ]] || settings+=("STORAGE__ENDPOINT=$S3_CONTAINER_ENDPOINT")
    # A restarted server needs the metadata the loaded one kept in its data home.
    jq -e '.restart or .mode != "load"' <<< "$spec" >/dev/null && settings+=(STORAGE__DATA_HOME=/oswal_home)
    while IFS= read -r setting; do settings+=("$setting"); done \
        < <(jq -r '.wal | to_entries[] | "WAL__\(.key)=\(.value)"' <<< "$spec")
    docker_env=() mounts=()
    for setting in "${settings[@]}"; do
        docker_env+=(-e "GREPTIMEDB_STANDALONE__$setting")
        printf 'GREPTIMEDB_STANDALONE__%s\n' "$setting" >> "$dir/config.env"
    done
    while IFS= read -r setting; do
        docker_env+=(-e "$setting")
        printf '%s\n' "$setting" >> "$dir/config.env"
    done < <(jq -r '.env // {} | to_entries[] | "\(.key)=\(.value)"' <<< "$spec")
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
    if jq -e '.restart or .mode != "load"' <<< "$spec" >/dev/null; then
        home_dir="$WAL_ROOT/$id-home"; mkdir -p "$home_dir"
        mounts+=(--mount "type=bind,src=$home_dir,dst=/oswal_home")
    fi
    trap finish EXIT
    start_server
    version=$(docker exec "$cid" greptime --version | sed -n 's/^version: //p')
    [[ -n "$version" ]]
    if jq -e '.mode == "crash"' <<< "$spec" >/dev/null; then crash_cycles; return; fi
    if jq -e '.mode == "drop"' <<< "$spec" >/dev/null; then drop_restart; return; fi
    corpus="$CORPUS_DIR/corpus-$(jq -r .rows <<< "$spec")" partitions=$(jq -r '.partitions // 1' <<< "$spec")
    # Only the load phase, run exactly as run-agent-observability.sh runs it, or with the
    # loader writing into a table split into regions that the cell created first.
    load=(scripts/agent-observability/greptimedb.sh load) script=()
    if ((partitions > 1)); then
        sql "$(python3 "$here/oswal-s3-bench.py" table --summary "$corpus/summary.json" --partitions "$partitions")" >/dev/null
        load=(/work/oswal-s3-bench-load.sh)
        script=(--mount "type=bind,src=$here/oswal-s3-bench-load.sh,dst=/work/oswal-s3-bench-load.sh,readonly")
    fi
    cpu_before=$(server_cpu) host_before=$(host_cpu)
    started=$(now)
    runtime --name "$name-load" --network host \
        --mount "type=bind,src=$corpus,dst=/work/corpus,readonly" ${script[@]+"${script[@]}"} \
        --mount "type=bind,src=$dir,dst=/results" \
        -e GRPC_ENDPOINT=127.0.0.1:4001 -e "DATA_PROFILE=$(jq -r .data_profile <<< "$spec")" -e RESULT_NAME=greptimedb-1 \
        -e "ENDPOINT=http://127.0.0.1:4000/v1/sql?db=public&format=greptimedb_v1" \
        -e "TARGET_IMAGE=$GREPTIMEDB_IMAGE" -e "TARGET_VERSION=$version" -e "TARGET_DIGEST=${GREPTIMEDB_IMAGE##*@}" \
        -e "TARGET_ID=$name" -e TARGET_HOST=127.0.0.1 -e PREPARE_WORKERS= -e "MAX_IN_FLIGHT=$concurrency" \
        -e "GREPTIMEDB_BATCH_SIZE=$(jq -r .batch_rows "$PLAN")" --entrypoint /bin/bash \
        "$RUNTIME_IMAGE" "${load[@]}" > "$dir/load.log" 2>&1
    load_seconds=$(elapsed "$started")
    host_after=$(host_cpu)
    server_cpu_seconds=$(awk -v a="$cpu_before" -v b="$(server_cpu)" 'BEGIN {if (a != "" && b != "") printf "%.3f", b - a}')
    # Busy share of all host CPUs during the load, the loader included.
    host_cpu_busy=$(awk -v a="$host_before" -v b="$host_after" \
        'BEGIN {split(a, x, " "); split(b, y, " "); if (y[2] > x[2]) printf "%.4f", (y[1] - x[1]) / (y[2] - x[2])}')
    jq -e --argjson rows "$(jq .rows <<< "$spec")" '.count_before == 0 and .count_after == $rows' \
        "$dir/greptimedb-1/load-result.json" >/dev/null
    if ((partitions > 1)); then
        # The server's own account of the rows each region took.
        sql "SELECT p.partition_name, s.region_id, s.region_rows, s.written_bytes_since_open
             FROM information_schema.partitions p JOIN information_schema.region_statistics s
             ON p.greptime_partition_id = s.region_id
             WHERE p.table_schema = 'public' AND p.table_name = 'agent_observations'
             ORDER BY s.region_id" > "$dir/regions.json"
        jq -e --argjson n "$partitions" '[.output[0].records.rows[] | select(.[2] > 0)] | length == $n' \
            "$dir/regions.json" >/dev/null || { echo "$id: fewer than $partitions regions took rows" >&2; exit 8; }
    fi
    # An idle window: what the server sends with no writes, between two scrapes.
    if jq -e '.idle_seconds > 0' <<< "$spec" >/dev/null; then
        phase=-idle; scrape_metrics || { echo "$id: metrics scrape failed" >&2; exit 4; }; phase=
        sleep "$(jq -r .idle_seconds <<< "$spec")"
    fi
    if jq -e '.restart' <<< "$spec" >/dev/null; then restart; fi
}

scrub() {
    : "${RESULTS:?}"
    [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" && -d "$RESULTS" ]] || return 0
    if grep -rqF -e "$AWS_ACCESS_KEY_ID" -e "$AWS_SECRET_ACCESS_KEY" "$RESULTS"; then
        echo '::error::A credential value appears in the results; removing them instead of uploading' >&2
        rm -rf "$RESULTS"
        return 1
    fi
}

case "${1:-}" in
    check-runtime) check_runtime "${2:?}" ;;
    check-images) check_images "${2:?}" ;;
    pull) pull_images ;;
    wal-disk) wal_disk "${2:?}" ;;
    generate) generate ;;
    cell) cell "${2:?}" ;;
    cells)
        : "${PLAN:?}"
        failed=0
        while IFS= read -r spec; do
            id=$(jq -r .id <<< "$spec")
            echo "::group::cell $id"
            bash "${BASH_SOURCE[0]}" cell "$spec" < /dev/null || { echo "::error::cell $id failed"; failed=1; }
            echo '::endgroup::'
        done < <(jq -c '.cells[]' "$PLAN")
        exit "$failed" ;;
    scrub) scrub ;;
    *) echo "usage: $0 check-runtime IMAGE | check-images JSON | pull | wal-disk DIR | generate | cells | scrub" >&2; exit 2 ;;
esac
