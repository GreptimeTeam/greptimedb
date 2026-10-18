#!/usr/bin/env bash
# Crossed-order L/N/F measurement over one shared data directory.
#
# Arms are interleaved per case rather than run as whole sweeps: the machine is
# shared, so a 3.4x swing on the never-fused control between two whole-arm sweeps
# is drift, not an effect. Each case runs L, N, F in a rotated order per round, so
# any drift lands on all three arms instead of one.
#
# L and N are the pre-fix commit with the native flag off and on; F is the same
# commit plus the per-field reset index. They are the three arms of section 4.3 of
# docs/fused-series-report.md.
#
# Every invocation writes into its own `records-<UTC timestamp>/` under OUT, so a
# re-run can never overwrite an earlier one, and an existing directory is refused
# instead of cleared. The per-case correctness results land in `<run>/results/`, and
# each case is compared across the arms of its round as soon as that round finishes:
# an arm that disagrees stops the sweep before the next round adds more timings.
#
#   OUT=DIR DATA=DIR BIN_DIR=DIR ROUNDS=3 CASES=S,G,W,C,control \
#     scripts/fused_series_bench_crossed.sh
set -euo pipefail

BIN_DIR=${BIN_DIR:-/mnt/nvme_rust/rust-targets/narthex-min-chain/fused-bench-bin}
OUT=${OUT:-$HOME/.cache/fused-bench}
DATA=${DATA:-$HOME/.cache/fused-scale/data-seed}
DRIVER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fused_series_bench.py"
ROUNDS=${ROUNDS:-3}
CASES=${CASES:-S,G,W,C,control}

# The driver and the servers stay inside the 15000-15199 band this machine's benchmark
# runs are allowed to bind; override any of them the same way.
HTTP=${HTTP:-15090}
GRPC=${GRPC:-15092}
MYSQL=${MYSQL:-15094}
POSTGRES=${POSTGRES:-15096}
PORTS=(--http "$HTTP" --grpc "$GRPC" --mysql "$MYSQL" --postgres "$POSTGRES")

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN=${RUN:-$OUT/records-$STAMP}
# Refuse rather than clear: a previous run's records are the only copy of measurements
# that may not be reproducible, so an existing directory is never overwritten.
if [ -e "$RUN" ]; then
  echo "refusing to write into $RUN: it already exists" >&2
  echo "set RUN=... to a fresh path, or let this script pick records-<UTC timestamp>" >&2
  exit 1
fi
mkdir -p "$(dirname "$RUN")"
mkdir "$RUN"
RESULTS="$RUN/results"
mkdir "$RESULTS"
echo "writing this run into $RUN"

# L and N are the same binary with the flag off and on; F is the fixed build.
declare -A BIN=(
  [L]="${BIN_L:-$BIN_DIR/greptime-f387830a020}"
  [N]="${BIN_N:-$BIN_DIR/greptime-f387830a020}"
  [F]="${BIN_F:-$BIN_DIR/greptime-fixed2}"
)
declare -A ARM=([L]=off [N]=on [F]=on)
declare -A COMMIT=(
  [L]=${COMMIT_L:-f387830a020}
  [N]=${COMMIT_N:-f387830a020}
  [F]=${COMMIT_F:-f387830a020+reset-index}
)

# Each round permutes the arm order, so an arm is not always the one that follows
# another arm's server shutdown. Rounds past the table keep rotating through it.
declare -A ORDER=(
  [1]="L N F"
  [2]="N F L"
  [3]="F L N"
)
order_for_round() {
  local round=$1
  local order=${ORDER[$round]:-}
  if [ -z "$order" ]; then
    case $(( (round - 1) % 3 )) in
      0) order="L N F" ;;
      1) order="N F L" ;;
      2) order="F L N" ;;
    esac
  fi
  echo "$order"
}

for case in ${CASES//,/ }; do
  for round in $(seq 1 "$ROUNDS"); do
    order=$(order_for_round "$round")
    files=()
    for arm in $order; do
      echo "=== case $case round $round arm $arm (${ARM[$arm]}, ${COMMIT[$arm]}) ==="
      python3 "$DRIVER" run \
        --data-home "$DATA" \
        --bin "${BIN[$arm]}" \
        --arm "${ARM[$arm]}" \
        --commit "${COMMIT[$arm]}" \
        --cases "$case" \
        --out "$RUN/c${case}-r${round}-${arm}.json" \
        --log "$RUN/c${case}-r${round}-${arm}.log" \
        --results "$RESULTS" \
        "${PORTS[@]}" \
        --warmup 2 --iterations 9 2>&1 | sed "s/^/  [$arm] /"
      files+=("$RESULTS/c${case}-r${round}-${arm}-${case}.json")
    done
    echo "--- case $case round $round: comparing ${#files[@]} arm(s) ---"
    python3 "$DRIVER" compare --case "$case" "${files[@]}" 2>&1 | sed "s/^/  [compare] /"
  done
done
echo "all runs complete: $RUN"
echo "summarise with: python3 $(dirname "$DRIVER")/fused_series_bench_summarize.py \"$RUN\""
