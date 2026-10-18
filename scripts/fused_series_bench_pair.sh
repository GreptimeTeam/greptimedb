#!/usr/bin/env bash
# Crossed-order two-arm measurement: two commits on the same fused path.
#
# This is the driver behind section 4.6 of docs/fused-series-report.md, which compares
# `611917eb0b1` (P, one kernel call per grid point) against `d42d14df06d` (F, one call per
# batch of windows). Both arms run with the native flag ON, so both fuse: the comparison is
# between two kernels on the fused path, not fusion against the ordinary plan. For that
# comparison use `fused_series_bench_crossed.sh` with `--arm off` as the baseline.
#
# Arms are interleaved per case rather than run as whole sweeps, and the arm order is
# rotated per round, so host drift lands on both arms instead of one. The machine is
# shared: an unchanged control case swings several percent between invocations here, which
# is why the control case is part of the default case list.
#
# Every invocation writes into its own `records-<UTC timestamp>/` under OUT, so a re-run
# can never overwrite an earlier one, and an existing directory is refused instead of
# cleared. Each case's results are compared across the arms of its round as soon as that
# round finishes: an arm that disagrees stops the sweep before the next round adds more
# timings.
#
# The two arms may also be the *same* binary with the native flag off and on, which is how the
# fused path is compared against the ordinary plan. Set ARM_P=off ARM_F=on for that; the driver
# refuses a flag-off arm that logs a fused selection, so the two cannot be confused.
#
#   # two commits, both fused
#   BIN_P=<parent> BIN_F=<this> scripts/fused_series_bench_pair.sh
#
#   # one binary, ordinary plan against fused
#   BIN_P=<this> BIN_F=<this> ARM_P=off ARM_F=on scripts/fused_series_bench_pair.sh
#
# When the arms are two commits rather than two flags, verify they are really different files:
# a rebuild that silently reused the previous one produces a clean "no difference".
#   strings <bin> | grep -c extrapolated_rate_batch
# is 0 before the batch path exists and non-zero after.
#
#   OUT=DIR DATA=DIR ROUNDS=3 CASES=S,G,W,C,control BIN_P=... BIN_F=... \
#     scripts/fused_series_bench_pair.sh
set -euo pipefail

OUT=${OUT:-$HOME/.cache/fused-bench}
DATA=${DATA:-$HOME/.cache/fused-scale/data-seed}
DRIVER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fused_series_bench.py"
SUMMARIZE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fused_series_bench_pair_summarize.py"
ROUNDS=${ROUNDS:-3}
CASES=${CASES:-S,G,W,C,control}

# The driver and the servers stay inside the 15000-15199 band this machine's benchmark runs
# are allowed to bind; override any of them the same way.
HTTP=${HTTP:-15090}
GRPC=${GRPC:-15092}
MYSQL=${MYSQL:-15094}
POSTGRES=${POSTGRES:-15096}
PORTS=(--http "$HTTP" --grpc "$GRPC" --mysql "$MYSQL" --postgres "$POSTGRES")

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN=${RUN:-$OUT/records-$STAMP}
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

# Checked before the array rather than with `${BIN_P:?...}`: an unquoted `:?` message
# containing an apostrophe ends the parameter expansion early, and bash then silently drops the
# rest of the assignment list -- which leaves `BIN[F]` unset and fails much later, at the point
# of use, with a message that points at the wrong line.
if [ -z "${BIN_P:-}" ]; then
  echo "set BIN_P to the earlier arm's binary" >&2
  exit 1
fi
if [ -z "${BIN_F:-}" ]; then
  echo "set BIN_F to the later arm's binary" >&2
  exit 1
fi
declare -A BIN=(
  [P]="$BIN_P"
  [F]="$BIN_F"
)
declare -A COMMIT=(
  [P]=${COMMIT_P:-unknown-P}
  [F]=${COMMIT_F:-unknown-F}
)

# Per-arm native flag, defaulting to `on` for both: two fused builds. `ARM_P=off ARM_F=on`
# compares the ordinary plan against the fused one, with the driver's hit check keeping them
# apart.
ARM_P=${ARM_P:-on}
ARM_F=${ARM_F:-on}
declare -A FLAG=([P]="$ARM_P" [F]="$ARM_F")

order_for_round() {
  case $(( ($1 - 1) % 2 )) in
    0) echo "P F" ;;
    1) echo "F P" ;;
  esac
}

for case in ${CASES//,/ }; do
  for round in $(seq 1 "$ROUNDS"); do
    order=$(order_for_round "$round")
    files=()
    for arm in $order; do
      echo "=== case $case round $round arm $arm (${COMMIT[$arm]}, flag ${FLAG[$arm]}) ==="
      python3 "$DRIVER" run \
        --data-home "$DATA" \
        --bin "${BIN[$arm]}" \
        --arm "${FLAG[$arm]}" \
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
echo "summarise with: python3 $SUMMARIZE \"$RUN\""
