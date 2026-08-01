#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf 'Usage: %s REPORT.json\n' "${0##*/}" >&2
  exit 2
fi

report="$1"

[[ -f "${report}" ]] || {
  printf 'Report does not exist: %s\n' "${report}" >&2
  exit 1
}
command -v jq >/dev/null || {
  printf 'jq is required\n' >&2
  exit 1
}
command -v uplot >/dev/null || {
  printf 'YouPlot is required; install it with: brew install youplot\n' >&2
  exit 1
}

jq -e '
  (.status | type == "string") and
  (.targets | type == "array" and length > 0) and
  all(.targets[];
    (.name | type == "string") and
    (.metrics.accepted_spans | type == "number") and
    (.visibility.observed_rows | type == "number") and
    (.metrics.accepted_spans_per_second | type == "number") and
    (.metrics.mean_http_latency_ms | type == "number") and
    (.metrics.failure_count | type == "number")
  )
' "${report}" >/dev/null || {
  printf 'Report is not a completed OTLP trace comparison: %s\n' "${report}" >&2
  exit 1
}

plot_metric() {
  local title="$1"
  local filter="$2"
  local color="$3"

  printf '\n'
  jq -r ".targets[] | [.name, ${filter}] | @tsv" "${report}" |
    uplot bar -t "${title}" -w 80 -c "${color}" -o -
}

jq -r '"Status: \(.status)"' "${report}"
row_count_status=0
jq -r '
  "\nAccepted spans vs visible table rows",
  (.targets[] |
    "  [\(if .metrics.accepted_spans == .visibility.observed_rows then "passed" else "failed" end)] \(.name): accepted=\(.metrics.accepted_spans), visible=\(.visibility.observed_rows)"
  )
' "${report}"
jq -e 'all(.targets[]; .metrics.accepted_spans == .visibility.observed_rows)' "${report}" >/dev/null || row_count_status=1

plot_metric "Accepted spans" ".metrics.accepted_spans" blue
plot_metric "Visible table rows" ".visibility.observed_rows" green
plot_metric "Throughput (spans/s, higher is better)" ".metrics.accepted_spans_per_second" cyan
plot_metric "Mean HTTP latency (ms, lower is better)" ".metrics.mean_http_latency_ms" yellow
plot_metric "Failures (lower is better)" ".metrics.failure_count" red

jq -r '
  "\nThresholds",
  (.thresholds[] |
    if has("actual_pct") then
      "  [\(.status)] \(.threshold): \(.actual_pct)% (limit \(.limit_pct)%; base \(.base), candidate \(.candidate))"
    elif has("actual") then
      "  [\(.status)] \(.threshold) [\(.target)]: \(.actual) (limit \(.limit))"
    else
      "  [\(.status)] \(.threshold): \(.reason // "no measured value")"
    end
  )
' "${report}"

exit "${row_count_status}"
