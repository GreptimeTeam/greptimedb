# Prometheus conformance: known differences

Every entry here is a difference the comparison found between GreptimeDB and the pinned Prometheus
engine (`v0.315.1`). An entry is a **reported** difference, not a silenced one: the run still prints
it, and it is listed again in the job summary.

Adding an entry requires a reason and a tracking reference. An entry without either is a bug in this
file, not an accepted difference.

| Query shape | Prometheus | GreptimeDB | Why this is here | Tracking |
| --- | --- | --- | --- | --- |
| `rate`, `irate`, `increase`, `delta` and the `*_over_time` aggregations over a range selector | The result series drops `__name__` | The result series keeps `__name__` | Found by this harness on its first run over `cases/rate.json`. Prometheus removes the metric name whenever a function changes what the samples mean; GreptimeDB's PromQL evaluator keeps it in the output labels. This is a real semantic difference in the result labels, not a representation difference, so it is reported rather than normalized away. | Needs an issue before this table can shrink. |

## How these are used

`narthex-prometheus-compat.py` compares strictly. A query whose only difference is listed above is
still reported as a failure in the run output and in `report.json`; the job then counts it separately
so the summary shows `agree / known-difference / failed` instead of hiding it in either bucket.

Nothing here may be used to ignore a difference in **values**, **timestamps**, **missing points** or
**extra series**. Only the listed label difference is covered.

## Set operations

`left_metric or right_metric` returns only the left series: the right side's two series are missing,
and the metric name on the left series is the left table's. This is an executor gap in the set
operation, not a representation difference, so it is **not** accepted. The harness keeps it as a
failure in `report.json` until it is fixed; `KNOWN-DIFFERENCES.md` records it here so the reason is
findable from the result.

Tracked: to be fixed; this entry documents the gap and must not be used to silence it.
