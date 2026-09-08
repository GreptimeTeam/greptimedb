# SST float BSS: mixed-data post-flush evidence

This report replaces the earlier unique-integer experiment as the primary evidence
for `sst_float_bss`. The detailed historical numbers remain in Git history; they
apply only to that superseded workload and must not be used for the current mixed
case.

> **Timing measurements are confounded.** After these runs, the user reported
> that other agents' builds saturated all 20 logical CPUs during measurement.
> No host-wide idle/load gate was enforced. The latency tables below are retained
> as raw observations only: neither improvements nor threshold failures establish
> BSS performance. SST byte counts and cross-target stored-row checks remain
> valid for the inspected dataset. Stable-host timing verification is pending.

## Case and data shape

- Generator/case revision: `149b49de5f3`.
- Base and candidate used the same release `greptime` binary. The only physical
  table difference was `experimental_sst_float_field_encoding`: `default` versus
  `byte_stream_split`; base used dictionary encoding and candidate used BSS
  without dictionaries. This is not a dictionary-policy-held-constant comparison.
- Each of three fresh post-flush runs wrote 1,000 series × 4,320 samples at
  60-second intervals: 4,320,000 rows per target. Three explicit flushes produced
  six SSTs per target: three × 1,105,920 rows and three × 334,080 rows, with 45
  `greptime_value` row groups total. This is observed layout, not a layout
  guarantee.
- `bounded_mixed` is a synthetic, uncalibrated design: a series hash selects its
  baseline and span band, and 60-sample anchor interpolation produces bounded
  fluctuation. It does not represent a measured production distribution.
- Exactly 950 equal-length series are integer-only and 50 are fractional-only,
  yielding 4,104,000 integer rows and 216,000 fractional rows per target. The
  equal-length 1,000-series design deliberately makes both the series and row
  shares 95%/5%.
- The 95% design is not an empirical claim. A [local preview survey project](http://192.168.50.85:8765/view/prometheus-metrics-186-project-preliminary)
  observed 94.79% integral **samples**; it does not establish a global rate,
  global uniqueness, or a 95% series distribution.

## Initial SST inspection and value verification

Inspection is limited to `data/greptime/public`; bytes exclude WAL, indexes,
metadata, and internal/private-table storage. Each of the three runs had the
same totals:

| Measure | Default | BSS | BSS change |
| --- | ---: | ---: | ---: |
| Selected SST bytes | 20,443,131 | 8,884,743 | -56.5392% |
| `greptime_value` compressed bytes | 20,201,188 | 8,643,964 | -57.2106% |
| SST files / rows / value row groups | 6 / 4,320,000 / 45 | 6 / 4,320,000 / 45 | — |

Footer inspection found `PLAIN`/`RLE`/`RLE_DICTIONARY` for the default value
column and `RLE`/`BYTE_STREAM_SPLIT` for BSS (both ZSTD level 1). The configured
storage threshold remains a 5% reduction; this repeated SST-size result passes
that threshold. It is post-flush evidence only, not a compaction result or a
general float-compression claim.

For every base/candidate group in all three runs, rows were compared by the same
primary key and timestamp and their Float64 bits were exact matches: 950
integer-only and 50 fractional-only series, 4,104,000 and 216,000 rows
respectively. The six canonical sorted-row SHA-256 results are identical:

```
48f87d7d01bc8a0ce9a9cdb3033f322e787784fcae61f1417ac25b05c4a40375
```

This verifies exact cross-target stored rows, not independent reconstruction of
the generator output. All 1,000 series had both rises and falls, and every one
of the 36 inspected SSTs contained both integer-valued and fractional values;
these were not stored as two disjoint file sets.

## Warmed endpoint SQL

Each query used three warmups followed by 15 measured endpoint requests. Values
are median/p95 milliseconds; delta is `(BSS median - default median) / default
median × 100%`. The guardrail remains a maximum 25% candidate median regression and
was not changed for these runs.

| Run | Query | Default median / p95 | BSS median / p95 | Delta | Guardrail |
| --- | --- | ---: | ---: | ---: | --- |
| mixed-1 | `sum_all_values` | 27.13 / 37.38 | 25.45 / 38.99 | -6.21% | pass |
| mixed-1 | `hourly_sum_values` | 118.78 / 136.63 | 130.93 / 185.26 | +10.23% | pass |
| mixed-2 | `sum_all_values` | 24.02 / 35.02 | 36.34 / 56.01 | +51.31% | **fail** |
| mixed-2 | `hourly_sum_values` | 95.58 / 120.40 | 85.45 / 148.03 | -10.60% | pass |
| mixed-3 | `sum_all_values` | 38.25 / 55.41 | 19.68 / 29.49 | -48.55% | pass |
| mixed-3 | `hourly_sum_values` | 56.77 / 148.29 | 121.87 / 143.16 | +114.66% | **fail** |

The two recorded threshold failures remain in the artifacts, but the saturated
shared host prevents attributing them to BSS. Passing entries likewise do not
establish performance acceptance. No CPU isolation or cache flush was used.

## Stopped-directory warm readers

For each of the three datasets, eight outer rounds alternate base/candidate order.
Each command runs eight iterations, discards the first, and takes the median of
iterations 2–8. Parquetbench reads every SST: sum the six per-file medians, then
take the median of the eight round totals. Scanbench scans the whole region and
reports the outer median of its eight warm medians. These are distinct metrics,
not interchangeable whole-query latencies. Deltas are ratios of separately
aggregated medians. Value projection selects `greptime_value`; all-column
parquetbench returns five physical columns, while scanbench uses `{}` (all
region columns). Projection order is fixed; target order alternates.

| Dataset | Projection | Reader | Base ms | BSS ms | Change |
| --- | --- | --- | ---: | ---: | ---: |
| mixed-1 | value | parquetbench | 95.957 | 68.442 | -28.67% |
| mixed-1 | value | scanbench | 144.293 | 115.421 | -20.01% |
| mixed-1 | allcolumns | parquetbench | 176.620 | 103.761 | -41.25% |
| mixed-1 | allcolumns | scanbench | 200.881 | 202.660 | +0.89% |
| mixed-2 | value | parquetbench | 112.151 | 74.275 | -33.77% |
| mixed-2 | value | scanbench | 109.824 | 78.189 | -28.80% |
| mixed-2 | allcolumns | parquetbench | 141.944 | 102.681 | -27.66% |
| mixed-2 | allcolumns | scanbench | 211.414 | 163.261 | -22.78% |
| mixed-3 | value | parquetbench | 121.827 | 67.565 | -44.54% |
| mixed-3 | value | scanbench | 100.968 | 79.878 | -20.89% |
| mixed-3 | allcolumns | parquetbench | 113.984 | 94.361 | -17.22% |
| mixed-3 | allcolumns | scanbench | 193.101 | 164.175 | -14.98% |

All 672 commands / 5,376 raw iterations returned the expected rows; timing,
projection configurations, parquet schemas and order metadata were checked.
Run 1 all-column scanbench differed by **+0.89%**, negligible relative to the
observed variation, not evidence of a regression. Its round medians ranged from
167.197–663.465 ms (base) and 133.339–557.352 ms (BSS). There is substantial
variability; these measurements do not establish a stable gain on every read
path. The reported concurrent CPU saturation affects apparent improvements as
well as slowdowns; no causal performance conclusion is drawn from these runs.

## Scope and recorded checks

This case compares default and BSS post-flush data only. It adds no benchmark
framework and no public artifact upload. Historical unique-integer results and
other mixed counter/gauge studies have different data and methods; they are
context only and do not apply to this workload.

Recorded checks for this change set: 37 targeted Rust tests, 20 Python tooling
tests, and 15 plans passed. Full-workspace tests were not run.

Local artifacts are under
`/mnt/nvme_rust/rust-targets/metric-bss-perf/experiments/mixed-{1,2,3}/`,
including `value-verification.json`, `verification.txt`, and each run's
`sst_float_bss/query-regression-report.json`.

Warm artifacts are in sibling `warm-mixed-{1,2,3}/` directories;
`mixed-warm-comparison.json` contains the table and per-round ranges.
`mixed-file-composition.json` records integer/fractional counts for each SST.
The shared release `greptime` SHA-256 is
`2ce0ab1670cd4bdb0f60c87af3e1f494ee66c3c6ab83bce943a073638514eb2b`.
