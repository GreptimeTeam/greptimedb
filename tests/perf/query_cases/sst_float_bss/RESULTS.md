# SST float BSS: historical unique-integer results

> Superseded workload: the current case now generates bounded, fluctuating
> 95% integer-valued / 5% fractional series. The numbers below belong only to
> the earlier global-unique-integer experiment, not the current case. Mixed
> workload measurements are pending.

## Decision context

This evidence supports a **minimal opt-in configuration, default off**. It shows a
large storage reduction for this controlled case, a SQL trade-off, and faster
stopped-directory warm readers; it does not establish a universal float benefit.

## Compared build and case

- Same release `greptime` binary for base and candidate: SHA-256
  `2ce0ab1670cd4bdb0f60c87af3e1f494ee66c3c6ab83bce943a073638514eb2b`.
- GreptimeDB `1.3.0-alpha.1`, source commit `dd4734d7b2ca4468f2c8a50e063fc9e7db53794b`.
- BSS case/harness source: `54f26ea8153`.
- The only DDL difference was `experimental_sst_float_field_encoding`:
  `default` (base) versus `byte_stream_split` (candidate). Both used
  `compaction.twcs.trigger_file_num = 100`.
- Three fresh runs wrote 1,024 series × 4,320 samples at 60 s intervals:
  4,423,680 rows per target. Values were unique monotonic integer-valued
  `DOUBLE`s.
- Each run used three 1,440-sample chunks and an explicit flush after each.
  The observed layout was six SSTs: three × 1,105,920 rows and three ×
  368,640 rows, compressed with ZSTD level 1. This is an observed post-flush
  layout, not a promised layout.

## SST and correctness results

The inspection covered `data/greptime/public` only. Each target had six selected
SSTs, 4,423,680 rows, and 45 inspected `greptime_value` chunks. Footer checks
found no BSS encoding in base and BSS in candidate; row-count, logical-schema,
and SQL result checks passed. The expected and returned `sum(greptime_value)`
was `9,784,470,159,360` for both targets. This verifies counts and aggregate-query
results, not an exhaustive row-by-row comparison. Base chunks used dictionary
encoding; candidate chunks used BSS without dictionaries, so this is not a
codec-only comparison with dictionary policy held constant. The bytes below
exclude WAL, indexes, metadata, and internal-table storage.

| Fresh run | Base SST bytes | Candidate SST bytes | Candidate change |
| --- | ---: | ---: | ---: |
| formal-1 | 13,830,799 | 612,899 | -95.5686% |
| formal-2 | 13,830,799 | 612,884 | -95.5687% |
| formal-3 | 13,830,799 | 612,884 | -95.5687% |

The configured storage threshold was at least a 5% reduction (`-5%`); all three
runs passed it.

## Warmed endpoint SQL

Each query had three warmups followed by 15 measured endpoint requests. Values
below are base/candidate median and p95 latency in milliseconds; delta is
`(candidate median - base median) / base median × 100%`.

| Run | Query | Base median / p95 | Candidate median / p95 | Delta |
| --- | --- | ---: | ---: | ---: |
| formal-1 | `sum_all_values` | 7.23 / 8.36 | 6.55 / 7.52 | -9.4% |
| formal-1 | `hourly_sum_values` | 23.11 / 28.71 | 28.76 / 34.74 | +24.4% |
| formal-2 | `sum_all_values` | 6.77 / 7.70 | 7.61 / 8.76 | +12.4% |
| formal-2 | `hourly_sum_values` | 25.48 / 33.30 | 29.26 / 30.61 | +14.8% |
| formal-3 | `sum_all_values` | 8.97 / 10.38 | 9.12 / 9.68 | +1.6% |
| formal-3 | `hourly_sum_values` | 33.44 / 43.80 | 36.89 / 44.14 | +10.3% |

The SQL guardrail was a maximum 25% candidate median regression; all six checks
passed. `hourly_sum_values` was slower in all three runs despite passing.

## Stopped-directory warm readers

These are real `greptime datanode parquetbench` and sequential `scanbench`
measurements against the complete landed SST sets, not endpoint SQL latency.
For each of three formal datasets, the reader script used eight outer rounds,
eight inner iterations per command, discarded iteration 1, computed the median
of iterations 2–8, then summed the six per-file parquet medians before taking
the median across outer rounds. Thus the parquet statistic is a **sum of
per-SST warm medians**, not a whole-region latency. `scanbench` instead covered
the whole region, taking the outer median of its eight warm medians.

Both projections were measured: `value` reads only `greptime_value`;
`allcolumns` selects all columns (five physical columns in direct parquetbench;
all region columns in scanbench). The targets alternated order. No
arbitrary base/candidate pair was selected. Across the three datasets this was
672 commands × 8 iterations = 5,376 timed iterations.

| Dataset | Projection | Reader | Base ms | Candidate ms | Delta |
| --- | --- | --- | ---: | ---: | ---: |
| formal-1 | value | parquetbench | 55.946 | 18.995 | -66.05% |
| formal-1 | value | scanbench | 75.772 | 40.161 | -47.00% |
| formal-1 | allcolumns | parquetbench | 65.515 | 28.804 | -56.03% |
| formal-1 | allcolumns | scanbench | 120.528 | 85.226 | -29.29% |
| formal-2 | value | parquetbench | 111.607 | 35.158 | -68.50% |
| formal-2 | value | scanbench | 140.960 | 77.098 | -45.31% |
| formal-2 | allcolumns | parquetbench | 120.564 | 72.044 | -40.24% |
| formal-2 | allcolumns | scanbench | 226.479 | 224.709 | -0.78% |
| formal-3 | value | parquetbench | 71.076 | 20.752 | -70.80% |
| formal-3 | value | scanbench | 79.159 | 42.435 | -46.39% |
| formal-3 | allcolumns | parquetbench | 72.522 | 34.300 | -52.70% |
| formal-3 | allcolumns | scanbench | 137.906 | 89.151 | -35.35% |

Warm results varied substantially, especially formal-2 all-column scanbench:
its eight round medians ranged from 120.158–422.669 ms (base) and
85.224–243.650 ms (candidate); candidate was slower in rounds 7 and 8.
These deltas are ratios of separately aggregated medians, not medians of paired
percentage changes. Scanbench's projection is verified from commands/configs,
not a printed schema. Target order alternated, but projection order stayed fixed.
The runs used a non-isolated local development host, with no CPU affinity or
cache drop. No specific cause of the variability was established.

## Scope, comparison, and artifacts

This is a post-flush test of monotonic integer-valued `DOUBLE`s. It is not a
general float-compression result, and no compaction result was tested.

[PR #8548](https://github.com/GreptimeTeam/greptimedb/pull/8548) used mixed
counters and gauges and reported 25.66% storage savings with 11.36–41.34% warm
slowdown. Its different dataset and methodology make these results neither a
direct comparison nor a contradiction.

The local artifacts were not uploaded. They are rooted at
`/mnt/nvme_rust/rust-targets/metric-bss-perf/experiments/`:

- `formal-{1,2,3}/sst_float_bss/query-regression-report.json` and
  `prepare-remote.json`: SST, schema/data, endpoint-SQL, and read-bench data.
- `warm-comparison.json`: the 12-row warm-reader table above.
- `warm-formal-{1,2,3}/summary.json`: raw warm-reader summaries.
- `environment.txt`: host and binary checksums.

Reproduce with the existing commands in `tests/perf/README.md` and the external
artifact scripts `tools/warm_readers.py` and `tools/verify.py`; this result adds
no new benchmark framework.

## Verification recorded for this case

- Release build/check: passed.
- Targeted `cargo nextest`: 35 tests passed.
- Python tooling tests: 20 tests passed.
- Plan validation: 15 plans passed.
- Existing default smoke driver: recorded status 0; both targets returned 16 rows.
- All three formal drivers: recorded status 0 and report `ok`; artifact verification passed.
  The outer Python driver always exits 0, so its exit code alone is not validation.

No full-workspace test suite or Clippy run is claimed here.
