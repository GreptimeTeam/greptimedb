# tracking: optimize Metric export and import

Tracking issue: [#9120](https://github.com/GreptimeTeam/greptimedb/issues/9120).

Implementation plan for the proposed RFC: [Metric Export and Import Optimization](../rfcs/2026-09-11-metric-export-import.md).

## Problem and target behavior

Metric datasets with many logical tables accumulate per-table query, DDL and file
handling overhead. Export all selected physical groups, including multi-region
tables, into the existing logical Parquet files, restore logical DDL in bounded batches, and remove redundant COPY
directory scans. Keep V2 snapshot compatibility and recovery boundaries explicit.

Background: [Discussion #7394](https://github.com/orgs/GreptimeTeam/discussions/7394).

## Available evidence

- [x] Physical-group export PoC: dense/sparse round trip through the unmodified
  import-v2 command used in the experiment; per-logical projection; width and memory measurements.
- [x] Batch DDL PoC: SQL/in-process-single/batch controls at 100/1,000/10,000
  tables, metadata/value verification, actual batch failure and retry.
- [x] COPY profile and local fix: 10,000-file data phase 146.496 → 5.481 s with
  batch DDL in both variants; generic explicit-file lookup avoids parent listing.

These boxes mean local PoC completion, not upstream delivery. Reports:
[export](metric-export-poc.md), [DDL](metric-import-ddl-poc.md),
[COPY](metric-import-copy-profile.md). The 5.481 s result does not use merged
writes and covers only 19,998 rows. No production throughput claim follows.

## PR plan

Plan **7 required PRs: 1 RFC PR and 6 implementation/validation PRs**. If the
merged-write experiment justifies first-release inclusion, add **1 conditional
PR**, for 8 in total. PR01–PR08 below are planning labels, not GitHub PR numbers.
Opening the tracking issue itself does not count as a PR.

| PR | Scope | Dependencies | Acceptance |
| --- | --- | --- | --- |
| PR01 — Metric export/import RFC | Agree the export and restore contracts, production DDL interface, compatibility targets and evidence; attach the PoC reports | None | Design reviewed; interface and release-scope decisions recorded |
| PR02 — COPY explicit-file lookup | Isolate the generic directory-scan fix with focused local/object-store regression coverage | None; can land before PR01 | Preserve file/directory behavior; remove repeated parent listing |
| PR03 — Physical-group exporter | Ordered query over all regions; logical routing/projection; existing Metric Parquet types; writer resource controls, cancellation and unit/integration tests | PR01 | Multiple physical groups/regions, including a logical table spanning regions; schema/value equivalence and resource-error behavior |
| PR04 — Export V2 integration | Connect the exporter to COPY DATABASE/V2 under the agreed gate; permissions, consistent metadata selection, chunk completion and owned-output retry | PR03 | Actual CLI export and unmodified import-v2 round trip; local/object-store cancellation, cleanup and retry |
| PR05 — Server batch logical DDL | Production server interface, per-table authorization, batch validation/bounds and existing CREATE procedure semantics | PR01 | API tests for valid/invalid/unauthorized batches, partial completion and errors |
| PR06 — Import V2 batch DDL client | Capability discovery, grouping and request dispatch; dependency barriers, old-server behavior and durable DDL state | PR05 | Remote CLI restore, fresh target IDs, ordinary tables/views, failure/retry and ambiguous completion handling |
| PR07 — Release acceptance and performance | Cross-version fixtures, distributed/object-store end-to-end coverage, release-build benchmark reports and user documentation | PR02, PR04, PR06 | Supported release readers pass; repeated scale/resource results and merged-write include/defer decision recorded |
| PR08 — Merged data writes (conditional) | Integrate bounded merged writes into the importer, including partial-success/retry tests and comparison against corrected per-table COPY | PR02, PR06 and the agreed experiment decision | Meet the pre-agreed benefit/memory criteria; preserve routing and recovery semantics |

PR02 proceeds independently. After PR01, the export chain **PR03 → PR04** and
restore chain **PR05 → PR06** can proceed in parallel; PR07 brings them together.
Run the merged-write PoC early on the corrected COPY baseline, so its decision
does not have to wait for PR07. If PR08 is selected, PR07's final release
acceptance includes it. Until PR04 lands, the new exporter remains internal and
is not exposed as an incomplete user feature.

Each implementation PR includes its own correctness, authorization and failure
tests where applicable. PR07 adds cross-component/release coverage; it is not a
place to defer tests required by earlier PRs. Multi-region support belongs in
PR03 and PR04 and is required for the first release.

## Delivery tasks

### 1. Generic COPY file lookup — PR02

- [ ] Prepare an isolated change containing the explicit-file lookup fix and
  focused regression coverage; exclude Metric test hooks and profiling machinery.
- [ ] Verify files/directories, patterns, missing paths, file-type checks and
  local COPY authorization. Add object-store coverage for known file paths and
  prefixed directories; local results alone do not establish remote behavior.
- [ ] Confirm the file-count scaling improvement and retain a regression case.

Dependencies: independent of RFC acceptance and Metric integration.

### 2. RFC decisions — PR01

- [ ] Review physical scan/routing, column union memory, metadata reconstruction,
  filename compatibility, global SQL ordering and V2 completion/retry semantics.
- [ ] Agree batch DDL transport, capability discovery, request bounds and
  authorization; current in-process test entry is not a production API.
- [ ] Agree experimental integration, formats, backends and actual release-reader
  compatibility targets. First-release Metric Parquet export must cover all
  selected physical groups, any region count and existing Metric Parquet types.
- [ ] Agree merged-write benchmark gain/memory criteria before the comparison.

### 3. Physical export integration — PR03 and PR04

- [ ] Integrate physical grouping into COPY DATABASE under the agreed gate;
  freeze metadata consistently with DDL, preserve ordinary-table handling and
  authorize every selected logical table before physical access.
- [ ] Project the selected physical union, route only admitted IDs, project each
  logical schema before dictionary expansion and emit canonical logical files.
- [ ] Preserve types, field order, time bounds, NULL/empty values, zero-row files,
  dotted names and dropped-table residue filtering. Test existing Metric Parquet
  types beyond the PoC's Float64/string/timestamp coverage.
- [ ] Query all regions of each physical group through global
  `ORDER BY __table_id`. Consume the ordered query result without per-region
  concatenation or physical-operator admission checks; retain runtime ID checks.
- [ ] Apply query/writer resource controls and test oversized-row and resource
  errors, incomplete chunk state and retry. Per-logical-table fallback does not
  satisfy physical-export acceptance.

Dependencies: accepted export contract from task 2.

### 4. Batch DDL integration — PR05 and PR06

- [ ] Implement the agreed authenticated transport and wire it to import-v2.
  Preserve literal physical-table names and ordinary DDL dependency barriers.
- [ ] Validate batch members and bounds before procedure submission, retaining
  CREATE semantics and schema-option inheritance.
- [ ] Verify fresh target IDs, reconstructed Metric metadata, effective options,
  physical routes, two-schema/ordinary-table/view restore and old-server fallback.
- [ ] Exercise partial completion, server errors and ambiguous transport failure;
  advance DDL completion only after the whole DDL phase succeeds.

Dependencies: accepted transport from task 2. Can proceed alongside task 3.

### 5. Recovery and compatibility — PR04, PR06 and PR07

- [ ] Interrupt export during a file, between files and before completion-state
  publication; retry owned unfinished outputs without altering completed chunks.
- [ ] Exercise local and object-store cancellation/partial-object cleanup and
  snapshot verification through the actual CLI path.
- [ ] Restore files with pinned supported release binaries, not only the experiment
  importer; verify the version-1 manifest and canonical DDL layout.
- [ ] Exercise data-task replay after partial import and state-write failures;
  document the inherited replay semantics without claiming exactly-once writes.

Dependencies: tasks 3 and 4 for complete end-to-end acceptance.

### 6. Scale and merged-write decision — PR07; conditional PR08

- [ ] Repeat release-build measurements across table counts up to approximately
  100,000, rows/file, physical union widths, regions and local/object-store backends.
  Include schema export/metadata discovery in end-to-end timing.
- [ ] Exercise multiple physical groups and distributed multi-region queries
  below/at/above the frontend partition budget. Include one logical table with
  rows across regions; verify globally ordered routing and exactly one complete
  file per logical table. Cover streaming-merge and additional-sort plans, with
  resource measurements and failure/retry; both must support physical export.
- [ ] Compare corrected per-table COPY with a bounded merged-write PoC using the
  same DDL/concurrency; measure total time, data time, request counts and memory.
- [ ] Record an explicit first-release include/defer decision for merged writes,
  backed by the agreed benefit/resource criteria and failure/retry validation.

Dependencies: task 1 is the performance baseline. Measurements can proceed while
tasks 2–5 are reviewed; merged writes are neither accepted nor ruled out yet.

## Completion

Close the tracking issue when accepted implementation slices are delivered,
production-path correctness/recovery and stated compatibility targets pass,
performance evidence is attached, and remaining scope has explicit
follow-up decisions. PoC timings or an accepted RFC alone do not close delivery.
