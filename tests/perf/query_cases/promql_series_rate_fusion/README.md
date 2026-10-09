# PromQL series rate-fusion regression fixture

This native Query Regression case isolates the candidate shape `SUM(Float64)` by
`dc` over one direct `prom_rate` expression, projected as Float64 with a
millisecond timestamp and zero offset. The immutable input uses the previously qualified dictionary-tag A direct-SST
generator, schema, and layout. All parameters are specified in this case; no
private or gitignored source file is required. It uses seed 25652, 4,096
instances × 512 samples = 2,097,152 rows, 16 `dc` values, 4 devices, and two flat
non-overlapping SSTs of 1,048,576 rows each, 8,192-row groups, 15-second
millisecond timestamps, primary key `(instance, dc, device)`, and deterministic
wave values from 0 through 100. It uses the same data generator, not the
original 127-slope counter or an archived original input.

The range window is 1704069900–1704071715 seconds at 15-second steps (122
evaluations), fully covered by fixture timestamps from 1704067200 through
1704074865. The first SST ends at 1704071025; the 5-minute lookback begins at
1704069600, so this range crosses the actual two-SST boundary and requests data
from both files. Actual scanned row counts must be audited from runtime plans;
the full 2,097,152-row fixture size is not an expected scan count for this query.
The primary range query expects 16 `dc` output groups per evaluation (1,952
result rows) and evaluates 4,096 series per evaluation (499,712
evaluation-points). The instant query at 1704071715 expects 16 output rows.
Primary `TQL EVAL` entries preserve complete response data and use 3 warmups/20
range iterations and 3 warmups/9 instant iterations; the range EVAL is also the
runner's untimed first-query validation. Separate one-shot `TQL ANALYZE VERBOSE`
entries capture runtime gate, actual plan, scan/index, and work-count diagnostics
without adding analyze overhead to timed samples. An untimed one-iteration SQL
audit returns one row per `dc`, verifying 131,072 rows and 256 instances per
group and min/max timestamps 1704067200 and 1704074865.

Controls are limited to grouped `sum_over_time` (wider non-rate path),
grouped `min by (dc) (rate(...))` (a different aggregation operator and a
negative fusion-gate control), and grouped `rate` with a 15-second offset
(offset gate). MIN keeps the same range, rate-window work, and compact 1,952-row
answer while not matching the SUM candidate. They reuse the same table and
range. The offset query's parser acceptance must be confirmed on the pinned
runtime. Each timed query retains the existing 10% candidate-latency-regression
threshold: a pass only means that configured guard did not trip; it is not proof
of candidate-gate activation or performance improvement. Do not loosen it to
hide noise.

After a native run, audit the normalized plans and complete raw responses,
including group labels, timestamps, numeric values with appropriate floating
point tolerance, and expected row counts. Also audit source SHAs, compiler,
features, fixture layout, the actual runtime gate, scans/indexes, and work counts.
Official performance acceptance still needs clean base/candidate medians,
control behavior, drift and tolerance review, and complete cross-arm answers.
This fixture alone cannot establish a gain. Finite-pool behavior, drops, and
spill behavior belong to code tests, not this CI case. No official workflow was
run as part of adding this fixture.
