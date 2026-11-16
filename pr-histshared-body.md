I hereby agree to the terms of the [GreptimeDB CLA](https://github.com/GreptimeTeam/.github/blob/main/CLA.md).

## Refer to a related PR or issue link (optional)

- Follow-up to #10086 / #10178 / #10199, which made the native route read, fold and average stored native histograms.
- Tracking issue: #8887 (native histogram stabilization).

## What's changed and what's your intention?

The legacy PromQL path still evaluates histogram UDFs through `common_query::NativeHistogram`, a second
complete implementation of the same arithmetic. The two disagree — `NativeHistogram` computes exponential
bucket bounds as `2^x`, the native kernel uses the bound table; `NativeHistogram` walks quantiles only from
the lowest bucket, the kernel walks from the highest once a finite-sum quantile reaches the median. So one
histogram has two answers that differ in their last places, and a bug fixed in one is still present in the
other.

This PR makes the legacy path read the native kernel instead of re-deriving the answer. It does not change
`FloatHistogram`, does not touch `servers`/`mito2`/`operator`, and keeps every legacy signature.

### What moves

- `histogram_eval/legacy_bridge.rs` (new): borrows a `NativeHistogram` as the `HistogramView` both
  representations share, calls `histogram_eval::native::{quantile, fraction}`, and maps the kernel's outcome
  back onto the NaN-observation notes the legacy UDFs record.
- The same bridge carries `add` / `sub` / `scale` / `divide_by` onto `FloatHistogram::{add, sub, mul, div}`,
  folding `AddOutcome` into the existing semantics. That reaches `neg`, the sum/avg accumulators,
  `delta`/`idelta`/`rate`/`increase`, and the arithmetic UDFs.
- `FloatHistogram::spans()` widens to `pub(crate)` (no semantic change) so the bridge can read a view's span
  layout.
- `common_query::NativeHistogram` keeps its own estimator for the API surface that still calls it:
  `promql_string`, `estimated_stdvar` and `to_prometheus_buckets` share `all_buckets`/`get_bound`, and
  `quantile_from_parts` is reached only through `quantile_with_info` (now delegating) and the public
  `quantile_histogram_view`. Removing any of it is a separate `common-query` cleanup: that crate cannot
  depend on `promql`, and retiring `quantile`/`fraction`/`negated` and `quantile_histogram_view` means
  auditing their public API and remaining tests first.

### Behaviour changes that are not last-place differences

Delegating means the legacy answers are now the kernel's answers. Six differences are observable, and all
six are cases where the legacy code did something the kernel does not. Three of them change what ordinary,
well-formed data answers and need an explicit decision; the other three are only reachable through input that
violates Prometheus's invariants, so reviewing them is a compatibility call rather than a correctness one.
This list is what two review passes found; it is not claimed to be exhaustive.

**Well-formed data (decision required)**

1. **`start_timestamp` is cleared when a histogram is derived from another.** Legacy `add`/`sub`/`scale`/
   `divide_by` kept the input's field; `FloatHistogram` clears it on every operation that derives a value.
   That is this module's own policy, documented at `float_histogram.rs`: Prometheus's `FloatHistogram` has no
   such field, so nothing here is a Prometheus rule being newly enforced. `rate`, `delta`, `increase`,
   `idelta`, the arithmetic UDFs and `-h` therefore answer `start_timestamp = None`. The sum/avg aggregates
   clear it only once they combine **two or more** samples: the first sample is passed through untouched, so
   a single-row aggregate keeps its stored start timestamp.
2. **`sum = NaN` with more than one bucket.** A NaN sum marks NaN observations, which is legal data. The
   kernel reproduces Prometheus's branch that walks the remaining buckets and leaves the last one for the
   interpolation, so the answer comes from the top bucket rather than the bucket the rank fell into. For
   schema 0, `span(0, 2)`, counts `[8, 1]`, `count = 10`, `q = 0.5`: legacy answered `2^-0.375 ~ 0.7711`
   (the bucket the rank lands in), the kernel answers `2^5 = 32`.
   `quantile_with_nan_sum_uses_the_last_bucket` pins `32.0`, and `quantile_with_info_is_the_native_quantile`
   pins agreement with the kernel across every fixture.
3. **Quantile ties at an exact bucket boundary.** Legacy always walked upward; Prometheus walks from the
   highest bucket once a finite-sum quantile reaches 0.5. At an exact tie the two pick different buckets whose
   bounds are both defensible; the kernel's answer now wins.

**Input that violates Prometheus's invariants (reviewer sign-off, not a data-path regression)**

4. **Zero-bucket widening clamps differently.** Both representations merge every bucket inside the raised
   threshold into `zero_count` and remove it from the spans, so the ordinary case agrees. They differ when a
   populated bucket straddles the new threshold: the kernel widens the threshold to that bucket's upper bound
   instead of splitting it, while legacy decides each bucket by its own upper bound and keeps the straddler.
   Legacy also does nothing at all when the threshold is unchanged.
5. **`count` larger than the buckets' total with a finite `sum`.** Legacy returned `NaN`; Prometheus clamps
   and interpolates, which the kernel now does.
6. **Custom buckets carrying a positive `zero_count`.** The kernel refuses ("histograms with custom buckets
   have no zero bucket") on `zero_count > 0` in the quantile path and `zero_count != 0` in the fraction path,
   so legacy now answers `NaN` where it used to estimate.

Two details that are not semantics: `add`/`sub` still `compact()` the sum so `promql_eq` and the serialized
spans keep their previous answers, and `scale`/`divide_by` fall back to the field-by-field multiplication for
a layout a view cannot express (spans disagreeing with the counts, or a bucket index leaving `i32`) — verified
to still scale such a histogram rather than dropping it, while `add`/`sub` return `None` for it exactly as
before.

## Verification

- `cargo nextest run -p promql -p common-query -p datatypes` — **1808 passed, 0 skipped, 0 failed**.
- `cargo clippy -p promql -p common-query --all-targets -- -D warnings` — clean.
- `cargo fmt --all -- --check` — clean.
- Both commits were rebuilt independently and checked: commit 1 alone passes fmt, clippy and
  `nextest -p promql -p common-query` (1384 passed).

Bit-equality tests in `histogram_eval/legacy_bridge.rs` (all passing):

| test | what it pins |
| --- | --- |
| `quantile_with_info_is_the_native_quantile` | six histograms (custom, gapped exponential, NaN sum, both sides + zero bucket, zero-bucket-only, empty) × 11 quantiles (`-inf, -0.1, 0, 0.001, 0.25, 0.5, 0.75, 1, 1.1, +inf, NaN`), compared with `to_bits()` |
| `fraction_with_info_is_the_native_fraction` | the same shape over eight bound pairs |
| `add_is_the_native_add` / `sub_is_the_native_sub` | bucket counts by index (`to_bits()`) plus `schema`/`zero_threshold`/`count`/`sum`/`zero_count` over five pairs (same layout, schema reduction, zero-threshold growth, mismatched custom bounds, NaN sum) |
| `scale_and_divide_by_are_the_native_mul_and_div` | the same comparison over factors `2, 0.5, -1, 0, +inf` |
| `nan_notes_follow_the_kernel_outcome` | locks the annotation mapping (NaNSkew vs NaNResult vs none) |

## Checklist

- [x] I have written the necessary rustdoc comments.
- [x] I have added the necessary unit tests and integration tests.
- [ ] This PR requires documentation updates.
- [ ] API changes are backward compatible.
- [x] Schema or data changes are backward compatible.
- [ ] This PR needs to be backported to release branches, and I have added the `backport-<target>` labels (e.g. `backport-v1.3` targets `release/v1.3`).

### Notes

- `NativeHistogram::{add,sub,scale,divide_by,quantile_with_info,fraction_with_info}` remain in `common_query`
  (public API, still covered by that crate's own tests) but no PromQL path reaches them any more. Deleting them
  is a follow-up in `common_query` alone.
- The next step in this direction is moving the legacy aggregate accumulator onto the native
  `HistogramAccumulator` fold/merge, so the sum/avg partial state has one implementation too.
