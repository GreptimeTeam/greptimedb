// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The legacy native-histogram estimators, delegated to the native kernel.
//!
//! `prom_native_histogram_quantile` and `prom_native_histogram_fraction` used to estimate with the
//! bucket walk inside `NativeHistogram` itself: exponential bucket bounds computed as `2^x`, and a
//! forward walk from the lowest bucket only. The native kernel (`histogram_eval`) walks the same
//! histogram the way Prometheus does -- the bound table in `bounds` and, for a quantile above the
//! median of a histogram whose sum is not NaN, a walk from the highest bucket down -- so keeping
//! both estimators leaves two answers for one histogram that differ in their last places.
//!
//! These functions read a [`NativeHistogram`] through the borrowed [`HistogramView`] that both
//! representations share, hand it to the kernel as the [`FloatHistogram`] the kernel owns, and
//! return the answer in the shape the legacy UDFs report it. The scalar arithmetic -- `add`, `sub`,
//! `scale`, `divide_by` -- takes the same route below the estimators, so Prometheus's rules for
//! reconciling a zero bucket, reducing a schema and intersecting custom bounds live in one place.

use common_query::native_histogram::{
    CounterResetHint, NativeHistogram, NativeHistogramQuantileInfo, Span,
};
use datafusion::error::Result;
use datatypes::series::histogram::{BucketSideView, HistogramView};

use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::histogram_eval::native;

/// A [`NativeHistogram`] borrowed as a [`HistogramView`], the form the native kernel reads.
///
/// A view holds its spans as two parallel slices while a `NativeHistogram` holds them as
/// [`Span`]s, so the offsets and lengths are copied here and the counts, bounds and metadata stay
/// borrowed for as long as the view lives.
pub(crate) struct LegacyView<'a> {
    histogram: &'a NativeHistogram,
    positive_offsets: Vec<i32>,
    positive_lengths: Vec<i32>,
    negative_offsets: Vec<i32>,
    negative_lengths: Vec<i32>,
}

impl<'a> LegacyView<'a> {
    pub(crate) fn new(histogram: &'a NativeHistogram) -> Self {
        let (positive_offsets, positive_lengths) = span_columns(&histogram.positive_spans);
        let (negative_offsets, negative_lengths) = span_columns(&histogram.negative_spans);
        Self {
            histogram,
            positive_offsets,
            positive_lengths,
            negative_offsets,
            negative_lengths,
        }
    }

    /// The view of the histogram these span columns were taken from.
    pub(crate) fn view(&self) -> HistogramView<'_> {
        HistogramView {
            schema: self.histogram.schema,
            zero_threshold: self.histogram.zero_threshold,
            sum: self.histogram.sum,
            count: self.histogram.count,
            zero_count: self.histogram.zero_count,
            reset_hint: self.histogram.reset_hint,
            start_timestamp: self.histogram.start_timestamp,
            custom_values: &self.histogram.custom_values,
            positive: BucketSideView {
                span_offsets: &self.positive_offsets,
                span_lengths: &self.positive_lengths,
                buckets: &self.histogram.positive_buckets,
            },
            negative: BucketSideView {
                span_offsets: &self.negative_offsets,
                span_lengths: &self.negative_lengths,
                buckets: &self.histogram.negative_buckets,
            },
        }
    }
}

fn span_columns(spans: &[Span]) -> (Vec<i32>, Vec<i32>) {
    spans.iter().map(|span| (span.offset, span.length)).unzip()
}

/// `histogram_quantile(q, histogram)`, with the NaN-observation note the legacy UDF records.
pub(crate) fn quantile_with_info(
    histogram: &NativeHistogram,
    q: f64,
) -> (f64, Option<NativeHistogramQuantileInfo>) {
    let view = LegacyView::new(histogram);
    match native::quantile(q, &view.view(), &mut Vec::new()) {
        Ok(value) => (value, nan_observation_info(histogram, q)),
        // The kernel refuses a layout it cannot walk, a custom histogram carrying a zero count
        // among them. The legacy estimator reported a histogram it could not read as no estimate,
        // which is the same NaN without a note about NaN observations.
        Err(_) => (f64::NAN, None),
    }
}

/// `histogram_fraction(lower, upper, histogram)`, with whether NaN observations were excluded.
pub(crate) fn fraction_with_info(
    histogram: &NativeHistogram,
    lower: f64,
    upper: f64,
) -> (f64, bool) {
    let view = LegacyView::new(histogram);
    match native::fraction(lower, upper, &view.view(), &mut Vec::new()) {
        Ok(value) => {
            // The same answer the legacy estimator gave: a fraction is degenerate, and reports
            // nothing, unless it runs for a non-empty histogram over an increasing pair of bounds.
            let excluded_nans = histogram.count != 0.0
                && !lower.is_nan()
                && !upper.is_nan()
                && lower < upper
                && histogram.sum.is_nan()
                && bucket_total(histogram) < histogram.count;
            (value, excluded_nans)
        }
        Err(_) => (f64::NAN, false),
    }
}

/// The note for a NaN observation, which the kernel accounts for but does not report.
///
/// A NaN observation leaves the sum NaN and puts fewer observations in the buckets than the count
/// holds them (Prometheus marks it the same way). The kernel then walks the buckets forward from
/// the lowest, so the note distinguishes the two outcomes: the estimate is skewed higher when the
/// requested rank falls inside the buckets' total, and NaN when it falls past it because the
/// observations the buckets cannot carry were the highest ones.
fn nan_observation_info(
    histogram: &NativeHistogram,
    q: f64,
) -> Option<NativeHistogramQuantileInfo> {
    if histogram.count == 0.0 || !(0.0..=1.0).contains(&q) {
        return None;
    }
    let total = bucket_total(histogram);
    if !(histogram.sum.is_nan() && total < histogram.count) {
        return None;
    }
    Some(if total < q * histogram.count {
        NativeHistogramQuantileInfo::NaNResult
    } else {
        NativeHistogramQuantileInfo::NaNSkew
    })
}

/// What the populated buckets hold, summed in the order the kernel walks them: the negative side
/// from its highest index down, the zero bucket, then the positive side upwards.
fn bucket_total(histogram: &NativeHistogram) -> f64 {
    let mut total = 0.0;
    for count in histogram.negative_buckets.iter().rev() {
        total += count;
    }
    total += histogram.zero_count;
    for count in &histogram.positive_buckets {
        total += count;
    }
    total
}

/// The native kernel's owned form of `histogram`.
///
/// A view borrows the offsets and lengths it needs from two temporary slices, so the copy happens
/// here and the `FloatHistogram` owns the counts that result.
fn float_histogram(histogram: &NativeHistogram) -> Result<FloatHistogram> {
    if steps_backwards(&histogram.positive_spans) || steps_backwards(&histogram.negative_spans) {
        return Err(datafusion::error::DataFusionError::Execution(
            "native histogram has an invalid layout".into(),
        ));
    }
    let view = LegacyView::new(histogram);
    FloatHistogram::try_from_view(&view.view())
}

/// Whether a span list steps backwards, which the legacy `bucket_indices` refused.
///
/// The kernel's `side_from_view` adds each offset to a running index and accepts a later span that
/// steps backwards, so the expanded indices repeat or go negative. Legacy refused such a list
/// outright, so the bridge refuses it too: `add`/`sub` answer `None` as they did, and
/// `scale`/`divide_by` keep their field-by-field fallback.
fn steps_backwards(spans: &[Span]) -> bool {
    spans.iter().skip(1).any(|span| span.offset < 0)
}

/// The legacy representation of `histogram`, with the span layout it carries.
fn legacy_histogram(histogram: &FloatHistogram) -> NativeHistogram {
    let [positive, negative] = histogram.spans();
    NativeHistogram {
        schema: histogram.schema,
        zero_threshold: histogram.zero_threshold,
        sum: histogram.sum,
        reset_hint: histogram.reset_hint,
        start_timestamp: histogram.start_timestamp,
        custom_values: histogram.custom_values.clone(),
        positive_spans: legacy_spans(positive),
        negative_spans: legacy_spans(negative),
        count: histogram.count,
        zero_count: histogram.zero_count,
        positive_buckets: bucket_counts(&histogram.positive),
        negative_buckets: bucket_counts(&histogram.negative),
    }
}

fn legacy_spans(spans: Vec<(i32, i32)>) -> Vec<Span> {
    spans
        .into_iter()
        .map(|(offset, length)| Span { offset, length })
        .collect()
}

fn bucket_counts(buckets: &[(i32, f64)]) -> Vec<f64> {
    buckets.iter().map(|&(_, count)| count).collect()
}

/// `left + right`, or `None` for a pair the kernel refuses to combine.
pub(crate) fn add(left: &NativeHistogram, right: &NativeHistogram) -> Option<NativeHistogram> {
    combine(left, right, false)
}

/// `left - right`, a gauge histogram the way Prometheus's `-` leaves it.
pub(crate) fn sub(left: &NativeHistogram, right: &NativeHistogram) -> Option<NativeHistogram> {
    combine(left, right, true)
}

fn combine(
    left: &NativeHistogram,
    right: &NativeHistogram,
    subtract: bool,
) -> Option<NativeHistogram> {
    let mut combined = float_histogram(left).ok()?;
    let other = float_histogram(right).ok()?;
    let outcome = if subtract {
        combined.sub(&other)
    } else {
        combined.add(&other)
    }
    .ok()?;
    // The outcome reports what the kernel already folded into the result, and the legacy callers
    // report both from the operands before the call: `custom_bounds_reconciled` is the intersection
    // the kernel keeps in `custom_values`, which `needs_custom_reconciliation` announces, and a
    // `counter_reset_collision` is the contradiction the kernel answers with an unknown hint, which
    // is what the legacy arithmetic derived from two hints that disagree. Holding the kernel to
    // both keeps a change there from dropping either.
    debug_assert_eq!(
        outcome.custom_bounds_reconciled,
        left.needs_custom_reconciliation(right)
    );
    if outcome.counter_reset_collision {
        debug_assert_eq!(combined.reset_hint, CounterResetHint::Unknown);
    }
    if subtract {
        combined.reset_hint = CounterResetHint::Gauge;
    }
    // The legacy arithmetic compacted a sum, so a bucket the two sides cancel out of leaves no
    // span behind; the kernel keeps the bucket until a caller drops it.
    combined.compact();
    Some(legacy_histogram(&combined))
}

/// `histogram * factor`, a gauge histogram for a negative factor.
pub(crate) fn scale(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let Ok(mut scaled) = float_histogram(histogram) else {
        // A layout the kernel cannot read -- spans that disagree with the counts, or a bucket index
        // that leaves `i32` once expanded -- keeps the field-by-field scaling the legacy caller
        // had, so such a histogram is still multiplied instead of dropped, and reports its layout
        // when it is serialized. Scaling has no bounds to reconcile, so nothing is lost here.
        return field_scaled(histogram, factor);
    };
    scaled.mul(factor);
    legacy_histogram(&scaled)
}

/// `histogram / divisor`, with no buckets left for a zero divisor.
pub(crate) fn divide_by(histogram: &NativeHistogram, divisor: f64) -> NativeHistogram {
    let Ok(mut divided) = float_histogram(histogram) else {
        return field_divided(histogram, divisor);
    };
    divided.div(divisor);
    legacy_histogram(&divided)
}

fn field_scaled(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut scaled = histogram.clone();
    scaled.count *= factor;
    scaled.zero_count *= factor;
    scaled.sum *= factor;
    for count in scaled
        .positive_buckets
        .iter_mut()
        .chain(&mut scaled.negative_buckets)
    {
        *count *= factor;
    }
    if factor < 0.0 {
        scaled.reset_hint = CounterResetHint::Gauge;
    }
    scaled
}

fn field_divided(histogram: &NativeHistogram, divisor: f64) -> NativeHistogram {
    let mut divided = histogram.clone();
    divided.count /= divisor;
    divided.zero_count /= divisor;
    divided.sum /= divisor;
    if divisor == 0.0 {
        divided.positive_spans.clear();
        divided.positive_buckets.clear();
        divided.negative_spans.clear();
        divided.negative_buckets.clear();
    } else {
        for count in divided
            .positive_buckets
            .iter_mut()
            .chain(&mut divided.negative_buckets)
        {
            *count /= divisor;
        }
    }
    if divisor < 0.0 {
        divided.reset_hint = CounterResetHint::Gauge;
    }
    divided
}

#[cfg(test)]
mod tests {
    use common_query::native_histogram::{
        CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, NativeHistogramQuantileInfo, Span,
    };
    use datatypes::series::histogram::{BucketSideView, HistogramView};

    use super::{
        add, divide_by, float_histogram, fraction_with_info, legacy_histogram, quantile_with_info,
        scale, sub,
    };
    use crate::histogram_eval::float_histogram::FloatHistogram;
    use crate::histogram_eval::native;

    /// Every kind of histogram the legacy representation can hold: exponential and custom, one
    /// side and two, a zero bucket, gaps between the spans, a NaN sum standing for NaN
    /// observations, and a histogram with no buckets at all.
    fn fixtures() -> Vec<(&'static str, NativeHistogram)> {
        let mut custom = empty(CUSTOM_BUCKETS_SCHEMA, 0.0, 7.0, 21.0, 0.0);
        custom.custom_values = vec![1.0, 2.0, 4.0];
        custom.positive_spans = vec![span(0, 3)];
        custom.positive_buckets = vec![2.0, 0.0, 5.0];

        let mut spaced = empty(3, 0.0, 6.0, 6.0, 0.0);
        spaced.positive_spans = vec![span(-3, 2), span(5, 2)];
        spaced.positive_buckets = vec![2.0, 1.0, 2.5, 0.5];

        let mut nan_sum = empty(0, 0.0, 10.0, f64::NAN, 0.0);
        nan_sum.positive_spans = vec![span(0, 1)];
        nan_sum.positive_buckets = vec![8.0];

        // With more than one bucket, the kernel's NaN branch walks the remaining buckets and leaves
        // the last one behind for the interpolation, so the chosen bucket is not the one the rank
        // fell into. A single-bucket fixture cannot reach that path.
        let mut nan_sum_multi_bucket = empty(0, 0.0, 10.0, f64::NAN, 0.0);
        nan_sum_multi_bucket.positive_spans = vec![span(0, 2)];
        nan_sum_multi_bucket.positive_buckets = vec![8.0, 1.0];

        let mut both_sides = empty(2, 0.5, 20.0, -3.0, 3.0);
        both_sides.positive_spans = vec![span(0, 3)];
        both_sides.positive_buckets = vec![4.0, 2.0, 5.0];
        both_sides.negative_spans = vec![span(0, 2)];
        both_sides.negative_buckets = vec![3.0, 3.0];

        // Its zero bucket holds observations of its own, so the layout is not empty even though
        // no span is populated.
        let zero_bucket_only = empty(1, 1.0, 4.0, 2.0, 4.0);
        let empty_histogram = empty(0, 0.0, 0.0, 0.0, 0.0);

        vec![
            ("custom", custom),
            ("spaced", spaced),
            ("nan_sum", nan_sum),
            ("nan_sum_multi_bucket", nan_sum_multi_bucket),
            ("both_sides", both_sides),
            ("zero_bucket_only", zero_bucket_only),
            ("empty", empty_histogram),
        ]
    }

    fn empty(
        schema: i32,
        zero_threshold: f64,
        count: f64,
        sum: f64,
        zero_count: f64,
    ) -> NativeHistogram {
        NativeHistogram {
            schema,
            zero_threshold,
            sum,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: Vec::new(),
            positive_spans: Vec::new(),
            negative_spans: Vec::new(),
            count,
            zero_count,
            positive_buckets: Vec::new(),
            negative_buckets: Vec::new(),
        }
    }

    fn span(offset: i32, length: i32) -> Span {
        Span { offset, length }
    }

    fn columns(spans: &[Span]) -> (Vec<i32>, Vec<i32>) {
        spans.iter().map(|span| (span.offset, span.length)).unzip()
    }

    /// The view of `histogram`, written out here rather than through [`super::LegacyView`] so that
    /// a conversion mistake shows up as a failed comparison instead of cancelling out.
    fn view<'a>(
        histogram: &'a NativeHistogram,
        positive: &'a (Vec<i32>, Vec<i32>),
        negative: &'a (Vec<i32>, Vec<i32>),
    ) -> HistogramView<'a> {
        HistogramView {
            schema: histogram.schema,
            zero_threshold: histogram.zero_threshold,
            sum: histogram.sum,
            count: histogram.count,
            zero_count: histogram.zero_count,
            reset_hint: histogram.reset_hint,
            start_timestamp: histogram.start_timestamp,
            custom_values: &histogram.custom_values,
            positive: BucketSideView {
                span_offsets: &positive.0,
                span_lengths: &positive.1,
                buckets: &histogram.positive_buckets,
            },
            negative: BucketSideView {
                span_offsets: &negative.0,
                span_lengths: &negative.1,
                buckets: &histogram.negative_buckets,
            },
        }
    }

    /// The legacy UDFs read their quantile out of the kernel now, so the two must agree to the
    /// last bit for every kind of histogram the legacy representation can hold.
    #[test]
    fn quantile_with_info_is_the_native_quantile() {
        for (name, histogram) in fixtures() {
            let positive = columns(&histogram.positive_spans);
            let negative = columns(&histogram.negative_spans);
            for q in [
                f64::NEG_INFINITY,
                -0.1,
                0.0,
                0.001,
                0.25,
                0.5,
                0.75,
                1.0,
                1.1,
                f64::INFINITY,
                f64::NAN,
            ] {
                let (value, _) = quantile_with_info(&histogram, q);
                let expected =
                    native::quantile(q, &view(&histogram, &positive, &negative), &mut Vec::new())
                        .unwrap();
                assert_eq!(
                    value.to_bits(),
                    expected.to_bits(),
                    "{name} q {q}: {value} vs {expected}"
                );
            }
        }
    }

    /// Prometheus's NaN branch walks the remaining buckets and leaves the last one for the
    /// interpolation, so a multi-bucket `sum = NaN` histogram answers with the top bucket rather
    /// than the bucket the rank fell into. Pinning the number here, rather than only comparing
    /// against the kernel, keeps a future kernel change from silently re-deciding it.
    #[test]
    fn quantile_with_nan_sum_uses_the_last_bucket() {
        let mut histogram = empty(0, 0.0, 10.0, f64::NAN, 0.0);
        histogram.positive_spans = vec![span(0, 2)];
        histogram.positive_buckets = vec![8.0, 1.0];

        // The nine observations the buckets hold are fewer than the count of ten and the sum is
        // NaN, so the note reports the skew: the rank still falls inside what the buckets carry.
        let (value, info) = quantile_with_info(&histogram, 0.5);
        assert_eq!(
            value.to_bits(),
            32.0f64.to_bits(),
            "rank 5 lands in (0.5, 1] (0.7711), but the NaN walk leaves (1, 2] behind and \
             fraction 5 / 1 puts the answer at 2^5"
        );
        assert_eq!(
            info,
            Some(NativeHistogramQuantileInfo::NaNSkew),
            "a rank inside the bucket total is still an estimate"
        );
    }

    /// The other half of the same note: a rank past the total the buckets carry is no estimate at
    /// all, because the observations the buckets cannot hold were the highest ones.
    #[test]
    fn quantile_with_nan_sum_below_the_rank_is_nan() {
        let mut histogram = empty(0, 0.0, 10.0, f64::NAN, 0.0);
        histogram.positive_spans = vec![span(0, 2)];
        histogram.positive_buckets = vec![8.0, 1.0];

        // The rank 0.95 * 10 = 9.5 lies past the 9 observations the buckets hold, so the forward
        // walk never reaches it and the estimate is NaN rather than the last bucket's bound.
        let (value, info) = quantile_with_info(&histogram, 0.95);
        assert!(
            value.is_nan(),
            "a rank the buckets cannot reach has no estimate, got {value}"
        );
        assert_eq!(
            info,
            Some(NativeHistogramQuantileInfo::NaNResult),
            "the note says the NaN observations were the highest ones"
        );

        // The single-bucket fixture the earlier note test uses reaches the same branch: the rank
        // 0.9 * 10 = 9 lies past the 8 observations the one bucket holds.
        let mut single = empty(0, 0.0, 10.0, f64::NAN, 0.0);
        single.positive_spans = vec![span(0, 1)];
        single.positive_buckets = vec![8.0];
        assert!(quantile_with_info(&single, 0.9).0.is_nan());
        assert_eq!(
            quantile_with_info(&single, 0.9).1,
            Some(NativeHistogramQuantileInfo::NaNResult)
        );
    }

    /// A tie between two buckets that do not touch resolves differently in the two walks, and that
    /// difference is why the kernel replaced the estimator: the legacy walk goes up from the lowest
    /// bucket and stops at the first one the rank fits in, while the kernel walks a finite-sum
    /// histogram at or above the median down from the highest. Both numbers are pinned as literals,
    /// the legacy one computed through the estimator still in `common/query`, so that a later
    /// change to either walk shows up as an arithmetic difference instead of a quiet one.
    #[test]
    fn exact_tie_answers_one_from_the_legacy_walk_and_two_from_the_kernel() {
        let mut tie = empty(0, 0.0, 2.0, 3.75, 0.0);
        // Index 0 and index 2 hold one observation each, and index 1 is empty, so the two buckets
        // do not touch and nothing interpolates across the gap.
        tie.positive_spans = vec![span(0, 1), span(1, 1)];
        tie.positive_buckets = vec![1.0, 1.0];

        // `NativeHistogram::quantile_with_info` is the estimator the legacy UDF used before the
        // kernel replaced it and is still in the workspace: rank 0.5 * 2 = 1 fills the bucket at
        // index 0 exactly, whose fraction is 1, so the answer is that bucket's upper bound.
        assert_eq!(
            tie.quantile_with_info(0.5),
            (1.0, None),
            "the legacy walk answers with the upper bound of (0.5, 1]"
        );

        // The kernel's backwards walk meets the bucket at index 2 first, and the rank inside it is
        // 0, so the answer is the lower bound of (2, 4].
        let (value, info) = quantile_with_info(&tie, 0.5);
        assert_eq!(
            value.to_bits(),
            2.0f64.to_bits(),
            "the kernel's walk answers with the lower bound of (2, 4]"
        );
        assert_eq!(
            info, None,
            "a tie between two buckets is not a NaN observation"
        );
    }

    /// The same agreement for `histogram_fraction`, whose legacy wrapper also returns a note.
    #[test]
    fn fraction_with_info_is_the_native_fraction() {
        for (name, histogram) in fixtures() {
            let positive = columns(&histogram.positive_spans);
            let negative = columns(&histogram.negative_spans);
            for (lower, upper) in [
                (f64::NEG_INFINITY, f64::INFINITY),
                (-2.0, 0.5),
                (0.0, 1.0),
                (1.0, 4.0),
                (2.0, 2.0),
                (3.0, 1.0),
                (f64::NAN, 1.0),
                (0.0, f64::NAN),
            ] {
                let (value, _) = fraction_with_info(&histogram, lower, upper);
                let expected = native::fraction(
                    lower,
                    upper,
                    &view(&histogram, &positive, &negative),
                    &mut Vec::new(),
                )
                .unwrap();
                assert_eq!(
                    value.to_bits(),
                    expected.to_bits(),
                    "{name} [{lower}, {upper}]: {value} vs {expected}"
                );
            }
        }
    }

    /// Pairs the legacy arithmetic has to reconcile: the same layout, a resolution difference, a
    /// zero bucket one side has to grow into the other, mismatched custom bounds, and NaN
    /// observations.
    fn pairs() -> Vec<(&'static str, NativeHistogram, NativeHistogram)> {
        let mut same_left = empty(2, 0.0, 6.0, 6.0, 1.0);
        same_left.positive_spans = vec![span(0, 2)];
        same_left.positive_buckets = vec![2.0, 3.0];
        same_left.negative_spans = vec![span(0, 1)];
        same_left.negative_buckets = vec![1.0];
        let mut same_right = empty(2, 0.0, 5.0, 8.0, 0.0);
        same_right.positive_spans = vec![span(-1, 3)];
        same_right.positive_buckets = vec![1.0, 2.0, 2.0];
        same_right.negative_spans = vec![span(1, 1)];
        same_right.negative_buckets = vec![2.0];

        let mut fine = empty(3, 0.0, 2.0, 2.0, 0.0);
        fine.positive_spans = vec![span(0, 2)];
        fine.positive_buckets = vec![1.0, 1.0];
        let mut coarse = empty(0, 0.0, 4.0, 5.0, 0.0);
        coarse.positive_spans = vec![span(0, 1)];
        coarse.positive_buckets = vec![4.0];

        let mut narrow_zero = empty(0, 0.0, 3.0, 3.0, 0.0);
        narrow_zero.positive_spans = vec![span(0, 1)];
        narrow_zero.positive_buckets = vec![3.0];
        let mut wide_zero = empty(0, 2.0, 4.0, 5.0, 1.0);
        wide_zero.positive_spans = vec![span(1, 1)];
        wide_zero.positive_buckets = vec![3.0];

        let mut custom_left = empty(CUSTOM_BUCKETS_SCHEMA, 0.0, 6.0, 12.0, 0.0);
        custom_left.custom_values = vec![1.0, 2.0, 4.0];
        custom_left.positive_spans = vec![span(0, 3)];
        custom_left.positive_buckets = vec![1.0, 3.0, 2.0];
        let mut custom_right = empty(CUSTOM_BUCKETS_SCHEMA, 0.0, 6.0, 14.0, 0.0);
        custom_right.custom_values = vec![2.0, 3.0, 4.0];
        custom_right.positive_spans = vec![span(0, 3)];
        custom_right.positive_buckets = vec![4.0, 1.0, 1.0];

        let mut nan_left = empty(1, 0.0, 10.0, f64::NAN, 0.0);
        nan_left.positive_spans = vec![span(0, 2)];
        nan_left.positive_buckets = vec![3.0, 5.0];
        let mut nan_right = empty(1, 0.0, 2.0, 2.0, 0.0);
        nan_right.positive_spans = vec![span(0, 1)];
        nan_right.positive_buckets = vec![2.0];

        // Every operand carries a start timestamp of its own, because the arithmetic is expected to
        // clear it: a sum differs from both samples it was derived from, so it names no start.
        same_left.start_timestamp = Some(1_000);
        same_right.start_timestamp = Some(2_000);
        fine.start_timestamp = Some(3_001);
        coarse.start_timestamp = Some(3_002);
        narrow_zero.start_timestamp = Some(3_003);
        wide_zero.start_timestamp = Some(3_004);
        custom_left.start_timestamp = Some(3_005);
        custom_right.start_timestamp = Some(3_006);
        nan_left.start_timestamp = Some(3_007);
        nan_right.start_timestamp = Some(3_008);

        vec![
            ("same_layout", same_left, same_right),
            ("schema_reduction", fine, coarse),
            ("zero_threshold", narrow_zero, wide_zero),
            ("custom_bounds", custom_left, custom_right),
            ("nan_sum", nan_left, nan_right),
        ]
    }

    /// The buckets a side holds that carry observations, by index and bit pattern.
    fn populated_buckets(spans: &[Span], buckets: &[f64]) -> Vec<(i32, u64)> {
        let (offsets, lengths) = columns(spans);
        let mut index = 0;
        let mut out = Vec::new();
        let mut counts = buckets.iter();
        for (position, (offset, length)) in offsets.into_iter().zip(lengths).enumerate() {
            index = if position == 0 {
                offset
            } else {
                index + offset
            };
            for _ in 0..length {
                let count = *counts.next().unwrap();
                if count != 0.0 {
                    out.push((index, count.to_bits()));
                }
                index += 1;
            }
        }
        out
    }

    fn populated(side: &[(i32, f64)]) -> Vec<(i32, u64)> {
        side.iter()
            .filter(|(_, count)| *count != 0.0)
            .map(|(index, count)| (*index, count.to_bits()))
            .collect()
    }

    /// Bucket counts by their exact bit patterns, so a comparison also holds for a NaN or a signed
    /// zero a scaling left behind.
    fn count_bits(counts: &[f64]) -> Vec<u64> {
        counts.iter().map(|count| count.to_bits()).collect()
    }

    fn side_count_bits(side: &[(i32, f64)]) -> Vec<u64> {
        side.iter().map(|&(_, count)| count.to_bits()).collect()
    }

    /// A span list as the `(offset, length)` pairs [`FloatHistogram::spans`] reports.
    fn segments(spans: &[Span]) -> Vec<(i32, i32)> {
        spans
            .iter()
            .map(|span| (span.offset, span.length))
            .collect()
    }

    /// Every field of the legacy payload against the kernel's, span segmentation included: a
    /// comparison of the populated buckets alone cannot tell one span of two buckets from two
    /// spans of one bucket each, and says nothing about the metadata beside the counts.
    ///
    /// `expected_start_timestamp` states the policy of the operation that produced the payload. One
    /// that derives a histogram from its input -- `add`, `sub`, `scale`, `divide_by` -- clears the
    /// start timestamp, because the field names the start of the sample the payload was read from
    /// and a derived value is not that sample; the kernel clears it in the same step, so both sides
    /// must carry the value passed here. `scale` and `divide_by` keep a field-by-field fallback for
    /// a layout the kernel cannot read, which derives nothing and copies the field, and the test
    /// that exercises it asserts `Some(..)` where it belongs. `quantile_with_info` and
    /// `fraction_with_info` return no payload and leave their input untouched, so they have no
    /// start timestamp to compare here.
    fn assert_same_payload(
        name: &str,
        legacy: &NativeHistogram,
        native: &FloatHistogram,
        expected_start_timestamp: Option<i64>,
    ) {
        assert_eq!(legacy.schema, native.schema, "{name} schema");
        assert_eq!(
            legacy.zero_threshold.to_bits(),
            native.zero_threshold.to_bits(),
            "{name} zero_threshold"
        );
        assert_eq!(
            legacy.count.to_bits(),
            native.count.to_bits(),
            "{name} count"
        );
        assert_eq!(legacy.sum.to_bits(), native.sum.to_bits(), "{name} sum");
        assert_eq!(
            legacy.zero_count.to_bits(),
            native.zero_count.to_bits(),
            "{name} zero_count"
        );
        assert_eq!(legacy.reset_hint, native.reset_hint, "{name} reset_hint");
        assert_eq!(
            legacy.custom_values, native.custom_values,
            "{name} custom_values"
        );
        assert_eq!(
            legacy.start_timestamp, expected_start_timestamp,
            "{name} start_timestamp"
        );
        assert_eq!(
            native.start_timestamp, expected_start_timestamp,
            "{name} kernel start_timestamp"
        );

        let [positive, negative] = native.spans();
        assert_eq!(
            segments(&legacy.positive_spans),
            positive,
            "{name} positive spans"
        );
        assert_eq!(
            segments(&legacy.negative_spans),
            negative,
            "{name} negative spans"
        );
        assert_eq!(
            count_bits(&legacy.positive_buckets),
            side_count_bits(&native.positive),
            "{name} positive buckets"
        );
        assert_eq!(
            count_bits(&legacy.negative_buckets),
            side_count_bits(&native.negative),
            "{name} negative buckets"
        );
        assert_eq!(
            populated_buckets(&legacy.positive_spans, &legacy.positive_buckets),
            populated(&native.positive),
            "{name} positive bucket indices"
        );
        assert_eq!(
            populated_buckets(&legacy.negative_spans, &legacy.negative_buckets),
            populated(&native.negative),
            "{name} negative bucket indices"
        );
    }

    /// The added histogram is the kernel's addition, bucket for bucket and bit for bit.
    #[test]
    fn add_is_the_native_add() {
        for (name, left, right) in pairs() {
            let combined = add(&left, &right).unwrap();
            let mut native = float_histogram(&left).unwrap();
            native.add(&float_histogram(&right).unwrap()).unwrap();
            // The bridge compacts a sum, dropping the buckets the two sides left empty, so the
            // kernel's own addition needs the same step before the two payloads can be compared
            // bucket for bucket.
            native.compact();
            assert_same_payload(name, &combined, &native, None);
        }
    }

    /// The same for subtraction, which the kernel reports as an addition of the negation.
    #[test]
    fn sub_is_the_native_sub() {
        for (name, left, right) in pairs() {
            let combined = sub(&left, &right).unwrap();
            assert_eq!(combined.reset_hint, CounterResetHint::Gauge, "{name}");
            let mut native = float_histogram(&left).unwrap();
            native.sub(&float_histogram(&right).unwrap()).unwrap();
            native.reset_hint = CounterResetHint::Gauge;
            native.compact();
            assert_same_payload(name, &combined, &native, None);
        }
    }

    /// Scaling and dividing move every count and leave the layout alone, as the kernel's own
    /// multiplication and division do.
    #[test]
    fn scale_and_divide_by_are_the_native_mul_and_div() {
        for (name, histogram, _) in pairs() {
            for factor in [2.0, 0.5, -1.0, 0.0, f64::INFINITY] {
                let scaled = scale(&histogram, factor);
                let mut native = float_histogram(&histogram).unwrap();
                native.mul(factor);
                assert_same_payload(name, &scaled, &native, None);

                let divided = divide_by(&histogram, factor);
                let mut native = float_histogram(&histogram).unwrap();
                native.div(factor);
                assert_same_payload(name, &divided, &native, None);
            }
        }
    }

    /// The note the legacy UDFs turn into an annotation follows the kernel's outcome instead of a
    /// second reading of the histogram.
    #[test]
    fn nan_notes_follow_the_kernel_outcome() {
        use NativeHistogramQuantileInfo::{NaNResult, NaNSkew};

        let nan_sum = fixtures()
            .into_iter()
            .find(|(name, _)| *name == "nan_sum")
            .map(|(_, histogram)| histogram)
            .unwrap();
        assert_eq!(quantile_with_info(&nan_sum, 0.5).1, Some(NaNSkew));
        assert_eq!(quantile_with_info(&nan_sum, 0.9).1, Some(NaNResult));
        assert_eq!(quantile_with_info(&nan_sum, 0.0).1, Some(NaNSkew));
        assert_eq!(quantile_with_info(&nan_sum, 1.0).1, Some(NaNResult));
        assert_eq!(quantile_with_info(&nan_sum, -1.0).1, None);
        assert_eq!(quantile_with_info(&nan_sum, 2.0).1, None);
        assert_eq!(quantile_with_info(&nan_sum, f64::NAN).1, None);

        let mut complete = empty(0, 0.0, 8.0, 8.0, 0.0);
        complete.positive_spans = vec![span(0, 1)];
        complete.positive_buckets = vec![8.0];
        assert_eq!(quantile_with_info(&complete, 0.5).1, None);

        assert!(fraction_with_info(&nan_sum, f64::NEG_INFINITY, f64::INFINITY).1);
        assert!(!fraction_with_info(&complete, f64::NEG_INFINITY, f64::INFINITY).1);
        assert!(!fraction_with_info(&nan_sum, f64::NAN, 1.0).1);
        assert!(!fraction_with_info(&nan_sum, 1.0, 1.0).1);
        assert!(!fraction_with_info(&nan_sum, 2.0, 1.0).1);
    }

    /// The two conversions are inverse: the payload survives `NativeHistogram` to
    /// [`FloatHistogram`] and back field for field, including the parts a comparison of the counts
    /// alone would not see.
    #[test]
    fn round_trip_preserves_the_payload_and_its_span_layout() {
        let mut exponential = empty(2, 0.5, 12.0, -7.5, 1.0);
        exponential.reset_hint = CounterResetHint::NotCounterReset;
        exponential.start_timestamp = Some(1_234_567);
        // A zero-length span carries nothing but its offset, and the two gaps between the three
        // populated segments are part of the layout the legacy representation reports: the counts
        // live at indices 2 and 3 and then 5, with nothing at 4.
        exponential.positive_spans = vec![span(0, 0), span(2, 2), span(1, 1)];
        exponential.positive_buckets = vec![1.0, 3.0, 2.0];
        exponential.negative_spans = vec![span(-2, 2)];
        exponential.negative_buckets = vec![2.5, 1.5];

        let mut custom = empty(CUSTOM_BUCKETS_SCHEMA, 0.0, 6.0, 9.0, 0.0);
        custom.reset_hint = CounterResetHint::Gauge;
        custom.start_timestamp = Some(42);
        custom.custom_values = vec![1.0, 2.0, 4.0, 8.0];
        custom.positive_spans = vec![span(0, 1), span(1, 2)];
        custom.positive_buckets = vec![1.0, 2.0, 3.0];

        for (name, original) in [("exponential", exponential), ("custom", custom)] {
            let round_tripped = legacy_histogram(&float_histogram(&original).unwrap());

            assert_eq!(
                round_tripped.reset_hint, original.reset_hint,
                "{name} reset_hint"
            );
            assert_eq!(
                round_tripped.start_timestamp, original.start_timestamp,
                "{name} start_timestamp"
            );
            assert_eq!(
                round_tripped.custom_values, original.custom_values,
                "{name} custom_values"
            );
            assert_eq!(
                round_tripped.positive_spans, original.positive_spans,
                "{name} positive spans, zero-length and gapped segments included"
            );
            assert_eq!(
                round_tripped.negative_spans, original.negative_spans,
                "{name} negative spans"
            );
            assert_eq!(
                count_bits(&round_tripped.positive_buckets),
                count_bits(&original.positive_buckets),
                "{name} positive buckets"
            );
            assert_eq!(
                count_bits(&round_tripped.negative_buckets),
                count_bits(&original.negative_buckets),
                "{name} negative buckets"
            );
            // Everything else the payload holds -- schema, zero threshold, count, sum and the zero
            // count -- is compared in one place above.
            assert_eq!(
                round_tripped, original,
                "{name}: the round trip changed the histogram"
            );
        }
    }

    /// A histogram an operation derived carries no start timestamp: the field names the start of
    /// the sample the payload was read from, and a sum, a difference, a product or a quotient is
    /// not that sample.
    #[test]
    fn the_arithmetic_clears_the_start_timestamp() {
        let mut left = empty(0, 0.0, 2.0, 3.0, 0.0);
        left.start_timestamp = Some(1_000);
        left.positive_spans = vec![span(0, 1)];
        left.positive_buckets = vec![2.0];
        let mut right = empty(0, 0.0, 1.0, 1.0, 0.0);
        right.start_timestamp = Some(2_000);
        right.positive_spans = vec![span(0, 1)];
        right.positive_buckets = vec![1.0];

        for (name, derived) in [
            ("add", add(&left, &right).unwrap()),
            ("sub", sub(&left, &right).unwrap()),
            ("scale", scale(&left, 2.0)),
            ("divide_by", divide_by(&left, 2.0)),
        ] {
            assert_eq!(
                derived.start_timestamp, None,
                "{name}: a derived histogram names no start"
            );
        }
        // Reading the operands is not deriving from them.
        assert_eq!(left.start_timestamp, Some(1_000));
        assert_eq!(right.start_timestamp, Some(2_000));
    }

    /// The estimators read the buckets rather than the sample's start timestamp, and neither of
    /// them hands back a payload, so the field neither changes the answer nor changes itself.
    #[test]
    fn the_estimators_leave_the_start_timestamp_alone() {
        let mut timed = empty(2, 0.5, 20.0, -3.0, 3.0);
        timed.start_timestamp = Some(1_700_000_000_000);
        timed.positive_spans = vec![span(0, 3)];
        timed.positive_buckets = vec![4.0, 2.0, 5.0];
        timed.negative_spans = vec![span(0, 2)];
        timed.negative_buckets = vec![3.0, 3.0];
        let mut untimed = timed.clone();
        untimed.start_timestamp = None;

        for q in [0.0, 0.25, 0.5, 0.75, 1.0] {
            assert_eq!(
                quantile_with_info(&timed, q).0.to_bits(),
                quantile_with_info(&untimed, q).0.to_bits(),
                "q {q}: the start timestamp takes no part in the estimate"
            );
        }
        for (lower, upper) in [(-1.0, 2.0), (0.0, 1.0)] {
            assert_eq!(
                fraction_with_info(&timed, lower, upper).0.to_bits(),
                fraction_with_info(&untimed, lower, upper).0.to_bits(),
                "[{lower}, {upper}]: the start timestamp takes no part in the estimate"
            );
        }
        assert_eq!(
            timed.start_timestamp,
            Some(1_700_000_000_000),
            "estimating from a histogram must not rewrite it"
        );
    }

    /// The arithmetic reconciles what the two operands disagree about, and reports it in the
    /// payload: the reset hint combination and the custom bounds.
    #[test]
    fn add_and_sub_reconcile_the_reset_hint_and_the_custom_bounds() {
        // A reset and a confirmation contradict each other, so a sum of the two cannot claim
        // either: the kernel answers with the hint that must be inspected.
        let mut reset = empty(0, 0.0, 2.0, 2.0, 0.0);
        reset.reset_hint = CounterResetHint::CounterReset;
        reset.positive_spans = vec![span(0, 1)];
        reset.positive_buckets = vec![2.0];
        let mut confirmed = empty(0, 0.0, 1.0, 1.0, 0.0);
        confirmed.reset_hint = CounterResetHint::NotCounterReset;
        confirmed.positive_spans = vec![span(0, 1)];
        confirmed.positive_buckets = vec![1.0];

        let sum = add(&reset, &confirmed).unwrap();
        assert_eq!(
            sum.reset_hint,
            CounterResetHint::Unknown,
            "a contradiction the payload has to be inspected for"
        );
        assert_eq!(sum.count.to_bits(), 3.0f64.to_bits());
        assert_eq!(sum.sum.to_bits(), 3.0f64.to_bits());
        assert_eq!(sum.positive_buckets, vec![3.0]);
        assert_eq!(
            add(&confirmed, &reset).unwrap().reset_hint,
            CounterResetHint::Unknown,
            "the same contradiction in the other order"
        );
        assert_eq!(
            add(&confirmed, &confirmed).unwrap().reset_hint,
            CounterResetHint::NotCounterReset,
            "two counters that agree keep their agreement"
        );
        assert_eq!(
            sub(&reset, &confirmed).unwrap().reset_hint,
            CounterResetHint::Gauge,
            "subtraction is a gauge operation whatever the operands said"
        );

        // Custom bounds that disagree leave the result with the bounds both sides carry.
        let mut left = empty(CUSTOM_BUCKETS_SCHEMA, 0.0, 6.0, 12.0, 0.0);
        left.custom_values = vec![1.0, 2.0, 4.0];
        left.positive_spans = vec![span(0, 3)];
        left.positive_buckets = vec![1.0, 3.0, 2.0];
        let mut right = empty(CUSTOM_BUCKETS_SCHEMA, 0.0, 6.0, 14.0, 0.0);
        right.custom_values = vec![2.0, 3.0, 4.0];
        right.positive_spans = vec![span(0, 3)];
        right.positive_buckets = vec![4.0, 1.0, 1.0];

        let combined = add(&left, &right).unwrap();
        assert_eq!(
            combined.custom_values,
            vec![2.0, 4.0],
            "the intersection of (1, 2, 4) and (2, 3, 4)"
        );
        assert_eq!(combined.count.to_bits(), 12.0f64.to_bits());
        assert_eq!(combined.sum.to_bits(), 26.0f64.to_bits());
        // Each side's buckets land in the interval of the merged bounds they fall in: `(-inf, 2]`
        // takes 1 + 3 from the left and 4 from the right, and `(2, 4]` takes 2 from the left and
        // 1 + 1 from the right. The empty `(4, inf)` bucket is dropped before it reaches a span.
        assert_eq!(combined.positive_buckets, vec![8.0, 4.0]);
        assert_eq!(combined.positive_spans, vec![span(0, 2)]);
    }

    /// A bucket the two sides cancel out of leaves no span behind, and `promql_eq` reads the
    /// buckets the spans expand to rather than the span list they are written with.
    #[test]
    fn sub_compacts_the_buckets_it_cancels_and_promql_eq_expands_the_spans() {
        let mut left = empty(2, 0.5, 4.0, 6.0, 0.0);
        left.positive_spans = vec![span(0, 2)];
        left.positive_buckets = vec![2.0, 1.5];
        left.negative_spans = vec![span(0, 1)];
        left.negative_buckets = vec![0.5];

        let difference = sub(&left, &left).unwrap();
        assert_eq!(difference.count.to_bits(), 0.0f64.to_bits());
        assert_eq!(difference.sum.to_bits(), 0.0f64.to_bits());
        assert_eq!(difference.zero_count.to_bits(), 0.0f64.to_bits());
        assert!(
            difference.positive_spans.is_empty(),
            "a cancelled bucket leaves no span behind, got {:?}",
            difference.positive_spans
        );
        assert!(difference.positive_buckets.is_empty());
        assert!(difference.negative_spans.is_empty());
        assert!(difference.negative_buckets.is_empty());
        // The same histogram as no histogram at all.
        assert!(
            difference.promql_eq(&empty(2, 0.5, 0.0, 0.0, 0.0)),
            "the cancelling subtraction is the empty histogram"
        );

        // Two spans of one bucket each describe the same buckets as one span of two: the first
        // span's offset is the bucket index, and each later offset is the gap after the previous
        // span, so `(0, 1), (0, 1)` puts the second bucket at index 1 while `(0, 1), (1, 1)`
        // leaves index 1 empty and puts it at index 2.
        let mut one_span = empty(0, 0.0, 3.0, 3.0, 0.0);
        one_span.positive_spans = vec![span(0, 2)];
        one_span.positive_buckets = vec![1.0, 2.0];
        let mut two_spans = empty(0, 0.0, 3.0, 3.0, 0.0);
        two_spans.positive_spans = vec![span(0, 1), span(0, 1)];
        two_spans.positive_buckets = vec![1.0, 2.0];
        assert!(
            one_span.promql_eq(&two_spans),
            "the same indices and counts under a different segmentation"
        );

        let mut gapped = empty(0, 0.0, 3.0, 3.0, 0.0);
        gapped.positive_spans = vec![span(0, 1), span(1, 1)];
        gapped.positive_buckets = vec![1.0, 2.0];
        assert!(
            !one_span.promql_eq(&gapped),
            "the same counts at indices 0 and 2 are not the same histogram"
        );
        let mut other_counts = two_spans.clone();
        other_counts.positive_buckets = vec![2.0, 1.0];
        assert!(
            !one_span.promql_eq(&other_counts),
            "the same indices with other counts are not the same histogram"
        );
    }

    /// Payloads the legacy caller could still produce and the kernel refuses to read: a span whose
    /// length disagrees with the counts, a span list that steps backwards, an index that leaves
    /// `i32` once expanded, and a negative span length. All four carry the same numbers, so the
    /// field-by-field answers below are the same for each of them.
    fn unreadable_layouts() -> Vec<(&'static str, NativeHistogram, Option<i64>)> {
        // The span claims one bucket and the counts hold two.
        let mut short_counts = empty(0, 0.0, 3.0, 5.0, 1.0);
        short_counts.reset_hint = CounterResetHint::NotCounterReset;
        short_counts.positive_spans = vec![span(0, 1)];
        short_counts.positive_buckets = vec![1.0, 2.0];
        let mut backwards = short_counts.clone();
        backwards.positive_spans = vec![span(0, 1), span(-1, 1)];
        let mut overflow = short_counts.clone();
        overflow.positive_spans = vec![span(i32::MAX, 2)];
        let mut negative_length = short_counts.clone();
        negative_length.positive_spans = vec![span(0, -1)];

        vec![
            ("short_counts", short_counts, Some(4_242)),
            ("backwards", backwards, Some(4_243)),
            ("overflow", overflow, Some(4_244)),
            ("negative_length", negative_length, Some(4_245)),
        ]
        .into_iter()
        .map(|(name, mut histogram, start_timestamp)| {
            histogram.start_timestamp = start_timestamp;
            (name, histogram, start_timestamp)
        })
        .collect()
    }

    /// A layout the kernel cannot walk keeps the field-by-field scaling and division the legacy
    /// caller had, so such a histogram is still multiplied instead of dropped, and keeps the start
    /// timestamp it was stored with.
    #[test]
    fn scale_and_divide_fall_back_to_the_fields_for_an_unreadable_layout() {
        for (name, histogram, start_timestamp) in unreadable_layouts() {
            // A negative factor turns the histogram into a gauge; the untouched hint otherwise.
            let negated = scale(&histogram, -0.5);
            assert_eq!(negated.count, -1.5, "{name} count");
            assert_eq!(negated.sum, -2.5, "{name} sum");
            assert_eq!(negated.zero_count, -0.5, "{name} zero_count");
            assert_eq!(
                negated.positive_buckets,
                vec![-0.5, -1.0],
                "{name} positive"
            );
            assert_eq!(negated.reset_hint, CounterResetHint::Gauge, "{name} hint");
            assert_eq!(
                negated.start_timestamp, start_timestamp,
                "{name}: the field-by-field fallback copies the payload, so the start timestamp \
                 survives where the kernel path clears it"
            );
            assert_eq!(
                negated.positive_spans, histogram.positive_spans,
                "{name}: the fallback keeps the layout it cannot expand"
            );

            let doubled = scale(&histogram, 2.0);
            assert_eq!(doubled.count, 6.0, "{name} count");
            assert_eq!(doubled.sum, 10.0, "{name} sum");
            assert_eq!(doubled.zero_count, 2.0, "{name} zero_count");
            assert_eq!(doubled.positive_buckets, vec![2.0, 4.0], "{name} positive");
            assert_eq!(
                doubled.reset_hint,
                CounterResetHint::NotCounterReset,
                "{name}: a positive factor leaves a hint alone"
            );
            assert_eq!(doubled.start_timestamp, start_timestamp, "{name}");

            let quartered = divide_by(&histogram, 4.0);
            assert_eq!(quartered.count, 0.75, "{name} count");
            assert_eq!(quartered.sum, 1.25, "{name} sum");
            assert_eq!(quartered.zero_count, 0.25, "{name} zero_count");
            assert_eq!(
                quartered.positive_buckets,
                vec![0.25, 0.5],
                "{name} positive"
            );
            assert_eq!(
                quartered.reset_hint,
                CounterResetHint::NotCounterReset,
                "{name}: a positive divisor leaves a hint alone"
            );
            assert_eq!(quartered.start_timestamp, start_timestamp, "{name}");

            // A zero divisor empties the buckets on this path too.
            let over_zero = divide_by(&histogram, 0.0);
            assert_eq!(over_zero.count, f64::INFINITY, "{name} count");
            assert_eq!(over_zero.sum, f64::INFINITY, "{name} sum");
            assert_eq!(over_zero.zero_count, f64::INFINITY, "{name} zero_count");
            assert!(over_zero.positive_spans.is_empty(), "{name}");
            assert!(over_zero.positive_buckets.is_empty(), "{name}");
            assert_eq!(over_zero.start_timestamp, start_timestamp, "{name}");

            // The addition and the subtraction refuse the same layout outright, as the legacy
            // arithmetic did, rather than answering with a payload the kernel never built.
            assert!(
                add(&histogram, &histogram).is_none(),
                "{name}: a layout the kernel cannot read has no sum"
            );
            assert!(
                sub(&histogram, &histogram).is_none(),
                "{name}: a layout the kernel cannot read has no difference"
            );
        }
    }

    /// The factors that are not a plain finite number, on a layout the kernel can walk: a zero
    /// divisor leaves no bucket, a NaN factor reaches every count, and a negative one turns the
    /// histogram into a gauge.
    #[test]
    fn scale_and_divide_by_the_edge_factors() {
        let mut histogram = empty(0, 0.0, 4.0, 6.0, 0.0);
        histogram.reset_hint = CounterResetHint::NotCounterReset;
        histogram.start_timestamp = Some(7);
        histogram.positive_spans = vec![span(0, 2)];
        histogram.positive_buckets = vec![3.0, 1.0];
        histogram.negative_spans = vec![span(0, 1)];
        histogram.negative_buckets = vec![1.0];

        let over_zero = divide_by(&histogram, 0.0);
        assert_eq!(over_zero.count, f64::INFINITY, "a positive count over zero");
        assert_eq!(over_zero.sum, f64::INFINITY, "a positive sum over zero");
        assert!(
            over_zero.zero_count.is_nan(),
            "a zero count over zero is NaN"
        );
        assert!(over_zero.positive_spans.is_empty());
        assert!(over_zero.positive_buckets.is_empty());
        assert!(over_zero.negative_spans.is_empty());
        assert!(over_zero.negative_buckets.is_empty());
        assert_eq!(over_zero.start_timestamp, None);
        assert_eq!(
            over_zero.reset_hint,
            CounterResetHint::NotCounterReset,
            "a zero divisor is not a negative one"
        );

        // A negative zero is still zero, so the buckets go the same way, and the quotients it
        // leaves behind are the negative infinities.
        let over_negative_zero = divide_by(&histogram, -0.0);
        assert_eq!(
            over_negative_zero.count,
            f64::NEG_INFINITY,
            "a positive count over negative zero"
        );
        assert_eq!(
            over_negative_zero.sum,
            f64::NEG_INFINITY,
            "a positive sum over negative zero"
        );
        assert!(over_negative_zero.zero_count.is_nan());
        assert!(over_negative_zero.positive_spans.is_empty());
        assert!(over_negative_zero.positive_buckets.is_empty());
        assert_eq!(over_negative_zero.start_timestamp, None);
        assert_eq!(
            over_negative_zero.reset_hint,
            CounterResetHint::NotCounterReset,
            "negative zero is not a negative factor"
        );

        let over_nan = scale(&histogram, f64::NAN);
        assert!(over_nan.count.is_nan() && over_nan.sum.is_nan());
        assert!(over_nan.positive_buckets.iter().all(|count| count.is_nan()));
        assert!(over_nan.negative_buckets.iter().all(|count| count.is_nan()));
        assert_eq!(
            over_nan.positive_spans,
            vec![span(0, 2)],
            "a NaN factor moves the counts, not the layout"
        );
        assert_eq!(over_nan.start_timestamp, None);
        assert_eq!(
            over_nan.reset_hint,
            CounterResetHint::NotCounterReset,
            "a NaN factor is not a negative one"
        );

        let negated = scale(&histogram, -2.0);
        assert_eq!(negated.count, -8.0);
        assert_eq!(negated.sum, -12.0);
        assert_eq!(negated.positive_buckets, vec![-6.0, -2.0]);
        assert_eq!(negated.negative_buckets, vec![-2.0]);
        assert_eq!(negated.positive_spans, vec![span(0, 2)]);
        assert_eq!(negated.reset_hint, CounterResetHint::Gauge);
        assert_eq!(negated.start_timestamp, None);

        let negated_division = divide_by(&histogram, -2.0);
        assert_eq!(negated_division.count, -2.0);
        assert_eq!(negated_division.sum, -3.0);
        assert_eq!(negated_division.positive_buckets, vec![-1.5, -0.5]);
        assert_eq!(negated_division.negative_buckets, vec![-0.5]);
        assert_eq!(negated_division.reset_hint, CounterResetHint::Gauge);
        assert_eq!(negated_division.start_timestamp, None);
    }
}
