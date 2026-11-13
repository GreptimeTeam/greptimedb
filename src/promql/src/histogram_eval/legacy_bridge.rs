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
    let view = LegacyView::new(histogram);
    FloatHistogram::try_from_view(&view.view())
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

    use super::{add, float_histogram, fraction_with_info, quantile_with_info, sub};
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

    fn assert_same_payload(name: &str, legacy: &NativeHistogram, native: &FloatHistogram) {
        assert_eq!(legacy.schema, native.schema, "{name}");
        assert_eq!(
            legacy.zero_threshold.to_bits(),
            native.zero_threshold.to_bits(),
            "{name}"
        );
        assert_eq!(legacy.count.to_bits(), native.count.to_bits(), "{name}");
        assert_eq!(legacy.sum.to_bits(), native.sum.to_bits(), "{name}");
        assert_eq!(
            legacy.zero_count.to_bits(),
            native.zero_count.to_bits(),
            "{name}"
        );
        assert_eq!(
            populated_buckets(&legacy.positive_spans, &legacy.positive_buckets),
            populated(&native.positive),
            "{name} positive"
        );
        assert_eq!(
            populated_buckets(&legacy.negative_spans, &legacy.negative_buckets),
            populated(&native.negative),
            "{name} negative"
        );
    }

    /// The added histogram is the kernel's addition, bucket for bucket and bit for bit.
    #[test]
    fn add_is_the_native_add() {
        for (name, left, right) in pairs() {
            let combined = add(&left, &right).unwrap();
            let mut native = float_histogram(&left).unwrap();
            native.add(&float_histogram(&right).unwrap()).unwrap();
            assert_same_payload(name, &combined, &native);
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
            assert_same_payload(name, &combined, &native);
        }
    }

    /// Scaling and dividing move every count and leave the layout alone, as the kernel's own
    /// multiplication and division do.
    #[test]
    fn scale_and_divide_by_are_the_native_mul_and_div() {
        for (name, histogram, _) in pairs() {
            for factor in [2.0, 0.5, -1.0, 0.0, f64::INFINITY] {
                let scaled = super::scale(&histogram, factor);
                let mut native = float_histogram(&histogram).unwrap();
                native.mul(factor);
                assert_same_payload(name, &scaled, &native);

                let divided = super::divide_by(&histogram, factor);
                let mut native = float_histogram(&histogram).unwrap();
                native.div(factor);
                assert_same_payload(name, &divided, &native);
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
}
