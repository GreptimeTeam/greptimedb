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
//! representations share and return the kernel's answer in the shape the legacy UDFs report it.
//! The scalar arithmetic that follows the same route lives below the estimators.

use common_query::native_histogram::{NativeHistogram, NativeHistogramQuantileInfo, Span};
use datatypes::series::histogram::{BucketSideView, HistogramView};

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

#[cfg(test)]
mod tests {
    use common_query::native_histogram::{
        CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, NativeHistogramQuantileInfo, Span,
    };
    use datatypes::series::histogram::{BucketSideView, HistogramView};

    use super::{fraction_with_info, quantile_with_info};
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
