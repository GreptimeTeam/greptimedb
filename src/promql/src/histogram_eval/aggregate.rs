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

//! The compensated `sum` and `avg` fold of native histograms, following the histogram paths of
//! Prometheus `aggregation`. The range functions `sum_over_time` and `avg_over_time` fold a window
//! with it, and the aggregate node runs the same steps per point through
//! `crate::aggregate::HistogramAccumulator`.

use datafusion::error::Result;

use crate::histogram_eval::float_histogram::{BucketLayout, EmptyBuckets, FloatHistogram};

/// A native histogram aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistogramAggregate {
    Sum,
    Avg,
}

/// The Kahan sum or the mean of `histograms` in order. A compact fold remembers represented
/// zeros through `last_rebin` (zero for a uniform schema). `None` preserves the full layout.
/// Returns `None` for an empty window or a mix of exponential and custom buckets. Prometheus
/// `sum_over_time` and `avg_over_time` run the same steps as the aggregations. The first error
/// ends the fold. The derived result carries no start timestamp.
///
/// The flag reports whether any add had to reconcile two different custom-bucket sets, which is
/// what Prometheus annotates with an info. It is carried out of the fold rather than recomputed:
/// the fold already calls [`FloatHistogram::kahan_add`] and receives that outcome.
pub(crate) fn sum_or_avg(
    op: HistogramAggregate,
    histograms: impl IntoIterator<Item = Result<FloatHistogram>>,
    last_rebin: Option<usize>,
) -> Result<(Option<FloatHistogram>, bool)> {
    let mut histograms = histograms.into_iter();
    let Some(first) = histograms.next() else {
        return Ok((None, false));
    };
    let mut value = first?;
    if last_rebin == Some(0) {
        value.compact();
    }
    let mut compensation: Option<FloatHistogram> = None;
    let mut empty = EmptyBuckets::default();
    let mut incremental_mean = false;
    let mut count = 1.0;
    let mut custom_reconciled = false;
    for (index, h) in histograms.enumerate() {
        let mut h = h?;
        // A mixed window still reports any custom bounds reconciled before the mismatch.
        if value.uses_custom_buckets() != h.uses_custom_buckets() {
            return Ok((None, custom_reconciled));
        }
        let index = index + 1;
        let layout = if last_rebin.is_some_and(|last| index > last) {
            h.compact();
            BucketLayout::Compact
        } else {
            BucketLayout::Preserve
        };
        count += 1.0;
        if incremental_mean {
            h.div(count);
        }
        let track_empty = last_rebin.is_some() && layout == BucketLayout::Preserve;
        let rebin_value = h.schema < value.schema;
        if track_empty && let Some(c) = compensation.as_mut() {
            empty.prepare_for_add(&mut value, c, &mut h)?;
        }
        match op {
            HistogramAggregate::Sum => {
                let (c, outcome) = value.kahan_add_with_layout(&h, compensation.take(), layout)?;
                custom_reconciled |= outcome.custom_bounds_reconciled;
                compensation = Some(c);
            }
            HistogramAggregate::Avg => {
                if !incremental_mean {
                    let mut sum = value.clone();
                    let (c, outcome) =
                        sum.kahan_add_with_layout(&h, compensation.clone(), layout)?;
                    custom_reconciled |= outcome.custom_bounds_reconciled;
                    if sum.has_overflow() {
                        value.div(count - 1.0);
                        if let Some(c) = compensation.as_mut() {
                            c.div(count - 1.0);
                        }
                        h.div(count);
                        incremental_mean = true;
                    } else {
                        value = sum;
                        compensation = Some(c);
                    }
                }
                if incremental_mean {
                    let q = (count - 1.0) / count;
                    if let Some(c) = compensation.as_mut() {
                        c.mul(q);
                    }
                    value.mul(q);
                    let (mut c, outcome) = value.kahan_add(&h, compensation.take())?;
                    // Scaling the mean can also empty buckets outside the incoming sample.
                    if layout == BucketLayout::Compact {
                        value.compact_with_compensation(&mut c);
                    }
                    custom_reconciled |= outcome.custom_bounds_reconciled;
                    compensation = Some(c);
                }
            }
        }
        if track_empty && let Some(c) = compensation.as_mut() {
            if last_rebin == Some(index) {
                value.compact_with_compensation(c);
                empty = EmptyBuckets::default();
            } else {
                // Copying the first input, reducing the accumulator, and scaling a mean can
                // leave zeros anywhere. An ordinary add changes only its input's buckets.
                empty.take_from(
                    &mut value,
                    c,
                    &h,
                    index == 1 || rebin_value || incremental_mean,
                );
            }
        }
    }
    if op == HistogramAggregate::Avg && !incremental_mean {
        value.div(count);
        if let Some(c) = compensation.as_mut() {
            c.div(count);
        }
    }
    if let Some(c) = compensation {
        value.add(&c)?;
    }
    // The result was derived from its inputs, so it carries no start timestamp of its own, like
    // every other operation that builds one. Clearing it here covers the fold that added nothing:
    // the first histogram of a window becomes the result as it arrived, and the adds above are
    // what drop the field otherwise, which would make the payload depend on the sample count.
    value.start_timestamp = None;
    Ok((Some(value), custom_reconciled))
}
