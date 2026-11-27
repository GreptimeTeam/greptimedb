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

use crate::histogram_eval::float_histogram::FloatHistogram;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistogramAggregate {
    Sum,
    Avg,
}

/// The Kahan sum or the mean of `histograms` in order, without compaction; `None` when there is
/// none or they mix exponential and custom buckets. The first input error ends the fold. The
/// result has no start timestamp. The flag reports whether any add reconciled two different
/// custom-bucket sets, which Prometheus annotates with an info.
pub(crate) fn sum_or_avg(
    op: HistogramAggregate,
    histograms: impl IntoIterator<Item = Result<FloatHistogram>>,
) -> Result<(Option<FloatHistogram>, bool)> {
    let mut value: Option<FloatHistogram> = None;
    let mut compensation: Option<FloatHistogram> = None;
    let mut mean: Option<FloatHistogram> = None;
    let mut count = 0.0;
    let mut custom_reconciled = false;
    for h in histograms {
        let h = h?;
        count += 1.0;
        let Some(current) = value.as_mut() else {
            value = Some(h);
            continue;
        };
        // Go drops the result with a warning for this addition error only; others propagate.
        if current.uses_custom_buckets() != h.uses_custom_buckets() {
            return Ok((None, custom_reconciled));
        }
        match op {
            HistogramAggregate::Sum => {
                let (c, outcome) = current.kahan_add(&h, compensation.take())?;
                custom_reconciled |= outcome.custom_bounds_reconciled;
                compensation = Some(c);
            }
            HistogramAggregate::Avg => {
                if mean.is_none() {
                    let mut sum = current.clone();
                    let (c, outcome) = sum.kahan_add(&h, compensation.clone())?;
                    custom_reconciled |= outcome.custom_bounds_reconciled;
                    if !sum.has_overflow() {
                        *current = sum;
                        compensation = Some(c);
                        continue;
                    }
                    let mut started = current.clone();
                    started.div(count - 1.0);
                    if let Some(c) = compensation.as_mut() {
                        c.div(count - 1.0);
                    }
                    mean = Some(started);
                }
                // Go switches to an incremental mean once the plain sum overflows.
                let q = (count - 1.0) / count;
                if let Some(c) = compensation.as_mut() {
                    c.mul(q);
                }
                let mut to_add = h;
                to_add.div(count);
                let mean = mean.as_mut().expect("set above");
                mean.mul(q);
                let (c, outcome) = mean.kahan_add(&to_add, compensation.take())?;
                custom_reconciled |= outcome.custom_bounds_reconciled;
                compensation = Some(c);
            }
        }
    }
    let Some(mut value) = value else {
        return Ok((None, custom_reconciled));
    };
    match op {
        HistogramAggregate::Sum => {
            if let Some(c) = compensation {
                value.add(&c)?;
            }
        }
        HistogramAggregate::Avg => match mean {
            Some(mut mean) => {
                if let Some(c) = compensation {
                    mean.add(&c)?;
                }
                value = mean;
            }
            None => {
                value.div(count);
                if let Some(mut c) = compensation {
                    c.div(count);
                    value.add(&c)?;
                }
            }
        },
    }
    // A single-input fold returns its input unchanged, so clear the start timestamp the adds
    // would otherwise have cleared.
    value.start_timestamp = None;
    Ok((Some(value), custom_reconciled))
}
