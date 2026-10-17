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

//! Prometheus `HistogramQuantile`, `HistogramFraction` and the `histogramVariance` estimate over
//! one native histogram, with the bucket iteration of `model/histogram`.

use datafusion::error::{DataFusionError, Result};
use datatypes::series::histogram::{BucketSideView, HistogramView};

pub(super) const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

#[derive(Debug, Clone, Copy)]
pub(super) struct NativeBucket {
    lower: f64,
    upper: f64,
    count: f64,
}

/// The buckets of `h` from the lowest to the highest, or the reverse: negative buckets, the zero
/// bucket when it has observations, then positive buckets (Go `AllBucketIterator` and
/// `AllReverseBucketIterator`). Buckets overlapping the zero bucket are clipped to it.
pub(super) fn all_buckets(
    h: &HistogramView<'_>,
    reverse: bool,
    out: &mut Vec<NativeBucket>,
) -> Result<()> {
    out.clear();
    let (low, high) = if reverse {
        (h.positive, h.negative)
    } else {
        (h.negative, h.positive)
    };
    // The first side runs from its highest index down, the second from its lowest index up.
    side(h, low, reverse, out)?;
    out.reverse();
    if h.zero_count > 0.0 {
        if h.schema == CUSTOM_BUCKETS_SCHEMA {
            return Err(DataFusionError::Execution(
                "histograms with custom buckets have no zero bucket".into(),
            ));
        }
        out.push(NativeBucket {
            lower: -h.zero_threshold,
            upper: h.zero_threshold,
            count: h.zero_count,
        });
    }
    side(h, high, !reverse, out)?;
    for bucket in out.iter_mut() {
        if bucket.upper < 0.0 && bucket.upper > -h.zero_threshold {
            bucket.upper = -h.zero_threshold;
        } else if bucket.lower > 0.0 && bucket.lower < h.zero_threshold {
            bucket.lower = h.zero_threshold;
        }
    }
    Ok(())
}

/// Appends the buckets of one side by increasing index.
fn side(
    h: &HistogramView<'_>,
    side: BucketSideView<'_>,
    positive: bool,
    out: &mut Vec<NativeBucket>,
) -> Result<()> {
    if side.span_offsets.len() != side.span_lengths.len() {
        return Err(invalid_layout());
    }
    let mut counts = side.buckets.iter();
    let mut index = 0i64;
    for (&offset, &length) in side.span_offsets.iter().zip(side.span_lengths) {
        if length < 0 {
            return Err(invalid_layout());
        }
        index += i64::from(offset);
        for _ in 0..length {
            let count = *counts.next().ok_or_else(invalid_layout)?;
            let idx = i32::try_from(index).map_err(|_| invalid_layout())?;
            out.push(bucket(h, idx, count, positive)?);
            index += 1;
        }
    }
    if counts.next().is_some() {
        return Err(invalid_layout());
    }
    Ok(())
}

fn bucket(h: &HistogramView<'_>, idx: i32, count: f64, positive: bool) -> Result<NativeBucket> {
    let bound = |idx: i32| bound(idx, h.schema, h.custom_values);
    Ok(if positive {
        NativeBucket {
            lower: bound(idx - 1)?,
            upper: bound(idx)?,
            count,
        }
    } else {
        NativeBucket {
            lower: -bound(idx)?,
            upper: -bound(idx - 1)?,
            count,
        }
    })
}

/// Go `getBound`.
pub(super) fn bound(idx: i32, schema: i32, custom_values: &[f64]) -> Result<f64> {
    if schema == CUSTOM_BUCKETS_SCHEMA {
        let len = custom_values.len() as i64;
        return match i64::from(idx) {
            -1 => Ok(f64::NEG_INFINITY),
            i if i == len => Ok(f64::INFINITY),
            i if (0..len).contains(&i) => Ok(custom_values[i as usize]),
            _ => Err(invalid_layout()),
        };
    }
    if !(-4..=8).contains(&schema) {
        return Err(invalid_layout());
    }
    // The upper bound of bucket `idx` is 2^(idx / 2^schema).
    let exponent = f64::from(idx) * 2f64.powi(-schema);
    if exponent == 1024.0 {
        // The last bucket of finite values; its computed bound would overflow to +Inf, which
        // Go reserves for the bucket of infinite observations.
        return Ok(f64::MAX);
    }
    Ok(exponent.exp2())
}

fn invalid_layout() -> DataFusionError {
    DataFusionError::Execution("native histogram has an invalid bucket layout".into())
}

/// Prometheus `HistogramQuantile`.
pub(super) fn quantile(
    q: f64,
    h: &HistogramView<'_>,
    buckets: &mut Vec<NativeBucket>,
) -> Result<f64> {
    if q < 0.0 {
        return Ok(f64::NEG_INFINITY);
    }
    if q > 1.0 {
        return Ok(f64::INFINITY);
    }
    if h.count == 0.0 || q.is_nan() {
        return Ok(f64::NAN);
    }
    let custom = h.schema == CUSTOM_BUCKETS_SCHEMA;
    let forward = h.sum.is_nan() || q < 0.5;
    all_buckets(h, !forward, buckets)?;
    let mut rank = if forward {
        q * h.count
    } else {
        (1.0 - q) * h.count
    };
    // Go keeps the last bucket visited, empty or not, and a zero bucket when there is none.
    let mut bucket = NativeBucket {
        lower: 0.0,
        upper: 0.0,
        count: 0.0,
    };
    let mut count = 0.0;
    let mut visited = 0;
    for &next in buckets.iter() {
        visited += 1;
        bucket = next;
        if bucket.count == 0.0 {
            continue;
        }
        count += bucket.count;
        if count >= rank {
            break;
        }
    }
    if !custom && bucket.lower < 0.0 && bucket.upper > 0.0 {
        if h.negative.buckets.is_empty() && !h.positive.buckets.is_empty() {
            bucket.lower = 0.0;
        } else if h.positive.buckets.is_empty() && !h.negative.buckets.is_empty() {
            bucket.upper = 0.0;
        }
    } else if custom {
        if bucket.lower == f64::NEG_INFINITY {
            if bucket.upper <= 0.0 {
                return Ok(bucket.upper);
            }
            bucket.lower = 0.0;
        } else if bucket.upper == f64::INFINITY {
            return Ok(bucket.lower);
        }
    }
    if count > h.count {
        count = h.count;
    }
    if count < rank {
        return Ok(if h.sum.is_nan() {
            f64::NAN
        } else {
            bucket.upper
        });
    }
    if forward {
        rank -= count - bucket.count;
    } else {
        rank = count - rank;
    }
    if h.sum.is_nan() {
        // Prometheus walks the remaining buckets to annotate NaN observations and leaves the last
        // one in `bucket`, which the interpolation below then uses.
        for &next in &buckets[visited..] {
            bucket = next;
        }
    }
    let fraction = rank / bucket.count;
    if custom || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return Ok(bucket.lower + (bucket.upper - bucket.lower) * fraction);
    }
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    if bucket.lower > 0.0 {
        return Ok((log_lower + (log_upper - log_lower) * fraction).exp2());
    }
    Ok(-(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2())
}

/// Prometheus `HistogramFraction`.
pub(super) fn fraction(
    lower: f64,
    upper: f64,
    h: &HistogramView<'_>,
    buckets: &mut Vec<NativeBucket>,
) -> Result<f64> {
    if h.count == 0.0 || lower.is_nan() || upper.is_nan() {
        return Ok(f64::NAN);
    }
    if lower >= upper {
        return Ok(0.0);
    }
    let custom = h.schema == CUSTOM_BUCKETS_SCHEMA;
    all_buckets(h, false, buckets)?;
    let (mut count, mut rank, mut lower_rank, mut upper_rank) = (0.0, 0.0, 0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);
    let mut visited = 0;
    for b in buckets.iter() {
        visited += 1;
        let mut b = *b;
        count += b.count;
        let mut zero_bucket = false;
        if b.lower <= 0.0 && b.upper >= 0.0 {
            zero_bucket = true;
            if h.negative.buckets.is_empty() && !h.positive.buckets.is_empty() {
                b.lower = 0.0;
            } else if h.positive.buckets.is_empty() && !h.negative.buckets.is_empty() {
                b.upper = 0.0;
            }
        }
        let interpolate = |v: f64| {
            if custom || zero_bucket {
                if b.lower == f64::NEG_INFINITY {
                    b.count
                } else {
                    rank + b.count * ((v - b.lower) / (b.upper - b.lower))
                }
            } else {
                rank + b.count * fraction_below_exponential(b, v)
            }
        };
        if !lower_set && b.lower >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && b.lower >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && b.lower < lower && b.upper > lower {
            lower_rank = interpolate(lower);
            lower_set = true;
        }
        if !upper_set && b.lower < upper && b.upper > upper {
            upper_rank = interpolate(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank += b.count;
    }
    if h.sum.is_nan() {
        for b in &buckets[visited..] {
            count += b.count;
        }
    } else {
        count = h.count;
    }
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    Ok((upper_rank - lower_rank) / h.count)
}

/// Go `Bucket.FractionBelow` with exponential interpolation.
fn fraction_below_exponential(b: NativeBucket, v: f64) -> f64 {
    let log_lower = b.lower.abs().log2();
    let log_upper = b.upper.abs().log2();
    let log_v = v.abs().log2();
    if v > 0.0 {
        (log_v - log_lower) / (log_upper - log_lower)
    } else {
        1.0 - ((log_v - log_upper) / (log_lower - log_upper))
    }
}

/// Prometheus `histogramVariance`: bucket midpoints weighted by count around the mean.
pub(super) fn variance(h: &HistogramView<'_>, buckets: &mut Vec<NativeBucket>) -> Result<f64> {
    let mean = h.sum / h.count;
    let custom = h.schema == CUSTOM_BUCKETS_SCHEMA;
    all_buckets(h, false, buckets)?;
    let (mut variance, mut compensation) = (0.0, 0.0);
    for bucket in buckets.iter().filter(|bucket| bucket.count != 0.0) {
        let value = if custom {
            (bucket.upper + bucket.lower) / 2.0
        } else if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
            0.0
        } else {
            let value = (bucket.upper * bucket.lower).sqrt();
            if bucket.upper < 0.0 { -value } else { value }
        };
        let delta = value - mean;
        (variance, compensation) = kahan_inc(bucket.count * delta * delta, variance, compensation);
    }
    Ok((variance + compensation) / h.count)
}

/// Prometheus `kahansum.Inc`.
pub(super) fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    if t.is_infinite() {
        return (t, 0.0);
    }
    let c = if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, c)
}

#[cfg(test)]
mod tests {
    /// The last bucket of finite values ends at `f64::MAX`; only the next one, which counts
    /// infinite observations, ends at +Inf.
    #[test]
    fn last_finite_bucket_ends_at_max() {
        for schema in [-4, 0, 3, 8] {
            let last = 1024i32 << schema.max(0) >> (-schema).max(0);
            assert_eq!(super::bound(last, schema, &[]).unwrap(), f64::MAX);
            assert_eq!(super::bound(last + 1, schema, &[]).unwrap(), f64::INFINITY);
        }
    }
}
