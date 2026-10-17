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
//! one native histogram, with the bucket iteration of `model/histogram` and the Go `math`
//! functions they call, so results match Prometheus on amd64 bit for bit.

use datafusion::error::{DataFusionError, Result};
use datatypes::series::histogram::{BucketSideView, HistogramView};

use super::bounds::EXPONENTIAL_BOUNDS;

const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

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
fn bound(idx: i32, schema: i32, custom_values: &[f64]) -> Result<f64> {
    if schema == CUSTOM_BUCKETS_SCHEMA {
        let len = custom_values.len() as i64;
        return match i64::from(idx) {
            -1 => Ok(f64::NEG_INFINITY),
            i if i == len => Ok(f64::INFINITY),
            i if (0..len).contains(&i) => Ok(custom_values[i as usize]),
            _ => Err(invalid_layout()),
        };
    }
    if schema < 0 {
        if schema < -4 {
            return Err(invalid_layout());
        }
        let exp = i64::from(idx) << -schema;
        if exp == 1024 {
            return Ok(f64::MAX);
        }
        return Ok(ldexp(1.0, exp));
    }
    let bounds = EXPONENTIAL_BOUNDS
        .get(schema as usize)
        .ok_or_else(invalid_layout)?;
    let frac = bounds[(idx & ((1 << schema) - 1)) as usize];
    let exp = i64::from(idx >> schema) + 1;
    if frac == 0.5 && exp == 1025 {
        return Ok(f64::MAX);
    }
    Ok(ldexp(frac, exp))
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
    let log_lower = log2(bucket.lower.abs());
    let log_upper = log2(bucket.upper.abs());
    if bucket.lower > 0.0 {
        return Ok(exp2(log_lower + (log_upper - log_lower) * fraction));
    }
    Ok(-exp2(
        log_upper + (log_lower - log_upper) * (1.0 - fraction),
    ))
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
    let log_lower = log2(b.lower.abs());
    let log_upper = log2(b.upper.abs());
    let log_v = log2(v.abs());
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
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
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

/// Go `math.Ldexp` for a positive normal `frac`.
fn ldexp(frac: f64, exp: i64) -> f64 {
    let bits = frac.to_bits();
    let exp = exp + ((bits >> 52) & 0x7FF) as i64 - 1023;
    if exp < -1075 {
        return 0.0;
    }
    if exp > 1023 {
        return f64::INFINITY;
    }
    let (exp, scale) = if exp < -1022 {
        (exp + 53, 1.0 / (1u64 << 53) as f64)
    } else {
        (exp, 1.0)
    };
    scale * f64::from_bits((bits & !(0x7FF << 52)) | (((exp + 1023) as u64) << 52))
}

/// Go `math.Frexp`.
fn frexp(x: f64) -> (f64, i64) {
    if x == 0.0 || x.is_infinite() || x.is_nan() {
        return (x, 0);
    }
    let (x, mut exp) = if x.abs() < f64::MIN_POSITIVE {
        (x * (1u64 << 52) as f64, -52)
    } else {
        (x, 0)
    };
    let bits = x.to_bits();
    exp += ((bits >> 52) & 0x7FF) as i64 - 1023 + 1;
    (f64::from_bits((bits & !(0x7FF << 52)) | (1022 << 52)), exp)
}

/// Go `math.Log`, whose amd64 assembly performs the same operations for normal inputs.
// Constants are copied verbatim from Go's `math` package.
#[allow(clippy::excessive_precision)]
fn log(x: f64) -> f64 {
    const LN2_HI: f64 = 6.93147180369123816490e-01;
    const LN2_LO: f64 = 1.90821492927058770002e-10;
    const L1: f64 = 6.666666666666735130e-01;
    const L2: f64 = 3.999999999940941908e-01;
    const L3: f64 = 2.857142874366239149e-01;
    const L4: f64 = 2.222219843214978396e-01;
    const L5: f64 = 1.818357216161805012e-01;
    const L6: f64 = 1.531383769920937332e-01;
    const L7: f64 = 1.479819860511658591e-01;
    if x.is_nan() || x == f64::INFINITY {
        return x;
    }
    if x < 0.0 {
        return f64::NAN;
    }
    if x == 0.0 {
        return f64::NEG_INFINITY;
    }
    let (mut f1, mut ki) = frexp(x);
    if f1 < std::f64::consts::SQRT_2 / 2.0 {
        f1 *= 2.0;
        ki -= 1;
    }
    let f = f1 - 1.0;
    let k = ki as f64;
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let t1 = s2 * (L1 + s4 * (L3 + s4 * (L5 + s4 * L7)));
    let t2 = s4 * (L2 + s4 * (L4 + s4 * L6));
    let r = t1 + t2;
    let hfsq = 0.5 * f * f;
    k * LN2_HI - ((hfsq - (s * (hfsq + r) + k * LN2_LO)) - f)
}

/// Go `math.Log2`.
fn log2(x: f64) -> f64 {
    let (frac, exp) = frexp(x);
    if frac == 0.5 {
        return (exp - 1) as f64;
    }
    log(frac) * std::f64::consts::LOG2_E + exp as f64
}

/// Go `math.Exp2`, which is not assembly on amd64.
// Constants are copied verbatim from Go's `math` package.
#[allow(clippy::excessive_precision)]
fn exp2(x: f64) -> f64 {
    const LN2_HI: f64 = 6.93147180369123816490e-01;
    const LN2_LO: f64 = 1.90821492927058770002e-10;
    const OVERFLOW: f64 = 1.0239999999999999e+03;
    const UNDERFLOW: f64 = -1.0740e+03;
    const P1: f64 = 1.66666666666666657415e-01;
    const P2: f64 = -2.77777777770155933842e-03;
    const P3: f64 = 6.61375632143793436117e-05;
    const P4: f64 = -1.65339022054652515390e-06;
    const P5: f64 = 4.13813679705723846039e-08;
    if x.is_nan() || x == f64::INFINITY {
        return x;
    }
    if x == f64::NEG_INFINITY || x < UNDERFLOW {
        return 0.0;
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }
    let k = if x > 0.0 {
        (x + 0.5) as i64
    } else if x < 0.0 {
        (x - 0.5) as i64
    } else {
        0
    };
    let t = x - k as f64;
    let hi = t * LN2_HI;
    let lo = -t * LN2_LO;
    let r = hi - lo;
    let t = r * r;
    let c = r - t * (P1 + t * (P2 + t * (P3 + t * (P4 + t * P5))));
    let y = 1.0 - ((lo - (r * c) / (2.0 - c)) - hi);
    ldexp(y, k)
}

#[cfg(test)]
mod tests {
    /// The bucket interpolation goes through Go's `math.Log2` and `math.Exp2`, which differ from
    /// the platform functions in the last bit on some arguments.
    #[test]
    fn log2_and_exp2_match_go() {
        let mut checked = 0;
        for line in include_str!("go_math_cases.txt").lines() {
            if line.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split(' ').collect();
            let bits = |field: &str| u64::from_str_radix(field, 16).unwrap();
            let argument = f64::from_bits(bits(fields[1]));
            let actual = match fields[0] {
                "log2" => super::log2(argument),
                "exp2" => super::exp2(argument),
                function => panic!("unknown function {function}"),
            };
            assert_eq!(actual.to_bits(), bits(fields[2]), "{line}");
            checked += 1;
        }
        assert_eq!(checked, 400);
    }
}
