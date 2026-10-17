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

//! Prometheus `BucketQuantile` and `BucketFraction` over the buckets of a classic histogram at
//! one point.

/// Relative difference under which neighbouring bucket counts are taken as equal.
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// Go's `strconv.ParseFloat`, which rejects finite literals out of the f64 range.
pub(super) fn parse_bound(text: &str) -> Option<f64> {
    let value: f64 = text.parse().ok()?;
    if value.is_infinite() {
        let unsigned = text.trim_start_matches(['+', '-']);
        if !unsigned.eq_ignore_ascii_case("inf") && !unsigned.eq_ignore_ascii_case("infinity") {
            return None;
        }
    }
    Some(value)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Bucket {
    pub(super) upper_bound: f64,
    pub(super) count: f64,
}

/// Prometheus `BucketQuantile` over buckets sorted by upper bound.
pub(super) fn bucket_quantile(q: f64, buckets: &mut Vec<Bucket>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    if buckets.last().unwrap().upper_bound != f64::INFINITY {
        return f64::NAN;
    }
    coalesce(buckets);
    ensure_monotonic(buckets);
    if buckets.len() < 2 {
        return f64::NAN;
    }
    let observations = buckets.last().unwrap().count;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    let b = go_search(buckets.len() - 1, |i| buckets[i].count >= rank);
    if b == buckets.len() - 1 {
        return buckets[buckets.len() - 2].upper_bound;
    }
    if b == 0 && buckets[0].upper_bound <= 0.0 {
        return buckets[0].upper_bound;
    }
    let mut bucket_start = 0.0;
    let bucket_end = buckets[b].upper_bound;
    let mut count = buckets[b].count;
    if b > 0 {
        bucket_start = buckets[b - 1].upper_bound;
        count -= buckets[b - 1].count;
        rank -= buckets[b - 1].count;
    }
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// Prometheus `BucketFraction` over buckets sorted by upper bound.
pub(super) fn bucket_fraction(lower: f64, upper: f64, buckets: &mut Vec<Bucket>) -> f64 {
    if buckets.last().unwrap().upper_bound != f64::INFINITY {
        return f64::NAN;
    }
    coalesce(buckets);
    let count = buckets.last().unwrap().count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let (mut rank, mut lower_rank, mut upper_rank) = (0.0, 0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);
    let mut lower_bound = if buckets[0].upper_bound <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    for (i, bucket) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].upper_bound;
        }
        let upper_bound = bucket.upper_bound;
        // An infinite-width bucket contributes nothing to the interpolation.
        let interpolate = |value: f64| {
            if lower_bound == f64::NEG_INFINITY {
                bucket.count
            } else {
                rank + (bucket.count - rank) * (value - lower_bound) / (upper_bound - lower_bound)
            }
        };
        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank = interpolate(lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank = interpolate(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = bucket.count;
    }
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    (upper_rank - lower_rank) / count
}

/// Merges buckets with the same upper bound.
fn coalesce(buckets: &mut Vec<Bucket>) {
    let mut last = buckets[0];
    let mut i = 0;
    for j in 1..buckets.len() {
        let bucket = buckets[j];
        if bucket.upper_bound == last.upper_bound {
            last.count += bucket.count;
        } else {
            buckets[i] = last;
            last = bucket;
            i += 1;
        }
    }
    buckets[i] = last;
    buckets.truncate(i + 1);
}

/// Prometheus `ensureMonotonicAndIgnoreSmallDeltas`: counts that differ from the previous one by
/// a negligible relative amount, or are lower, are raised to it.
fn ensure_monotonic(buckets: &mut [Bucket]) {
    let mut prev = buckets[0].count;
    for bucket in &mut buckets[1..] {
        let curr = bucket.count;
        if curr == prev {
            continue;
        }
        if almost_equal(prev, curr, SMALL_DELTA_TOLERANCE) || curr < prev {
            bucket.count = prev;
            continue;
        }
        prev = curr;
    }
}

/// Prometheus `almost.Equal`.
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < epsilon * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// Go's `sort.Search`: the smallest index in `[0, n)` where `f` holds, or `n`, found by the same
/// bisection, so a predicate that is not monotonic (NaN counts) picks the same index.
fn go_search(n: usize, f: impl Fn(usize) -> bool) -> usize {
    let (mut i, mut j) = (0, n);
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}
