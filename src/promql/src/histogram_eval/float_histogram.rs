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

//! Arithmetic on owned native histograms, following Prometheus `FloatHistogram`
//! (`model/histogram/float_histogram.go`): `Add`, `KahanAdd`, `Sub`, `Mul`, `Div`, `Compact`,
//! `CopyToSchema`, `DetectReset` and `Equals`, including zero bucket reconciliation, resolution
//! reduction and the intersection of mismatched custom bounds.
//!
//! Each side is a list of `(bucket index, count)` sorted by index. Go splices span layouts in
//! place; the bucket indices and the order of additions to each bucket do not depend on the
//! layout, so the results are the same.

use datafusion::error::{DataFusionError, Result};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};

use crate::functions::kahan_inc;
use crate::histogram_eval::native::{CUSTOM_BUCKETS_SCHEMA, bound};

type Buckets = Vec<(i32, f64)>;

/// A native histogram with absolute bucket counts.
#[derive(Debug, Clone)]
pub(crate) struct FloatHistogram {
    pub(crate) reset_hint: CounterResetHint,
    /// The start timestamp of the sample this histogram was read from, in milliseconds.
    ///
    /// It is payload rather than a property of a computed value: [`Self::try_from_view`] carries
    /// it because the sample it reads has one and [`Self::push_to`] writes it back, while every
    /// operation that derives a histogram from its input -- [`Self::add`], [`Self::kahan_add`],
    /// [`Self::mul`], [`Self::div`], [`Self::compact`] and [`Self::copy_to_schema`], with the
    /// constructors beside them -- leaves it `None`, as Go's `FloatHistogram`, which has no such
    /// field, does. [`Self::equals`] does not compare it for the same reason.
    ///
    /// It is not cosmetic: a reader detects a counter reset from a start timestamp that falls
    /// inside the range it reads, so a sample handed on without one is a sample whose reset cannot
    /// be told from its bucket counts alone.
    pub(crate) start_timestamp: Option<i64>,
    pub(crate) schema: i32,
    pub(crate) zero_threshold: f64,
    pub(crate) zero_count: f64,
    pub(crate) count: f64,
    pub(crate) sum: f64,
    pub(crate) positive: Buckets,
    pub(crate) negative: Buckets,
    pub(crate) custom_values: Vec<f64>,
    /// The positive and negative spans of a histogram read from a view, kept until its buckets
    /// change, because Go `Equals` compares span layouts. Otherwise consecutive indices form one
    /// span, the layout Go `Compact` produces.
    layout: Option<[Vec<(i32, i32)>; 2]>,
}

/// What an addition or subtraction reports besides its result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AddOutcome {
    /// One side was a counter reset and the other was not.
    pub(crate) counter_reset_collision: bool,
    /// Custom bounds differed and the result uses their intersection.
    pub(crate) custom_bounds_reconciled: bool,
}

impl FloatHistogram {
    pub(crate) fn try_from_view(view: &HistogramView<'_>) -> Result<Self> {
        Ok(Self {
            reset_hint: view.reset_hint,
            start_timestamp: view.start_timestamp,
            schema: view.schema,
            zero_threshold: view.zero_threshold,
            zero_count: view.zero_count,
            count: view.count,
            sum: view.sum,
            positive: side_from_view(view.positive)?,
            negative: side_from_view(view.negative)?,
            custom_values: view.custom_values.to_vec(),
            layout: Some([view.positive, view.negative].map(|side| {
                side.span_offsets
                    .iter()
                    .copied()
                    .zip(side.span_lengths.iter().copied())
                    .collect()
            })),
        })
    }

    /// The bytes this histogram's own heap buffers hold, by capacity.
    ///
    /// Everything a histogram keeps besides its scalars is on the heap: the bucket lists of both
    /// sides, the custom bounds, and the span layout of a histogram read from a view, with the
    /// `[Vec; 2]` array that holds the two layout vectors. No scalar of the struct shows any of it:
    /// a histogram of 4096 positive buckets is 64 KiB of buckets, and a window that retains one
    /// histogram per grid point retains that much per point.
    ///
    /// The layout array is stored inline in the histogram, so a ledger charging
    /// `size_of::<FloatHistogram>()` per slot already pays for those 48 bytes. Counting the whole
    /// footprint as one figure here costs a header of over-estimate on a reservation, against a
    /// payload of tens of kilobytes that would otherwise be missing from it entirely.
    pub(crate) fn heap_bytes(&self) -> usize {
        fn buckets(side: &Buckets) -> usize {
            side.capacity() * std::mem::size_of::<(i32, f64)>()
        }
        let layout = self.layout.as_ref().map_or(0, |sides| {
            std::mem::size_of::<[Vec<(i32, i32)>; 2]>().saturating_add(
                sides
                    .iter()
                    .map(|side| side.capacity() * std::mem::size_of::<(i32, i32)>())
                    .fold(0, usize::saturating_add),
            )
        });
        buckets(&self.positive)
            .saturating_add(buckets(&self.negative))
            .saturating_add(
                self.custom_values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<f64>()),
            )
            .saturating_add(layout)
    }

    /// The spans of each side.
    fn spans(&self) -> [Vec<(i32, i32)>; 2] {
        self.layout
            .clone()
            .unwrap_or_else(|| [&self.positive, &self.negative].map(canonical_spans))
    }

    /// Appends the histogram to `builder`; consecutive bucket indices share a span.
    pub(crate) fn push_to(&self, builder: &mut HistogramBatchBuilder) -> Result<u32> {
        let [positive_spans, negative_spans] = self.spans();
        let (positive_offsets, positive_lengths): (Vec<_>, Vec<_>) =
            positive_spans.into_iter().unzip();
        let (negative_offsets, negative_lengths): (Vec<_>, Vec<_>) =
            negative_spans.into_iter().unzip();
        let positive_counts: Vec<f64> = self.positive.iter().map(|&(_, count)| count).collect();
        let negative_counts: Vec<f64> = self.negative.iter().map(|&(_, count)| count).collect();
        builder
            .push(HistogramView {
                schema: self.schema,
                zero_threshold: self.zero_threshold,
                sum: self.sum,
                count: self.count,
                zero_count: self.zero_count,
                reset_hint: self.reset_hint,
                start_timestamp: self.start_timestamp,
                custom_values: &self.custom_values,
                positive: BucketSideView {
                    span_offsets: &positive_offsets,
                    span_lengths: &positive_lengths,
                    buckets: &positive_counts,
                },
                negative: BucketSideView {
                    span_offsets: &negative_offsets,
                    span_lengths: &negative_lengths,
                    buckets: &negative_counts,
                },
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    /// A histogram with no observations and the bucket layout of `self`, like Go's
    /// `&FloatHistogram{Schema: h.Schema, CustomValues: h.CustomValues}`.
    pub(crate) fn empty_like(&self) -> Self {
        Self {
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            schema: self.schema,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive: Vec::new(),
            negative: Vec::new(),
            custom_values: self.custom_values.clone(),
            layout: None,
        }
    }

    pub(crate) fn uses_custom_buckets(&self) -> bool {
        self.schema == CUSTOM_BUCKETS_SCHEMA
    }

    /// Go `Mul`.
    pub(crate) fn mul(&mut self, factor: f64) {
        // A result this operation derived, so it carries no start timestamp of its own.
        self.start_timestamp = None;
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for (_, count) in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            *count *= factor;
        }
        if factor < 0.0 {
            self.reset_hint = CounterResetHint::Gauge;
        }
    }

    /// Go `Div`; dividing by zero removes every bucket.
    pub(crate) fn div(&mut self, scalar: f64) {
        // A result this operation derived, so it carries no start timestamp of its own.
        self.start_timestamp = None;
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.positive.clear();
            self.negative.clear();
            self.layout = None;
            return;
        }
        for (_, count) in self.positive.iter_mut().chain(self.negative.iter_mut()) {
            *count /= scalar;
        }
        if scalar < 0.0 {
            self.reset_hint = CounterResetHint::Gauge;
        }
    }

    /// Go `HasOverflow`: whether any count, the sum or a custom bound is infinite.
    pub(crate) fn has_overflow(&self) -> bool {
        [self.zero_count, self.count, self.sum]
            .into_iter()
            .chain(
                self.positive
                    .iter()
                    .chain(&self.negative)
                    .map(|&(_, count)| count),
            )
            .chain(self.custom_values.iter().copied())
            .any(f64::is_infinite)
    }

    /// Go `Compact(0)`: drops empty buckets.
    pub(crate) fn compact(&mut self) {
        // A result this operation derived, so it carries no start timestamp of its own.
        self.start_timestamp = None;
        self.positive.retain(|&(_, count)| count != 0.0);
        self.negative.retain(|&(_, count)| count != 0.0);
        self.layout = None;
    }

    /// Go `CopyToSchema`; `target` must not exceed the schema.
    pub(crate) fn copy_to_schema(&self, target: i32) -> Result<Self> {
        if target == self.schema {
            // The copy is a histogram this call produced, like the reduced one below: a start
            // timestamp belongs to the sample that carried it, not to a value derived from it.
            return Ok(Self {
                start_timestamp: None,
                ..self.clone()
            });
        }
        if self.uses_custom_buckets() || target == CUSTOM_BUCKETS_SCHEMA || target > self.schema {
            return Err(incompatible_schema());
        }
        Ok(Self {
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            schema: target,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            positive: reduce_resolution(&self.positive, self.schema, target),
            negative: reduce_resolution(&self.negative, self.schema, target),
            custom_values: Vec::new(),
            layout: None,
        })
    }

    /// Go `Add`.
    pub(crate) fn add(&mut self, other: &Self) -> Result<AddOutcome> {
        self.add_or_sub(other, false)
    }

    /// Go `Sub`. The `-` operator also sets the hint to gauge, which is left to the caller.
    pub(crate) fn sub(&mut self, other: &Self) -> Result<AddOutcome> {
        self.add_or_sub(other, true)
    }

    fn add_or_sub(&mut self, other: &Self, subtract: bool) -> Result<AddOutcome> {
        check_schemas(self, other)?;
        self.layout = None;
        // A result this operation derived, so it carries no start timestamp of its own.
        self.start_timestamp = None;
        let mut outcome = AddOutcome {
            counter_reset_collision: self.adjust_counter_reset(other),
            custom_bounds_reconciled: false,
        };
        let sign = if subtract { -1.0 } else { 1.0 };
        if !self.uses_custom_buckets() {
            let (other_zero_count, _) = self.reconcile_zero_buckets(other, None)?;
            if subtract {
                self.zero_count -= other_zero_count;
            } else {
                self.zero_count += other_zero_count;
            }
        }
        if subtract {
            self.count -= other.count;
            self.sum -= other.sum;
        } else {
            self.count += other.count;
            self.sum += other.sum;
        }

        if self.uses_custom_buckets() {
            if self.custom_values == other.custom_values {
                add_buckets(self, &other.positive, sign, None, None, true)?;
            } else {
                outcome.custom_bounds_reconciled = true;
                let bounds = intersect_custom_bounds(&self.custom_values, &other.custom_values);
                (self.positive, _) = add_custom_buckets_with_mismatches(
                    subtract,
                    &self.positive,
                    &self.custom_values,
                    &other.positive,
                    &other.custom_values,
                    None,
                    &bounds,
                );
                self.custom_values = bounds;
            }
            return Ok(outcome);
        }

        let reduced;
        let (other_positive, other_negative) = if other.schema < self.schema {
            self.positive = reduce_resolution(&self.positive, self.schema, other.schema);
            self.negative = reduce_resolution(&self.negative, self.schema, other.schema);
            self.schema = other.schema;
            (&other.positive, &other.negative)
        } else if other.schema > self.schema {
            reduced = [&other.positive, &other.negative]
                .map(|side| reduce_resolution(side, other.schema, self.schema));
            (&reduced[0], &reduced[1])
        } else {
            (&other.positive, &other.negative)
        };
        add_buckets(self, other_positive, sign, None, None, true)?;
        add_buckets(self, other_negative, sign, None, None, false)?;
        Ok(outcome)
    }

    /// Go `KahanAdd`: adds `other` with compensation `c`, created when `None`, and returns the
    /// updated compensation.
    pub(crate) fn kahan_add(
        &mut self,
        other: &Self,
        c: Option<Self>,
    ) -> Result<(Self, AddOutcome)> {
        check_schemas(self, other)?;
        self.layout = None;
        // A result this operation derived, so it carries no start timestamp of its own.
        self.start_timestamp = None;
        let mut outcome = AddOutcome {
            counter_reset_collision: self.adjust_counter_reset(other),
            custom_bounds_reconciled: false,
        };
        let mut c = c.unwrap_or_else(|| self.new_compensation());
        if !self.uses_custom_buckets() {
            let (other_zero_count, other_c_zero_count) =
                self.reconcile_zero_buckets(other, Some(&mut c))?;
            (self.zero_count, c.zero_count) =
                kahan_inc(other_zero_count, self.zero_count, c.zero_count);
            (self.zero_count, c.zero_count) =
                kahan_inc(other_c_zero_count, self.zero_count, c.zero_count);
        }
        (self.count, c.count) = kahan_inc(other.count, self.count, c.count);
        (self.sum, c.sum) = kahan_inc(other.sum, self.sum, c.sum);

        if self.uses_custom_buckets() {
            if self.custom_values == other.custom_values {
                add_buckets(self, &other.positive, 1.0, Some(&mut c), None, true)?;
            } else {
                outcome.custom_bounds_reconciled = true;
                let bounds = intersect_custom_bounds(&self.custom_values, &other.custom_values);
                let compensation: Vec<f64> = c.positive.iter().map(|&(_, c)| c).collect();
                let (positive, positive_c) = add_custom_buckets_with_mismatches(
                    false,
                    &self.positive,
                    &self.custom_values,
                    &other.positive,
                    &other.custom_values,
                    Some(&compensation),
                    &bounds,
                );
                self.positive = positive;
                c.positive = positive_c;
                self.custom_values = bounds.clone();
                c.custom_values = bounds;
            }
            return Ok((c, outcome));
        }

        let mut other_positive = None;
        let mut other_negative = None;
        if other.schema < self.schema {
            (self.positive, c.positive) =
                kahan_reduce_resolution(&self.positive, &c.positive, self.schema, other.schema);
            (self.negative, c.negative) =
                kahan_reduce_resolution(&self.negative, &c.negative, self.schema, other.schema);
            self.schema = other.schema;
        } else if other.schema > self.schema {
            if !other.positive.is_empty() {
                let zeros = zero_compensation(&other.positive);
                other_positive = Some(kahan_reduce_resolution(
                    &other.positive,
                    &zeros,
                    other.schema,
                    self.schema,
                ));
            }
            if !other.negative.is_empty() {
                let zeros = zero_compensation(&other.negative);
                other_negative = Some(kahan_reduce_resolution(
                    &other.negative,
                    &zeros,
                    other.schema,
                    self.schema,
                ));
            }
        }
        let (positive, positive_c) = match &other_positive {
            Some((buckets, c)) => (buckets, Some(c)),
            None => (&other.positive, None),
        };
        add_buckets(self, positive, 1.0, Some(&mut c), positive_c, true)?;
        let (negative, negative_c) = match &other_negative {
            Some((buckets, c)) => (buckets, Some(c)),
            None => (&other.negative, None),
        };
        add_buckets(self, negative, 1.0, Some(&mut c), negative_c, false)?;
        c.schema = self.schema;
        c.zero_threshold = self.zero_threshold;
        Ok((c, outcome))
    }

    /// Go `newCompensationHistogram`: the layout of `self` with zero counts.
    fn new_compensation(&self) -> Self {
        Self {
            reset_hint: self.reset_hint,
            start_timestamp: None,
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive: zero_compensation(&self.positive),
            negative: if self.uses_custom_buckets() {
                Vec::new()
            } else {
                zero_compensation(&self.negative)
            },
            custom_values: self.custom_values.clone(),
            layout: None,
        }
    }

    /// Go `adjustCounterReset`.
    fn adjust_counter_reset(&mut self, other: &Self) -> bool {
        use CounterResetHint::{Gauge, Unknown};
        match (self.reset_hint, other.reset_hint) {
            (mine, theirs) if mine == theirs => false,
            (Gauge, _) => false,
            (_, Gauge) => {
                self.reset_hint = Gauge;
                false
            }
            (Unknown, _) => false,
            (_, Unknown) => {
                self.reset_hint = Unknown;
                false
            }
            _ => {
                self.reset_hint = Unknown;
                true
            }
        }
    }

    /// Go `zeroCountForLargerThreshold`: the zero count after widening the zero bucket to at
    /// least `larger_threshold`, the threshold actually needed so no bucket straddles it, and
    /// the Kahan compensation of the count.
    fn zero_count_for_larger_threshold(
        &self,
        mut larger_threshold: f64,
        c: Option<&Self>,
    ) -> Result<(f64, f64, f64)> {
        let mut c_zero_count = c.map_or(0.0, |c| c.zero_count);
        if larger_threshold == self.zero_threshold {
            return Ok((self.zero_count, larger_threshold, c_zero_count));
        }
        'outer: loop {
            // Go keeps the compensation accumulated by an abandoned pass.
            let mut zero_count = self.zero_count;
            for (position, &(idx, count)) in self.positive.iter().enumerate() {
                let (lower, upper) = (self.bound(idx - 1)?, self.bound(idx)?);
                if lower >= larger_threshold {
                    break;
                }
                (zero_count, c_zero_count) = kahan_inc(count, zero_count, c_zero_count);
                if let Some(c) = c {
                    (zero_count, c_zero_count) =
                        kahan_inc(c.positive[position].1, zero_count, c_zero_count);
                }
                if upper > larger_threshold {
                    if count != 0.0 {
                        larger_threshold = upper;
                    }
                    break;
                }
            }
            for (position, &(idx, count)) in self.negative.iter().enumerate() {
                let (lower, upper) = (-self.bound(idx)?, -self.bound(idx - 1)?);
                if upper <= -larger_threshold {
                    break;
                }
                (zero_count, c_zero_count) = kahan_inc(count, zero_count, c_zero_count);
                if let Some(c) = c {
                    (zero_count, c_zero_count) =
                        kahan_inc(c.negative[position].1, zero_count, c_zero_count);
                }
                if lower < -larger_threshold {
                    if count != 0.0 {
                        larger_threshold = -lower;
                        continue 'outer;
                    }
                    break;
                }
            }
            return Ok((zero_count, larger_threshold, c_zero_count));
        }
    }

    /// Go `trimBucketsInZeroBucket`: empties the buckets inside the zero bucket, then compacts.
    fn trim_buckets_in_zero_bucket(&mut self, c: Option<&mut Self>) -> Result<()> {
        let mut trimmed_positive = 0;
        for &(idx, _) in &self.positive {
            if self.bound(idx - 1)? >= self.zero_threshold {
                break;
            }
            trimmed_positive += 1;
        }
        let mut trimmed_negative = 0;
        for &(idx, _) in &self.negative {
            if -self.bound(idx - 1)? <= -self.zero_threshold {
                break;
            }
            trimmed_negative += 1;
        }
        for bucket in &mut self.positive[..trimmed_positive] {
            bucket.1 = 0.0;
        }
        for bucket in &mut self.negative[..trimmed_negative] {
            bucket.1 = 0.0;
        }
        match c {
            Some(c) => {
                for bucket in &mut c.positive[..trimmed_positive] {
                    bucket.1 = 0.0;
                }
                for bucket in &mut c.negative[..trimmed_negative] {
                    bucket.1 = 0.0;
                }
                compact_with(&mut self.positive, &mut c.positive);
                compact_with(&mut self.negative, &mut c.negative);
            }
            None => self.compact(),
        }
        Ok(())
    }

    /// Go `reconcileZeroBuckets`: widens whichever zero bucket is narrower until both match, and
    /// returns the zero count of `other` for that threshold with its compensation.
    fn reconcile_zero_buckets(
        &mut self,
        other: &Self,
        mut c: Option<&mut Self>,
    ) -> Result<(f64, f64)> {
        let mut other_zero_count = other.zero_count;
        let mut other_zero_threshold = other.zero_threshold;
        let mut other_c_zero_count = 0.0;
        while other_zero_threshold != self.zero_threshold {
            if self.zero_threshold > other_zero_threshold {
                (other_zero_count, other_zero_threshold, other_c_zero_count) =
                    other.zero_count_for_larger_threshold(self.zero_threshold, None)?;
            }
            if other_zero_threshold > self.zero_threshold {
                let (zero_count, threshold, c_zero_count) =
                    self.zero_count_for_larger_threshold(other_zero_threshold, c.as_deref())?;
                self.zero_count = zero_count;
                self.zero_threshold = threshold;
                if let Some(c) = c.as_deref_mut() {
                    c.zero_count = c_zero_count;
                }
                self.trim_buckets_in_zero_bucket(c.as_deref_mut())?;
            }
        }
        Ok((other_zero_count, other_c_zero_count))
    }

    fn bound(&self, idx: i32) -> Result<f64> {
        bound(idx, self.schema, &self.custom_values)
    }

    /// Go `DetectReset`: whether `self` cannot follow `previous` without a counter reset.
    pub(crate) fn detect_reset(&self, previous: &Self) -> Result<bool> {
        match self.reset_hint {
            CounterResetHint::CounterReset => return Ok(true),
            CounterResetHint::NotCounterReset => return Ok(false),
            _ => {}
        }
        if self.count < previous.count {
            return Ok(true);
        }
        if self.uses_custom_buckets() {
            if !previous.uses_custom_buckets() {
                return Ok(true);
            }
            if self.custom_values != previous.custom_values {
                return self.detect_reset_with_mismatched_custom_bounds(previous);
            }
        }
        if self.schema > previous.schema || self.zero_threshold < previous.zero_threshold {
            return Ok(true);
        }
        let (previous_zero_count, threshold, _) =
            previous.zero_count_for_larger_threshold(self.zero_threshold, None)?;
        if threshold != self.zero_threshold || self.zero_count < previous_zero_count {
            return Ok(true);
        }
        for (current, earlier) in [
            (&self.positive, &previous.positive),
            (&self.negative, &previous.negative),
        ] {
            let current = iterate(current, self.schema, self.schema, self.zero_threshold)?;
            let earlier = iterate(earlier, previous.schema, self.schema, self.zero_threshold)?;
            if side_reset(&current, &earlier) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Go `detectResetWithMismatchedCustomBounds`: compares the counts rolled up to each bound
    /// both histograms share.
    fn detect_reset_with_mismatched_custom_bounds(&self, previous: &Self) -> Result<bool> {
        let current_bounds = &self.custom_values;
        let previous_bounds = &previous.custom_values;
        let mut current = RollUp::new(self);
        let mut earlier = RollUp::new(previous);
        let (mut i, mut j) = (0, 0);
        while i <= current_bounds.len() && j <= previous_bounds.len() {
            let current_bound = current_bounds.get(i).copied().unwrap_or(f64::INFINITY);
            let previous_bound = previous_bounds.get(j).copied().unwrap_or(f64::INFINITY);
            if current_bound == previous_bound {
                let current_sum = current.sum_up_to(current_bound)?;
                let previous_sum = earlier.sum_up_to(current_bound)?;
                if current_sum < previous_sum {
                    return Ok(true);
                }
                i += 1;
                j += 1;
            } else if current_bound < previous_bound {
                i += 1;
            } else {
                j += 1;
            }
        }
        Ok(false)
    }

    /// Go `Equals`: bit equality of counts and of the bucket layout.
    pub(crate) fn equals(&self, other: &Self) -> bool {
        let same_side = |a: &Buckets, b: &Buckets| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| a.0 == b.0 && a.1.to_bits() == b.1.to_bits())
        };
        self.schema == other.schema
            && self.count.to_bits() == other.count.to_bits()
            && self.sum.to_bits() == other.sum.to_bits()
            && (!self.uses_custom_buckets() || self.custom_values == other.custom_values)
            && self.zero_threshold == other.zero_threshold
            && self.zero_count.to_bits() == other.zero_count.to_bits()
            && same_side(&self.negative, &other.negative)
            && same_side(&self.positive, &other.positive)
            && {
                let ([positive, negative], [other_positive, other_negative]) =
                    (self.spans(), other.spans());
                spans_match(&negative, &other_negative) && spans_match(&positive, &other_positive)
            }
    }
}

/// The rolling sum of custom buckets up to a bound, as Go's `rollupSumForBound` closure keeps
/// it across calls.
struct RollUp<'a> {
    histogram: &'a FloatHistogram,
    next: usize,
    started: bool,
    has_more: bool,
}

impl<'a> RollUp<'a> {
    fn new(histogram: &'a FloatHistogram) -> Self {
        Self {
            histogram,
            next: 0,
            started: false,
            has_more: false,
        }
    }

    fn sum_up_to(&mut self, bound: f64) -> Result<f64> {
        if self.started && !self.has_more {
            return Ok(0.0);
        }
        self.started = true;
        let mut sum = 0.0;
        while let Some(&(idx, count)) = self.histogram.positive.get(self.next) {
            if self.histogram.bound(idx)? > bound {
                self.has_more = true;
                return Ok(sum);
            }
            sum += count;
            self.next += 1;
        }
        self.has_more = false;
        Ok(sum)
    }
}

fn incompatible_schema() -> DataFusionError {
    DataFusionError::Execution(
        "cannot apply this operation to histograms with exponential and custom buckets".into(),
    )
}

fn check_schemas(a: &FloatHistogram, b: &FloatHistogram) -> Result<()> {
    if a.uses_custom_buckets() != b.uses_custom_buckets() {
        return Err(incompatible_schema());
    }
    Ok(())
}

/// Go `floatBucketIterator`: the buckets of `side` merged down to `target`, without the leading
/// ones whose upper bound is at most `threshold`.
fn iterate(side: &Buckets, schema: i32, target: i32, threshold: f64) -> Result<Buckets> {
    let mut out: Buckets = Vec::with_capacity(side.len());
    for &(idx, count) in side {
        let target_idx = target_index(idx, schema, target);
        match out.last_mut() {
            Some(last) if schema != target && last.0 == target_idx => last.1 += count,
            _ => out.push((target_idx, count)),
        }
    }
    // Go starts skipping only for a nonzero start value.
    if target != CUSTOM_BUCKETS_SCHEMA && threshold != 0.0 {
        let mut skipped = 0;
        for &(idx, _) in &out {
            if bound(idx, target, &[])? > threshold {
                break;
            }
            skipped += 1;
        }
        out.drain(..skipped);
    }
    Ok(out)
}

/// Go `targetIdx`.
fn target_index(idx: i32, schema: i32, target: i32) -> i32 {
    if schema == target {
        return idx;
    }
    ((idx - 1) >> (schema - target)) + 1
}

/// Go `reduceResolution` without delta encoding.
fn reduce_resolution(side: &Buckets, schema: i32, target: i32) -> Buckets {
    let mut out: Buckets = Vec::with_capacity(side.len());
    for &(idx, count) in side {
        let target_idx = target_index(idx, schema, target);
        match out.last_mut() {
            Some(last) if last.0 == target_idx => last.1 += count,
            _ => out.push((target_idx, count)),
        }
    }
    out
}

/// Go `kahanReduceResolution`.
fn kahan_reduce_resolution(
    side: &Buckets,
    compensation: &Buckets,
    schema: i32,
    target: i32,
) -> (Buckets, Buckets) {
    let mut out: Buckets = Vec::with_capacity(side.len());
    let mut out_c: Buckets = Vec::with_capacity(side.len());
    for (&(idx, count), &(_, c)) in side.iter().zip(compensation) {
        let target_idx = target_index(idx, schema, target);
        match (out.last_mut(), out_c.last_mut()) {
            (Some(last), Some(last_c)) if last.0 == target_idx => {
                (last.1, last_c.1) = kahan_inc(count, last.1, last_c.1);
                (last.1, last_c.1) = kahan_inc(c, last.1, last_c.1);
            }
            _ => {
                out.push((target_idx, count));
                out_c.push((target_idx, c));
            }
        }
    }
    (out, out_c)
}

fn zero_compensation(side: &Buckets) -> Buckets {
    side.iter().map(|&(idx, _)| (idx, 0.0)).collect()
}

/// Go `kahanCompact(0)`: drops the buckets whose count and compensation are both zero.
fn compact_with(side: &mut Buckets, compensation: &mut Buckets) {
    let keep: Vec<bool> = side
        .iter()
        .zip(compensation.iter())
        .map(|(&(_, count), &(_, c))| count != 0.0 || c != 0.0)
        .collect();
    let mut flags = keep.iter();
    side.retain(|_| *flags.next().unwrap());
    let mut flags = keep.iter();
    compensation.retain(|_| *flags.next().unwrap());
}

/// Go `addBuckets` and `kahanAddBuckets`: adds `sign * other` to one side of `h`, inserting the
/// buckets `h` lacks. Leading buckets of `other` inside the zero bucket are skipped.
fn add_buckets(
    h: &mut FloatHistogram,
    other: &Buckets,
    sign: f64,
    c: Option<&mut FloatHistogram>,
    other_c: Option<&Buckets>,
    positive: bool,
) -> Result<()> {
    let exponential = !h.uses_custom_buckets();
    let threshold = h.zero_threshold;
    let schema = h.schema;
    let side = if positive {
        &mut h.positive
    } else {
        &mut h.negative
    };
    let mut side_c = c.map(|c| {
        if positive {
            &mut c.positive
        } else {
            &mut c.negative
        }
    });
    let mut below_threshold = true;
    let mut position = 0;
    for (k, &(idx, count)) in other.iter().enumerate() {
        if below_threshold && exponential && bound(idx, schema, &[])? <= threshold {
            continue;
        }
        below_threshold = false;
        let value = count * sign;
        let value_c = other_c.map_or(0.0, |other_c| other_c[k].1) * sign;
        while position < side.len() && side[position].0 < idx {
            position += 1;
        }
        if position < side.len() && side[position].0 == idx {
            match side_c.as_deref_mut() {
                Some(side_c) => {
                    let (sum, comp) = (&mut side[position].1, &mut side_c[position].1);
                    (*sum, *comp) = kahan_inc(value, *sum, *comp);
                    if value_c != 0.0 {
                        (*sum, *comp) = kahan_inc(value_c, *sum, *comp);
                    }
                }
                None => side[position].1 += value,
            }
        } else {
            side.insert(position, (idx, value));
            if let Some(side_c) = side_c.as_deref_mut() {
                side_c.insert(position, (idx, value_c));
            }
        }
    }
    Ok(())
}

/// Go `intersectCustomBucketBounds`.
fn intersect_custom_bounds(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            out.push(a[i]);
            i += 1;
            j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// Go `addCustomBucketsWithMismatches`: maps both sides onto the intersected bounds, adding
/// with Kahan summation, and drops buckets left empty. Returns the buckets and their
/// compensation.
fn add_custom_buckets_with_mismatches(
    subtract: bool,
    a: &Buckets,
    a_bounds: &[f64],
    b: &Buckets,
    b_bounds: &[f64],
    a_compensation: Option<&[f64]>,
    bounds: &[f64],
) -> (Buckets, Buckets) {
    let mut target = vec![0.0; bounds.len() + 1];
    let mut target_c = vec![0.0; bounds.len() + 1];
    let mut map = |side: &Buckets, side_bounds: &[f64], negate: bool, c: Option<&[f64]>| {
        let mut intersect_idx = 0;
        for (k, &(idx, value)) in side.iter().enumerate() {
            let mut target_idx = target.len() - 1;
            if let Some(&source_bound) = usize::try_from(idx).ok().and_then(|i| side_bounds.get(i))
            {
                while intersect_idx < bounds.len() {
                    if bounds[intersect_idx] >= source_bound {
                        target_idx = intersect_idx;
                        break;
                    }
                    intersect_idx += 1;
                }
            }
            let (sum, comp) = (&mut target[target_idx], &mut target_c[target_idx]);
            if negate {
                (*sum, *comp) = kahan_inc(-value, *sum, *comp);
            } else {
                (*sum, *comp) = kahan_inc(value, *sum, *comp);
                if let Some(c) = c {
                    (*sum, *comp) = kahan_inc(c[k], *sum, *comp);
                }
            }
        }
    };
    map(a, a_bounds, false, a_compensation);
    map(b, b_bounds, subtract, None);
    let mut out = Vec::new();
    let mut out_c = Vec::new();
    for (i, (&value, &c)) in target.iter().zip(&target_c).enumerate() {
        if value == 0.0 && c == 0.0 {
            continue;
        }
        out.push((i as i32, value));
        out_c.push((i as i32, c));
    }
    (out, out_c)
}

/// Go `detectReset` over two sides with matching indices.
fn side_reset(current: &Buckets, previous: &Buckets) -> bool {
    let mut current = current.iter().peekable();
    for &(idx, count) in previous {
        while current.next_if(|bucket| bucket.0 < idx).is_some() {}
        match current.peek() {
            Some(&&(current_idx, current_count)) if current_idx == idx => {
                if current_count < count {
                    return true;
                }
            }
            _ if count != 0.0 => return true,
            _ => {}
        }
    }
    false
}

fn side_from_view(side: BucketSideView<'_>) -> Result<Buckets> {
    let invalid = || DataFusionError::Execution("native histogram has an invalid layout".into());
    if side.span_offsets.len() != side.span_lengths.len() {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(side.buckets.len());
    let mut counts = side.buckets.iter();
    let mut idx = 0i64;
    for (&offset, &length) in side.span_offsets.iter().zip(side.span_lengths) {
        if length < 0 {
            return Err(invalid());
        }
        idx += i64::from(offset);
        for _ in 0..length {
            let count = *counts.next().ok_or_else(invalid)?;
            out.push((i32::try_from(idx).map_err(|_| invalid())?, count));
            idx += 1;
        }
    }
    if counts.next().is_some() {
        return Err(invalid());
    }
    Ok(out)
}

/// Spans with one span per run of consecutive indices.
fn canonical_spans(side: &Buckets) -> Vec<(i32, i32)> {
    let mut spans: Vec<(i32, i32)> = Vec::new();
    let mut next = None;
    for &(idx, _) in side {
        match next {
            Some(expected) if expected == idx => spans.last_mut().unwrap().1 += 1,
            _ => spans.push((next.map_or(idx, |expected| idx - expected), 1)),
        }
        next = Some(idx + 1);
    }
    spans
}

/// Go `spansMatch`: equal layouts once zero-length spans are folded into the next span.
fn spans_match(a: &[(i32, i32)], b: &[(i32, i32)]) -> bool {
    fn fold(spans: &[(i32, i32)]) -> Vec<(i32, i32)> {
        let mut out = Vec::new();
        let mut offset = 0;
        for &(span_offset, length) in spans {
            offset += span_offset;
            if length != 0 {
                out.push((offset, length));
                offset = 0;
            }
        }
        out
    }
    fold(a) == fold(b)
}
