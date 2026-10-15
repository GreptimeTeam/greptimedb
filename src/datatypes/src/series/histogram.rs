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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Float64Array, Float64Builder, Int32Array, Int32Builder,
    Int64Array, Int64Builder, ListArray, ListBuilder, PrimitiveArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Fields, Float64Type, Int32Type};
use arrow::error::ArrowError;

use crate::Result;
use crate::error::InvalidSeriesSnafu;

/// Reset hint retained by native histograms, including values unknown to this version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterResetHint {
    /// The payload must be inspected to determine whether a reset occurred.
    Unknown,
    /// This is the first histogram after a counter reset.
    CounterReset,
    /// No counter reset occurred since the previous histogram.
    NotCounterReset,
    /// This is a gauge histogram, so counter resets do not apply.
    Gauge,
    /// A value not understood by this version.
    Unrecognized(i32),
}
impl From<i32> for CounterResetHint {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::CounterReset,
            2 => Self::NotCounterReset,
            3 => Self::Gauge,
            v => Self::Unrecognized(v),
        }
    }
}
impl From<CounterResetHint> for i32 {
    fn from(value: CounterResetHint) -> Self {
        match value {
            CounterResetHint::Unknown => 0,
            CounterResetHint::CounterReset => 1,
            CounterResetHint::NotCounterReset => 2,
            CounterResetHint::Gauge => 3,
            CounterResetHint::Unrecognized(v) => v,
        }
    }
}
pub(crate) fn fields() -> Fields {
    let list = |t| DataType::List(Arc::new(Field::new("item", t, false)));
    vec![
        Field::new("schema", DataType::Int32, false),
        Field::new("zero_threshold", DataType::Float64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("count", DataType::Float64, false),
        Field::new("zero_count", DataType::Float64, false),
        Field::new("reset_hint", DataType::Int32, false),
        Field::new("start_timestamp", DataType::Int64, true),
        Field::new("custom_values", list(DataType::Float64), false),
        Field::new("positive_span_offsets", list(DataType::Int32), false),
        Field::new("positive_span_lengths", list(DataType::Int32), false),
        Field::new("positive_buckets", list(DataType::Float64), false),
        Field::new("negative_span_offsets", list(DataType::Int32), false),
        Field::new("negative_span_lengths", list(DataType::Int32), false),
        Field::new("negative_buckets", list(DataType::Float64), false),
    ]
    .into()
}
fn invalid<T>(reason: impl Into<String>) -> Result<T> {
    Err(InvalidSeriesSnafu {
        reason: reason.into(),
    }
    .build())
}
fn arrow_result<T>(r: std::result::Result<T, ArrowError>) -> Result<T> {
    r.map_err(|e| {
        InvalidSeriesSnafu {
            reason: format!("invalid histogram Arrow array: {e}"),
        }
        .build()
    })
}

/// A borrowed set of spans and absolute bucket counts.
#[derive(Clone, Copy, Debug)]
pub struct BucketSideView<'a> {
    /// Span offsets.
    pub span_offsets: &'a [i32],
    /// Span lengths.
    pub span_lengths: &'a [i32],
    /// Absolute bucket counts.
    pub buckets: &'a [f64],
}
/// A borrowed histogram row.
#[derive(Clone, Copy, Debug)]
pub struct HistogramView<'a> {
    /// Histogram schema.
    pub schema: i32,
    /// Zero bucket threshold.
    pub zero_threshold: f64,
    /// Sum of observations.
    pub sum: f64,
    /// Total count.
    pub count: f64,
    /// Count in the zero bucket.
    pub zero_count: f64,
    /// Counter reset hint.
    pub reset_hint: CounterResetHint,
    /// Optional start timestamp in milliseconds.
    pub start_timestamp: Option<i64>,
    /// Custom bucket bounds.
    pub custom_values: &'a [f64],
    /// Positive bucket side.
    pub positive: BucketSideView<'a>,
    /// Negative bucket side.
    pub negative: BucketSideView<'a>,
}

#[derive(Debug)]
struct HistogramList<T: ArrowPrimitiveType> {
    offsets: OffsetBuffer<i32>,
    values: PrimitiveArray<T>,
}
impl<T: ArrowPrimitiveType> HistogramList<T> {
    fn try_new(a: &ListArray) -> Result<Self> {
        let v = a
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                InvalidSeriesSnafu {
                    reason: "histogram list item type mismatch".to_string(),
                }
                .build()
            })?;
        if a.null_count() != 0 || v.null_count() != 0 {
            return invalid("histogram list rows and items must not be null");
        }
        Ok(Self {
            offsets: a.offsets().clone(),
            values: v.clone(),
        })
    }
    fn view(&self, row: usize) -> &[T::Native] {
        let s = self.offsets[row] as usize;
        let e = self.offsets[row + 1] as usize;
        &self.values.values()[s..e]
    }
}
/// Columnar owner of native histogram rows.
#[derive(Debug)]
pub struct HistogramBatch {
    array: StructArray,
    schema: Int32Array,
    threshold: Float64Array,
    sum: Float64Array,
    count: Float64Array,
    zero: Float64Array,
    hint: Int32Array,
    start: Int64Array,
    custom: HistogramList<Float64Type>,
    po: HistogramList<Int32Type>,
    pl: HistogramList<Int32Type>,
    pb: HistogramList<Float64Type>,
    no: HistogramList<Int32Type>,
    nl: HistogramList<Int32Type>,
    nb: HistogramList<Float64Type>,
}
impl HistogramBatch {
    /// Validates and wraps an Arrow struct with the required ordered schema.
    pub fn try_from_arrow(array: StructArray) -> Result<Self> {
        if array.fields() != &fields() {
            return invalid("histogram fields, order, types, or nullability mismatch");
        }
        if array.null_count() != 0 {
            return invalid("histogram struct rows must not be null");
        }
        let c = array.columns();
        if c.iter()
            .enumerate()
            .any(|(i, a)| i != 6 && a.null_count() != 0)
        {
            return invalid("non-nullable histogram column contains null");
        }
        macro_rules! col {
            ($i:expr,$t:ty) => {
                c[$i].as_any().downcast_ref::<$t>().cloned().ok_or_else(|| {
                    InvalidSeriesSnafu {
                        reason: format!("histogram column {} type mismatch", $i),
                    }
                    .build()
                })
            };
        }
        macro_rules! list {
            ($i:expr,$t:ty) => {{
                let a = c[$i].as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    InvalidSeriesSnafu {
                        reason: format!("histogram list column {} type mismatch", $i),
                    }
                    .build()
                })?;
                HistogramList::<$t>::try_new(a)?
            }};
        }
        let batch = Self {
            schema: col!(0, Int32Array)?,
            threshold: col!(1, Float64Array)?,
            sum: col!(2, Float64Array)?,
            count: col!(3, Float64Array)?,
            zero: col!(4, Float64Array)?,
            hint: col!(5, Int32Array)?,
            start: col!(6, Int64Array)?,
            custom: list!(7, Float64Type),
            po: list!(8, Int32Type),
            pl: list!(9, Int32Type),
            pb: list!(10, Float64Type),
            no: list!(11, Int32Type),
            nl: list!(12, Int32Type),
            nb: list!(13, Float64Type),
            array,
        };
        for i in 0..batch.len() {
            validate_spans(batch.po.view(i), batch.pl.view(i), batch.pb.view(i).len())?;
            validate_spans(batch.no.view(i), batch.nl.view(i), batch.nb.view(i).len())?
        }
        Ok(batch)
    }
    /// Returns the owning Arrow struct array.
    pub fn as_arrow(&self) -> &StructArray {
        &self.array
    }
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.array.len()
    }
    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }
    /// Returns a borrowed row, or `None` when out of bounds.
    pub fn view(&self, i: usize) -> Option<HistogramView<'_>> {
        if i >= self.len() {
            return None;
        }
        Some(HistogramView {
            schema: self.schema.value(i),
            zero_threshold: self.threshold.value(i),
            sum: self.sum.value(i),
            count: self.count.value(i),
            zero_count: self.zero.value(i),
            reset_hint: self.hint.value(i).into(),
            start_timestamp: self.start.is_valid(i).then(|| self.start.value(i)),
            custom_values: self.custom.view(i),
            positive: BucketSideView {
                span_offsets: self.po.view(i),
                span_lengths: self.pl.view(i),
                buckets: self.pb.view(i),
            },
            negative: BucketSideView {
                span_offsets: self.no.view(i),
                span_lengths: self.nl.view(i),
                buckets: self.nb.view(i),
            },
        })
    }
}
fn validate_spans(offsets: &[i32], lengths: &[i32], buckets: usize) -> Result<()> {
    if offsets.len() != lengths.len() {
        return invalid("span offsets and lengths differ");
    }
    let mut cursor = 0i32;
    let mut total = 0usize;
    for (i, (&offset, &length)) in offsets.iter().zip(lengths).enumerate() {
        if length < 0 || (i > 0 && offset < 0) {
            return invalid("negative span length or noninitial offset");
        }
        if i == 0 {
            cursor = offset
        } else {
            cursor = cursor.checked_add(offset).ok_or_else(|| {
                InvalidSeriesSnafu {
                    reason: "span index overflow".to_string(),
                }
                .build()
            })?
        }
        total = total.checked_add(length as usize).ok_or_else(|| {
            InvalidSeriesSnafu {
                reason: "bucket count overflow".to_string(),
            }
            .build()
        })?;
        cursor = cursor.checked_add(length).ok_or_else(|| {
            InvalidSeriesSnafu {
                reason: "span cursor overflow".to_string(),
            }
            .build()
        })?;
    }
    if total != buckets {
        return invalid("span lengths do not match bucket count");
    }
    Ok(())
}

/// Builds a histogram batch directly in Arrow builders.
pub struct HistogramBatchBuilder {
    schema: Int32Builder,
    threshold: Float64Builder,
    sum: Float64Builder,
    count: Float64Builder,
    zero: Float64Builder,
    hint: Int32Builder,
    start: Int64Builder,
    custom: ListBuilder<Float64Builder>,
    po: ListBuilder<Int32Builder>,
    pl: ListBuilder<Int32Builder>,
    pb: ListBuilder<Float64Builder>,
    no: ListBuilder<Int32Builder>,
    nl: ListBuilder<Int32Builder>,
    nb: ListBuilder<Float64Builder>,
    rows: usize,
}
impl Default for HistogramBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl HistogramBatchBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self {
            schema: Int32Builder::new(),
            threshold: Float64Builder::new(),
            sum: Float64Builder::new(),
            count: Float64Builder::new(),
            zero: Float64Builder::new(),
            hint: Int32Builder::new(),
            start: Int64Builder::new(),
            custom: lb64(),
            po: lb32(),
            pl: lb32(),
            pb: lb64(),
            no: lb32(),
            nl: lb32(),
            nb: lb64(),
            rows: 0,
        }
    }
    /// Validates and appends a row, returning its index without staging bucket vectors.
    pub fn push(&mut self, r: HistogramView<'_>) -> Result<u32> {
        preflight(
            r,
            self.rows,
            [
                self.custom.values_ref().len(),
                self.po.values_ref().len(),
                self.pl.values_ref().len(),
                self.pb.values_ref().len(),
                self.no.values_ref().len(),
                self.nl.values_ref().len(),
                self.nb.values_ref().len(),
            ],
        )?;
        let index = u32::try_from(self.rows).map_err(|_| {
            InvalidSeriesSnafu {
                reason: "histogram row index overflow".to_string(),
            }
            .build()
        })?;
        self.schema.append_value(r.schema);
        self.threshold.append_value(r.zero_threshold);
        self.sum.append_value(r.sum);
        self.count.append_value(r.count);
        self.zero.append_value(r.zero_count);
        self.hint.append_value(r.reset_hint.into());
        if let Some(v) = r.start_timestamp {
            self.start.append_value(v)
        } else {
            self.start.append_null()
        }
        append64(&mut self.custom, r.custom_values);
        append32(&mut self.po, r.positive.span_offsets);
        append32(&mut self.pl, r.positive.span_lengths);
        append64(&mut self.pb, r.positive.buckets);
        append32(&mut self.no, r.negative.span_offsets);
        append32(&mut self.nl, r.negative.span_lengths);
        append64(&mut self.nb, r.negative.buckets);
        self.rows += 1;
        Ok(index)
    }
    /// Finishes by transferring builder buffers to the batch.
    pub fn finish(mut self) -> Result<Arc<HistogramBatch>> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.schema.finish()),
            Arc::new(self.threshold.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.count.finish()),
            Arc::new(self.zero.finish()),
            Arc::new(self.hint.finish()),
            Arc::new(self.start.finish()),
            Arc::new(self.custom.finish()),
            Arc::new(self.po.finish()),
            Arc::new(self.pl.finish()),
            Arc::new(self.pb.finish()),
            Arc::new(self.no.finish()),
            Arc::new(self.nl.finish()),
            Arc::new(self.nb.finish()),
        ];
        Ok(Arc::new(HistogramBatch::try_from_arrow(arrow_result(
            StructArray::try_new(fields(), arrays, None),
        )?)?))
    }
}
fn preflight(r: HistogramView<'_>, rows: usize, existing: [usize; 7]) -> Result<()> {
    if rows >= u32::MAX as usize {
        return invalid("histogram row index overflow");
    }
    let added = [
        r.custom_values.len(),
        r.positive.span_offsets.len(),
        r.positive.span_lengths.len(),
        r.positive.buckets.len(),
        r.negative.span_offsets.len(),
        r.negative.span_lengths.len(),
        r.negative.buckets.len(),
    ];
    for (old, new) in existing.into_iter().zip(added) {
        if old
            .checked_add(new)
            .filter(|n| *n <= i32::MAX as usize)
            .is_none()
        {
            return invalid("histogram list values exceed Arrow offset limit");
        }
    }
    validate_spans(
        r.positive.span_offsets,
        r.positive.span_lengths,
        r.positive.buckets.len(),
    )?;
    validate_spans(
        r.negative.span_offsets,
        r.negative.span_lengths,
        r.negative.buckets.len(),
    )
}
fn lb64() -> ListBuilder<Float64Builder> {
    ListBuilder::new(Float64Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Float64,
        false,
    )))
}
fn lb32() -> ListBuilder<Int32Builder> {
    ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )))
}
fn append64(b: &mut ListBuilder<Float64Builder>, v: &[f64]) {
    for x in v {
        b.values().append_value(*x)
    }
    b.append(true)
}
fn append32(b: &mut ListBuilder<Int32Builder>, v: &[i32]) {
    for x in v {
        b.values().append_value(*x)
    }
    b.append(true)
}

#[cfg(test)]
#[path = "histogram/tests.rs"]
mod tests;
