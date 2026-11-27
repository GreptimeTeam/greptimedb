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

//! Native histogram sample column encoding 8.
//!
//! One chunk holds a whole struct column of a block, laid out child by child. Every child is
//! encoded with the nullable XOR codec of encoding 5: floats keep their bit patterns, integers
//! are ZigZag-mapped first, and a list is its nullable lengths followed by its flattened items.
//! The chunk header names the child count and each child's kind, so a reader decodes the chunk
//! only against the exact struct type it was written for.
//!
//! ```text
//! child_count   u8
//! child_kinds   u8 x child_count       1 Float64, 2 Int32, 3 Int64, 4 Timestamp(ms),
//!                                      0x10 | kind for a list
//! list_items    uvarint                items of all valid lists in the chunk
//! validity      ceil(rows / 8) bytes   only when the struct has null rows
//! child 0 .. child 17, in struct order:
//!   scalar      section(rows values)
//!   list        section(rows lengths), then section(items), omitted if it has no items
//! section(n)    null_count uvarint; unless all n are null: byte_length uvarint and an
//!               encoding 5 body
//! ```
//!
//! The header records kinds, not names, so children of one kind are told apart by position
//! alone. The struct order, from `NATIVE_HISTOGRAM_FIELD_NAMES`:
//!
//! ```text
//!  0 schema                  Int32           9 negative_span_lengths   List<Int32>
//!  1 zero_threshold          Float64        10 count_i64               Int64
//!  2 sum                     Float64        11 zero_count_i64          Int64
//!  3 reset_hint              Int32          12 positive_buckets_i64    List<Int64>
//!  4 start_timestamp         Timestamp(ms)  13 negative_buckets_i64    List<Int64>
//!  5 custom_values           List<Float64>  14 count_f64               Float64
//!  6 positive_span_offsets   List<Int32>    15 zero_count_f64          Float64
//!  7 positive_span_lengths   List<Int32>    16 positive_buckets_f64    List<Float64>
//!  8 negative_span_offsets   List<Int32>    17 negative_buckets_f64    List<Float64>
//! ```
//!
//! The part-file layout around the chunk is in `docs/formats/mito-samples.md`, "Native
//! histogram encoding 8".

use std::sync::Arc;

use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, ListArray, StructArray,
    TimestampMillisecondArray,
};
use datatypes::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use datatypes::arrow::datatypes::{
    DataType, Fields, Float64Type, Int32Type, Int64Type, TimeUnit, TimestampMillisecondType,
};

use crate::error::Result;
use crate::series_index::samples::codec::{self, Cursor, put_varint};
use crate::series_index::samples::metadata::CHUNK_LIMIT;
use crate::series_index::samples::{MAX_ROWS, invalid, require};

/// Sample column encoding ID of a native histogram struct chunk.
pub(super) const ENCODING: u16 = 8;
/// Upper bound of one encoded value, its validity bit included: a full varint plus a bitmap
/// byte, which also covers a section's 8-byte first value.
const VALUE_BOUND: usize = 11;
/// Upper bound of one row's framing: its struct validity bit.
const ROW_FRAMING_BOUND: usize = 1;
/// Upper bound of one child's framing: its kind byte and the count/length varints of at most
/// two nullable sections.
const CHILD_FRAMING_BOUND: usize = 64;
/// Upper bound of the chunk header besides child framing and struct validity: the child count
/// and the list item count.
const HEADER_BOUND: usize = 16;
/// Memory a reader retains per decoded value: the decoded `Option<u64>`, its Arrow value and
/// offset, and a copy of both when the block is narrowed to the selected rows.
const DECODED_VALUE_BYTES: usize = 48;

/// Encodes a native histogram struct column. All rows are encoded, null rows included, and the
/// struct validity is kept apart from the children's own validity.
pub(super) fn encode(array: &StructArray) -> Result<Vec<u8>> {
    let layout = layout(array.fields())?;
    let rows = array.len();
    let mut body = Vec::new();
    let mut list_values = 0usize;
    for (child, column) in layout.iter().zip(array.columns()) {
        match child {
            Child::Scalar(scalar) => {
                let values = Values::new(*scalar, column.as_ref())?;
                put_nullable(
                    &mut body,
                    &(0..rows).map(|i| values.get(i)).collect::<Vec<_>>(),
                );
            }
            Child::List(scalar) => {
                let list = column
                    .as_list_opt::<i32>()
                    .ok_or_else(|| invalid("invalid native histogram list child"))?;
                let offsets = list.value_offsets();
                let lengths = (0..rows)
                    .map(|i| {
                        list.is_valid(i)
                            .then(|| (offsets[i + 1] - offsets[i]) as u64)
                    })
                    .collect::<Vec<_>>();
                put_nullable(&mut body, &lengths);
                let values = Values::new(*scalar, list.values().as_ref())?;
                // A null list's slot range is not part of its value, so only valid lists
                // contribute items.
                let items = (0..rows)
                    .filter(|&i| list.is_valid(i))
                    .flat_map(|i| offsets[i] as usize..offsets[i + 1] as usize)
                    .map(|j| values.get(j))
                    .collect::<Vec<_>>();
                if !items.is_empty() {
                    put_nullable(&mut body, &items);
                }
                list_values += items.len();
            }
        }
    }
    let mut out = vec![
        u8::try_from(layout.len()).map_err(|_| invalid("too many native histogram children"))?,
    ];
    out.extend(layout.iter().map(|child| child.code()));
    put_varint(&mut out, list_values as u64);
    if array.null_count() > 0 {
        let start = out.len();
        out.resize(start + rows.div_ceil(8), 0);
        for i in (0..rows).filter(|&i| array.is_valid(i)) {
            out[start + i / 8] |= 1 << (i % 8);
        }
    }
    out.extend(body);
    Ok(out)
}

/// Memory the reader must account for before decoding a chunk of `rows` rows.
pub(super) fn decoded_bytes(bytes: &[u8], rows: usize) -> Result<usize> {
    let mut c = Cursor::new(bytes);
    let children = c.u8()? as usize;
    c.take(children)?;
    let list_values = list_value_count(&mut c)?;
    Ok((rows * children + list_values) * DECODED_VALUE_BYTES)
}

/// Decodes a chunk written by [`encode`] as `data_type`, which must be the struct type the chunk
/// was written for. `nulls` is the descriptor's struct-level null count.
pub(super) fn decode(
    bytes: &[u8],
    data_type: &DataType,
    rows: usize,
    nulls: u32,
) -> Result<StructArray> {
    let DataType::Struct(fields) = data_type else {
        return Err(invalid("native histogram chunk requires a struct type"));
    };
    let layout = layout(fields)?;
    let nulls = nulls as usize;
    require(
        (1..=MAX_ROWS).contains(&rows) && nulls <= rows,
        "invalid native histogram row count",
    )?;
    let mut c = Cursor::new(bytes);
    require(
        c.u8()? as usize == layout.len(),
        "native histogram child count mismatch",
    )?;
    for child in &layout {
        require(
            c.u8()? == child.code(),
            "native histogram child kind mismatch",
        )?;
    }
    let mut remaining = list_value_count(&mut c)?;
    let validity = if nulls == 0 {
        None
    } else {
        let bitmap = c.take(rows.div_ceil(8))?;
        if !rows.is_multiple_of(8) {
            require(
                bitmap[bitmap.len() - 1] >> (rows % 8) == 0,
                "nonzero native histogram validity padding",
            )?;
        }
        let validity = BooleanBuffer::new(Buffer::from(bitmap.to_vec()), 0, rows);
        require(
            validity.count_set_bits() + nulls == rows,
            "native histogram validity count mismatch",
        )?;
        Some(NullBuffer::new(validity))
    };
    let mut children = Vec::with_capacity(layout.len());
    for (child, field) in layout.iter().zip(fields.iter()) {
        let array = match child {
            Child::Scalar(scalar) => scalar.build(take_nullable(&mut c, rows)?)?,
            Child::List(scalar) => {
                let DataType::List(item) = field.data_type() else {
                    return Err(invalid("native histogram list child requires a list type"));
                };
                let lengths = take_nullable(&mut c, rows)?;
                let mut offsets = Vec::with_capacity(rows + 1);
                offsets.push(0i32);
                let mut total = 0usize;
                for length in &lengths {
                    total = total
                        .checked_add(length.unwrap_or(0) as usize)
                        .filter(|total| *total <= remaining)
                        .ok_or_else(|| invalid("native histogram list value count mismatch"))?;
                    // `remaining` is at most `CHUNK_LIMIT`, so every offset fits `i32`.
                    offsets.push(total as i32);
                }
                remaining -= total;
                let items = if total == 0 {
                    Vec::new()
                } else {
                    take_nullable(&mut c, total)?
                };
                let list_nulls = lengths
                    .iter()
                    .any(Option::is_none)
                    .then(|| NullBuffer::from_iter(lengths.iter().map(Option::is_some)));
                Arc::new(
                    ListArray::try_new(
                        item.clone(),
                        OffsetBuffer::new(offsets.into()),
                        scalar.build(items)?,
                        list_nulls,
                    )
                    .map_err(|e| invalid(format!("invalid native histogram list: {e}")))?,
                )
            }
        };
        children.push(array);
    }
    require(remaining == 0, "native histogram list value count mismatch")?;
    c.end()?;
    StructArray::try_new(fields.clone(), children, validity)
        .map_err(|e| invalid(format!("invalid native histogram struct: {e}")))
}

/// Requires one sample with `children` struct children and `list_items` bucket list items to
/// fit a chunk on its own, which is what lets the writers place every accepted sample.
pub(crate) fn check_sample_size(children: usize, list_items: usize) -> Result<()> {
    require(
        ChunkBound::default()
            .with_bytes(children, row_bytes(children, list_items))
            .is_some(),
        "native histogram sample exceeds the Narthex samples chunk limit",
    )
}

/// Requires every row of a native histogram column to fit a chunk on its own.
pub(crate) fn check_array_sample_sizes(array: &StructArray) -> Result<()> {
    (0..array.len())
        .try_for_each(|row| check_sample_size(array.num_columns(), list_items(array, row)))
}

/// The encoded size bound of a chunk that is still being filled, so a writer can cut a block
/// before its histogram chunk would exceed [`CHUNK_LIMIT`].
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ChunkBound {
    rows: usize,
    bytes: usize,
}

impl ChunkBound {
    /// The bound with `array[row]` appended, or `None` when that row no longer fits the chunk.
    pub(super) fn with_row(&self, array: &StructArray, row: usize) -> Option<Self> {
        self.with_bytes(
            array.num_columns(),
            row_bytes(array.num_columns(), list_items(array, row)),
        )
    }

    fn with_bytes(&self, children: usize, row_bytes: usize) -> Option<Self> {
        let bytes = self.bytes + row_bytes;
        let rows = self.rows + 1;
        let chunk = HEADER_BOUND + children * CHILD_FRAMING_BOUND + rows.div_ceil(8) + bytes;
        (chunk <= CHUNK_LIMIT).then_some(Self { rows, bytes })
    }
}

/// Upper bound of the bytes one row adds to a chunk: one value per child, which covers a list's
/// length, one value per list item, and its struct validity bit.
fn row_bytes(children: usize, list_items: usize) -> usize {
    ROW_FRAMING_BOUND + VALUE_BOUND * (children + list_items)
}

/// Items of the valid lists of `array[row]`.
fn list_items(array: &StructArray, row: usize) -> usize {
    array
        .columns()
        .iter()
        .filter_map(|column| column.as_list_opt::<i32>())
        .filter(|list| list.is_valid(row))
        .map(|list| list.value_length(row) as usize)
        .sum()
}

/// Reads the chunk's total list item count. A writer cuts chunks by [`ChunkBound`], which charges
/// every item [`VALUE_BOUND`] bytes, so a larger count is corrupt rather than merely large.
fn list_value_count(c: &mut Cursor<'_>) -> Result<usize> {
    let count = c.varint()?;
    require(
        count <= CHUNK_LIMIT as u64,
        "native histogram list value count exceeds limit",
    )?;
    Ok(count as usize)
}

/// Writes `values` as a null count followed, unless every value is null, by the byte length
/// and body of encoding 5.
fn put_nullable(out: &mut Vec<u8>, values: &[Option<u64>]) {
    let nulls = values.iter().filter(|v| v.is_none()).count();
    put_varint(out, nulls as u64);
    if nulls < values.len() {
        let bytes = codec::encode_float(values);
        put_varint(out, bytes.len() as u64);
        out.extend(bytes);
    }
}

/// Reads `count` values written by [`put_nullable`]. The caller bounds `count`.
fn take_nullable(c: &mut Cursor<'_>, count: usize) -> Result<Vec<Option<u64>>> {
    let nulls = c.varint()?;
    require(
        nulls <= count as u64,
        "native histogram null count exceeds values",
    )?;
    let nulls = nulls as usize;
    if nulls == count {
        return Ok(vec![None; count]);
    }
    let length = usize::try_from(c.varint()?)
        .map_err(|_| invalid("native histogram section length overflow"))?;
    codec::decode_xor(c.take(length)?, count, nulls)
}

/// A child value type and its lossless `u64` mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scalar {
    Float64,
    Int32,
    Int64,
    TimestampMillisecond,
}

impl Scalar {
    fn from_arrow(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Float64 => Ok(Self::Float64),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(Self::TimestampMillisecond),
            other => Err(invalid(format!(
                "unsupported native histogram child type {other}"
            ))),
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Float64 => 1,
            Self::Int32 => 2,
            Self::Int64 => 3,
            Self::TimestampMillisecond => 4,
        }
    }

    fn build(self, values: Vec<Option<u64>>) -> Result<ArrayRef> {
        let signed = |bits: u64| codec::unzig(bits) as i64;
        Ok(match self {
            Self::Float64 => Arc::new(Float64Array::from_iter(
                values.into_iter().map(|v| v.map(f64::from_bits)),
            )),
            Self::Int32 => Arc::new(Int32Array::from(
                values
                    .into_iter()
                    .map(|v| {
                        v.map(|bits| {
                            i32::try_from(signed(bits))
                                .map_err(|_| invalid("native histogram Int32 value overflow"))
                        })
                        .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            Self::Int64 => Arc::new(Int64Array::from_iter(
                values.into_iter().map(|v| v.map(signed)),
            )),
            Self::TimestampMillisecond => Arc::new(TimestampMillisecondArray::from_iter(
                values.into_iter().map(|v| v.map(signed)),
            )),
        })
    }
}

/// Typed access to one child's values as their `u64` mapping.
enum Values<'a> {
    Float64(&'a Float64Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    TimestampMillisecond(&'a TimestampMillisecondArray),
}

impl<'a> Values<'a> {
    fn new(scalar: Scalar, array: &'a dyn Array) -> Result<Self> {
        let values = match scalar {
            Scalar::Float64 => array.as_primitive_opt::<Float64Type>().map(Self::Float64),
            Scalar::Int32 => array.as_primitive_opt::<Int32Type>().map(Self::Int32),
            Scalar::Int64 => array.as_primitive_opt::<Int64Type>().map(Self::Int64),
            Scalar::TimestampMillisecond => array
                .as_primitive_opt::<TimestampMillisecondType>()
                .map(Self::TimestampMillisecond),
        };
        values.ok_or_else(|| invalid("native histogram child does not match its type"))
    }

    fn get(&self, index: usize) -> Option<u64> {
        match self {
            Self::Float64(a) => a.is_valid(index).then(|| a.value(index).to_bits()),
            Self::Int32(a) => a
                .is_valid(index)
                .then(|| codec::zig(i64::from(a.value(index)))),
            Self::Int64(a) => a.is_valid(index).then(|| codec::zig(a.value(index))),
            Self::TimestampMillisecond(a) => a.is_valid(index).then(|| codec::zig(a.value(index))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Child {
    Scalar(Scalar),
    List(Scalar),
}

impl Child {
    fn code(self) -> u8 {
        match self {
            Self::Scalar(scalar) => scalar.code(),
            Self::List(scalar) => 0x10 | scalar.code(),
        }
    }
}

fn layout(fields: &Fields) -> Result<Vec<Child>> {
    fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::List(item) => Scalar::from_arrow(item.data_type()).map(Child::List),
            other => Scalar::from_arrow(other).map(Child::Scalar),
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use common_query::native_histogram::{
        COUNT_F64_FIELD, COUNT_I64_FIELD, COUNTER_RESET_HINT, CUSTOM_BUCKETS_SCHEMA,
        CUSTOM_VALUES_FIELD, GAUGE_RESET_HINT, NEGATIVE_BUCKETS_F64_FIELD,
        NEGATIVE_BUCKETS_I64_FIELD, NEGATIVE_SPAN_LENGTHS_FIELD, NEGATIVE_SPAN_OFFSETS_FIELD,
        NOT_COUNTER_RESET_HINT, POSITIVE_BUCKETS_F64_FIELD, POSITIVE_BUCKETS_I64_FIELD,
        POSITIVE_SPAN_LENGTHS_FIELD, POSITIVE_SPAN_OFFSETS_FIELD, RESET_HINT_FIELD, SCHEMA_FIELD,
        START_TIMESTAMP_FIELD, SUM_FIELD, ZERO_COUNT_F64_FIELD, ZERO_COUNT_I64_FIELD,
        ZERO_THRESHOLD_FIELD, native_histogram_value_type,
    };
    use datatypes::arrow::array::new_empty_array;
    use datatypes::arrow::compute::concat;
    use datatypes::arrow::datatypes::Field;
    use datatypes::data_type::DataType as _;

    use super::*;

    /// One histogram row of a test fixture; the count family it carries is the populated one.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct TestHistogram {
        pub(crate) schema: i32,
        pub(crate) zero_threshold: f64,
        pub(crate) sum: f64,
        pub(crate) reset_hint: i32,
        pub(crate) start: Option<i64>,
        pub(crate) custom_values: Option<Vec<f64>>,
        pub(crate) positive_spans: Vec<(i32, i32)>,
        pub(crate) negative_spans: Vec<(i32, i32)>,
        /// `(count, zero_count, positive deltas, negative deltas)`.
        pub(crate) ints: Option<(i64, i64, Vec<i64>, Vec<i64>)>,
        /// `(count, zero_count, positive counts, negative counts)`.
        pub(crate) floats: Option<(f64, f64, Vec<f64>, Vec<f64>)>,
    }

    /// Builds the canonical struct column, with null children under a null row as ingestion
    /// writes them.
    pub(crate) fn histogram_array(rows: &[Option<TestHistogram>]) -> StructArray {
        let DataType::Struct(fields) = native_histogram_value_type().as_arrow_type() else {
            unreachable!("the native histogram type is a struct")
        };
        let floats = |f: &dyn Fn(&TestHistogram) -> Option<f64>| -> ArrayRef {
            Arc::new(Float64Array::from_iter(
                rows.iter().map(|row| row.as_ref().and_then(f)),
            ))
        };
        let ints = |f: &dyn Fn(&TestHistogram) -> Option<i64>| -> ArrayRef {
            Arc::new(Int64Array::from_iter(
                rows.iter().map(|row| row.as_ref().and_then(f)),
            ))
        };
        let int32s = |f: &dyn Fn(&TestHistogram) -> i32| -> ArrayRef {
            Arc::new(Int32Array::from_iter(
                rows.iter().map(|row| row.as_ref().map(f)),
            ))
        };
        let children = fields
            .iter()
            .map(|field| -> ArrayRef {
                let list = |f: &dyn Fn(&TestHistogram) -> Option<ArrayRef>| {
                    list_array(
                        field,
                        &rows
                            .iter()
                            .map(|row| row.as_ref().and_then(f))
                            .collect::<Vec<_>>(),
                    )
                };
                let spans = |spans: &[(i32, i32)], offsets: bool| -> ArrayRef {
                    Arc::new(Int32Array::from_iter_values(
                        spans
                            .iter()
                            .map(|(offset, length)| if offsets { *offset } else { *length }),
                    ))
                };
                match field.name().as_str() {
                    SCHEMA_FIELD => int32s(&|h| h.schema),
                    RESET_HINT_FIELD => int32s(&|h| h.reset_hint),
                    ZERO_THRESHOLD_FIELD => floats(&|h| Some(h.zero_threshold)),
                    SUM_FIELD => floats(&|h| Some(h.sum)),
                    START_TIMESTAMP_FIELD => Arc::new(TimestampMillisecondArray::from_iter(
                        rows.iter().map(|row| row.as_ref().and_then(|h| h.start)),
                    )),
                    CUSTOM_VALUES_FIELD => list(&|h| {
                        h.custom_values
                            .clone()
                            .map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
                    }),
                    POSITIVE_SPAN_OFFSETS_FIELD => list(&|h| Some(spans(&h.positive_spans, true))),
                    POSITIVE_SPAN_LENGTHS_FIELD => list(&|h| Some(spans(&h.positive_spans, false))),
                    NEGATIVE_SPAN_OFFSETS_FIELD => list(&|h| Some(spans(&h.negative_spans, true))),
                    NEGATIVE_SPAN_LENGTHS_FIELD => list(&|h| Some(spans(&h.negative_spans, false))),
                    COUNT_I64_FIELD => ints(&|h| h.ints.as_ref().map(|i| i.0)),
                    ZERO_COUNT_I64_FIELD => ints(&|h| h.ints.as_ref().map(|i| i.1)),
                    POSITIVE_BUCKETS_I64_FIELD => list(&|h| {
                        h.ints
                            .as_ref()
                            .map(|i| Arc::new(Int64Array::from(i.2.clone())) as ArrayRef)
                    }),
                    NEGATIVE_BUCKETS_I64_FIELD => list(&|h| {
                        h.ints
                            .as_ref()
                            .map(|i| Arc::new(Int64Array::from(i.3.clone())) as ArrayRef)
                    }),
                    COUNT_F64_FIELD => floats(&|h| h.floats.as_ref().map(|f| f.0)),
                    ZERO_COUNT_F64_FIELD => floats(&|h| h.floats.as_ref().map(|f| f.1)),
                    POSITIVE_BUCKETS_F64_FIELD => list(&|h| {
                        h.floats
                            .as_ref()
                            .map(|f| Arc::new(Float64Array::from(f.2.clone())) as ArrayRef)
                    }),
                    NEGATIVE_BUCKETS_F64_FIELD => list(&|h| {
                        h.floats
                            .as_ref()
                            .map(|f| Arc::new(Float64Array::from(f.3.clone())) as ArrayRef)
                    }),
                    other => unreachable!("unknown native histogram field {other}"),
                }
            })
            .collect();
        let validity = NullBuffer::from_iter(rows.iter().map(Option::is_some));
        StructArray::new(
            fields,
            children,
            (validity.null_count() > 0).then_some(validity),
        )
    }

    fn list_array(field: &Field, rows: &[Option<ArrayRef>]) -> ArrayRef {
        let DataType::List(item) = field.data_type() else {
            unreachable!("{} is a list", field.name())
        };
        let mut offsets = vec![0i32];
        for row in rows {
            offsets.push(offsets.last().unwrap() + row.as_ref().map_or(0, |a| a.len() as i32));
        }
        let parts = rows
            .iter()
            .flatten()
            .map(|a| a.as_ref())
            .collect::<Vec<_>>();
        let values = if parts.is_empty() {
            new_empty_array(item.data_type())
        } else {
            concat(&parts).unwrap()
        };
        let validity = NullBuffer::from_iter(rows.iter().map(Option::is_some));
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(offsets.into()),
            values,
            (validity.null_count() > 0).then_some(validity),
        ))
    }

    pub(crate) fn fixture() -> Vec<Option<TestHistogram>> {
        vec![
            // Exponential integer histogram with a negative span offset and a counter reset.
            Some(TestHistogram {
                schema: 3,
                zero_threshold: 0.001,
                sum: 12.5,
                reset_hint: COUNTER_RESET_HINT.into(),
                start: Some(-1_000),
                positive_spans: vec![(-2, 2), (3, 1)],
                negative_spans: vec![(0, 1)],
                ints: Some((7, 1, vec![2, -1, 1], vec![-3])),
                ..Default::default()
            }),
            None,
            // Float gauge histogram with no populated bucket at all.
            Some(TestHistogram {
                sum: -0.0,
                reset_hint: GAUGE_RESET_HINT.into(),
                floats: Some((0.0, 0.0, vec![], vec![])),
                ..Default::default()
            }),
            // Custom buckets carry their bounds, including +Inf.
            Some(TestHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                sum: 4.25,
                reset_hint: NOT_COUNTER_RESET_HINT.into(),
                custom_values: Some(vec![0.1, 0.5, f64::INFINITY]),
                positive_spans: vec![(0, 3)],
                floats: Some((6.0, 0.0, vec![1.0, 2.5, 2.5], vec![])),
                ..Default::default()
            }),
        ]
    }

    #[test]
    fn histogram_chunk_round_trips_and_names_its_type() {
        let array = histogram_array(&fixture());
        let bytes = encode(&array).unwrap();
        let decoded = decode(
            &bytes,
            array.data_type(),
            array.len(),
            array.null_count() as u32,
        )
        .unwrap();
        assert_eq!(array, decoded);
        // One-row slices keep their own offsets: a slice of the custom-bucket row and a slice of
        // the null row decode to the same rows.
        for row in [3, 1] {
            let slice = array.slice(row, 1);
            let decoded = decode(
                &encode(&slice).unwrap(),
                slice.data_type(),
                1,
                slice.null_count() as u32,
            )
            .unwrap();
            assert_eq!(slice, decoded);
        }

        // The chunk is only readable as the struct it was written for.
        let DataType::Struct(fields) = array.data_type() else {
            unreachable!()
        };
        let mut changed = fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        changed[0] = Field::new(changed[0].name(), DataType::Int64, true);
        let error = decode(
            &bytes,
            &DataType::Struct(changed.into()),
            array.len(),
            array.null_count() as u32,
        )
        .unwrap_err();
        assert!(error.to_string().contains("child kind mismatch"), "{error}");
        let fewer = fields.iter().skip(1).cloned().collect::<Fields>();
        let error = decode(&bytes, &DataType::Struct(fewer), array.len(), 1).unwrap_err();
        assert!(
            error.to_string().contains("child count mismatch"),
            "{error}"
        );
        // The descriptor's struct null count must agree with the stored validity.
        assert!(decode(&bytes, array.data_type(), array.len(), 0).is_err());
    }
}
