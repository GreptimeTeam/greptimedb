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

//! Batch-level statistics for bulk memtable pruning.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow;
use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    GenericBinaryArray, GenericStringArray, PrimitiveArray, StringViewArray,
};
use datatypes::arrow::compute::kernels::aggregate;
use datatypes::arrow::datatypes::{
    ArrowPrimitiveType, ArrowTimestampType, DataType as ArrowDataType, Date32Type, Date64Type,
    Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    i256,
};
use datatypes::data_type::{ConcreteDataType, DataType};
use snafu::ResultExt;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{ComputeArrowSnafu, Result};

type ScalarPair = (ScalarValue, ScalarValue);

/// Per-batch min/max statistics for columns in a [`MultiBulkPart`](crate::memtable::bulk::part::MultiBulkPart).
#[derive(Debug, Clone)]
pub(crate) struct BatchStats {
    /// Number of batches.
    num_batches: usize,
    /// Column stats by region column id.
    columns: HashMap<ColumnId, ColumnStats>,
}

#[derive(Debug, Clone)]
struct ColumnStats {
    min_values: ArrayRef,
    max_values: ArrayRef,
}

impl BatchStats {
    /// Computes batch statistics from a slice of record batches.
    pub(crate) fn compute(
        batches: &[common_recordbatch::DfRecordBatch],
        metadata: &RegionMetadata,
    ) -> Self {
        let mut columns = HashMap::with_capacity(metadata.column_metadatas.len());

        for column in &metadata.column_metadatas {
            let Some(stats) = compute_column_stats(batches, column) else {
                continue;
            };
            columns.insert(column.column_id, stats);
        }

        Self {
            num_batches: batches.len(),
            columns,
        }
    }

    fn min_values(&self, column_id: ColumnId) -> Option<ArrayRef> {
        self.columns
            .get(&column_id)
            .map(|stats| stats.min_values.clone())
    }

    fn max_values(&self, column_id: ColumnId) -> Option<ArrayRef> {
        self.columns
            .get(&column_id)
            .map(|stats| stats.max_values.clone())
    }
}

fn compute_column_stats(
    batches: &[common_recordbatch::DfRecordBatch],
    column: &ColumnMetadata,
) -> Option<ColumnStats> {
    if matches!(
        &column.column_schema.data_type,
        ConcreteDataType::Json(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Struct(_)
            | ConcreteDataType::Vector(_)
    ) {
        return None;
    }

    let arrow_type = column.column_schema.data_type.as_arrow_type();
    if !is_supported_arrow_type(&arrow_type) {
        return None;
    }

    let column_idx = batches
        .first()?
        .schema()
        .index_of(&column.column_schema.name)
        .ok()?;
    let null_scalar = ScalarValue::try_new_null(&arrow_type).ok()?;
    let mut mins = Vec::with_capacity(batches.len());
    let mut maxes = Vec::with_capacity(batches.len());

    for batch in batches {
        let Some((min, max)) = min_max_scalar(batch.column(column_idx), &arrow_type).ok()? else {
            mins.push(null_scalar.clone());
            maxes.push(null_scalar.clone());
            continue;
        };

        mins.push(min);
        maxes.push(max);
    }

    Some(ColumnStats {
        min_values: ScalarValue::iter_to_array(mins).ok()?,
        max_values: ScalarValue::iter_to_array(maxes).ok()?,
    })
}

fn is_supported_arrow_type(arrow_type: &ArrowDataType) -> bool {
    match arrow_type {
        ArrowDataType::Boolean
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::Utf8View
        | ArrowDataType::Binary
        | ArrowDataType::LargeBinary
        | ArrowDataType::BinaryView
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::Date32
        | ArrowDataType::Date64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Decimal128(_, _)
        | ArrowDataType::Decimal256(_, _) => true,
        ArrowDataType::Time32(unit) => matches!(
            unit,
            arrow::datatypes::TimeUnit::Second | arrow::datatypes::TimeUnit::Millisecond
        ),
        ArrowDataType::Time64(unit) => matches!(
            unit,
            arrow::datatypes::TimeUnit::Microsecond | arrow::datatypes::TimeUnit::Nanosecond
        ),
        ArrowDataType::Dictionary(_, value_type) => is_supported_arrow_type(value_type),
        _ => false,
    }
}

/// Returns exact min/max scalars for a supported Arrow array.
///
/// Empty/all-null arrays return `Ok(None)`. Unsupported arrays return `Ok(None)`.
fn min_max_scalar(array: &ArrayRef, logical_type: &ArrowDataType) -> Result<Option<ScalarPair>> {
    if array.is_empty() || array.null_count() == array.len() {
        return Ok(None);
    }

    if let ArrowDataType::Dictionary(_, value_type) = array.data_type() {
        let decoded =
            arrow::compute::cast(array.as_ref(), value_type).context(ComputeArrowSnafu)?;
        return min_max_scalar(&decoded, value_type);
    }

    let stats = match logical_type {
        ArrowDataType::Boolean => boolean_min_max(array.as_boolean()),
        ArrowDataType::Int8 => primitive_min_max::<Int8Type>(array.as_primitive::<Int8Type>()),
        ArrowDataType::Int16 => primitive_min_max::<Int16Type>(array.as_primitive::<Int16Type>()),
        ArrowDataType::Int32 => primitive_min_max::<Int32Type>(array.as_primitive::<Int32Type>()),
        ArrowDataType::Int64 => primitive_min_max::<Int64Type>(array.as_primitive::<Int64Type>()),
        ArrowDataType::UInt8 => primitive_min_max::<UInt8Type>(array.as_primitive::<UInt8Type>()),
        ArrowDataType::UInt16 => {
            primitive_min_max::<UInt16Type>(array.as_primitive::<UInt16Type>())
        }
        ArrowDataType::UInt32 => {
            primitive_min_max::<UInt32Type>(array.as_primitive::<UInt32Type>())
        }
        ArrowDataType::UInt64 => {
            primitive_min_max::<UInt64Type>(array.as_primitive::<UInt64Type>())
        }
        ArrowDataType::Float32 => {
            primitive_min_max::<Float32Type>(array.as_primitive::<Float32Type>())
        }
        ArrowDataType::Float64 => {
            primitive_min_max::<Float64Type>(array.as_primitive::<Float64Type>())
        }
        ArrowDataType::Utf8 => string_min_max(array.as_string::<i32>()),
        ArrowDataType::LargeUtf8 => string_min_max(array.as_string::<i64>()),
        ArrowDataType::Utf8View => string_view_min_max(array.as_string_view()),
        ArrowDataType::Binary => binary_min_max(array.as_binary::<i32>()),
        ArrowDataType::LargeBinary => binary_min_max(array.as_binary::<i64>()),
        ArrowDataType::BinaryView => binary_view_min_max(array.as_binary_view()),
        ArrowDataType::FixedSizeBinary(_) => {
            fixed_size_binary_min_max(array.as_fixed_size_binary())
        }
        ArrowDataType::Date32 => {
            primitive_min_max::<Date32Type>(array.as_primitive::<Date32Type>())
        }
        ArrowDataType::Date64 => {
            primitive_min_max::<Date64Type>(array.as_primitive::<Date64Type>())
        }
        ArrowDataType::Timestamp(unit, timezone) => match unit {
            arrow::datatypes::TimeUnit::Second => timestamp_min_max::<TimestampSecondType>(
                array.as_primitive::<TimestampSecondType>(),
                timezone.clone(),
            ),
            arrow::datatypes::TimeUnit::Millisecond => {
                timestamp_min_max::<TimestampMillisecondType>(
                    array.as_primitive::<TimestampMillisecondType>(),
                    timezone.clone(),
                )
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                timestamp_min_max::<TimestampMicrosecondType>(
                    array.as_primitive::<TimestampMicrosecondType>(),
                    timezone.clone(),
                )
            }
            arrow::datatypes::TimeUnit::Nanosecond => timestamp_min_max::<TimestampNanosecondType>(
                array.as_primitive::<TimestampNanosecondType>(),
                timezone.clone(),
            ),
        },
        ArrowDataType::Time32(unit) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                primitive_min_max::<Time32SecondType>(array.as_primitive::<Time32SecondType>())
            }
            arrow::datatypes::TimeUnit::Millisecond => primitive_min_max::<Time32MillisecondType>(
                array.as_primitive::<Time32MillisecondType>(),
            ),
            _ => None,
        },
        ArrowDataType::Time64(unit) => match unit {
            arrow::datatypes::TimeUnit::Microsecond => primitive_min_max::<Time64MicrosecondType>(
                array.as_primitive::<Time64MicrosecondType>(),
            ),
            arrow::datatypes::TimeUnit::Nanosecond => primitive_min_max::<Time64NanosecondType>(
                array.as_primitive::<Time64NanosecondType>(),
            ),
            _ => None,
        },
        ArrowDataType::Decimal128(precision, scale) => {
            decimal128_min_max(array.as_primitive::<Decimal128Type>(), *precision, *scale)
        }
        ArrowDataType::Decimal256(precision, scale) => {
            decimal256_min_max(array.as_primitive::<Decimal256Type>(), *precision, *scale)
        }
        _ => None,
    };

    Ok(stats)
}

trait ScalarValueFromPrimitive: ArrowPrimitiveType {
    fn scalar(value: Self::Native) -> ScalarValue;
}

macro_rules! impl_scalar_value_from_primitive {
    ($arrow_type:ty, $variant:ident) => {
        impl ScalarValueFromPrimitive for $arrow_type {
            fn scalar(value: Self::Native) -> ScalarValue {
                ScalarValue::$variant(Some(value))
            }
        }
    };
}

impl_scalar_value_from_primitive!(Int8Type, Int8);
impl_scalar_value_from_primitive!(Int16Type, Int16);
impl_scalar_value_from_primitive!(Int32Type, Int32);
impl_scalar_value_from_primitive!(Int64Type, Int64);
impl_scalar_value_from_primitive!(UInt8Type, UInt8);
impl_scalar_value_from_primitive!(UInt16Type, UInt16);
impl_scalar_value_from_primitive!(UInt32Type, UInt32);
impl_scalar_value_from_primitive!(UInt64Type, UInt64);
impl_scalar_value_from_primitive!(Float32Type, Float32);
impl_scalar_value_from_primitive!(Float64Type, Float64);
impl_scalar_value_from_primitive!(Date32Type, Date32);
impl_scalar_value_from_primitive!(Date64Type, Date64);
impl_scalar_value_from_primitive!(Time32SecondType, Time32Second);
impl_scalar_value_from_primitive!(Time32MillisecondType, Time32Millisecond);
impl_scalar_value_from_primitive!(Time64MicrosecondType, Time64Microsecond);
impl_scalar_value_from_primitive!(Time64NanosecondType, Time64Nanosecond);

fn primitive_min_max<T>(array: &PrimitiveArray<T>) -> Option<ScalarPair>
where
    T: ScalarValueFromPrimitive,
{
    let min = aggregate::min::<T>(array)?;
    let max = aggregate::max::<T>(array)?;
    Some((T::scalar(min), T::scalar(max)))
}

fn timestamp_min_max<T>(array: &PrimitiveArray<T>, timezone: Option<Arc<str>>) -> Option<ScalarPair>
where
    T: ArrowTimestampType,
{
    let min = aggregate::min::<T>(array)?;
    let max = aggregate::max::<T>(array)?;
    Some((
        ScalarValue::new_timestamp::<T>(Some(min), timezone.clone()),
        ScalarValue::new_timestamp::<T>(Some(max), timezone),
    ))
}

fn decimal128_min_max(
    array: &PrimitiveArray<Decimal128Type>,
    precision: u8,
    scale: i8,
) -> Option<ScalarPair> {
    let min = aggregate::min::<Decimal128Type>(array)?;
    let max = aggregate::max::<Decimal128Type>(array)?;
    Some((
        ScalarValue::Decimal128(Some(min), precision, scale),
        ScalarValue::Decimal128(Some(max), precision, scale),
    ))
}

fn decimal256_min_max(
    array: &PrimitiveArray<Decimal256Type>,
    precision: u8,
    scale: i8,
) -> Option<ScalarPair> {
    let min: i256 = aggregate::min::<Decimal256Type>(array)?;
    let max: i256 = aggregate::max::<Decimal256Type>(array)?;
    Some((
        ScalarValue::Decimal256(Some(min), precision, scale),
        ScalarValue::Decimal256(Some(max), precision, scale),
    ))
}

fn boolean_min_max(array: &BooleanArray) -> Option<ScalarPair> {
    let min = aggregate::min_boolean(array)?;
    let max = aggregate::max_boolean(array)?;
    Some((
        ScalarValue::Boolean(Some(min)),
        ScalarValue::Boolean(Some(max)),
    ))
}

fn string_min_max<O>(array: &GenericStringArray<O>) -> Option<ScalarPair>
where
    O: arrow::array::OffsetSizeTrait,
{
    let min = aggregate::min_string(array)?;
    let max = aggregate::max_string(array)?;
    if O::IS_LARGE {
        Some((
            ScalarValue::LargeUtf8(Some(min.to_string())),
            ScalarValue::LargeUtf8(Some(max.to_string())),
        ))
    } else {
        Some((
            ScalarValue::Utf8(Some(min.to_string())),
            ScalarValue::Utf8(Some(max.to_string())),
        ))
    }
}

fn string_view_min_max(array: &StringViewArray) -> Option<ScalarPair> {
    let min = aggregate::min_string_view(array)?;
    let max = aggregate::max_string_view(array)?;
    Some((
        ScalarValue::Utf8View(Some(min.to_string())),
        ScalarValue::Utf8View(Some(max.to_string())),
    ))
}

fn binary_min_max<O>(array: &GenericBinaryArray<O>) -> Option<ScalarPair>
where
    O: arrow::array::OffsetSizeTrait,
{
    let min = aggregate::min_binary(array)?;
    let max = aggregate::max_binary(array)?;
    if O::IS_LARGE {
        Some((
            ScalarValue::LargeBinary(Some(min.to_vec())),
            ScalarValue::LargeBinary(Some(max.to_vec())),
        ))
    } else {
        Some((
            ScalarValue::Binary(Some(min.to_vec())),
            ScalarValue::Binary(Some(max.to_vec())),
        ))
    }
}

fn binary_view_min_max(array: &BinaryViewArray) -> Option<ScalarPair> {
    let min = aggregate::min_binary_view(array)?;
    let max = aggregate::max_binary_view(array)?;
    Some((
        ScalarValue::BinaryView(Some(min.to_vec())),
        ScalarValue::BinaryView(Some(max.to_vec())),
    ))
}

fn fixed_size_binary_min_max(array: &FixedSizeBinaryArray) -> Option<ScalarPair> {
    let min = aggregate::min_fixed_size_binary(array)?;
    let max = aggregate::max_fixed_size_binary(array)?;
    Some((
        ScalarValue::FixedSizeBinary(array.value_length(), Some(min.to_vec())),
        ScalarValue::FixedSizeBinary(array.value_length(), Some(max.to_vec())),
    ))
}

/// Adapter implementing [`PruningStatistics`] for [`BatchStats`].
pub(crate) struct BatchPruningStats<'a> {
    stats: &'a BatchStats,
    metadata: &'a RegionMetadataRef,
}

impl<'a> BatchPruningStats<'a> {
    /// Creates a new [`BatchPruningStats`].
    pub(crate) fn new(stats: &'a BatchStats, metadata: &'a RegionMetadataRef) -> Self {
        Self { stats, metadata }
    }
}

impl PruningStatistics for BatchPruningStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let col = self.metadata.column_by_name(&column.name)?;
        self.stats.min_values(col.column_id)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let col = self.metadata.column_by_name(&column.name)?;
        self.stats.max_values(col.column_id)
    }

    fn num_containers(&self) -> usize {
        self.stats.num_batches
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
