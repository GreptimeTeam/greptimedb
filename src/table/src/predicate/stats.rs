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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::parquet::file::statistics::Statistics as ParquetStats;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use paste::paste;

/// Row-group-level pruning statistics backed by parquet metadata.
/// Currently unused in PR1; wired into the scan path in a follow-up PR.
#[allow(dead_code)]
pub struct RowGroupPruningStatistics<'a> {
    pub meta_data: &'a [RowGroupMetaData],
    pub schema: &'a datatypes::schema::SchemaRef,
}

#[allow(dead_code)]
impl<'a> RowGroupPruningStatistics<'a> {
    pub fn new(
        meta_data: &'a [RowGroupMetaData],
        schema: &'a datatypes::schema::SchemaRef,
    ) -> Self {
        Self { meta_data, schema }
    }

    fn field_by_name(&self, name: &str) -> Option<(usize, &DataType)> {
        let idx = self.schema.column_index_by_name(name)?;
        let data_type = &self.schema.arrow_schema().fields.get(idx)?.data_type();
        Some((idx, data_type))
    }
}

macro_rules! impl_min_max_values {
    ($self:ident, $col:ident, $min_max: ident) => {{
        let arrow_schema = $self.schema.arrow_schema().clone();
        let (column_index, field) = if let Some((v, f)) = arrow_schema.column_with_name(&$col.name)
        {
            (v, f)
        } else {
            return None;
        };
        let data_type = field.data_type();
        let null_scalar: ScalarValue = if let Ok(v) = data_type.try_into() {
            v
        } else {
            return None;
        };

        let scalar_values = $self
            .meta_data
            .iter()
            .map(|meta| {
                if column_index >= meta.num_columns() {
                    return None;
                }
                let stats = meta.column(column_index).statistics()?;
                // NOTE: Keep this type-matching logic in sync with the
                // `parquet_bound_scalar` function in
                // `src/common/query/src/aggr_stats.rs`.
                paste! {
                    match stats {
                        ParquetStats::Boolean(s) => {
                            if !matches!(data_type, DataType::Boolean) {
                                return None;
                            }
                            Some(ScalarValue::Boolean(Some(*s.[<$min_max _opt>]()?)))
                        }
                        ParquetStats::Int32(s) => {
                            let raw = *s.[<$min_max _opt>]()?;
                            match data_type {
                                DataType::Int32
                                | DataType::UInt32
                                | DataType::Int16
                                | DataType::UInt16
                                | DataType::Int8
                                | DataType::UInt8 => Some(ScalarValue::Int32(Some(raw))),
                                DataType::Time32(TimeUnit::Second) => {
                                    Some(ScalarValue::Time32Second(Some(raw)))
                                }
                                DataType::Time32(TimeUnit::Millisecond) => {
                                    Some(ScalarValue::Time32Millisecond(Some(raw)))
                                }
                                DataType::Date32 => Some(ScalarValue::Date32(Some(raw))),
                                _ => None,
                            }
                        }
                        ParquetStats::Int64(s) => {
                            let raw = *s.[<$min_max _opt>]()?;
                            match data_type {
                                DataType::Int64 | DataType::UInt64 => {
                                    Some(ScalarValue::Int64(Some(raw)))
                                }
                                // Timestamp timezone is tracked separately in GreptimeDB's
                                // extension-type system, not in Arrow DataType, so we pass None
                                // here — consistent with datatypes::value::timestamp_to_scalar_value.
                                DataType::Timestamp(TimeUnit::Second, _) => {
                                    Some(ScalarValue::TimestampSecond(Some(raw), None))
                                }
                                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                                    Some(ScalarValue::TimestampMillisecond(Some(raw), None))
                                }
                                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                                    Some(ScalarValue::TimestampMicrosecond(Some(raw), None))
                                }
                                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                                    Some(ScalarValue::TimestampNanosecond(Some(raw), None))
                                }
                                DataType::Time64(TimeUnit::Microsecond) => {
                                    Some(ScalarValue::Time64Microsecond(Some(raw)))
                                }
                                DataType::Time64(TimeUnit::Nanosecond) => {
                                    Some(ScalarValue::Time64Nanosecond(Some(raw)))
                                }
                                DataType::Duration(TimeUnit::Second) => {
                                    Some(ScalarValue::DurationSecond(Some(raw)))
                                }
                                DataType::Duration(TimeUnit::Millisecond) => {
                                    Some(ScalarValue::DurationMillisecond(Some(raw)))
                                }
                                DataType::Duration(TimeUnit::Microsecond) => {
                                    Some(ScalarValue::DurationMicrosecond(Some(raw)))
                                }
                                DataType::Duration(TimeUnit::Nanosecond) => {
                                    Some(ScalarValue::DurationNanosecond(Some(raw)))
                                }
                                // Date64 is not supported by Value::try_from.
                                DataType::Date64 => None,
                                _ => None,
                            }
                        }
                        ParquetStats::Int96(_) => None,
                        ParquetStats::Float(s) => {
                            if !matches!(data_type, DataType::Float32) {
                                return None;
                            }
                            Some(ScalarValue::Float32(Some(*s.[<$min_max _opt>]()?)))
                        }
                        ParquetStats::Double(s) => {
                            if !matches!(data_type, DataType::Float64) {
                                return None;
                            }
                            Some(ScalarValue::Float64(Some(*s.[<$min_max _opt>]()?)))
                        }
                        ParquetStats::ByteArray(s) => {
                            let bytes = s.[<$min_max _bytes_opt>]()?;
                            match data_type {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    String::from_utf8(bytes.to_owned())
                                        .ok()
                                        .map(|s| ScalarValue::Utf8(Some(s)))
                                }
                                _ => None,
                            }
                        }
                        ParquetStats::FixedLenByteArray(_) => None,
                    }
                }
            })
            .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
            .collect::<Vec<ScalarValue>>();
        debug_assert_eq!(scalar_values.len(), $self.meta_data.len());
        ScalarValue::iter_to_array(scalar_values).ok()
    }};
}

impl PruningStatistics for RowGroupPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        impl_min_max_values!(self, column, min)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        impl_min_max_values!(self, column, max)
    }

    fn num_containers(&self) -> usize {
        self.meta_data.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let (idx, _) = self.field_by_name(&column.name)?;
        let mut values: Vec<Option<u64>> = Vec::with_capacity(self.meta_data.len());
        for m in self.meta_data {
            if idx >= m.num_columns() {
                return None;
            }
            let col = m.column(idx);
            let stat = col.statistics()?;
            let bs = stat.null_count_opt()?;
            values.push(Some(bs));
        }
        Some(Arc::new(UInt64Array::from(values)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        // TODO(LFC): Impl it.
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        // TODO(LFC): Impl it.
        None
    }
}
