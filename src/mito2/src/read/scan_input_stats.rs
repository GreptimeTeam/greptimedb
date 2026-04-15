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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{Array, AsArray, UInt64Array};
use datatypes::arrow::compute::{cast, max, max_boolean, max_string, min, min_boolean, min_string};
use datatypes::arrow::datatypes::{
    DataType as ArrowDataType, Float32Type, Float64Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt32Type, UInt64Type,
};
use datatypes::value::Value;
use store_api::metadata::RegionMetadata;
use store_api::scan_stats::{
    RegionScanColumnStats as RegionScanColumnInputStats,
    RegionScanFileStats as RegionScanFileInputStats, RegionScanStats as RegionScanInputStats,
};

use crate::read::scan_region::ScanInput;
use crate::sst::file::FileHandle;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::stats::RowGroupPruningStats;

pub fn build_scan_input_stats(
    input: &ScanInput,
    metadata: &RegionMetadata,
) -> std::result::Result<RegionScanInputStats, BoxedError> {
    let files = input
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let partition_expr_matches_region = file_partition_expr_matches_region(metadata, file)?;
            Ok(RegionScanFileInputStats {
                file_ordinal: index,
                exact_num_rows: exact_file_num_rows(file),
                time_range: exact_file_time_range(file),
                field_stats: build_file_field_stats(input, metadata, file)?,
                partition_expr_matches_region,
            })
        })
        .collect::<std::result::Result<Vec<_>, BoxedError>>()?;

    Ok(RegionScanInputStats { files })
}

fn exact_file_num_rows(file: &FileHandle) -> Option<usize> {
    Some(file.num_rows())
}

fn exact_file_time_range(
    file: &FileHandle,
) -> Option<(common_time::Timestamp, common_time::Timestamp)> {
    (file.meta_ref().num_row_groups != 0).then_some(file.time_range())
}

fn build_file_field_stats(
    input: &ScanInput,
    metadata: &RegionMetadata,
    file: &FileHandle,
) -> std::result::Result<HashMap<String, RegionScanColumnInputStats>, BoxedError> {
    // TODO(ruihang): extract stats only for columns referenced by the supported aggregates
    // instead of eagerly materializing every field column for every file.
    let Some(parquet_meta) = input
        .cache_strategy
        .get_parquet_meta_data_from_mem_cache(file.file_id())
    else {
        return Ok(HashMap::new());
    };

    let region_metadata = Arc::new(metadata.clone());
    let file_path = format!("{:?}", file.file_id());
    let read_format = ReadFormat::new(
        region_metadata.clone(),
        None,
        input.flat_format,
        Some(parquet_meta.file_metadata().schema_descr().num_columns()),
        &file_path,
        false,
    )
    .map_err(BoxedError::new)?;
    let row_groups = parquet_meta.row_groups();
    let pruning_stats =
        RowGroupPruningStats::new(row_groups, &read_format, Some(region_metadata), false);

    metadata
        .column_metadatas
        .iter()
        .filter(|column| column.semantic_type == SemanticType::Field)
        .filter_map(|column| {
            let stats = build_field_column_stats(
                column.column_schema.name.as_str(),
                row_groups,
                &pruning_stats,
            )
            .transpose();
            match stats {
                Some(Ok(stats)) => Some(Ok((column.column_schema.name.to_string(), stats))),
                Some(Err(err)) => Some(Err(err)),
                None => None,
            }
        })
        .collect()
}

fn build_field_column_stats(
    column_name: &str,
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    pruning_stats: &impl PruningStatistics,
) -> std::result::Result<Option<RegionScanColumnInputStats>, BoxedError> {
    let column = Column::from_name(column_name);
    let min_value = aggregate_column_min_value(pruning_stats.min_values(&column).as_deref())?;
    let max_value = aggregate_column_max_value(pruning_stats.max_values(&column).as_deref())?;
    let exact_non_null_rows =
        aggregate_exact_non_null_rows(pruning_stats.null_counts(&column).as_deref(), row_groups)?;

    if min_value.is_none() && max_value.is_none() && exact_non_null_rows.is_none() {
        return Ok(None);
    }

    Ok(Some(RegionScanColumnInputStats {
        min_value,
        max_value,
        exact_non_null_rows,
    }))
}

fn aggregate_column_min_value(
    values: Option<&dyn Array>,
) -> std::result::Result<Option<Value>, BoxedError> {
    aggregate_column_extreme_value(values, true)
}

fn aggregate_column_max_value(
    values: Option<&dyn Array>,
) -> std::result::Result<Option<Value>, BoxedError> {
    aggregate_column_extreme_value(values, false)
}

fn aggregate_column_extreme_value(
    values: Option<&dyn Array>,
    is_min: bool,
) -> std::result::Result<Option<Value>, BoxedError> {
    let Some(values) = values else {
        return Ok(None);
    };
    if values.is_empty() || values.null_count() > 0 {
        return Ok(None);
    }

    if let Some(value) = aggregate_column_extreme_value_with_compute(values, is_min)? {
        return Ok(Some(value));
    }

    aggregate_column_extreme_value_fallback(values, is_min)
}

fn aggregate_column_extreme_value_with_compute(
    values: &dyn Array,
    is_min: bool,
) -> std::result::Result<Option<Value>, BoxedError> {
    if let ArrowDataType::Dictionary(_, value_type) = values.data_type() {
        let casted = cast(values, value_type.as_ref()).map_err(|err| {
            BoxedError::new(PlainError::new(
                format!("failed to cast dictionary stats array to value type: {err}"),
                StatusCode::Unexpected,
            ))
        })?;
        return aggregate_column_extreme_value_with_compute(casted.as_ref(), is_min);
    }

    macro_rules! compute_primitive_extreme {
        ($array_ty:ty, $variant:ident) => {{
            let array = values.as_primitive::<$array_ty>();
            let scalar = if is_min {
                min(array).map(|value| ScalarValue::$variant(Some(value)))
            } else {
                max(array).map(|value| ScalarValue::$variant(Some(value)))
            };
            scalar
                .map(|value| Value::try_from(value).map_err(BoxedError::new))
                .transpose()
        }};
    }

    macro_rules! compute_timestamp_extreme {
        ($array_ty:ty, $variant:ident, $tz:expr) => {{
            let array = values.as_primitive::<$array_ty>();
            let scalar = if is_min {
                min(array).map(|value| ScalarValue::$variant(Some(value), $tz.clone()))
            } else {
                max(array).map(|value| ScalarValue::$variant(Some(value), $tz.clone()))
            };
            scalar
                .map(|value| Value::try_from(value).map_err(BoxedError::new))
                .transpose()
        }};
    }

    match values.data_type() {
        ArrowDataType::Boolean => {
            let array = values.as_boolean();
            let scalar = if is_min {
                min_boolean(array).map(|value| ScalarValue::Boolean(Some(value)))
            } else {
                max_boolean(array).map(|value| ScalarValue::Boolean(Some(value)))
            };
            scalar
                .map(|value| Value::try_from(value).map_err(BoxedError::new))
                .transpose()
        }
        ArrowDataType::Utf8 => {
            let array = values.as_string::<i32>();
            let scalar = if is_min {
                min_string(array)
            } else {
                max_string(array)
            }
            .map(|value| ScalarValue::Utf8(Some(value.to_string())));
            scalar
                .map(|value| Value::try_from(value).map_err(BoxedError::new))
                .transpose()
        }
        ArrowDataType::LargeUtf8 => {
            let array = values.as_string::<i64>();
            let scalar = if is_min {
                min_string(array)
            } else {
                max_string(array)
            }
            .map(|value| ScalarValue::LargeUtf8(Some(value.to_string())));
            scalar
                .map(|value| Value::try_from(value).map_err(BoxedError::new))
                .transpose()
        }
        ArrowDataType::UInt32 => compute_primitive_extreme!(UInt32Type, UInt32),
        ArrowDataType::UInt64 => compute_primitive_extreme!(UInt64Type, UInt64),
        ArrowDataType::Int32 => compute_primitive_extreme!(Int32Type, Int32),
        ArrowDataType::Int64 => compute_primitive_extreme!(Int64Type, Int64),
        ArrowDataType::Float32 => compute_primitive_extreme!(Float32Type, Float32),
        ArrowDataType::Float64 => compute_primitive_extreme!(Float64Type, Float64),
        ArrowDataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, tz) => {
            compute_timestamp_extreme!(TimestampSecondType, TimestampSecond, tz)
        }
        ArrowDataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, tz) => {
            compute_timestamp_extreme!(TimestampMillisecondType, TimestampMillisecond, tz)
        }
        ArrowDataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Microsecond, tz) => {
            compute_timestamp_extreme!(TimestampMicrosecondType, TimestampMicrosecond, tz)
        }
        ArrowDataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Nanosecond, tz) => {
            compute_timestamp_extreme!(TimestampNanosecondType, TimestampNanosecond, tz)
        }
        _ => Ok(None),
    }
}

fn aggregate_column_extreme_value_fallback(
    values: &dyn Array,
    is_min: bool,
) -> std::result::Result<Option<Value>, BoxedError> {
    let scalars = (0..values.len())
        .map(|index| {
            ScalarValue::try_from_array(values, index).map_err(|err| {
                BoxedError::new(PlainError::new(
                    format!("failed to extract scalar value from stats array: {err}"),
                    StatusCode::Unexpected,
                ))
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut iter = scalars
        .into_iter()
        .map(|value| Value::try_from(value).map_err(BoxedError::new));
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    let first = first?;

    iter.try_fold(first, |current, value| {
        let value = value?;
        let next = if is_min {
            Value::min(current, value)
        } else {
            Value::max(current, value)
        };
        Ok::<_, BoxedError>(next)
    })
    .map(Some)
}

fn aggregate_exact_non_null_rows(
    null_counts: Option<&dyn Array>,
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
) -> std::result::Result<Option<usize>, BoxedError> {
    let Some(null_counts) = null_counts else {
        return Ok(None);
    };
    if null_counts.null_count() > 0 {
        return Ok(None);
    }
    let Some(null_counts) = null_counts.as_any().downcast_ref::<UInt64Array>() else {
        return Ok(None);
    };

    row_groups
        .iter()
        .zip(null_counts.iter())
        .try_fold(0usize, |acc, (row_group, null_count)| {
            let row_count = usize::try_from(row_group.num_rows()).map_err(|err| {
                BoxedError::new(PlainError::new(
                    format!("failed to convert row group row count to usize: {err}"),
                    StatusCode::Unexpected,
                ))
            })?;
            let null_count = usize::try_from(null_count.unwrap_or_default()).map_err(|err| {
                BoxedError::new(PlainError::new(
                    format!("failed to convert parquet null count to usize: {err}"),
                    StatusCode::Unexpected,
                ))
            })?;
            Ok::<_, BoxedError>(acc + row_count.saturating_sub(null_count))
        })
        .map(Some)
}

fn file_partition_expr_matches_region(
    metadata: &RegionMetadata,
    file: &FileHandle,
) -> std::result::Result<bool, BoxedError> {
    let file_partition_expr = file
        .meta_ref()
        .partition_expr
        .as_ref()
        .map(|expr| expr.as_json_str())
        .transpose()
        .map_err(BoxedError::new)?;
    Ok(file_partition_expr == metadata.partition_expr)
}

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit as TimestampUnit;
    use datatypes::arrow::array::{
        DictionaryArray, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use datatypes::arrow::datatypes::Int32Type;

    use super::*;

    #[test]
    fn test_aggregate_column_extreme_value_uses_numeric_fast_path() {
        let values = UInt64Array::from(vec![Some(7), Some(2), Some(11)]);

        let min_value = aggregate_column_min_value(Some(&values)).unwrap();
        let max_value = aggregate_column_max_value(Some(&values)).unwrap();

        assert_eq!(Some(Value::UInt64(2)), min_value);
        assert_eq!(Some(Value::UInt64(11)), max_value);
    }

    #[test]
    fn test_aggregate_column_extreme_value_uses_string_fast_path() {
        let values = StringArray::from(vec![Some("delta"), Some("alpha"), Some("gamma")]);

        let min_value = aggregate_column_min_value(Some(&values)).unwrap();
        let max_value = aggregate_column_max_value(Some(&values)).unwrap();

        assert_eq!(Some(Value::String("alpha".into())), min_value);
        assert_eq!(Some(Value::String("gamma".into())), max_value);
    }

    #[test]
    fn test_aggregate_column_extreme_value_uses_timestamp_fast_path() {
        let values = TimestampMillisecondArray::from(vec![Some(7), Some(2), Some(11)]);

        let min_value = aggregate_column_min_value(Some(&values)).unwrap();
        let max_value = aggregate_column_max_value(Some(&values)).unwrap();

        assert_eq!(
            Some(Value::Timestamp(common_time::Timestamp::new(
                2,
                TimestampUnit::Millisecond
            ))),
            min_value
        );
        assert_eq!(
            Some(Value::Timestamp(common_time::Timestamp::new(
                11,
                TimestampUnit::Millisecond
            ))),
            max_value
        );
    }

    #[test]
    fn test_aggregate_column_extreme_value_dictionary_falls_back() {
        let values =
            DictionaryArray::<Int32Type>::from_iter([Some("delta"), Some("alpha"), Some("gamma")]);

        let min_value = aggregate_column_min_value(Some(&values)).unwrap();
        let max_value = aggregate_column_max_value(Some(&values)).unwrap();

        assert_eq!(Some(Value::String("alpha".into())), min_value);
        assert_eq!(Some(Value::String("gamma".into())), max_value);
    }

    #[test]
    fn test_aggregate_column_extreme_value_returns_none_when_any_stats_are_null() {
        let values = Int64Array::from(vec![Some(7), None, Some(11)]);

        assert_eq!(None, aggregate_column_min_value(Some(&values)).unwrap());
        assert_eq!(None, aggregate_column_max_value(Some(&values)).unwrap());
    }
}
