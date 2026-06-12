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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use datafusion::parquet::file::statistics::Statistics as ParquetStats;
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::schema::SchemaRef as RegionSchemaRef;
use datatypes::value::Value;
use store_api::region_engine::{FileStatsItem, SupportedStatAggr};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FileColumnStats {
    pub null_count: Option<u64>,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StatsCandidateFile {
    pub num_rows: Option<u64>,
    pub column_stats: HashMap<String, FileColumnStats>,
}

impl StatsCandidateFile {
    /// Builds a candidate from raw file-parquet stats, returning `Ok(None)` when
    /// the file is unsuitable for aggregate-stats optimisation (partition
    /// mismatch, missing metadata, or unsatisfiable requirement).
    pub fn from_file_stats(
        file_stats: &FileStatsItem,
        region_partition_expr: Option<&str>,
        requirements: &[SupportedStatAggr],
        region_schema: &RegionSchemaRef,
    ) -> Result<Option<Self>> {
        // Check partition match first — cheapest rejection.
        if !matches_partition_expr(
            file_stats.file_partition_expr.as_deref(),
            region_partition_expr,
        ) {
            return Ok(None);
        }

        // CountRows only needs num_rows — skip column-stats collection when
        // num_rows is already missing (file will be rejected anyway).
        let only_need_row_count = requirements
            .iter()
            .all(|r| matches!(r, SupportedStatAggr::CountRows));
        if only_need_row_count && file_stats.num_rows.is_none() {
            return Ok(None);
        }

        let column_names = required_columns(requirements);
        let column_stats = collect_column_stats(file_stats, region_schema, &column_names)?;

        let candidate = Self {
            num_rows: file_stats.num_rows,
            column_stats,
        };
        for requirement in requirements {
            if candidate.stat_value(requirement)?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(candidate))
    }

    pub fn stat_value(&self, requirement: &SupportedStatAggr) -> Result<Option<Value>> {
        match requirement {
            SupportedStatAggr::CountRows => self.num_rows.map(count_value).transpose(),
            SupportedStatAggr::CountNonNull { column_name } => {
                let Some(column_stats) = self.column_stats.get(column_name) else {
                    return Ok(None);
                };
                let Some(num_rows) = self.num_rows else {
                    return Ok(None);
                };
                let Some(null_count) = column_stats.null_count else {
                    return Ok(None);
                };
                let Some(non_null_count) = num_rows.checked_sub(null_count) else {
                    return Err(DataFusionError::Internal(format!(
                        "StatsScanExec found null_count > num_rows for column {}",
                        column_name
                    )));
                };
                count_value(non_null_count).map(Some)
            }
            SupportedStatAggr::MinValue { column_name } => Ok(self
                .column_stats
                .get(column_name)
                .and_then(|stats| stats.min_value.clone())),
            SupportedStatAggr::MaxValue { column_name } => Ok(self
                .column_stats
                .get(column_name)
                .and_then(|stats| stats.max_value.clone())),
        }
    }
}

/// Converts a row count (or non-null count) into a [`Value`] for aggregate-stats
/// output.  Currently maps to `Value::Int64` because the downstream aggregate-stats
/// consumer (e.g. `AggrStats` optimizer rewrite rule in follow-up PRs) uses
/// `DataType::Int64` for count columns.  If that contract changes we can switch to
/// `Value::UInt64` without modifying the file-rejection logic above.
///
/// Returns an error when the count exceeds `i64::MAX` — in practice a parquet file
/// with > 9.2 × 10¹⁸ rows is impossible, but we validate defensively.
fn count_value(value: u64) -> Result<Value> {
    let value = i64::try_from(value).map_err(|_| {
        DataFusionError::Internal(format!(
            "StatsScanExec count state exceeds Int64 range: {}",
            value
        ))
    })?;
    Ok(Value::Int64(value))
}

/// Returns whether the file belongs to the same partition as the region.
///
/// | file_expr | region_expr | result | rationale                    |
/// |-----------|-------------|--------|------------------------------|
/// | None      | None        | true   | both unpartitioned           |
/// | Some(a)   | Some(b)     | a == b | string equality match        |
/// | None      | Some(_)     | false  | unpartitioned file in partitioned region — mismatch |
/// | Some(_)   | None        | false  | partitioned file in unpartitioned region — mismatch |
fn matches_partition_expr(
    file_partition_expr: Option<&str>,
    region_partition_expr: Option<&str>,
) -> bool {
    match (file_partition_expr, region_partition_expr) {
        (Some(file_expr), Some(region_expr)) => file_expr == region_expr,
        (None, None) => true,
        _ => false,
    }
}

fn required_columns(requirements: &[SupportedStatAggr]) -> HashSet<String> {
    requirements
        .iter()
        .filter_map(|requirement| match requirement {
            SupportedStatAggr::CountRows => None,
            SupportedStatAggr::CountNonNull { column_name }
            | SupportedStatAggr::MinValue { column_name }
            | SupportedStatAggr::MaxValue { column_name } => Some(column_name.clone()),
        })
        .collect()
}

fn collect_column_stats(
    file_stats: &FileStatsItem,
    region_schema: &RegionSchemaRef,
    column_names: &HashSet<String>,
) -> Result<HashMap<String, FileColumnStats>> {
    column_names
        .iter()
        .map(|column_name| {
            Ok((
                column_name.clone(),
                collect_one_column_stats(file_stats, region_schema, column_name)?,
            ))
        })
        .collect()
}

fn collect_one_column_stats(
    file_stats: &FileStatsItem,
    region_schema: &RegionSchemaRef,
    column_name: &str,
) -> Result<FileColumnStats> {
    let Some(column_index) = region_schema.column_index_by_name(column_name) else {
        return Ok(FileColumnStats::default());
    };

    let arrow_type = region_schema.arrow_schema().field(column_index).data_type();

    Ok(FileColumnStats {
        null_count: sum_null_counts(file_stats, column_index)?,
        min_value: best_row_group_value(file_stats, column_index, Ordering::Less, arrow_type)?,
        max_value: best_row_group_value(file_stats, column_index, Ordering::Greater, arrow_type)?,
    })
}

fn sum_null_counts(file_stats: &FileStatsItem, column_index: usize) -> Result<Option<u64>> {
    if file_stats.row_groups.is_empty() {
        return Ok(None);
    }

    let mut total = 0_u64;
    for row_group in &file_stats.row_groups {
        if column_index >= row_group.metadata.num_columns() {
            return Ok(None);
        }
        let Some(stats) = row_group.metadata.column(column_index).statistics() else {
            return Ok(None);
        };
        let Some(value) = stats.null_count_opt() else {
            return Ok(None);
        };
        total = total.checked_add(value).ok_or_else(|| {
            DataFusionError::Internal("StatsScanExec null-count overflow".to_string())
        })?;
    }
    Ok(Some(total))
}

fn best_row_group_value(
    file_stats: &FileStatsItem,
    column_index: usize,
    target: Ordering,
    arrow_type: &DataType,
) -> Result<Option<Value>> {
    let mut best = None;

    for row_group in &file_stats.row_groups {
        if column_index >= row_group.metadata.num_columns() {
            return Ok(None);
        }
        let Some(stats) = row_group.metadata.column(column_index).statistics() else {
            return Ok(None);
        };
        let Some(scalar) = parquet_bound_scalar(stats, target, arrow_type) else {
            return Ok(None);
        };
        let value = Value::try_from(scalar).map_err(|error| {
            DataFusionError::Internal(format!(
                "StatsScanExec failed to convert row-group scalar: {}",
                error
            ))
        })?;
        let should_replace = best.as_ref().is_none_or(|current| {
            value
                .partial_cmp(current)
                .is_some_and(|ordering| ordering == target)
        });
        if should_replace {
            best = Some(value);
        }
    }

    Ok(best)
}

/// Converts a parquet column-bound statistic into the logical [`ScalarValue`]
/// appropriate for the column's Arrow [`DataType`].
///
/// **Keep in sync** with the type-matching logic in
/// `src/table/src/predicate/stats.rs`'s `impl_min_max_values!` macro.
///
/// # NaN handling
/// Parquet float/double stats can contain NaN. Through `ScalarValue::Float32` →
/// `Value::Float32(OrderedF32)`, NaN is preserved and placed above all finite
/// values by `OrderedFloat` ordering semantics, so:
/// - Max: NaN becomes the file max if any row group contains it
/// - Min: NaN won't be selected as the minimum (NaN > finite)
fn parquet_bound_scalar(
    stats: &ParquetStats,
    target: Ordering,
    arrow_type: &DataType,
) -> Option<ScalarValue> {
    let use_min = target == Ordering::Less;

    match stats {
        ParquetStats::Boolean(stats) => {
            if !matches!(arrow_type, DataType::Boolean) {
                return None;
            }
            Some(ScalarValue::Boolean(Some(if use_min {
                *stats.min_opt()?
            } else {
                *stats.max_opt()?
            })))
        }
        ParquetStats::Int32(stats) => {
            let raw = if use_min {
                *stats.min_opt()?
            } else {
                *stats.max_opt()?
            };
            match arrow_type {
                DataType::Int32
                | DataType::UInt32
                | DataType::Int16
                | DataType::UInt16
                | DataType::Int8
                | DataType::UInt8 => Some(ScalarValue::Int32(Some(raw))),
                DataType::Time32(TimeUnit::Second) => Some(ScalarValue::Time32Second(Some(raw))),
                DataType::Time32(TimeUnit::Millisecond) => {
                    Some(ScalarValue::Time32Millisecond(Some(raw)))
                }
                DataType::Date32 => Some(ScalarValue::Date32(Some(raw))),
                _ => None,
            }
        }
        ParquetStats::Int64(stats) => {
            let raw = if use_min {
                *stats.min_opt()?
            } else {
                *stats.max_opt()?
            };
            match arrow_type {
                DataType::Int64 | DataType::UInt64 => Some(ScalarValue::Int64(Some(raw))),
                // Timestamp timezone is tracked separately in GreptimeDB's
                // extension-type system, not in Arrow DataType, so we pass None
                // here — consistent with `datatypes::value::timestamp_to_scalar_value`.
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
                // Date64 is not supported by Value::try_from (see datatypes/src/value.rs:1232).
                // Producing ScalarValue::Date64 here would cause a runtime error when
                // converting to Value in best_row_group_value(). Return None to safely
                // fall back to row-scan.
                DataType::Date64 => None,
                _ => None,
            }
        }
        ParquetStats::Int96(_) => None,
        ParquetStats::Float(stats) => {
            if !matches!(arrow_type, DataType::Float32) {
                return None;
            }
            // NaN is preserved through OrderedFloat; see module-level doc above.
            Some(ScalarValue::Float32(Some(if use_min {
                *stats.min_opt()?
            } else {
                *stats.max_opt()?
            })))
        }
        ParquetStats::Double(stats) => {
            if !matches!(arrow_type, DataType::Float64) {
                return None;
            }
            // NaN is preserved through OrderedFloat; see module-level doc above.
            Some(ScalarValue::Float64(Some(if use_min {
                *stats.min_opt()?
            } else {
                *stats.max_opt()?
            })))
        }
        ParquetStats::ByteArray(stats) => {
            let bytes = if use_min {
                stats.min_bytes_opt()?
            } else {
                stats.max_bytes_opt()?
            };
            match arrow_type {
                DataType::Utf8 | DataType::LargeUtf8 => String::from_utf8(bytes.to_owned())
                    .ok()
                    .map(|s| ScalarValue::Utf8(Some(s))),
                _ => None,
            }
        }
        ParquetStats::FixedLenByteArray(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::parquet::data_type::ByteArray;
    use datafusion::parquet::file::statistics::Statistics as ParquetStats;
    use datatypes::arrow::datatypes::TimeUnit;
    use datatypes::schema::Schema;

    use super::*;

    // ---------------------------------------------------------------------------
    // matches_partition_expr
    // ---------------------------------------------------------------------------

    #[test]
    fn matches_partition_both_none() {
        assert!(matches_partition_expr(None, None));
    }

    #[test]
    fn matches_partition_both_same() {
        assert!(matches_partition_expr(Some("a = 1"), Some("a = 1")));
    }

    #[test]
    fn matches_partition_both_diff() {
        assert!(!matches_partition_expr(Some("a = 1"), Some("b = 2")));
    }

    #[test]
    fn matches_partition_file_none_region_some() {
        // Unpartitioned file cannot serve a partitioned region.
        assert!(!matches_partition_expr(None, Some("a = 1")));
    }

    #[test]
    fn matches_partition_file_some_region_none() {
        // Partitioned file cannot serve an unpartitioned region.
        assert!(!matches_partition_expr(Some("a = 1"), None));
    }

    // ---------------------------------------------------------------------------
    // required_columns
    // ---------------------------------------------------------------------------

    #[test]
    fn required_columns_empty() {
        assert!(required_columns(&[]).is_empty());
    }

    #[test]
    fn required_columns_count_rows_only() {
        assert!(required_columns(&[SupportedStatAggr::CountRows]).is_empty());
    }

    #[test]
    fn required_columns_extracts_column_names() {
        let reqs = &[
            SupportedStatAggr::CountNonNull {
                column_name: "col_a".into(),
            },
            SupportedStatAggr::MinValue {
                column_name: "col_b".into(),
            },
            SupportedStatAggr::MaxValue {
                column_name: "col_a".into(),
            },
        ];
        let cols = required_columns(reqs);
        assert_eq!(cols.len(), 2);
        assert!(cols.contains("col_a"));
        assert!(cols.contains("col_b"));
    }

    // ---------------------------------------------------------------------------
    // count_value
    // ---------------------------------------------------------------------------

    #[test]
    fn count_value_zero() {
        assert_eq!(count_value(0).unwrap(), Value::Int64(0));
    }

    #[test]
    fn count_value_large() {
        assert_eq!(count_value(1_000_000).unwrap(), Value::Int64(1_000_000));
    }

    #[test]
    fn count_value_i64_max() {
        assert_eq!(
            count_value(i64::MAX as u64).unwrap(),
            Value::Int64(i64::MAX)
        );
    }

    #[test]
    fn count_value_overflow() {
        assert!(count_value(i64::MAX as u64 + 1).is_err());
    }

    // ---------------------------------------------------------------------------
    // parquet_bound_scalar — type mapping correctness
    // ---------------------------------------------------------------------------

    fn stats_boolean(
        min: Option<bool>,
        max: Option<bool>,
        null_count: Option<u64>,
    ) -> ParquetStats {
        ParquetStats::boolean(min, max, None, null_count, false)
    }

    fn stats_i32(min: Option<i32>, max: Option<i32>, null_count: Option<u64>) -> ParquetStats {
        ParquetStats::int32(min, max, None, null_count, false)
    }

    fn stats_i64(min: Option<i64>, max: Option<i64>, null_count: Option<u64>) -> ParquetStats {
        ParquetStats::int64(min, max, None, null_count, false)
    }

    fn scalar_min(stats: &ParquetStats, dt: &DataType) -> Option<ScalarValue> {
        parquet_bound_scalar(stats, Ordering::Less, dt)
    }

    fn scalar_max(stats: &ParquetStats, dt: &DataType) -> Option<ScalarValue> {
        parquet_bound_scalar(stats, Ordering::Greater, dt)
    }

    #[test]
    fn bound_scalar_boolean() {
        let s = stats_boolean(Some(false), Some(true), Some(0));
        assert_eq!(
            scalar_min(&s, &DataType::Boolean),
            Some(ScalarValue::Boolean(Some(false)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Boolean),
            Some(ScalarValue::Boolean(Some(true)))
        );
    }

    #[test]
    fn bound_scalar_boolean_type_mismatch() {
        let s = stats_boolean(Some(false), Some(true), Some(0));
        // Int32 column with Boolean parquet stats → None (type mismatch)
        assert!(scalar_min(&s, &DataType::Int32).is_none());
    }

    #[test]
    fn bound_scalar_int32_to_int_variants() {
        let s = stats_i32(Some(-10), Some(99), Some(1));
        // Int32
        assert_eq!(
            scalar_min(&s, &DataType::Int32),
            Some(ScalarValue::Int32(Some(-10)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Int32),
            Some(ScalarValue::Int32(Some(99)))
        );
        // UInt32
        assert_eq!(
            scalar_min(&s, &DataType::UInt32),
            Some(ScalarValue::Int32(Some(-10)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::UInt32),
            Some(ScalarValue::Int32(Some(99)))
        );
        // Int8
        assert_eq!(
            scalar_min(&s, &DataType::Int8),
            Some(ScalarValue::Int32(Some(-10)))
        );
        // Date32
        assert_eq!(
            scalar_min(&s, &DataType::Date32),
            Some(ScalarValue::Date32(Some(-10)))
        );
    }

    #[test]
    fn bound_scalar_int32_time32_second() {
        let s = stats_i32(Some(3600), Some(7200), Some(0));
        assert_eq!(
            scalar_min(&s, &DataType::Time32(TimeUnit::Second)),
            Some(ScalarValue::Time32Second(Some(3600)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Time32(TimeUnit::Second)),
            Some(ScalarValue::Time32Second(Some(7200)))
        );
    }

    #[test]
    fn bound_scalar_int32_time32_millisecond() {
        let s = stats_i32(Some(5000), Some(15000), Some(0));
        assert_eq!(
            scalar_min(&s, &DataType::Time32(TimeUnit::Millisecond)),
            Some(ScalarValue::Time32Millisecond(Some(5000)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Time32(TimeUnit::Millisecond)),
            Some(ScalarValue::Time32Millisecond(Some(15000)))
        );
    }

    #[test]
    fn bound_scalar_int64_to_timestamp_variants() {
        let s = stats_i64(Some(1_000_000), Some(5_000_000), Some(0));
        // TimestampSecond
        assert_eq!(
            scalar_min(&s, &DataType::Timestamp(TimeUnit::Second, None)),
            Some(ScalarValue::TimestampSecond(Some(1_000_000), None))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Timestamp(TimeUnit::Second, None)),
            Some(ScalarValue::TimestampSecond(Some(5_000_000), None))
        );
        // TimestampMillisecond — same raw i64 value, different ScalarValue variant
        assert_eq!(
            scalar_min(&s, &DataType::Timestamp(TimeUnit::Millisecond, None)),
            Some(ScalarValue::TimestampMillisecond(Some(1_000_000), None))
        );
        // TimestampMicrosecond
        assert_eq!(
            scalar_min(&s, &DataType::Timestamp(TimeUnit::Microsecond, None)),
            Some(ScalarValue::TimestampMicrosecond(Some(1_000_000), None))
        );
        // TimestampNanosecond
        assert_eq!(
            scalar_min(&s, &DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Some(ScalarValue::TimestampNanosecond(Some(1_000_000), None))
        );
    }

    #[test]
    fn bound_scalar_int64_time64_variants() {
        let s = stats_i64(Some(42), Some(99), Some(0));
        assert_eq!(
            scalar_min(&s, &DataType::Time64(TimeUnit::Microsecond)),
            Some(ScalarValue::Time64Microsecond(Some(42)))
        );
        assert_eq!(
            scalar_min(&s, &DataType::Time64(TimeUnit::Nanosecond)),
            Some(ScalarValue::Time64Nanosecond(Some(42)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Time64(TimeUnit::Microsecond)),
            Some(ScalarValue::Time64Microsecond(Some(99)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Time64(TimeUnit::Nanosecond)),
            Some(ScalarValue::Time64Nanosecond(Some(99)))
        );
    }

    #[test]
    fn bound_scalar_int64_duration_variants() {
        let s = stats_i64(Some(100), Some(200), Some(0));
        assert_eq!(
            scalar_min(&s, &DataType::Duration(TimeUnit::Second)),
            Some(ScalarValue::DurationSecond(Some(100)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Duration(TimeUnit::Millisecond)),
            Some(ScalarValue::DurationMillisecond(Some(200)))
        );
        assert_eq!(
            scalar_min(&s, &DataType::Duration(TimeUnit::Microsecond)),
            Some(ScalarValue::DurationMicrosecond(Some(100)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Duration(TimeUnit::Nanosecond)),
            Some(ScalarValue::DurationNanosecond(Some(200)))
        );
    }

    #[test]
    fn bound_scalar_int64_date64_rejected() {
        let s = stats_i64(Some(1), Some(10), Some(0));
        // Date64 is not supported by Value::try_from — should be None (safe fallback)
        assert!(scalar_min(&s, &DataType::Date64).is_none());
        assert!(scalar_max(&s, &DataType::Date64).is_none());
    }

    #[test]
    fn bound_scalar_int32_type_mismatch_returns_none() {
        // Int32 parquet stats for a Timestamp column → type mismatch → None
        let s = stats_i32(Some(1), Some(100), Some(0));
        assert!(scalar_min(&s, &DataType::Timestamp(TimeUnit::Second, None)).is_none());
        assert!(scalar_min(&s, &DataType::Time64(TimeUnit::Microsecond)).is_none());
        assert!(scalar_min(&s, &DataType::Duration(TimeUnit::Second)).is_none());
    }

    #[test]
    fn bound_scalar_int64_type_mismatch_returns_none() {
        // Int64 parquet stats for a Time32 column → type mismatch → None
        let s = stats_i64(Some(1), Some(100), Some(0));
        assert!(scalar_min(&s, &DataType::Time32(TimeUnit::Second)).is_none());
        assert!(scalar_min(&s, &DataType::Int32).is_none());
    }

    #[test]
    fn bound_scalar_float32() {
        let s = ParquetStats::float(Some(1.0_f32), Some(10.0_f32), None, Some(0), false);
        assert_eq!(
            scalar_min(&s, &DataType::Float32),
            Some(ScalarValue::Float32(Some(1.0)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Float32),
            Some(ScalarValue::Float32(Some(10.0)))
        );
        // Type mismatch
        assert!(scalar_min(&s, &DataType::Float64).is_none());
    }

    #[test]
    fn bound_scalar_float64() {
        let s = ParquetStats::double(Some(1.0_f64), Some(10.0_f64), None, Some(0), false);
        assert_eq!(
            scalar_min(&s, &DataType::Float64),
            Some(ScalarValue::Float64(Some(1.0)))
        );
        assert_eq!(
            scalar_max(&s, &DataType::Float64),
            Some(ScalarValue::Float64(Some(10.0)))
        );
        // Type mismatch
        assert!(scalar_min(&s, &DataType::Float32).is_none());
    }

    #[test]
    fn bound_scalar_byte_array_utf8() {
        let b = ByteArray::from("hello");
        let b2 = ByteArray::from("world");
        let s = ParquetStats::byte_array(Some(b), Some(b2), None, Some(0), false);
        assert_eq!(
            scalar_min(&s, &DataType::Utf8),
            Some(ScalarValue::Utf8(Some("hello".to_string())))
        );
        assert_eq!(
            scalar_max(&s, &DataType::LargeUtf8),
            Some(ScalarValue::Utf8(Some("world".to_string())))
        );
    }

    #[test]
    fn bound_scalar_byte_array_non_utf8() {
        let invalid = ByteArray::from(vec![0xff, 0xfe, 0xfd]);
        let s =
            ParquetStats::byte_array(Some(invalid.clone()), Some(invalid), None, Some(0), false);
        // Invalid UTF-8 → returns None (string::from_utf8 fails)
        assert!(scalar_min(&s, &DataType::Utf8).is_none());
    }

    #[test]
    fn bound_scalar_byte_array_type_mismatch() {
        let b = ByteArray::from("test");
        let s = ParquetStats::byte_array(Some(b.clone()), Some(b), None, Some(0), false);
        // ByteArray stats for a non-string column → None
        assert!(scalar_min(&s, &DataType::Int32).is_none());
    }

    #[test]
    fn bound_scalar_int96_rejected() {
        let s = ParquetStats::int96(None, None, None, None, false);
        assert!(scalar_min(&s, &DataType::Int32).is_none());
    }

    #[test]
    fn bound_scalar_fixed_len_byte_array_rejected() {
        let s = ParquetStats::fixed_len_byte_array(None, None, None, None, false);
        assert!(scalar_min(&s, &DataType::Utf8).is_none());
    }

    // ---------------------------------------------------------------------------
    // StatsCandidateFile::stat_value
    // ---------------------------------------------------------------------------

    fn make_candidate(
        num_rows: Option<u64>,
        column_stats: HashMap<String, FileColumnStats>,
    ) -> StatsCandidateFile {
        StatsCandidateFile {
            num_rows,
            column_stats,
        }
    }

    #[test]
    fn stat_value_count_rows_some() {
        let c = make_candidate(Some(42), HashMap::new());
        assert_eq!(
            c.stat_value(&SupportedStatAggr::CountRows).unwrap(),
            Some(Value::Int64(42))
        );
    }

    #[test]
    fn stat_value_count_rows_none() {
        let c = make_candidate(None, HashMap::new());
        assert!(
            c.stat_value(&SupportedStatAggr::CountRows)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn stat_value_count_non_null() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "col".to_string(),
            FileColumnStats {
                null_count: Some(30),
                min_value: None,
                max_value: None,
            },
        );
        let c = make_candidate(Some(100), col_stats);
        assert_eq!(
            c.stat_value(&SupportedStatAggr::CountNonNull {
                column_name: "col".into()
            })
            .unwrap(),
            Some(Value::Int64(70))
        );
    }

    #[test]
    fn stat_value_count_non_null_missing_column() {
        let c = make_candidate(Some(100), HashMap::new());
        assert_eq!(
            c.stat_value(&SupportedStatAggr::CountNonNull {
                column_name: "missing".into()
            })
            .unwrap(),
            None
        );
    }

    #[test]
    fn stat_value_count_non_null_num_rows_none() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "col".to_string(),
            FileColumnStats {
                null_count: Some(30),
                min_value: None,
                max_value: None,
            },
        );
        let c = make_candidate(None, col_stats);
        assert_eq!(
            c.stat_value(&SupportedStatAggr::CountNonNull {
                column_name: "col".into()
            })
            .unwrap(),
            None
        );
    }

    #[test]
    fn stat_value_count_non_null_null_count_none() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "col".to_string(),
            FileColumnStats {
                null_count: None,
                min_value: None,
                max_value: None,
            },
        );
        let c = make_candidate(Some(100), col_stats);
        assert_eq!(
            c.stat_value(&SupportedStatAggr::CountNonNull {
                column_name: "col".into()
            })
            .unwrap(),
            None
        );
    }

    #[test]
    fn stat_value_count_non_null_null_gt_rows() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "col".to_string(),
            FileColumnStats {
                null_count: Some(150),
                min_value: None,
                max_value: None,
            },
        );
        let c = make_candidate(Some(100), col_stats);
        assert!(
            c.stat_value(&SupportedStatAggr::CountNonNull {
                column_name: "col".into()
            })
            .is_err()
        );
    }

    #[test]
    fn stat_value_min_value_present() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "col".to_string(),
            FileColumnStats {
                null_count: None,
                min_value: Some(Value::Int32(3)),
                max_value: Some(Value::Int32(99)),
            },
        );
        let c = make_candidate(Some(100), col_stats);
        assert_eq!(
            c.stat_value(&SupportedStatAggr::MinValue {
                column_name: "col".into()
            })
            .unwrap(),
            Some(Value::Int32(3))
        );
        assert_eq!(
            c.stat_value(&SupportedStatAggr::MaxValue {
                column_name: "col".into()
            })
            .unwrap(),
            Some(Value::Int32(99))
        );
    }

    #[test]
    fn stat_value_min_value_missing_column() {
        let c = make_candidate(Some(100), HashMap::new());
        assert_eq!(
            c.stat_value(&SupportedStatAggr::MinValue {
                column_name: "col".into()
            })
            .unwrap(),
            None
        );
    }

    #[test]
    fn stat_value_min_value_none() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "col".to_string(),
            FileColumnStats {
                null_count: None,
                min_value: None,
                max_value: None,
            },
        );
        let c = make_candidate(Some(100), col_stats);
        assert_eq!(
            c.stat_value(&SupportedStatAggr::MinValue {
                column_name: "col".into()
            })
            .unwrap(),
            None
        );
    }

    // ---------------------------------------------------------------------------
    // StatsCandidateFile::from_file_stats
    // ---------------------------------------------------------------------------

    #[test]
    fn from_file_stats_count_rows_num_rows_none_rejected() {
        let file_stats = FileStatsItem {
            file_id: "f".into(),
            num_rows: None,
            file_partition_expr: None,
            row_groups: vec![],
        };
        let region_schema: RegionSchemaRef = Arc::new(Schema::new(vec![]));
        let candidate = StatsCandidateFile::from_file_stats(
            &file_stats,
            None,
            &[SupportedStatAggr::CountRows],
            &region_schema,
        )
        .unwrap();
        assert!(candidate.is_none());
    }

    #[test]
    fn from_file_stats_mixed_req_short_circuits_when_count_rows_only_and_missing() {
        // Only CountRows is requested and num_rows is None — early return before
        // column-stats collection (partition already matched).
        let file_stats = FileStatsItem {
            file_id: "f".into(),
            num_rows: None,
            file_partition_expr: None,
            row_groups: vec![],
        };
        let region_schema: RegionSchemaRef = Arc::new(Schema::new(vec![]));
        let candidate = StatsCandidateFile::from_file_stats(
            &file_stats,
            None,
            &[SupportedStatAggr::CountRows],
            &region_schema,
        )
        .unwrap();
        assert!(candidate.is_none());
    }

    // ---------------------------------------------------------------------------
    // TODO: RowGroupMetaData-dependent tests
    //
    // The following functions require constructed RowGroupMetaData with real
    // column statistics.  Because parquet-57.3 RowGroupMetaData needs a full
    // SchemaDescriptor and ColumnChunkMetaData chain, these are best covered
    // by integration tests that read actual parquet files:
    //
    //   - best_row_group_value: min/max across multiple row groups
    //   - sum_null_counts: multi-row-group aggregation, overflow
    //   - collect_one_column_stats: column missing from schema, missing from a single row group
    //   - collet_column_stats: integration of the above
    //   - from_file_stats with non-empty row_groups for CountNonNull, MinValue, MaxValue
    // ---------------------------------------------------------------------------
}
