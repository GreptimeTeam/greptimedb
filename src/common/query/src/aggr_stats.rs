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
use datatypes::schema::SchemaRef as RegionSchemaRef;
use datatypes::value::Value;
use store_api::region_engine::{FileStatsItem, SupportStatAggr};

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
    pub fn from_file_stats(
        file_stats: &FileStatsItem,
        region_partition_expr: Option<&str>,
        requirements: &[SupportStatAggr],
        region_schema: &RegionSchemaRef,
    ) -> Result<Option<Self>> {
        let column_names = required_columns(requirements);
        let column_stats = collect_column_stats(file_stats, region_schema, &column_names)?;
        if !matches_partition_expr(
            file_stats.file_partition_expr.as_deref(),
            region_partition_expr,
        ) {
            return Ok(None);
        }

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

    pub fn stat_value(&self, requirement: &SupportStatAggr) -> Result<Option<Value>> {
        match requirement {
            SupportStatAggr::CountRows => self.num_rows.map(count_value).transpose(),
            SupportStatAggr::CountNonNull { column_name } => {
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
            SupportStatAggr::MinValue { column_name } => Ok(self
                .column_stats
                .get(column_name)
                .and_then(|stats| stats.min_value.clone())),
            SupportStatAggr::MaxValue { column_name } => Ok(self
                .column_stats
                .get(column_name)
                .and_then(|stats| stats.max_value.clone())),
        }
    }
}

fn count_value(value: u64) -> Result<Value> {
    let value = i64::try_from(value).map_err(|_| {
        DataFusionError::Internal(format!(
            "StatsScanExec count state exceeds Int64 range: {}",
            value
        ))
    })?;
    Ok(Value::Int64(value))
}

fn matches_partition_expr(
    file_partition_expr: Option<&str>,
    region_partition_expr: Option<&str>,
) -> bool {
    matches!(
        (file_partition_expr, region_partition_expr),
        (Some(file_expr), Some(region_expr)) if file_expr == region_expr
    )
}

fn required_columns(requirements: &[SupportStatAggr]) -> HashSet<String> {
    requirements
        .iter()
        .filter_map(|requirement| match requirement {
            SupportStatAggr::CountRows => None,
            SupportStatAggr::CountNonNull { column_name }
            | SupportStatAggr::MinValue { column_name }
            | SupportStatAggr::MaxValue { column_name } => Some(column_name.clone()),
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

    Ok(FileColumnStats {
        null_count: sum_null_counts(file_stats, column_index)?,
        min_value: best_row_group_value(file_stats, column_index, Ordering::Less)?,
        max_value: best_row_group_value(file_stats, column_index, Ordering::Greater)?,
    })
}

fn sum_null_counts(file_stats: &FileStatsItem, column_index: usize) -> Result<Option<u64>> {
    if file_stats.row_groups.is_empty() {
        return Ok(None);
    }

    let mut total = 0_u64;
    for row_group in &file_stats.row_groups {
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
) -> Result<Option<Value>> {
    let mut best = None;

    for row_group in &file_stats.row_groups {
        let Some(stats) = row_group.metadata.column(column_index).statistics() else {
            return Ok(None);
        };
        let Some(scalar) = parquet_bound_scalar(stats, target) else {
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

fn parquet_bound_scalar(stats: &ParquetStats, target: Ordering) -> Option<ScalarValue> {
    let use_min = target == Ordering::Less;

    match stats {
        ParquetStats::Boolean(stats) => Some(ScalarValue::Boolean(Some(if use_min {
            *stats.min_opt()?
        } else {
            *stats.max_opt()?
        }))),
        ParquetStats::Int32(stats) => Some(ScalarValue::Int32(Some(if use_min {
            *stats.min_opt()?
        } else {
            *stats.max_opt()?
        }))),
        ParquetStats::Int64(stats) => Some(ScalarValue::Int64(Some(if use_min {
            *stats.min_opt()?
        } else {
            *stats.max_opt()?
        }))),
        ParquetStats::Int96(_) => None,
        ParquetStats::Float(stats) => Some(ScalarValue::Float32(Some(if use_min {
            *stats.min_opt()?
        } else {
            *stats.max_opt()?
        }))),
        ParquetStats::Double(stats) => Some(ScalarValue::Float64(Some(if use_min {
            *stats.min_opt()?
        } else {
            *stats.max_opt()?
        }))),
        ParquetStats::ByteArray(stats) => {
            let bytes = if use_min {
                stats.min_bytes_opt()?
            } else {
                stats.max_bytes_opt()?
            };
            Some(ScalarValue::Utf8(String::from_utf8(bytes.to_owned()).ok()))
        }
        ParquetStats::FixedLenByteArray(_) => None,
    }
}
