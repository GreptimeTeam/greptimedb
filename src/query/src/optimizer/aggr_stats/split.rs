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

use common_function::aggrs::aggr_wrapper::StateWrapper;
use common_time::Timestamp;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_common::{Result as DfResult, ScalarValue};
use datatypes::arrow::datatypes::DataType;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use store_api::scan_stats::RegionScanStats;

use super::StatsAgg;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FileStatsRequirement {
    FileExactRowCount,
    FileTimeRange,
    RowGroupMinMax,
    RowGroupNullCount,
}

/// Splits scan input files into two buckets for one aggregate rewrite path.
///
/// `stats_file_ordinals` and `scan_file_ordinals` store
/// `RegionScanFileStats::file_ordinal`, not indexes into
/// `RegionScanStats::files`. The optimizer later uses these ordinals to decide
/// which physical files can be answered from metadata and which still need a
/// real scan.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileSplit<T> {
    /// File ordinals whose stats is sufficient to contribute to the rewrite.
    pub stats_file_ordinals: Vec<usize>,
    /// File ordinals that still need to be scanned because required stats are missing.
    pub scan_file_ordinals: Vec<usize>,
    /// Aggregate contribution computed from `stats_file_ordinals` only.
    pub stats: T,
}

impl<T> FileSplit<T> {
    fn has_stats_files(&self) -> bool {
        !self.stats_file_ordinals.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// File-level time bounds collected from metadata-only files.
pub(super) struct TimeBounds {
    pub min: Option<Timestamp>,
    pub max: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Field value bounds collected from metadata-only files.
pub(super) struct ValueBounds {
    pub min: Option<Value>,
    pub max: Option<Value>,
}

pub(super) type CountStarFileSplit = FileSplit<usize>;
pub(super) type FieldCountFileSplit = FileSplit<usize>;
pub(super) type TimeFileSplit = FileSplit<TimeBounds>;
pub(super) type FieldMinMaxFileSplit = FileSplit<ValueBounds>;

fn stats_file_ordinals(aggregate: &StatsAgg, scan_input_stats: &RegionScanStats) -> Vec<usize> {
    match aggregate {
        StatsAgg::CountStar => split_count_star_files(scan_input_stats).stats_file_ordinals,
        StatsAgg::CountField { column_name, .. } => {
            split_count_field_files(scan_input_stats, column_name).stats_file_ordinals
        }
        StatsAgg::CountTimeIndex { .. }
        | StatsAgg::MinTimeIndex { .. }
        | StatsAgg::MaxTimeIndex { .. } => split_time_files(scan_input_stats).stats_file_ordinals,
        StatsAgg::MinField { column_name, .. } | StatsAgg::MaxField { column_name, .. } => {
            split_min_max_field_files(scan_input_stats, column_name).stats_file_ordinals
        }
    }
}

/// Returns file ordinals that every aggregate in the list can answer from stats.
#[allow(dead_code)]
pub(super) fn common_stats_file_ordinals(
    aggregates: &[StatsAgg],
    scan_input_stats: &RegionScanStats,
) -> Vec<usize> {
    let Some(first) = aggregates.first() else {
        return Vec::new();
    };

    let mut common = stats_file_ordinals(first, scan_input_stats)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();

    for aggregate in &aggregates[1..] {
        let ordinals = stats_file_ordinals(aggregate, scan_input_stats)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        common.retain(|ordinal| ordinals.contains(ordinal));
    }

    scan_input_stats
        .files
        .iter()
        .filter_map(|file| {
            common
                .contains(&file.file_ordinal)
                .then_some(file.file_ordinal)
        })
        .collect()
}

pub(super) fn filter_stats_by_file_ordinals(
    scan_input_stats: &RegionScanStats,
    file_ordinals: &[usize],
) -> RegionScanStats {
    let selected = file_ordinals
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();

    RegionScanStats {
        files: scan_input_stats
            .files
            .iter()
            .filter(|file| selected.contains(&file.file_ordinal))
            .cloned()
            .collect(),
    }
}

pub(super) trait StatsAggExt {
    fn has_stats_files(&self, scan_input_stats: &RegionScanStats) -> bool;
}

impl StatsAggExt for StatsAgg {
    fn has_stats_files(&self, scan_input_stats: &RegionScanStats) -> bool {
        match self {
            StatsAgg::CountStar => split_count_star_files(scan_input_stats).has_stats_files(),
            StatsAgg::CountField { column_name, .. } => {
                split_count_field_files(scan_input_stats, column_name).has_stats_files()
            }
            StatsAgg::CountTimeIndex { .. }
            | StatsAgg::MinTimeIndex { .. }
            | StatsAgg::MaxTimeIndex { .. } => split_time_files(scan_input_stats).has_stats_files(),
            StatsAgg::MinField { column_name, .. } | StatsAgg::MaxField { column_name, .. } => {
                split_min_max_field_files(scan_input_stats, column_name).has_stats_files()
            }
        }
    }
}

#[cfg(test)]
impl StatsAgg {
    pub(super) fn file_stats_requirement(&self) -> FileStatsRequirement {
        match self {
            StatsAgg::CountStar => FileStatsRequirement::FileExactRowCount,
            StatsAgg::CountField { .. } => FileStatsRequirement::RowGroupNullCount,
            StatsAgg::CountTimeIndex { .. }
            | StatsAgg::MinTimeIndex { .. }
            | StatsAgg::MaxTimeIndex { .. } => FileStatsRequirement::FileTimeRange,
            StatsAgg::MinField { .. } | StatsAgg::MaxField { .. } => {
                FileStatsRequirement::RowGroupMinMax
            }
        }
    }
}

pub(super) fn has_partition_expr_mismatch(scan_input_stats: Option<&RegionScanStats>) -> bool {
    scan_input_stats
        .map(|stats| {
            stats
                .files
                .iter()
                .any(|file| !file.partition_expr_matches_region)
        })
        .unwrap_or(false)
}

pub(super) fn split_count_star_files(scan_input_stats: &RegionScanStats) -> CountStarFileSplit {
    scan_input_stats.files.iter().fold(
        FileSplit {
            stats_file_ordinals: Vec::new(),
            scan_file_ordinals: Vec::new(),
            stats: 0,
        },
        |mut split, file| {
            if file.partition_expr_matches_region
                && let Some(num_rows) = file.exact_num_rows
            {
                split.stats_file_ordinals.push(file.file_ordinal);
                split.stats += num_rows;
                return split;
            }

            split.scan_file_ordinals.push(file.file_ordinal);
            split
        },
    )
}

pub(super) fn split_time_files(scan_input_stats: &RegionScanStats) -> TimeFileSplit {
    scan_input_stats.files.iter().fold(
        FileSplit {
            stats_file_ordinals: Vec::new(),
            scan_file_ordinals: Vec::new(),
            stats: TimeBounds::default(),
        },
        |mut split, file| {
            if file.partition_expr_matches_region
                && let Some((min_ts, max_ts)) = file.time_range
            {
                split.stats_file_ordinals.push(file.file_ordinal);
                split.stats.min = Some(match split.stats.min {
                    Some(current) => Timestamp::min(current, min_ts),
                    None => min_ts,
                });
                split.stats.max = Some(match split.stats.max {
                    Some(current) => Timestamp::max(current, max_ts),
                    None => max_ts,
                });
                return split;
            }

            split.scan_file_ordinals.push(file.file_ordinal);
            split
        },
    )
}

pub(super) fn split_count_field_files(
    scan_input_stats: &RegionScanStats,
    column_name: &str,
) -> FieldCountFileSplit {
    scan_input_stats.files.iter().fold(
        FileSplit {
            stats_file_ordinals: Vec::new(),
            scan_file_ordinals: Vec::new(),
            stats: 0,
        },
        |mut split, file| {
            if file.partition_expr_matches_region
                && let Some(non_null_rows) = file
                    .field_stats
                    .get(column_name)
                    .and_then(|stats| stats.exact_non_null_rows)
            {
                split.stats_file_ordinals.push(file.file_ordinal);
                split.stats += non_null_rows;
                return split;
            }

            split.scan_file_ordinals.push(file.file_ordinal);
            split
        },
    )
}

pub(super) fn split_min_max_field_files(
    scan_input_stats: &RegionScanStats,
    column_name: &str,
) -> FieldMinMaxFileSplit {
    scan_input_stats.files.iter().fold(
        FileSplit {
            stats_file_ordinals: Vec::new(),
            scan_file_ordinals: Vec::new(),
            stats: ValueBounds::default(),
        },
        |mut split, file| {
            if file.partition_expr_matches_region
                && let Some(stats) = file.field_stats.get(column_name)
                && let (Some(min_value), Some(max_value)) =
                    (stats.min_value.clone(), stats.max_value.clone())
            {
                split.stats_file_ordinals.push(file.file_ordinal);
                split.stats.min = Some(match split.stats.min {
                    Some(current) => Value::min(current, min_value),
                    None => min_value,
                });
                split.stats.max = Some(match split.stats.max {
                    Some(current) => Value::max(current, max_value),
                    None => max_value,
                });
                return split;
            }

            split.scan_file_ordinals.push(file.file_ordinal);
            split
        },
    )
}

// These helpers are implemented for the upcoming mixed stats-plus-scan rewrite in task 3.
#[allow(dead_code)]
pub(super) fn partial_state_from_stats(
    aggregate: &StatsAgg,
    scan_input_stats: &RegionScanStats,
) -> DfResult<Option<ScalarValue>> {
    match aggregate {
        StatsAgg::CountStar => {
            let split = split_count_star_files(scan_input_stats);
            if !split.has_stats_files() {
                return Ok(None);
            }

            let wrapper = StateWrapper::new((*count_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    &[],
                    vec![ScalarValue::Int64(Some(split.stats as i64))],
                )
                .map(Some)
        }
        StatsAgg::CountField {
            arg_type,
            column_name,
        } => {
            let split = split_count_field_files(scan_input_stats, column_name);
            if !split.has_stats_files() {
                return Ok(None);
            }

            let wrapper = StateWrapper::new((*count_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    std::slice::from_ref(arg_type),
                    vec![ScalarValue::Int64(Some(split.stats as i64))],
                )
                .map(Some)
        }
        StatsAgg::CountTimeIndex { arg_type } => {
            let split = split_count_star_files(scan_input_stats);
            if !split.has_stats_files() {
                return Ok(None);
            }

            let wrapper = StateWrapper::new((*count_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    std::slice::from_ref(arg_type),
                    vec![ScalarValue::Int64(Some(split.stats as i64))],
                )
                .map(Some)
        }
        StatsAgg::MinField {
            arg_type,
            column_name,
        } => {
            let split = split_min_max_field_files(scan_input_stats, column_name);
            let Some(value) = split.stats.min.as_ref() else {
                return Ok(None);
            };

            let wrapper = StateWrapper::new((*min_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    std::slice::from_ref(arg_type),
                    vec![stats_value_scalar(value, arg_type)?],
                )
                .map(Some)
        }
        StatsAgg::MinTimeIndex { arg_type } => {
            let split = split_time_files(scan_input_stats);
            let Some(timestamp) = split.stats.min else {
                return Ok(None);
            };

            let wrapper = StateWrapper::new((*min_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    std::slice::from_ref(arg_type),
                    vec![timestamp_scalar_value(&timestamp, arg_type)?],
                )
                .map(Some)
        }
        StatsAgg::MaxField {
            arg_type,
            column_name,
        } => {
            let split = split_min_max_field_files(scan_input_stats, column_name);
            let Some(value) = split.stats.max.as_ref() else {
                return Ok(None);
            };

            let wrapper = StateWrapper::new((*max_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    std::slice::from_ref(arg_type),
                    vec![stats_value_scalar(value, arg_type)?],
                )
                .map(Some)
        }
        StatsAgg::MaxTimeIndex { arg_type } => {
            let split = split_time_files(scan_input_stats);
            let Some(timestamp) = split.stats.max else {
                return Ok(None);
            };

            let wrapper = StateWrapper::new((*max_udaf()).clone())?;
            wrapper
                .value_from_custom_state_fields(
                    std::slice::from_ref(arg_type),
                    vec![timestamp_scalar_value(&timestamp, arg_type)?],
                )
                .map(Some)
        }
    }
}

#[allow(dead_code)]
fn timestamp_scalar_value(timestamp: &Timestamp, arg_type: &DataType) -> DfResult<ScalarValue> {
    match arg_type {
        DataType::Timestamp(unit, tz) => {
            let converted = timestamp.convert_to((*unit).into()).ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "failed to convert timestamp {timestamp:?} to {unit:?}"
                ))
            })?;
            Ok(match unit {
                datatypes::arrow::datatypes::TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(converted.value()), tz.clone())
                }
                datatypes::arrow::datatypes::TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(converted.value()), tz.clone())
                }
                datatypes::arrow::datatypes::TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(converted.value()), tz.clone())
                }
                datatypes::arrow::datatypes::TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(converted.value()), tz.clone())
                }
            })
        }
        _ => Err(datafusion_common::DataFusionError::Internal(format!(
            "expected timestamp arg type, got {arg_type:?}"
        ))),
    }
}

#[allow(dead_code)]
fn stats_value_scalar(value: &Value, arg_type: &DataType) -> DfResult<ScalarValue> {
    let output_type = ConcreteDataType::try_from(arg_type).map_err(|err| {
        datafusion_common::DataFusionError::Internal(format!(
            "failed to convert arrow type {arg_type:?} to concrete type: {err}"
        ))
    })?;
    value.try_to_scalar_value(&output_type).map_err(|err| {
        datafusion_common::DataFusionError::Internal(format!(
            "failed to convert stats value {value:?} to scalar for {arg_type:?}: {err}"
        ))
    })
}
