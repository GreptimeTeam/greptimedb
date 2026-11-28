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

//! Series index construction, search, and maintenance.
//!
//! Narthex index artifacts are owned and published by the region manifest.

pub(crate) mod artifact;
mod artifact_writer;
pub(crate) mod columnar;
pub(crate) mod format;
pub(crate) mod identity;
pub(crate) mod samples;
mod task;
#[cfg(test)]
mod tests;
mod writer;

#[cfg(test)]
pub(crate) use tests::prepare_region_with_metadata;

use datatypes::prelude::ConcreteDataType;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME as TABLE_ID_COLUMN,
    DATA_SCHEMA_TSID_COLUMN_NAME as TSID_COLUMN,
};
use store_api::storage::consts::ReservedColumnId;

pub(crate) use crate::series_index::task::{SeriesIndexTaskState, spawn_series_index_tasks};

pub(crate) const MIN_TS_COLUMN: &str = "__series_min_ts";
pub(crate) const MAX_TS_COLUMN: &str = "__series_max_ts";
pub(crate) const ROW_COUNT_COLUMN: &str = "__series_row_count";

/// Identifies one series in a physical metric region.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct MetricSeriesId {
    /// Logical table ID inside the physical metric region.
    pub table_id: u32,
    /// Time-series ID inside the logical table.
    pub tsid: u64,
}

pub(crate) fn is_sparse_metric_metadata(metadata: &store_api::metadata::RegionMetadataRef) -> bool {
    let valid_prefix = metadata
        .primary_key
        .starts_with(&[ReservedColumnId::table_id(), ReservedColumnId::tsid()]);
    let valid_types = metadata
        .column_by_id(ReservedColumnId::table_id())
        .zip(metadata.column_by_id(ReservedColumnId::tsid()))
        .is_some_and(|(table_id, tsid)| {
            table_id.column_schema.data_type == ConcreteDataType::uint32_datatype()
                && tsid.column_schema.data_type == ConcreteDataType::uint64_datatype()
        });

    metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse && valid_prefix && valid_types
}
