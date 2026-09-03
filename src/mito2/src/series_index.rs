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

//! Series index writer and searcher.

mod searcher;
mod writer;

use std::collections::HashMap;

use common_time::timestamp::TimeUnit;
use futures::stream::BoxStream;
pub use searcher::SeriesIndexSearcher;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME as TABLE_ID_COLUMN,
    DATA_SCHEMA_TSID_COLUMN_NAME as TSID_COLUMN,
};
pub use writer::{
    SeriesIndexWriter, SeriesIndexWriterMetrics, SeriesIndexWriterOptions, series_index_schema,
};

use crate::error::Result;

pub(crate) const MIN_TS_COLUMN: &str = "__series_min_ts";
pub(crate) const MAX_TS_COLUMN: &str = "__series_max_ts";
pub(crate) const ROW_COUNT_COLUMN: &str = "__series_row_count";
pub(crate) const METRIC_SERIES_ID_BATCH_SIZE: usize = 500;

/// Arrow field metadata key on `__series_min_ts`/`__series_max_ts` recording
/// the time index unit their raw i64 values are stored in, so a searcher can
/// interpret each file with the unit it was written with (the region's time
/// index unit may have been widened after the file was written).
pub(crate) const TIME_UNIT_META_KEY: &str = "time_unit";

/// Builds the field metadata recording that ts values are stored in `unit`.
/// The recorded string is `TimeUnit`'s [`Display`](std::fmt::Display) form
/// ("Millisecond", ...), parsed back with [`FromStr`]
/// (`common_time::timestamp::TimeUnit`).
pub(crate) fn time_unit_metadata(unit: TimeUnit) -> HashMap<String, String> {
    HashMap::from([(TIME_UNIT_META_KEY.to_string(), unit.to_string())])
}

/// Identifies one series in a physical metric region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricSeriesId {
    /// Logical table ID inside the physical metric region.
    pub table_id: u32,
    /// Time-series ID inside the logical table.
    pub tsid: u64,
}

/// Stream of bounded batches of matching metric-series IDs.
pub type MetricSeriesIdStream = BoxStream<'static, Result<Vec<MetricSeriesId>>>;
