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

//! Constants used in metric engine.

use crate::storage::RegionGroup;

/// region group value for data region inside a metric region
pub const METRIC_DATA_REGION_GROUP: RegionGroup = 0;

/// region group value for metadata region inside a metric region
pub const METRIC_METADATA_REGION_GROUP: RegionGroup = 1;

pub const METADATA_SCHEMA_TIMESTAMP_COLUMN_NAME: &str = "ts";
pub const METADATA_SCHEMA_KEY_COLUMN_NAME: &str = "k";
pub const METADATA_SCHEMA_VALUE_COLUMN_NAME: &str = "v";

pub const METADATA_SCHEMA_TIMESTAMP_COLUMN_INDEX: usize = 0;
pub const METADATA_SCHEMA_KEY_COLUMN_INDEX: usize = 1;
pub const METADATA_SCHEMA_VALUE_COLUMN_INDEX: usize = 2;

/// Column name of internal column `__metric` that stores the original metric name
pub const DATA_SCHEMA_TABLE_ID_COLUMN_NAME: &str = "__table_id";
pub const DATA_SCHEMA_TSID_COLUMN_NAME: &str = "__tsid";

pub const METADATA_REGION_SUBDIR: &str = "metadata";
pub const DATA_REGION_SUBDIR: &str = "data";

pub const METRIC_ENGINE_NAME: &str = "metric";

pub const FILE_ENGINE_NAME: &str = "file";

/// Metadata key present in the `CREATE TABLE ... WITH ()` clause. This key is
/// used to identify the table is a physical metric table. E.g.:
/// ```sql
/// CREATE TABLE physical_table (
///     ...
/// )
/// ENGINE = metric
/// WITH (
///     physical_metric_table,
/// );
/// ```
pub const PHYSICAL_TABLE_METADATA_KEY: &str = "physical_metric_table";

/// Metadata key present in the `CREATE TABLE ... WITH ()` clause. This key is
/// used to identify a logical table and associate it with a corresponding physical
/// table . E.g.:
/// ```sql
/// CREATE TABLE logical_table (
///     ...
/// )
/// ENGINE = metric
/// WITH (
///     on_physical_table = "physical_table",
/// );
/// ```
/// And this key will be translated to corresponding physical **REGION** id in metasrv.
pub const LOGICAL_TABLE_METADATA_KEY: &str = "on_physical_table";

/// HashMap key to be used in the region server's extension response.
/// Represent a list of column metadata that are added to physical table.
pub const ALTER_PHYSICAL_EXTENSION_KEY: &str = "ALTER_PHYSICAL";

/// HashMap key to be used in the region server's extension response.
/// Represent the manifest info of a region.
pub const MANIFEST_INFO_EXTENSION_KEY: &str = "MANIFEST_INFO";

/// Returns true if it's a internal column of the metric engine.
pub fn is_metric_engine_internal_column(name: &str) -> bool {
    name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME || name == DATA_SCHEMA_TSID_COLUMN_NAME
}

/// Returns true if it's metric engine
pub fn is_metric_engine(name: &str) -> bool {
    name == METRIC_ENGINE_NAME
}

/// Option key for metric engine index type.
/// Used to identify the primary key index type of the metric engine.
/// ```sql
/// CREATE TABLE table_name (
///     ...
/// )
/// ENGINE = metric
/// WITH (
///     physical_metric_table = "",
///     index.type = "inverted",
///     index.inverted_index.segment_row_count = "256",
/// );
/// ```
pub const METRIC_ENGINE_INDEX_TYPE_OPTION: &str = "index.type";

/// Option key for the granularity of the skipping index in the metric engine.
/// This key is used to specify the granularity of the primary key index (skipping index) in the metric engine.
/// ```sql
/// CREATE TABLE table_name (
///     ...
/// )
/// ENGINE = metric
/// WITH (
///     physical_metric_table = "",
///     index.type = "skipping",
///     index.granularity = "102400",
/// );
/// ```
pub const METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION: &str = "index.granularity";

/// Default granularity for the skipping index in the metric engine.
pub const METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION_DEFAULT: u32 = 102400;

/// Returns true if the `key` is a valid option key for the metric engine.
pub fn is_metric_engine_option_key(key: &str) -> bool {
    [
        PHYSICAL_TABLE_METADATA_KEY,
        LOGICAL_TABLE_METADATA_KEY,
        METRIC_ENGINE_INDEX_TYPE_OPTION,
        METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION,
    ]
    .contains(&key)
}
