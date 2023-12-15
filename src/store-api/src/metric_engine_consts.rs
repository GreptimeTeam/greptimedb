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
