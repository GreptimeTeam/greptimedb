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

pub const SYSTEM_CATALOG_NAME: &str = "system";
pub const INFORMATION_SCHEMA_NAME: &str = "information_schema";
pub const SYSTEM_CATALOG_TABLE_NAME: &str = "system_catalog";
pub const DEFAULT_CATALOG_NAME: &str = "greptime";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
pub const DEFAULT_PRIVATE_SCHEMA_NAME: &str = "greptime_private";

/// Reserves [0,MIN_USER_TABLE_ID) for internal usage.
/// User defined table id starts from this value.
pub const MIN_USER_TABLE_ID: u32 = 1024;
/// the max system table id
pub const MAX_SYS_TABLE_ID: u32 = MIN_USER_TABLE_ID - 1;
/// system_catalog table id
pub const SYSTEM_CATALOG_TABLE_ID: u32 = 0;
/// scripts table id
pub const SCRIPTS_TABLE_ID: u32 = 1;
/// numbers table id
pub const NUMBERS_TABLE_ID: u32 = 2;

/// ----- Begin of information_schema tables -----
/// id for information_schema.tables
pub const INFORMATION_SCHEMA_TABLES_TABLE_ID: u32 = 3;
/// id for information_schema.columns
pub const INFORMATION_SCHEMA_COLUMNS_TABLE_ID: u32 = 4;
/// id for information_schema.engines
pub const INFORMATION_SCHEMA_ENGINES_TABLE_ID: u32 = 5;
/// id for information_schema.column_privileges
pub const INFORMATION_SCHEMA_COLUMN_PRIVILEGES_TABLE_ID: u32 = 6;
/// id for information_schema.column_statistics
pub const INFORMATION_SCHEMA_COLUMN_STATISTICS_TABLE_ID: u32 = 7;
/// id for information_schema.build_info
pub const INFORMATION_SCHEMA_BUILD_INFO_TABLE_ID: u32 = 8;
/// ----- End of information_schema tables -----

pub const MITO_ENGINE: &str = "mito";
pub const MITO2_ENGINE: &str = "mito2";
pub const METRIC_ENGINE: &str = "metric";

pub fn default_engine() -> &'static str {
    MITO_ENGINE
}

pub const FILE_ENGINE: &str = "file";

pub const SEMANTIC_TYPE_PRIMARY_KEY: &str = "TAG";
pub const SEMANTIC_TYPE_FIELD: &str = "FIELD";
pub const SEMANTIC_TYPE_TIME_INDEX: &str = "TIMESTAMP";
