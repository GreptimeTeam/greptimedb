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
/// id for information_schema.CHARACTER_SETS
pub const INFORMATION_SCHEMA_CHARACTER_SETS_TABLE_ID: u32 = 9;
/// id for information_schema.COLLATIONS
pub const INFORMATION_SCHEMA_COLLATIONS_TABLE_ID: u32 = 10;
/// id for information_schema.COLLATIONS
pub const INFORMATION_SCHEMA_COLLATION_CHARACTER_SET_APPLICABILITY_TABLE_ID: u32 = 11;
/// id for information_schema.CHECK_CONSTRAINTS
pub const INFORMATION_SCHEMA_CHECK_CONSTRAINTS_TABLE_ID: u32 = 12;
/// id for information_schema.EVENTS
pub const INFORMATION_SCHEMA_EVENTS_TABLE_ID: u32 = 13;
/// id for information_schema.FILES
pub const INFORMATION_SCHEMA_FILES_TABLE_ID: u32 = 14;
/// id for information_schema.SCHEMATA
pub const INFORMATION_SCHEMA_SCHEMATA_TABLE_ID: u32 = 15;
/// id for information_schema.KEY_COLUMN_USAGE
pub const INFORMATION_SCHEMA_KEY_COLUMN_USAGE_TABLE_ID: u32 = 16;
/// id for information_schema.OPTIMIZER_TRACE
pub const INFORMATION_SCHEMA_OPTIMIZER_TRACE_TABLE_ID: u32 = 17;
/// id for information_schema.PARAMETERS
pub const INFORMATION_SCHEMA_PARAMETERS_TABLE_ID: u32 = 18;
/// id for information_schema.PROFILING
pub const INFORMATION_SCHEMA_PROFILING_TABLE_ID: u32 = 19;
/// id for information_schema.REFERENTIAL_CONSTRAINTS
pub const INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS_TABLE_ID: u32 = 20;
/// id for information_schema.ROUTINES
pub const INFORMATION_SCHEMA_ROUTINES_TABLE_ID: u32 = 21;
/// id for information_schema.SCHEMA_PRIVILEGES
pub const INFORMATION_SCHEMA_SCHEMA_PRIVILEGES_TABLE_ID: u32 = 22;
/// id for information_schema.TABLE_PRIVILEGES
pub const INFORMATION_SCHEMA_TABLE_PRIVILEGES_TABLE_ID: u32 = 23;
/// id for information_schema.TRIGGERS
pub const INFORMATION_SCHEMA_TRIGGERS_TABLE_ID: u32 = 24;
/// id for information_schema.GLOBAL_STATUS
pub const INFORMATION_SCHEMA_GLOBAL_STATUS_TABLE_ID: u32 = 25;
/// id for information_schema.SESSION_STATUS
pub const INFORMATION_SCHEMA_SESSION_STATUS_TABLE_ID: u32 = 26;
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
