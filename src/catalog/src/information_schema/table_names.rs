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

/// All table names in `information_schema`.

pub const TABLES: &str = "tables";
pub const COLUMNS: &str = "columns";
pub const ENGINES: &str = "engines";
pub const COLUMN_PRIVILEGES: &str = "column_privileges";
pub const COLUMN_STATISTICS: &str = "column_statistics";
pub const BUILD_INFO: &str = "build_info";
pub const CHARACTER_SETS: &str = "character_sets";
pub const COLLATIONS: &str = "collations";
pub const COLLATION_CHARACTER_SET_APPLICABILITY: &str = "collation_character_set_applicability";
pub const CHECK_CONSTRAINTS: &str = "check_constraints";
pub const EVENTS: &str = "events";
pub const FILES: &str = "files";
pub const SCHEMATA: &str = "schemata";
pub const KEY_COLUMN_USAGE: &str = "key_column_usage";
pub const OPTIMIZER_TRACE: &str = "optimizer_trace";
pub const PARAMETERS: &str = "parameters";
pub const PROFILING: &str = "profiling";
pub const REFERENTIAL_CONSTRAINTS: &str = "referential_constraints";
pub const ROUTINES: &str = "routines";
pub const SCHEMA_PRIVILEGES: &str = "schema_privileges";
pub const TABLE_PRIVILEGES: &str = "table_privileges";
pub const TRIGGERS: &str = "triggers";
pub const GLOBAL_STATUS: &str = "global_status";
pub const SESSION_STATUS: &str = "session_status";
