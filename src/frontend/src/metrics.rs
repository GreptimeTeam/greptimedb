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

pub(crate) const METRIC_HANDLE_SQL_ELAPSED: &str = "frontend.handle_sql_elapsed";
pub(crate) const METRIC_HANDLE_SCRIPTS_ELAPSED: &str = "frontend.handle_scripts_elapsed";
pub(crate) const METRIC_RUN_SCRIPT_ELAPSED: &str = "frontend.run_script_elapsed";

/// frontend metrics
/// Metrics for creating table in dist mode.
pub const DIST_CREATE_TABLE: &str = "frontend.dist.create_table";
pub const DIST_CREATE_TABLE_IN_META: &str = "frontend.dist.create_table.update_meta";
pub const DIST_CREATE_TABLE_IN_DATANODE: &str = "frontend.dist.create_table.invoke_datanode";
