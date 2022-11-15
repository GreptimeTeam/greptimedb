// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! query engine metrics

pub static METRIC_PARSE_SQL_ELAPSED: &str = "query.parse_sql_elapsed";
pub static METRIC_OPTIMIZE_LOGICAL_ELAPSED: &str = "query.optimize_logicalplan_elapsed";
pub static METRIC_OPTIMIZE_PHYSICAL_ELAPSED: &str = "query.optimize_physicalplan_elapsed";
pub static METRIC_CREATE_PHYSICAL_ELAPSED: &str = "query.create_physicalplan_elapsed";
pub static METRIC_EXEC_PLAN_ELAPSED: &str = "query.execute_plan_elapsed";
