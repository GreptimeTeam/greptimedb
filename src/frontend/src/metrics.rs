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
pub(crate) const METRIC_HANDLE_PROMQL_ELAPSED: &str = "frontend.handle_promql_elapsed";
pub(crate) const METRIC_EXEC_PLAN_ELAPSED: &str = "frontend.exec_plan_elapsed";
pub(crate) const METRIC_HANDLE_SCRIPTS_ELAPSED: &str = "frontend.handle_scripts_elapsed";
pub(crate) const METRIC_RUN_SCRIPT_ELAPSED: &str = "frontend.run_script_elapsed";

/// The samples count of Prometheus remote write.
pub const PROM_STORE_REMOTE_WRITE_SAMPLES: &str = "frontend.prometheus.remote_write.samples";

pub const OTLP_METRICS_ROWS: &str = "frontend.otlp.metrics.rows";
pub const OTLP_TRACES_ROWS: &str = "frontend.otlp.traces.rows";
