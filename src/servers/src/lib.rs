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

#![feature(assert_matches)]
#![feature(try_blocks)]
#![feature(exclusive_wrapper)]

use datatypes::schema::Schema;
use query::plan::LogicalPlan;
use serde::{Deserialize, Serialize};

pub mod configurator;
pub mod error;
pub mod export_metrics;
pub mod grpc;
pub mod heartbeat_options;
pub mod http;
pub mod influxdb;
pub mod interceptor;
pub mod line_writer;
mod metrics;
pub mod metrics_handler;
pub mod mysql;
pub mod opentsdb;
pub mod otlp;
pub mod postgres;
pub mod prom_store;
pub mod prometheus_handler;
pub mod query_handler;
mod row_writer;
pub mod server;
mod shutdown;
pub mod tls;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    Distributed,
}

/// Cached SQL and logical plan for database interfaces
#[derive(Clone)]
pub struct SqlPlan {
    query: String,
    plan: Option<LogicalPlan>,
    schema: Option<Schema>,
}
