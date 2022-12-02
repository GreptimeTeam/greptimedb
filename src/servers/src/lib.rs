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

#![feature(assert_matches)]

use serde::{Deserialize, Serialize};

pub mod context;
pub mod error;
pub mod grpc;
pub mod http;
pub mod influxdb;
pub mod line_writer;
pub mod mysql;
pub mod opentsdb;
pub mod postgres;
pub mod prometheus;
pub mod query_handler;
pub mod server;
pub mod tls;

mod shutdown;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    Distributed,
}
