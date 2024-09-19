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
#![feature(let_chains)]
#![feature(if_let_guard)]

use datafusion_expr::LogicalPlan;
use datatypes::schema::Schema;

pub mod addrs;
pub mod configurator;
pub mod error;
pub mod export_metrics;
pub mod grpc;
pub mod heartbeat_options;
pub mod http;
pub mod influxdb;
pub mod interceptor;
mod metrics;
pub mod metrics_handler;
pub mod mysql;
pub mod opentsdb;
pub mod otlp;
pub mod postgres;
mod prom_row_builder;
pub mod prom_store;
pub mod prometheus_handler;
pub mod proto;
pub mod query_handler;
pub mod repeated_field;
mod row_writer;
pub mod server;
pub mod tls;

pub use common_config::Mode;

/// Cached SQL and logical plan for database interfaces
#[derive(Clone)]
pub struct SqlPlan {
    query: String,
    plan: Option<LogicalPlan>,
    schema: Option<Schema>,
}

/// Install the ring crypto provider for rustls process-wide. see:
///
///  https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html#using-the-per-process-default-cryptoprovider
///
/// for more information.
pub fn install_ring_crypto_provider() -> Result<(), String> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .map_err(|ret| {
            format!(
                "CryptoProvider already installed as: {:?}, but providing {:?}",
                rustls::crypto::CryptoProvider::get_default(),
                ret
            )
        })
}
