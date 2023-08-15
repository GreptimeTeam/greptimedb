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

use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use config::ConfigError;
use rustyline::error::ReadlineError;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to iter stream, source: {}", source))]
    IterStream {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to start datanode, source: {}", source))]
    StartDatanode {
        location: Location,
        source: datanode::error::Error,
    },

    #[snafu(display("Failed to shutdown datanode, source: {}", source))]
    ShutdownDatanode {
        location: Location,
        source: datanode::error::Error,
    },

    #[snafu(display("Failed to start frontend, source: {}", source))]
    StartFrontend {
        location: Location,
        source: frontend::error::Error,
    },

    #[snafu(display("Failed to shutdown frontend, source: {}", source))]
    ShutdownFrontend {
        location: Location,
        source: frontend::error::Error,
    },

    #[snafu(display("Failed to build meta server, source: {}", source))]
    BuildMetaServer {
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Failed to start meta server, source: {}", source))]
    StartMetaServer {
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Failed to shutdown meta server, source: {}", source))]
    ShutdownMetaServer {
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Missing config, msg: {}", msg))]
    MissingConfig { msg: String, location: Location },

    #[snafu(display("Illegal config: {}", msg))]
    IllegalConfig { msg: String, location: Location },

    #[snafu(display("Illegal auth config: {}", source))]
    IllegalAuthConfig {
        location: Location,
        source: auth::Error,
    },

    #[snafu(display("Unsupported selector type, {} source: {}", selector_type, source))]
    UnsupportedSelectorType {
        selector_type: String,
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Invalid REPL command: {reason}"))]
    InvalidReplCommand { reason: String },

    #[snafu(display("Cannot create REPL: {}", source))]
    ReplCreation {
        source: ReadlineError,
        location: Location,
    },

    #[snafu(display("Error reading command: {}", source))]
    Readline {
        source: ReadlineError,
        location: Location,
    },

    #[snafu(display("Failed to request database, sql: {sql}, source: {source}"))]
    RequestDatabase {
        sql: String,
        location: Location,
        source: client::Error,
    },

    #[snafu(display("Failed to collect RecordBatches, source: {source}"))]
    CollectRecordBatches {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to pretty print Recordbatches, source: {source}"))]
    PrettyPrintRecordBatches {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to start Meta client, source: {}", source))]
    StartMetaClient {
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to parse SQL: {}, source: {}", sql, source))]
    ParseSql {
        sql: String,
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement, source: {}", source))]
    PlanStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to encode logical plan in substrait, source: {}", source))]
    SubstraitEncodeLogicalPlan {
        location: Location,
        source: substrait::error::Error,
    },

    #[snafu(display("Failed to load layered config, source: {}", source))]
    LoadLayeredConfig {
        source: ConfigError,
        location: Location,
    },

    #[snafu(display("Failed to start catalog manager, source: {}", source))]
    StartCatalogManager {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to connect to Etcd at {etcd_addr}, source: {}", source))]
    ConnectEtcd {
        etcd_addr: String,
        source: etcd_client::Error,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::StartDatanode { source, .. } => source.status_code(),
            Error::StartFrontend { source, .. } => source.status_code(),
            Error::ShutdownDatanode { source, .. } => source.status_code(),
            Error::ShutdownFrontend { source, .. } => source.status_code(),
            Error::StartMetaServer { source, .. } => source.status_code(),
            Error::ShutdownMetaServer { source, .. } => source.status_code(),
            Error::BuildMetaServer { source, .. } => source.status_code(),
            Error::UnsupportedSelectorType { source, .. } => source.status_code(),
            Error::IterStream { source, .. } => source.status_code(),
            Error::MissingConfig { .. }
            | Error::LoadLayeredConfig { .. }
            | Error::IllegalConfig { .. }
            | Error::InvalidReplCommand { .. }
            | Error::IllegalAuthConfig { .. }
            | Error::ConnectEtcd { .. } => StatusCode::InvalidArguments,

            Error::ReplCreation { .. } | Error::Readline { .. } => StatusCode::Internal,
            Error::RequestDatabase { source, .. } => source.status_code(),
            Error::CollectRecordBatches { source, .. }
            | Error::PrettyPrintRecordBatches { source, .. } => source.status_code(),
            Error::StartMetaClient { source, .. } => source.status_code(),
            Error::ParseSql { source, .. } | Error::PlanStatement { source, .. } => {
                source.status_code()
            }
            Error::SubstraitEncodeLogicalPlan { source, .. } => source.status_code(),
            Error::StartCatalogManager { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
