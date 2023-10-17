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
use common_macro::stack_trace_debug;
use config::ConfigError;
use rustyline::error::ReadlineError;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to create default catalog and schema"))]
    InitMetadata {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to iter stream"))]
    IterStream {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to start procedure manager"))]
    StartProcedureManager {
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to stop procedure manager"))]
    StopProcedureManager {
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to start datanode"))]
    StartDatanode {
        location: Location,
        source: datanode::error::Error,
    },

    #[snafu(display("Failed to shutdown datanode"))]
    ShutdownDatanode {
        location: Location,
        source: datanode::error::Error,
    },

    #[snafu(display("Failed to start frontend"))]
    StartFrontend {
        location: Location,
        source: frontend::error::Error,
    },

    #[snafu(display("Failed to shutdown frontend"))]
    ShutdownFrontend {
        location: Location,
        source: frontend::error::Error,
    },

    #[snafu(display("Failed to build meta server"))]
    BuildMetaServer {
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Failed to start meta server"))]
    StartMetaServer {
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Failed to shutdown meta server"))]
    ShutdownMetaServer {
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Missing config, msg: {}", msg))]
    MissingConfig { msg: String, location: Location },

    #[snafu(display("Illegal config: {}", msg))]
    IllegalConfig { msg: String, location: Location },

    #[snafu(display("Unsupported selector type: {}", selector_type))]
    UnsupportedSelectorType {
        selector_type: String,
        location: Location,
        source: meta_srv::error::Error,
    },

    #[snafu(display("Invalid REPL command: {reason}"))]
    InvalidReplCommand { reason: String },

    #[snafu(display("Cannot create REPL"))]
    ReplCreation {
        #[snafu(source)]
        error: ReadlineError,
        location: Location,
    },

    #[snafu(display("Error reading command"))]
    Readline {
        #[snafu(source)]
        error: ReadlineError,
        location: Location,
    },

    #[snafu(display("Failed to request database, sql: {sql}"))]
    RequestDatabase {
        sql: String,
        location: Location,
        source: client::Error,
    },

    #[snafu(display("Failed to collect RecordBatches"))]
    CollectRecordBatches {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to pretty print Recordbatches"))]
    PrettyPrintRecordBatches {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to start Meta client"))]
    StartMetaClient {
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to parse SQL: {}", sql))]
    ParseSql {
        sql: String,
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement"))]
    PlanStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to encode logical plan in substrait"))]
    SubstraitEncodeLogicalPlan {
        location: Location,
        source: substrait::error::Error,
    },

    #[snafu(display("Failed to load layered config"))]
    LoadLayeredConfig {
        #[snafu(source)]
        error: ConfigError,
        location: Location,
    },

    #[snafu(display("Failed to start catalog manager"))]
    StartCatalogManager {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to connect to Etcd at {etcd_addr}"))]
    ConnectEtcd {
        etcd_addr: String,
        #[snafu(source)]
        error: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to connect server at {addr}"))]
    ConnectServer {
        addr: String,
        source: client::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to serde json"))]
    SerdeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Expect data from output, but got another thing"))]
    NotDataFromOutput { location: Location },

    #[snafu(display("Empty result from output"))]
    EmptyResult { location: Location },

    #[snafu(display("Failed to manipulate file"))]
    FileIo {
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Invalid database name: {}", database))]
    InvalidDatabaseName {
        location: Location,
        database: String,
    },

    #[snafu(display("Failed to create directory {}", dir))]
    CreateDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
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
            Error::IterStream { source, .. } | Error::InitMetadata { source, .. } => {
                source.status_code()
            }
            Error::ConnectServer { source, .. } => source.status_code(),
            Error::MissingConfig { .. }
            | Error::LoadLayeredConfig { .. }
            | Error::IllegalConfig { .. }
            | Error::InvalidReplCommand { .. }
            | Error::ConnectEtcd { .. }
            | Error::NotDataFromOutput { .. }
            | Error::CreateDir { .. }
            | Error::EmptyResult { .. }
            | Error::InvalidDatabaseName { .. } => StatusCode::InvalidArguments,
            Error::StartProcedureManager { source, .. }
            | Error::StopProcedureManager { source, .. } => source.status_code(),
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

            Error::SerdeJson { .. } | Error::FileIo { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
