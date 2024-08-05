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

use common_datasource::file_format::Format;
use common_error::define_into_tonic_status;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use store_api::storage::RegionNumber;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to invalidate table cache"))]
    InvalidateTableCache {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to open raft engine backend"))]
    OpenRaftEngineBackend {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to handle heartbeat response"))]
    HandleHeartbeatResponse {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("External error"))]
    External {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to query"))]
    RequestQuery {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Runtime resource error"))]
    RuntimeResource {
        #[snafu(implicit)]
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Failed to start server"))]
    StartServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown server"))]
    ShutdownServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to parse address {}", addr))]
    ParseAddr {
        addr: String,
        #[snafu(source)]
        error: std::net::AddrParseError,
    },

    #[snafu(display("Failed to parse SQL"))]
    ParseSql {
        #[snafu(implicit)]
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Failed to convert vector to GRPC column, reason: {}", reason))]
    VectorToGrpcColumn {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid SQL, error: {}", err_msg))]
    InvalidSql {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Incomplete GRPC request: {}", err_msg))]
    IncompleteGrpcRequest {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find Datanode by region: {:?}", region))]
    FindDatanode {
        region: RegionNumber,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("General catalog error"))]
    Catalog {
        #[snafu(implicit)]
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to start Meta client"))]
    StartMetaClient {
        #[snafu(implicit)]
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to create heartbeat stream to Metasrv"))]
    CreateMetaHeartbeatStream {
        source: meta_client::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table route for table id {}", table_id))]
    FindTableRoute {
        table_id: u32,
        #[snafu(implicit)]
        location: Location,
        source: partition::error::Error,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table occurs error"))]
    Table {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Cannot find column by name: {}", msg))]
    ColumnNotFound {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to plan statement"))]
    PlanStatement {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to read table: {table_name}"))]
    ReadTable {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute logical plan"))]
    ExecLogicalPlan {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Operation to region server failed"))]
    InvokeRegionServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to find leaders when altering table, table: {}", table))]
    LeaderNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to found context value: {}", key))]
    ContextValueNotFound {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("SQL execution intercepted"))]
    SqlExecIntercepted {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    // TODO(ruihang): merge all query execution error kinds
    #[snafu(display("Failed to execute PromQL query {}", query))]
    ExecutePromql {
        query: String,
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to create logical plan for prometheus query"))]
    PromStoreRemoteQueryPlan {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to describe schema for given statement"))]
    DescribeStatement {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Illegal primary keys definition: {}", msg))]
    IllegalPrimaryKeysDef {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "python")]
    #[snafu(display("Failed to start script manager"))]
    StartScriptManager {
        #[snafu(implicit)]
        location: Location,
        source: script::error::Error,
    },

    #[snafu(display("Failed to copy table: {}", table_name))]
    CopyTable {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to insert value into table: {}", table_name))]
    Insert {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Unsupported format: {:?}", format))]
    UnsupportedFormat {
        #[snafu(implicit)]
        location: Location,
        format: Format,
    },

    #[snafu(display("Failed to pass permission check"))]
    Permission {
        source: auth::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty data: {}", msg))]
    EmptyData {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "No valid default value can be built automatically, column: {}",
        column,
    ))]
    ColumnNoneDefaultValue {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region request, reason: {}", reason))]
    InvalidRegionRequest { reason: String },

    #[snafu(display("Table operation error"))]
    TableOperation {
        source: operator::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid auth config"))]
    IllegalAuthConfig { source: auth::error::Error },

    #[snafu(display("Failed to serialize options to TOML"))]
    TomlFormat {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source(from(common_config::error::Error, Box::new)))]
        source: Box<common_config::error::Error>,
    },

    #[snafu(display("Failed to get cache from cache registry: {}", name))]
    CacheRequired {
        #[snafu(implicit)]
        location: Location,
        name: String,
    },

    #[snafu(display("Invalid tls config"))]
    InvalidTlsConfig {
        #[snafu(source)]
        error: common_grpc::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::TomlFormat { .. }
            | Error::ParseAddr { .. }
            | Error::InvalidSql { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. }
            | Error::IllegalPrimaryKeysDef { .. }
            | Error::SchemaExists { .. }
            | Error::ColumnNotFound { .. }
            | Error::UnsupportedFormat { .. }
            | Error::IllegalAuthConfig { .. }
            | Error::EmptyData { .. }
            | Error::ColumnNoneDefaultValue { .. }
            | Error::IncompleteGrpcRequest { .. }
            | Error::InvalidTlsConfig { .. } => StatusCode::InvalidArguments,

            Error::NotSupported { .. } => StatusCode::Unsupported,

            Error::Permission { source, .. } => source.status_code(),

            Error::DescribeStatement { source, .. } => source.status_code(),

            Error::HandleHeartbeatResponse { source, .. } => source.status_code(),

            Error::RuntimeResource { source, .. } => source.status_code(),
            Error::PromStoreRemoteQueryPlan { source, .. }
            | Error::ExecutePromql { source, .. } => source.status_code(),

            Error::SqlExecIntercepted { source, .. } => source.status_code(),
            Error::StartServer { source, .. } => source.status_code(),
            Error::ShutdownServer { source, .. } => source.status_code(),

            Error::ParseSql { source, .. } => source.status_code(),

            Error::InvalidateTableCache { source, .. } => source.status_code(),

            Error::Table { source, .. }
            | Error::CopyTable { source, .. }
            | Error::Insert { source, .. } => source.status_code(),

            Error::OpenRaftEngineBackend { .. } => StatusCode::StorageUnavailable,

            Error::RequestQuery { source, .. } => source.status_code(),

            Error::FindDatanode { .. } => StatusCode::RegionNotReady,

            Error::VectorToGrpcColumn { .. } | Error::CacheRequired { .. } => StatusCode::Internal,

            Error::InvalidRegionRequest { .. } => StatusCode::IllegalState,

            Error::ContextValueNotFound { .. } => StatusCode::Unexpected,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,

            Error::Catalog { source, .. } => source.status_code(),

            Error::StartMetaClient { source, .. }
            | Error::CreateMetaHeartbeatStream { source, .. } => source.status_code(),

            Error::PlanStatement { source, .. }
            | Error::ReadTable { source, .. }
            | Error::ExecLogicalPlan { source, .. } => source.status_code(),

            Error::LeaderNotFound { .. } => StatusCode::StorageUnavailable,
            Error::InvokeRegionServer { source, .. } => source.status_code(),

            Error::External { source, .. } => source.status_code(),
            Error::FindTableRoute { source, .. } => source.status_code(),

            #[cfg(feature = "python")]
            Error::StartScriptManager { source, .. } => source.status_code(),

            Error::TableOperation { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);

impl From<operator::error::Error> for Error {
    fn from(e: operator::error::Error) -> Error {
        Error::TableOperation {
            source: e,
            location: Location::default(),
        }
    }
}
