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
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::error::Error as TableError;

/// Business error of datanode.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to execute async task"))]
    AsyncTaskExecute {
        location: Location,
        source: Arc<Error>,
    },

    #[snafu(display("Failed to watch change"))]
    WatchAsyncTaskChange {
        location: Location,
        #[snafu(source)]
        error: tokio::sync::watch::error::RecvError,
    },

    #[snafu(display("Failed to handle heartbeat response"))]
    HandleHeartbeatResponse {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to get info from meta server"))]
    GetMetadata {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute logical plan"))]
    ExecuteLogicalPlan {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to decode logical plan"))]
    DecodeLogicalPlan {
        location: Location,
        source: substrait::error::Error,
    },

    #[snafu(display("Incorrect internal state: {}", state))]
    IncorrectInternalState { state: String, location: Location },

    #[snafu(display("Catalog not found: {}", name))]
    CatalogNotFound { name: String, location: Location },

    #[snafu(display("Schema not found: {}", name))]
    SchemaNotFound { name: String, location: Location },

    #[snafu(display("Missing timestamp column in request"))]
    MissingTimestampColumn { location: Location },

    #[snafu(display(
        "Columns and values number mismatch, columns: {}, values: {}",
        columns,
        values
    ))]
    ColumnValuesNumberMismatch { columns: usize, values: usize },

    #[snafu(display("Failed to delete value from table: {}", table_name))]
    Delete {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to start server"))]
    StartServer {
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to parse address {}", addr))]
    ParseAddr {
        addr: String,
        #[snafu(source)]
        error: std::net::AddrParseError,
    },

    #[snafu(display("Failed to create directory {}", dir))]
    CreateDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to remove directory {}", dir))]
    RemoveDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to open log store"))]
    OpenLogStore {
        location: Location,
        source: Box<log_store::error::Error>,
    },

    #[snafu(display("Failed to init backend"))]
    InitBackend {
        #[snafu(source)]
        error: object_store::Error,
        location: Location,
    },

    #[snafu(display("Runtime resource error"))]
    RuntimeResource {
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Expect KvBackend but not found"))]
    MissingKvBackend { location: Location },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String },

    #[snafu(display("Not support SQL, error: {}", msg))]
    NotSupportSql { msg: String },

    #[snafu(display("Specified timestamp key or primary key column not found: {}", name))]
    KeyColumnNotFound { name: String, location: Location },

    #[snafu(display("Illegal primary keys definition: {}", msg))]
    IllegalPrimaryKeysDef { msg: String, location: Location },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists { name: String, location: Location },

    #[snafu(display("Failed to access catalog"))]
    Catalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to initialize meta client"))]
    MetaClientInit {
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display(
        "Table id provider not found, cannot execute SQL directly on datanode in distributed mode"
    ))]
    TableIdProviderNotFound { location: Location },

    #[snafu(display("Missing node id in Datanode config"))]
    MissingNodeId { location: Location },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, location: Location },

    #[snafu(display("Cannot find requested database: {}-{}", catalog, schema))]
    DatabaseNotFound { catalog: String, schema: String },

    #[snafu(display(
        "No valid default value can be built automatically, column: {}",
        column,
    ))]
    ColumnNoneDefaultValue { column: String, location: Location },

    #[snafu(display("Failed to shutdown server"))]
    ShutdownServer {
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown instance"))]
    ShutdownInstance {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Payload not exist"))]
    PayloadNotExist { location: Location },

    #[snafu(display("Missing WAL dir config"))]
    MissingWalDirConfig { location: Location },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("Failed to handle request for region {}", region_id))]
    HandleRegionRequest {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("RegionId {} not found", region_id))]
    RegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region {} not ready", region_id))]
    RegionNotReady {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region {} is busy", region_id))]
    RegionBusy {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region engine {} is not registered", name))]
    RegionEngineNotFound { name: String, location: Location },

    #[snafu(display("Unsupported gRPC request, kind: {}", kind))]
    UnsupportedGrpcRequest { kind: String, location: Location },

    #[snafu(display("Unsupported output type, expected: {}", expected))]
    UnsupportedOutput {
        expected: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to get metadata from engine {} for region_id {}",
        engine,
        region_id,
    ))]
    GetRegionMetadata {
        engine: String,
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to build region requests"))]
    BuildRegionRequests {
        location: Location,
        source: store_api::metadata::MetadataError,
    },

    #[snafu(display("Failed to stop region engine {}", name))]
    StopRegionEngine {
        name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to find logical regions in physical region {}",
        physical_region_id
    ))]
    FindLogicalRegions {
        physical_region_id: RegionId,
        source: metric_engine::error::Error,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            ExecuteLogicalPlan { source, .. } => source.status_code(),

            BuildRegionRequests { source, .. } => source.status_code(),
            HandleHeartbeatResponse { source, .. } | GetMetadata { source, .. } => {
                source.status_code()
            }

            DecodeLogicalPlan { source, .. } => source.status_code(),

            Delete { source, .. } => source.status_code(),

            ColumnValuesNumberMismatch { .. }
            | InvalidSql { .. }
            | NotSupportSql { .. }
            | KeyColumnNotFound { .. }
            | IllegalPrimaryKeysDef { .. }
            | MissingTimestampColumn { .. }
            | CatalogNotFound { .. }
            | SchemaNotFound { .. }
            | SchemaExists { .. }
            | DatabaseNotFound { .. }
            | MissingNodeId { .. }
            | ColumnNoneDefaultValue { .. }
            | MissingWalDirConfig { .. }
            | MissingKvBackend { .. } => StatusCode::InvalidArguments,

            PayloadNotExist { .. } | Unexpected { .. } | WatchAsyncTaskChange { .. } => {
                StatusCode::Unexpected
            }

            AsyncTaskExecute { source, .. } => source.status_code(),

            // TODO(yingwen): Further categorize http error.
            ParseAddr { .. }
            | CreateDir { .. }
            | RemoveDir { .. }
            | Catalog { .. }
            | MissingRequiredField { .. }
            | IncorrectInternalState { .. }
            | ShutdownInstance { .. }
            | RegionEngineNotFound { .. }
            | UnsupportedOutput { .. }
            | GetRegionMetadata { .. } => StatusCode::Internal,

            RegionNotFound { .. } => StatusCode::RegionNotFound,
            RegionNotReady { .. } => StatusCode::RegionNotReady,
            RegionBusy { .. } => StatusCode::RegionBusy,

            StartServer { source, .. } | ShutdownServer { source, .. } => source.status_code(),

            InitBackend { .. } => StatusCode::StorageUnavailable,

            OpenLogStore { source, .. } => source.status_code(),
            RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
            MetaClientInit { source, .. } => source.status_code(),
            TableIdProviderNotFound { .. } | UnsupportedGrpcRequest { .. } => {
                StatusCode::Unsupported
            }
            HandleRegionRequest { source, .. } => source.status_code(),
            StopRegionEngine { source, .. } => source.status_code(),

            FindLogicalRegions { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
