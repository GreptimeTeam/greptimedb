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

use common_error::define_into_tonic_status;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::error::Error as TableError;
use tokio::time::error::Elapsed;

/// Business error of datanode.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to execute async task"))]
    AsyncTaskExecute {
        #[snafu(implicit)]
        location: Location,
        source: Arc<Error>,
    },

    #[snafu(display("Failed to watch change"))]
    WatchAsyncTaskChange {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: tokio::sync::watch::error::RecvError,
    },

    #[snafu(display("Failed to handle heartbeat response"))]
    HandleHeartbeatResponse {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to get info from meta server"))]
    GetMetadata {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute logical plan"))]
    ExecuteLogicalPlan {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to create plan decoder"))]
    NewPlanDecoder {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to decode logical plan"))]
    DecodeLogicalPlan {
        #[snafu(implicit)]
        location: Location,
        source: common_query::error::Error,
    },

    #[snafu(display("Catalog not found: {}", name))]
    CatalogNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema not found: {}", name))]
    SchemaNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing timestamp column in request"))]
    MissingTimestampColumn {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to delete value from table: {}", table_name))]
    Delete {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to start server"))]
    StartServer {
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
        #[snafu(implicit)]
        location: Location,
        source: Box<log_store::error::Error>,
    },

    #[snafu(display("Failed to init backend"))]
    InitBackend {
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String },

    #[snafu(display("Illegal primary keys definition: {}", msg))]
    IllegalPrimaryKeysDef {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to access catalog"))]
    Catalog {
        #[snafu(implicit)]
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to initialize meta client"))]
    MetaClientInit {
        #[snafu(implicit)]
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Missing node id in Datanode config"))]
    MissingNodeId {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build http client"))]
    BuildHttpClient {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: reqwest::Error,
    },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField {
        name: String,
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

    #[snafu(display("Failed to shutdown server"))]
    ShutdownServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown instance"))]
    ShutdownInstance {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Payload not exist"))]
    PayloadNotExist {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to handle request for region {}", region_id))]
    HandleRegionRequest {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to open batch regions"))]
    HandleBatchOpenRequest {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to handle batch ddl request, ddl_type: {}", ddl_type))]
    HandleBatchDdlRequest {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
        ddl_type: String,
    },

    #[snafu(display("RegionId {} not found", region_id))]
    RegionNotFound {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} not ready", region_id))]
    RegionNotReady {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is busy", region_id))]
    RegionBusy {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region engine {} is not registered", name))]
    RegionEngineNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported output type, expected: {}", expected))]
    UnsupportedOutput {
        expected: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build region requests"))]
    BuildRegionRequests {
        #[snafu(implicit)]
        location: Location,
        source: store_api::metadata::MetadataError,
    },

    #[snafu(display("Failed to stop region engine {}", name))]
    StopRegionEngine {
        name: String,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build mito engine"))]
    BuildMitoEngine {
        source: mito2::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build metric engine"))]
    BuildMetricEngine {
        source: metric_engine::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize options to TOML"))]
    TomlFormat {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source(from(common_config::error::Error, Box::new)))]
        source: Box<common_config::error::Error>,
    },

    #[snafu(display(
        "Failed to get region metadata from engine {} for region_id {}",
        engine,
        region_id,
    ))]
    GetRegionMetadata {
        engine: String,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("DataFusion"))]
    DataFusion {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to acquire permit, source closed"))]
    ConcurrentQueryLimiterClosed {
        #[snafu(source)]
        error: tokio::sync::AcquireError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to acquire permit under timeouts"))]
    ConcurrentQueryLimiterTimeout {
        #[snafu(source)]
        error: Elapsed,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cache not found in registry"))]
    MissingCache {
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            NewPlanDecoder { source, .. } | ExecuteLogicalPlan { source, .. } => {
                source.status_code()
            }

            BuildRegionRequests { source, .. } => source.status_code(),
            HandleHeartbeatResponse { source, .. } | GetMetadata { source, .. } => {
                source.status_code()
            }

            DecodeLogicalPlan { source, .. } => source.status_code(),

            Delete { source, .. } => source.status_code(),

            InvalidSql { .. }
            | IllegalPrimaryKeysDef { .. }
            | MissingTimestampColumn { .. }
            | CatalogNotFound { .. }
            | SchemaNotFound { .. }
            | SchemaExists { .. }
            | MissingNodeId { .. }
            | ColumnNoneDefaultValue { .. }
            | Catalog { .. }
            | MissingRequiredField { .. }
            | RegionEngineNotFound { .. }
            | ParseAddr { .. }
            | TomlFormat { .. } => StatusCode::InvalidArguments,

            PayloadNotExist { .. }
            | Unexpected { .. }
            | WatchAsyncTaskChange { .. }
            | BuildHttpClient { .. } => StatusCode::Unexpected,

            AsyncTaskExecute { source, .. } => source.status_code(),

            CreateDir { .. } | RemoveDir { .. } | ShutdownInstance { .. } | DataFusion { .. } => {
                StatusCode::Internal
            }

            RegionNotFound { .. } => StatusCode::RegionNotFound,
            RegionNotReady { .. } => StatusCode::RegionNotReady,
            RegionBusy { .. } => StatusCode::RegionBusy,

            StartServer { source, .. } | ShutdownServer { source, .. } => source.status_code(),

            InitBackend { .. } => StatusCode::StorageUnavailable,

            OpenLogStore { source, .. } => source.status_code(),
            MetaClientInit { source, .. } => source.status_code(),
            UnsupportedOutput { .. } => StatusCode::Unsupported,
            HandleRegionRequest { source, .. }
            | GetRegionMetadata { source, .. }
            | HandleBatchOpenRequest { source, .. }
            | HandleBatchDdlRequest { source, .. } => source.status_code(),
            StopRegionEngine { source, .. } => source.status_code(),

            FindLogicalRegions { source, .. } => source.status_code(),
            BuildMitoEngine { source, .. } => source.status_code(),
            BuildMetricEngine { source, .. } => source.status_code(),
            ConcurrentQueryLimiterClosed { .. } | ConcurrentQueryLimiterTimeout { .. } => {
                StatusCode::RegionBusy
            }
            MissingCache { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
