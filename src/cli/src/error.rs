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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_meta::peer::Peer;
use object_store::Error as ObjectStoreError;
use snafu::{Location, Snafu};
use store_api::storage::TableId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to install ring crypto provider: {}", msg))]
    InitTlsProvider {
        #[snafu(implicit)]
        location: Location,
        msg: String,
    },

    #[snafu(display("Failed to create default catalog and schema"))]
    InitMetadata {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to init DDL manager"))]
    InitDdlManager {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to init default timezone"))]
    InitTimezone {
        #[snafu(implicit)]
        location: Location,
        source: common_time::error::Error,
    },

    #[snafu(display("Failed to start procedure manager"))]
    StartProcedureManager {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to stop procedure manager"))]
    StopProcedureManager {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to start wal options allocator"))]
    StartWalOptionsAllocator {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to get table metadata"))]
    TableMetadata {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Unexpected error: {}", msg))]
    Unexpected {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing config, msg: {}", msg))]
    MissingConfig {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Illegal config: {}", msg))]
    IllegalConfig {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse proxy options: {}", error))]
    ParseProxyOpts {
        #[snafu(source)]
        error: reqwest::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build reqwest client: {}", error))]
    BuildClient {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: reqwest::Error,
    },

    #[snafu(display("Failed to parse SQL: {}", sql))]
    ParseSql {
        sql: String,
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement"))]
    PlanStatement {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to load layered config"))]
    LoadLayeredConfig {
        #[snafu(source(from(common_config::error::Error, Box::new)))]
        source: Box<common_config::error::Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to connect to Etcd at {etcd_addr}"))]
    ConnectEtcd {
        etcd_addr: String,
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serde json"))]
    SerdeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to run http request: {reason}"))]
    HttpQuerySql {
        reason: String,
        #[snafu(source)]
        error: reqwest::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty result from output"))]
    EmptyResult {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to manipulate file"))]
    FileIo {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to create directory {}", dir))]
    CreateDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to spawn thread"))]
    SpawnThread {
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Other error"))]
    Other {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build runtime"))]
    BuildRuntime {
        #[snafu(implicit)]
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Failed to get cache from cache registry: {}", name))]
    CacheRequired {
        #[snafu(implicit)]
        location: Location,
        name: String,
    },

    #[snafu(display("Failed to build cache registry"))]
    BuildCacheRegistry {
        #[snafu(implicit)]
        location: Location,
        source: cache::error::Error,
    },

    #[snafu(display("Failed to initialize meta client"))]
    MetaClientInit {
        #[snafu(implicit)]
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Cannot find schema {schema} in catalog {catalog}"))]
    SchemaNotFound {
        catalog: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {table_id}"))]
    TableNotFound {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("OpenDAL operator failed"))]
    OpenDal {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: ObjectStoreError,
    },

    #[snafu(display("S3 config need be set"))]
    S3ConfigNotSet {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Output directory not set"))]
    OutputDirNotSet {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty store addresses"))]
    EmptyStoreAddrs {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported memory backend"))]
    UnsupportedMemoryBackend {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("File path invalid: {}", msg))]
    InvalidFilePath {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid arguments: {}", msg))]
    InvalidArguments {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to init backend"))]
    InitBackend {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: ObjectStoreError,
    },

    #[snafu(display("Covert column schemas to defs failed"))]
    CovertColumnSchemasToDefs {
        #[snafu(implicit)]
        location: Location,
        source: operator::error::Error,
    },

    #[snafu(display("Failed to send request to datanode: {}", peer))]
    SendRequestToDatanode {
        peer: Peer,
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InitMetadata { source, .. }
            | Error::InitDdlManager { source, .. }
            | Error::TableMetadata { source, .. } => source.status_code(),

            Error::MissingConfig { .. }
            | Error::LoadLayeredConfig { .. }
            | Error::IllegalConfig { .. }
            | Error::InitTimezone { .. }
            | Error::ConnectEtcd { .. }
            | Error::CreateDir { .. }
            | Error::EmptyResult { .. }
            | Error::InvalidFilePath { .. }
            | Error::UnsupportedMemoryBackend { .. }
            | Error::InvalidArguments { .. }
            | Error::ParseProxyOpts { .. } => StatusCode::InvalidArguments,

            Error::CovertColumnSchemasToDefs { source, .. } => source.status_code(),
            Error::SendRequestToDatanode { source, .. } => source.status_code(),

            Error::StartProcedureManager { source, .. }
            | Error::StopProcedureManager { source, .. } => source.status_code(),
            Error::StartWalOptionsAllocator { source, .. } => source.status_code(),
            Error::HttpQuerySql { .. } => StatusCode::Internal,
            Error::ParseSql { source, .. } | Error::PlanStatement { source, .. } => {
                source.status_code()
            }
            Error::Unexpected { .. } => StatusCode::Unexpected,

            Error::SerdeJson { .. }
            | Error::FileIo { .. }
            | Error::SpawnThread { .. }
            | Error::InitTlsProvider { .. }
            | Error::BuildClient { .. } => StatusCode::Unexpected,

            Error::Other { source, .. } => source.status_code(),
            Error::OpenDal { .. } | Error::InitBackend { .. } => StatusCode::Internal,
            Error::S3ConfigNotSet { .. }
            | Error::OutputDirNotSet { .. }
            | Error::EmptyStoreAddrs { .. } => StatusCode::InvalidArguments,

            Error::BuildRuntime { source, .. } => source.status_code(),

            Error::CacheRequired { .. } | Error::BuildCacheRegistry { .. } => StatusCode::Internal,
            Error::MetaClientInit { source, .. } => source.status_code(),
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::SchemaNotFound { .. } => StatusCode::DatabaseNotFound,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
