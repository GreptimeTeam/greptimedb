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

use std::str::Utf8Error;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use crate::peer::Peer;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to get sequence: {}", err_msg))]
    NextSequence { err_msg: String, location: Location },

    #[snafu(display("Sequence out of range: {}, start={}, step={}", name, start, step))]
    SequenceOutOfRange {
        name: String,
        start: u64,
        step: u64,
        location: Location,
    },

    #[snafu(display("Unexpected sequence value: {}", err_msg))]
    UnexpectedSequenceValue { err_msg: String, location: Location },

    #[snafu(display("Table info not found: {}", table_name))]
    TableInfoNotFound {
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to register procedure loader, type name: {}, source: {}",
        type_name,
        source
    ))]
    RegisterProcedureLoader {
        type_name: String,
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to submit procedure, source: {source}"))]
    SubmitProcedure {
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Unsupported operation {}, location: {}", operation, location))]
    Unsupported {
        operation: String,
        location: Location,
    },

    #[snafu(display("Failed to wait procedure done, source: {source}"))]
    WaitProcedure {
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to convert RawTableInfo into TableInfo: {}", source))]
    ConvertRawTableInfo {
        location: Location,
        source: datatypes::Error,
    },

    #[snafu(display("Primary key '{key}' not found when creating region request, at {location}"))]
    PrimaryKeyNotFound { key: String, location: Location },

    #[snafu(display(
        "Failed to build table meta for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableMeta {
        table_name: String,
        source: table::metadata::TableMetaBuilderError,
        location: Location,
    },

    #[snafu(display("Table occurs error, source: {}", source))]
    Table {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Table route not found: {}", table_name))]
    TableRouteNotFound {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to decode protobuf, source: {}", source))]
    DecodeProto {
        location: Location,
        source: prost::DecodeError,
    },

    #[snafu(display("Failed to encode object into json, source: {}", source))]
    EncodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Failed to decode object from json, source: {}", source))]
    DecodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Payload not exist"))]
    PayloadNotExist { location: Location },

    #[snafu(display("Failed to send message: {err_msg}"))]
    SendMessage { err_msg: String, location: Location },

    #[snafu(display("Failed to serde json, source: {}", source))]
    SerdeJson {
        source: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse value {} into key {}", value, key))]
    ParseOption {
        key: String,
        value: String,
        location: Location,
    },

    #[snafu(display("Corrupted table route data, err: {}", err_msg))]
    RouteInfoCorrupted { err_msg: String, location: Location },

    #[snafu(display("Illegal state from server, code: {}, error: {}", code, err_msg))]
    IllegalServerState {
        code: i32,
        err_msg: String,
        location: Location,
    },

    #[snafu(display("Failed to convert alter table request, source: {source}, at {location}"))]
    ConvertAlterTableRequest {
        source: common_grpc_expr::error::Error,
        location: Location,
    },

    #[snafu(display("Invalid protobuf message: {err_msg}, at {location}"))]
    InvalidProtoMsg { err_msg: String, location: Location },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected { err_msg: String, location: Location },

    #[snafu(display("Table already exists, table: {}", table_name))]
    TableAlreadyExists {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Catalog already exists, catalog: {}", catalog))]
    CatalogAlreadyExists { catalog: String, location: Location },

    #[snafu(display("Schema already exists, catalog:{}, schema: {}", catalog, schema))]
    SchemaAlreadyExists {
        catalog: String,
        schema: String,
        location: Location,
    },

    #[snafu(display("Failed to convert raw key to str, source: {}", source))]
    ConvertRawKey {
        location: Location,
        source: Utf8Error,
    },

    #[snafu(display("Table nod found, table: {}", table_name))]
    TableNotFound {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to rename table, reason: {}", reason))]
    RenameTable { reason: String, location: Location },

    #[snafu(display("Invalid table metadata, err: {}", err_msg))]
    InvalidTableMetadata { err_msg: String, location: Location },

    #[snafu(display("Failed to get kv cache, err: {}", err_msg))]
    GetKvCache { err_msg: String },

    #[snafu(display("Get null from cache, key: {}", key))]
    CacheNotGet { key: String, location: Location },

    #[snafu(display("Etcd txn error: {err_msg}"))]
    EtcdTxnOpResponse { err_msg: String, location: Location },

    #[snafu(display(
        "Failed to move region {} in table {}, err: {}",
        region,
        table_id,
        err_msg
    ))]
    MoveRegion {
        table_id: TableId,
        region: RegionNumber,
        err_msg: String,
        location: Location,
    },

    #[snafu(display("Invalid catalog value, source: {}", source))]
    InvalidCatalogValue {
        source: common_catalog::error::Error,
        location: Location,
    },

    #[snafu(display("{}", source))]
    External {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Invalid heartbeat response, location: {}", location))]
    InvalidHeartbeatResponse { location: Location },

    #[snafu(display("Failed to operate on datanode: {}, source: {}", peer, source))]
    OperateDatanode {
        location: Location,
        peer: Peer,
        source: BoxedError,
    },

    #[snafu(display("Retry later, source: {}", source))]
    RetryLater { source: BoxedError },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            IllegalServerState { .. } | EtcdTxnOpResponse { .. } => StatusCode::Internal,

            SerdeJson { .. }
            | ParseOption { .. }
            | RouteInfoCorrupted { .. }
            | InvalidProtoMsg { .. }
            | InvalidTableMetadata { .. }
            | MoveRegion { .. }
            | Unexpected { .. }
            | TableInfoNotFound { .. }
            | NextSequence { .. }
            | SequenceOutOfRange { .. }
            | UnexpectedSequenceValue { .. }
            | InvalidHeartbeatResponse { .. } => StatusCode::Unexpected,

            SendMessage { .. }
            | GetKvCache { .. }
            | CacheNotGet { .. }
            | CatalogAlreadyExists { .. }
            | SchemaAlreadyExists { .. }
            | RenameTable { .. }
            | Unsupported { .. } => StatusCode::Internal,

            PrimaryKeyNotFound { .. } => StatusCode::InvalidArguments,

            TableNotFound { .. } => StatusCode::TableNotFound,
            TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,

            EncodeJson { .. }
            | DecodeJson { .. }
            | PayloadNotExist { .. }
            | ConvertRawKey { .. }
            | DecodeProto { .. }
            | BuildTableMeta { .. }
            | TableRouteNotFound { .. }
            | ConvertRawTableInfo { .. } => StatusCode::Unexpected,

            SubmitProcedure { source, .. } | WaitProcedure { source, .. } => source.status_code(),
            RegisterProcedureLoader { source, .. } => source.status_code(),
            External { source, .. } => source.status_code(),
            OperateDatanode { source, .. } => source.status_code(),
            Table { source, .. } => source.status_code(),
            RetryLater { source, .. } => source.status_code(),
            InvalidCatalogValue { source, .. } => source.status_code(),
            ConvertAlterTableRequest { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Error {
    /// Creates a new [Error::RetryLater] error from source `err`.
    pub fn retry_later<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::RetryLater {
            source: BoxedError::new(err),
        }
    }

    /// Determine whether it is a retry later type through [StatusCode]
    pub fn is_retry_later(&self) -> bool {
        matches!(self, Error::RetryLater { .. })
    }
}
