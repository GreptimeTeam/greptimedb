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

use common_error::prelude::*;
use common_query::logical_plan::Expr;
use datafusion_common::ScalarValue;
use datatypes::prelude::Value;
use store_api::storage::RegionId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to request Datanode, source: {}", source))]
    RequestDatanode {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Runtime resource error, source: {}", source))]
    RuntimeResource {
        #[snafu(backtrace)]
        source: common_runtime::error::Error,
    },

    #[snafu(display("Failed to start server, source: {}", source))]
    StartServer {
        #[snafu(backtrace)]
        source: servers::error::Error,
    },

    #[snafu(display("Failed to parse address {}, source: {}", addr, source))]
    ParseAddr {
        addr: String,
        source: std::net::AddrParseError,
    },

    #[snafu(display("Failed to parse SQL, source: {}", source))]
    ParseSql {
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display(
        "Invalid column proto definition, column: {}, source: {}",
        column,
        source
    ))]
    InvalidColumnDef {
        column: String,
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display(
        "Failed to convert column default constraint, column: {}, source: {}",
        column_name,
        source
    ))]
    ConvertColumnDefaultConstraint {
        column_name: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid SQL, error: {}", err_msg))]
    InvalidSql {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Illegal Frontend state: {}", err_msg))]
    IllegalFrontendState {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Incomplete GRPC result: {}", err_msg))]
    IncompleteGrpcResult {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to convert DataFusion's ScalarValue: {:?}, source: {}",
        value,
        source
    ))]
    ConvertScalarValue {
        value: ScalarValue,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to find partition column: {}", column_name))]
    FindPartitionColumn {
        column_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find region, reason: {}", reason))]
    FindRegion {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find regions by filters: {:?}", filters))]
    FindRegions {
        filters: Vec<Expr>,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find Datanode by region: {:?}", region))]
    FindDatanode {
        region: RegionId,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Expect {} region keys, actual {}", expect, actual))]
    RegionKeysSize {
        expect: usize,
        actual: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Columns and values number mismatch, columns: {}, values: {}",
        columns,
        values,
    ))]
    ColumnValuesNumberMismatch {
        columns: usize,
        values: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to join task, source: {}", source))]
    JoinTask {
        source: common_runtime::JoinError,
        backtrace: Backtrace,
    },

    #[snafu(display("General catalog error: {}", source))]
    Catalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to serialize or deserialize catalog entry: {}", source))]
    CatalogEntrySerde {
        #[snafu(backtrace)]
        source: common_catalog::error::Error,
    },

    #[snafu(display("Failed to start Meta client, source: {}", source))]
    StartMetaClient {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to request Meta, source: {}", source))]
    RequestMeta {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to get cache, error: {}", err_msg))]
    GetCache {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find table routes for table {}", table_name))]
    FindTableRoutes {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create AlterExpr from Alter statement, source: {}", source))]
    AlterExprFromStmt {
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display("Failed to build CreateExpr on insertion: {}", source))]
    BuildCreateExprOnInsertion {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display(
        "Failed to convert GRPC InsertRequest to table InsertRequest, source: {}",
        source
    ))]
    ToTableInsertRequest {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to find catalog by name: {}", catalog_name))]
    CatalogNotFound {
        catalog_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find schema, schema info: {}", schema_info))]
    SchemaNotFound {
        schema_info: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Table occurs error, source: {}", source))]
    Table {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to find region routes for table {}", table_name))]
    FindRegionRoutes {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to serialize value to json, source: {}", source))]
    SerializeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to deserialize value from json, source: {}", source))]
    DeserializeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to find leader peer for region {} in table {}",
        region,
        table_name
    ))]
    FindLeaderPeer {
        region: u64,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to find partition info for region {} in table {}",
        region,
        table_name
    ))]
    FindRegionPartition {
        region: u64,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Illegal table routes data for table {}, error message: {}",
        table_name,
        err_msg
    ))]
    IllegalTableRoutesData {
        table_name: String,
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot find primary key column by name: {}", msg))]
    PrimaryKeyNotFound { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to execute statement, source: {}", source))]
    ExecuteStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to build DataFusion logical plan, source: {}", source))]
    BuildDfLogicalPlan {
        source: datafusion_common::DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("{source}"))]
    InvokeDatanode {
        #[snafu(backtrace)]
        source: datanode::error::Error,
    },

    #[snafu(display("Missing meta_client_opts section in config"))]
    MissingMetasrvOpts { backtrace: Backtrace },

    #[snafu(display("Failed to convert AlterExpr to AlterRequest, source: {}", source))]
    AlterExprToRequest {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to find leaders when altering table, table: {}", table))]
    LeaderNotFound { table: String, backtrace: Backtrace },

    #[snafu(display("Table already exists: `{}`", table))]
    TableAlreadyExist { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to encode Substrait logical plan, source: {}", source))]
    EncodeSubstraitLogicalPlan {
        #[snafu(backtrace)]
        source: substrait::error::Error,
    },

    #[snafu(display(
        "Failed to build a vector from values, value: {}, source: {}",
        value,
        source
    ))]
    BuildVector {
        value: Value,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to found context value: {}", key))]
    ContextValueNotFound { key: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to build table meta for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableMeta {
        table_name: String,
        source: table::metadata::TableMetaBuilderError,
        backtrace: Backtrace,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Failed to find new columns on insertion: {}", source))]
    FindNewColumnsOnInsertion {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display(
        "Failed to build default value, column: {}, source: {}",
        column,
        source
    ))]
    ColumnDefaultValue {
        column: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "No valid default value can be built automatically, column: {}",
        column,
    ))]
    ColumnNoneDefaultValue {
        column: String,
        backtrace: Backtrace,
    },

    #[snafu(display("SQL execution intercepted, source: {}", source))]
    SqlExecIntercepted {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ParseAddr { .. }
            | Error::InvalidSql { .. }
            | Error::FindRegion { .. }
            | Error::FindRegions { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::FindPartitionColumn { .. }
            | Error::ColumnValuesNumberMismatch { .. }
            | Error::RegionKeysSize { .. } => StatusCode::InvalidArguments,

            Error::NotSupported { .. } => StatusCode::Unsupported,

            Error::RuntimeResource { source, .. } => source.status_code(),

            Error::StartServer { source, .. } => source.status_code(),
            Error::SqlExecIntercepted { source, .. } => source.status_code(),

            Error::ParseSql { source } | Error::AlterExprFromStmt { source } => {
                source.status_code()
            }

            Error::Table { source } => source.status_code(),

            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::ConvertScalarValue { source, .. } => source.status_code(),

            Error::RequestDatanode { source } => source.status_code(),

            Error::ColumnDataType { source } | Error::InvalidColumnDef { source, .. } => {
                source.status_code()
            }

            Error::FindDatanode { .. }
            | Error::GetCache { .. }
            | Error::FindTableRoutes { .. }
            | Error::SerializeJson { .. }
            | Error::DeserializeJson { .. }
            | Error::FindRegionRoutes { .. }
            | Error::FindLeaderPeer { .. }
            | Error::FindRegionPartition { .. }
            | Error::IllegalTableRoutesData { .. }
            | Error::BuildDfLogicalPlan { .. }
            | Error::BuildTableMeta { .. } => StatusCode::Internal,

            Error::IllegalFrontendState { .. }
            | Error::IncompleteGrpcResult { .. }
            | Error::ContextValueNotFound { .. } => StatusCode::Unexpected,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            Error::JoinTask { .. } => StatusCode::Unexpected,
            Error::Catalog { source, .. } => source.status_code(),
            Error::CatalogEntrySerde { source, .. } => source.status_code(),

            Error::StartMetaClient { source } | Error::RequestMeta { source } => {
                source.status_code()
            }
            Error::SchemaNotFound { .. } => StatusCode::InvalidArguments,
            Error::CatalogNotFound { .. } => StatusCode::InvalidArguments,

            Error::BuildCreateExprOnInsertion { source }
            | Error::ToTableInsertRequest { source }
            | Error::FindNewColumnsOnInsertion { source } => source.status_code(),

            Error::PrimaryKeyNotFound { .. } => StatusCode::InvalidArguments,
            Error::ExecuteStatement { source, .. } => source.status_code(),
            Error::MissingMetasrvOpts { .. } => StatusCode::InvalidArguments,
            Error::AlterExprToRequest { source, .. } => source.status_code(),
            Error::LeaderNotFound { .. } => StatusCode::StorageUnavailable,
            Error::TableAlreadyExist { .. } => StatusCode::TableAlreadyExists,
            Error::EncodeSubstraitLogicalPlan { source } => source.status_code(),
            Error::BuildVector { source, .. } => source.status_code(),
            Error::InvokeDatanode { source } => source.status_code(),
            Error::ColumnDefaultValue { source, .. } => source.status_code(),
            Error::ColumnNoneDefaultValue { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        tonic::Status::new(tonic::Code::Internal, err.to_string())
    }
}
