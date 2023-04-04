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
use snafu::Location;
use store_api::storage::RegionId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{source}"))]
    External {
        #[snafu(backtrace)]
        source: BoxedError,
    },

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

    #[snafu(display("Failed to shutdown server, source: {}", source))]
    ShutdownServer {
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

    #[snafu(display("Missing insert values"))]
    MissingInsertValues { location: Location },

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
    InvalidSql { err_msg: String, location: Location },

    #[snafu(display("Illegal Frontend state: {}", err_msg))]
    IllegalFrontendState { err_msg: String, location: Location },

    #[snafu(display("Incomplete GRPC result: {}", err_msg))]
    IncompleteGrpcResult { err_msg: String, location: Location },

    #[snafu(display("Failed to find Datanode by region: {:?}", region))]
    FindDatanode {
        region: RegionId,
        location: Location,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest { reason: String, location: Location },

    #[snafu(display("Table `{}` not exist", table_name))]
    TableNotFound {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Columns and values number mismatch, columns: {}, values: {}",
        columns,
        values,
    ))]
    ColumnValuesNumberMismatch {
        columns: usize,
        values: usize,
        location: Location,
    },

    #[snafu(display("Failed to join task, source: {}", source))]
    JoinTask {
        source: common_runtime::JoinError,
        location: Location,
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

    #[snafu(display("Failed to create table route for table {}", table_name))]
    CreateTableRoute {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to find table route for table {}", table_name))]
    FindTableRoute {
        table_name: String,
        #[snafu(backtrace)]
        source: partition::error::Error,
    },

    #[snafu(display("Failed to create table info, source: {}", source))]
    CreateTableInfo {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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
        location: Location,
    },

    #[snafu(display("Failed to find schema, schema info: {}", schema_info))]
    SchemaNotFound {
        schema_info: String,
        location: Location,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists { name: String, location: Location },

    #[snafu(display("Table occurs error, source: {}", source))]
    Table {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to find region route for table {}", table_name))]
    FindRegionRoute {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Cannot find primary key column by name: {}", msg))]
    PrimaryKeyNotFound { msg: String, location: Location },

    #[snafu(display("Failed to execute statement, source: {}", source))]
    ExecuteStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement, source: {}", source))]
    PlanStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to parse query, source: {}", source))]
    ParseQuery {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute logical plan, source: {}", source))]
    ExecLogicalPlan {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to build DataFusion logical plan, source: {}", source))]
    BuildDfLogicalPlan {
        source: datafusion_common::DataFusionError,
        location: Location,
    },

    #[snafu(display("{source}"))]
    InvokeDatanode {
        #[snafu(backtrace)]
        source: datanode::error::Error,
    },

    #[snafu(display("Missing meta_client_options section in config"))]
    MissingMetasrvOpts { location: Location },

    #[snafu(display("Failed to convert AlterExpr to AlterRequest, source: {}", source))]
    AlterExprToRequest {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to find leaders when altering table, table: {}", table))]
    LeaderNotFound { table: String, location: Location },

    #[snafu(display("Table already exists: `{}`", table))]
    TableAlreadyExist { table: String, location: Location },

    #[snafu(display("Failed to encode Substrait logical plan, source: {}", source))]
    EncodeSubstraitLogicalPlan {
        #[snafu(backtrace)]
        source: substrait::error::Error,
    },

    #[snafu(display("Failed to found context value: {}", key))]
    ContextValueNotFound { key: String, location: Location },

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
    ColumnNoneDefaultValue { column: String, location: Location },

    #[snafu(display("SQL execution intercepted, source: {}", source))]
    SqlExecIntercepted {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to deserialize partition in meta to partition def, source: {}",
        source
    ))]
    DeserializePartition {
        #[snafu(backtrace)]
        source: partition::error::Error,
    },

    // TODO(ruihang): merge all query execution error kinds
    #[snafu(display("failed to execute PromQL query {}, source: {}", query, source))]
    ExecutePromql {
        query: String,
        #[snafu(backtrace)]
        source: servers::error::Error,
    },

    #[snafu(display("Failed to describe schema for given statement, source: {}", source))]
    DescribeStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Illegal primary keys definition: {}", msg))]
    IllegalPrimaryKeysDef { msg: String, location: Location },

    #[snafu(display("Unrecognized table option: {}", source))]
    UnrecognizedTableOption {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to start script manager, source: {}", source))]
    StartScriptManager {
        #[snafu(backtrace)]
        source: script::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ParseAddr { .. }
            | Error::InvalidSql { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::ColumnValuesNumberMismatch { .. }
            | Error::IllegalPrimaryKeysDef { .. }
            | Error::CatalogNotFound { .. }
            | Error::SchemaNotFound { .. }
            | Error::SchemaExists { .. }
            | Error::MissingInsertValues { .. }
            | Error::PrimaryKeyNotFound { .. }
            | Error::MissingMetasrvOpts { .. }
            | Error::ColumnNoneDefaultValue { .. } => StatusCode::InvalidArguments,

            Error::NotSupported { .. } => StatusCode::Unsupported,

            Error::RuntimeResource { source, .. } => source.status_code(),
            Error::ExecutePromql { source, .. } => source.status_code(),

            Error::SqlExecIntercepted { source, .. } => source.status_code(),
            Error::StartServer { source, .. } => source.status_code(),
            Error::ShutdownServer { source, .. } => source.status_code(),

            Error::ParseSql { source } => source.status_code(),

            Error::Table { source } => source.status_code(),

            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::CreateTableInfo { source } => source.status_code(),

            Error::RequestDatanode { source } => source.status_code(),

            Error::ColumnDataType { source } | Error::InvalidColumnDef { source, .. } => {
                source.status_code()
            }

            Error::FindDatanode { .. }
            | Error::CreateTableRoute { .. }
            | Error::FindRegionRoute { .. }
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
            Error::BuildCreateExprOnInsertion { source }
            | Error::ToTableInsertRequest { source }
            | Error::FindNewColumnsOnInsertion { source } => source.status_code(),

            Error::ExecuteStatement { source, .. }
            | Error::PlanStatement { source }
            | Error::ParseQuery { source }
            | Error::ExecLogicalPlan { source }
            | Error::DescribeStatement { source } => source.status_code(),

            Error::AlterExprToRequest { source, .. } => source.status_code(),
            Error::LeaderNotFound { .. } => StatusCode::StorageUnavailable,
            Error::TableAlreadyExist { .. } => StatusCode::TableAlreadyExists,
            Error::EncodeSubstraitLogicalPlan { source } => source.status_code(),
            Error::InvokeDatanode { source } => source.status_code(),
            Error::ColumnDefaultValue { source, .. } => source.status_code(),

            Error::External { source } => source.status_code(),
            Error::DeserializePartition { source, .. } | Error::FindTableRoute { source, .. } => {
                source.status_code()
            }
            Error::UnrecognizedTableOption { .. } => StatusCode::InvalidArguments,

            Error::StartScriptManager { source } => source.status_code(),
        }
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
