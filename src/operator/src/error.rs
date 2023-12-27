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
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::parquet;
use datatypes::arrow::error::ArrowError;
use datatypes::value::Value;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Table already exists: `{}`", table))]
    TableAlreadyExists { table: String, location: Location },

    #[snafu(display("Failed to invalidate table cache"))]
    InvalidateTableCache {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute ddl"))]
    ExecuteDdl {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("external error"))]
    External {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to insert data"))]
    RequestInserts {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to delete data"))]
    RequestDeletes {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to parse SQL"))]
    ParseSql {
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Failed to convert value to sql value: {}", value))]
    ConvertSqlValue {
        value: Value,
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Column datatype error"))]
    ColumnDataType {
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Invalid column proto definition, column: {}", column))]
    InvalidColumnDef {
        column: String,
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Failed to convert column default constraint, column: {}", column_name))]
    ConvertColumnDefaultConstraint {
        column_name: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid SQL, error: {}", err_msg))]
    InvalidSql { err_msg: String, location: Location },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest { reason: String, location: Location },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest { reason: String, location: Location },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: common_runtime::JoinError,
        location: Location,
    },

    #[snafu(display("General catalog error"))]
    Catalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to find table partition rule for table {}", table_name))]
    FindTablePartitionRule {
        table_name: String,
        location: Location,
        source: partition::error::Error,
    },

    #[snafu(display("Failed to split insert request"))]
    SplitInsert {
        source: partition::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to split delete request"))]
    SplitDelete {
        source: partition::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to find leader for region"))]
    FindRegionLeader {
        source: partition::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to create table info"))]
    CreateTableInfo {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to build CreateExpr on insertion"))]
    BuildCreateExprOnInsertion {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to find schema, schema info: {}", schema_info))]
    SchemaNotFound {
        schema_info: String,
        location: Location,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists { name: String, location: Location },

    #[snafu(display("Table occurs error"))]
    Table {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Cannot find column by name: {}", msg))]
    ColumnNotFound { msg: String, location: Location },

    #[snafu(display("Failed to execute statement"))]
    ExecuteStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement"))]
    PlanStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to parse query"))]
    ParseQuery {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute logical plan"))]
    ExecLogicalPlan {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to build DataFusion logical plan"))]
    BuildDfLogicalPlan {
        #[snafu(source)]
        error: datafusion_common::DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to convert AlterExpr to AlterRequest"))]
    AlterExprToRequest {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to build table meta for table: {}", table_name))]
    BuildTableMeta {
        table_name: String,
        #[snafu(source)]
        error: table::metadata::TableMetaBuilderError,
        location: Location,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Failed to find new columns on insertion"))]
    FindNewColumnsOnInsertion {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to convert into vectors"))]
    IntoVectors {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to deserialize partition in meta to partition def"))]
    DeserializePartition {
        location: Location,
        source: partition::error::Error,
    },

    #[snafu(display("Failed to describe schema for given statement"))]
    DescribeStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Illegal primary keys definition: {}", msg))]
    IllegalPrimaryKeysDef { msg: String, location: Location },

    #[snafu(display("Unrecognized table option"))]
    UnrecognizedTableOption {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Missing time index column"))]
    MissingTimeIndexColumn {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to build regex"))]
    BuildRegex {
        location: Location,
        #[snafu(source)]
        error: regex::Error,
    },

    #[snafu(display("Failed to copy table: {}", table_name))]
    CopyTable {
        table_name: String,
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to insert value into table: {}", table_name))]
    Insert {
        table_name: String,
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to parse data source url"))]
    ParseUrl {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Unsupported format: {:?}", format))]
    UnsupportedFormat { location: Location, format: Format },

    #[snafu(display("Failed to parse file format"))]
    ParseFileFormat {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build data source backend"))]
    BuildBackend {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to list objects"))]
    ListObjects {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to infer schema from path: {}", path))]
    InferSchema {
        path: String,
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build csv config"))]
    BuildCsvConfig {
        #[snafu(source)]
        error: common_datasource::file_format::csv::CsvConfigBuilderError,
        location: Location,
    },

    #[snafu(display("Failed to write stream to path: {}", path))]
    WriteStreamToFile {
        path: String,
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to read object in path: {}", path))]
    ReadObject {
        path: String,
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Failed to read record batch"))]
    ReadDfRecordBatch {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to read parquet file"))]
    ReadParquet {
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Failed to read orc schema"))]
    ReadOrc {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build parquet record batch stream"))]
    BuildParquetRecordBatchStream {
        location: Location,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to build file stream"))]
    BuildFileStream {
        location: Location,
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Schema datatypes not match at index {}, expected table schema: {}, actual file schema: {}",
        index,
        table_schema,
        file_schema
    ))]
    InvalidSchema {
        index: usize,
        table_schema: String,
        file_schema: String,
        location: Location,
    },

    #[snafu(display("Failed to project schema"))]
    ProjectSchema {
        #[snafu(source)]
        error: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to encode object into json"))]
    EncodeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to prepare immutable table"))]
    PrepareImmutableTable {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Invalid COPY parameter, key: {}, value: {}", key, value))]
    InvalidCopyParameter {
        key: String,
        value: String,
        location: Location,
    },

    #[snafu(display("Table metadata manager error"))]
    TableMetadataManager {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to read record batch"))]
    ReadRecordBatch {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build column vectors"))]
    BuildColumnVectors {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Missing insert body"))]
    MissingInsertBody {
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build default value, column: {}", column))]
    ColumnDefaultValue {
        column: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "No valid default value can be built automatically, column: {}",
        column,
    ))]
    ColumnNoneDefaultValue { column: String, location: Location },

    #[snafu(display(
        "Invalid partition columns when creating table '{}', reason: {}",
        table,
        reason
    ))]
    InvalidPartitionColumns {
        table: String,
        reason: String,
        location: Location,
    },

    #[snafu(display("Failed to prepare file table"))]
    PrepareFileTable {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to infer file table schema"))]
    InferFileTableSchema {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("The schema of the file table is incompatible with the table schema"))]
    SchemaIncompatible {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Invalid table name: {}", table_name))]
    InvalidTableName {
        table_name: String,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSql { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. }
            | Error::IllegalPrimaryKeysDef { .. }
            | Error::SchemaNotFound { .. }
            | Error::SchemaExists { .. }
            | Error::ColumnNotFound { .. }
            | Error::BuildRegex { .. }
            | Error::InvalidSchema { .. }
            | Error::PrepareImmutableTable { .. }
            | Error::BuildCsvConfig { .. }
            | Error::ProjectSchema { .. }
            | Error::UnsupportedFormat { .. }
            | Error::ColumnNoneDefaultValue { .. }
            | Error::InvalidPartitionColumns { .. }
            | Error::PrepareFileTable { .. }
            | Error::InferFileTableSchema { .. }
            | Error::SchemaIncompatible { .. }
            | Error::InvalidTableName { .. } => StatusCode::InvalidArguments,

            Error::TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,

            Error::NotSupported { .. } => StatusCode::Unsupported,

            Error::TableMetadataManager { source, .. } => source.status_code(),

            Error::ConvertSqlValue { source, .. } | Error::ParseSql { source, .. } => {
                source.status_code()
            }

            Error::InvalidateTableCache { source, .. } => source.status_code(),

            Error::ParseFileFormat { source, .. } | Error::InferSchema { source, .. } => {
                source.status_code()
            }

            Error::Table { source, .. }
            | Error::CopyTable { source, .. }
            | Error::Insert { source, .. } => source.status_code(),

            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::CreateTableInfo { source, .. }
            | Error::IntoVectors { source, .. } => source.status_code(),

            Error::RequestInserts { source, .. } => source.status_code(),
            Error::RequestDeletes { source, .. } => source.status_code(),

            Error::ColumnDataType { source, .. } | Error::InvalidColumnDef { source, .. } => {
                source.status_code()
            }

            Error::MissingTimeIndexColumn { source, .. } => source.status_code(),

            Error::BuildDfLogicalPlan { .. }
            | Error::BuildTableMeta { .. }
            | Error::MissingInsertBody { .. } => StatusCode::Internal,

            Error::EncodeJson { .. } => StatusCode::Unexpected,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,

            Error::JoinTask { .. } => StatusCode::Internal,

            Error::BuildParquetRecordBatchStream { .. }
            | Error::BuildFileStream { .. }
            | Error::WriteStreamToFile { .. }
            | Error::ReadDfRecordBatch { .. }
            | Error::Unexpected { .. } => StatusCode::Unexpected,

            Error::Catalog { source, .. } => source.status_code(),

            Error::BuildCreateExprOnInsertion { source, .. }
            | Error::FindNewColumnsOnInsertion { source, .. } => source.status_code(),

            Error::ExecuteStatement { source, .. }
            | Error::PlanStatement { source, .. }
            | Error::ParseQuery { source, .. }
            | Error::ExecLogicalPlan { source, .. }
            | Error::DescribeStatement { source, .. } => source.status_code(),

            Error::AlterExprToRequest { source, .. } => source.status_code(),

            Error::External { source, .. } => source.status_code(),
            Error::DeserializePartition { source, .. }
            | Error::FindTablePartitionRule { source, .. }
            | Error::SplitInsert { source, .. }
            | Error::SplitDelete { source, .. }
            | Error::FindRegionLeader { source, .. } => source.status_code(),

            Error::UnrecognizedTableOption { .. } => StatusCode::InvalidArguments,

            Error::ReadObject { .. } | Error::ReadParquet { .. } | Error::ReadOrc { .. } => {
                StatusCode::StorageUnavailable
            }

            Error::ListObjects { source, .. }
            | Error::ParseUrl { source, .. }
            | Error::BuildBackend { source, .. } => source.status_code(),

            Error::ExecuteDdl { source, .. } => source.status_code(),
            Error::InvalidCopyParameter { .. } => StatusCode::InvalidArguments,

            Error::ReadRecordBatch { source, .. } | Error::BuildColumnVectors { source, .. } => {
                source.status_code()
            }

            Error::ColumnDefaultValue { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
