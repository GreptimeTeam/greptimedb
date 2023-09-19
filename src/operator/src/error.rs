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
use datafusion::parquet;
use datatypes::arrow::error::ArrowError;
use datatypes::value::Value;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to invalidate table cache, source: {}", source))]
    InvalidateTableCache {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute ddl, source: {}", source))]
    ExecuteDdl {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("{source}"))]
    External {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to insert data, source: {}", source))]
    RequestInserts {
        #[snafu(backtrace)]
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to delete data, source: {}", source))]
    RequestDeletes {
        #[snafu(backtrace)]
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to parse SQL, source: {}", source))]
    ParseSql {
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display("Failed to convert value to sql value: {}", value))]
    ConvertSqlValue {
        value: Value,
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
    InvalidSql { err_msg: String, location: Location },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest { reason: String, location: Location },

    #[snafu(display("Invalid DeleteRequest, reason: {}", reason))]
    InvalidDeleteRequest { reason: String, location: Location },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

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

    #[snafu(display(
        "Failed to find table partition rule for table {}, source: {}",
        table_name,
        source
    ))]
    FindTablePartitionRule {
        table_name: String,
        #[snafu(backtrace)]
        source: partition::error::Error,
    },

    #[snafu(display("Failed to split insert request, source: {}", source))]
    SplitInsert {
        source: partition::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to split delete request, source: {}", source))]
    SplitDelete {
        source: partition::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to find leader for region, source: {}", source))]
    FindRegionLeader {
        source: partition::error::Error,
        location: Location,
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

    #[snafu(display("Cannot find column by name: {}", msg))]
    ColumnNotFound { msg: String, location: Location },

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

    #[snafu(display("Failed to convert AlterExpr to AlterRequest, source: {}", source))]
    AlterExprToRequest {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

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

    #[snafu(display("Failed to convert into vectors, source: {}", source))]
    IntoVectors {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Failed to deserialize partition in meta to partition def, source: {}",
        source
    ))]
    DeserializePartition {
        #[snafu(backtrace)]
        source: partition::error::Error,
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

    #[snafu(display("Missing time index column: {}", source))]
    MissingTimeIndexColumn {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to build regex, source: {}", source))]
    BuildRegex {
        location: Location,
        source: regex::Error,
    },

    #[snafu(display("Failed to copy table: {}, source: {}", table_name, source))]
    CopyTable {
        table_name: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display(
        "Failed to insert value into table: {}, source: {}",
        table_name,
        source
    ))]
    Insert {
        table_name: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to parse data source url, source: {}", source))]
    ParseUrl {
        #[snafu(backtrace)]
        source: common_datasource::error::Error,
    },

    #[snafu(display("Unsupported format: {:?}", format))]
    UnsupportedFormat { location: Location, format: Format },

    #[snafu(display("Failed to parse file format, source: {}", source))]
    ParseFileFormat {
        #[snafu(backtrace)]
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build data source backend, source: {}", source))]
    BuildBackend {
        #[snafu(backtrace)]
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to list objects, source: {}", source))]
    ListObjects {
        #[snafu(backtrace)]
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to infer schema from path: {}, source: {}", path, source))]
    InferSchema {
        path: String,
        #[snafu(backtrace)]
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build csv config: {}", source))]
    BuildCsvConfig {
        source: common_datasource::file_format::csv::CsvConfigBuilderError,
        location: Location,
    },

    #[snafu(display("Failed to write stream to path: {}, source: {}", path, source))]
    WriteStreamToFile {
        path: String,
        #[snafu(backtrace)]
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to read object in path: {}, source: {}", path, source))]
    ReadObject {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Failed to read record batch, source: {}", source))]
    ReadDfRecordBatch {
        source: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to read parquet file, source: {}", source))]
    ReadParquet {
        source: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Failed to read orc schema, source: {}", source))]
    ReadOrc {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build parquet record batch stream, source: {}", source))]
    BuildParquetRecordBatchStream {
        location: Location,
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to build file stream, source: {}", source))]
    BuildFileStream {
        location: Location,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to write parquet file, source: {}", source))]
    WriteParquet {
        #[snafu(backtrace)]
        source: storage::error::Error,
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

    #[snafu(display("Failed to project schema: {}", source))]
    ProjectSchema {
        source: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to encode object into json, source: {}", source))]
    EncodeJson {
        source: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to prepare immutable table: {}", source))]
    PrepareImmutableTable {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Invalid COPY parameter, key: {}, value: {}", key, value))]
    InvalidCopyParameter {
        key: String,
        value: String,
        location: Location,
    },

    #[snafu(display("Table metadata manager error: {}", source))]
    TableMetadataManager {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to read record batch, source: {}", source))]
    ReadRecordBatch {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build column vectors, source: {}", source))]
    BuildColumnVectors {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Missing insert body, source: {source}"))]
    MissingInsertBody {
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display(
        "Failed to build default value, column: {}, source: {}",
        column,
        source
    ))]
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

    #[snafu(display("Failed to prepare file table: {}", source))]
    PrepareFileTable {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to infer file table schema: {}", source))]
    InferFileTableSchema {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display(
        "The schema of the file table is incompatible with the table schema: {}",
        source
    ))]
    SchemaIncompatible {
        #[snafu(backtrace)]
        source: query::error::Error,
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
            | Error::SchemaIncompatible { .. } => StatusCode::InvalidArguments,

            Error::NotSupported { .. } => StatusCode::Unsupported,

            Error::TableMetadataManager { source, .. } => source.status_code(),

            Error::ConvertSqlValue { source, .. } | Error::ParseSql { source } => {
                source.status_code()
            }

            Error::InvalidateTableCache { source, .. } => source.status_code(),

            Error::ParseFileFormat { source, .. } | Error::InferSchema { source, .. } => {
                source.status_code()
            }

            Error::Table { source }
            | Error::CopyTable { source, .. }
            | Error::Insert { source, .. } => source.status_code(),

            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::CreateTableInfo { source }
            | Error::IntoVectors { source } => source.status_code(),

            Error::RequestInserts { source } => source.status_code(),
            Error::RequestDeletes { source } => source.status_code(),

            Error::ColumnDataType { source } | Error::InvalidColumnDef { source, .. } => {
                source.status_code()
            }

            Error::MissingTimeIndexColumn { source, .. } => source.status_code(),

            Error::BuildDfLogicalPlan { .. }
            | Error::BuildTableMeta { .. }
            | Error::MissingInsertBody { .. } => StatusCode::Internal,

            Error::EncodeJson { .. } => StatusCode::Unexpected,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,

            Error::JoinTask { .. }
            | Error::BuildParquetRecordBatchStream { .. }
            | Error::ReadDfRecordBatch { .. }
            | Error::BuildFileStream { .. }
            | Error::WriteStreamToFile { .. }
            | Error::Unexpected { .. } => StatusCode::Unexpected,

            Error::Catalog { source, .. } => source.status_code(),

            Error::BuildCreateExprOnInsertion { source }
            | Error::FindNewColumnsOnInsertion { source } => source.status_code(),

            Error::ExecuteStatement { source, .. }
            | Error::PlanStatement { source }
            | Error::ParseQuery { source }
            | Error::ExecLogicalPlan { source }
            | Error::DescribeStatement { source } => source.status_code(),

            Error::AlterExprToRequest { source, .. } => source.status_code(),

            Error::External { source } => source.status_code(),
            Error::DeserializePartition { source, .. }
            | Error::FindTablePartitionRule { source, .. }
            | Error::SplitInsert { source, .. }
            | Error::SplitDelete { source, .. }
            | Error::FindRegionLeader { source, .. } => source.status_code(),

            Error::UnrecognizedTableOption { .. } => StatusCode::InvalidArguments,

            Error::ReadObject { .. } | Error::ReadParquet { .. } | Error::ReadOrc { .. } => {
                StatusCode::StorageUnavailable
            }

            Error::ListObjects { source }
            | Error::ParseUrl { source }
            | Error::BuildBackend { source } => source.status_code(),

            Error::WriteParquet { source, .. } => source.status_code(),
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
