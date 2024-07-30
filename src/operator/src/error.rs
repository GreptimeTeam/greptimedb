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
use datafusion::parquet;
use datatypes::arrow::error::ArrowError;
use snafu::{Location, Snafu};
use table::metadata::TableType;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Table already exists: `{}`", table))]
    TableAlreadyExists {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("View already exists: `{name}`"))]
    ViewAlreadyExists {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to invalidate table cache"))]
    InvalidateTableCache {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute ddl"))]
    ExecuteDdl {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("external error"))]
    External {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to insert data"))]
    RequestInserts {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to delete data"))]
    RequestDeletes {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to send request to region"))]
    RequestRegion {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Unsupported region request"))]
    UnsupportedRegionRequest {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse SQL"))]
    ParseSql {
        #[snafu(implicit)]
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Failed to convert identifier: {}", ident))]
    ConvertIdentifier {
        ident: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to extract table names"))]
    ExtractTableNames {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to get schema from logical plan"))]
    GetSchema {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Column datatype error"))]
    ColumnDataType {
        #[snafu(implicit)]
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Invalid column proto definition, column: {}", column))]
    InvalidColumnDef {
        column: String,
        #[snafu(implicit)]
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Invalid statement to create view"))]
    InvalidViewStmt {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expect {expected} columns for view {view_name}, but found {actual}"))]
    ViewColumnsMismatch {
        view_name: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Invalid view \"{view_name}\": {msg}"))]
    InvalidView {
        msg: String,
        view_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert column default constraint, column: {}", column_name))]
    ConvertColumnDefaultConstraint {
        column_name: String,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert expr to struct"))]
    InvalidExpr {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Invalid partition"))]
    InvalidPartition {
        #[snafu(implicit)]
        location: Location,
        source: partition::error::Error,
    },

    #[snafu(display("Invalid SQL, error: {}", err_msg))]
    InvalidSql {
        err_msg: String,
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

    #[snafu(display("Flow not found: {}", flow_name))]
    FlowNotFound { flow_name: String },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: common_runtime::JoinError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("General catalog error"))]
    Catalog {
        #[snafu(implicit)]
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to find view info for: {}", view_name))]
    FindViewInfo {
        view_name: String,
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("View info not found: {}", view_name))]
    ViewInfoNotFound {
        view_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("View not found: {}", view_name))]
    ViewNotFound {
        view_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table partition rule for table {}", table_name))]
    FindTablePartitionRule {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
        source: partition::error::Error,
    },

    #[snafu(display("Failed to split insert request"))]
    SplitInsert {
        source: partition::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to split delete request"))]
    SplitDelete {
        source: partition::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find leader for region"))]
    FindRegionLeader {
        source: partition::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create table info"))]
    CreateTableInfo {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to build CreateExpr on insertion"))]
    BuildCreateExprOnInsertion {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to find schema, schema info: {}", schema_info))]
    SchemaNotFound {
        schema_info: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema `{name}` is in use"))]
    SchemaInUse {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema `{name}` is read-only"))]
    SchemaReadOnly {
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

    #[snafu(display("Failed to execute statement"))]
    ExecuteStatement {
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

    #[snafu(display("Failed to parse query"))]
    ParseQuery {
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

    #[snafu(display("Failed to build DataFusion logical plan"))]
    BuildDfLogicalPlan {
        #[snafu(source)]
        error: datafusion_common::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert AlterExpr to AlterRequest"))]
    AlterExprToRequest {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to build table meta for table: {}", table_name))]
    BuildTableMeta {
        table_name: String,
        #[snafu(source)]
        error: table::metadata::TableMetaBuilderError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Failed to find new columns on insertion"))]
    FindNewColumnsOnInsertion {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to convert into vectors"))]
    IntoVectors {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to deserialize partition in meta to partition def"))]
    DeserializePartition {
        #[snafu(implicit)]
        location: Location,
        source: partition::error::Error,
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

    #[snafu(display("Unrecognized table option"))]
    UnrecognizedTableOption {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Missing time index column"))]
    MissingTimeIndexColumn {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to build regex"))]
    BuildRegex {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: regex::Error,
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

    #[snafu(display("Failed to parse data source url"))]
    ParseUrl {
        #[snafu(implicit)]
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Unsupported format: {:?}", format))]
    UnsupportedFormat {
        #[snafu(implicit)]
        location: Location,
        format: Format,
    },

    #[snafu(display("Failed to parse file format"))]
    ParseFileFormat {
        #[snafu(implicit)]
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build data source backend"))]
    BuildBackend {
        #[snafu(implicit)]
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to list objects"))]
    ListObjects {
        #[snafu(implicit)]
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to infer schema from path: {}", path))]
    InferSchema {
        path: String,
        #[snafu(implicit)]
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build csv config"))]
    BuildCsvConfig {
        #[snafu(source)]
        error: common_datasource::file_format::csv::CsvConfigBuilderError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to write stream to path: {}", path))]
    WriteStreamToFile {
        path: String,
        #[snafu(implicit)]
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to read object in path: {}", path))]
    ReadObject {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Failed to read record batch"))]
    ReadDfRecordBatch {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read parquet file metadata"))]
    ReadParquetMetadata {
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read orc schema"))]
    ReadOrc {
        source: common_datasource::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build parquet record batch stream"))]
    BuildParquetRecordBatchStream {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to build file stream"))]
    BuildFileStream {
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to project schema"))]
    ProjectSchema {
        #[snafu(source)]
        error: ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode object into json"))]
    EncodeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid COPY parameter, key: {}, value: {}", key, value))]
    InvalidCopyParameter {
        key: String,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid COPY DATABASE location, must end with '/': {}", value))]
    InvalidCopyDatabasePath {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table metadata manager error"))]
    TableMetadataManager {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing insert body"))]
    MissingInsertBody {
        source: sql::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse sql value"))]
    ParseSqlValue {
        source: sql::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build default value, column: {}", column))]
    ColumnDefaultValue {
        column: String,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
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

    #[snafu(display(
        "Invalid partition columns when creating table '{}', reason: {}",
        table,
        reason
    ))]
    InvalidPartitionColumns {
        table: String,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to prepare file table"))]
    PrepareFileTable {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to infer file table schema"))]
    InferFileTableSchema {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("The schema of the file table is incompatible with the table schema"))]
    SchemaIncompatible {
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Invalid table name: {}", table_name))]
    InvalidTableName {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid view name: {name}"))]
    InvalidViewName {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty {} expr", name))]
    EmptyDdlExpr {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create logical tables: {}", reason))]
    CreateLogicalTables {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid partition rule: {}", reason))]
    InvalidPartitionRule {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid configuration value."))]
    InvalidConfigValue {
        source: session::session_config::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid timestamp range, start: `{}`, end: `{}`", start, end))]
    InvalidTimestampRange {
        start: String,
        end: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert between logical plan and substrait plan"))]
    SubstraitCodec {
        #[snafu(implicit)]
        location: Location,
        source: substrait::error::Error,
    },

    #[snafu(display(
        "Show create table only for base table. {} is {}",
        table_name,
        table_type
    ))]
    ShowCreateTableBaseOnly {
        table_name: String,
        table_type: TableType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Create physical expr error"))]
    PhysicalExpr {
        #[snafu(source)]
        error: common_recordbatch::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSql { .. }
            | Error::InvalidConfigValue { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::InvalidDeleteRequest { .. }
            | Error::IllegalPrimaryKeysDef { .. }
            | Error::SchemaNotFound { .. }
            | Error::SchemaExists { .. }
            | Error::SchemaInUse { .. }
            | Error::ColumnNotFound { .. }
            | Error::BuildRegex { .. }
            | Error::InvalidSchema { .. }
            | Error::BuildCsvConfig { .. }
            | Error::ProjectSchema { .. }
            | Error::UnsupportedFormat { .. }
            | Error::ColumnNoneDefaultValue { .. }
            | Error::InvalidPartitionColumns { .. }
            | Error::PrepareFileTable { .. }
            | Error::InferFileTableSchema { .. }
            | Error::SchemaIncompatible { .. }
            | Error::UnsupportedRegionRequest { .. }
            | Error::InvalidTableName { .. }
            | Error::InvalidViewName { .. }
            | Error::InvalidView { .. }
            | Error::InvalidExpr { .. }
            | Error::ViewColumnsMismatch { .. }
            | Error::InvalidViewStmt { .. }
            | Error::ConvertIdentifier { .. }
            | Error::InvalidPartition { .. }
            | Error::PhysicalExpr { .. } => StatusCode::InvalidArguments,

            Error::TableAlreadyExists { .. } | Error::ViewAlreadyExists { .. } => {
                StatusCode::TableAlreadyExists
            }

            Error::NotSupported { .. }
            | Error::ShowCreateTableBaseOnly { .. }
            | Error::SchemaReadOnly { .. } => StatusCode::Unsupported,

            Error::TableMetadataManager { source, .. } => source.status_code(),

            Error::ParseSql { source, .. } => source.status_code(),

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

            Error::RequestInserts { source, .. } | Error::FindViewInfo { source, .. } => {
                source.status_code()
            }
            Error::RequestRegion { source, .. } => source.status_code(),
            Error::RequestDeletes { source, .. } => source.status_code(),
            Error::SubstraitCodec { source, .. } => source.status_code(),

            Error::ColumnDataType { source, .. } | Error::InvalidColumnDef { source, .. } => {
                source.status_code()
            }

            Error::MissingTimeIndexColumn { source, .. } => source.status_code(),

            Error::BuildDfLogicalPlan { .. }
            | Error::BuildTableMeta { .. }
            | Error::MissingInsertBody { .. } => StatusCode::Internal,

            Error::EncodeJson { .. } => StatusCode::Unexpected,

            Error::ViewNotFound { .. }
            | Error::ViewInfoNotFound { .. }
            | Error::TableNotFound { .. } => StatusCode::TableNotFound,

            Error::FlowNotFound { .. } => StatusCode::FlowNotFound,

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
            | Error::GetSchema { source, .. }
            | Error::ExtractTableNames { source, .. }
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

            Error::ReadObject { .. }
            | Error::ReadParquetMetadata { .. }
            | Error::ReadOrc { .. } => StatusCode::StorageUnavailable,

            Error::ListObjects { source, .. }
            | Error::ParseUrl { source, .. }
            | Error::BuildBackend { source, .. } => source.status_code(),

            Error::ExecuteDdl { source, .. } => source.status_code(),
            Error::InvalidCopyParameter { .. } | Error::InvalidCopyDatabasePath { .. } => {
                StatusCode::InvalidArguments
            }

            Error::ColumnDefaultValue { source, .. } => source.status_code(),

            Error::EmptyDdlExpr { .. }
            | Error::InvalidPartitionRule { .. }
            | Error::ParseSqlValue { .. }
            | Error::InvalidTimestampRange { .. } => StatusCode::InvalidArguments,

            Error::CreateLogicalTables { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
