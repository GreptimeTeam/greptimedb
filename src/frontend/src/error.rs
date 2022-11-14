use std::any::Any;

use common_error::prelude::*;
use common_query::logical_plan::Expr;
use datafusion_common::ScalarValue;
use store_api::storage::RegionId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to connect Datanode at {}, source: {}", addr, source))]
    ConnectDatanode {
        addr: String,
        #[snafu(backtrace)]
        source: client::Error,
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

    #[snafu(display("Failed to execute OpenTSDB put, reason: {}", reason))]
    ExecOpentsdbPut {
        reason: String,
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

    #[snafu(display("Failed to bump table id when creating table, source: {}", source))]
    BumpTableId {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to create table, source: {}", source))]
    CreateTable {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to create database: {}, source: {}", name, source))]
    CreateDatabase {
        name: String,
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to alter table, source: {}", source))]
    AlterTable {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to insert values to table, source: {}", source))]
    Insert {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to select from table, source: {}", source))]
    Select {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to create table on insertion, source: {}", source))]
    CreateTableOnInsertion {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to alter table on insertion, source: {}", source))]
    AlterTableOnInsertion {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to build CreateExpr on insertion: {}", source))]
    BuildCreateExprOnInsertion {
        #[snafu(backtrace)]
        source: common_insert::error::Error,
    },

    #[snafu(display("Failed to find new columns on insertion: {}", source))]
    FindNewColumnsOnInsertion {
        #[snafu(backtrace)]
        source: common_insert::error::Error,
    },

    #[snafu(display("Failed to deserialize insert batching: {}", source))]
    DeserializeInsertBatch {
        #[snafu(backtrace)]
        source: common_insert::error::Error,
    },

    #[snafu(display("Failed to deserialize insert batching: {}", source))]
    InsertBatchToRequest {
        #[snafu(backtrace)]
        source: common_insert::error::Error,
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

    #[snafu(display("Failed to get catalog manager"))]
    CatalogManager { backtrace: Backtrace },

    #[snafu(display("Failed to get full table name, source: {}", source))]
    FullTableName {
        #[snafu(backtrace)]
        source: sql::error::Error,
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

    #[snafu(display("Invalid admin result, source: {}", source))]
    InvalidAdminResult {
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Cannot find primary key column by name: {}", msg))]
    PrimaryKeyNotFound { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to execute sql: {}, source: {}", sql, source))]
    ExecuteSql {
        sql: String,
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to create a RecordBatch, source: {}", source))]
    NewRecordBatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to do vector computation, source: {}", source))]
    VectorComputation {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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
            | Error::CatalogManager { .. }
            | Error::RegionKeysSize { .. } => StatusCode::InvalidArguments,

            Error::RuntimeResource { source, .. } => source.status_code(),

            Error::StartServer { source, .. } => source.status_code(),

            Error::ParseSql { source } => source.status_code(),

            Error::FullTableName { source, .. } => source.status_code(),

            Error::Table { source } => source.status_code(),

            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::ConvertScalarValue { source, .. }
            | Error::VectorComputation { source } => source.status_code(),

            Error::ConnectDatanode { source, .. }
            | Error::RequestDatanode { source }
            | Error::InvalidAdminResult { source } => source.status_code(),

            Error::ColumnDataType { .. }
            | Error::FindDatanode { .. }
            | Error::GetCache { .. }
            | Error::FindTableRoutes { .. }
            | Error::SerializeJson { .. }
            | Error::DeserializeJson { .. }
            | Error::FindRegionRoutes { .. }
            | Error::FindLeaderPeer { .. }
            | Error::FindRegionPartition { .. }
            | Error::IllegalTableRoutesData { .. }
            | Error::UnsupportedExpr { .. } => StatusCode::Internal,

            Error::IllegalFrontendState { .. } | Error::IncompleteGrpcResult { .. } => {
                StatusCode::Unexpected
            }
            Error::ExecOpentsdbPut { .. } => StatusCode::Internal,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            Error::JoinTask { .. } => StatusCode::Unexpected,
            Error::Catalog { source, .. } => source.status_code(),
            Error::CatalogEntrySerde { source, .. } => source.status_code(),

            Error::StartMetaClient { source } | Error::RequestMeta { source } => {
                source.status_code()
            }
            Error::BumpTableId { source, .. } => source.status_code(),
            Error::SchemaNotFound { .. } => StatusCode::InvalidArguments,
            Error::CatalogNotFound { .. } => StatusCode::InvalidArguments,
            Error::CreateTable { source, .. } => source.status_code(),
            Error::AlterTable { source, .. } => source.status_code(),
            Error::Insert { source, .. } => source.status_code(),
            Error::BuildCreateExprOnInsertion { source, .. } => source.status_code(),
            Error::CreateTableOnInsertion { source, .. } => source.status_code(),
            Error::AlterTableOnInsertion { source, .. } => source.status_code(),
            Error::Select { source, .. } => source.status_code(),
            Error::FindNewColumnsOnInsertion { source, .. } => source.status_code(),
            Error::DeserializeInsertBatch { source, .. } => source.status_code(),
            Error::PrimaryKeyNotFound { .. } => StatusCode::InvalidArguments,
            Error::ExecuteSql { source, .. } => source.status_code(),
            Error::InsertBatchToRequest { source, .. } => source.status_code(),
            Error::NewRecordBatch { source } => source.status_code(),
            Error::CreateDatabase { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
