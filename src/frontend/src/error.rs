use std::any::Any;

use common_error::prelude::*;
use common_query::logical_plan::Expr;
use datafusion_common::ScalarValue;
use store_api::storage::RegionId;

use crate::mock::DatanodeId;

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

    #[snafu(display("Failed to get Datanode instance: {:?}", datanode))]
    DatanodeInstance {
        datanode: DatanodeId,
        backtrace: Backtrace,
    },

    #[snafu(display("Invaild InsertRequest, reason: {}", reason))]
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

    #[snafu(display("Failed access catalog: {}", source))]
    Catalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to parse catalog entry: {}", source))]
    ParseCatalogEntry {
        #[snafu(backtrace)]
        source: common_catalog::error::Error,
    },

    #[snafu(display("Cannot find datanode by id: {}", node_id))]
    DatanodeNotAvailable { node_id: u64, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ConnectDatanode { .. }
            | Error::ParseAddr { .. }
            | Error::InvalidSql { .. }
            | Error::FindRegion { .. }
            | Error::FindRegions { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::FindPartitionColumn { .. }
            | Error::ColumnValuesNumberMismatch { .. }
            | Error::RegionKeysSize { .. } => StatusCode::InvalidArguments,

            Error::RuntimeResource { source, .. } => source.status_code(),

            Error::StartServer { source, .. } => source.status_code(),

            Error::ParseSql { source } => source.status_code(),

            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::ConvertScalarValue { source, .. } => source.status_code(),

            Error::RequestDatanode { source } => source.status_code(),

            Error::ColumnDataType { .. }
            | Error::FindDatanode { .. }
            | Error::DatanodeInstance { .. } => StatusCode::Internal,

            Error::IllegalFrontendState { .. } | Error::IncompleteGrpcResult { .. } => {
                StatusCode::Unexpected
            }
            Error::ExecOpentsdbPut { .. } => StatusCode::Internal,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            Error::JoinTask { .. } => StatusCode::Unexpected,
            Error::Catalog { source, .. } => source.status_code(),
            Error::ParseCatalogEntry { source, .. } => source.status_code(),
            Error::DatanodeNotAvailable { .. } => StatusCode::StorageUnavailable,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
