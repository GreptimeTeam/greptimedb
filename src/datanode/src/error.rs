use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::*;
use datatypes::prelude::ConcreteDataType;
use table::error::Error as TableError;
use table_engine::error::Error as TableEngineError;

/// Business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to execute sql, source: {}", source))]
    ExecuteSql {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Fail to create catalog list, source: {}", source))]
    NewCatalog {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Fail to create table: {}, {}", table_name, source))]
    CreateTable {
        table_name: String,
        source: TableEngineError,
    },

    #[snafu(display("Fail to get table: {}, {}", table_name, source))]
    GetTable {
        table_name: String,
        source: BoxedError,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },

    #[snafu(display(
        "Columns and values number mismatch, columns: {}, values: {}",
        columns,
        values
    ))]
    ColumnValuesNumberMismatch { columns: usize, values: usize },

    #[snafu(display("Fail to parse value: {}, {}", msg, backtrace))]
    ParseSqlValue { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Column {} expect type: {:?}, actual: {:?}",
        column_name,
        expect,
        actual
    ))]
    ColumnTypeMismatch {
        column_name: String,
        expect: ConcreteDataType,
        actual: ConcreteDataType,
    },

    #[snafu(display("Fail to insert value to table: {}, {}", table_name, source))]
    Insert {
        table_name: String,
        source: TableError,
    },

    // The error source of http error is clear even without backtrace now so
    // a backtrace is not carried in this varaint.
    #[snafu(display("Fail to start HTTP server, source: {}", source))]
    StartHttp { source: hyper::Error },

    #[snafu(display("Fail to parse address {}, source: {}", addr, source))]
    ParseAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteSql { source } | Error::NewCatalog { source } => source.status_code(),
            // TODO(yingwen): Further categorize http error.
            Error::StartHttp { .. } | Error::ParseAddr { .. } => StatusCode::Internal,
            Error::CreateTable { source, .. } => source.status_code(),
            Error::GetTable { source, .. } => source.status_code(),
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,
            Error::ColumnValuesNumberMismatch { .. }
            | Error::ParseSqlValue { .. }
            | Error::ColumnTypeMismatch { .. } => StatusCode::InvalidArguments,
            Error::Insert { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;

    use super::*;

    fn throw_query_error() -> std::result::Result<(), query::error::Error> {
        Err(query::error::Error::new(MockError::with_backtrace(
            StatusCode::Internal,
        )))
    }

    fn assert_internal_error(err: &Error) {
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Internal, err.status_code());
    }

    #[test]
    fn test_error() {
        let err = throw_query_error().context(ExecuteSqlSnafu).err().unwrap();
        assert_internal_error(&err);
        let err = throw_query_error().context(NewCatalogSnafu).err().unwrap();
        assert_internal_error(&err);
    }
}
