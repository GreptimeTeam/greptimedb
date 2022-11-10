use std::any::Any;

use api::DecodeError;
use common_error::ext::ErrorExt;
use common_error::prelude::{Snafu, StatusCode};
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },

    #[snafu(display("Failed to convert bytes to insert batch, source: {}", source))]
    DecodeInsert { source: DecodeError },

    #[snafu(display("Illegal insert data"))]
    IllegalInsertData,

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Failed to create schema when creating table, source: {}", source))]
    CreateSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Duplicated timestamp column in gRPC requests, exists {}, duplicated: {}",
        exists,
        duplicated
    ))]
    DuplicatedTimestampColumn {
        exists: String,
        duplicated: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing timestamp column in request"))]
    MissingTimestampColumn { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,
            Error::DecodeInsert { .. } | Error::IllegalInsertData { .. } => {
                StatusCode::InvalidArguments
            }
            Error::ColumnDataType { .. } => StatusCode::Internal,
            Error::CreateSchema { .. }
            | Error::DuplicatedTimestampColumn { .. }
            | Error::MissingTimestampColumn { .. } => StatusCode::InvalidArguments,
        }
    }
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
