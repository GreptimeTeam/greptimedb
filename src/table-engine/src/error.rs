use std::any::Any;

use common_error::prelude::*;

// TODO(boyan): use ErrorExt instead.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to create table, source: {}", source))]
    CreateTable {
        source: BoxedError,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            //TODO: should return the source's status code after use ErrorExt in BoxedError.
            Error::CreateTable { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
