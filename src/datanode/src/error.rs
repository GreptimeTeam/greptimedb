use common_error::prelude::*;
use snafu::Snafu;

/// Business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to execute sql, source: {}", source))]
    ExecuteSql { source: query::error::Error },

    #[snafu(display("Fail to create catalog list, source: {}", source))]
    NewCatalog { source: query::error::Error },

    // The error source of http error is clear even without backtrace now so
    // a backtrace is not carried in this varaint.
    #[snafu(display("Fail to start HTTP server, source: {}", source))]
    StartHttp { source: hyper::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteSql { source } | Error::NewCatalog { source } => source.status_code(),
            // TODO(yingwen): Further categorize http error.
            Error::StartHttp { .. } => StatusCode::Internal,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }
}
