use common_error::prelude::*;

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
