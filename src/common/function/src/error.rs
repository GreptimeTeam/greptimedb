use std::any::Any;

use common_error::prelude::*;
use common_query::error::Error as QueryError;
use datatypes::error::Error as DataTypeError;
use snafu::GenerateImplicitData;

common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Fail to get scalar vector, {}", source))]
    GetScalarVector {
        source: DataTypeError,
        backtrace: Backtrace,
    },
}

impl ErrorExt for InnerError {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}

impl From<Error> for QueryError {
    fn from(err: Error) -> Self {
        QueryError::External {
            msg: err.to_string(),
            backtrace: Backtrace::generate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raise_datatype_error() -> std::result::Result<(), DataTypeError> {
        Err(DataTypeError::Conversion {
            from: "test".to_string(),
            backtrace: Backtrace::generate(),
        })
    }

    #[test]
    fn test_get_scalar_vector_error() {
        let err = raise_datatype_error()
            .context(GetScalarVectorSnafu)
            .err()
            .unwrap();
        assert!(err.backtrace_opt().is_some());

        let query_error = QueryError::from(Error::from(err));
        assert!(matches!(query_error, QueryError::External { .. }));
    }
}
