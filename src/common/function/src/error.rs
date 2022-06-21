use std::any::Any;

use common_error::prelude::*;
pub use common_query::error::{Error, Result};
use datatypes::error::Error as DataTypeError;

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

#[cfg(test)]
mod tests {
    use snafu::GenerateImplicitData;

    use super::*;

    fn raise_datatype_error() -> std::result::Result<(), DataTypeError> {
        Err(DataTypeError::Conversion {
            from: "test".to_string(),
            backtrace: Backtrace::generate(),
        })
    }

    #[test]
    fn test_get_scalar_vector_error() {
        let err: Error = raise_datatype_error()
            .context(GetScalarVectorSnafu)
            .err()
            .unwrap()
            .into();
        assert!(err.backtrace_opt().is_some());
    }
}
