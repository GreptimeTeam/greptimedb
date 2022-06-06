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
