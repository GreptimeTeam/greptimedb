use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::prelude::StatusCode;
use datatypes::prelude::ConcreteDataType;
use snafu::prelude::*;
use snafu::{Backtrace, ErrorCompat};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unknown proto column datatype: {}", datatype))]
    UnknownColumnDataType { datatype: i32, backtrace: Backtrace },

    #[snafu(display("Failed to create column datatype from {:?}", from))]
    IntoColumnDataType {
        from: ConcreteDataType,
        backtrace: Backtrace,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UnknownColumnDataType { .. } => StatusCode::InvalidArguments,
            Error::IntoColumnDataType { .. } => StatusCode::Unexpected,
        }
    }
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
