use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::prelude::{Snafu, StatusCode};
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid catalog info: {}", key))]
    InvalidCatalog { key: String, backtrace: Backtrace },

    #[snafu(display("Failed to deserialize catalog entry value: {}", raw))]
    DeserializeCatalogEntryValue {
        raw: String,
        backtrace: Backtrace,
        source: serde_json::error::Error,
    },

    #[snafu(display("Failed to serialize catalog entry value"))]
    SerializeCatalogEntryValue {
        backtrace: Backtrace,
        source: serde_json::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidCatalog { .. }
            | Error::DeserializeCatalogEntryValue { .. }
            | Error::SerializeCatalogEntryValue { .. } => StatusCode::Unexpected,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
