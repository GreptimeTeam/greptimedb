use std::any::Any;

use common_error::prelude::{ErrorCompat, ErrorExt, Snafu, StatusCode};
use snafu::Backtrace;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to serialize data, source: {}", source))]
    Serialize { source: serde_json::Error },

    #[snafu(display("Failed to serialize data, source:"))]
    Deserialize,
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        // Inner serialization and deserialization error should not be exposed to users.
        StatusCode::Internal
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
