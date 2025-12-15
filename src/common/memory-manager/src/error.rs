use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display(
        "Memory limit exceeded: requested {requested_bytes} bytes, limit {limit_bytes} bytes"
    ))]
    MemoryLimitExceeded {
        requested_bytes: u64,
        limit_bytes: u64,
    },

    #[snafu(display("Memory semaphore unexpectedly closed"))]
    MemorySemaphoreClosed,
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            MemoryLimitExceeded { .. } => StatusCode::RuntimeResourcesExhausted,
            MemorySemaphoreClosed => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
