use std::any::Any;

use common_error::prelude::*;
use tokio::task::JoinError;

common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display(
        "Runtime Failed to build runtime, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    BuildRuntime {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Runtime Failed to join task, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    JoinTask {
        source: JoinError,
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
