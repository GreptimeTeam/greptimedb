use datatypes::arrow::error::ArrowError;
use snafu::{Backtrace, Snafu};

// TODO(boyan): use ErrorExt instead.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arrow error: {}", source))]
    Arrow {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Storage error: {}, source: {}", msg, source))]
    Storage { source: BoxedError, msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;
