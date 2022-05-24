use datatypes::arrow::error::ArrowError;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arrow error: {}", source))]
    Arrow {
        source: ArrowError,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
