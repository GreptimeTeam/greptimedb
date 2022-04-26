use arrow::error::ArrowError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arrow error: {}", source))]
    Arrow { source: ArrowError },
}
pub type Result<T> = std::result::Result<T, Error>;
