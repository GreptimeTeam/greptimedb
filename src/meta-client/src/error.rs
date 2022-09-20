use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Connect failed to {}, source: {}", url, source))]
    ConnectFailed {
        url: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;
