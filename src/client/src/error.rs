use api::convert::DecodeError;
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

    #[snafu(display("Missing {}, expected {}, actual {}", name, expected, actual))]
    MissingResult {
        name: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Missing result header"))]
    MissingHeader,

    #[snafu(display("Tonic internal error, source: {}", source))]
    TonicStatus {
        source: tonic::Status,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to decode select result, source: {}", source))]
    DecodeSelect { source: DecodeError },

    #[snafu(display("Error occurred on the data node, code: {}, msg: {}", code, msg))]
    Datanode { code: u32, msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;
