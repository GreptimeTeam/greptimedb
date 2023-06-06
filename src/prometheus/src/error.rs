use std::any::Any;

use common_error::prelude::*;
use snafu::Location;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to write prometheus series, source: {}", source))]
    PromSeriesWrite {
        #[snafu(backtrace)]
        source: common_grpc::error::Error,
    },
    #[snafu(display("Failed to decompress prometheus remote request, source: {}", source))]
    DecompressPromRemoteRequest {
        location: Location,
        source: snap::Error,
    },

    #[snafu(display("Invalid prometheus remote request, msg: {}", msg))]
    InvalidPromRemoteRequest { msg: String, location: Location },

    #[snafu(display("Invalid prometheus remote read query result, msg: {}", msg))]
    InvalidPromRemoteReadQueryResult { msg: String, location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            InvalidPromRemoteReadQueryResult { .. } => StatusCode::Internal,

            DecompressPromRemoteRequest { .. } | InvalidPromRemoteRequest { .. } => {
                StatusCode::InvalidArguments
            }
            PromSeriesWrite { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
