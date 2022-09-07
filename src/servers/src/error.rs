use std::any::Any;
use std::net::SocketAddr;

use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Internal error: {}", err_msg))]
    Internal { err_msg: String },

    #[snafu(display("Internal IO error, source: {}", source))]
    InternalIo { source: std::io::Error },

    #[snafu(display("Tokio IO error: {}, source: {}", err_msg, source))]
    TokioIo {
        err_msg: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to convert vector, source: {}", source))]
    VectorConversion {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to collect recordbatch, source: {}", source))]
    CollectRecordbatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to start HTTP server, source: {}", source))]
    StartHttp { source: hyper::Error },

    #[snafu(display("Failed to start gRPC server, source: {}", source))]
    StartGrpc { source: tonic::transport::Error },

    #[snafu(display("Failed to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[snafu(display("Failed to execute query: {}, source: {}", query, source))]
    ExecuteQuery {
        query: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to insert script with name: {}, source: {}", name, source))]
    InsertScript {
        name: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to execute script by name: {}, source: {}", name, source))]
    ExecuteScript {
        name: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            Internal { .. }
            | InternalIo { .. }
            | TokioIo { .. }
            | VectorConversion { .. }
            | CollectRecordbatch { .. }
            | StartHttp { .. }
            | StartGrpc { .. }
            | TcpBind { .. } => StatusCode::Internal,

            InsertScript { source, .. }
            | ExecuteScript { source, .. }
            | ExecuteQuery { source, .. } => source.status_code(),

            NotSupported { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        tonic::Status::new(tonic::Code::Internal, err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::InternalIo { source: e }
    }
}
