use std::any::Any;
use std::net::SocketAddr;

use axum::http::StatusCode as HttpStatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_error::prelude::*;
use serde_json::json;

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

    #[snafu(display("Invalid query: {}", reason))]
    InvalidQuery {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("connection reset by peer"))]
    ConnResetByPeer { backtrace: Backtrace },

    #[snafu(display("Hyper error, source: {}", source))]
    Hyper { source: hyper::Error },

    #[cfg(feature = "opentsdb")]
    #[snafu(display("Invalid Opentsdb line, source: {}", source))]
    InvalidOpentsdbLine {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[cfg(feature = "opentsdb")]
    #[snafu(display("Invalid Opentsdb Json request, source: {}", source))]
    InvalidOpentsdbJsonRequest {
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[cfg(feature = "opentsdb")]
    #[snafu(display(
        "Failed to put Opentsdb data point: {:?}, source: {}",
        data_point,
        source
    ))]
    PutOpentsdbDataPoint {
        data_point: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },
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

            NotSupported { .. } | InvalidQuery { .. } | ConnResetByPeer { .. } => {
                StatusCode::InvalidArguments
            }

            Hyper { .. } => StatusCode::Unknown,

            #[cfg(feature = "opentsdb")]
            InvalidOpentsdbLine { .. } | InvalidOpentsdbJsonRequest { .. } => {
                StatusCode::InvalidArguments
            }
            #[cfg(feature = "opentsdb")]
            PutOpentsdbDataPoint { source, .. } => source.status_code(),
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

#[cfg(feature = "opentsdb")]
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::InvalidOpentsdbLine { .. }
            | Error::InvalidOpentsdbJsonRequest { .. }
            | Error::InvalidQuery { .. } => (HttpStatusCode::BAD_REQUEST, self.to_string()),
            _ => (HttpStatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
