use std::any::Any;
use std::net::SocketAddr;

use axum::{
    response::{IntoResponse, Response},
    Json,
};
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

    #[snafu(display("Invalid query: {}", reason))]
    InvalidQuery {
        reason: String,
        backtrace: Backtrace,
    },

    #[cfg(feature = "influxdb")]
    #[snafu(display("Failed to parse influxdb line protocol, source: {}", source))]
    InfluxdbLineProtocol {
        #[snafu(backtrace)]
        source: crate::influxdb::line_protocol::Error,
    },

    #[cfg(feature = "influxdb")]
    #[snafu(display("Write type mismatch, column name: {}", column_name))]
    TypeMismatch { column_name: String },
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

            NotSupported { .. } | InvalidQuery { .. } => StatusCode::InvalidArguments,

            #[cfg(feature = "influxdb")]
            InfluxdbLineProtocol { .. } | TypeMismatch { .. } => StatusCode::InvalidArguments,
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

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            #[cfg(feature = "influxdb")]
            Error::InfluxdbLineProtocol { .. } | Error::TypeMismatch { .. } => {
                (axum::http::StatusCode::BAD_REQUEST, self.to_string())
            }
            _ => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                self.to_string(),
            ),
        };
        let body = Json(serde_json::json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
