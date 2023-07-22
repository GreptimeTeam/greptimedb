// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::any::Any;
use std::net::SocketAddr;
use std::string::FromUtf8Error;

use axum::http::StatusCode as HttpStatusCode;
use axum::response::{IntoResponse, Response};
use axum::{http, Json};
use base64::DecodeError;
use catalog;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_error::{INNER_ERROR_CODE, INNER_ERROR_MSG};
use common_telemetry::logging;
use datatypes::prelude::ConcreteDataType;
use query::parser::PromQuery;
use serde_json::json;
use snafu::{ErrorCompat, Location, Snafu};
use tonic::codegen::http::{HeaderMap, HeaderValue};
use tonic::metadata::MetadataMap;
use tonic::Code;

use crate::auth;

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

    #[snafu(display("Failed to collect recordbatch, source: {}", source))]
    CollectRecordbatch {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to start HTTP server, source: {}", source))]
    StartHttp { source: hyper::Error },

    #[snafu(display("Failed to start gRPC server, source: {}", source))]
    StartGrpc { source: tonic::transport::Error },

    #[snafu(display("{} server is already started", server))]
    AlreadyStarted { server: String, location: Location },

    #[snafu(display("Failed to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[snafu(display("Failed to execute query: {}, source: {}", query, source))]
    ExecuteQuery {
        query: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to execute plan, source: {}", source))]
    ExecutePlan {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("{source}"))]
    ExecuteGrpcQuery {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to check database validity, source: {}", source))]
    CheckDatabaseValidity {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to describe statement, source: {}", source))]
    DescribeStatement { source: BoxedError },

    #[snafu(display("Failed to insert script with name: {}, source: {}", name, source))]
    InsertScript {
        name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to execute script by name: {}, source: {}", name, source))]
    ExecuteScript {
        name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Invalid query: {}", reason))]
    InvalidQuery { reason: String, location: Location },

    #[snafu(display("Failed to parse InfluxDB line protocol, source: {}", source))]
    InfluxdbLineProtocol {
        location: Location,
        source: influxdb_line_protocol::Error,
    },

    #[snafu(display("Failed to write InfluxDB line protocol, source: {}", source))]
    InfluxdbLinesWrite {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to write prometheus series, source: {}", source))]
    PromSeriesWrite {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to write OTLP metrics, source: {}", source))]
    OtlpMetricsWrite {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to convert time precision, name: {}", name))]
    TimePrecision { name: String, location: Location },

    #[snafu(display("Connection reset by peer"))]
    ConnResetByPeer { location: Location },

    #[snafu(display("Hyper error, source: {}", source))]
    Hyper { source: hyper::Error },

    #[snafu(display("Invalid OpenTSDB line, source: {}", source))]
    InvalidOpentsdbLine {
        source: FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Invalid OpenTSDB Json request, source: {}", source))]
    InvalidOpentsdbJsonRequest {
        source: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to decode prometheus remote request, source: {}", source))]
    DecodePromRemoteRequest {
        location: Location,
        source: prost::DecodeError,
    },

    #[snafu(display("Failed to decode OTLP request, source: {}", source))]
    DecodeOtlpRequest {
        location: Location,
        source: prost::DecodeError,
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

    #[snafu(display("Invalid Flight ticket, source: {}", source))]
    InvalidFlightTicket {
        source: api::DecodeError,
        location: Location,
    },

    #[snafu(display("Tls is required for {}, plain connection is rejected", server))]
    TlsRequired { server: String },

    #[snafu(display("Failed to get user info, source: {}", source))]
    Auth {
        location: Location,
        source: auth::Error,
    },

    #[snafu(display("Not found http or grpc authorization header"))]
    NotFoundAuthHeader {},

    #[snafu(display("Not found influx http authorization info"))]
    NotFoundInfluxAuth {},

    #[snafu(display("Invalid visibility ASCII chars, source: {}", source))]
    InvisibleASCII {
        source: hyper::header::ToStrError,
        location: Location,
    },

    #[snafu(display("Unsupported http auth scheme, name: {}", name))]
    UnsupportedAuthScheme { name: String },

    #[snafu(display("Invalid http authorization header"))]
    InvalidAuthorizationHeader { location: Location },

    #[snafu(display("Invalid base64 value, source: {:?}", source))]
    InvalidBase64Value {
        source: DecodeError,
        location: Location,
    },

    #[snafu(display("Invalid utf-8 value, source: {:?}", source))]
    InvalidUtf8Value {
        source: FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Error accessing catalog: {}", source))]
    CatalogError { source: catalog::error::Error },

    #[snafu(display("Cannot find requested database: {}-{}", catalog, schema))]
    DatabaseNotFound { catalog: String, schema: String },

    #[cfg(feature = "mem-prof")]
    #[snafu(display("Failed to dump profile data, source: {}", source))]
    DumpProfileData {
        location: Location,
        source: common_mem_prof::error::Error,
    },

    #[snafu(display("Invalid prepare statement: {}", err_msg))]
    InvalidPrepareStatement { err_msg: String },

    #[snafu(display("Invalid flush argument: {}", err_msg))]
    InvalidFlushArgument { err_msg: String },

    #[snafu(display("Failed to build gRPC reflection service, source: {}", source))]
    GrpcReflectionService {
        source: tonic_reflection::server::Error,
        location: Location,
    },

    #[snafu(display("Failed to build HTTP response, source: {source}"))]
    BuildHttpResponse {
        source: http::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse PromQL: {query:?}, source: {source}"))]
    ParsePromQL {
        query: PromQuery,
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to get param types, source: {source}, location: {location}"))]
    GetPreparedStmtParams {
        source: query::error::Error,
        location: Location,
    },

    #[snafu(display("{}", reason))]
    UnexpectedResult { reason: String, location: Location },

    // this error is used for custom error mapping
    // please do not delete it
    #[snafu(display("Other error, source: {}", source))]
    Other {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failed to join task, source: {}", source))]
    JoinTask {
        source: tokio::task::JoinError,
        location: Location,
    },

    #[cfg(feature = "pprof")]
    #[snafu(display("Failed to dump pprof data, source: {}", source))]
    DumpPprof { source: common_pprof::Error },

    #[snafu(display("{source}"))]
    Metrics { source: BoxedError },

    #[snafu(display("DataFrame operation error, source: {source}, location: {location}"))]
    DataFrame {
        source: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display(
        "Failed to replace params with values in prepared statement, source: {source}, location: {location}"
    ))]
    ReplacePreparedStmtParams {
        source: query::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert scalar value, source: {source}, location: {location}"))]
    ConvertScalarValue {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display(
        "Expected type: {:?}, actual: {:?}, location: {location}",
        expected,
        actual
    ))]
    PreparedStmtTypeMismatch {
        expected: ConcreteDataType,
        actual: opensrv_mysql::ColumnType,
        location: Location,
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
            | CollectRecordbatch { .. }
            | StartHttp { .. }
            | StartGrpc { .. }
            | AlreadyStarted { .. }
            | InvalidPromRemoteReadQueryResult { .. }
            | TcpBind { .. }
            | CatalogError { .. }
            | GrpcReflectionService { .. }
            | BuildHttpResponse { .. } => StatusCode::Internal,

            InsertScript { source, .. }
            | ExecuteScript { source, .. }
            | ExecuteQuery { source, .. }
            | ExecutePlan { source, .. }
            | ExecuteGrpcQuery { source, .. }
            | CheckDatabaseValidity { source, .. } => source.status_code(),

            NotSupported { .. }
            | InvalidQuery { .. }
            | InfluxdbLineProtocol { .. }
            | ConnResetByPeer { .. }
            | InvalidOpentsdbLine { .. }
            | InvalidOpentsdbJsonRequest { .. }
            | DecodePromRemoteRequest { .. }
            | DecodeOtlpRequest { .. }
            | DecompressPromRemoteRequest { .. }
            | InvalidPromRemoteRequest { .. }
            | InvalidFlightTicket { .. }
            | InvalidPrepareStatement { .. }
            | DataFrame { .. }
            | PreparedStmtTypeMismatch { .. }
            | TimePrecision { .. } => StatusCode::InvalidArguments,

            InfluxdbLinesWrite { source, .. }
            | PromSeriesWrite { source, .. }
            | OtlpMetricsWrite { source, .. } => source.status_code(),

            Hyper { .. } => StatusCode::Unknown,
            TlsRequired { .. } => StatusCode::Unknown,
            Auth { source, .. } => source.status_code(),
            DescribeStatement { source } => source.status_code(),

            NotFoundAuthHeader { .. } | NotFoundInfluxAuth { .. } => StatusCode::AuthHeaderNotFound,
            InvisibleASCII { .. }
            | UnsupportedAuthScheme { .. }
            | InvalidAuthorizationHeader { .. }
            | InvalidBase64Value { .. }
            | InvalidUtf8Value { .. } => StatusCode::InvalidAuthHeader,

            DatabaseNotFound { .. } => StatusCode::DatabaseNotFound,
            #[cfg(feature = "mem-prof")]
            DumpProfileData { source, .. } => source.status_code(),
            InvalidFlushArgument { .. } => StatusCode::InvalidArguments,

            ReplacePreparedStmtParams { source, .. }
            | GetPreparedStmtParams { source, .. }
            | ParsePromQL { source, .. } => source.status_code(),
            Other { source, .. } => source.status_code(),

            UnexpectedResult { .. } => StatusCode::Unexpected,

            JoinTask { source, .. } => {
                if source.is_cancelled() {
                    StatusCode::Cancelled
                } else if source.is_panic() {
                    StatusCode::Unexpected
                } else {
                    StatusCode::Unknown
                }
            }

            #[cfg(feature = "pprof")]
            DumpPprof { source, .. } => source.status_code(),

            Metrics { source } => source.status_code(),

            ConvertScalarValue { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Returns the tonic [Code] of a [StatusCode].
fn status_to_tonic_code(status_code: StatusCode) -> Code {
    match status_code {
        StatusCode::Success => Code::Ok,
        StatusCode::Unknown => Code::Unknown,
        StatusCode::Unsupported => Code::Unimplemented,
        StatusCode::Unexpected
        | StatusCode::Internal
        | StatusCode::PlanQuery
        | StatusCode::EngineExecuteQuery => Code::Internal,
        StatusCode::InvalidArguments | StatusCode::InvalidSyntax => Code::InvalidArgument,
        StatusCode::Cancelled => Code::Cancelled,
        StatusCode::TableAlreadyExists | StatusCode::TableColumnExists => Code::AlreadyExists,
        StatusCode::TableNotFound
        | StatusCode::TableColumnNotFound
        | StatusCode::DatabaseNotFound
        | StatusCode::UserNotFound => Code::NotFound,
        StatusCode::StorageUnavailable => Code::Unavailable,
        StatusCode::RuntimeResourcesExhausted | StatusCode::RateLimited => Code::ResourceExhausted,
        StatusCode::UnsupportedPasswordType
        | StatusCode::UserPasswordMismatch
        | StatusCode::AuthHeaderNotFound
        | StatusCode::InvalidAuthHeader => Code::Unauthenticated,
        StatusCode::AccessDenied => Code::PermissionDenied,
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let mut headers = HeaderMap::<HeaderValue>::with_capacity(2);

        // If either of the status_code or error msg cannot convert to valid HTTP header value
        // (which is a very rare case), just ignore. Client will use Tonic status code and message.
        let status_code = err.status_code();
        if let Ok(code) = HeaderValue::from_bytes(status_code.to_string().as_bytes()) {
            let _ = headers.insert(INNER_ERROR_CODE, code);
        }
        let root_error = err.iter_chain().last().unwrap();
        if let Ok(err_msg) = HeaderValue::from_bytes(root_error.to_string().as_bytes()) {
            let _ = headers.insert(INNER_ERROR_MSG, err_msg);
        }

        let metadata = MetadataMap::from_headers(headers);
        tonic::Status::with_metadata(status_to_tonic_code(status_code), err.to_string(), metadata)
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
            Error::InfluxdbLineProtocol { .. }
            | Error::InfluxdbLinesWrite { .. }
            | Error::PromSeriesWrite { .. }
            | Error::InvalidOpentsdbLine { .. }
            | Error::InvalidOpentsdbJsonRequest { .. }
            | Error::DecodePromRemoteRequest { .. }
            | Error::DecodeOtlpRequest { .. }
            | Error::DecompressPromRemoteRequest { .. }
            | Error::InvalidPromRemoteRequest { .. }
            | Error::InvalidQuery { .. }
            | Error::TimePrecision { .. } => (HttpStatusCode::BAD_REQUEST, self.to_string()),
            _ => {
                logging::error!(self; "Failed to handle HTTP request");

                (HttpStatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
