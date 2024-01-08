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
use common_macro::stack_trace_debug;
use common_telemetry::{debug, error};
use datatypes::prelude::ConcreteDataType;
use query::parser::PromQuery;
use serde_json::json;
use snafu::{Location, Snafu};
use tonic::Code;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Internal error: {}", err_msg))]
    Internal { err_msg: String },

    #[snafu(display("Internal IO error"))]
    InternalIo {
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Tokio IO error: {}", err_msg))]
    TokioIo {
        err_msg: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to collect recordbatch"))]
    CollectRecordbatch {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to start HTTP server"))]
    StartHttp {
        #[snafu(source)]
        error: hyper::Error,
    },

    #[snafu(display("Failed to start gRPC server"))]
    StartGrpc {
        #[snafu(source)]
        error: tonic::transport::Error,
    },

    #[snafu(display("{} server is already started", server))]
    AlreadyStarted { server: String, location: Location },

    #[snafu(display("Failed to bind address {}", addr))]
    TcpBind {
        addr: SocketAddr,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to convert to TcpIncoming"))]
    TcpIncoming {
        #[snafu(source)]
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to execute query, query: {}", query))]
    ExecuteQuery {
        query: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to execute plan"))]
    ExecutePlan {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Execute gRPC query error"))]
    ExecuteGrpcQuery {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Execute gRPC request error"))]
    ExecuteGrpcRequest {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to check database validity"))]
    CheckDatabaseValidity {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to describe statement"))]
    DescribeStatement { source: BoxedError },

    #[snafu(display("Failed to insert script with name: {}", name))]
    InsertScript {
        name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to execute script by name: {}", name))]
    ExecuteScript {
        name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Invalid request parameter: {}", reason))]
    InvalidParameter { reason: String, location: Location },

    #[snafu(display("Invalid query: {}", reason))]
    InvalidQuery { reason: String, location: Location },

    #[snafu(display("Failed to parse InfluxDB line protocol"))]
    InfluxdbLineProtocol {
        location: Location,
        #[snafu(source)]
        error: influxdb_line_protocol::Error,
    },

    #[snafu(display("Failed to write InfluxDB line protocol"))]
    InfluxdbLinesWrite {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to write prometheus series"))]
    PromSeriesWrite {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to write OTLP metrics"))]
    OtlpMetricsWrite {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to convert time precision, name: {}", name))]
    TimePrecision { name: String, location: Location },

    #[snafu(display("Connection reset by peer"))]
    ConnResetByPeer { location: Location },

    #[snafu(display("Hyper error"))]
    Hyper {
        #[snafu(source)]
        error: hyper::Error,
    },

    #[snafu(display("Invalid OpenTSDB line"))]
    InvalidOpentsdbLine {
        #[snafu(source)]
        error: FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Invalid OpenTSDB Json request"))]
    InvalidOpentsdbJsonRequest {
        #[snafu(source)]
        error: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to decode prometheus remote request"))]
    DecodePromRemoteRequest {
        location: Location,
        #[snafu(source)]
        error: prost::DecodeError,
    },

    #[snafu(display("Failed to decode OTLP request"))]
    DecodeOtlpRequest {
        location: Location,
        #[snafu(source)]
        error: prost::DecodeError,
    },

    #[snafu(display("Failed to decompress prometheus remote request"))]
    DecompressPromRemoteRequest {
        location: Location,
        #[snafu(source)]
        error: snap::Error,
    },

    #[snafu(display("Failed to send prometheus remote request"))]
    SendPromRemoteRequest {
        location: Location,
        #[snafu(source)]
        error: reqwest::Error,
    },

    #[snafu(display("Invalid export metrics config, msg: {}", msg))]
    InvalidExportMetricsConfig { msg: String, location: Location },

    #[snafu(display("Failed to compress prometheus remote request"))]
    CompressPromRemoteRequest {
        location: Location,
        #[snafu(source)]
        error: snap::Error,
    },

    #[snafu(display("Invalid prometheus remote request, msg: {}", msg))]
    InvalidPromRemoteRequest { msg: String, location: Location },

    #[snafu(display("Invalid prometheus remote read query result, msg: {}", msg))]
    InvalidPromRemoteReadQueryResult { msg: String, location: Location },

    #[snafu(display("Invalid Flight ticket"))]
    InvalidFlightTicket {
        #[snafu(source)]
        error: api::DecodeError,
        location: Location,
    },

    #[snafu(display("Tls is required for {}, plain connection is rejected", server))]
    TlsRequired { server: String },

    #[snafu(display("Failed to get user info"))]
    Auth {
        location: Location,
        source: auth::error::Error,
    },

    #[snafu(display("Not found http or grpc authorization header"))]
    NotFoundAuthHeader {},

    #[snafu(display("Not found influx http authorization info"))]
    NotFoundInfluxAuth {},

    #[snafu(display("Invalid visibility ASCII chars"))]
    InvisibleASCII {
        #[snafu(source)]
        error: hyper::header::ToStrError,
        location: Location,
    },

    #[snafu(display("Unsupported http auth scheme, name: {}", name))]
    UnsupportedAuthScheme { name: String },

    #[snafu(display("Invalid http authorization header"))]
    InvalidAuthorizationHeader { location: Location },

    #[snafu(display("Invalid base64 value"))]
    InvalidBase64Value {
        #[snafu(source)]
        error: DecodeError,
        location: Location,
    },

    #[snafu(display("Invalid utf-8 value"))]
    InvalidUtf8Value {
        #[snafu(source)]
        error: FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Error accessing catalog"))]
    CatalogError { source: catalog::error::Error },

    #[snafu(display("Cannot find requested database: {}-{}", catalog, schema))]
    DatabaseNotFound { catalog: String, schema: String },

    #[cfg(feature = "mem-prof")]
    #[snafu(display("Failed to dump profile data"))]
    DumpProfileData {
        location: Location,
        source: common_mem_prof::error::Error,
    },

    #[snafu(display("Invalid prepare statement: {}", err_msg))]
    InvalidPrepareStatement { err_msg: String },

    #[snafu(display("Invalid flush argument: {}", err_msg))]
    InvalidFlushArgument { err_msg: String },

    #[snafu(display("Failed to build gRPC reflection service"))]
    GrpcReflectionService {
        #[snafu(source)]
        error: tonic_reflection::server::Error,
        location: Location,
    },

    #[snafu(display("Failed to build HTTP response"))]
    BuildHttpResponse {
        #[snafu(source)]
        error: http::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse PromQL: {query:?}"))]
    ParsePromQL {
        query: PromQuery,
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to get param types"))]
    GetPreparedStmtParams {
        source: query::error::Error,
        location: Location,
    },

    #[snafu(display("{}", reason))]
    UnexpectedResult { reason: String, location: Location },

    // this error is used for custom error mapping
    // please do not delete it
    #[snafu(display("Other error"))]
    Other {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: tokio::task::JoinError,
        location: Location,
    },

    #[cfg(feature = "pprof")]
    #[snafu(display("Failed to dump pprof data"))]
    DumpPprof {
        source: crate::http::pprof::nix::Error,
    },

    #[cfg(not(windows))]
    #[snafu(display("Failed to update jemalloc metrics"))]
    UpdateJemallocMetrics {
        #[snafu(source)]
        error: tikv_jemalloc_ctl::Error,
        location: Location,
    },

    #[snafu(display("DataFrame operation error"))]
    DataFrame {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to replace params with values in prepared statement"))]
    ReplacePreparedStmtParams {
        source: query::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert scalar value"))]
    ConvertScalarValue {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Expected type: {:?}, actual: {:?}", expected, actual))]
    PreparedStmtTypeMismatch {
        expected: ConcreteDataType,
        actual: opensrv_mysql::ColumnType,
        location: Location,
    },

    #[snafu(display(
        "Column: {}, {} incompatible, expected: {}, actual: {}",
        column_name,
        datatype,
        expected,
        actual
    ))]
    IncompatibleSchema {
        column_name: String,
        datatype: String,
        expected: i32,
        actual: i32,
        location: Location,
    },

    #[snafu(display("Failed to convert to json"))]
    ToJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to decode url"))]
    UrlDecode {
        #[snafu(source)]
        error: FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to convert Mysql value, error: {}", err_msg))]
    MysqlValueConversion { err_msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            Internal { .. }
            | InternalIo { .. }
            | TokioIo { .. }
            | StartHttp { .. }
            | StartGrpc { .. }
            | AlreadyStarted { .. }
            | InvalidPromRemoteReadQueryResult { .. }
            | TcpBind { .. }
            | SendPromRemoteRequest { .. }
            | TcpIncoming { .. }
            | CatalogError { .. }
            | GrpcReflectionService { .. }
            | BuildHttpResponse { .. } => StatusCode::Internal,

            #[cfg(not(windows))]
            UpdateJemallocMetrics { .. } => StatusCode::Internal,

            CollectRecordbatch { .. } => StatusCode::EngineExecuteQuery,

            InsertScript { source, .. }
            | ExecuteScript { source, .. }
            | ExecuteQuery { source, .. }
            | ExecutePlan { source, .. }
            | ExecuteGrpcQuery { source, .. }
            | ExecuteGrpcRequest { source, .. }
            | CheckDatabaseValidity { source, .. } => source.status_code(),

            NotSupported { .. }
            | InvalidParameter { .. }
            | InvalidQuery { .. }
            | InfluxdbLineProtocol { .. }
            | ConnResetByPeer { .. }
            | InvalidOpentsdbLine { .. }
            | InvalidOpentsdbJsonRequest { .. }
            | DecodePromRemoteRequest { .. }
            | DecodeOtlpRequest { .. }
            | CompressPromRemoteRequest { .. }
            | DecompressPromRemoteRequest { .. }
            | InvalidPromRemoteRequest { .. }
            | InvalidExportMetricsConfig { .. }
            | InvalidFlightTicket { .. }
            | InvalidPrepareStatement { .. }
            | DataFrame { .. }
            | PreparedStmtTypeMismatch { .. }
            | TimePrecision { .. }
            | UrlDecode { .. }
            | IncompatibleSchema { .. }
            | MysqlValueConversion { .. } => StatusCode::InvalidArguments,

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

            JoinTask { error, .. } => {
                if error.is_cancelled() {
                    StatusCode::Cancelled
                } else if error.is_panic() {
                    StatusCode::Unexpected
                } else {
                    StatusCode::Unknown
                }
            }

            #[cfg(feature = "pprof")]
            DumpPprof { source, .. } => source.status_code(),

            ConvertScalarValue { source, .. } => source.status_code(),

            ToJson { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Returns the tonic [Code] of a [StatusCode].
pub fn status_to_tonic_code(status_code: StatusCode) -> Code {
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
        StatusCode::TableAlreadyExists
        | StatusCode::TableColumnExists
        | StatusCode::RegionAlreadyExists => Code::AlreadyExists,
        StatusCode::TableNotFound
        | StatusCode::RegionNotFound
        | StatusCode::TableColumnNotFound
        | StatusCode::DatabaseNotFound
        | StatusCode::UserNotFound => Code::NotFound,
        StatusCode::StorageUnavailable | StatusCode::RegionNotReady => Code::Unavailable,
        StatusCode::RuntimeResourcesExhausted
        | StatusCode::RateLimited
        | StatusCode::RegionBusy => Code::ResourceExhausted,
        StatusCode::UnsupportedPasswordType
        | StatusCode::UserPasswordMismatch
        | StatusCode::AuthHeaderNotFound
        | StatusCode::InvalidAuthHeader => Code::Unauthenticated,
        StatusCode::AccessDenied | StatusCode::PermissionDenied | StatusCode::RegionReadonly => {
            Code::PermissionDenied
        }
    }
}

#[macro_export]
macro_rules! define_into_tonic_status {
    ($Error: ty) => {
        impl From<$Error> for tonic::Status {
            fn from(err: $Error) -> Self {
                use common_error::{GREPTIME_DB_HEADER_ERROR_CODE, GREPTIME_DB_HEADER_ERROR_MSG};
                use tonic::codegen::http::{HeaderMap, HeaderValue};
                use tonic::metadata::MetadataMap;

                let mut headers = HeaderMap::<HeaderValue>::with_capacity(2);

                // If either of the status_code or error msg cannot convert to valid HTTP header value
                // (which is a very rare case), just ignore. Client will use Tonic status code and message.
                let status_code = err.status_code();
                headers.insert(
                    GREPTIME_DB_HEADER_ERROR_CODE,
                    HeaderValue::from(status_code as u32),
                );
                let root_error = err.output_msg();

                if let Ok(err_msg) = HeaderValue::from_bytes(root_error.as_bytes()) {
                    let _ = headers.insert(GREPTIME_DB_HEADER_ERROR_MSG, err_msg);
                }

                let metadata = MetadataMap::from_headers(headers);
                tonic::Status::with_metadata(
                    $crate::error::status_to_tonic_code(status_code),
                    root_error,
                    metadata,
                )
            }
        }
    };
}

define_into_tonic_status!(Error);

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::InternalIo { error: e }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let error_msg = self.output_msg();
        let status = match self {
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
            | Error::TimePrecision { .. } => HttpStatusCode::BAD_REQUEST,
            _ => {
                if self.status_code().should_log_error() {
                    error!(self; "Failed to handle HTTP request: ");
                } else {
                    debug!("Failed to handle HTTP request: {self}");
                }

                HttpStatusCode::INTERNAL_SERVER_ERROR
            }
        };
        let body = Json(json!({
            "error": error_msg,
        }));
        (status, body).into_response()
    }
}
