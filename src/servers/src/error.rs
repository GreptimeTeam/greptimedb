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

use axum::response::{IntoResponse, Response};
use axum::{http, Json};
use base64::DecodeError;
use common_error::define_into_tonic_status;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_telemetry::{error, warn};
use datatypes::prelude::ConcreteDataType;
use headers::ContentType;
use query::parser::PromQuery;
use serde_json::json;
use snafu::{Location, Snafu};

use crate::http::error_result::status_code_to_http_status;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Arrow error"))]
    Arrow {
        #[snafu(source)]
        error: arrow_schema::ArrowError,
    },

    #[snafu(display("Internal error: {}", err_msg))]
    Internal { err_msg: String },

    #[snafu(display("Unsupported data type: {}, reason: {}", data_type, reason))]
    UnsupportedDataType {
        data_type: ConcreteDataType,
        reason: String,
    },

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
        #[snafu(implicit)]
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
    AlreadyStarted {
        server: String,
        #[snafu(implicit)]
        location: Location,
    },

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

    #[snafu(display("Failed to execute query"))]
    ExecuteQuery {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to execute plan"))]
    ExecutePlan {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Execute gRPC query error"))]
    ExecuteGrpcQuery {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Execute gRPC request error"))]
    ExecuteGrpcRequest {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to check database validity"))]
    CheckDatabaseValidity {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to describe statement"))]
    DescribeStatement { source: BoxedError },

    #[snafu(display("Failed to insert script with name: {}", name))]
    InsertScript {
        name: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Pipeline management api error"))]
    Pipeline {
        source: pipeline::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute script by name: {}", name))]
    ExecuteScript {
        name: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Invalid request parameter: {}", reason))]
    InvalidParameter {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid query: {}", reason))]
    InvalidQuery {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse InfluxDB line protocol"))]
    InfluxdbLineProtocol {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: influxdb_line_protocol::Error,
    },

    #[snafu(display("Failed to write row"))]
    RowWriter {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to convert time precision, name: {}", name))]
    TimePrecision {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Hyper error"))]
    Hyper {
        #[snafu(source)]
        error: hyper::Error,
    },

    #[snafu(display("Invalid OpenTSDB Json request"))]
    InvalidOpentsdbJsonRequest {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode prometheus remote request"))]
    DecodePromRemoteRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: prost::DecodeError,
    },

    #[snafu(display("Failed to decode OTLP request"))]
    DecodeOtlpRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: prost::DecodeError,
    },

    #[snafu(display("Failed to decompress snappy prometheus remote request"))]
    DecompressSnappyPromRemoteRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: snap::Error,
    },

    #[snafu(display("Failed to decompress zstd prometheus remote request"))]
    DecompressZstdPromRemoteRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to send prometheus remote request"))]
    SendPromRemoteRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: reqwest::Error,
    },

    #[snafu(display("Invalid export metrics config, msg: {}", msg))]
    InvalidExportMetricsConfig {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to compress prometheus remote request"))]
    CompressPromRemoteRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: snap::Error,
    },

    #[snafu(display("Invalid prometheus remote request, msg: {}", msg))]
    InvalidPromRemoteRequest {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid prometheus remote read query result, msg: {}", msg))]
    InvalidPromRemoteReadQueryResult {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid Flight ticket"))]
    InvalidFlightTicket {
        #[snafu(source)]
        error: api::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Tls is required for {}, plain connection is rejected", server))]
    TlsRequired { server: String },

    #[snafu(display("Failed to get user info"))]
    Auth {
        #[snafu(implicit)]
        location: Location,
        source: auth::error::Error,
    },

    #[snafu(display("Not found http or grpc authorization header"))]
    NotFoundAuthHeader {},

    #[snafu(display("Not found influx http authorization info"))]
    NotFoundInfluxAuth {},

    #[snafu(display("Unsupported http auth scheme, name: {}", name))]
    UnsupportedAuthScheme { name: String },

    #[snafu(display("Invalid visibility ASCII chars"))]
    InvalidAuthHeaderInvisibleASCII {
        #[snafu(source)]
        error: hyper::header::ToStrError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid utf-8 value"))]
    InvalidAuthHeaderInvalidUtf8Value {
        #[snafu(source)]
        error: FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid http authorization header"))]
    InvalidAuthHeader {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid base64 value"))]
    InvalidBase64Value {
        #[snafu(source)]
        error: DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid utf-8 value"))]
    InvalidUtf8Value {
        #[snafu(source)]
        error: FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error accessing catalog"))]
    Catalog {
        source: catalog::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find requested table: {}.{}.{}", catalog, schema, table))]
    TableNotFound {
        catalog: String,
        schema: String,
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "mem-prof")]
    #[snafu(display("Failed to dump profile data"))]
    DumpProfileData {
        #[snafu(implicit)]
        location: Location,
        source: common_mem_prof::error::Error,
    },

    #[snafu(display("Invalid prepare statement: {}", err_msg))]
    InvalidPrepareStatement {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build HTTP response"))]
    BuildHttpResponse {
        #[snafu(source)]
        error: http::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse PromQL: {query:?}"))]
    ParsePromQL {
        query: Box<PromQuery>,
        #[snafu(implicit)]
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("{}", reason))]
    UnexpectedResult {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    // this error is used for custom error mapping
    // please do not delete it
    #[snafu(display("Other error"))]
    Other {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: tokio::task::JoinError,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFrame operation error"))]
    DataFrame {
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert scalar value"))]
    ConvertScalarValue {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected type: {:?}, actual: {:?}", expected, actual))]
    PreparedStmtTypeMismatch {
        expected: ConcreteDataType,
        actual: opensrv_mysql::ColumnType,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert to json"))]
    ToJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse payload as json"))]
    ParseJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported content type: {:?}", content_type))]
    UnsupportedContentType {
        content_type: ContentType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode url"))]
    UrlDecode {
        #[snafu(source)]
        error: FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert Mysql value, error: {}", err_msg))]
    MysqlValueConversion {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing query context"))]
    MissingQueryContext {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to initialize a watcher for file {}", path))]
    FileWatch {
        path: String,
        #[snafu(source)]
        error: notify::Error,
    },

    #[snafu(display("Timestamp overflow: {}", error))]
    TimestampOverflow {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("OpenTelemetry log error"))]
    OpenTelemetryLog {
        source: pipeline::etl_error::Error,
        #[snafu(implicit)]
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
            | StartHttp { .. }
            | StartGrpc { .. }
            | TcpBind { .. }
            | SendPromRemoteRequest { .. }
            | TcpIncoming { .. }
            | BuildHttpResponse { .. }
            | Arrow { .. }
            | FileWatch { .. } => StatusCode::Internal,

            AlreadyStarted { .. } | InvalidPromRemoteReadQueryResult { .. } => {
                StatusCode::IllegalState
            }

            UnsupportedDataType { .. } => StatusCode::Unsupported,

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

            Pipeline { source, .. } => source.status_code(),

            NotSupported { .. }
            | InvalidParameter { .. }
            | InvalidQuery { .. }
            | InfluxdbLineProtocol { .. }
            | InvalidOpentsdbJsonRequest { .. }
            | DecodePromRemoteRequest { .. }
            | DecodeOtlpRequest { .. }
            | CompressPromRemoteRequest { .. }
            | DecompressSnappyPromRemoteRequest { .. }
            | DecompressZstdPromRemoteRequest { .. }
            | InvalidPromRemoteRequest { .. }
            | InvalidExportMetricsConfig { .. }
            | InvalidFlightTicket { .. }
            | InvalidPrepareStatement { .. }
            | DataFrame { .. }
            | PreparedStmtTypeMismatch { .. }
            | TimePrecision { .. }
            | UrlDecode { .. }
            | IncompatibleSchema { .. }
            | MissingQueryContext { .. }
            | MysqlValueConversion { .. }
            | ParseJson { .. }
            | UnsupportedContentType { .. }
            | TimestampOverflow { .. }
            | OpenTelemetryLog { .. } => StatusCode::InvalidArguments,

            Catalog { source, .. } => source.status_code(),
            RowWriter { source, .. } => source.status_code(),

            Hyper { .. } => StatusCode::Unknown,
            TlsRequired { .. } => StatusCode::Unknown,
            Auth { source, .. } => source.status_code(),
            DescribeStatement { source } => source.status_code(),

            NotFoundAuthHeader { .. } | NotFoundInfluxAuth { .. } => StatusCode::AuthHeaderNotFound,
            InvalidAuthHeaderInvisibleASCII { .. }
            | UnsupportedAuthScheme { .. }
            | InvalidAuthHeader { .. }
            | InvalidBase64Value { .. }
            | InvalidAuthHeaderInvalidUtf8Value { .. } => StatusCode::InvalidAuthHeader,

            TableNotFound { .. } => StatusCode::TableNotFound,

            #[cfg(feature = "mem-prof")]
            DumpProfileData { source, .. } => source.status_code(),

            InvalidUtf8Value { .. } => StatusCode::InvalidArguments,

            ParsePromQL { source, .. } => source.status_code(),
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

define_into_tonic_status!(Error);

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::InternalIo { error: e }
    }
}

fn log_error_if_necessary(error: &Error) {
    if error.status_code().should_log_error() {
        error!(error; "Failed to handle HTTP request ");
    } else {
        warn!(error; "Failed to handle HTTP request ");
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let error_msg = self.output_msg();
        let status = status_code_to_http_status(&self.status_code());

        log_error_if_necessary(&self);

        let body = Json(json!({
            "error": error_msg,
        }));
        (status, body).into_response()
    }
}
