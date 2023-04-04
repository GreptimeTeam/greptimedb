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
use common_error::prelude::*;
use query::parser::PromQuery;
use serde_json::json;
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
        #[snafu(backtrace)]
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
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("{source}"))]
    ExecuteGrpcQuery {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to execute sql statement, source: {}", source))]
    ExecuteStatement {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to check database validity, source: {}", source))]
    CheckDatabaseValidity {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to describe statement, source: {}", source))]
    DescribeStatement { source: BoxedError },

    #[snafu(display("Failed to execute alter: {}, source: {}", query, source))]
    ExecuteAlter {
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
    InvalidQuery { reason: String, location: Location },

    #[snafu(display("Failed to parse InfluxDB line protocol, source: {}", source))]
    InfluxdbLineProtocol {
        #[snafu(backtrace)]
        source: influxdb_line_protocol::Error,
    },

    #[snafu(display("Failed to write InfluxDB line protocol, source: {}", source))]
    InfluxdbLinesWrite {
        #[snafu(backtrace)]
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

    #[snafu(display(
        "Failed to put OpenTSDB data point: {:?}, source: {}",
        data_point,
        source
    ))]
    PutOpentsdbDataPoint {
        data_point: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to decode prometheus remote request, source: {}", source))]
    DecodePromRemoteRequest {
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

    #[snafu(display("Failed to start frontend service, source: {}", source))]
    StartFrontend {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to build context, msg: {}", err_msg))]
    BuildingContext { err_msg: String, location: Location },

    #[snafu(display("Tls is required for {}, plain connection is rejected", server))]
    TlsRequired { server: String },

    #[snafu(display("Failed to get user info, source: {}", source))]
    Auth {
        #[snafu(backtrace)]
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

    #[snafu(display("Failed to convert Flight Message, source: {}", source))]
    ConvertFlightMessage {
        #[snafu(backtrace)]
        source: common_grpc::error::Error,
    },

    #[snafu(display("Cannot find requested database: {}-{}", catalog, schema))]
    DatabaseNotFound { catalog: String, schema: String },

    #[cfg(feature = "mem-prof")]
    #[snafu(display("Failed to dump profile data, source: {}", source))]
    DumpProfileData {
        #[snafu(backtrace)]
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
        #[snafu(backtrace)]
        source: query::error::Error,
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
            | BuildingContext { .. }
            | BuildHttpResponse { .. } => StatusCode::Internal,

            InsertScript { source, .. }
            | ExecuteScript { source, .. }
            | ExecuteQuery { source, .. }
            | ExecuteGrpcQuery { source, .. }
            | ExecuteStatement { source, .. }
            | CheckDatabaseValidity { source, .. }
            | ExecuteAlter { source, .. }
            | PutOpentsdbDataPoint { source, .. } => source.status_code(),

            NotSupported { .. }
            | InvalidQuery { .. }
            | InfluxdbLineProtocol { .. }
            | ConnResetByPeer { .. }
            | InvalidOpentsdbLine { .. }
            | InvalidOpentsdbJsonRequest { .. }
            | DecodePromRemoteRequest { .. }
            | DecompressPromRemoteRequest { .. }
            | InvalidPromRemoteRequest { .. }
            | InvalidFlightTicket { .. }
            | InvalidPrepareStatement { .. }
            | TimePrecision { .. } => StatusCode::InvalidArguments,

            InfluxdbLinesWrite { source, .. } | ConvertFlightMessage { source } => {
                source.status_code()
            }

            Hyper { .. } => StatusCode::Unknown,
            TlsRequired { .. } => StatusCode::Unknown,
            StartFrontend { source, .. } => source.status_code(),
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

            ParsePromQL { source, .. } => source.status_code(),
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
        let mut headers = HeaderMap::<HeaderValue>::with_capacity(2);

        // If either of the status_code or error msg cannot convert to valid HTTP header value
        // (which is a very rare case), just ignore. Client will use Tonic status code and message.
        if let Ok(code) = HeaderValue::from_bytes(err.status_code().to_string().as_bytes()) {
            headers.insert(INNER_ERROR_CODE, code);
        }
        let root_error = err.iter_chain().last().unwrap();
        if let Ok(err_msg) = HeaderValue::from_bytes(root_error.to_string().as_bytes()) {
            headers.insert(INNER_ERROR_MSG, err_msg);
        }

        let metadata = MetadataMap::from_headers(headers);
        tonic::Status::with_metadata(Code::Internal, err.to_string(), metadata)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::InternalIo { source: e }
    }
}

impl From<auth::Error> for Error {
    fn from(e: auth::Error) -> Self {
        Error::Auth { source: e }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::InfluxdbLineProtocol { .. }
            | Error::InfluxdbLinesWrite { .. }
            | Error::InvalidOpentsdbLine { .. }
            | Error::InvalidOpentsdbJsonRequest { .. }
            | Error::DecodePromRemoteRequest { .. }
            | Error::DecompressPromRemoteRequest { .. }
            | Error::InvalidPromRemoteRequest { .. }
            | Error::InvalidQuery { .. }
            | Error::TimePrecision { .. } => (HttpStatusCode::BAD_REQUEST, self.to_string()),
            _ => (HttpStatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
