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

use std::string::FromUtf8Error;

use common_error::prelude::*;
use snafu::Location;
use tokio::sync::mpsc::error::SendError;
use tonic::codegen::http;
use tonic::{Code, Status};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to send shutdown signal"))]
    SendShutdownSignal { source: SendError<()> },

    #[snafu(display("Failed to shutdown {} server, source: {}", server, source))]
    ShutdownServer {
        #[snafu(backtrace)]
        source: servers::error::Error,
        server: String,
    },

    #[snafu(display("Error stream request next is None"))]
    StreamNone { location: Location },

    #[snafu(display("Empty key is not allowed"))]
    EmptyKey { location: Location },

    #[snafu(display("Failed to execute via Etcd, source: {}", source))]
    EtcdFailed {
        source: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to connect to Etcd, source: {}", source))]
    ConnectEtcd {
        source: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: String,
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("Failed to start gRPC server, source: {}", source))]
    StartGrpc {
        source: tonic::transport::Error,
        location: Location,
    },
    #[snafu(display("Failed to start http server, source: {}", source))]
    StartHttp {
        #[snafu(backtrace)]
        source: servers::error::Error,
    },
    #[snafu(display("Failed to parse address {}, source: {}", addr, source))]
    ParseAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
    #[snafu(display("Empty table name"))]
    EmptyTableName { location: Location },

    #[snafu(display("Invalid datanode lease key: {}", key))]
    InvalidLeaseKey { key: String, location: Location },

    #[snafu(display("Invalid datanode stat key: {}", key))]
    InvalidStatKey { key: String, location: Location },

    #[snafu(display("Failed to parse datanode lease key from utf8: {}", source))]
    LeaseKeyFromUtf8 {
        source: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse datanode lease value from utf8: {}", source))]
    LeaseValueFromUtf8 {
        source: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse datanode stat key from utf8: {}", source))]
    StatKeyFromUtf8 {
        source: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse datanode stat value from utf8: {}", source))]
    StatValueFromUtf8 {
        source: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to serialize to json: {}", input))]
    SerializeToJson {
        input: String,
        source: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize from json: {}", input))]
    DeserializeFromJson {
        input: String,
        source: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse number: {}, source: {}", err_msg, source))]
    ParseNum {
        err_msg: String,
        source: std::num::ParseIntError,
        location: Location,
    },

    #[snafu(display("Invalid arguments: {}", err_msg))]
    InvalidArguments { err_msg: String, location: Location },

    #[snafu(display("Invalid result with a txn response: {}", err_msg))]
    InvalidTxnResult { err_msg: String, location: Location },

    #[snafu(display("Cannot parse catalog value, source: {}", source))]
    InvalidCatalogValue {
        #[snafu(backtrace)]
        source: common_catalog::error::Error,
    },

    #[snafu(display("Unexcepted sequence value: {}", err_msg))]
    UnexceptedSequenceValue { err_msg: String, location: Location },

    #[snafu(display("Failed to decode table route, source: {}", source))]
    DecodeTableRoute {
        source: prost::DecodeError,
        location: Location,
    },

    #[snafu(display("Table route not found: {}", key))]
    TableRouteNotFound { key: String, location: Location },

    #[snafu(display("Failed to get sequence: {}", err_msg))]
    NextSequence { err_msg: String, location: Location },

    #[snafu(display("Sequence out of range: {}, start={}, step={}", name, start, step))]
    SequenceOutOfRange {
        name: String,
        start: u64,
        step: u64,
        location: Location,
    },

    #[snafu(display("MetaSrv has no leader at this moment"))]
    NoLeader { location: Location },

    #[snafu(display("Table {} not found", name))]
    TableNotFound { name: String, location: Location },

    #[snafu(display(
        "Failed to move the value of {} because other clients caused a race condition",
        key
    ))]
    MoveValue { key: String, location: Location },

    #[snafu(display("Unsupported selector type, {}", selector_type))]
    UnsupportedSelectorType {
        selector_type: String,
        location: Location,
    },

    #[snafu(display("Failed to decode table global value, source: {}", source))]
    DecodeTableGlobalValue {
        source: prost::DecodeError,
        location: Location,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("Invalid KVs length, expected: {}, actual: {}", expected, actual))]
    InvalidKvsLength {
        expected: usize,
        actual: usize,
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        #[snafu(backtrace)]
        source: common_grpc::error::Error,
    },

    #[snafu(display(
        "Failed to batch get KVs from leader's in_memory kv store, source: {}",
        source
    ))]
    BatchGet {
        source: tonic::Status,
        location: Location,
    },

    #[snafu(display(
        "Failed to batch range KVs from leader's in_memory kv store, source: {}",
        source
    ))]
    Range {
        source: tonic::Status,
        location: Location,
    },

    #[snafu(display("Response header not found"))]
    ResponseHeaderNotFound { location: Location },

    #[snafu(display("The requested meta node is not leader, node addr: {}", node_addr))]
    IsNotLeader {
        node_addr: String,
        location: Location,
    },

    #[snafu(display("MetaSrv has no meta peer client"))]
    NoMetaPeerClient { location: Location },

    #[snafu(display("Invalid http body, source: {}", source))]
    InvalidHttpBody {
        source: http::Error,
        location: Location,
    },

    #[snafu(display(
        "The number of retries for the grpc call {} exceeded the limit, {}",
        func_name,
        retry_num
    ))]
    ExceededRetryLimit {
        func_name: String,
        retry_num: usize,
        location: Location,
    },

    #[snafu(display("An error occurred in Meta, source: {}", source))]
    MetaInternal {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to lock based on etcd, source: {}", source))]
    Lock {
        source: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to unlock based on etcd, source: {}", source))]
    Unlock {
        source: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to grant lease, source: {}", source))]
    LeaseGrant {
        source: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Distributed lock is not configured"))]
    LockNotConfig { location: Location },

    #[snafu(display("Invalid utf-8 value, source: {:?}", source))]
    InvalidUtf8Value {
        source: FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Missing required parameter, param: {:?}", param))]
    MissingRequiredParameter { param: String },

    #[snafu(display("Failed to recover procedure, source: {source}"))]
    RecoverProcedure {
        #[snafu(backtrace)]
        source: common_procedure::Error,
    },

    #[snafu(display("Schema already exists, name: {schema_name}"))]
    SchemaAlreadyExists {
        schema_name: String,
        location: Location,
    },

    #[snafu(display("Pusher not found: {pusher_id}"))]
    PusherNotFound {
        pusher_id: String,
        location: Location,
    },

    #[snafu(display("Failed to push message: {err_msg}"))]
    PushMessage { err_msg: String, location: Location },

    #[snafu(display("Mailbox already closed: {id}"))]
    MailboxClosed { id: u64, location: Location },

    #[snafu(display("Mailbox timeout: {id}"))]
    MailboxTimeout { id: u64, location: Location },

    #[snafu(display("Mailbox receiver got an error: {id}, {err_msg}"))]
    MailboxReceiver {
        id: u64,
        err_msg: String,
        location: Location,
    },

    #[snafu(display("Missing request header"))]
    MissingRequestHeader { location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        Status::new(Code::Internal, err.to_string())
    }
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::StreamNone { .. }
            | Error::EtcdFailed { .. }
            | Error::ConnectEtcd { .. }
            | Error::TcpBind { .. }
            | Error::SerializeToJson { .. }
            | Error::DeserializeFromJson { .. }
            | Error::DecodeTableRoute { .. }
            | Error::DecodeTableGlobalValue { .. }
            | Error::NoLeader { .. }
            | Error::CreateChannel { .. }
            | Error::BatchGet { .. }
            | Error::Range { .. }
            | Error::ResponseHeaderNotFound { .. }
            | Error::IsNotLeader { .. }
            | Error::NoMetaPeerClient { .. }
            | Error::InvalidHttpBody { .. }
            | Error::Lock { .. }
            | Error::Unlock { .. }
            | Error::LeaseGrant { .. }
            | Error::LockNotConfig { .. }
            | Error::ExceededRetryLimit { .. }
            | Error::SendShutdownSignal { .. }
            | Error::ParseAddr { .. }
            | Error::SchemaAlreadyExists { .. }
            | Error::PusherNotFound { .. }
            | Error::PushMessage { .. }
            | Error::MailboxClosed { .. }
            | Error::MailboxTimeout { .. }
            | Error::MailboxReceiver { .. }
            | Error::StartGrpc { .. } => StatusCode::Internal,
            Error::EmptyKey { .. }
            | Error::MissingRequiredParameter { .. }
            | Error::MissingRequestHeader { .. }
            | Error::EmptyTableName { .. }
            | Error::InvalidLeaseKey { .. }
            | Error::InvalidStatKey { .. }
            | Error::ParseNum { .. }
            | Error::UnsupportedSelectorType { .. }
            | Error::InvalidArguments { .. } => StatusCode::InvalidArguments,
            Error::LeaseKeyFromUtf8 { .. }
            | Error::LeaseValueFromUtf8 { .. }
            | Error::StatKeyFromUtf8 { .. }
            | Error::StatValueFromUtf8 { .. }
            | Error::UnexceptedSequenceValue { .. }
            | Error::TableRouteNotFound { .. }
            | Error::NextSequence { .. }
            | Error::SequenceOutOfRange { .. }
            | Error::MoveValue { .. }
            | Error::InvalidKvsLength { .. }
            | Error::InvalidTxnResult { .. }
            | Error::InvalidUtf8Value { .. }
            | Error::Unexpected { .. } => StatusCode::Unexpected,
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::InvalidCatalogValue { source, .. } => source.status_code(),
            Error::MetaInternal { source } => source.status_code(),
            Error::RecoverProcedure { source } => source.status_code(),
            Error::ShutdownServer { source, .. } | Error::StartHttp { source } => {
                source.status_code()
            }
        }
    }
}

// for form tonic
pub(crate) fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn std::error::Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
