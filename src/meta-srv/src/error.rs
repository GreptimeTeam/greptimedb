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
use tokio::sync::mpsc::error::SendError;
use tonic::codegen::http;
use tonic::{Code, Status};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to send shutdown signal"))]
    SendShutdownSignal { source: SendError<()> },

    #[snafu(display("Error stream request next is None"))]
    StreamNone { backtrace: Backtrace },

    #[snafu(display("Empty key is not allowed"))]
    EmptyKey { backtrace: Backtrace },

    #[snafu(display("Failed to execute via Etcd, source: {}", source))]
    EtcdFailed {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to connect to Etcd, source: {}", source))]
    ConnectEtcd {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to start gRPC server, source: {}", source))]
    StartGrpc {
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Empty table name"))]
    EmptyTableName { backtrace: Backtrace },

    #[snafu(display("Invalid datanode lease key: {}", key))]
    InvalidLeaseKey { key: String, backtrace: Backtrace },

    #[snafu(display("Invalid datanode stat key: {}", key))]
    InvalidStatKey { key: String, backtrace: Backtrace },

    #[snafu(display("Failed to parse datanode lease key from utf8: {}", source))]
    LeaseKeyFromUtf8 {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse datanode lease value from utf8: {}", source))]
    LeaseValueFromUtf8 {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse datanode stat key from utf8: {}", source))]
    StatKeyFromUtf8 {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse datanode stat value from utf8: {}", source))]
    StatValueFromUtf8 {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to serialize to json: {}", input))]
    SerializeToJson {
        input: String,
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to deserialize from json: {}", input))]
    DeserializeFromJson {
        input: String,
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse number: {}, source: {}", err_msg, source))]
    ParseNum {
        err_msg: String,
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid arguments: {}", err_msg))]
    InvalidArguments {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid result with a txn response: {}", err_msg))]
    InvalidTxnResult {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot parse catalog value, source: {}", source))]
    InvalidCatalogValue {
        #[snafu(backtrace)]
        source: common_catalog::error::Error,
    },

    #[snafu(display("Unexcepted sequence value: {}", err_msg))]
    UnexceptedSequenceValue {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode table route, source: {}", source))]
    DecodeTableRoute {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Table route not found: {}", key))]
    TableRouteNotFound { key: String, backtrace: Backtrace },

    #[snafu(display("Failed to get sequence: {}", err_msg))]
    NextSequence {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("MetaSrv has no leader at this moment"))]
    NoLeader { backtrace: Backtrace },

    #[snafu(display("Table {} not found", name))]
    TableNotFound { name: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to move the value of {} because other clients caused a race condition",
        key
    ))]
    MoveValue { key: String, backtrace: Backtrace },

    #[snafu(display("Unsupported selector type, {}", selector_type))]
    UnsupportedSelectorType {
        selector_type: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode table global value, source: {}", source))]
    DecodeTableGlobalValue {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid KVs length, expected: {}, actual: {}", expected, actual))]
    InvalidKvsLength {
        expected: usize,
        actual: usize,
        backtrace: Backtrace,
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
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to batch range KVs from leader's in_memory kv store, source: {}",
        source
    ))]
    Range {
        source: tonic::Status,
        backtrace: Backtrace,
    },

    #[snafu(display("Response header not found"))]
    ResponseHeaderNotFound { backtrace: Backtrace },

    #[snafu(display("The requested meta node is not leader, node addr: {}", node_addr))]
    IsNotLeader {
        node_addr: String,
        backtrace: Backtrace,
    },

    #[snafu(display("MetaSrv has no meta peer client"))]
    NoMetaPeerClient { backtrace: Backtrace },

    #[snafu(display("Invalid http body, source: {}", source))]
    InvalidHttpBody {
        source: http::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "The number of retries for the grpc call {} exceeded the limit, {}",
        func_name,
        retry_num
    ))]
    ExceededRetryLimit {
        func_name: String,
        retry_num: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("An error occurred in Meta, source: {}", source))]
    MetaInternal {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to lock based on etcd, source: {}", source))]
    Lock {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to unlock based on etcd, source: {}", source))]
    Unlock {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to grant lease, source: {}", source))]
    LeaseGrant {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Distributed lock is not configured"))]
    LockNotConfig { backtrace: Backtrace },

    #[snafu(display("Invalid utf-8 value, source: {:?}", source))]
    InvalidUtf8Value {
        source: FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing required parameter, param: {:?}", param))]
    MissingRequiredParameter { param: String },

    #[snafu(display("Failed to recover procedure, source: {source}"))]
    RecoverProcedure {
        #[snafu(backtrace)]
        source: common_procedure::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        Status::new(Code::Internal, err.to_string())
    }
}

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

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
            | Error::StartGrpc { .. } => StatusCode::Internal,
            Error::EmptyKey { .. }
            | Error::MissingRequiredParameter { .. }
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
            | Error::MoveValue { .. }
            | Error::InvalidKvsLength { .. }
            | Error::InvalidTxnResult { .. }
            | Error::InvalidUtf8Value { .. }
            | Error::Unexpected { .. } => StatusCode::Unexpected,
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::InvalidCatalogValue { source, .. } => source.status_code(),
            Error::MetaInternal { source } => source.status_code(),
            Error::RecoverProcedure { source } => source.status_code(),
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

#[cfg(test)]
mod tests {
    use super::*;

    type StdResult<E> = std::result::Result<(), E>;

    fn throw_none_option() -> Option<String> {
        None
    }

    fn throw_etcd_client_error() -> StdResult<etcd_client::Error> {
        Err(etcd_client::Error::InvalidArgs("".to_string()))
    }

    fn throw_serde_json_error() -> StdResult<serde_json::error::Error> {
        serde_json::from_str("invalid json")
    }

    #[test]
    fn test_stream_node_error() {
        let e = throw_none_option().context(StreamNoneSnafu).err().unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_empty_key_error() {
        let e = throw_none_option().context(EmptyKeySnafu).err().unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_etcd_failed_error() {
        let e = throw_etcd_client_error()
            .context(EtcdFailedSnafu)
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_connect_etcd_error() {
        let e = throw_etcd_client_error()
            .context(ConnectEtcdSnafu)
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_tcp_bind_error() {
        fn throw_std_error() -> StdResult<std::io::Error> {
            Err(std::io::ErrorKind::NotFound.into())
        }
        let e = throw_std_error()
            .context(TcpBindSnafu { addr: "127.0.0.1" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_start_grpc_error() {
        fn throw_tonic_error() -> StdResult<tonic::transport::Error> {
            tonic::transport::Endpoint::new("http//http").map(|_| ())
        }
        let e = throw_tonic_error().context(StartGrpcSnafu).err().unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_empty_table_error() {
        let e = throw_none_option()
            .context(EmptyTableNameSnafu)
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_invalid_lease_key_error() {
        let e = throw_none_option()
            .context(InvalidLeaseKeySnafu { key: "test" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_lease_key_fromutf8_test() {
        fn throw_fromutf8_error() -> StdResult<std::string::FromUtf8Error> {
            let sparkle_heart = vec![0, 159, 146, 150];
            String::from_utf8(sparkle_heart).map(|_| ())
        }
        let e = throw_fromutf8_error()
            .context(LeaseKeyFromUtf8Snafu)
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Unexpected);
    }

    #[test]
    fn test_serialize_to_json_error() {
        let e = throw_serde_json_error()
            .context(SerializeToJsonSnafu { input: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_deserialize_from_json_error() {
        let e = throw_serde_json_error()
            .context(DeserializeFromJsonSnafu { input: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_parse_num_error() {
        fn throw_parse_int_error() -> StdResult<std::num::ParseIntError> {
            "invalid num".parse::<i64>().map(|_| ())
        }
        let e = throw_parse_int_error()
            .context(ParseNumSnafu { err_msg: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_invalid_arguments_error() {
        let e = throw_none_option()
            .context(InvalidArgumentsSnafu { err_msg: "test" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_invalid_txn_error() {
        let e = throw_none_option()
            .context(InvalidTxnResultSnafu { err_msg: "test" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Unexpected);
    }
}
