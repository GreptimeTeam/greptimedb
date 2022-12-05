// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use api::serde::DecodeError;
use common_error::prelude::*;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Connect failed to {}, source: {}", url, source))]
    ConnectFailed {
        url: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing {}, expected {}, actual {}", name, expected, actual))]
    MissingResult {
        name: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Missing result header"))]
    MissingHeader,

    #[snafu(display("Tonic internal error, addr: {}, source: {}", addr, source))]
    TonicStatus {
        addr: String,
        source: tonic::Status,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to decode select result, source: {}", source))]
    DecodeSelect { source: DecodeError },

    #[snafu(display("Error occurred on the data node, code: {}, msg: {}", code, msg))]
    Datanode { code: u32, msg: String },

    #[snafu(display("Failed to encode physical plan: {:?}, source: {}", physical, source))]
    EncodePhysical {
        physical: Arc<dyn ExecutionPlan>,
        #[snafu(backtrace)]
        source: common_grpc::Error,
    },

    #[snafu(display("Mutate result has failure {}", failure))]
    MutateFailure { failure: u32, backtrace: Backtrace },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Failed to create RecordBatches, source: {}", source))]
    CreateRecordBatches {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, backtrace: Backtrace },

    #[snafu(display("Failed to convert schema, source: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Failed to create gRPC channel, peer address: {}, source: {}",
        addr,
        source
    ))]
    CreateChannel {
        addr: String,
        #[snafu(backtrace)]
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to convert column to vector, source: {}", source))]
    ColumnToVector {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ConnectFailed { .. }
            | Error::MissingResult { .. }
            | Error::MissingHeader { .. }
            | Error::TonicStatus { .. }
            | Error::DecodeSelect { .. }
            | Error::Datanode { .. }
            | Error::EncodePhysical { .. }
            | Error::MutateFailure { .. }
            | Error::ColumnDataType { .. }
            | Error::MissingField { .. } => StatusCode::Internal,
            Error::ConvertSchema { source } => source.status_code(),
            Error::CreateRecordBatches { source } => source.status_code(),
            Error::CreateChannel { source, .. } => source.status_code(),
            Error::IllegalGrpcClientState { .. } => StatusCode::Unexpected,
            Error::ColumnToVector { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
