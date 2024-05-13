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

use arrow_schema::ArrowError;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::parquet::errors::ParquetError;
use snafu::{Location, Snafu};
use url::ParseError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported compression type: {}", compression_type))]
    UnsupportedCompressionType {
        compression_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported backend protocol: {}, url: {}", protocol, url))]
    UnsupportedBackendProtocol {
        protocol: String,
        #[snafu(implicit)]
        location: Location,
        url: String,
    },

    #[snafu(display("Unsupported format protocol: {}", format))]
    UnsupportedFormat {
        format: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("empty host: {}", url))]
    EmptyHostPath {
        url: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid url: {}", url))]
    InvalidUrl {
        url: String,
        #[snafu(source)]
        error: ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build backend"))]
    BuildBackend {
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build orc reader"))]
    OrcReader {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: orc_rust::error::OrcError,
    },

    #[snafu(display("Failed to read object from path: {}", path))]
    ReadObject {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Failed to write object to path: {}", path))]
    WriteObject {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Failed to write"))]
    AsyncWrite {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to write record batch"))]
    WriteRecordBatch {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("Failed to encode record batch"))]
    EncodeRecordBatch {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: ParquetError,
    },

    #[snafu(display("Failed to read record batch"))]
    ReadRecordBatch {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to read parquet"))]
    ReadParquetSnafu {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to convert parquet to schema"))]
    ParquetToSchema {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to infer schema from file"))]
    InferSchema {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: arrow_schema::ArrowError,
    },

    #[snafu(display("Failed to list object in path: {}", path))]
    ListObjects {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Invalid connection: {}", msg))]
    InvalidConnection {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to join handle"))]
    JoinHandle {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: tokio::task::JoinError,
    },

    #[snafu(display("Failed to parse format {} with value: {}", key, value))]
    ParseFormat {
        key: &'static str,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to merge schema"))]
    MergeSchema {
        #[snafu(source)]
        error: arrow_schema::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Buffered writer closed"))]
    BufferedWriterClosed {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to write parquet file, path: {}", path))]
    WriteParquet {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            BuildBackend { .. }
            | ListObjects { .. }
            | ReadObject { .. }
            | WriteObject { .. }
            | AsyncWrite { .. }
            | WriteParquet { .. } => StatusCode::StorageUnavailable,

            UnsupportedBackendProtocol { .. }
            | UnsupportedCompressionType { .. }
            | UnsupportedFormat { .. }
            | InvalidConnection { .. }
            | InvalidUrl { .. }
            | EmptyHostPath { .. }
            | InferSchema { .. }
            | ReadParquetSnafu { .. }
            | ParquetToSchema { .. }
            | ParseFormat { .. }
            | MergeSchema { .. } => StatusCode::InvalidArguments,

            JoinHandle { .. }
            | ReadRecordBatch { .. }
            | WriteRecordBatch { .. }
            | EncodeRecordBatch { .. }
            | BufferedWriterClosed { .. }
            | OrcReader { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
