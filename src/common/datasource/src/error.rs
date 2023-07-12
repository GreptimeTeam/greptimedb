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
use datafusion::parquet::errors::ParquetError;
use snafu::{Location, Snafu};
use url::ParseError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported compression type: {}", compression_type))]
    UnsupportedCompressionType {
        compression_type: String,
        location: Location,
    },

    #[snafu(display("Unsupported backend protocol: {}", protocol))]
    UnsupportedBackendProtocol {
        protocol: String,
        location: Location,
    },

    #[snafu(display("Unsupported format protocol: {}", format))]
    UnsupportedFormat { format: String, location: Location },

    #[snafu(display("empty host: {}", url))]
    EmptyHostPath { url: String, location: Location },

    #[snafu(display("Invalid url: {}, error :{}", url, source))]
    InvalidUrl {
        url: String,
        source: ParseError,
        location: Location,
    },

    #[snafu(display("Failed to build backend, source: {}", source))]
    BuildBackend {
        source: object_store::Error,
        location: Location,
    },

    #[snafu(display("Failed to build orc reader, source: {}", source))]
    OrcReader {
        location: Location,
        source: orc_rust::error::Error,
    },

    #[snafu(display("Failed to read object from path: {}, source: {}", path, source))]
    ReadObject {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Failed to write object to path: {}, source: {}", path, source))]
    WriteObject {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Failed to write: {}", source))]
    AsyncWrite {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("Failed to write record batch: {}", source))]
    WriteRecordBatch {
        location: Location,
        source: ArrowError,
    },

    #[snafu(display("Failed to encode record batch: {}", source))]
    EncodeRecordBatch {
        location: Location,
        source: ParquetError,
    },

    #[snafu(display("Failed to read record batch: {}", source))]
    ReadRecordBatch {
        location: Location,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to read parquet source: {}", source))]
    ReadParquetSnafu {
        location: Location,
        source: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to convert parquet to schema: {}", source))]
    ParquetToSchema {
        location: Location,
        source: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to infer schema from file, source: {}", source))]
    InferSchema {
        location: Location,
        source: arrow_schema::ArrowError,
    },

    #[snafu(display("Failed to list object in path: {}, source: {}", path, source))]
    ListObjects {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Invalid connection: {}", msg))]
    InvalidConnection { msg: String, location: Location },

    #[snafu(display("Failed to join handle: {}", source))]
    JoinHandle {
        location: Location,
        source: tokio::task::JoinError,
    },

    #[snafu(display("Failed to parse format {} with value: {}", key, value))]
    ParseFormat {
        key: &'static str,
        value: String,
        location: Location,
    },

    #[snafu(display("Failed to merge schema: {}", source))]
    MergeSchema {
        source: arrow_schema::ArrowError,
        location: Location,
    },

    #[snafu(display("Buffered writer closed"))]
    BufferedWriterClosed { location: Location },
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
            | AsyncWrite { .. } => StatusCode::StorageUnavailable,

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

    fn location_opt(&self) -> Option<common_error::snafu::Location> {
        use Error::*;
        match self {
            OrcReader { location, .. } => Some(*location),
            BuildBackend { location, .. } => Some(*location),
            ReadObject { location, .. } => Some(*location),
            ListObjects { location, .. } => Some(*location),
            InferSchema { location, .. } => Some(*location),
            ReadParquetSnafu { location, .. } => Some(*location),
            ParquetToSchema { location, .. } => Some(*location),
            JoinHandle { location, .. } => Some(*location),
            ParseFormat { location, .. } => Some(*location),
            MergeSchema { location, .. } => Some(*location),
            WriteObject { location, .. } => Some(*location),
            ReadRecordBatch { location, .. } => Some(*location),
            WriteRecordBatch { location, .. } => Some(*location),
            AsyncWrite { location, .. } => Some(*location),
            EncodeRecordBatch { location, .. } => Some(*location),
            BufferedWriterClosed { location, .. } => Some(*location),

            UnsupportedBackendProtocol { location, .. } => Some(*location),
            EmptyHostPath { location, .. } => Some(*location),
            InvalidUrl { location, .. } => Some(*location),
            InvalidConnection { location, .. } => Some(*location),
            UnsupportedCompressionType { location, .. } => Some(*location),
            UnsupportedFormat { location, .. } => Some(*location),
        }
    }
}
