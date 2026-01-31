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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to decode protobuf"))]
    DecodeProto {
        #[snafu(source)]
        error: prost::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize bitmap"))]
    DeserializeBitmap {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize bitmap"))]
    SerializeBitmap {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Vector index blob too small"))]
    BlobTooSmall {
        min_size: usize,
        actual_size: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Vector index blob truncated"))]
    BlobTruncated {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid vector index blob"))]
    InvalidBlob {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown vector index engine type: {engine_type}"))]
    UnknownEngineType {
        engine_type: i32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown vector distance metric: {metric}"))]
    UnknownDistanceMetric {
        metric: i32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Vector index engine error: {reason}"))]
    Engine {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to map HNSW key to row offset: {reason}"))]
    KeyMapping {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error"))]
    External {
        #[snafu(source)]
        error: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to finish vector index: {reason}"))]
    Finish {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Row count overflow: {current} + {increment} exceeds u32::MAX"))]
    RowCountOverflow {
        current: u64,
        increment: u64,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            DecodeProto { .. }
            | DeserializeBitmap { .. }
            | SerializeBitmap { .. }
            | BlobTooSmall { .. }
            | BlobTruncated { .. }
            | InvalidBlob { .. }
            | UnknownEngineType { .. }
            | UnknownDistanceMetric { .. }
            | Engine { .. }
            | KeyMapping { .. }
            | External { .. }
            | Finish { .. }
            | RowCountOverflow { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
