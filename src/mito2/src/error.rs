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

use common_datasource::compression::CompressionType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use snafu::{Location, Snafu};
use store_api::manifest::ManifestVersion;

use crate::worker::WorkerId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("OpenDAL operator failed. Location: {}, source: {}", location, source))]
    OpenDal {
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display(
        "Fail to compress object by {}, path: {}, source: {}",
        compress_type,
        path,
        source
    ))]
    CompressObject {
        compress_type: CompressionType,
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Fail to decompress object by {}, path: {}, source: {}",
        compress_type,
        path,
        source
    ))]
    DecompressObject {
        compress_type: CompressionType,
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to ser/de json object. Location: {}, source: {}",
        location,
        source
    ))]
    SerdeJson {
        location: Location,
        source: serde_json::Error,
    },

    #[snafu(display("Invalid scan index, start: {}, end: {}", start, end))]
    InvalidScanIndex {
        start: ManifestVersion,
        end: ManifestVersion,
        location: Location,
    },

    #[snafu(display("Invalid UTF-8 content. Location: {}, source: {}", location, source))]
    Utf8 {
        location: Location,
        source: std::str::Utf8Error,
    },

    #[snafu(display("Failed to join handle, location: {}, source: {}", location, source))]
    Join {
        source: common_runtime::JoinError,
        location: Location,
    },

    #[snafu(display("Worker {} is stopped, location: {}", id, location))]
    WorkerStopped { id: WorkerId, location: Location },

    #[snafu(display("Failed to recv result, location: {}, source: {}", location, source))]
    Recv {
        source: tokio::sync::oneshot::error::RecvError,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            OpenDal { .. } => StatusCode::StorageUnavailable,
            CompressObject { .. } | DecompressObject { .. } | SerdeJson { .. } | Utf8 { .. } => {
                StatusCode::Unexpected
            }
            InvalidScanIndex { .. } => StatusCode::InvalidArguments,
            Join { .. } | WorkerStopped { .. } | Recv { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
