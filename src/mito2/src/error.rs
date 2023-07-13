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
use common_error::prelude::*;
use snafu::Location;
use store_api::manifest::ManifestVersion;
use store_api::storage::{ColumnDescriptorBuilderError, ColumnId};

use crate::manifest::action::RegionMetaAction;
use crate::region::metadata::VersionNumber;

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

    #[snafu(display(
        "Unsupported action when making manifest checkpoint: {:?}. Location {}",
        action,
        location
    ))]
    ManifestCheckpoint {
        action: RegionMetaAction,
        location: Location,
    },

    #[snafu(display(
        "Expect altering metadata with version {}, given {}. Location {}",
        expect,
        given,
        location
    ))]
    InvalidAlterVersion {
        expect: VersionNumber,
        given: VersionNumber,
        location: Location,
    },

    #[snafu(display(
        "Invalid alter operation on {}: {}. Location {}",
        name,
        reason,
        location
    ))]
    InvalidAlterOperation {
        name: String,
        reason: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to convert to column schema, source: {}. Location {}",
        source,
        location
    ))]
    ToColumnSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Failed to build column descriptor, source: {}. Location {}",
        source,
        location
    ))]
    BuildColumnDescriptor {
        source: ColumnDescriptorBuilderError,
        location: Location,
    },

    #[snafu(display(
        "Failed to access k-v metadata, source: {}. Location {}",
        source,
        location
    ))]
    AccessKvMetadata {
        source: datatypes::Error,
        location: Location,
    },

    #[snafu(display("Column name {} is reserved by the system", name))]
    ReservedColumn { name: String, location: Location },

    #[snafu(display("Column name {} already exists", name))]
    ColNameExists { name: String, location: Location },

    #[snafu(display("Column id {} already exists", id))]
    ColIdExists { id: ColumnId, location: Location },

    #[snafu(display("Missing timestamp key column"))]
    MissingTimestamp { location: Location },

    #[snafu(display("Error from storage metadata: {}. Location: {}", source, location))]
    StorageMetadata {
        source: storage::metadata::Error,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    #[allow(clippy::match_single_binding)]
    fn status_code(&self) -> StatusCode {
        match self {
            _ => todo!(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Error {
    /// Returns true if the error is the object path to delete
    /// doesn't exist.
    pub fn is_opendal_not_found(&self) -> bool {
        if let Error::OpenDal { source, .. } = self {
            source.kind() == object_store::ErrorKind::NotFound
        } else {
            false
        }
    }
}
