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

use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use object_store::error::retry_hint_from_opendal_error;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported operation: {}", operation))]
    Unsupported {
        operation: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected engine: {}", engine))]
    UnexpectedEngine {
        engine: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region metadata"))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region not found, region_id: {}", region_id))]
    RegionNotFound {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Region already exists with a different definition, region_id: {}",
        region_id
    ))]
    RegionExists {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to check object from path: {}", path))]
    CheckObject {
        path: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Fail to encode object into json"))]
    EncodeJson {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Fail to decode object from json"))]
    DecodeJson {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Failed to store region manifest, region_id: {}", region_id))]
    StoreRegionManifest {
        #[snafu(source)]
        error: object_store::Error,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to load region manifest, region_id: {}", region_id))]
    LoadRegionManifest {
        #[snafu(source)]
        error: object_store::Error,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to delete region manifest, region_id: {},", region_id))]
    DeleteRegionManifest {
        #[snafu(source)]
        error: object_store::Error,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Manifest already exists: {}", path))]
    ManifestExists {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            // Unlike file-engine, an unsupported operation (e.g. scan) must surface as
            // `Unimplemented` (StatusCode::Unsupported) so the client gets a clear
            // "not implemented" signal instead of a generic "invalid arguments".
            Unsupported { .. } => StatusCode::Unsupported,

            InvalidMetadata { .. } | MissingRequiredField { .. } => StatusCode::InvalidArguments,

            RegionNotFound { .. } => StatusCode::RegionNotFound,

            RegionExists { .. } => StatusCode::RegionAlreadyExists,

            CheckObject { .. }
            | StoreRegionManifest { .. }
            | LoadRegionManifest { .. }
            | DeleteRegionManifest { .. } => StatusCode::StorageUnavailable,

            EncodeJson { .. }
            | DecodeJson { .. }
            | ManifestExists { .. }
            | UnexpectedEngine { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn retry_hint(&self) -> RetryHint {
        use Error::*;

        match self {
            CheckObject { error, .. }
            | StoreRegionManifest { error, .. }
            | LoadRegionManifest { error, .. }
            | DeleteRegionManifest { error, .. } => retry_hint_from_opendal_error(error),
            _ => RetryHint::NonRetryable,
        }
    }
}
