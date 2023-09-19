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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported operation: {}", operation))]
    Unsupported {
        operation: String,
        location: Location,
    },

    #[snafu(display("Unexpected engine: {}", engine))]
    UnexpectedEngine { engine: String, location: Location },

    #[snafu(display("Invalid region metadata, source: {}", source))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
        location: Location,
    },

    #[snafu(display("Region {} already exists", region_id))]
    RegionExists {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region not found, region_id: {}", region_id))]
    RegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Failed to check object from path: {}, source: {}", path, source))]
    CheckObject {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Fail to encode object into json, source: {}", source))]
    EncodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Fail to decode object from json, source: {}", source))]
    DecodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display(
        "Failed to store region manifest, region_id: {}, source: {}",
        region_id,
        source,
    ))]
    StoreRegionManifest {
        source: object_store::Error,
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display(
        "Failed to load region manifest, region_id: {}, source: {}",
        region_id,
        source,
    ))]
    LoadRegionManifest {
        source: object_store::Error,
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display(
        "Failed to delete region manifest, region_id: {}, source: {}",
        region_id,
        source,
    ))]
    DeleteRegionManifest {
        source: object_store::Error,
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Manifest already exists: {}", path))]
    ManifestExists { path: String, location: Location },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, location: Location },

    #[snafu(display("Failed to build backend, source: {}", source))]
    BuildBackend {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to build csv config: {}", source))]
    BuildCsvConfig {
        source: common_datasource::file_format::csv::CsvConfigBuilderError,
        location: Location,
    },

    #[snafu(display("Failed to build stream: {}", source))]
    BuildStream {
        source: DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to project schema: {}", source))]
    ProjectArrowSchema {
        source: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to project schema: {}", source))]
    ProjectSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to build stream adapter: {}", source))]
    BuildStreamAdapter {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to parse file format: {}", source))]
    ParseFileFormat {
        location: Location,
        source: common_datasource::error::Error,
    },

    #[snafu(display("Failed to generate parquet scan plan: {}", source))]
    ParquetScanPlan {
        source: DataFusionError,
        location: Location,
    },

    #[snafu(display(
        "Projection out of bounds, column_index: {}, bounds: {}",
        column_index,
        bounds
    ))]
    ProjectionOutOfBounds {
        column_index: usize,
        bounds: usize,
        location: Location,
    },

    #[snafu(display("Failed to extract column from filter: {}", source))]
    ExtractColumnFromFilter {
        source: DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to create default value for column: {}", column))]
    CreateDefault {
        column: String,
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Missing default value for column: {}", column))]
    MissingColumnNoDefault { column: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            BuildCsvConfig { .. }
            | ProjectArrowSchema { .. }
            | ProjectSchema { .. }
            | MissingRequiredField { .. }
            | Unsupported { .. }
            | InvalidMetadata { .. }
            | ProjectionOutOfBounds { .. }
            | CreateDefault { .. }
            | MissingColumnNoDefault { .. } => StatusCode::InvalidArguments,

            RegionExists { .. } => StatusCode::RegionAlreadyExists,
            RegionNotFound { .. } => StatusCode::RegionNotFound,

            BuildBackend { source, .. } => source.status_code(),
            BuildStreamAdapter { source, .. } => source.status_code(),
            ParseFileFormat { source, .. } => source.status_code(),

            CheckObject { .. }
            | StoreRegionManifest { .. }
            | LoadRegionManifest { .. }
            | DeleteRegionManifest { .. } => StatusCode::StorageUnavailable,

            EncodeJson { .. }
            | DecodeJson { .. }
            | ManifestExists { .. }
            | BuildStream { .. }
            | ParquetScanPlan { .. }
            | UnexpectedEngine { .. }
            | ExtractColumnFromFilter { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for common_procedure::Error {
    fn from(e: Error) -> common_procedure::Error {
        common_procedure::Error::from_error_ext(e)
    }
}
