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

use common_datasource::file_format::Format;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use table::metadata::{TableInfoBuilderError, TableMetaBuilderError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
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

    #[snafu(display("Failed to drop table, table: {}, source: {}", table_name, source))]
    DropTable {
        source: BoxedError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to write table manifest, table: {}, source: {}",
        table_name,
        source,
    ))]
    WriteTableManifest {
        source: object_store::Error,
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to write immutable manifest, path: {}", path))]
    WriteImmutableManifest { path: String, location: Location },

    #[snafu(display("Failed to delete table table manifest, source: {}", source,))]
    DeleteTableManifest {
        source: object_store::Error,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to read table manifest, table: {}, source: {}",
        table_name,
        source,
    ))]
    ReadTableManifest {
        source: object_store::Error,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build table meta for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableMeta {
        source: TableMetaBuilderError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build table info for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableInfo {
        source: TableInfoBuilderError,
        table_name: String,
        location: Location,
    },

    #[snafu(display("Table already exists: {}", table_name))]
    TableExists {
        location: Location,
        table_name: String,
    },

    #[snafu(display(
        "Failed to convert metadata from deserialized data, source: {}",
        source
    ))]
    ConvertRaw {
        location: Location,
        source: table::metadata::ConvertError,
    },

    #[snafu(display("Invalid schema, source: {}", source))]
    InvalidRawSchema {
        location: Location,
        source: datatypes::error::Error,
    },

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
    ProjectSchema {
        source: ArrowError,
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

    #[snafu(display("Failed to convert schema: {}", source))]
    ConvertSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Unsupported format: {:?}", format))]
    UnsupportedFormat { format: Format, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            TableExists { .. }
            | BuildTableMeta { .. }
            | BuildTableInfo { .. }
            | InvalidRawSchema { .. }
            | BuildCsvConfig { .. }
            | ProjectSchema { .. }
            | MissingRequiredField { .. }
            | ConvertSchema { .. }
            | UnsupportedFormat { .. } => StatusCode::InvalidArguments,

            BuildBackend { source, .. } => source.status_code(),
            BuildStreamAdapter { source, .. } => source.status_code(),
            ParseFileFormat { source, .. } => source.status_code(),

            WriteTableManifest { .. }
            | DeleteTableManifest { .. }
            | ReadTableManifest { .. }
            | CheckObject { .. } => StatusCode::StorageUnavailable,

            EncodeJson { .. }
            | DecodeJson { .. }
            | ConvertRaw { .. }
            | DropTable { .. }
            | WriteImmutableManifest { .. }
            | BuildStream { .. }
            | ParquetScanPlan { .. } => StatusCode::Unexpected,
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
