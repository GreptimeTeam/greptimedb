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
use snafu::{Location, Snafu};
use store_api::storage::RegionNumber;
use table::metadata::{TableInfoBuilderError, TableMetaBuilderError, TableVersion};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
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

    #[snafu(display("Invalid primary key: {}", msg))]
    InvalidPrimaryKey { msg: String, location: Location },

    #[snafu(display("Missing timestamp index for table: {}", table_name))]
    MissingTimestampIndex {
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build row key descriptor for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildRowKeyDescriptor {
        source: store_api::storage::RowKeyDescriptorBuilderError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build column descriptor for table: {}, column: {}, source: {}",
        table_name,
        column_name,
        source,
    ))]
    BuildColumnDescriptor {
        source: store_api::storage::ColumnDescriptorBuilderError,
        table_name: String,
        column_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build column family descriptor for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildColumnFamilyDescriptor {
        source: store_api::storage::ColumnFamilyDescriptorBuilderError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build region descriptor for table: {}, region: {}, source: {}",
        table_name,
        region_name,
        source,
    ))]
    BuildRegionDescriptor {
        source: store_api::storage::RegionDescriptorBuilderError,
        table_name: String,
        region_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to update table metadata to manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    UpdateTableManifest {
        location: Location,
        source: storage::error::Error,
        table_name: String,
    },

    #[snafu(display(
        "Failed to scan table metadata from manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    ScanTableManifest {
        location: Location,
        source: storage::error::Error,
        table_name: String,
    },

    #[snafu(display("Table already exists: {}", table_name))]
    TableExists {
        location: Location,
        table_name: String,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound {
        location: Location,
        table_name: String,
    },

    #[snafu(display(
        "Projected column not found in region, column: {}",
        column_qualified_name
    ))]
    ProjectedColumnNotFound {
        location: Location,
        column_qualified_name: String,
    },

    #[snafu(display(
        "Failed to convert metadata from deserialized data, source: {}",
        source
    ))]
    ConvertRaw {
        location: Location,
        source: table::metadata::ConvertError,
    },

    #[snafu(display("Cannot find region, table: {}, region: {}", table, region))]
    RegionNotFound {
        table: String,
        region: RegionNumber,
        location: Location,
    },

    #[snafu(display("Invalid schema, source: {}", source))]
    InvalidRawSchema { source: datatypes::error::Error },

    #[snafu(display("Stale version found, expect: {}, current: {}", expect, current))]
    StaleVersion {
        expect: TableVersion,
        current: TableVersion,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            BuildRowKeyDescriptor { .. }
            | BuildColumnDescriptor { .. }
            | BuildColumnFamilyDescriptor { .. }
            | BuildTableMeta { .. }
            | BuildTableInfo { .. }
            | BuildRegionDescriptor { .. }
            | ProjectedColumnNotFound { .. }
            | InvalidPrimaryKey { .. }
            | MissingTimestampIndex { .. }
            | TableNotFound { .. }
            | InvalidRawSchema { .. }
            | StaleVersion { .. } => StatusCode::InvalidArguments,

            TableExists { .. } => StatusCode::TableAlreadyExists,

            ConvertRaw { .. } => StatusCode::Unexpected,

            ScanTableManifest { .. } | UpdateTableManifest { .. } => StatusCode::StorageUnavailable,
            RegionNotFound { .. } => StatusCode::Internal,
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
