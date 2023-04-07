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

use common_error::prelude::*;
use snafu::Location;
use table::metadata::{TableInfoBuilderError, TableMetaBuilderError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to drop table, table :{}, source: {}", table_name, source,))]
    DropTable {
        source: BoxedError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to create table manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    CreateTableManifest {
        source: BoxedError,
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to delete table table manifest, source: {}", source,))]
    DeleteTableManifest {
        source: storage::error::Error,
        location: Location,
    },

    #[snafu(display(
        "Failed to recover table manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    RecoverTableManifest {
        source: BoxedError,
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
        #[snafu(backtrace)]
        source: table::metadata::ConvertError,
    },

    #[snafu(display("Invalid schema, source: {}", source))]
    InvalidRawSchema { source: datatypes::error::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            TableExists { .. }
            | BuildTableMeta { .. }
            | BuildTableInfo { .. }
            | InvalidRawSchema { .. } => StatusCode::InvalidArguments,

            CreateTableManifest { .. }
            | DeleteTableManifest { .. }
            | RecoverTableManifest { .. } => StatusCode::StorageUnavailable,

            ConvertRaw { .. } | DropTable { .. } => StatusCode::Unexpected,
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
