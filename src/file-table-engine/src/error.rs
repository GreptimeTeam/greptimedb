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
use serde_json::error::Error as JsonError;
use snafu::Location;
use table::metadata::{TableInfoBuilderError, TableMetaBuilderError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to encode object into json , source: {}", source))]
    EncodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Fail to decode object from json , source: {}", source))]
    DecodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Failed to write table metadata to manifest, source: {}", source,))]
    WriteTableManifest {
        source: storage::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to read table manifest,  source: {}", source,))]
    ReadTableManifest {
        source: storage::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to delete table table manifest, source: {}", source,))]
    DeleteTableManifest {
        source: object_store::Error,
        location: Location,
    },

    #[snafu(display("Failed to drop table, table :{}, source: {}", table_name, source,))]
    DropTable {
        source: BoxedError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to create table metadata to manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    CreateTableManifest {
        source: BoxedError,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to recover table metadata to manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    RecoverTableManifest {
        source: BoxedError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to build table meta for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableMeta {
        source: TableMetaBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build table info for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableInfo {
        source: TableInfoBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Table already exists: {}", table_name))]
    TableExists {
        backtrace: Backtrace,
        table_name: String,
    },

    #[snafu(display(
        "Failed to scan table metadata from manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    ScanTableManifest {
        #[snafu(backtrace)]
        source: storage::error::Error,
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
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            TableExists { .. } | BuildTableMeta { .. } | BuildTableInfo { .. } => {
                StatusCode::InvalidArguments
            }

            ScanTableManifest { .. }
            | ReadTableManifest { .. }
            | WriteTableManifest { .. }
            | CreateTableManifest { .. }
            | DeleteTableManifest { .. }
            | RecoverTableManifest { .. } => StatusCode::StorageUnavailable,

            ConvertRaw { .. } | EncodeJson { .. } | DecodeJson { .. } | DropTable { .. } => {
                StatusCode::Unexpected
            }
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
