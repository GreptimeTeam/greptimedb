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
use common_error::prelude::{Snafu, StatusCode};
use snafu::{Backtrace, ErrorCompat, Location};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid catalog info: {}", key))]
    InvalidCatalog { key: String, location: Location },

    #[snafu(display("Failed to deserialize catalog entry value: {}", raw))]
    DeserializeCatalogEntryValue {
        raw: String,
        location: Location,
        source: serde_json::error::Error,
    },

    #[snafu(display("Failed to serialize catalog entry value"))]
    SerializeCatalogEntryValue {
        location: Location,
        source: serde_json::error::Error,
    },

    #[snafu(display("Failed to parse node id: {}", key))]
    ParseNodeId { key: String, location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidCatalog { .. }
            | Error::DeserializeCatalogEntryValue { .. }
            | Error::SerializeCatalogEntryValue { .. } => StatusCode::Unexpected,
            Error::ParseNodeId { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
