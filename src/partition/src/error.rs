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
use datafusion_expr::Expr;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to get cache, error: {}", err_msg))]
    GetCache {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to request Meta, source: {}", source))]
    RequestMeta {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to find table routes for table {}", table_name))]
    FindTableRoutes {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to serialize value to json, source: {}", source))]
    SerializeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to deserialize value from json, source: {}", source))]
    DeserializeJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Expect {} region keys, actual {}", expect, actual))]
    RegionKeysSize {
        expect: usize,
        actual: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find region, reason: {}", reason))]
    FindRegion {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find regions by filters: {:?}", filters))]
    FindRegions {
        filters: Vec<Expr>,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find partition column: {}", column_name))]
    FindPartitionColumn {
        column_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid InsertRequest, reason: {}", reason))]
    InvalidInsertRequest {
        reason: String,
        backtrace: Backtrace,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::GetCache { .. } => StatusCode::StorageUnavailable,
            Error::RequestMeta { source, .. } => source.status_code(),
            Error::FindRegion { .. }
            | Error::FindRegions { .. }
            | Error::RegionKeysSize { .. }
            | Error::FindTableRoutes { .. }
            | Error::InvalidInsertRequest { .. }
            | Error::FindPartitionColumn { .. } => StatusCode::InvalidArguments,
            Error::SerializeJson { .. } | Error::DeserializeJson { .. } => StatusCode::Internal,
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
