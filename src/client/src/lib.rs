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

mod client;
pub mod client_manager;
mod database;
pub mod error;
pub mod load_balance;
mod metrics;
pub mod region;
mod stream_insert;

pub use api;
use api::v1::greptime_response::Response;
use api::v1::{AffectedRows, GreptimeResponse};
pub use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::status_code::StatusCode;
pub use common_query::Output;
pub use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use snafu::OptionExt;

pub use self::client::Client;
pub use self::database::Database;
pub use self::error::{Error, Result};
pub use self::stream_insert::StreamInserter;
use crate::error::{IllegalDatabaseResponseSnafu, ServerSnafu};

pub fn from_grpc_response(response: GreptimeResponse) -> Result<u32> {
    let header = response.header.context(IllegalDatabaseResponseSnafu {
        err_msg: "missing header",
    })?;
    let status = header.status.context(IllegalDatabaseResponseSnafu {
        err_msg: "missing status",
    })?;

    if StatusCode::is_success(status.status_code) {
        let res = response.response.context(IllegalDatabaseResponseSnafu {
            err_msg: "missing response",
        })?;
        match res {
            Response::AffectedRows(AffectedRows { value }) => Ok(value),
        }
    } else {
        let status_code =
            StatusCode::from_u32(status.status_code).context(IllegalDatabaseResponseSnafu {
                err_msg: format!("invalid status: {:?}", status),
            })?;
        ServerSnafu {
            code: status_code,
            msg: status.err_msg,
        }
        .fail()
    }
}
