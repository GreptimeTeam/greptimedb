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
mod stream_insert;

pub use api;
use api::v1::greptime_response::Response;
use api::v1::{AffectedRows, GreptimeResponse, ResponseHeader, Status};
pub use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::status_code::StatusCode;

pub use self::client::Client;
pub use self::database::Database;
pub use self::error::{Error, Result};
pub use self::stream_insert::StreamInserter;
use crate::error::{IllegalDatabaseResponseSnafu, ServerSnafu};

pub fn parse_grpc_response(response: GreptimeResponse) -> Result<u32> {
    match (response.header, response.response) {
        (Some(_), Some(Response::AffectedRows(AffectedRows { value }))) => Ok(value),
        (
            Some(ResponseHeader {
                status:
                    Some(Status {
                        status_code,
                        err_msg,
                    }),
            }),
            None,
        ) => ServerSnafu {
            code: StatusCode::from(status_code),
            msg: err_msg,
        }
        .fail(),
        (header, res) => IllegalDatabaseResponseSnafu {
            err_msg: format!("unexpected header: {:?} or response: {:?}", header, res),
        }
        .fail(),
    }
}
