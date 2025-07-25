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

use std::sync::Arc;

use axum::extract::{self, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_meta::sequence::SequenceRef;
use serde::{Deserialize, Serialize};
use servers::http::result::error_result::ErrorResponse;
use snafu::{ensure, ResultExt};

use crate::error::{Result, RuntimeSwitchManagerSnafu, SetNextSequenceSnafu, UnexpectedSnafu};

pub type TableIdSequenceHandlerRef = Arc<TableIdSequenceHandler>;

#[derive(Clone)]
pub(crate) struct TableIdSequenceHandler {
    pub(crate) table_id_sequence: SequenceRef,
    pub(crate) runtime_switch_manager: RuntimeSwitchManagerRef,
}

impl TableIdSequenceHandler {
    async fn set_next_table_id(&self, next_table_id: u32) -> Result<()> {
        ensure!(
            self.runtime_switch_manager
                .recovery_mode()
                .await
                .context(RuntimeSwitchManagerSnafu)?,
            UnexpectedSnafu {
                violated: "Setting next table id is only allowed in recovery mode",
            }
        );

        self.table_id_sequence
            .set(next_table_id as u64)
            .await
            .context(SetNextSequenceSnafu)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NextTableIdResponse {
    pub(crate) next_table_id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ResetTableIdRequest {
    pub(crate) next_table_id: u32,
}

/// Set the next table id.
#[axum_macros::debug_handler]
pub(crate) async fn set_next_table_id(
    State(handler): State<TableIdSequenceHandlerRef>,
    extract::Json(ResetTableIdRequest { next_table_id }): extract::Json<ResetTableIdRequest>,
) -> Response {
    match handler.set_next_table_id(next_table_id).await {
        Ok(_) => (StatusCode::OK, Json(NextTableIdResponse { next_table_id })).into_response(),
        Err(e) => ErrorResponse::from_error(e).into_response(),
    }
}

/// Get the next table id without incrementing the sequence.
#[axum_macros::debug_handler]
pub(crate) async fn get_next_table_id(
    State(handler): State<TableIdSequenceHandlerRef>,
) -> Response {
    let next_table_id = handler.table_id_sequence.peek().await as u32;

    (StatusCode::OK, Json(NextTableIdResponse { next_table_id })).into_response()
}
