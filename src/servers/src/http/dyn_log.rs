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

use axum::http::StatusCode;
use axum::response::IntoResponse;
use common_telemetry::info;
use common_telemetry::tracing_subscriber::filter::{self, Targets};
use common_telemetry::tracing_subscriber::reload::Handle;
use common_telemetry::tracing_subscriber::Registry;
use once_cell::sync::OnceCell;
use snafu::OptionExt;

use crate::error::{InternalSnafu, InvalidParameterSnafu, Result};

pub static RELOAD_HANDLE: OnceCell<Handle<Targets, Registry>> = OnceCell::new();

#[axum_macros::debug_handler]
pub async fn dyn_log_handler(level: String) -> Result<impl IntoResponse> {
    let new_filter = level.parse::<filter::Targets>().map_err(|e| {
        InvalidParameterSnafu {
            reason: format!("Invalid filter: {e:?}"),
        }
        .build()
    })?;
    let mut old_filter = None;
    RELOAD_HANDLE
        .get()
        .context(InternalSnafu {
            err_msg: "Reload handler not initialized",
        })?
        .modify(|filter| {
            old_filter = Some(filter.clone());
            *filter = new_filter.clone()
        })
        .map_err(|e| {
            InternalSnafu {
                err_msg: format!("Fail to modify filter: {e:?}"),
            }
            .build()
        })?;
    let change_note = format!(
        "Log Level changed from {:?} to {:?}",
        old_filter.map(|f| f.to_string()),
        new_filter.to_string()
    );
    info!("{}", change_note.clone());
    Ok((StatusCode::OK, change_note))
}
