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

use common_error::ext::ErrorExt;
use common_telemetry::{debug, error};
use pgwire::error::PgWireError;

use crate::postgres::types::PgErrorCode;

pub fn convert_err(e: impl ErrorExt) -> PgWireError {
    let status_code = e.status_code();
    if status_code.should_log_error() {
        let root_error = e.root_cause().unwrap_or(&e);
        error!(e; "Failed to handle postgres query, code: {}, error: {}", status_code, root_error.to_string());
    } else {
        debug!(
            "Failed to handle postgres query, code: {}, error: {:?}",
            status_code, e
        );
    }

    PgWireError::UserError(Box::new(
        PgErrorCode::from(status_code).to_err_info(e.output_msg()),
    ))
}
