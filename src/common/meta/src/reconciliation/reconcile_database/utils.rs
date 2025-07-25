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

use common_procedure::{watcher, Context as ProcedureContext, ProcedureId};
use common_telemetry::error;
use futures::future::{join_all, try_join_all};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ProcedureStateReceiverNotFoundSnafu, ProcedureStateReceiverSnafu, Result, WaitProcedureSnafu,
};

/// Wait for inflight subprocedures.
///
/// If `fast_fail` is true, the function will return an error if any subprocedure fails.
/// Otherwise, the function will continue waiting for all subprocedures to complete.
pub(crate) async fn wait_for_inflight_subprocedures(
    procedure_ctx: &ProcedureContext,
    subprocedures: &[ProcedureId],
    fast_fail: bool,
) -> Result<()> {
    let mut receivers = Vec::with_capacity(subprocedures.len());
    for procedure_id in subprocedures {
        let receiver = procedure_ctx
            .provider
            .procedure_state_receiver(*procedure_id)
            .await
            .context(ProcedureStateReceiverSnafu {
                procedure_id: *procedure_id,
            })?
            .context(ProcedureStateReceiverNotFoundSnafu {
                procedure_id: *procedure_id,
            })?;
        receivers.push(receiver);
    }

    let mut tasks = Vec::with_capacity(receivers.len());
    for receiver in receivers.iter_mut() {
        let fut = watcher::wait(receiver);
        tasks.push(fut);
    }

    if fast_fail {
        try_join_all(tasks).await.context(WaitProcedureSnafu)?;
    } else {
        for result in join_all(tasks).await {
            if let Err(e) = result {
                error!(e; "inflight subprocedure failed: {}, procedure_id: {}", e, procedure_ctx.procedure_id);
            }
        }
    }

    Ok(())
}
