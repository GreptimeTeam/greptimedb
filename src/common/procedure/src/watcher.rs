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

use snafu::ResultExt;
use tokio::sync::watch::Receiver;

use crate::error::{ProcedureExecSnafu, Result, WaitWatcherSnafu};
use crate::procedure::ProcedureState;

/// Watcher to watch procedure state.
pub type Watcher = Receiver<ProcedureState>;

/// Wait the [Watcher] until the [ProcedureState] is done.
pub async fn wait(watcher: &mut Watcher) -> Result<()> {
    loop {
        watcher.changed().await.context(WaitWatcherSnafu)?;
        match &*watcher.borrow() {
            ProcedureState::Running => (),
            ProcedureState::Done => {
                return Ok(());
            }
            ProcedureState::Failed { error } => {
                return Err(error.clone()).context(ProcedureExecSnafu);
            }
            ProcedureState::Retry { error } => {
                return Err(error.clone()).context(ProcedureExecSnafu);
            }
        }
    }
}
