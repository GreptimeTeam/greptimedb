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

use common_telemetry::debug;
use snafu::ResultExt;
use tokio::sync::watch::Receiver;

use crate::error::{ProcedureExecSnafu, Result, WaitWatcherSnafu};
use crate::procedure::{Output, ProcedureState};

/// Watcher to watch procedure state.
pub type Watcher = Receiver<ProcedureState>;

/// Wait the [Watcher] until the [ProcedureState] is done.
pub async fn wait(watcher: &mut Watcher) -> Result<Option<Output>> {
    loop {
        watcher.changed().await.context(WaitWatcherSnafu)?;
        match &*watcher.borrow() {
            ProcedureState::Running => (),
            ProcedureState::Done { output } => {
                return Ok(output.clone());
            }
            ProcedureState::Failed { error } => {
                return Err(error.clone()).context(ProcedureExecSnafu);
            }
            ProcedureState::Retrying { error } => {
                debug!("retrying, source: {}", error)
            }
            ProcedureState::RollingBack { error } => {
                debug!("rolling back, source: {:?}", error)
            }
            ProcedureState::PrepareRollback { error } => {
                debug!("commit rollback, source: {}", error)
            }
            ProcedureState::Poisoned { error, .. } => {
                debug!("poisoned, source: {}", error);
                return Err(error.clone()).context(ProcedureExecSnafu);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_test_util::temp_dir::create_temp_dir;

    use super::*;
    use crate::error::Error;
    use crate::local::{test_util, LocalManager, ManagerConfig};
    use crate::procedure::PoisonKeys;
    use crate::store::state_store::ObjectStateStore;
    use crate::test_util::InMemoryPoisonStore;
    use crate::{
        Context, LockKey, Procedure, ProcedureId, ProcedureManager, ProcedureWithId, Status,
    };

    #[tokio::test]
    async fn test_success_after_retry() {
        let dir = create_temp_dir("after_retry");
        let config = ManagerConfig {
            parent_path: "data/".to_string(),
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
            ..Default::default()
        };
        let state_store = Arc::new(ObjectStateStore::new(test_util::new_object_store(&dir)));
        let poison_manager = Arc::new(InMemoryPoisonStore::default());
        let manager = LocalManager::new(config, state_store, poison_manager);
        manager.start().await.unwrap();

        #[derive(Debug)]
        struct MockProcedure {
            error: bool,
        }

        #[async_trait]
        impl Procedure for MockProcedure {
            fn type_name(&self) -> &str {
                "MockProcedure"
            }

            async fn execute(&mut self, _ctx: &Context) -> Result<Status> {
                if self.error {
                    self.error = !self.error;
                    Err(Error::retry_later(MockError::new(StatusCode::Internal)))
                } else {
                    Ok(Status::done_with_output("hello"))
                }
            }

            fn dump(&self) -> Result<String> {
                Ok(String::new())
            }

            fn lock_key(&self) -> LockKey {
                LockKey::single_exclusive("test.submit")
            }

            fn poison_keys(&self) -> PoisonKeys {
                PoisonKeys::default()
            }
        }

        let procedure_id = ProcedureId::random();
        let mut watcher = manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(MockProcedure { error: true }),
            })
            .await
            .unwrap();

        let output = wait(&mut watcher).await.unwrap().unwrap();
        let output = output.downcast::<&str>().unwrap();
        assert_eq!(output.as_ref(), &"hello");
    }
}
