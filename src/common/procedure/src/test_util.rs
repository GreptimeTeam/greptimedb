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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::lock::Mutex;
use snafu::ensure;
use tokio::sync::watch::{self, Receiver, Sender};

use super::*;
use crate::error;
use crate::store::poison_store::PoisonStore;

/// A poison store that uses an in-memory map to store the poison state.
#[derive(Debug, Default)]
pub struct InMemoryPoisonStore {
    map: Arc<RwLock<HashMap<String, String>>>,
}

impl InMemoryPoisonStore {
    /// Create a new in-memory poison manager.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl PoisonStore for InMemoryPoisonStore {
    async fn try_put_poison(&self, key: String, token: String) -> Result<()> {
        let mut map = self.map.write().unwrap();
        match map.entry(key) {
            Entry::Vacant(v) => {
                v.insert(token.to_string());
            }
            Entry::Occupied(o) => {
                let value = o.get();
                ensure!(
                    value == &token,
                    error::UnexpectedSnafu {
                        err_msg: format!("The poison is already set by other token {}", value)
                    }
                );
            }
        }
        Ok(())
    }

    async fn delete_poison(&self, key: String, token: String) -> Result<()> {
        let mut map = self.map.write().unwrap();
        match map.entry(key) {
            Entry::Vacant(_) => {
                // do nothing
            }
            Entry::Occupied(o) => {
                let value = o.get();
                ensure!(
                    value == &token,
                    error::UnexpectedSnafu {
                        err_msg: format!("The poison is not set by the token {}", value)
                    }
                );

                o.remove();
            }
        }
        Ok(())
    }

    async fn get_poison(&self, key: &str) -> Result<Option<String>> {
        let map = self.map.read().unwrap();
        let key = key.to_string();
        Ok(map.get(&key).cloned())
    }
}

type ProcedureMap = HashMap<
    ProcedureId,
    (
        Box<dyn Procedure>,
        Sender<ProcedureState>,
        Receiver<ProcedureState>,
    ),
>;

#[derive(Debug, Default)]
pub struct MockProcedureManager {
    pub procedures: Mutex<ProcedureMap>,
}

#[async_trait::async_trait]
impl ProcedureManager for MockProcedureManager {
    fn register_loader(&self, _name: &str, _loader: BoxedProcedureLoader) -> Result<()> {
        Ok(())
    }

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn submit(&self, procedure: ProcedureWithId) -> Result<Watcher> {
        let (state_sender, state_receiver) = watch::channel(ProcedureState::Running);
        self.procedures.lock().await.insert(
            procedure.id,
            (procedure.procedure, state_sender, state_receiver.clone()),
        );
        Ok(state_receiver)
    }

    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        let procedures = self.procedures.lock().await;
        if let Some((_, state_sender, _)) = procedures.get(&procedure_id) {
            Ok(Some(state_sender.borrow().clone()))
        } else {
            Ok(None)
        }
    }

    fn procedure_watcher(&self, _procedure_id: ProcedureId) -> Option<Watcher> {
        None
    }

    async fn list_procedures(&self) -> Result<Vec<ProcedureInfo>> {
        Ok(vec![])
    }
}

impl MockProcedureManager {
    pub async fn len(&self) -> usize {
        let procedures = self.procedures.lock().await;
        procedures.len()
    }

    pub async fn is_empty(&self) -> bool {
        let procedures = self.procedures.lock().await;
        procedures.is_empty()
    }

    pub async fn contains_procedure(&self, procedure_id: ProcedureId) -> bool {
        let procedures = self.procedures.lock().await;
        procedures.contains_key(&procedure_id)
    }

    pub async fn finish_procedure(&self, procedure_id: ProcedureId) -> Result<()> {
        let mut procedures = self.procedures.lock().await;
        assert!(procedures.contains_key(&procedure_id));
        let (_, state_sender, _) = procedures.remove(&procedure_id).unwrap();
        state_sender
            .send(ProcedureState::Done { output: None })
            .unwrap();
        Ok(())
    }

    pub async fn finish_k_procedures(&self, k: usize) -> Result<()> {
        let mut procedures = self.procedures.lock().await;
        let procedure_ids = procedures.keys().take(k).cloned().collect::<Vec<_>>();
        for procedure_id in procedure_ids {
            let (_, state_sender, _) = procedures.remove(&procedure_id).unwrap();
            state_sender
                .send(ProcedureState::Done { output: None })
                .unwrap();
        }
        Ok(())
    }
}
