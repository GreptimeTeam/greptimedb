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

use snafu::ensure;

use super::*;
use crate::error;
use crate::poison::PoisonManager;
use crate::procedure::PoisonKey;

/// A poison manager that uses an in-memory map to store the poison state.
#[derive(Debug, Default)]
pub struct InMemoryPoisonManager {
    map: Arc<RwLock<HashMap<String, ProcedureId>>>,
}

impl InMemoryPoisonManager {
    /// Create a new in-memory poison manager.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl PoisonManager for InMemoryPoisonManager {
    async fn set_poison(&self, token: &PoisonKey, procedure_id: ProcedureId) -> Result<()> {
        let mut map = self.map.write().unwrap();

        let key = token.to_string();
        match map.entry(key) {
            Entry::Vacant(v) => {
                v.insert(procedure_id);
            }
            Entry::Occupied(o) => {
                let procedure = o.get();
                ensure!(
                    procedure == &procedure_id,
                    error::UnexpectedSnafu {
                        err_msg: format!(
                            "The poison is already set by other procedure {}",
                            procedure
                        )
                    }
                );
            }
        }
        Ok(())
    }

    async fn delete_poison(&self, token: &PoisonKey, procedure_id: ProcedureId) -> Result<()> {
        let mut map = self.map.write().unwrap();
        let key = token.to_string();

        match map.entry(key) {
            Entry::Vacant(_) => {
                // do nothing
            }
            Entry::Occupied(o) => {
                let procedure = o.get();
                ensure!(
                    procedure == &procedure_id,
                    error::UnexpectedSnafu {
                        err_msg: format!("The poison is not set by the procedure {}", procedure)
                    }
                );

                o.remove();
            }
        }
        Ok(())
    }

    async fn get_poison(&self, key: &PoisonKey) -> Result<Option<ProcedureId>> {
        let map = self.map.read().unwrap();
        let key = key.to_string();
        Ok(map.get(&key).cloned())
    }
}
