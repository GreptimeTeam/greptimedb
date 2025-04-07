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
