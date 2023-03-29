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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::engine::TableEngineRef;

pub trait TableEngineManager: Send + Sync {
    fn engine(&self, name: &str) -> Option<TableEngineRef>;
    fn register_engine(&self, name: &str, engine: TableEngineRef);
    fn default(&self) -> TableEngineRef;
}
pub type TableEngineManagerRef = Arc<dyn TableEngineManager>;

/// Simple in-memory table engine manager
pub struct MemoryTableEngineManager {
    pub engines: RwLock<HashMap<String, TableEngineRef>>,
    pub default_token: String,
}

impl MemoryTableEngineManager {
    pub fn new(default: &str, engine: TableEngineRef) -> Self {
        let engines = RwLock::new(HashMap::new());
        let default_token = default.to_string();
        // it's safe to unwrap
        engines.write().unwrap().insert(default_token, engine);

        MemoryTableEngineManager {
            engines,
            default_token: default.to_string(),
        }
    }
}

impl TableEngineManager for MemoryTableEngineManager {
    fn engine(&self, name: &str) -> Option<TableEngineRef> {
        let engines = self.engines.read().unwrap();
        engines.get(name).cloned()
    }

    fn register_engine(&self, name: &str, engine: TableEngineRef) {
        self.engines
            .write()
            .unwrap()
            .insert(name.to_string(), engine);
    }

    fn default(&self) -> TableEngineRef {
        let engines = self.engines.read().unwrap();
        // it's safe to unwrap
        engines.get(&self.default_token).unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::TableEngine;
    use crate::test_util::MockTableEngine;

    #[test]
    fn test_table_engine_manager() {
        let table_engine = MockTableEngine::new();
        let table_engine_ref = Arc::new(table_engine);
        let table_engine_manager =
            MemoryTableEngineManager::new("mock_mito", table_engine_ref.clone());

        table_engine_manager.register_engine("yet_another", table_engine_ref.clone());

        assert_eq!(
            table_engine_manager.default().name(),
            table_engine_ref.name()
        );

        let got = table_engine_manager.engine("mock_mito");

        assert_eq!(got.unwrap().name(), table_engine_ref.name());

        let got = table_engine_manager.engine("yet_another");

        assert_eq!(got.unwrap().name(), table_engine_ref.name());

        let missing = table_engine_manager.engine("not_exists");

        assert!(missing.is_none());
    }
}
