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

use async_trait::async_trait;
use common_telemetry::error;
use snafu::{ensure, OptionExt};

use crate::engine::TableEngineRef;
use crate::error::{EngineExistSnafu, EngineNotFoundSnafu, Result};

#[async_trait::async_trait]
pub trait TableEngineManager: Send + Sync {
    /// returns `error::EngineNotFound` if engine not found
    fn engine(&self, name: &str) -> Result<TableEngineRef>;

    /// returns `error::EngineExist` if engine exists
    fn register_engine(&self, name: &str, engine: TableEngineRef) -> Result<()>;

    /// closes all registered engines
    async fn close(&self) -> Result<()>;
}
pub type TableEngineManagerRef = Arc<dyn TableEngineManager>;

/// Simple in-memory table engine manager
pub struct MemoryTableEngineManager {
    pub engines: RwLock<HashMap<String, TableEngineRef>>,
}

impl MemoryTableEngineManager {
    pub fn new(engine: TableEngineRef) -> Self {
        MemoryTableEngineManager::alias(engine.name().to_string(), engine)
    }

    pub fn alias(name: String, engine: TableEngineRef) -> Self {
        let mut engines = HashMap::new();
        engines.insert(name, engine);
        let engines = RwLock::new(engines);

        MemoryTableEngineManager { engines }
    }
}

#[async_trait]
impl TableEngineManager for MemoryTableEngineManager {
    fn engine(&self, name: &str) -> Result<TableEngineRef> {
        let engines = self.engines.read().unwrap();
        engines
            .get(name)
            .cloned()
            .with_context(|| EngineNotFoundSnafu {
                engine: name.to_string(),
            })
    }

    fn register_engine(&self, name: &str, engine: TableEngineRef) -> Result<()> {
        let mut engines = self.engines.write().unwrap();

        ensure!(
            !engines.contains_key(name),
            EngineExistSnafu {
                engine: name.to_string()
            }
        );

        engines.insert(name.to_string(), engine);

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let engines = {
            let engines = self.engines.write().unwrap();
            engines.values().cloned().collect::<Vec<_>>()
        };

        if let Err(err) =
            futures::future::try_join_all(engines.iter().map(|engine| engine.close())).await
        {
            error!("Failed to close engine: {}", err);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::engine::TableEngine;
    use crate::error;
    use crate::test_util::MockTableEngine;

    #[test]
    fn test_table_engine_manager() {
        let table_engine = MockTableEngine::new();
        let table_engine_ref = Arc::new(table_engine);
        let table_engine_manager = MemoryTableEngineManager::new(table_engine_ref.clone());

        table_engine_manager
            .register_engine("yet_another", table_engine_ref.clone())
            .unwrap();

        let got = table_engine_manager.engine(table_engine_ref.name());

        assert_eq!(got.unwrap().name(), table_engine_ref.name());

        let got = table_engine_manager.engine("yet_another");

        assert_eq!(got.unwrap().name(), table_engine_ref.name());

        let missing = table_engine_manager.engine("not_exists");

        assert_matches!(missing.err().unwrap(), error::Error::EngineNotFound { .. })
    }
}
