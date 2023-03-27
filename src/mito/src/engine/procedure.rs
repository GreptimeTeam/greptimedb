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

mod alter;
mod create;

use std::sync::Arc;

pub(crate) use alter::AlterMitoTable;
use common_procedure::ProcedureManager;
pub(crate) use create::CreateMitoTable;
use store_api::storage::StorageEngine;

use crate::engine::MitoEngineInner;

/// Register all procedure loaders to the procedure manager.
///
/// # Panics
/// Panics on error.
pub(crate) fn register_procedure_loaders<S: StorageEngine>(
    engine_inner: Arc<MitoEngineInner<S>>,
    procedure_manager: &dyn ProcedureManager,
) {
    // The procedure names are expected to be unique, so we just panic on error.
    CreateMitoTable::register_loader(engine_inner, procedure_manager);
}

#[cfg(test)]
mod procedure_test_util {
    use async_trait::async_trait;
    use common_procedure::{
        BoxedProcedure, Context, ContextProvider, ProcedureId, ProcedureState, Result, Status,
    };
    use common_test_util::temp_dir::TempDir;
    use log_store::NoopLogStore;
    use storage::compaction::noop::NoopCompactionScheduler;
    use storage::config::EngineConfig as StorageEngineConfig;
    use storage::EngineImpl;

    use super::*;
    use crate::engine::{EngineConfig, MitoEngine};
    use crate::table::test_util;

    struct MockContextProvider {}

    #[async_trait]
    impl ContextProvider for MockContextProvider {
        async fn procedure_state(
            &self,
            _procedure_id: ProcedureId,
        ) -> Result<Option<ProcedureState>> {
            Ok(Some(ProcedureState::Done))
        }
    }

    pub struct TestEnv {
        pub table_engine: MitoEngine<EngineImpl<NoopLogStore>>,
        pub dir: TempDir,
    }

    pub async fn setup_test_engine(path: &str) -> TestEnv {
        let (dir, object_store) = test_util::new_test_object_store(path).await;
        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        let storage_engine = EngineImpl::new(
            StorageEngineConfig::default(),
            Arc::new(NoopLogStore::default()),
            object_store.clone(),
            compaction_scheduler,
        );
        let table_engine = MitoEngine::new(EngineConfig::default(), storage_engine, object_store);

        TestEnv { table_engine, dir }
    }

    pub async fn execute_procedure_until_done(procedure: &mut BoxedProcedure) {
        let ctx = Context {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider {}),
        };

        loop {
            if let Status::Done = procedure.execute(&ctx).await.unwrap() {
                break;
            }
        }
    }
}
