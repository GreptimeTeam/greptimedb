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

use std::sync::Arc;

use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_procedure::{Context as ProcedureContext, ProcedureId};
use common_procedure_test::MockContextProvider;

use crate::procedure::region_migration::Context;

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    table_metadata_manager: TableMetadataManagerRef,
}

impl TestingEnv {
    /// Returns an empty [TestingEnv].
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        Self {
            table_metadata_manager,
        }
    }

    /// Returns a context of region migration procedure.
    pub fn context(&self) -> Context {
        Context {
            table_metadata_manager: self.table_metadata_manager.clone(),
        }
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    /// Returns a [ProcedureContext] with a random [ProcedureId] and a [MockContextProvider].
    pub fn procedure_context() -> ProcedureContext {
        ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        }
    }
}
