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
use common_meta::kv_backend::ResettableKvBackendRef;
use common_meta::sequence::SequenceBuilder;
use common_meta::state_store::KvStateStore;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::ProcedureManagerRef;

use super::Context;
use crate::cache_invalidator::MetasrvCacheInvalidator;
use crate::cluster::MetaPeerClientBuilder;
use crate::metasrv::MetasrvInfo;
use crate::procedure::test_util::MailboxContext;

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    table_metadata_manager: TableMetadataManagerRef,
    mailbox_ctx: MailboxContext,
    server_addr: String,
    procedure_manager: ProcedureManagerRef,
    in_memory: ResettableKvBackendRef,
}

impl Default for TestingEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestingEnv {
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let in_memory = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();

        let mailbox_ctx = MailboxContext::new(mailbox_sequence);

        let state_store = Arc::new(KvStateStore::new(kv_backend));
        let procedure_manager = Arc::new(LocalManager::new(ManagerConfig::default(), state_store));

        Self {
            table_metadata_manager,
            mailbox_ctx,
            server_addr: "localhost".to_string(),
            procedure_manager,
            in_memory,
        }
    }

    pub fn new_context(&self) -> Context {
        Context {
            table_metadata_manager: self.table_metadata_manager.clone(),
            mailbox: self.mailbox_ctx.mailbox().clone(),
            server_addr: self.server_addr.clone(),
            cache_invalidator: Arc::new(MetasrvCacheInvalidator::new(
                self.mailbox_ctx.mailbox().clone(),
                MetasrvInfo {
                    server_addr: self.server_addr.to_string(),
                },
            )),
            meta_peer_client: MetaPeerClientBuilder::default()
                .election(None)
                .in_memory(self.in_memory.clone())
                .build()
                .map(Arc::new)
                // Safety: all required fields set at initialization
                .unwrap(),
        }
    }

    pub fn mailbox_context_mut(&mut self) -> &mut MailboxContext {
        &mut self.mailbox_ctx
    }

    pub fn procedure_manager(&self) -> ProcedureManagerRef {
        self.procedure_manager.clone()
    }
}
