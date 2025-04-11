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
use common_meta::region_registry::{LeaderRegionRegistry, LeaderRegionRegistryRef};
use common_meta::sequence::SequenceBuilder;
use common_meta::state_store::KvStateStore;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::test_util::InMemoryPoisonStore;
use common_procedure::ProcedureManagerRef;

use crate::procedure::test_util::MailboxContext;

pub struct TestEnv {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub leader_region_registry: LeaderRegionRegistryRef,
    pub procedure_manager: ProcedureManagerRef,
    pub mailbox: MailboxContext,
    pub server_addr: String,
}

impl TestEnv {
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let leader_region_registry = Arc::new(LeaderRegionRegistry::new());
        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();

        let mailbox_ctx = MailboxContext::new(mailbox_sequence);

        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
        let poison_manager = Arc::new(InMemoryPoisonStore::default());
        let procedure_manager = Arc::new(LocalManager::new(
            ManagerConfig::default(),
            state_store,
            poison_manager,
        ));

        Self {
            table_metadata_manager,
            leader_region_registry,
            procedure_manager,
            mailbox: mailbox_ctx,
            server_addr: "localhost".to_string(),
        }
    }
}
