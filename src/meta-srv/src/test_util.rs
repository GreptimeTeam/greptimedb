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

use common_procedure::local::{LocalManager, ManagerConfig};

use crate::handler::{HeartbeatMailbox, Pushers};
use crate::lock::memory::MemLock;
use crate::metasrv::SelectorContext;
use crate::procedure::region_failover::RegionFailoverManager;
use crate::procedure::state_store::MetaStateStore;
use crate::selector::lease_based::LeaseBasedSelector;
use crate::sequence::Sequence;
use crate::service::store::memory::MemStore;

pub(crate) fn create_region_failover_manager() -> Arc<RegionFailoverManager> {
    let kv_store = Arc::new(MemStore::new());

    let pushers = Pushers::default();
    let mailbox_sequence = Sequence::new("test_heartbeat_mailbox", 0, 100, kv_store.clone());
    let mailbox = HeartbeatMailbox::create(pushers, mailbox_sequence);

    let state_store = Arc::new(MetaStateStore::new(kv_store.clone()));
    let procedure_manager = Arc::new(LocalManager::new(ManagerConfig::default(), state_store));

    let selector = Arc::new(LeaseBasedSelector);
    let selector_ctx = SelectorContext {
        datanode_lease_secs: 10,
        server_addr: "127.0.0.1:3002".to_string(),
        kv_store,
        catalog: None,
        schema: None,
    };

    Arc::new(RegionFailoverManager::new(
        mailbox,
        procedure_manager,
        selector,
        selector_ctx,
        Arc::new(MemLock::default()),
    ))
}
