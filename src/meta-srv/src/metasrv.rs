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

pub mod builder;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use api::v1::meta::Peer;
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};

use crate::cluster::MetaPeerClient;
use crate::election::Election;
use crate::handler::HeartbeatHandlerGroup;
use crate::lock::DistLockRef;
use crate::selector::{Selector, SelectorType};
use crate::sequence::SequenceRef;
use crate::service::store::kv::{KvStoreRef, ResettableKvStoreRef};

pub const TABLE_ID_SEQ: &str = "table_id";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetaSrvOptions {
    pub bind_addr: String,
    pub server_addr: String,
    pub store_addr: String,
    pub datanode_lease_secs: i64,
    pub selector: SelectorType,
    pub use_memory_store: bool,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3002".to_string(),
            server_addr: "127.0.0.1:3002".to_string(),
            store_addr: "127.0.0.1:2379".to_string(),
            datanode_lease_secs: 15,
            selector: SelectorType::default(),
            use_memory_store: false,
        }
    }
}

#[derive(Clone)]
pub struct Context {
    pub datanode_lease_secs: i64,
    pub server_addr: String,
    pub in_memory: ResettableKvStoreRef,
    pub kv_store: KvStoreRef,
    pub election: Option<ElectionRef>,
    pub skip_all: Arc<AtomicBool>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
}

impl Context {
    pub fn is_skip_all(&self) -> bool {
        self.skip_all.load(Ordering::Relaxed)
    }

    pub fn set_skip_all(&self) {
        self.skip_all.store(true, Ordering::Relaxed);
    }

    pub fn reset_in_memory(&self) {
        self.in_memory.reset();
    }
}

pub struct LeaderValue(pub String);

pub type SelectorRef = Arc<dyn Selector<Context = Context, Output = Vec<Peer>>>;
pub type ElectionRef = Arc<dyn Election<Leader = LeaderValue>>;

#[derive(Clone)]
pub struct MetaSrv {
    started: Arc<AtomicBool>,
    options: MetaSrvOptions,
    // It is only valid at the leader node and is used to temporarily
    // store some data that will not be persisted.
    in_memory: ResettableKvStoreRef,
    kv_store: KvStoreRef,
    table_id_sequence: SequenceRef,
    selector: SelectorRef,
    handler_group: HeartbeatHandlerGroup,
    election: Option<ElectionRef>,
    meta_peer_client: Option<MetaPeerClient>,
    lock: Option<DistLockRef>,
}

impl MetaSrv {
    pub async fn start(&self) {
        if self
            .started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            warn!("MetaSrv already started");
            return;
        }

        if let Some(election) = self.election() {
            let election = election.clone();
            let started = self.started.clone();
            common_runtime::spawn_bg(async move {
                while started.load(Ordering::Relaxed) {
                    let res = election.campaign().await;
                    if let Err(e) = res {
                        warn!("MetaSrv election error: {}", e);
                    }
                    info!("MetaSrv re-initiate election");
                }
                info!("MetaSrv stopped");
            });
        }

        info!("MetaSrv started");
    }

    pub fn shutdown(&self) {
        self.started.store(false, Ordering::Relaxed);
    }

    #[inline]
    pub fn options(&self) -> &MetaSrvOptions {
        &self.options
    }

    #[inline]
    pub fn in_memory(&self) -> ResettableKvStoreRef {
        self.in_memory.clone()
    }

    #[inline]
    pub fn kv_store(&self) -> KvStoreRef {
        self.kv_store.clone()
    }

    #[inline]
    pub fn table_id_sequence(&self) -> SequenceRef {
        self.table_id_sequence.clone()
    }

    #[inline]
    pub fn selector(&self) -> SelectorRef {
        self.selector.clone()
    }

    #[inline]
    pub fn handler_group(&self) -> HeartbeatHandlerGroup {
        self.handler_group.clone()
    }

    #[inline]
    pub fn election(&self) -> Option<ElectionRef> {
        self.election.clone()
    }

    #[inline]
    pub fn meta_peer_client(&self) -> Option<MetaPeerClient> {
        self.meta_peer_client.clone()
    }

    #[inline]
    pub fn lock(&self) -> Option<DistLockRef> {
        self.lock.clone()
    }

    #[inline]
    pub fn new_ctx(&self) -> Context {
        let datanode_lease_secs = self.options().datanode_lease_secs;
        let server_addr = self.options().server_addr.clone();
        let in_memory = self.in_memory();
        let kv_store = self.kv_store();
        let election = self.election();
        let skip_all = Arc::new(AtomicBool::new(false));
        Context {
            datanode_lease_secs,
            server_addr,
            in_memory,
            kv_store,
            election,
            skip_all,
            catalog: None,
            schema: None,
            table: None,
        }
    }
}
