use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use api::v1::meta::Peer;
use common_telemetry::info;
use common_telemetry::warn;
use serde::Deserialize;
use serde::Serialize;

use crate::election::Election;
use crate::handler::check_leader::CheckLeaderHandler;
use crate::handler::datanode_lease::DatanodeLeaseHandler;
use crate::handler::response_header::ResponseHeaderHandler;
use crate::handler::HeartbeatHandlerGroup;
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::Selector;
use crate::sequence::Sequence;
use crate::sequence::SequenceRef;
use crate::service::store::kv::KvStoreRef;

pub const TABLE_ID_SEQ: &str = "table_id";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaSrvOptions {
    pub bind_addr: String,
    pub server_addr: String,
    pub store_addr: String,
    pub datanode_lease_secs: i64,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:3002".to_string(),
            server_addr: "0.0.0.0:3002".to_string(),
            store_addr: "0.0.0.0:2379".to_string(),
            datanode_lease_secs: 15,
        }
    }
}

#[derive(Clone)]
pub struct Context {
    pub datanode_lease_secs: i64,
    pub server_addr: String,
    pub kv_store: KvStoreRef,
    pub election: Option<ElectionRef>,
    pub skip_all: Arc<AtomicBool>,
}

impl Context {
    pub fn is_skip_all(&self) -> bool {
        self.skip_all.load(Ordering::Relaxed)
    }

    pub fn set_skip_all(&self) {
        self.skip_all.store(true, Ordering::Relaxed);
    }
}

pub struct LeaderValue(pub String);

pub type SelectorRef = Arc<dyn Selector<Context = Context, Output = Vec<Peer>>>;
pub type ElectionRef = Arc<dyn Election<Leader = LeaderValue>>;

#[derive(Clone)]
pub struct MetaSrv {
    started: Arc<AtomicBool>,
    options: MetaSrvOptions,
    kv_store: KvStoreRef,
    table_id_sequence: SequenceRef,
    selector: SelectorRef,
    handler_group: HeartbeatHandlerGroup,
    election: Option<ElectionRef>,
}

impl MetaSrv {
    pub async fn new(
        options: MetaSrvOptions,
        kv_store: KvStoreRef,
        selector: Option<SelectorRef>,
        election: Option<ElectionRef>,
    ) -> Self {
        let started = Arc::new(AtomicBool::new(false));
        let table_id_sequence = Arc::new(Sequence::new(TABLE_ID_SEQ, 10, kv_store.clone()));
        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector {}));
        let handler_group = HeartbeatHandlerGroup::default();
        handler_group.add_handler(ResponseHeaderHandler).await;
        handler_group.add_handler(CheckLeaderHandler).await;
        handler_group.add_handler(DatanodeLeaseHandler).await;

        Self {
            started,
            options,
            kv_store,
            table_id_sequence,
            selector,
            handler_group,
            election,
        }
    }

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
    pub fn new_ctx(&self) -> Context {
        let datanode_lease_secs = self.options().datanode_lease_secs;
        let server_addr = self.options().server_addr.clone();
        let kv_store = self.kv_store();
        let election = self.election();
        let skip_all = Arc::new(AtomicBool::new(false));
        Context {
            datanode_lease_secs,
            server_addr,
            kv_store,
            election,
            skip_all,
        }
    }
}
