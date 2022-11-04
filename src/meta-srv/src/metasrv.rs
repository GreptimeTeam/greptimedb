use std::sync::Arc;

use api::v1::meta::Peer;
use serde::Deserialize;
use serde::Serialize;

use crate::handler::datanode_lease::DatanodeLeaseHandler;
use crate::handler::response_header::ResponseHeaderHandler;
use crate::handler::HeartbeatHandlerGroup;
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::Selector;
use crate::service::store::kv::KvStoreRef;

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
            store_addr: "0.0.0.0:2380".to_string(),
            datanode_lease_secs: 15,
        }
    }
}

#[derive(Clone)]
pub struct Context {
    pub datanode_lease_secs: i64,
    pub kv_store: KvStoreRef,
}

pub type SelectorRef = Arc<dyn Selector<Context = Context, Output = Vec<Peer>>>;

#[derive(Clone)]
pub struct MetaSrv {
    options: MetaSrvOptions,
    kv_store: KvStoreRef,
    selector: SelectorRef,
    handler_group: HeartbeatHandlerGroup,
}

impl MetaSrv {
    pub async fn new(
        options: MetaSrvOptions,
        kv_store: KvStoreRef,
        selector: Option<SelectorRef>,
    ) -> Self {
        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector {}));
        let handler_group = HeartbeatHandlerGroup::default();
        handler_group.add_handler(ResponseHeaderHandler).await;
        handler_group.add_handler(DatanodeLeaseHandler).await;

        Self {
            options,
            kv_store,
            selector,
            handler_group,
        }
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
    pub fn selector(&self) -> SelectorRef {
        self.selector.clone()
    }

    #[inline]
    pub fn handler_group(&self) -> HeartbeatHandlerGroup {
        self.handler_group.clone()
    }

    #[inline]
    pub fn new_ctx(&self) -> Context {
        let datanode_lease_secs = self.options().datanode_lease_secs;
        let kv_store = self.kv_store();
        Context {
            datanode_lease_secs,
            kv_store,
        }
    }
}
