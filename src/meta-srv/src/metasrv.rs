use serde::Deserialize;
use serde::Serialize;

use crate::handler::datanode_lease::DatanodeLeaseHandler;
use crate::handler::response_header::ResponseHeaderHandler;
use crate::handler::HeartbeatHandlers;
use crate::service::store::kv::KvStoreRef;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaSrvOptions {
    pub bind_addr: String,
    pub server_addr: String,
    pub store_addr: String,
    pub dn_lease_secs: i64,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:3002".to_string(),
            server_addr: "0.0.0.0:3002".to_string(),
            store_addr: "0.0.0.0:2380".to_string(),
            dn_lease_secs: 15,
        }
    }
}

#[derive(Clone)]
pub struct MetaSrv {
    options: MetaSrvOptions,
    kv_store: KvStoreRef,
    heartbeat_handlers: HeartbeatHandlers,
}

impl MetaSrv {
    pub async fn new(options: MetaSrvOptions, kv_store: KvStoreRef) -> Self {
        let heartbeat_handlers = HeartbeatHandlers::default();
        heartbeat_handlers.add_handler(ResponseHeaderHandler).await;
        heartbeat_handlers.add_handler(DatanodeLeaseHandler).await;

        Self {
            options,
            kv_store,
            heartbeat_handlers,
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
    pub fn heartbeat_handlers(&self) -> HeartbeatHandlers {
        self.heartbeat_handlers.clone()
    }
}
