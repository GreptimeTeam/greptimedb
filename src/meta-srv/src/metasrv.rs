use serde::{Deserialize, Serialize};

use crate::service::{route::RouteRef, store::kv::KvStoreRef};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaSrvOptions {
    pub server_addr: String,
    pub store_addr: String,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            server_addr: "0.0.0.0:3002".to_string(),
            store_addr: "0.0.0.0:2380".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct MetaSrv {
    kv_store: KvStoreRef,
    route: RouteRef,
}

impl MetaSrv {
    pub fn new(kv_store: KvStoreRef, route: RouteRef) -> Self {
        Self { kv_store, route }
    }

    pub fn kv_store(&self) -> KvStoreRef {
        self.kv_store.clone()
    }

    pub fn route(&self) -> RouteRef {
        self.route.clone()
    }
}
