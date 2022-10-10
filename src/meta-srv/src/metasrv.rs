use serde::Deserialize;
use serde::Serialize;

use crate::service::store::kv::KvStoreRef;

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
}

impl MetaSrv {
    pub fn new(kv_store: KvStoreRef) -> Self {
        Self { kv_store }
    }

    pub fn kv_store(&self) -> KvStoreRef {
        self.kv_store.clone()
    }
}
