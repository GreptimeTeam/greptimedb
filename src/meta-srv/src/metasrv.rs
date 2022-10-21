use serde::Deserialize;
use serde::Serialize;

use crate::service::store::kv::KvStoreRef;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaSrvOptions {
    pub bind_addr: String,
    pub server_addr: String,
    pub store_addr: String,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:3002".to_string(),
            server_addr: "0.0.0.0:3002".to_string(),
            store_addr: "0.0.0.0:2380".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct MetaSrv {
    options: MetaSrvOptions,
    kv_store: KvStoreRef,
}

impl MetaSrv {
    pub fn new(options: MetaSrvOptions, kv_store: KvStoreRef) -> Self {
        Self { options, kv_store }
    }

    pub fn options(&self) -> &MetaSrvOptions {
        &self.options
    }

    pub fn kv_store(&self) -> KvStoreRef {
        self.kv_store.clone()
    }
}
