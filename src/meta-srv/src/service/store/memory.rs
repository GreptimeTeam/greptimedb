use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Mutex;

#[derive(Clone)]
pub struct MemStore {
    #[allow(dead_code)]
    inner: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Default::default())),
        }
    }
}
