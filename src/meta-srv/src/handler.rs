pub(crate) mod response_header;

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use common_telemetry::info;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

#[async_trait::async_trait]
pub trait HeartbeatHandler: Send + Sync {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        res: HeartbeatResponse,
        store: KvStoreRef,
    ) -> Result<HeartbeatResponse>;
}

pub type Pusher = Sender<std::result::Result<HeartbeatResponse, tonic::Status>>;

#[derive(Clone)]
pub struct HeartbeatHandlers {
    inner: Arc<RwLock<Inner>>,
}

impl HeartbeatHandlers {
    pub fn new(kv_store: KvStoreRef) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            kv_store,
            handlers: Default::default(),
            pushers: Default::default(),
        }));

        Self { inner }
    }

    pub async fn add_handler(&self, handler: impl HeartbeatHandler + 'static) {
        let mut inner = self.inner.write().await;
        inner.add_handler(handler);
    }

    pub async fn register(&self, key: impl Into<String>, pusher: Pusher) {
        let mut inner = self.inner.write().await;
        let key = key.into();
        info!("Pusher register: {}", &key);
        inner.register(key, pusher);
    }

    pub async fn unregister(&self, key: impl AsRef<str>) -> Option<Pusher> {
        let mut inner = self.inner.write().await;
        info!("Pusher unregister: {}", key.as_ref());
        inner.unregister(key)
    }

    pub async fn handle(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let inner = self.inner.read().await;
        inner.handle(req).await
    }
}

struct Inner {
    kv_store: KvStoreRef,
    handlers: Vec<Box<dyn HeartbeatHandler>>,
    pushers: HashMap<String, Pusher>,
}

impl Inner {
    fn add_handler(&mut self, handler: impl HeartbeatHandler + 'static) {
        self.handlers.push(Box::new(handler));
    }

    fn register(&mut self, key: impl Into<String>, pusher: Pusher) {
        self.pushers.insert(key.into(), pusher);
    }

    fn unregister(&mut self, key: impl AsRef<str>) -> Option<Pusher> {
        self.pushers.remove(key.as_ref())
    }

    async fn handle(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let mut res = HeartbeatResponse::default();
        for h in &self.handlers {
            res = h.handle(&req, res, self.kv_store.clone()).await?;
        }
        Ok(res)
    }
}
