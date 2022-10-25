pub(crate) mod response_header;

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::ResponseHeader;
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
        ctx: &mut Context,
        store: KvStoreRef,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct Context {
    pub header: Option<ResponseHeader>,
    pub states: Vec<State>,
    pub instructions: Vec<Instruction>,
}

impl From<Context> for HeartbeatResponse {
    fn from(ctx: Context) -> Self {
        Self {
            header: ctx.header,
            // TODO(jiachun): set other fields
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub enum State {}

#[derive(Debug)]
pub enum Instruction {}

pub type Pusher = Sender<std::result::Result<HeartbeatResponse, tonic::Status>>;

#[derive(Clone)]
pub struct HeartbeatHandlers {
    kv_store: KvStoreRef,
    handlers: Arc<RwLock<Vec<Box<dyn HeartbeatHandler>>>>,
    pushers: Arc<RwLock<HashMap<String, Pusher>>>,
}

impl HeartbeatHandlers {
    pub fn new(kv_store: KvStoreRef) -> Self {
        Self {
            kv_store,
            handlers: Arc::new(RwLock::new(Default::default())),
            pushers: Arc::new(RwLock::new(Default::default())),
        }
    }

    pub async fn add_handler(&self, handler: impl HeartbeatHandler + 'static) {
        let mut handlers = self.handlers.write().await;
        handlers.push(Box::new(handler));
    }

    pub async fn register(&self, key: impl AsRef<str>, pusher: Pusher) {
        let mut pushers = self.pushers.write().await;
        let key = key.as_ref();
        info!("Pusher register: {}", key);
        pushers.insert(key.into(), pusher);
    }

    pub async fn unregister(&self, key: impl AsRef<str>) -> Option<Pusher> {
        let mut pushers = self.pushers.write().await;
        let key = key.as_ref();
        info!("Pusher unregister: {}", key);
        pushers.remove(key)
    }

    pub async fn handle(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let mut ctx = Context::default();
        let handlers = self.handlers.read().await;
        for h in handlers.iter() {
            h.handle(&req, &mut ctx, self.kv_store.clone()).await?;
        }
        Ok(ctx.into())
    }
}
