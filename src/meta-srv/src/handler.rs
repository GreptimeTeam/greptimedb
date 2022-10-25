pub(crate) mod response_header;

use std::collections::BTreeMap;
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
        ctx: &mut HeartbeatAccumulator,
        store: KvStoreRef,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct HeartbeatAccumulator {
    pub header: Option<ResponseHeader>,
    pub states: Vec<State>,
    pub instructions: Vec<Instruction>,
}

impl HeartbeatAccumulator {
    pub fn into_payload(self) -> Vec<Vec<u8>> {
        // TODO(jiachun): to HeartbeatResponse payload
        vec![]
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
    pushers: Arc<RwLock<BTreeMap<String, Pusher>>>,
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
        let mut acc = HeartbeatAccumulator::default();
        let handlers = self.handlers.read().await;
        for h in handlers.iter() {
            h.handle(&req, &mut acc, self.kv_store.clone()).await?;
        }
        let header = std::mem::take(&mut acc.header);
        let res = HeartbeatResponse {
            header,
            payload: acc.into_payload(),
        };
        Ok(res)
    }
}
