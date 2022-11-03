pub(crate) mod datanode_lease;
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
        ctx: &Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct Context {
    pub server_addr: String, // also server_id
    pub kv_store: KvStoreRef,
}

impl Context {
    #[inline]
    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    #[inline]
    pub fn kv_store(&self) -> KvStoreRef {
        self.kv_store.clone()
    }
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

#[derive(Clone, Default)]
pub struct HeartbeatHandlers {
    handlers: Arc<RwLock<Vec<Box<dyn HeartbeatHandler>>>>,
    pushers: Arc<RwLock<BTreeMap<String, Pusher>>>,
}

impl HeartbeatHandlers {
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

    pub async fn handle(&self, req: HeartbeatRequest, ctx: Context) -> Result<HeartbeatResponse> {
        let mut acc = HeartbeatAccumulator::default();
        let handlers = self.handlers.read().await;
        for h in handlers.iter() {
            h.handle(&req, &ctx, &mut acc).await?;
        }
        let header = std::mem::take(&mut acc.header);
        let res = HeartbeatResponse {
            header,
            payload: acc.into_payload(),
        };
        Ok(res)
    }
}
