// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use check_leader_handler::CheckLeaderHandler;
pub use collect_stats_handler::CollectStatsHandler;
pub use failure_handler::RegionFailureHandler;
pub use keep_lease_handler::KeepLeaseHandler;
pub use on_leader_start::OnLeaderStartHandler;
pub use persist_stats_handler::PersistStatsHandler;
pub use response_header_handler::ResponseHeaderHandler;

mod check_leader_handler;
mod collect_stats_handler;
mod failure_handler;
mod instruction;
mod keep_lease_handler;
pub mod node_stat;
mod on_leader_start;
mod persist_stats_handler;
mod response_header_handler;

use std::collections::BTreeMap;
use std::sync::Arc;

use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, ResponseHeader};
use common_telemetry::info;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

use self::instruction::Instruction;
use self::node_stat::Stat;
use crate::error::Result;
use crate::metasrv::Context;

#[async_trait::async_trait]
pub trait HeartbeatHandler: Send + Sync {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct HeartbeatAccumulator {
    pub header: Option<ResponseHeader>,
    pub instructions: Vec<Instruction>,
    pub stat: Option<Stat>,
}

impl HeartbeatAccumulator {
    pub fn into_payload(self) -> Vec<Vec<u8>> {
        // TODO(jiachun): to HeartbeatResponse payload
        vec![]
    }
}

pub type Pusher = Sender<std::result::Result<HeartbeatResponse, tonic::Status>>;

#[derive(Clone, Default)]
pub struct HeartbeatHandlerGroup {
    handlers: Arc<RwLock<Vec<Box<dyn HeartbeatHandler>>>>,
    pushers: Arc<RwLock<BTreeMap<String, Pusher>>>,
}

impl HeartbeatHandlerGroup {
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

    pub async fn handle(
        &self,
        req: HeartbeatRequest,
        mut ctx: Context,
    ) -> Result<HeartbeatResponse> {
        let mut acc = HeartbeatAccumulator::default();
        let handlers = self.handlers.read().await;
        for h in handlers.iter() {
            h.handle(&req, &mut ctx, &mut acc).await?;
        }
        let header = std::mem::take(&mut acc.header);
        let res = HeartbeatResponse {
            header,
            payload: acc.into_payload(),
        };
        Ok(res)
    }
}
