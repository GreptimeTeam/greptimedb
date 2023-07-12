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

use api::v1::meta::{HeartbeatRequest, ResponseHeader, Role, PROTOCOL_VERSION};

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

#[derive(Default)]
pub struct ResponseHeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for ResponseHeaderHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        _ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        let HeartbeatRequest { header, .. } = req;
        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: header.as_ref().map_or(0, |h| h.cluster_id),
            ..Default::default()
        };
        acc.header = Some(res_header);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use api::v1::meta::{HeartbeatResponse, RequestHeader};
    use common_meta::key::TableMetadataManager;

    use super::*;
    use crate::cluster::MetaPeerClientBuilder;
    use crate::handler::{Context, HeartbeatMailbox, Pushers};
    use crate::sequence::Sequence;
    use crate::service::store::cached_kv::LeaderCachedKvStore;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_handle_heartbeat_resp_header() {
        let in_memory = Arc::new(MemStore::new());
        let kv_store = Arc::new(MemStore::new());
        let leader_cached_kv_store =
            Arc::new(LeaderCachedKvStore::with_always_leader(kv_store.clone()));
        let seq = Sequence::new("test_seq", 0, 10, kv_store.clone());
        let mailbox = HeartbeatMailbox::create(Pushers::default(), seq);
        let meta_peer_client = MetaPeerClientBuilder::default()
            .election(None)
            .in_memory(in_memory.clone())
            .build()
            .map(Arc::new)
            // Safety: all required fields set at initialization
            .unwrap();
        let mut ctx = Context {
            server_addr: "127.0.0.1:0000".to_string(),
            in_memory,
            kv_store: kv_store.clone(),
            leader_cached_kv_store,
            meta_peer_client,
            mailbox,
            election: None,
            skip_all: Arc::new(AtomicBool::new(false)),
            is_infancy: false,
            table_metadata_manager: Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
                kv_store.clone(),
            ))),
        };

        let req = HeartbeatRequest {
            header: Some(RequestHeader::new((1, 2), Role::Datanode)),
            ..Default::default()
        };
        let mut acc = HeartbeatAccumulator::default();

        let response_handler = ResponseHeaderHandler {};
        response_handler
            .handle(&req, &mut ctx, &mut acc)
            .await
            .unwrap();
        let header = std::mem::take(&mut acc.header);
        let res = HeartbeatResponse {
            header,
            mailbox_message: acc.into_mailbox_message(),
            ..Default::default()
        };
        assert_eq!(1, res.header.unwrap().cluster_id);
    }
}
