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

use std::sync::Arc;

use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{HeartbeatResponse, MailboxMessage, RequestHeader};
use common_meta::instruction::{InstructionReply, SimpleReply};
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use common_meta::sequence::Sequence;
use common_meta::DatanodeId;
use common_procedure::{Context as ProcedureContext, ProcedureId};
use common_procedure_test::MockContextProvider;
use common_time::util::current_time_millis;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};

use super::ContextFactoryImpl;
use crate::error::Result;
use crate::handler::{HeartbeatMailbox, Pusher, Pushers};
use crate::procedure::region_migration::PersistentContext;
use crate::region::lease_keeper::{OpeningRegionKeeper, OpeningRegionKeeperRef};
use crate::service::mailbox::{Channel, MailboxRef};

pub type MockHeartbeatReceiver = Receiver<std::result::Result<HeartbeatResponse, tonic::Status>>;

/// The context of mailbox.
pub struct MailboxContext {
    mailbox: MailboxRef,
    // The pusher is used in the mailbox.
    pushers: Pushers,
}

impl MailboxContext {
    pub fn new(sequence: Sequence) -> Self {
        let pushers = Pushers::default();
        let mailbox = HeartbeatMailbox::create(pushers.clone(), sequence);

        Self { mailbox, pushers }
    }

    /// Inserts a pusher for `datanode_id`
    pub async fn insert_heartbeat_response_receiver(
        &mut self,
        datanode_id: DatanodeId,
        tx: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
    ) {
        let pusher_id = Channel::Datanode(datanode_id).pusher_id();
        let pusher = Pusher::new(tx, &RequestHeader::default());
        let _ = self.pushers.insert(pusher_id, pusher).await;
    }

    pub fn mailbox(&self) -> &MailboxRef {
        &self.mailbox
    }
}

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    table_metadata_manager: TableMetadataManagerRef,
    mailbox_ctx: MailboxContext,
    opening_region_keeper: OpeningRegionKeeperRef,
    server_addr: String,
}

impl TestingEnv {
    /// Returns an empty [TestingEnv].
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        let mailbox_sequence = Sequence::new("test_heartbeat_mailbox", 0, 1, kv_backend.clone());

        let mailbox_ctx = MailboxContext::new(mailbox_sequence);
        let opening_region_keeper = Arc::new(OpeningRegionKeeper::default());

        Self {
            table_metadata_manager,
            opening_region_keeper,
            mailbox_ctx,
            server_addr: "localhost".to_string(),
        }
    }

    /// Returns a context of region migration procedure.
    pub fn context_factory(&self) -> ContextFactoryImpl {
        ContextFactoryImpl {
            table_metadata_manager: self.table_metadata_manager.clone(),
            opening_region_keeper: self.opening_region_keeper.clone(),
            volatile_ctx: Default::default(),
            mailbox: self.mailbox_ctx.mailbox().clone(),
            server_addr: self.server_addr.to_string(),
        }
    }

    /// Returns the mutable [MailboxContext].
    pub fn mailbox_context(&mut self) -> &mut MailboxContext {
        &mut self.mailbox_ctx
    }

    /// Returns the [TableMetadataManagerRef]
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    /// Returns the [OpeningRegionKeeperRef]
    pub fn opening_region_keeper(&self) -> &OpeningRegionKeeperRef {
        &self.opening_region_keeper
    }

    /// Returns a [ProcedureContext] with a random [ProcedureId] and a [MockContextProvider].
    pub fn procedure_context() -> ProcedureContext {
        ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        }
    }
}

/// Generates a [InstructionReply::CloseRegion] reply.
pub fn new_close_region_reply(id: u64) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::CloseRegion(SimpleReply {
                result: false,
                error: None,
            }))
            .unwrap(),
        )),
    }
}

/// Sends a mock reply.
pub fn send_mock_reply(
    mailbox: MailboxRef,
    mut rx: MockHeartbeatReceiver,
    msg: impl FnOnce(u64) -> Result<MailboxMessage> + Send + 'static,
) {
    common_runtime::spawn_bg(async move {
        let resp = rx.recv().await.unwrap().unwrap();
        let reply_id = resp.mailbox_message.unwrap().id;
        mailbox.on_recv(reply_id, msg(reply_id)).await.unwrap();
    });
}

/// Generates a [PersistentContext].
pub fn new_persistent_context(from: u64, to: u64, region_id: RegionId) -> PersistentContext {
    PersistentContext {
        from_peer: Peer::empty(from),
        to_peer: Peer::empty(to),
        region_id,
        cluster_id: 0,
    }
}
