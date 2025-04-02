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

use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{HeartbeatResponse, MailboxMessage};
use common_meta::instruction::{
    DowngradeRegionReply, InstructionReply, SimpleReply, UpgradeRegionReply,
};
use common_meta::sequence::Sequence;
use common_time::util::current_time_millis;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
use crate::handler::{HeartbeatMailbox, Pusher, Pushers};
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
        channel: Channel,
        tx: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
    ) {
        let pusher_id = channel.pusher_id();
        let pusher = Pusher::new(tx);
        let _ = self.pushers.insert(pusher_id.string_key(), pusher).await;
    }

    pub fn mailbox(&self) -> &MailboxRef {
        &self.mailbox
    }
}

/// Sends a mock reply.
pub fn send_mock_reply(
    mailbox: MailboxRef,
    mut rx: MockHeartbeatReceiver,
    msg: impl Fn(u64) -> Result<MailboxMessage> + Send + 'static,
) {
    common_runtime::spawn_global(async move {
        while let Some(Ok(resp)) = rx.recv().await {
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox.on_recv(reply_id, msg(reply_id)).await.unwrap();
        }
    });
}

/// Generates a [InstructionReply::OpenRegion] reply.
pub fn new_open_region_reply(id: u64, result: bool, error: Option<String>) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::OpenRegion(SimpleReply { result, error }))
                .unwrap(),
        )),
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

/// Generates a [InstructionReply::DowngradeRegion] reply.
pub fn new_downgrade_region_reply(
    id: u64,
    last_entry_id: Option<u64>,
    exist: bool,
    error: Option<String>,
) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::DowngradeRegion(DowngradeRegionReply {
                last_entry_id,
                exists: exist,
                error,
            }))
            .unwrap(),
        )),
    }
}

/// Generates a [InstructionReply::UpgradeRegion] reply.
pub fn new_upgrade_region_reply(
    id: u64,
    ready: bool,
    exists: bool,
    error: Option<String>,
) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::UpgradeRegion(UpgradeRegionReply {
                ready,
                exists,
                error,
            }))
            .unwrap(),
        )),
    }
}
