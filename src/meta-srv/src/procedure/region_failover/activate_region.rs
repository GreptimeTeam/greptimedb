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

use std::collections::HashMap;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use async_trait::async_trait;
use common_meta::instruction::{Instruction, InstructionReply, OpenRegion, SimpleReply};
use common_meta::key::datanode_table::{DatanodeTableKey, RegionInfo};
use common_meta::peer::Peer;
use common_meta::RegionIdent;
use common_telemetry::{debug, info};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionNumber;

use super::update_metadata::UpdateRegionMetadata;
use super::{RegionFailoverContext, State};
use crate::error::{
    self, Error, Result, RetryLaterSnafu, SerializeToJsonSnafu, UnexpectedInstructionReplySnafu,
};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_failover::OPEN_REGION_MESSAGE_TIMEOUT;
use crate::service::mailbox::{Channel, MailboxReceiver};

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct ActivateRegion {
    candidate: Peer,
    // If the meta leader node dies during the execution of the procedure,
    // the new leader node needs to remark the failed region as "inactive"
    // to prevent it from renewing the lease.
    remark_inactive_region: bool,
    // An `None` option stands for uninitialized.
    region_storage_path: Option<String>,
    region_options: Option<HashMap<String, String>>,
    region_wal_options: Option<HashMap<RegionNumber, String>>,
}

impl ActivateRegion {
    pub(super) fn new(candidate: Peer) -> Self {
        Self {
            candidate,
            remark_inactive_region: false,
            region_storage_path: None,
            region_options: None,
            region_wal_options: None,
        }
    }

    async fn send_open_region_message(
        &mut self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
        timeout: Duration,
    ) -> Result<MailboxReceiver> {
        let table_id = failed_region.table_id;
        // Retrieves the wal options from failed datanode table value.
        let datanode_table_value = ctx
            .table_metadata_manager
            .datanode_table_manager()
            .get(&DatanodeTableKey::new(failed_region.datanode_id, table_id))
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::DatanodeTableNotFoundSnafu {
                table_id,
                datanode_id: failed_region.datanode_id,
            })?;

        let candidate_ident = RegionIdent {
            datanode_id: self.candidate.id,
            ..failed_region.clone()
        };
        info!("Activating region: {candidate_ident:?}");

        let RegionInfo {
            region_storage_path,
            region_options,
            region_wal_options,
            ..
        } = datanode_table_value.region_info;

        let instruction = Instruction::OpenRegion(OpenRegion::new(
            candidate_ident.clone(),
            &region_storage_path,
            region_options.clone(),
            region_wal_options.clone(),
            false,
        ));

        self.region_storage_path = Some(region_storage_path);
        self.region_options = Some(region_options);
        self.region_wal_options = Some(region_wal_options);

        let msg = MailboxMessage::json_message(
            "Activate Region",
            &format!("Metasrv@{}", ctx.selector_ctx.server_addr),
            &format!(
                "Datanode-(id={}, addr={})",
                self.candidate.id, self.candidate.addr
            ),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let ch = Channel::Datanode(self.candidate.id);
        ctx.mailbox.send(&ch, msg, timeout).await
    }

    async fn handle_response(
        &mut self,
        mailbox_receiver: MailboxReceiver,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        match mailbox_receiver.await? {
            Ok(msg) => {
                debug!("Received activate region reply: {msg:?}");

                let reply = HeartbeatMailbox::json_reply(&msg)?;
                let InstructionReply::OpenRegion(SimpleReply { result, error }) = reply else {
                    return UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect open region reply",
                    }
                    .fail();
                };
                if result {
                    Ok(Box::new(UpdateRegionMetadata::new(
                        self.candidate.clone(),
                        self.region_storage_path
                            .clone()
                            .context(error::UnexpectedSnafu {
                                violated: "expected region_storage_path",
                            })?,
                        self.region_options
                            .clone()
                            .context(error::UnexpectedSnafu {
                                violated: "expected region_options",
                            })?,
                        self.region_wal_options
                            .clone()
                            .context(error::UnexpectedSnafu {
                                violated: "expected region_wal_options",
                            })?,
                    )))
                } else {
                    // The region could be just indeed cannot be opened by the candidate, retry
                    // would be in vain. Then why not just end the failover procedure? Because we
                    // currently lack the methods or any maintenance tools to manage the whole
                    // procedures things, it would be easier to let the procedure keep running.
                    let reason = format!(
                        "Region {failed_region:?} is not opened by Datanode {:?}, error: {error:?}",
                        self.candidate,
                    );
                    RetryLaterSnafu { reason }.fail()
                }
            }
            Err(Error::MailboxTimeout { .. }) => {
                let reason = format!(
                    "Mailbox received timeout for activate failed region {failed_region:?} on Datanode {:?}", 
                    self.candidate,
                );
                RetryLaterSnafu { reason }.fail()
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
#[typetag::serde]
impl State for ActivateRegion {
    async fn next(
        &mut self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        let mailbox_receiver = self
            .send_open_region_message(ctx, failed_region, OPEN_REGION_MESSAGE_TIMEOUT)
            .await?;

        self.handle_response(mailbox_receiver, failed_region).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::mailbox_message::Payload;
    use common_meta::instruction::SimpleReply;

    use super::super::tests::TestingEnvBuilder;
    use super::*;

    #[tokio::test]
    async fn test_activate_region_success() {
        common_telemetry::init_default_ut_logging();

        let mut env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let candidate = 2;
        let mut state = ActivateRegion::new(Peer::new(candidate, ""));
        let mailbox_receiver = state
            .send_open_region_message(&env.context, &failed_region, Duration::from_millis(100))
            .await
            .unwrap();

        let message_id = mailbox_receiver.message_id();

        // verify that the open region message is sent
        let rx = env.heartbeat_receivers.get_mut(&candidate).unwrap();
        let resp = rx.recv().await.unwrap().unwrap();
        let received = &resp.mailbox_message.unwrap();
        assert_eq!(received.id, message_id);
        assert_eq!(received.subject, "Activate Region");
        assert_eq!(received.from, "Metasrv@127.0.0.1:3002");
        assert_eq!(received.to, "Datanode-(id=2, addr=)");
        assert_eq!(
            received.payload,
            Some(Payload::Json(
                serde_json::to_string(&Instruction::OpenRegion(OpenRegion::new(
                    RegionIdent {
                        datanode_id: candidate,
                        ..failed_region.clone()
                    },
                    &env.path,
                    HashMap::new(),
                    HashMap::new(),
                    false
                )))
                .unwrap(),
            ))
        );

        // simulating response from Datanode
        env.context
            .mailbox
            .on_recv(
                message_id,
                Ok(MailboxMessage {
                    id: message_id,
                    subject: "Activate Region".to_string(),
                    from: "Datanode-2".to_string(),
                    to: "Metasrv".to_string(),
                    timestamp_millis: common_time::util::current_time_millis(),
                    payload: Some(Payload::Json(
                        serde_json::to_string(&InstructionReply::OpenRegion(SimpleReply {
                            result: true,
                            error: None,
                        }))
                        .unwrap(),
                    )),
                }),
            )
            .await
            .unwrap();

        let next_state = state
            .handle_response(mailbox_receiver, &failed_region)
            .await
            .unwrap();
        assert_eq!(
            format!("{next_state:?}"),
            r#"UpdateRegionMetadata { candidate: Peer { id: 2, addr: "" }, region_storage_path: "greptime/public", region_options: {}, region_wal_options: {} }"#
        );
    }

    #[tokio::test]
    async fn test_activate_region_timeout() {
        common_telemetry::init_default_ut_logging();

        let mut env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let candidate = 2;
        let mut state = ActivateRegion::new(Peer::new(candidate, ""));
        let mailbox_receiver = state
            .send_open_region_message(&env.context, &failed_region, Duration::from_millis(100))
            .await
            .unwrap();

        // verify that the open region message is sent
        let rx = env.heartbeat_receivers.get_mut(&candidate).unwrap();
        let resp = rx.recv().await.unwrap().unwrap();
        let received = &resp.mailbox_message.unwrap();
        assert_eq!(received.id, mailbox_receiver.message_id());
        assert_eq!(received.subject, "Activate Region");
        assert_eq!(received.from, "Metasrv@127.0.0.1:3002");
        assert_eq!(received.to, "Datanode-(id=2, addr=)");
        assert_eq!(
            received.payload,
            Some(Payload::Json(
                serde_json::to_string(&Instruction::OpenRegion(OpenRegion::new(
                    RegionIdent {
                        datanode_id: candidate,
                        ..failed_region.clone()
                    },
                    &env.path,
                    HashMap::new(),
                    HashMap::new(),
                    false
                )))
                .unwrap(),
            ))
        );

        let result = state
            .handle_response(mailbox_receiver, &failed_region)
            .await;
        assert!(matches!(result, Err(Error::RetryLater { .. })));
    }
}
