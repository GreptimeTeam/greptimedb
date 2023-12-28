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

use std::time::Duration;

use api::v1::meta::MailboxMessage;
use async_trait::async_trait;
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionStatus;
use common_meta::RegionIdent;
use common_telemetry::{debug, info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use super::activate_region::ActivateRegion;
use super::{RegionFailoverContext, State};
use crate::error::{
    self, Error, Result, RetryLaterSnafu, SerializeToJsonSnafu, UnexpectedInstructionReplySnafu,
};
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::{Channel, MailboxReceiver};

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct DeactivateRegion {
    candidate: Peer,
}

impl DeactivateRegion {
    pub(super) fn new(candidate: Peer) -> Self {
        Self { candidate }
    }

    async fn mark_leader_downgraded(
        &self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<()> {
        let table_id = failed_region.table_id;

        let table_route_value = ctx
            .table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        ctx.table_metadata_manager
            .update_leader_region_status(table_id, &table_route_value, |region| {
                if region.region.id.region_number() == failed_region.region_number {
                    Some(Some(RegionStatus::Downgraded))
                } else {
                    None
                }
            })
            .await
            .context(error::UpdateTableRouteSnafu)?;

        Ok(())
    }

    async fn send_close_region_message(
        &self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<MailboxReceiver> {
        let instruction = Instruction::CloseRegion(failed_region.clone());

        let msg = MailboxMessage::json_message(
            "Deactivate Region",
            &format!("Metasrv@{}", ctx.selector_ctx.server_addr),
            &format!("Datanode-{}", failed_region.datanode_id),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let ch = Channel::Datanode(failed_region.datanode_id);
        let timeout = Duration::from_secs(ctx.region_lease_secs);
        ctx.mailbox.send(&ch, msg, timeout).await
    }

    async fn handle_response(
        &self,
        _ctx: &RegionFailoverContext,
        mailbox_receiver: MailboxReceiver,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        match mailbox_receiver.await? {
            Ok(msg) => {
                debug!("Received deactivate region reply: {msg:?}");

                let reply = HeartbeatMailbox::json_reply(&msg)?;
                let InstructionReply::CloseRegion(SimpleReply { result, error }) = reply else {
                    return UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect close region reply",
                    }
                    .fail();
                };
                if result {
                    Ok(Box::new(ActivateRegion::new(self.candidate.clone())))
                } else {
                    // Under rare circumstances would a Datanode fail to close a Region.
                    // So simply retry.
                    let reason = format!(
                        "Region {failed_region:?} is not closed by Datanode {}, error: {error:?}",
                        failed_region.datanode_id,
                    );
                    RetryLaterSnafu { reason }.fail()
                }
            }
            Err(Error::MailboxTimeout { .. }) => {
                // We have configured the timeout to match the region lease timeout before making
                // the call and have disabled region lease renewal. Therefore, if a timeout error
                // occurs, it can be concluded that the region has been closed. With this information,
                // we can proceed confidently to the next step.
                Ok(Box::new(ActivateRegion::new(self.candidate.clone())))
            }
            Err(e) => Err(e),
        }
    }

    /// Sleep for `region_lease_expiry_seconds`, to make sure the region is closed (by its
    /// region alive keeper). This is critical for region not being opened in multiple Datanodes
    /// simultaneously.
    async fn wait_for_region_lease_expiry(&self, ctx: &RegionFailoverContext) {
        tokio::time::sleep(Duration::from_secs(ctx.region_lease_secs)).await;
    }
}

#[async_trait]
#[typetag::serde]
impl State for DeactivateRegion {
    async fn next(
        &mut self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        info!("Deactivating region: {failed_region:?}");
        self.mark_leader_downgraded(ctx, failed_region).await?;
        let result = self.send_close_region_message(ctx, failed_region).await;
        let mailbox_receiver = match result {
            Ok(mailbox_receiver) => mailbox_receiver,
            Err(Error::PusherNotFound { .. }) => {
                warn!(
                    "Datanode {} is not reachable, skip deactivating region {}, just wait for the region lease to expire",
                    failed_region.datanode_id, failed_region
                );
                // See the mailbox received timeout situation comments above.
                self.wait_for_region_lease_expiry(ctx).await;
                return Ok(Box::new(ActivateRegion::new(self.candidate.clone())));
            }
            Err(e) => return Err(e),
        };

        self.handle_response(ctx, mailbox_receiver, failed_region)
            .await
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::mailbox_message::Payload;
    use common_meta::instruction::SimpleReply;

    use super::super::tests::TestingEnvBuilder;
    use super::*;

    #[tokio::test]
    async fn test_mark_leader_downgraded() {
        common_telemetry::init_default_ut_logging();

        let env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let state = DeactivateRegion::new(Peer::new(2, ""));

        state
            .mark_leader_downgraded(&env.context, &failed_region)
            .await
            .unwrap();

        let table_id = failed_region.table_id;

        let table_route_value = env
            .context
            .table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        let should_downgraded = table_route_value
            .region_routes()
            .unwrap()
            .iter()
            .find(|route| route.region.id.region_number() == failed_region.region_number)
            .unwrap();

        assert!(should_downgraded.is_leader_downgraded());
    }

    #[tokio::test]
    async fn test_deactivate_region_success() {
        common_telemetry::init_default_ut_logging();

        let mut env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let state = DeactivateRegion::new(Peer::new(2, ""));
        let mailbox_receiver = state
            .send_close_region_message(&env.context, &failed_region)
            .await
            .unwrap();

        let message_id = mailbox_receiver.message_id();

        // verify that the close region message is sent
        let rx = env
            .heartbeat_receivers
            .get_mut(&failed_region.datanode_id)
            .unwrap();
        let resp = rx.recv().await.unwrap().unwrap();
        let received = &resp.mailbox_message.unwrap();
        assert_eq!(received.id, message_id);
        assert_eq!(received.subject, "Deactivate Region");
        assert_eq!(received.from, "Metasrv@127.0.0.1:3002");
        assert_eq!(received.to, "Datanode-1");
        assert_eq!(
            received.payload,
            Some(Payload::Json(
                serde_json::to_string(&Instruction::CloseRegion(failed_region.clone())).unwrap(),
            ))
        );

        // simulating response from Datanode
        env.context
            .mailbox
            .on_recv(
                message_id,
                Ok(MailboxMessage {
                    id: message_id,
                    subject: "Deactivate Region".to_string(),
                    from: "Datanode-1".to_string(),
                    to: "Metasrv".to_string(),
                    timestamp_millis: common_time::util::current_time_millis(),
                    payload: Some(Payload::Json(
                        serde_json::to_string(&InstructionReply::CloseRegion(SimpleReply {
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
            .handle_response(&env.context, mailbox_receiver, &failed_region)
            .await
            .unwrap();
        assert_eq!(
            format!("{next_state:?}"),
            r#"ActivateRegion { candidate: Peer { id: 2, addr: "" }, remark_inactive_region: false, region_storage_path: None, region_options: None, region_wal_options: None }"#
        );
    }

    #[tokio::test]
    async fn test_deactivate_region_timeout() {
        common_telemetry::init_default_ut_logging();

        let mut env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let state = DeactivateRegion::new(Peer::new(2, ""));
        let mailbox_receiver = state
            .send_close_region_message(&env.context, &failed_region)
            .await
            .unwrap();

        // verify that the open region message is sent
        let rx = env
            .heartbeat_receivers
            .get_mut(&failed_region.datanode_id)
            .unwrap();
        let resp = rx.recv().await.unwrap().unwrap();
        let received = &resp.mailbox_message.unwrap();
        assert_eq!(received.id, mailbox_receiver.message_id());
        assert_eq!(received.subject, "Deactivate Region");
        assert_eq!(received.from, "Metasrv@127.0.0.1:3002");
        assert_eq!(received.to, "Datanode-1");
        assert_eq!(
            received.payload,
            Some(Payload::Json(
                serde_json::to_string(&Instruction::CloseRegion(failed_region.clone())).unwrap(),
            ))
        );

        let next_state = state
            .handle_response(&env.context, mailbox_receiver, &failed_region)
            .await
            .unwrap();
        // Timeout or not, proceed to `ActivateRegion`.
        assert_eq!(
            format!("{next_state:?}"),
            r#"ActivateRegion { candidate: Peer { id: 2, addr: "" }, remark_inactive_region: false, region_storage_path: None, region_options: None, region_wal_options: None }"#
        );
    }
}
