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

use api::v1::meta::MailboxMessage;
use async_trait::async_trait;
use common_meta::instruction::{Instruction, TableIdent};
use common_meta::RegionIdent;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::failover_end::RegionFailoverEnd;
use super::{RegionFailoverContext, State};
use crate::error::{self, Result};
use crate::service::mailbox::BroadcastChannel;

#[derive(Serialize, Deserialize, Debug, Default)]
pub(super) struct InvalidateCache;

impl InvalidateCache {
    async fn broadcast_invalidate_table_cache_messages(
        &self,
        ctx: &RegionFailoverContext,
        table_ident: &TableIdent,
    ) -> Result<()> {
        let instruction = Instruction::InvalidateTableCache(table_ident.clone());

        let msg = &MailboxMessage::json_message(
            "Invalidate Table Cache",
            &format!("Metasrv@{}", ctx.selector_ctx.server_addr),
            "Frontend broadcast",
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        ctx.mailbox
            .broadcast(&BroadcastChannel::Frontend, msg)
            .await
    }
}

#[async_trait]
#[typetag::serde]
impl State for InvalidateCache {
    async fn next(
        mut self: Box<Self>,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        let table_ident = TableIdent::from(failed_region.clone());
        self.broadcast_invalidate_table_cache_messages(ctx, &table_ident)
            .await?;

        Ok(Box::new(RegionFailoverEnd))
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::mailbox_message::Payload;

    use super::super::tests::{TestingEnv, TestingEnvBuilder};
    use super::*;

    #[tokio::test]
    async fn test_invalidate_table_cache() {
        common_telemetry::init_default_ut_logging();

        let TestingEnv {
            context,
            failed_region,
            mut heartbeat_receivers,
        } = TestingEnvBuilder::new().build().await;

        let state = InvalidateCache;
        let table_ident: TableIdent = failed_region.clone().into();

        // lexicographical order
        // frontend-4,5,6,7
        let next_state = Box::new(state)
            .next(&context, &failed_region)
            .await
            .unwrap();
        assert_eq!(format!("{next_state:?}"), "RegionFailoverEnd");

        let frontend_id = |idx: usize| -> u64 { (idx + 4) as u64 };

        for idx in 0..4 {
            // frontend id starts from 4
            let rx = heartbeat_receivers.get_mut(&frontend_id(idx)).unwrap();
            let resp = rx.recv().await.unwrap().unwrap();
            let received = &resp.mailbox_message.unwrap();

            assert_eq!(received.id, 0);
            assert_eq!(received.subject, "Invalidate Table Cache");
            assert_eq!(received.from, "Metasrv@127.0.0.1:3002");
            assert_eq!(received.to, "Frontend broadcast");

            assert_eq!(
                received.payload,
                Some(Payload::Json(
                    serde_json::to_string(&Instruction::InvalidateTableCache(table_ident.clone()))
                        .unwrap(),
                ))
            );
        }
    }
}
