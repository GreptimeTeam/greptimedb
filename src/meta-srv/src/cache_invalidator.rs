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
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::{CacheInvalidator, Context};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::instruction::{CacheIdent, Instruction};
use common_telemetry::tracing_context::TracingContext;
use snafu::ResultExt;

use crate::metasrv::MetasrvInfo;
use crate::service::mailbox::{BroadcastChannel, MailboxRef};

const DEFAULT_SUBJECT: &str = "Invalidate table";

pub struct MetasrvCacheInvalidator {
    mailbox: MailboxRef,
    // Metasrv infos
    info: MetasrvInfo,
}

impl MetasrvCacheInvalidator {
    pub fn new(mailbox: MailboxRef, info: MetasrvInfo) -> Self {
        Self { mailbox, info }
    }
}

impl MetasrvCacheInvalidator {
    async fn broadcast(&self, ctx: &Context, instruction: Instruction) -> MetaResult<()> {
        let subject = &ctx
            .subject
            .clone()
            .unwrap_or_else(|| DEFAULT_SUBJECT.to_string());

        let tracing_ctx = TracingContext::from_current_span();
        let mut msg = MailboxMessage::json_message(
            subject,
            &format!("Metasrv@{}", self.info.server_addr),
            "Frontend broadcast",
            common_time::util::current_time_millis(),
            &instruction,
            Some(tracing_ctx.to_w3c()),
        )
        .with_context(|_| meta_error::SerdeJsonSnafu)?;

        self.mailbox
            .broadcast(&BroadcastChannel::Frontend, &msg)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;

        msg.to = "Datanode broadcast".to_string();
        self.mailbox
            .broadcast(&BroadcastChannel::Datanode, &msg)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;

        msg.to = "Flownode broadcast".to_string();
        self.mailbox
            .broadcast(&BroadcastChannel::Flownode, &msg)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

#[async_trait]
impl CacheInvalidator for MetasrvCacheInvalidator {
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> MetaResult<()> {
        let instruction = Instruction::InvalidateCaches(caches.to_vec());
        self.broadcast(ctx, instruction).await
    }
}
