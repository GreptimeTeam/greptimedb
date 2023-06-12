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
use api::v1::meta::MailboxMessage;
use common_time::util::current_time_millis;
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::heartbeat::mailbox::{IncomingMessage, MessageMeta, OutgoingMessage};
use crate::instruction::Instruction;

pub fn mailbox_message_to_incoming_message(m: MailboxMessage) -> Result<IncomingMessage> {
    m.payload
        .map(|payload| match payload {
            Payload::Json(json) => {
                let instruction: Instruction = serde_json::from_str(&json)?;
                Ok((
                    MessageMeta {
                        id: m.id,
                        subject: m.subject,
                        to: m.to,
                        from: m.from,
                    },
                    instruction,
                ))
            }
        })
        .transpose()
        .context(error::DecodeJsonSnafu)?
        .context(error::PayloadNotExistSnafu)
}

pub fn outgoing_message_to_mailbox_message(
    (meta, reply): OutgoingMessage,
) -> Result<MailboxMessage> {
    Ok(MailboxMessage {
        id: meta.id,
        subject: meta.subject,
        from: meta.to,
        to: meta.from,
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&reply).context(error::EncodeJsonSnafu)?,
        )),
    })
}
