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

use tokio::sync::mpsc;

use crate::heartbeat::mailbox::{HeartbeatMailbox, MessageMeta};
use crate::instruction::{InstructionReply, SimpleReply};

#[tokio::test]
async fn test_heartbeat_mailbox() {
    let (tx, mut rx) = mpsc::channel(8);

    let mailbox = HeartbeatMailbox::new(tx);

    let meta = MessageMeta::new_test(1, "test", "foo", "bar");
    let reply = InstructionReply::OpenRegion(SimpleReply {
        result: true,
        error: None,
    });
    mailbox.send((meta.clone(), reply.clone())).await.unwrap();

    let message = rx.recv().await.unwrap();
    assert_eq!(message.0, meta);
    assert_eq!(message.1, reply);
}
