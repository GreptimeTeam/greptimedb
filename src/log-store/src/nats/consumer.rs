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

//! WAL entry consumer backed by NATS JetStream pull consumers.
//!
//! `NatsConsumer` creates an ephemeral pull consumer scoped to a single
//! subject, replays from a given sequence number, and yields complete
//! (possibly multi-part) [`Entry`] values until the high watermark is reached.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::stream::Stream;
use async_nats::Message;
use bytes::Bytes;
use common_telemetry::warn;
use futures::StreamExt;
use snafu::ResultExt;
use store_api::logstore::entry::Entry;
use store_api::logstore::provider::NatsProvider;
use store_api::storage::RegionId;

use crate::error::{ConsumeNatsMessagesSnafu, CreateNatsConsumerSnafu, GetLastNatsMessageSnafu, Result};
use crate::nats::record::{NatsRecord, maybe_emit_entry, remaining_entries};

/// Fetches all WAL entries for `subject` starting at `start_seq`.
///
/// Returns a `Vec<Vec<Entry>>` (one inner `Vec` per fetch batch) that the
/// caller can flatten into a single stream.  The function stops reading once
/// it has seen messages up to `end_seq` (the watermark fetched before opening
/// the consumer).
pub(crate) async fn read_entries(
    stream: &Stream,
    provider: Arc<NatsProvider>,
    subject: String,
    start_seq: u64,
    consumer_wait_timeout: Duration,
) -> Result<Vec<Entry>> {
    // Step 1: determine the high watermark for this subject.
    let end_seq = match stream
        .get_last_raw_message_by_subject(&subject)
        .await
    {
        Ok(msg) => msg.sequence,
        Err(_) => {
            // No messages on this subject yet — return empty.
            return Ok(vec![]);
        }
    };

    if start_seq > end_seq {
        return Ok(vec![]);
    }

    // Step 2: create an ephemeral pull consumer starting at start_seq.
    let consumer_config = PullConfig {
        filter_subject: subject.clone(),
        deliver_policy: if start_seq == 0 {
            DeliverPolicy::All
        } else {
            DeliverPolicy::ByStartSequence {
                start_sequence: start_seq,
            }
        },
        // Ephemeral: no durable name.
        ..Default::default()
    };

    let consumer = stream
        .create_consumer(consumer_config)
        .await
        .context(CreateNatsConsumerSnafu {
            subject: subject.clone(),
        })?;

    // Step 3: fetch messages until we reach end_seq.
    let mut buffered_records: HashMap<RegionId, Vec<NatsRecord>> = HashMap::new();
    let mut entries: Vec<Entry> = Vec::new();

    let mut messages = consumer
        .messages()
        .await
        .context(ConsumeNatsMessagesSnafu {
            subject: subject.clone(),
        })?;

    while let Some(msg_result) = messages.next().await {
        match msg_result {
            Ok(msg) => {
                let nats_seq = msg
                    .info()
                    .map(|i| i.stream_sequence)
                    .unwrap_or_default();

                let record = NatsRecord::from_nats_message(
                    msg.headers.as_ref(),
                    msg.payload.clone(),
                    nats_seq,
                )?;

                if let Some(entry) = maybe_emit_entry(&provider, record, &mut buffered_records)? {
                    entries.push(entry);
                }

                // Acknowledge so the server knows we processed the message.
                let _ = msg.ack().await;

                if nats_seq >= end_seq {
                    break;
                }
            }
            Err(e) => {
                warn!("NATS WAL consumer error on subject {}: {}", subject, e);
                break;
            }
        }
    }

    // Flush any incomplete multi-part entries.
    if let Some(remaining) = remaining_entries(&provider, &mut buffered_records) {
        entries.extend(remaining);
    }

    Ok(entries)
}
