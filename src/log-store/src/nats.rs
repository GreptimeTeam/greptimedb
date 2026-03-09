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

//! NATS JetStream WAL log-store implementation.
//!
//! This module provides a Write-Ahead Log backend backed by [NATS JetStream].
//! It mirrors the existing Kafka WAL in architecture, replacing Kafka primitives
//! with NATS equivalents:
//!
//! | Kafka concept            | NATS equivalent                             |
//! |--------------------------|---------------------------------------------|
//! | Topic per region         | Subject per region (in a shared stream)     |
//! | Partition offset         | Stream sequence number (`u64`)              |
//! | `produce()` + `offset`  | `jetstream.publish()` → `PublishAck`         |
//! | `fetch_high_watermark()` | `stream.get_last_raw_message_by_subject()`  |
//! | `delete_records(offset)` | `stream.purge().filter(subj).sequence(n)`   |
//!
//! [NATS JetStream]: https://docs.nats.io/nats-concepts/jetstream

pub(crate) mod client;
pub(crate) mod consumer;
pub(crate) mod log_store;
pub(crate) mod producer;
pub(crate) mod purge_worker;
pub(crate) mod record;
pub(crate) mod util;

/// Type alias re-exported for convenience.
pub use log_store::NatsLogStore;

/// The entry-id type used by the NATS WAL.
///
/// NATS JetStream sequence numbers are `u64`, 1-indexed per stream.
/// We expose them as `u64` matching `EntryId = u64`.
pub(crate) type EntryId = u64;
