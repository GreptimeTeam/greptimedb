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

use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};

/// Fixed directory generation used by standalone and distributed WAL stores.
///
/// The directory remains stable across process restarts. This is distinct from
/// both the heartbeat node epoch and the store epoch carried in object headers.
pub const FIXED_DIRECTORY_GENERATION: u64 = 0;

/// Plugin marker allowing metasrv and its datanodes to use object-store WAL.
///
/// OSS does not register this marker. Cluster integration registers it only after
/// the distributed WAL prerequisites, including handoff and takeover, are available.
/// It does not itself configure storage or implement region failover.
#[derive(Debug, Clone, Copy)]
pub struct EnableObjectStoreWal;

/// When an append to the object store WAL returns.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// An append returns after the object holding its entries is durable.
    #[default]
    Durable,
    /// An append returns once its entries are admitted and their ids are
    /// assigned; the object is created in the background.
    Enqueued,
}

/// What a read does with a segment that still does not decode after a
/// second fetch, because its checksum does not match or its content
/// disagrees with its footer entry.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CorruptedSegmentAction {
    /// The segment is skipped and recorded as a hole of its region; the other
    /// regions of the object are unaffected.
    #[default]
    Skip,
    /// The read fails.
    Fail,
}

/// Object store wal configurations for datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ObjectStoreWalConfig {
    /// Name of the storage provider that holds the WAL objects.
    /// An empty name selects the default object store.
    pub storage_provider: String,
    /// Path prefix of the WAL objects inside the storage provider.
    /// In distributed mode, metasrv and all datanodes must use the same public
    /// root prefix and shared object storage. A mismatched prefix prevents regions
    /// from opening.
    ///
    /// The store runs under a node prefix derived from it, see
    /// [`node_prefix`](Self::node_prefix).
    pub prefix: String,
    /// Interval of flushing buffered entries to the object store, at least
    /// 10ms, defaults to 100ms.
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// The max size of a single batch object, defaults to 8MiB.
    pub max_batch_bytes: ReadableSize,
    /// When an append returns, defaults to `durable`.
    pub ack_mode: AckMode,
    /// Size of the unpersisted backlog at which appends stall until an
    /// upload completes in `enqueued` mode, defaults to 64MiB.
    pub max_unpersisted_bytes: ReadableSize,
    /// Age of the oldest unpersisted entry at which appends stall until an
    /// upload completes in `enqueued` mode, defaults to 8s.
    #[serde(with = "humantime_serde")]
    pub max_unpersisted_age: Duration,
    /// What a read does with a segment that still does not decode after a
    /// second fetch, because its checksum does not match or its content
    /// disagrees with its footer entry, defaults to `skip`.
    pub on_corrupted_segment: CorruptedSegmentAction,
    /// Whether a region whose WAL history the configured location does not
    /// hold opens anyway, with its writes that were not flushed lost, instead
    /// of failing to open, defaults to false. Meant for one start after the
    /// old WAL location is lost.
    pub accept_missing_history: bool,
}

impl Default for ObjectStoreWalConfig {
    fn default() -> Self {
        Self {
            storage_provider: String::new(),
            prefix: "wal".to_string(),
            flush_interval: Duration::from_millis(100),
            max_batch_bytes: ReadableSize::mb(8),
            ack_mode: AckMode::Durable,
            max_unpersisted_bytes: ReadableSize::mb(64),
            max_unpersisted_age: Duration::from_secs(8),
            on_corrupted_segment: CorruptedSegmentAction::Skip,
            accept_missing_history: false,
        }
    }
}

impl ObjectStoreWalConfig {
    /// Validates the public root prefix without changing its stored identity.
    /// Other WAL settings are irrelevant to metasrv and validated by the datanode.
    pub fn validate_prefix(&self) -> Result<(), &'static str> {
        let prefix = self.prefix.trim();
        if prefix.is_empty() {
            return Err("must not be empty");
        }
        if prefix.starts_with('/') {
            return Err("must be a relative path");
        }
        if prefix
            .split('/')
            .any(|component| component.is_empty() || component == "..")
        {
            return Err("must not contain empty or `..` components");
        }
        // The node suffix preserves trailing whitespace in the root's last component.
        if self
            .prefix
            .trim_start()
            .split('/')
            .any(|component| component == ".")
        {
            return Err("must not contain `.` components");
        }
        Ok(())
    }

    /// Returns the prefix the store of `node_id` runs under in `generation`:
    /// `<prefix>/datanodes/<node_id>/epochs/<generation>`.
    ///
    /// Writers and remote readers derive concrete node directories here.
    /// Distributed region metadata retains the public root instead; standalone
    /// WAL options retain this concrete prefix for compatibility.
    pub fn node_prefix(&self, node_id: u64, generation: u64) -> String {
        format!("{}/datanodes/{node_id}/epochs/{generation}", self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_prefix() {
        for prefix in [
            "", "   ", "/wal", "wal/", "wal//a", "wal/../a", ".", "wal/./a",
        ] {
            let config = ObjectStoreWalConfig {
                prefix: prefix.to_string(),
                ..Default::default()
            };
            assert!(config.validate_prefix().is_err(), "{prefix:?}");
        }
        for prefix in ["wal", "tenant/wal", " wal ", "wal/. ", ". "] {
            let config = ObjectStoreWalConfig {
                prefix: prefix.to_string(),
                ..Default::default()
            };
            assert_eq!(Ok(()), config.validate_prefix());
            assert_eq!(prefix, config.prefix);
        }
    }

    #[test]
    fn test_node_prefix() {
        let config = ObjectStoreWalConfig::default();
        assert_eq!(
            config.node_prefix(0, FIXED_DIRECTORY_GENERATION),
            "wal/datanodes/0/epochs/0"
        );

        let config = ObjectStoreWalConfig {
            prefix: "cluster-a/wal".to_string(),
            ..Default::default()
        };
        assert_eq!(
            config.node_prefix(3, 7),
            "cluster-a/wal/datanodes/3/epochs/7"
        );
    }
}
