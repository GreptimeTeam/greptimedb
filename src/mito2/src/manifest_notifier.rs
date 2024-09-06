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

use api::v1::{Mutation, OpType, WalEntry};
use store_api::logstore::provider::Provider;
use store_api::logstore::LogStore;
use store_api::manifest::ManifestVersion;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::Result;
use crate::region::version::{VersionControlData, VersionControlRef};
use crate::wal::{EntryId, WalWriter};

/// Used to notify follower regions to apply manifest changes.
pub(crate) struct ManifestNotifier {
    /// Id of region to write.
    region_id: RegionId,
    /// VersionControl of the region.
    version_control: VersionControlRef,
    /// Next sequence number to write.
    ///
    /// The context assigns a unique sequence number for each row.
    next_sequence: SequenceNumber,
    /// Next entry id of WAL to write.
    next_entry_id: EntryId,
    /// Valid WAL entry to write.
    ///
    /// We keep [WalEntry] instead of mutations to avoid taking mutations
    /// out of the context to construct the wal entry when we write to the wal.
    wal_entry: WalEntry,

    /// Wal options of the region being written to.
    provider: Provider,
}

impl ManifestNotifier {
    pub(crate) fn new(
        region_id: RegionId,
        version_control: &VersionControlRef,
        provider: Provider,
    ) -> ManifestNotifier {
        let VersionControlData {
            committed_sequence,
            last_entry_id,
            ..
        } = version_control.current();

        ManifestNotifier {
            region_id,
            version_control: version_control.clone(),
            next_sequence: committed_sequence + 1,
            next_entry_id: last_entry_id + 1,
            wal_entry: WalEntry::default(),
            provider,
        }
    }

    // Push manifest notification.
    pub(crate) fn push_notification(&mut self, version: ManifestVersion) {
        self.wal_entry.mutations.push(Mutation {
            op_type: OpType::Notify.into(),
            sequence: self.next_sequence,
            rows: None,
            manifest_notification: Some(api::v1::ManifestNotification { version }),
        });
        self.next_sequence += 1;
    }

    /// Encode and add WAL entry to the writer.
    pub(crate) fn add_wal_entry<S: LogStore>(
        &mut self,
        wal_writer: &mut WalWriter<S>,
    ) -> Result<()> {
        wal_writer.add_entry(
            self.region_id,
            self.next_entry_id,
            &self.wal_entry,
            &self.provider,
        )?;
        self.next_entry_id += 1;
        Ok(())
    }

    /// Updates next entry id.
    pub(crate) fn set_next_entry_id(&mut self, next_entry_id: EntryId) {
        self.next_entry_id = next_entry_id
    }

    pub(crate) fn finish(&mut self) {
        self.version_control
            .set_sequence_and_entry_id(self.next_sequence - 1, self.next_entry_id - 1);
    }
}
