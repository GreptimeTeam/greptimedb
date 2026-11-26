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

use std::sync::Arc;
use std::time::Instant;

use common_telemetry::{info, warn};
use snafu::{OptionExt, ensure};
use store_api::logstore::LogStore;
use store_api::storage::SequenceNumber;

use crate::engine::region_hook::PendingManifestHook;
use crate::error::{self, Result};
use crate::manifest::action::{
    HandoffFloor, PendingTail, RegionEdit, RegionManifest, UptoKind, WalRecoveryEdit,
    cap_wal_release,
};
use crate::region::MitoRegion;
use crate::region::opener::{check_wal_history, replay_memtable};
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::wal::{EntryId, Wal};

/// How far a handover raises the committed sequence above the largest sequence this node knows
/// the region used, in the manifest or on this node, so that writes accepted while the recorded
/// tail is replayed outrank its rows. Those rows keep the sequences their source gave them, which
/// this node does not know: the gap allows the source fewer than 2^40 sequences that the manifest
/// does not record.
const HANDOVER_SEQUENCE_GAP: SequenceNumber = 1 << 40;
/// The largest committed sequence a handover raises a region to, which allows about 2^22
/// handovers that record a tail. A handover that would pass it fails, leaving the sequences above
/// it to the region's writes, which allocate them without checking for overflow.
const MAX_HANDOVER_SEQUENCE: SequenceNumber = 1 << 62;

/// Returns the entry id a hand-off of the region has to cover: the largest of the one its source
/// reported, its flushed entry id and the bounds of its pending tails.
fn handoff_entry_id(
    reported_entry_id: EntryId,
    flushed_entry_id: EntryId,
    pending_tails: &[PendingTail],
) -> EntryId {
    pending_tails
        .iter()
        .map(|tail| tail.upto)
        .fold(reported_entry_id.max(flushed_entry_id), EntryId::max)
}

/// Hands a region whose manifest is published conditionally over to this node `node_id`, which
/// becomes its writer, from the source node `source_node_id` that reported `reported_entry_id`
/// as its last entry. Replays nothing.
///
/// The log store first hands the region over (see [Wal::handoff]). Then the catchup commits, after
/// the manifest the region holds, this node's handoff floor, the bound that hand-off returned,
/// and, if the source has entries after where the manifest recovers its WAL from, a reported tail
/// of the source over them, raising the committed sequence by [HANDOVER_SEQUENCE_GAP]. The tail is
/// replayed in the background once the region is writable.
///
/// A region that skips the WAL commits an empty edit. Fails, before the log store hands anything
/// over, if the source has such entries and the request names no source, or if the region has no
/// sequences left for another handover.
pub(crate) async fn hand_over<S: LogStore>(
    region: &MitoRegion,
    wal: &Wal<S>,
    node_id: Option<u64>,
    source_node_id: Option<u64>,
    reported_entry_id: EntryId,
) -> Result<Option<PendingManifestHook>> {
    let region_id = region.region_id;
    if region.version().options.skip_wal {
        return region.manifest_ctx.commit_catchup().await;
    }
    let node_id = node_id.with_context(|| error::UnexpectedSnafu {
        reason: format!("No node id to hand region {region_id} over to"),
    })?;
    let manifest = region.manifest_ctx.manifest().await;
    let bound = handoff_entry_id(
        reported_entry_id,
        manifest.flushed_entry_id,
        &manifest.pending_tails,
    );
    // A handover that cannot commit after this manifest fails before the log store hands the
    // region over. The commit builds the edit again for the manifest it commits after.
    handover_edit(
        &manifest,
        region.version_control.current().last_sequence,
        HandoffFloor {
            node_id,
            entry_id: bound,
        },
        source_node_id,
        reported_entry_id,
    )?;
    let floor = wal.handoff(region_id, &region.provider, bound).await?;
    let mut manager = region.manifest_ctx.manifest_manager.write().await;
    let edit = handover_edit(
        &manager.manifest(),
        region.version_control.current().last_sequence,
        HandoffFloor {
            node_id,
            entry_id: floor,
        },
        source_node_id,
        reported_entry_id,
    )?;
    let pending = region
        .manifest_ctx
        .commit_catchup_locked(&mut manager, edit.clone())
        .await?;
    if pending.is_some() {
        region
            .version_control
            .apply_edit(Some(edit), &[], region.file_purger.clone());
    }
    Ok(pending)
}

/// Returns the edit of a handover to `floor.node_id` (see [hand_over]) that commits after
/// `manifest`. `last_sequence` is the largest sequence the region used on this node.
fn handover_edit(
    manifest: &RegionManifest,
    last_sequence: SequenceNumber,
    floor: HandoffFloor,
    source_node_id: Option<u64>,
    reported_entry_id: EntryId,
) -> Result<RegionEdit> {
    let region_id = manifest.metadata.region_id;
    let mut edit = RegionEdit {
        wal_recovery: WalRecoveryEdit {
            pending_tails: None,
            handoff_floor: Some(floor),
        },
        ..Default::default()
    };
    // Entries of the source up to here are flushed, in its tails or below its handoff floor.
    let after = manifest.wal_replay_start(source_node_id);
    if after >= reported_entry_id {
        return Ok(edit);
    }
    let node_id = source_node_id.with_context(|| error::InvalidRequestSnafu {
        region_id,
        reason: format!(
            "the request names no source of the entries in ({after}, {reported_entry_id}] to recover"
        ),
    })?;
    let committed_sequence = manifest
        .committed_sequence
        .unwrap_or_default()
        .max(manifest.flushed_sequence)
        .max(last_sequence)
        .checked_add(HANDOVER_SEQUENCE_GAP)
        .filter(|sequence| *sequence <= MAX_HANDOVER_SEQUENCE)
        .with_context(|| error::UnexpectedSnafu {
            reason: format!("Region {region_id} has no sequences left for another handover"),
        })?;
    let mut tails = manifest.pending_tails.clone();
    tails.push(PendingTail {
        node_id,
        after,
        upto: reported_entry_id,
        upto_kind: UptoKind::Reported,
    });
    edit.committed_sequence = Some(committed_sequence);
    edit.wal_recovery.pending_tails = Some(tails);
    Ok(edit)
}

pub struct RegionCatchupTask<S> {
    entry_receiver: Option<WalEntryReceiver>,
    region: Arc<MitoRegion>,
    replay_checkpoint_entry_id: Option<u64>,
    expected_last_entry_id: Option<u64>,
    /// The largest entry id the region acknowledged on this node.
    acknowledged_entry_id: u64,
    /// Whether the catchup makes the region writable on this node.
    set_writable: bool,
    /// Id of this node.
    node_id: Option<u64>,
    allow_stale_entries: bool,
    wal: Wal<S>,
}

impl<S: LogStore> RegionCatchupTask<S> {
    pub fn new(region: Arc<MitoRegion>, wal: Wal<S>, allow_stale_entries: bool) -> Self {
        Self {
            entry_receiver: None,
            region,
            replay_checkpoint_entry_id: None,
            expected_last_entry_id: None,
            acknowledged_entry_id: 0,
            set_writable: false,
            node_id: None,
            allow_stale_entries,
            wal,
        }
    }

    /// Sets whether the catchup makes the region writable on this node.
    pub(crate) fn with_set_writable(mut self, set_writable: bool) -> Self {
        self.set_writable = set_writable;
        self
    }

    /// Sets the id of this node, whose WAL the catchup replays.
    pub(crate) fn with_node_id(mut self, node_id: Option<u64>) -> Self {
        self.node_id = node_id;
        self
    }

    /// Sets the expected last entry id.
    pub(crate) fn with_expected_last_entry_id(
        mut self,
        expected_last_entry_id: Option<u64>,
    ) -> Self {
        self.expected_last_entry_id = expected_last_entry_id;
        self
    }

    /// Sets the largest entry id the region acknowledged on this node, which the replay of a
    /// remote WAL waits to be durable and must reach.
    pub(crate) fn with_acknowledged_entry_id(mut self, acknowledged_entry_id: u64) -> Self {
        self.acknowledged_entry_id = acknowledged_entry_id;
        self
    }

    /// Sets the entry receiver.
    pub(crate) fn with_entry_receiver(mut self, entry_receiver: Option<WalEntryReceiver>) -> Self {
        self.entry_receiver = entry_receiver;
        self
    }

    /// Sets the replay checkpoint entry id.
    pub(crate) fn with_replay_checkpoint_entry_id(
        mut self,
        replay_checkpoint_entry_id: Option<u64>,
    ) -> Self {
        self.replay_checkpoint_entry_id = replay_checkpoint_entry_id;
        self
    }

    pub async fn run(&mut self) -> Result<()> {
        if self.region.provider.is_remote_wal() {
            self.remote_wal_catchup().await
        } else {
            self.local_wal_catchup().await
        }
    }

    async fn remote_wal_catchup(&mut self) -> Result<()> {
        let flushed_entry_id = self.region.version_control.current().last_entry_id;
        // Entries up to where the manifest recovers this node's WAL from are flushed, left to
        // the pending tails or below this node's handoff floor.
        let manifest = self.region.manifest_ctx.manifest().await;
        let replay_from_entry_id = self
            .replay_checkpoint_entry_id
            .unwrap_or(flushed_entry_id)
            .max(flushed_entry_id)
            .max(manifest.wal_replay_start(self.node_id));
        let region_id = self.region.region_id;
        info!(
            "Trying to replay memtable for region: {region_id}, provider: {:?}, replay from entry id: {replay_from_entry_id}, flushed entry id: {flushed_entry_id}",
            self.region.provider
        );
        let version = self.region.version_control.current().version;
        // A catchup that makes the region writable and carries the entry id its source reported
        // hands the region over to this node's log store; of the catchups of a region whose
        // manifest is published conditionally, only a catchup again gets here, the others are
        // handed over by [hand_over]. From now on the region takes ids above every id the
        // source, the manifest or a pending tail used, so a later rise of its flushed entry id
        // passes none of its new entries, and this node's log store holds its history from here
        // on. A catchup that still has to replay entries above where its
        // replay starts hands nothing over, so if it cannot reach them it leaves nothing behind.
        if self.set_writable
            && let Some(reported_entry_id) = self.expected_last_entry_id
            && reported_entry_id <= replay_from_entry_id
            && !version.options.skip_wal
        {
            let entry_id = handoff_entry_id(
                reported_entry_id,
                version.flushed_entry_id,
                &version.pending_tails,
            );
            self.wal
                .handoff(region_id, &self.region.provider, entry_id)
                .await?;
        }
        check_wal_history(&self.wal, &self.region.provider, &version).await?;
        let timer = Instant::now();
        // The replay reads only what the log store has made durable.
        self.wal
            .durability_barrier(region_id, &self.region.provider)
            .wait(self.acknowledged_entry_id)
            .await?;
        let wal_entry_reader = self
            .entry_receiver
            .take()
            .map(|r| Box::new(r) as _)
            .unwrap_or_else(|| self.wal.wal_entry_reader(&self.region.provider, region_id));
        let on_region_opened = self.wal.on_region_opened();
        let last_entry_id = replay_memtable(
            &self.region.provider,
            wal_entry_reader,
            region_id,
            replay_from_entry_id,
            cap_wal_release(&manifest.pending_tails, self.node_id, replay_from_entry_id),
            &self.region.version_control,
            self.allow_stale_entries,
            on_region_opened,
        )
        .await?;
        info!(
            "Elapsed: {:?}, region: {region_id}, provider: {:?} catchup finished. replay from entry id: {replay_from_entry_id}, flushed entry id: {flushed_entry_id}, last entry id: {last_entry_id}, expected: {:?}, acknowledged: {}.",
            timer.elapsed(),
            self.region.provider,
            self.expected_last_entry_id,
            self.acknowledged_entry_id
        );
        // The replay also reaches the entries the region acknowledged on this node.
        let expected_last_entry_id = self
            .expected_last_entry_id
            .unwrap_or_default()
            .max(self.acknowledged_entry_id);
        ensure!(
            // The replayed last entry id may be greater than the `expected_last_entry_id`.
            last_entry_id >= expected_last_entry_id,
            error::UnexpectedSnafu {
                reason: format!(
                    "Failed to catchup region {}, it was expected to replay to {} (expected: {:?}, acknowledged: {}), but actually replayed to {}",
                    region_id,
                    expected_last_entry_id,
                    self.expected_last_entry_id,
                    self.acknowledged_entry_id,
                    last_entry_id,
                ),
            }
        );
        Ok(())
    }

    async fn local_wal_catchup(&mut self) -> Result<()> {
        let version = self.region.version_control.current();
        let mut flushed_entry_id = version.last_entry_id;
        let region_id = self.region.region_id;
        let latest_entry_id = self
            .wal
            .store()
            .latest_entry_id(&self.region.provider)
            .unwrap_or_default();
        info!(
            "Skips to replay memtable for region: {}, flushed entry id: {}, latest entry id: {}",
            region_id, flushed_entry_id, latest_entry_id
        );

        if latest_entry_id > flushed_entry_id {
            warn!(
                "Found latest entry id is greater than flushed entry id, using latest entry id as flushed entry id, region: {}, latest entry id: {}, flushed entry id: {}",
                region_id, latest_entry_id, flushed_entry_id
            );
            flushed_entry_id = latest_entry_id;
            self.region.version_control.set_entry_id(flushed_entry_id);
        }
        let on_region_opened = self.wal.on_region_opened();
        on_region_opened(region_id, flushed_entry_id, &self.region.provider).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::error::Error;
    use crate::manifest::action::{RegionChange, RegionManifestBuilder};
    use crate::sst::FormatType;

    const NODE_ID: u64 = 1;
    const SOURCE: u64 = 2;

    fn tail(node_id: u64, after: EntryId, upto: EntryId) -> PendingTail {
        PendingTail {
            node_id,
            after,
            upto,
            upto_kind: UptoKind::Reported,
        }
    }

    /// Returns a manifest flushed through entry 10 and sequence 9, with sequence 7 committed,
    /// the given tails and the handoff floor of [SOURCE].
    fn manifest(tails: Vec<PendingTail>, source_floor: Option<EntryId>) -> RegionManifest {
        let mut builder = RegionManifestBuilder::default();
        builder.apply_change(
            0,
            RegionChange {
                metadata: crate::test_util::memtable_util::metadata_for_test(),
                sst_format: FormatType::PrimaryKey,
                append_mode: None,
            },
        );
        builder.apply_edit(
            1,
            RegionEdit {
                flushed_entry_id: Some(10),
                flushed_sequence: Some(9),
                committed_sequence: Some(7),
                wal_recovery: WalRecoveryEdit {
                    pending_tails: Some(tails),
                    handoff_floor: source_floor.map(|entry_id| HandoffFloor {
                        node_id: SOURCE,
                        entry_id,
                    }),
                },
                ..Default::default()
            },
        );
        builder.try_build().unwrap()
    }

    fn floor(entry_id: EntryId) -> HandoffFloor {
        HandoffFloor {
            node_id: NODE_ID,
            entry_id,
        }
    }

    #[test]
    fn test_handover_records_source_entries_after_its_recovery_start() {
        let other = tail(3, 10, 30);
        for (tails, source_floor, after) in [
            (vec![], None, 10),
            (vec![tail(SOURCE, 10, 20), other.clone()], None, 20),
            (vec![tail(SOURCE, 10, 20)], Some(25), 25),
        ] {
            let manifest = manifest(tails.clone(), source_floor);
            let edit = handover_edit(&manifest, 12, floor(50), Some(SOURCE), 40).unwrap();

            let mut expected = tails;
            expected.push(tail(SOURCE, after, 40));
            assert_eq!(edit.wal_recovery.pending_tails, Some(expected));
            assert_eq!(edit.wal_recovery.handoff_floor, Some(floor(50)));
            // Above the sequence the region used, which is above those in the manifest.
            assert_eq!(edit.committed_sequence, Some(12 + HANDOVER_SEQUENCE_GAP));
            assert_eq!(edit.flushed_entry_id, None);
        }
        let edit = handover_edit(&manifest(vec![], None), 0, floor(50), Some(SOURCE), 40).unwrap();
        assert_eq!(edit.committed_sequence, Some(9 + HANDOVER_SEQUENCE_GAP));
        let mut raised = manifest(vec![], None);
        raised.committed_sequence = Some(20);
        let edit = handover_edit(&raised, 12, floor(50), Some(SOURCE), 40).unwrap();
        assert_eq!(edit.committed_sequence, Some(20 + HANDOVER_SEQUENCE_GAP));
    }

    #[test]
    fn test_handover_without_new_source_entries_records_only_the_floor() {
        for (tails, source_floor, source, reported) in [
            (vec![], None, Some(SOURCE), 10),
            (vec![tail(SOURCE, 10, 40)], None, Some(SOURCE), 40),
            (vec![], Some(40), Some(SOURCE), 40),
            // Without new entries the source need not be known.
            (vec![], None, None, 8),
        ] {
            let manifest = manifest(tails, source_floor);
            let edit = handover_edit(&manifest, 12, floor(50), source, reported).unwrap();
            assert_eq!(
                edit,
                RegionEdit {
                    wal_recovery: WalRecoveryEdit {
                        pending_tails: None,
                        handoff_floor: Some(floor(50)),
                    },
                    ..Default::default()
                }
            );
        }
    }

    #[test]
    fn test_handover_fails_if_the_source_of_new_entries_is_unknown() {
        // Another node's tail and floor do not count for an unknown source.
        let mut manifest = manifest(vec![tail(3, 10, 40)], None);
        manifest.handoff_floors = BTreeMap::from([(3, 45)]);
        let error = handover_edit(&manifest, 12, floor(50), None, 40).unwrap_err();
        assert!(matches!(error, Error::InvalidRequest { .. }), "{error:?}");
    }

    #[test]
    fn test_handover_fails_without_sequences_left() {
        let manifest = manifest(vec![], None);
        let last_sequence = MAX_HANDOVER_SEQUENCE - HANDOVER_SEQUENCE_GAP;
        let edit = handover_edit(&manifest, last_sequence, floor(50), Some(SOURCE), 40).unwrap();
        assert_eq!(edit.committed_sequence, Some(MAX_HANDOVER_SEQUENCE));
        let error =
            handover_edit(&manifest, last_sequence + 1, floor(50), Some(SOURCE), 40).unwrap_err();
        assert!(matches!(error, Error::Unexpected { .. }), "{error:?}");
    }
}
