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

//! Handling catchup request and the end of a region's wait for its Leader lease.

use std::collections::HashSet;
use std::sync::Arc;

use common_telemetry::{debug, error, info, warn};
use snafu::ensure;
use store_api::logstore::LogStore;
use store_api::region_engine::SettableRegionRoleState;
use store_api::region_request::RegionCatchupRequest;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::config::IndexBuildMode;
use crate::error::{self, Result};
use crate::region::catchup::{RegionCatchupTask, hand_over};
use crate::region::opener::RegionOpener;
use crate::region::{
    CatchupRegionsRef, MitoRegion, MitoRegionRef, RegionLeaderState, RegionRoleState,
};
use crate::request::{
    BackgroundNotify, BuildIndexRequest, CaughtUpAgain, HandedOver, OptionOutputTx,
    WorkerRequestWithTime,
};
use crate::sst::index::IndexBuildType;
use crate::tail_replay::{notify_worker, release_wal_to_replay_start};
#[cfg(feature = "hdfs-object-store")]
use crate::temp_file_sweeper;
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::worker::{
    DeferredLeaseWork, RegionWorkerLoop, WorkerListener, drain_ended_staging, handle_reload,
    handle_tail_replay,
};

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Catches the region up from its WAL, or, for a region whose manifest is published
    /// conditionally and a catchup that makes it writable and carries the entry its source
    /// reported, hands it over from that source without replaying (see [hand_over]). A region
    /// that awaits its Leader lease is caught up again into a replacement instance, which
    /// replaces it only if the replay succeeds, see [Self::catch_up_again]. The replacement holds
    /// only the rows its WAL replays: unflushed rows written with `skip_wal`, and unflushed rows
    /// under a local WAL, are dropped, while flushed rows stay in its SSTs. Only a catchup with
    /// `set_writable` promotes the region again. The replay of a remote WAL first waits until the
    /// entries the region acknowledged are durable.
    pub(crate) async fn handle_catchup_request(
        &mut self,
        region_id: RegionId,
        request: RegionCatchupRequest,
        entry_receiver: Option<WalEntryReceiver>,
        sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            sender.send(Err(error::RegionNotFoundSnafu { region_id }.build()));
            return;
        };

        if region.is_writable() && !region.manifest_ctx.awaits_leader_lease() {
            debug!("Region {region_id} is writable, skip catchup");
            sender.send(Ok(0));
            return;
        }

        // Before the demotion below, so a catchup in progress keeps the region it promotes.
        if self.catchup_regions.is_region_exists(region_id)
            || region.manifest_ctx.is_reload_pending()
        {
            warn!("Region {region_id} under catching up or waiting for a reload");
            sender.send(Err(error::RegionBusySnafu { region_id }.build()));
            return;
        }

        // A Leader lease may have ended the region's wait since the check above.
        let Some(awaits_leader_lease) = region.manifest_ctx.begin_catchup(region_id) else {
            debug!("Region {region_id} is writable, skip catchup");
            sender.send(Ok(0));
            return;
        };
        // A catchup promoted the region and no Leader lease followed, so the region may lack
        // what the leader has written since, also when a failed catchup again demoted it.
        if awaits_leader_lease {
            self.catch_up_again(region, request, entry_receiver, sender)
                .await;
            return;
        }

        // A catchup that makes a region whose manifest is published conditionally writable, and
        // carries the last entry its source reported, hands the region over from that source,
        // replaying nothing. Its memtable must be empty: the hand-off floor would skip the entries
        // of the rows it holds, and a reopen would drop them, which for rows written without the
        // WAL is their only copy. The refused region keeps its rows as a Follower; a retry succeeds
        // once a Leader lease has made it writable again (the catchup is then skipped) or it was
        // reopened without those rows.
        let handover = if request.set_writable && region.manifest_ctx.is_conditional_publication() {
            request.entry_id
        } else {
            None
        };
        // A tail replay of this instance that has not reported its end may still have a
        // committed edit for the worker to apply. A handover's edit applied first changes the
        // version's pending tails, so the worker would skip the replay's edit and leave its SSTs
        // out of the version. The edit of a replay of an earlier instance never applies to this
        // one (`apply_tails_edit` checks the instance), so only a replay of this instance refuses
        // the handover.
        if handover.is_some()
            && (!region.version().memtables.is_empty()
                || self
                    .tail_replays
                    .get(&region_id)
                    .is_some_and(|(owner, _)| Arc::ptr_eq(owner, &region)))
        {
            warn!(
                "Region {region_id} has rows in its memtable or a running tail replay, refusing to hand it over"
            );
            region.manifest_ctx.end_catchup();
            sender.send(Err(error::RegionBusySnafu { region_id }.build()));
            return;
        }
        // The worker writes a region inline, so every write acknowledged so far has an entry id
        // at or below this one.
        let last_entry_id = region.version_control.current().last_entry_id;
        // If the memtable is not empty or the manifest has been updated, we need to reopen the region.
        let region = match self.reopen_region_if_needed(region.clone()).await {
            Ok(region) => region,
            Err(e) => {
                region.manifest_ctx.end_catchup();
                sender.send(Err(e));
                return;
            }
        };

        self.catchup_regions.insert_region(region_id);
        let finish = self.catchup_finish(region.clone(), sender);
        let wal = self.wal.clone();
        let allow_stale_entries = self.config.allow_stale_entries;
        let node_lease = self.node_lease.clone();
        let node_id = self.file_ref_manager.node_id();
        common_runtime::spawn_global(async move {
            // Under conditional manifest publication, the region becomes writable only once its
            // catchup committed the next manifest version.
            let result = match handover {
                Some(entry_id) => {
                    match hand_over(&region, &wal, node_id, request.source_node_id, entry_id).await
                    {
                        Ok((hook, edit)) => {
                            hook.fire().await;
                            // The worker applies the edit and promotes the region.
                            let notify = BackgroundNotify::HandedOver(HandedOver {
                                region,
                                edit,
                                sender: finish.sender,
                            });
                            notify_worker(&finish.worker_sender, region_id, notify).await;
                            return;
                        }
                        Err(e) => Err(e),
                    }
                }
                None => {
                    let mut task = RegionCatchupTask::new(region.clone(), wal, allow_stale_entries)
                        .with_entry_receiver(entry_receiver)
                        .with_expected_last_entry_id(request.entry_id)
                        .with_set_writable(request.set_writable)
                        .with_node_id(node_id)
                        .with_replay_checkpoint_entry_id(request.checkpoint.map(|c| c.entry_id))
                        .with_acknowledged_entry_id(last_entry_id);
                    match task.run().await {
                        Ok(_) if request.set_writable => region.manifest_ctx.commit_catchup().await,
                        result => result.map(|()| None),
                    }
                }
            };
            match result {
                Ok(hook) => {
                    if let Some(hook) = hook {
                        hook.fire().await;
                    }
                    let promoted = if !request.set_writable {
                        region.manifest_ctx.end_catchup();
                        false
                    } else {
                        // The region takes writes under the node lease until its next Leader
                        // grant.
                        node_lease.seed(&region.manifest_ctx.leader_deadline);
                        region.manifest_ctx.promote_after_catchup(region_id)
                    };
                    finish.run(promoted).await;
                }
                Err(err) => {
                    error!(err; "Failed to catchup region {region_id}");
                    region.manifest_ctx.end_catchup();
                    finish.catchup_regions.remove_region(region_id);
                    if region.manifest_ctx.is_stopped() {
                        finish
                            .sender
                            .send(Err(error::RegionNotFoundSnafu { region_id }.build()));
                    } else {
                        finish.sender.send(Err(err));
                    }
                }
            }
        });
    }

    /// Catches a region up again that a catchup promoted and that no Leader lease followed. The
    /// region stays registered, demoted unless it is in a transient leader state, while a
    /// replacement opened from its manifest replays the WAL outside the worker;
    /// [Self::handle_caught_up_again] registers the replacement only if its replay succeeded (on
    /// a remote WAL it must reach the entries the region acknowledged), the region took no
    /// write and made no commit meanwhile and, for a catchup that makes the region writable, the
    /// replacement's catchup commit succeeded (under conditional manifest publication it commits
    /// an empty edit). Otherwise the region keeps its memtable, so a later Leader lease can promote
    /// it; one that waits for a reload is reloaded instead.
    async fn catch_up_again(
        &mut self,
        region: MitoRegionRef,
        request: RegionCatchupRequest,
        entry_receiver: Option<WalEntryReceiver>,
        sender: OptionOutputTx,
    ) {
        let region_id = region.region_id;
        warn!("Region {region_id} awaits its Leader lease, catching it up again");
        self.cancel_tail_replay(region_id);
        // The worker writes a region inline, so every write acknowledged so far has an entry id
        // at or below this one, and a later write changes the last sequence.
        let current = region.version_control.current();
        let (acknowledged_entry_id, last_sequence) = (current.last_entry_id, current.last_sequence);
        let opener = match self.region_reopener(&region) {
            Ok(opener) => opener.skip_wal_replay(true),
            Err(e) => {
                region.manifest_ctx.end_catchup();
                sender.send(Err(e));
                return;
            }
        };
        let manifest_version = region.manifest_ctx.manifest_version().await;
        self.catchup_regions.insert_region(region_id);
        let wal = self.wal.clone();
        let node_id = self.file_ref_manager.node_id();
        let config = self.config.clone();
        let worker_sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let result = async {
                let replacement = opener.open(&config, &wal).await?;
                RegionCatchupTask::new(replacement.clone(), wal, config.allow_stale_entries)
                    .with_entry_receiver(entry_receiver)
                    .with_node_id(node_id)
                    .with_expected_last_entry_id(request.entry_id)
                    .with_set_writable(request.set_writable)
                    .with_replay_checkpoint_entry_id(request.checkpoint.map(|c| c.entry_id))
                    .with_acknowledged_entry_id(acknowledged_entry_id)
                    .run()
                    .await?;
                Ok(replacement)
            }
            .await;
            let notify = BackgroundNotify::CaughtUpAgain(CaughtUpAgain {
                old: region,
                manifest_version,
                last_sequence,
                result,
                set_writable: request.set_writable,
                sender,
            });
            notify_worker(&worker_sender, region_id, notify).await;
        });
    }

    /// Replaces the region with the instance its catchup again replayed into, or keeps the region
    /// if the replay failed, the region was closed or reopened, waits for a reload or a flush,
    /// committed or took a write since the catchup began, or the catchup commit of the
    /// replacement failed. A kept region ends its catchup, so a later Leader lease can promote it
    /// with its memtable; one that waits for a reload is reloaded instead.
    pub(crate) async fn handle_caught_up_again(
        &mut self,
        region_id: RegionId,
        caught_up: CaughtUpAgain,
    ) {
        let CaughtUpAgain {
            old,
            manifest_version,
            last_sequence,
            result,
            set_writable,
            sender,
        } = caught_up;
        let registered = self
            .regions
            .get_region(region_id)
            .is_some_and(|registered| Arc::ptr_eq(&registered, &old));
        // Commits of the old instance wait until the replacement is registered.
        let manager = old.manifest_ctx.manifest_manager.read().await;
        let unchanged = !old.manifest_ctx.is_reload_pending()
            // Its own commits only: a version that only storage has is the replacement's.
            && manager.manifest().manifest_version == manifest_version
            && !self.flush_scheduler.is_flush_requested(region_id)
            // The catchup does not demote a region in a transient leader state: an edit that fails
            // meanwhile makes it writable again, and the replay misses the writes it then
            // acknowledges, with or without the WAL.
            && old.version_control.current().last_sequence == last_sequence;
        let mut catchup_hook = None;
        let replacement = match result {
            _ if !registered => Err(error::RegionNotFoundSnafu { region_id }.build()),
            Ok(_) if !unchanged => Err(error::RegionBusySnafu { region_id }.build()),
            // The replacement's own commit guards pass, as it is unregistered; the checks of the
            // old instance above do the guarding. The old instance's commits wait on the read
            // guard held here, so once the commit succeeds nothing below can refuse the swap.
            // This awaits one conditional PUT and one LIST on the worker, under that guard: the
            // commit cannot move into a spawned task, since the old instance could then commit or
            // be fenced between it and the checks. A catchup again is rare.
            Ok(replacement) if set_writable => {
                replacement.manifest_ctx.commit_catchup().await.map(|hook| {
                    catchup_hook = hook;
                    replacement
                })
            }
            result => result,
        };
        let replacement = match replacement {
            Ok(replacement) => replacement,
            Err(e) => {
                drop(manager);
                warn!(e; "Region {region_id} is not replaced by its catchup again");
                self.catchup_regions.remove_region(region_id);
                old.manifest_ctx.end_catchup();
                sender.send(Err(e));
                return;
            }
        };
        // Preserve allocated sequences without publishing the old memtables.
        replacement
            .version_control
            .update_last_sequence(old.version_control.current().last_sequence);
        // Leases apply through the shared region map, so only those from heartbeats sent after
        // the replacement may promote the new instance.
        replacement.manifest_ctx.ignore_earlier_promotions();
        // Requests waiting for the old instance are rejected as a close rejects them.
        self.fail_region_stalled_requests_as_not_found(&region_id);
        self.reject_region_requests_as_not_found(region_id);
        // Leases apply through the shared region map, so a writable catchup keeps them from
        // promoting the registered replacement before the promotion below (including while its
        // hook runs); that promotion ends this catchup.
        if set_writable {
            let begun = replacement.manifest_ctx.begin_catchup(region_id);
            debug_assert_eq!(begun, Some(false));
        }
        self.register_region(replacement.clone());
        drop(manager);
        // A hook may read the manifest of the region, so it fires once no manifest lock is held.
        if let Some(hook) = catchup_hook {
            hook.fire().await;
        }
        if let Some(state) = &self.series_index_task_state {
            state.wake();
        }
        info!("Region {region_id} is caught up again into a new instance");
        let promoted = set_writable && {
            // The region takes writes under the node lease until its next Leader grant.
            self.node_lease
                .seed(&replacement.manifest_ctx.leader_deadline);
            replacement.manifest_ctx.promote_after_catchup(region_id)
        };
        let finish = self.catchup_finish(replacement, sender);
        common_runtime::spawn_global(async move {
            finish.run(promoted).await;
        });
    }

    /// Applies the edit a catchup committed for the handover of a region and promotes the
    /// region, on the worker, in order with the edits of flushes and compactions. Ends the catchup
    /// with an error if the region was replaced or stopped since; the committed edit stays in the
    /// manifest, which the next open recovers, and this node's WAL is still released up to the
    /// committed manifest's replay start, capped by this node's pending tails, before the reply.
    pub(crate) fn handle_handed_over(&mut self, region_id: RegionId, handed_over: HandedOver) {
        let HandedOver {
            region,
            edit,
            sender,
        } = handed_over;
        let registered = self
            .regions
            .get_region(region_id)
            .is_some_and(|registered| Arc::ptr_eq(&registered, &region));
        if !registered || region.manifest_ctx.is_stopped() {
            warn!("Region {region_id} was replaced or stopped before its handover applied");
            region.manifest_ctx.end_catchup();
            self.catchup_regions.remove_region(region_id);
            let error = error::RegionNotFoundSnafu { region_id }.build();
            // The committed handover still releases this node's entries up to where the committed
            // manifest replays its WAL, keeping this node's pending tails.
            let wal = self.wal.clone();
            let node_id = self.file_ref_manager.node_id();
            common_runtime::spawn_global(async move {
                release_wal_to_replay_start(&region, &wal, node_id).await;
                sender.send(Err(error));
            });
            return;
        }
        if let Some(edit) = edit {
            region
                .version_control
                .apply_edit(Some(edit), &[], region.file_purger.clone());
        }
        // The region takes writes under the node lease until its next Leader grant.
        self.node_lease.seed(&region.manifest_ctx.leader_deadline);
        let promoted = region.manifest_ctx.promote_after_catchup(region_id);
        let finish = self.catchup_finish(region.clone(), sender);
        let wal = self.wal.clone();
        let node_id = self.file_ref_manager.node_id();
        // The finish drains through this worker, so it must not be awaited here.
        common_runtime::spawn_global(async move {
            // A handover replayed nothing, so this node's entries up to its handoff floor are
            // released here, keeping its pending tails. The committed manifest alone decides the
            // release, so it does not depend on the promotion.
            release_wal_to_replay_start(&region, &wal, node_id).await;
            finish.run(promoted).await;
        });
    }

    /// Returns whether a manifest sync or a follower's option change of `region` is refused
    /// with RegionBusy: the instance is in a catchup, which replays the WAL into its memtable and
    /// reads its options and manifest outside the worker, or it is fenced while the worker holds
    /// a catchup or reload registration for the region. A reload reopens the region with the
    /// options it read when it began, which would drop an option change; a sync shares the
    /// predicate.
    pub(crate) fn is_catching_up_or_reloading(&self, region: &MitoRegion) -> bool {
        region.manifest_ctx.is_catching_up()
            || (self.catchup_regions.is_region_exists(region.region_id)
                && region.manifest_ctx.is_reload_pending())
    }

    fn catchup_finish(&self, region: MitoRegionRef, sender: OptionOutputTx) -> CatchupFinish {
        CatchupFinish {
            region,
            sender,
            catchup_regions: self.catchup_regions.clone(),
            worker_sender: self.sender.clone(),
            #[cfg(feature = "hdfs-object-store")]
            sweep_older_than: self.config.gc.unknown_file_lingering_time,
            #[cfg(feature = "hdfs-object-store")]
            sweep_limiter: self.sweep_limiter.clone(),
        }
    }

    /// Reopens a region for its catchup, which keeps Leader leases from promoting the reopened
    /// instance until it ends.
    async fn reopen_region(&mut self, region: &Arc<MitoRegion>) -> Result<Arc<MitoRegion>> {
        let region_id = region.region_id;
        // Commits of the old instance wait until the reopened one is registered, so a conflict
        // either fenced the old instance before the check below or happens after it is replaced.
        let manager = region.manifest_ctx.manifest_manager.read().await;
        ensure!(
            !region.manifest_ctx.is_reload_pending(),
            error::RegionBusySnafu { region_id }
        );
        let manifest_version = manager.manifest().manifest_version;
        let flushed_entry_id = region.version_control.current().last_entry_id;
        info!(
            "Reopening the region: {region_id}, manifest version: {manifest_version}, flushed entry id: {flushed_entry_id}"
        );
        let reopened_region = self
            .region_reopener(region)?
            .skip_wal_replay(true)
            .open(&self.config, &self.wal)
            .await?;
        debug_assert!(!reopened_region.is_writable());
        // Registered below, where a Leader lease can reach it before the catchup ends.
        let begun = reopened_region.manifest_ctx.begin_catchup(region.region_id);
        debug_assert_eq!(begun, Some(false));
        // Preserve allocated sequences without publishing the old memtables.
        reopened_region
            .version_control
            .update_last_sequence(region.version_control.current().last_sequence);
        self.register_region(reopened_region.clone());
        drop(manager);
        if let Some(state) = &self.series_index_task_state {
            state.wake();
        }

        Ok(reopened_region)
    }

    /// Returns an opener of the region from its manifest, with its current options.
    pub(crate) fn region_reopener(&self, region: &MitoRegion) -> Result<RegionOpener> {
        RegionOpener::new(
            region.region_id,
            region.table_dir(),
            region.access_layer.path_type(),
            self.node_lease.clone(),
            self.memtable_builder_provider.clone(),
            self.object_store_manager.clone(),
            self.purge_scheduler.clone(),
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
            self.time_provider.clone(),
            self.file_ref_manager.clone(),
            self.partition_expr_fetcher.clone(),
        )
        .cache(Some(self.cache_manager.clone()))
        .series_index_store(self.series_index_store.clone())
        .series_index_purger(self.series_index_purger.clone())
        .hook(self.plugins.get())
        .options(region.version().options.clone())
    }

    async fn reopen_region_if_needed(
        &mut self,
        region: Arc<MitoRegion>,
    ) -> Result<Arc<MitoRegion>> {
        // Note: Currently, We protect the split brain by ensuring the memtable table is empty.
        // It's expensive to execute catch-up requests without `set_writable=true` multiple times.
        let version = region.version();
        let is_empty_memtable = version.memtables.is_empty();

        // Utilizes the short circuit evaluation.
        let region = if !is_empty_memtable || region.manifest_ctx.has_update().await? {
            if !is_empty_memtable {
                warn!(
                    "Region {} memtables is not empty, which should not happen, manifest version: {}, last entry id: {}",
                    region.region_id,
                    region.manifest_ctx.manifest_version().await,
                    region.version_control.current().last_entry_id
                );
            }
            self.listener
                .on_catchup_reopen_begin(region.region_id)
                .await;
            self.reopen_region(&region).await?
        } else {
            region
        };

        Ok(region)
    }

    /// Starts the work that a region deferred while it awaited its Leader lease.
    pub(crate) async fn handle_leader_lease_wait_ended(&mut self, region_id: RegionId) {
        let Some(region) = self.regions.get_region(region_id) else {
            return;
        };
        // As for any flush, the completion of these handles the stalled writes.
        self.maybe_flush_worker();
        self.maybe_flush_write_regions(HashSet::from([region_id]));
        if region.state() == RegionRoleState::Leader(RegionLeaderState::Writable) {
            self.schedule_compaction(&region).await;
        } else {
            info!(
                "Region {region_id} is {:?} when its Leader lease wait ends, starting no compaction",
                region.state()
            );
        }
        // The sweep after the batch runs it, after the requests the batch started, or once the
        // region is a writable leader.
        let work = self.defer_lease_work(&region);
        work.missing_index_pass = true;
        work.backfill = true;
        self.listener.on_leader_lease_wait_ended(region_id);
    }

    /// Finishes the deferred work of every region that is a writable leader with its Leader
    /// lease. The worker loop calls it after each batch, so the work runs after the requests the
    /// batch started; a DDL that the batch queued behind a flush or a compaction has not started.
    /// It costs one `is_empty()` per batch and, per entry, which regions rarely have, one region
    /// lookup (a read lock of the region map) and a few atomic loads, with no I/O or await until
    /// a region qualifies; the `Vec` of the regions that qualify allocates only once one does.
    pub(crate) async fn sweep_deferred_lease_work(&mut self) {
        if self.deferred_lease_work.is_empty() {
            return;
        }
        let regions: Vec<_> = self
            .deferred_lease_work
            .keys()
            .filter_map(|region_id| self.regions.get_region(*region_id))
            .filter(|region| region.may_finish_deferred_lease_work())
            .collect();
        for region in regions {
            self.finish_deferred_lease_work(region).await;
        }
    }

    /// Runs the deferred work of a region that was a writable leader with its Leader lease:
    /// builds the missing indexes, re-issues the kept index build retries and saves the
    /// backfilled metadata. A role change since then makes each step keep its work due.
    async fn finish_deferred_lease_work(&mut self, region: MitoRegionRef) {
        let region_id = region.region_id;
        let Some(work) = self.deferred_lease_work.remove(&region_id) else {
            return;
        };
        self.listener.on_deferred_lease_work_begin(region_id).await;
        if work.missing_index_pass && self.build_missing_indexes(&region).await {
            // A role change outside the worker stopped the pass, so the region does the work
            // once it is a writable leader again. Nothing deferred work for the region while the
            // pass ran: it is not a retry, and the worker handles no request until it returns.
            self.deferred_lease_work.insert(region_id, work);
            return;
        }
        // A retry that the region refuses again keeps itself.
        for request in work.retries {
            self.handle_rebuild_index_inner(request, OptionOutputTx::new(None), true)
                .await;
        }
        if work.backfill {
            spawn_persist_backfilled_metadata(&self.listener, &self.sender, region);
        }
    }

    /// Defers the save of the backfilled metadata that found `source` not a writable leader with
    /// its Leader lease. A close or a drop stopped a source whose work it removed, so a later
    /// instance does not take it over.
    pub(crate) fn handle_backfill_deferred(&mut self, source: MitoRegionRef) {
        let region_id = source.region_id;
        self.listener.on_backfill_deferred(region_id);
        if source.manifest_ctx.is_stopped() {
            return;
        }
        if let Some(region) = self.regions.get_region(region_id) {
            self.defer_lease_work(&region).backfill = true;
        }
    }

    /// Builds the indexes of the region's files that lack them, which a region that waits does
    /// not build. Returns true if the state of the region stopped it.
    async fn build_missing_indexes(&mut self, region: &MitoRegionRef) -> bool {
        if self.config.index.build_mode != IndexBuildMode::Async {
            return false;
        }
        let request = BuildIndexRequest {
            region_id: region.region_id,
            build_type: IndexBuildType::Flush,
            file_metas: Vec::new(),
            result_senders: Vec::new(),
        };
        self.handle_rebuild_index(request, OptionOutputTx::new(None))
            .await
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Returns the work `region` has due once it is a writable leader with its Leader lease,
    /// recording that some is due.
    pub(crate) fn defer_lease_work(&mut self, region: &MitoRegionRef) -> &mut DeferredLeaseWork {
        let region_id = region.region_id;
        self.deferred_lease_work.entry(region_id).or_insert_with(|| {
            if !region.may_finish_deferred_lease_work() {
                info!(
                    "Region {region_id} is {:?}, deferring work until it is a writable leader with its Leader lease",
                    region.state()
                );
            }
            DeferredLeaseWork::default()
        })
    }
}

/// What ends a successful catchup: it finalizes the leadership of a promoted region, ends the
/// catchup and replies.
struct CatchupFinish {
    region: MitoRegionRef,
    sender: OptionOutputTx,
    catchup_regions: CatchupRegionsRef,
    worker_sender: Sender<WorkerRequestWithTime>,
    #[cfg(feature = "hdfs-object-store")]
    sweep_older_than: std::time::Duration,
    #[cfg(feature = "hdfs-object-store")]
    sweep_limiter: Option<Arc<tokio::sync::Semaphore>>,
}

impl CatchupFinish {
    async fn run(self, promoted: bool) {
        let region = self.region;
        let region_id = region.region_id;
        if promoted {
            // Finalize leadership: persist backfilled metadata.
            let drain = drain_ended_staging(&self.worker_sender, region_id);
            match region
                .set_role_state_gracefully(SettableRegionRoleState::Leader, drain)
                .await
            {
                Ok(()) => {
                    #[cfg(feature = "hdfs-object-store")]
                    temp_file_sweeper::spawn_region_sweep(
                        region.access_layer(),
                        region_id,
                        self.sweep_older_than,
                        self.sweep_limiter,
                    );
                }
                Err(err) => {
                    error!(err; "Failed to set region {region_id} to leader");
                }
            }
            handle_tail_replay::notify_writable(&self.worker_sender, &region);
        }
        self.catchup_regions.remove_region(region_id);
        // A close during the catchup stopped the region, which no longer serves it. A commit
        // during the catchup, including the backfill above, may have found a newer manifest.
        if region.manifest_ctx.is_stopped() {
            self.sender
                .send(Err(error::RegionNotFoundSnafu { region_id }.build()));
        } else if region.manifest_ctx.is_reload_pending() {
            self.sender
                .send(Err(error::RegionBusySnafu { region_id }.build()));
        } else {
            self.sender.send(Ok(0));
        }
    }
}

/// Persists the metadata the opener filled in, which the catchup of a region that waits does
/// not, without blocking the worker. The commit runs outside the worker, so it asks for the
/// reload of a region it fences.
fn spawn_persist_backfilled_metadata(
    listener: &WorkerListener,
    sender: &Sender<WorkerRequestWithTime>,
    region: MitoRegionRef,
) {
    listener.on_persist_backfilled_metadata_begin(region.region_id);
    let listener = listener.clone();
    let sender = sender.clone();
    common_runtime::spawn_global(async move {
        listener
            .on_persist_backfilled_metadata_lock_begin(region.region_id)
            .await;
        persist_backfilled_metadata_and_notify_if_fenced(&sender, &region).await;
    });
}

/// Persists the metadata the opener filled in for `region` and, if the commit fenced the region,
/// asks its worker through `sender` for the reload. A save that finds the region not a writable
/// leader with its Leader lease, such as in a change the worker started after it spawned the
/// save, has the worker defer it.
pub(crate) async fn persist_backfilled_metadata_and_notify_if_fenced(
    sender: &Sender<WorkerRequestWithTime>,
    region: &MitoRegionRef,
) {
    if !region.persist_backfilled_metadata().await {
        let notify = BackgroundNotify::BackfillDeferred {
            region: region.clone(),
        };
        notify_worker(sender, region.region_id, notify).await;
    }
    handle_reload::notify_if_fenced(sender, region).await;
}

/// Tells the worker that a lease ended the wait of `region` for its Leader lease, without
/// waiting.
pub(crate) fn notify_leader_lease_wait_ended(
    sender: &Sender<WorkerRequestWithTime>,
    region: &MitoRegionRef,
) {
    let sender = sender.clone();
    let region_id = region.region_id;
    common_runtime::spawn_global(async move {
        notify_worker(&sender, region_id, BackgroundNotify::LeaderLeaseWaitEnded).await;
    });
}
