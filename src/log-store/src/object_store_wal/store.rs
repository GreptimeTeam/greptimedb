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

//! Object store WAL construction, recovery, writes and region reads.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use common_wal::config::object_store::ObjectStoreWalConfig;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::{ObjectStoreProvider, Provider};
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream, WalIndex};
use store_api::storage::RegionId;
#[cfg(any(test, feature = "testing"))]
use tokio::sync::watch;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;

use crate::error::{
    CorruptedWalObjectSnafu, Error, IncompleteWalEntrySnafu, InvalidProviderSnafu,
    InvalidWalObjectSnafu, InvalidWalObjectStoreSnafu, MismatchedWalPrefixSnafu,
    MismatchedWalRegionSnafu, ObjectStoreWalSnafu, ObjectStoreWalStoppedSnafu, Result,
    WalObjectHistoryGapSnafu, WalObjectSequenceExhaustedSnafu, WalObjectSequenceUnsettledSnafu,
};
use crate::object_store_wal::batch::{OBJECT_SEQ_LIMIT, OpenBatch, sequence_floor};
use crate::object_store_wal::catalog::ObjectCatalog;
use crate::object_store_wal::format::{
    EncodedObject, FixedTrailer, FooterEntry, HEADER_LEN, Header, MIN_OBJECT_LEN, Record,
    TRAILER_LEN, decode_footer, decode_header, decode_segment, decode_trailer, encode_object,
    footer_range, verify_segment_ranges,
};
use crate::object_store_wal::io::{ListedObject, ObjectStoreIo, PutResult};

const COMMAND_BUFFER: usize = 1024;
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
const MAX_IN_FLIGHT_CREATES: usize = 4;
/// Number of objects whose footers recovery fetches at a time.
const RECOVERY_CONCURRENCY: usize = 8;
/// Bytes recovery reads from the end of an object in one request. The window
/// holds the trailer and the footer of an object with up to 1364 regions of
/// 48 bytes each, so a second request for the footer is rare.
const RECOVERY_TAIL_WINDOW: usize = 64 * 1024;

/// A log store over the immutable WAL objects under one prefix.
///
/// Appends are admitted into an open batch. A background actor seals the batch
/// when it reaches the size limit or the flush interval elapses and creates
/// the object under the next sequence while it keeps admitting entries into
/// the next batch; up to [`MAX_IN_FLIGHT_CREATES`] creates run at a time.
/// Objects are indexed in the catalog and their batches acknowledged in
/// sequence order, so an acknowledged entry never has a missing predecessor.
/// An append returns once its object is durable and indexed.
pub(crate) struct ObjectStoreLogStore {
    prefix: String,
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    obsolete_entry_ids: ObsoleteEntryIds,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    command_tx: mpsc::Sender<Command>,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    creates_held: watch::Sender<bool>,
    #[cfg(any(test, feature = "testing"))]
    creates_fail: Arc<AtomicBool>,
}

type ObsoleteEntryIds = Arc<Mutex<HashMap<RegionId, EntryId>>>;

type TerminalError = Arc<Mutex<Option<Arc<Error>>>>;

impl fmt::Debug for ObjectStoreLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreLogStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreLogStore {
    /// Builds the store under the node and generation prefix derived from `config`,
    /// recovering the catalog from the objects that already exist. Recovery
    /// fails on the first corrupted or conflicting object.
    pub(crate) async fn try_new(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
        node_id: u64,
        generation: u64,
    ) -> Result<Arc<Self>> {
        let prefix = config.node_prefix(node_id, generation);
        let io = ObjectStoreIo::new(object_store, &prefix)?;
        Self::open(Arc::new(io), config, prefix).await
    }

    async fn open(
        io: Arc<dyn WalObjectIo>,
        config: &ObjectStoreWalConfig,
        prefix: String,
    ) -> Result<Arc<Self>> {
        ensure!(
            config.flush_interval >= MIN_FLUSH_INTERVAL,
            InvalidWalObjectStoreSnafu {
                reason: format!(
                    "flush interval {:?} is shorter than {MIN_FLUSH_INTERVAL:?}",
                    config.flush_interval
                ),
            }
        );

        let max_batch_bytes = positive_bytes(config.max_batch_bytes.as_bytes(), "max batch bytes")?;
        let (catalog, next_object_seq, durable_entry_ids) = recover(io.as_ref()).await?;
        let catalog = Arc::new(RwLock::new(catalog));
        let obsolete_entry_ids = ObsoleteEntryIds::default();
        let terminal_error = TerminalError::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        #[cfg(any(test, feature = "testing"))]
        let (admitted_appends_tx, admitted_appends_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (creates_held_tx, creates_held_rx) = watch::channel(false);
        #[cfg(any(test, feature = "testing"))]
        let creates_fail = Arc::new(AtomicBool::new(false));

        let actor = Actor {
            io: io.clone(),
            catalog: catalog.clone(),
            obsolete_entry_ids: obsolete_entry_ids.clone(),
            terminal_error: terminal_error.clone(),
            stopped: stopped.clone(),
            command_rx,
            open_batch: OpenBatch::new(max_batch_bytes),
            issued_entry_ids: durable_entry_ids,
            pending: Vec::new(),
            sealed: VecDeque::new(),
            creates: FuturesUnordered::new(),
            draining: false,
            stop: Vec::new(),
            next_object_seq: Some(next_object_seq),
            unresolved_object_seq: None,
            writer_instance: uuid::Uuid::new_v4().into_bytes(),
            flush_interval: config.flush_interval,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_tx,
            #[cfg(any(test, feature = "testing"))]
            creates_held: creates_held_rx,
            #[cfg(any(test, feature = "testing"))]
            creates_fail: creates_fail.clone(),
        };
        common_runtime::spawn_global(actor.run());
        Ok(Arc::new(Self {
            prefix,
            io,
            catalog,
            obsolete_entry_ids,
            terminal_error,
            stopped,
            command_tx,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_rx,
            #[cfg(any(test, feature = "testing"))]
            creates_held: creates_held_tx,
            #[cfg(any(test, feature = "testing"))]
            creates_fail,
        }))
    }

    fn durable_entry_id_of(&self, region_id: RegionId) -> EntryId {
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        catalog.region_max_entry_id(region_id).unwrap_or(0)
    }

    /// Returns the region of `provider`, which must select this store's prefix.
    fn region_of(&self, provider: &Provider) -> Result<RegionId> {
        let provider =
            provider
                .as_object_store_provider()
                .with_context(|| InvalidProviderSnafu {
                    expected: ObjectStoreProvider::type_name(),
                    actual: provider.type_name(),
                })?;
        ensure!(
            provider.prefix == self.prefix,
            MismatchedWalPrefixSnafu {
                expected: self.prefix.clone(),
                actual: provider.prefix.clone(),
            }
        );
        Ok(provider.region_id)
    }

    fn check_region(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_terminal()?;
        let provider_region = self.region_of(provider)?;
        ensure!(
            provider_region == region_id,
            MismatchedWalRegionSnafu {
                region_id,
                reason: format!("provider belongs to region {provider_region}"),
            }
        );
        Ok(())
    }

    fn check_terminal(&self) -> Result<()> {
        match terminal(&self.terminal_error) {
            Some(error) => Err(shared(&error)),
            None => Ok(()),
        }
    }
}

/// Records the obsolete watermark of `region_id`, which never moves down.
fn record_obsolete(obsolete_entry_ids: &ObsoleteEntryIds, region_id: RegionId, entry_id: EntryId) {
    obsolete_entry_ids
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(region_id)
        .and_modify(|current| *current = (*current).max(entry_id))
        .or_insert(entry_id);
}

fn positive_bytes(bytes: u64, name: &str) -> Result<usize> {
    usize::try_from(bytes)
        .ok()
        .filter(|bytes| *bytes > 0)
        .with_context(|| InvalidWalObjectStoreSnafu {
            reason: format!("{name} {bytes} is zero or too large"),
        })
}

#[cfg(any(test, feature = "testing"))]
impl ObjectStoreLogStore {
    /// Waits until the actor has admitted at least `expected` append calls
    /// since the store was built.
    pub(crate) async fn wait_for_admitted_appends(&self, expected: usize) -> Result<()> {
        self.admitted_appends
            .clone()
            .wait_for(|count| *count >= expected)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Seals the open batch regardless of its size and age and returns once
    /// its object is durable and indexed, or with the error that failed it.
    pub(crate) async fn seal_open_batch(&self) -> Result<()> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            ObjectStoreWalStoppedSnafu
        );
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Seal {
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }

    /// Parks every conditional create that starts from now on until
    /// [`release_creates`](Self::release_creates), so a test can observe
    /// entries that are admitted but not durable. A create that is parked when
    /// the store is dropped never runs.
    pub(crate) fn hold_creates(&self) {
        self.creates_held.send_replace(true);
    }

    /// Lets the creates parked by [`hold_creates`](Self::hold_creates) run.
    pub(crate) fn release_creates(&self) {
        self.creates_held.send_replace(false);
    }

    /// Makes every create that runs from now on fail with a transient object
    /// store error instead of writing.
    pub(crate) fn fail_creates(&self) {
        self.creates_fail.store(true, Ordering::Release);
    }

    /// Sets the stopped flag without sending the stop command, which is the
    /// state a store is in between the two steps of [`stop`](LogStore::stop).
    pub(crate) fn begin_stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}

#[async_trait::async_trait]
impl LogStore for ObjectStoreLogStore {
    type Error = Error;

    /// Stops the store. Creates in flight run to completion and acknowledge
    /// their entries if they succeed.
    async fn stop(&self) -> Result<()> {
        self.stopped.store(true, Ordering::Release);
        let (response_tx, response_rx) = oneshot::channel();
        let sent = self
            .command_tx
            .send(Command::Stop {
                response: response_tx,
            })
            .await;
        // A closed channel means the actor already exited.
        if sent.is_ok() {
            return response_rx.await.unwrap_or(Ok(()));
        }
        Ok(())
    }

    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            ObjectStoreWalStoppedSnafu
        );
        self.check_terminal()?;
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }
        for entry in &entries {
            let region_id = self.region_of(entry.provider())?;
            ensure!(
                region_id == entry.region_id(),
                MismatchedWalRegionSnafu {
                    region_id: entry.region_id(),
                    reason: format!("provider belongs to region {region_id}"),
                }
            );
            ensure!(entry.is_complete(), IncompleteWalEntrySnafu { region_id });
        }

        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Append {
                entries,
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }

    /// Reads the provider's region from `entry_id`, hiding obsolete entries.
    /// The catalog locates each segment, so a caller-supplied index is unnecessary.
    async fn read(
        &self,
        provider: &Provider,
        entry_id: EntryId,
        _index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Error>> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        let obsolete = self
            .obsolete_entry_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&region_id)
            .copied();
        let start_entry_id =
            entry_id.max(obsolete.map_or(0, |obsolete| obsolete.saturating_add(1)));
        let objects = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            match catalog.region_max_entry_id(region_id) {
                Some(max_entry_id)
                    if start_entry_id <= max_entry_id && obsolete != Some(EntryId::MAX) =>
                {
                    catalog
                        .objects_for_entry_range(region_id, start_entry_id, max_entry_id)?
                        .into_iter()
                        .map(|(object_seq, entry)| (object_seq, entry.clone()))
                        .collect::<Vec<_>>()
                }
                _ => Vec::new(),
            }
        };

        let io = self.io.clone();
        let provider = provider.clone();
        Ok(Box::pin(try_stream! {
            for (object_seq, footer_entry) in objects {
                let bytes = io
                    .get_range(object_seq, footer_entry.segment_offset, footer_entry.segment_len)
                    .await?;
                let records = decode_segment(&bytes, &footer_entry)
                    .with_context(|_| InvalidWalObjectSnafu {
                        path: io.object_path(object_seq),
                    })?;
                let entries = records
                    .into_iter()
                    .filter(|record| record.entry_id >= start_entry_id)
                    .map(|record| {
                        Entry::Naive(NaiveEntry {
                            provider: provider.clone(),
                            region_id,
                            entry_id: record.entry_id,
                            data: record.payload.into(),
                        })
                    })
                    .collect::<Vec<_>>();
                if !entries.is_empty() {
                    yield entries;
                }
            }
        }))
    }

    async fn create_namespace(&self, ns: &Provider) -> Result<()> {
        self.check_terminal()?;
        self.region_of(ns).map(|_| ())
    }

    async fn delete_namespace(&self, ns: &Provider) -> Result<()> {
        self.check_terminal()?;
        self.region_of(ns).map(|_| ())
    }

    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        self.check_terminal()?;
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        let regions = catalog
            .objects_in_order()
            .flat_map(|(_, footer)| footer.iter().map(|entry| entry.region_id))
            .collect::<BTreeSet<_>>();
        Ok(regions
            .into_iter()
            .map(|region_id| Provider::object_store_provider(region_id, self.prefix.clone()))
            .collect())
    }

    /// Moves the obsolete watermark of the region up to `entry_id` and makes
    /// every id the region is assigned from now on greater than `entry_id`:
    /// the sequence of the next object is raised above the object `entry_id`
    /// names unless it is there already, so a watermark assigned under
    /// another prefix, or one whose object this prefix no longer holds, is
    /// never passed by a new id. Zero names no object and needs no floor.
    ///
    /// The watermark and the floor are applied together by the actor: a call
    /// cancelled before its command is queued publishes neither, while a
    /// command already queued still applies both. The floor moves only from a
    /// settled state: while a sealed batch is not durable, while the open
    /// batch holds ids under the next sequence, or while a rolled back
    /// sequence may have left an object that its create has not reconciled,
    /// the call fails with [`Error::WalObjectSequenceUnsettled`] and records
    /// neither. The watermark is kept in memory; the caller re-establishes it
    /// after a restart.
    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        self.check_region(provider, region_id)?;
        let (response_tx, response_rx) = oneshot::channel();
        let command = Command::Obsolete {
            region_id,
            entry_id,
            response: response_tx,
        };
        let answered = match self.command_tx.send(command).await {
            Ok(()) => response_rx.await.ok(),
            Err(_) => None,
        };
        match answered {
            Some(result) => result,
            // The actor exited, before the command was queued or with it
            // still queued behind the stop: nothing is assigned an id any
            // more, so the watermark alone is consistent.
            None => {
                record_obsolete(&self.obsolete_entry_ids, region_id, entry_id);
                Ok(())
            }
        }
    }

    /// Hides every entry of the region in memory. The caller re-establishes
    /// the watermark after a restart.
    async fn obsolete_all(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_region(provider, region_id)?;
        record_obsolete(&self.obsolete_entry_ids, region_id, EntryId::MAX);
        Ok(())
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        self.check_region(provider, region_id)?;
        Ok(Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data,
        }))
    }

    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        Ok(self.durable_entry_id_of(region_id))
    }
}

type AppendResponse = oneshot::Sender<Result<AppendBatchResponse>>;

enum Command {
    Append {
        entries: Vec<Entry>,
        response: AppendResponse,
    },
    /// Answered once the watermark is recorded and no id of the region at
    /// or below `entry_id` can be assigned, or with the reason neither was
    /// done.
    Obsolete {
        region_id: RegionId,
        entry_id: EntryId,
        response: oneshot::Sender<Result<()>>,
    },
    Stop {
        response: oneshot::Sender<Result<()>>,
    },
    #[cfg(any(test, feature = "testing"))]
    Seal {
        response: oneshot::Sender<Result<()>>,
    },
}

/// An append waiting for the object that holds its entries.
struct PendingAppend {
    last_entry_ids: HashMap<RegionId, EntryId>,
    response: AppendResponse,
}

/// Where the conditional create of a sealed batch stands.
enum CreateState {
    /// The create has not started because no slot was free.
    Pending,
    InFlight,
    Created,
    Failed(Arc<Error>),
}

/// A sealed and encoded batch that holds its object sequence and waits to be
/// created, indexed and acknowledged.
struct SealedBatch {
    object_seq: u64,
    bytes: Bytes,
    footer: Vec<FooterEntry>,
    waiters: Vec<PendingAppend>,
    #[cfg(any(test, feature = "testing"))]
    seal_waiters: Vec<oneshot::Sender<Result<()>>>,
    state: CreateState,
}

impl SealedBatch {
    fn is_in_flight(&self) -> bool {
        matches!(self.state, CreateState::InFlight)
    }

    fn fail(self, error: impl Fn() -> Error) {
        for waiter in self.waiters {
            let _ = waiter.response.send(Err(error()));
        }
        #[cfg(any(test, feature = "testing"))]
        for waiter in self.seal_waiters {
            let _ = waiter.send(Err(error()));
        }
    }
}

type CreateOutcome = (u64, Result<PutResult>);

/// The actor that owns the open batch and the sealed batches until they are
/// durable.
///
/// Sealed batches form a pipeline in sequence order. A batch is created under
/// its sequence as soon as one of [`MAX_IN_FLIGHT_CREATES`] slots is free, but
/// it is indexed and acknowledged only once every earlier batch is, so the
/// acknowledged history of a region never has a missing predecessor. The
/// outcomes of a create are:
///
/// | Situation | Sequence | Waiters | Store |
/// | --- | --- | --- | --- |
/// | created, or identical retry | advances | acknowledged in order | healthy |
/// | transient error, no later object created | rolls back to the failed batch | this and every later batch fail; entry ids roll back to the durable watermark | healthy |
/// | transient error, a later object is durable | unchanged | this and every later batch fail | poisoned: the later object cannot be rolled back, and its entries were never acknowledged |
/// | conflicting object, encoding or catalog error | unchanged | every batch that is not durable fails | poisoned |
/// | created at the last representable sequence | cannot advance | acknowledged | poisoned: no later batch can be allocated a sequence |
///
/// A transient error stops new creates from starting until every create in
/// flight has completed, because only then is it known whether a later object
/// exists. After `stop` began nothing is admitted and no create starts;
/// creates in flight run to completion and acknowledge if they succeed.
struct Actor {
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    obsolete_entry_ids: ObsoleteEntryIds,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    command_rx: mpsc::Receiver<Command>,
    open_batch: OpenBatch,
    /// Largest entry id ever handed out per region, whether it became
    /// durable or was rolled back. Unlike the accepted ids of the open batch
    /// it never moves down.
    issued_entry_ids: HashMap<RegionId, EntryId>,
    /// Waiters of the open batch.
    pending: Vec<PendingAppend>,
    /// Batches that are not durable yet, in sequence order.
    sealed: VecDeque<SealedBatch>,
    creates: FuturesUnordered<BoxFuture<'static, CreateOutcome>>,
    /// Set by a transient failure: no create starts until every create in
    /// flight has completed.
    draining: bool,
    /// Callers of `stop`, answered once nothing is in flight.
    stop: Vec<oneshot::Sender<Result<()>>>,
    /// Sequence of the next sealed batch, `None` once the sequence is
    /// exhausted. The open batch assigns its entry ids from it.
    next_object_seq: Option<u64>,
    /// Largest sequence whose batch failed transiently and was rolled back:
    /// its object may exist. The sequences from the next one up to it are
    /// reused in order, so the conditional create at each reconciles it; no
    /// sequence is skipped until an object at or above it is indexed.
    unresolved_object_seq: Option<u64>,
    writer_instance: [u8; 16],
    flush_interval: Duration,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Sender<usize>,
    #[cfg(any(test, feature = "testing"))]
    creates_held: watch::Receiver<bool>,
    #[cfg(any(test, feature = "testing"))]
    creates_fail: Arc<AtomicBool>,
}

impl Actor {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick completes immediately.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Nothing starts after stop began.
                    if !self.is_stopped() {
                        self.flush_open_batch();
                    }
                }
                Some((object_seq, result)) = self.creates.next(), if !self.creates.is_empty() => {
                    self.on_create_completed(object_seq, result);
                }
                command = self.command_rx.recv() => match command {
                    Some(Command::Append { entries, response }) => {
                        self.handle_append(entries, response);
                    }
                    Some(Command::Obsolete { region_id, entry_id, response }) => {
                        self.handle_obsolete(region_id, entry_id, response);
                    }
                    Some(Command::Stop { response }) => {
                        self.handle_stop(response);
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::Seal { response }) => {
                        self.handle_seal(response);
                    }
                    // Every sender is gone: the store was dropped without
                    // `stop`. The creates in flight are dropped with the actor.
                    None => return,
                },
            }
            if self.finish_stop() {
                return;
            }
        }
    }

    fn handle_append(&mut self, entries: Vec<Entry>, response: AppendResponse) {
        // The append was queued before `stop` set the flag; nothing that is
        // not durable yet gets admitted once it is set.
        if self.is_stopped() {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            return;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        self.admit(entries, response);
    }

    /// Admits `entries` into the open batch, which assigns their ids under
    /// the next object sequence. The caller waits for the object.
    fn admit(&mut self, entries: Vec<Entry>, response: AppendResponse) {
        // A region would run past the position range of the open batch: the
        // batch is sealed and the entries open the next one. The size limit
        // seals a batch long before a million entries of one region, so this
        // is a theoretical bound.
        if !self.open_batch.is_empty() && self.open_batch.would_exhaust_positions(&entries) {
            self.flush_open_batch();
            if let Some(error) = terminal(&self.terminal_error) {
                let _ = response.send(Err(shared(&error)));
                return;
            }
        }
        let Some(object_seq) = self.next_object_seq else {
            let error = self.poison(
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: OBJECT_SEQ_LIMIT - 1,
                }
                .build(),
            );
            let _ = response.send(Err(shared(&error)));
            return;
        };
        let last_entry_ids = match self.open_batch.admit(object_seq, entries) {
            Ok(last_entry_ids) => last_entry_ids,
            // Nothing was admitted: the append alone runs past the position
            // range, which no object can hold.
            Err(error) => {
                let _ = response.send(Err(error));
                return;
            }
        };
        for (region_id, entry_id) in &last_entry_ids {
            self.issued_entry_ids
                .entry(*region_id)
                .and_modify(|issued| *issued = (*issued).max(*entry_id))
                .or_insert(*entry_id);
        }
        self.pending.push(PendingAppend {
            last_entry_ids,
            response,
        });
        #[cfg(any(test, feature = "testing"))]
        self.admitted_appends.send_modify(|count| *count += 1);
        if self.open_batch.should_seal() {
            self.flush_open_batch();
        }
    }

    /// Seals the open batch as the object `next_object_seq` and starts its
    /// create when a slot is free. Returns whether a batch was sealed.
    fn flush_open_batch(&mut self) -> bool {
        if self.open_batch.is_empty() {
            return false;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            self.fail_unacknowledged(|| shared(&error));
            return false;
        }
        let Some(object_seq) = self.next_object_seq else {
            self.poison(
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: OBJECT_SEQ_LIMIT - 1,
                }
                .build(),
            );
            return false;
        };

        let (entries, _) = self.open_batch.seal();
        let encoded = match encode_batch(object_seq, self.writer_instance, entries) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.poison(error);
                return false;
            }
        };
        self.next_object_seq = object_seq
            .checked_add(1)
            .filter(|next_object_seq| *next_object_seq < OBJECT_SEQ_LIMIT);
        if self.next_object_seq.is_none() {
            // The batch takes the last representable sequence: it is created
            // and acknowledged, but no later batch can be allocated one.
            set_terminal(
                &self.terminal_error,
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: object_seq,
                }
                .build(),
            );
        }
        self.sealed.push_back(SealedBatch {
            object_seq,
            bytes: encoded.bytes,
            footer: encoded.footer,
            waiters: std::mem::take(&mut self.pending),
            #[cfg(any(test, feature = "testing"))]
            seal_waiters: Vec::new(),
            state: CreateState::Pending,
        });
        self.start_creates();
        true
    }

    /// Starts the creates of pending batches in sequence order while fewer
    /// than [`MAX_IN_FLIGHT_CREATES`] are in flight. Nothing starts once
    /// stop began.
    fn start_creates(&mut self) {
        if self.draining || self.is_stopped() {
            return;
        }
        let mut in_flight = self
            .sealed
            .iter()
            .filter(|batch| batch.is_in_flight())
            .count();
        for batch in self.sealed.iter_mut() {
            if in_flight >= MAX_IN_FLIGHT_CREATES {
                break;
            }
            if !matches!(batch.state, CreateState::Pending) {
                continue;
            }
            batch.state = CreateState::InFlight;
            in_flight += 1;
            let io = self.io.clone();
            let object_seq = batch.object_seq;
            let bytes = batch.bytes.clone();
            #[cfg(any(test, feature = "testing"))]
            let mut creates_held = self.creates_held.clone();
            #[cfg(any(test, feature = "testing"))]
            let creates_fail = self.creates_fail.clone();
            self.creates.push(Box::pin(async move {
                #[cfg(any(test, feature = "testing"))]
                let _ = creates_held.wait_for(|held| !*held).await;
                #[cfg(any(test, feature = "testing"))]
                if creates_fail.load(Ordering::Acquire) {
                    let error = object_store::Error::new(
                        object_store::ErrorKind::Unexpected,
                        "injected create failure",
                    )
                    .set_temporary();
                    let result = Err(error).context(crate::error::WalObjectStoreSnafu {
                        operation: "write",
                        path: io.object_path(object_seq),
                    });
                    return (object_seq, result);
                }
                (object_seq, io.put_if_absent(object_seq, bytes).await)
            }));
        }
    }

    fn on_create_completed(&mut self, object_seq: u64, result: Result<PutResult>) {
        // A batch the store gave up on when it poisoned itself: the object
        // may exist, but nothing was acknowledged for it.
        let Some(index) = self
            .sealed
            .iter()
            .position(|batch| batch.object_seq == object_seq)
        else {
            return;
        };
        match result {
            Ok(_) => self.sealed[index].state = CreateState::Created,
            // The object store did not confirm the object. The caller retries
            // the append itself, so the batch fails once it is known that no
            // later object exists.
            Err(error @ Error::WalObjectStore { .. }) => {
                self.sealed[index].state = CreateState::Failed(Arc::new(error));
                self.draining = true;
            }
            Err(error) => {
                self.poison(error);
                return;
            }
        }
        self.settle();
    }

    /// Indexes and acknowledges the sealed batches from the front as far as
    /// they are created, resolves a failed batch at the front once nothing is
    /// in flight, and starts the creates that a free slot allows.
    fn settle(&mut self) {
        enum Next {
            Wait,
            Index,
            Gap {
                object_seq: u64,
                later_object_seq: u64,
            },
            RollBack {
                object_seq: u64,
                error: Arc<Error>,
            },
        }
        while let Some(front) = self.sealed.front() {
            let next = match &front.state {
                CreateState::Pending | CreateState::InFlight => Next::Wait,
                CreateState::Created => Next::Index,
                CreateState::Failed(error) => {
                    if self.sealed.iter().any(SealedBatch::is_in_flight) {
                        Next::Wait
                    } else if let Some(later) = self
                        .sealed
                        .iter()
                        .skip(1)
                        .find(|batch| matches!(batch.state, CreateState::Created))
                    {
                        Next::Gap {
                            object_seq: front.object_seq,
                            later_object_seq: later.object_seq,
                        }
                    } else {
                        Next::RollBack {
                            object_seq: front.object_seq,
                            error: error.clone(),
                        }
                    }
                }
            };
            match next {
                Next::Wait => break,
                Next::Index => {
                    if !self.index_front() {
                        break;
                    }
                }
                Next::Gap {
                    object_seq,
                    later_object_seq,
                } => {
                    self.poison(
                        WalObjectHistoryGapSnafu {
                            object_seq,
                            later_object_seq,
                        }
                        .build(),
                    );
                    break;
                }
                Next::RollBack { object_seq, error } => {
                    self.roll_back(object_seq, error);
                    break;
                }
            }
        }
        self.start_creates();
    }

    /// Indexes the created object at the front and acknowledges its waiters.
    /// Returns false when the catalog rejected it, which poisons the store.
    fn index_front(&mut self) -> bool {
        let Some(front) = self.sealed.front() else {
            return false;
        };
        let indexed = self
            .catalog
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert_object(front.object_seq, front.footer.clone());
        if let Err(error) = indexed {
            self.poison(error);
            return false;
        }
        let Some(batch) = self.sealed.pop_front() else {
            return false;
        };
        for waiter in batch.waiters {
            let _ = waiter.response.send(Ok(AppendBatchResponse {
                last_entry_ids: waiter.last_entry_ids,
            }));
        }
        #[cfg(any(test, feature = "testing"))]
        for waiter in batch.seal_waiters {
            let _ = waiter.send(Ok(()));
        }
        // Objects are indexed in sequence order, so every rolled back
        // sequence up to this one was written again and reconciled.
        if self
            .unresolved_object_seq
            .is_some_and(|unresolved| unresolved <= batch.object_seq)
        {
            self.unresolved_object_seq = None;
        }
        true
    }

    /// Drops every batch that is not durable after the batch `object_seq`
    /// failed to be created while no later object was created. Its sequence
    /// stays free and the entry ids are handed out again, so a retry of the
    /// same entries writes the same object. Waiters of a store that was
    /// stopped meanwhile learn that instead of the I/O error, like every
    /// other entry that never became durable.
    fn roll_back(&mut self, object_seq: u64, error: Arc<Error>) {
        self.next_object_seq = Some(object_seq);
        // Every batch that failed may have left an object behind.
        let failed = self
            .sealed
            .iter()
            .filter(|batch| matches!(batch.state, CreateState::Failed(_)))
            .map(|batch| batch.object_seq)
            .max();
        self.unresolved_object_seq = self.unresolved_object_seq.max(failed);
        self.draining = false;
        let stopped = self.is_stopped();
        let failure = || {
            if stopped {
                ObjectStoreWalStoppedSnafu.build()
            } else {
                shared(&error)
            }
        };
        for batch in self.sealed.drain(..) {
            batch.fail(failure);
        }
        self.reset_open_batch();
        self.fail_unacknowledged(failure);
    }

    /// Records `error` as terminal and fails every waiter that is not
    /// acknowledged with it, or with the stopped error if the store was
    /// stopped meanwhile. Creates in flight run to completion, but their
    /// outcome is ignored: the entries of an object they create were never
    /// acknowledged, like those of a crash between creation and
    /// acknowledgement. Returns the recorded error.
    fn poison(&mut self, error: Error) -> Arc<Error> {
        let error = set_terminal(&self.terminal_error, error);
        self.draining = false;
        let stopped = self.is_stopped();
        let failure = || {
            if stopped {
                ObjectStoreWalStoppedSnafu.build()
            } else {
                shared(&error)
            }
        };
        for batch in self.sealed.drain(..) {
            batch.fail(failure);
        }
        self.reset_open_batch();
        self.fail_unacknowledged(failure);
        error
    }

    /// Drops the entries of the open batch; the next admission hands out the
    /// same ids again under the sequence the batch is at.
    fn reset_open_batch(&mut self) {
        self.open_batch.reset();
    }

    /// Fails the waiters of the open batch.
    fn fail_unacknowledged(&mut self, error: impl Fn() -> Error) {
        for pending in self.pending.drain(..) {
            let _ = pending.response.send(Err(error()));
        }
    }

    /// Makes sure no id of the region at or below `entry_id` is assigned
    /// from now on, then records the obsolete watermark of the region;
    /// neither is done when the sequence cannot be raised.
    fn handle_obsolete(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        response: oneshot::Sender<Result<()>>,
    ) {
        let result = self.raise_sequence_floor(entry_id).map(|_| {
            record_obsolete(&self.obsolete_entry_ids, region_id, entry_id);
        });
        let _ = response.send(result);
    }

    /// Moves the sequence of the next object above the object that holds
    /// `entry_id` unless it is there already. The move fails while the next
    /// sequence is not settled: the open batch handed out ids under it, a
    /// create is in flight, or a rolled back batch may have left an object
    /// at it. Skipping such a sequence would leave an object that recovery
    /// indexes but this store never did, so the next create at that sequence
    /// has to reconcile it first. A floor that does not fit an entry id
    /// poisons the store like an exhausted sequence.
    fn raise_sequence_floor(&mut self, entry_id: EntryId) -> Result<()> {
        // Nothing is assigned an id after stop began.
        if self.is_stopped() {
            return Ok(());
        }
        if let Some(error) = terminal(&self.terminal_error) {
            return Err(shared(&error));
        }
        let sequence_floor = sequence_floor(entry_id);
        let Some(next_object_seq) = self.next_object_seq else {
            return Ok(());
        };
        // A failed create rolls the sequence back to the first batch that is
        // not durable, so ids below the floor are out of reach only when
        // that sequence is at or above it, not the speculative next one.
        let lowest_object_seq = self
            .sealed
            .front()
            .map_or(next_object_seq, |batch| batch.object_seq);
        if lowest_object_seq >= sequence_floor {
            return Ok(());
        }
        if let Some(batch) = self.sealed.front() {
            return WalObjectSequenceUnsettledSnafu {
                object_seq: batch.object_seq,
            }
            .fail();
        }
        ensure!(
            self.open_batch.is_empty()
                && self
                    .unresolved_object_seq
                    .is_none_or(|unresolved| unresolved < next_object_seq),
            WalObjectSequenceUnsettledSnafu {
                object_seq: next_object_seq,
            }
        );
        if sequence_floor < OBJECT_SEQ_LIMIT {
            self.next_object_seq = Some(sequence_floor);
            Ok(())
        } else {
            self.next_object_seq = None;
            let error = self.poison(
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: OBJECT_SEQ_LIMIT - 1,
                }
                .build(),
            );
            Err(shared(&error))
        }
    }

    /// Begins stopping. Nothing is admitted from now on; the open batch and
    /// the batches whose create has not started are dropped and their
    /// sequences freed. Stop is answered by [`finish_stop`](Self::finish_stop)
    /// once nothing is in flight.
    fn handle_stop(&mut self, response: oneshot::Sender<Result<()>>) {
        self.stop.push(response);
        self.reset_open_batch();
        for pending in self.pending.drain(..) {
            let _ = pending
                .response
                .send(Err(ObjectStoreWalStoppedSnafu.build()));
        }
        if let Some(index) = self
            .sealed
            .iter()
            .position(|batch| matches!(batch.state, CreateState::Pending))
        {
            self.next_object_seq = Some(self.sealed[index].object_seq);
            for batch in self.sealed.drain(index..) {
                batch.fail(|| ObjectStoreWalStoppedSnafu.build());
            }
        }
    }

    /// Answers the callers of `stop` once every sealed batch is settled and
    /// every create has completed. Returns true when the actor is done.
    fn finish_stop(&mut self) -> bool {
        if self.stop.is_empty() || !self.sealed.is_empty() || !self.creates.is_empty() {
            return false;
        }
        for response in self.stop.drain(..) {
            let _ = response.send(Ok(()));
        }
        true
    }

    #[cfg(any(test, feature = "testing"))]
    fn handle_seal(&mut self, response: oneshot::Sender<Result<()>>) {
        if self.is_stopped() {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            return;
        }
        if self.flush_open_batch()
            && let Some(batch) = self.sealed.back_mut()
        {
            batch.seal_waiters.push(response);
            return;
        }
        let result = terminal(&self.terminal_error).map_or(Ok(()), |error| Err(shared(&error)));
        let _ = response.send(result);
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

fn terminal(terminal_error: &TerminalError) -> Option<Arc<Error>> {
    terminal_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

/// Records `error` as the terminal error unless one is already recorded, and
/// returns the recorded one.
fn set_terminal(terminal_error: &TerminalError, error: Error) -> Arc<Error> {
    terminal_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get_or_insert_with(|| Arc::new(error))
        .clone()
}

/// Wraps an error that several callers receive.
fn shared(error: &Arc<Error>) -> Error {
    ObjectStoreWalSnafu.into_error(error.clone())
}

fn encode_batch(
    object_seq: u64,
    writer_instance: [u8; 16],
    entries: Vec<Entry>,
) -> Result<EncodedObject> {
    let records = entries
        .into_iter()
        .map(|entry| Record {
            region_id: entry.region_id(),
            entry_id: entry.entry_id(),
            payload: Bytes::from(entry.into_bytes()),
        })
        .collect::<Vec<_>>();
    encode_object(
        Header {
            object_seq,
            writer_instance,
        },
        &records,
    )
}

/// Rebuilds the catalog from object footers,
/// so recovery costs a few small reads per object however large the objects
/// are. Segments are not read; a segment checksum is verified by the read that
/// decodes it. Footers are fetched for up to [`RECOVERY_CONCURRENCY`] objects
/// at a time and indexed in sequence order, so the catalog checks the entry
/// ranges of every object against its predecessors like a sequential replay.
async fn recover(io: &dyn WalObjectIo) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
    let objects = io.list().await?;
    let mut catalog = ObjectCatalog::default();
    for (object, footer) in fetch_footers(io, objects, RECOVERY_CONCURRENCY).await? {
        catalog
            .insert_object(object.object_seq, footer)
            .with_context(|_| InvalidWalObjectSnafu { path: object.path })?;
    }
    finish_recovery(catalog)
}

fn finish_recovery(
    catalog: ObjectCatalog,
) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
    let next_object_seq = catalog.next_object_seq()?;
    let durable_entry_ids = durable_entry_ids(&catalog);
    Ok((catalog, next_object_seq, durable_entry_ids))
}

/// Fetches and verifies the footers of `objects`, up to `concurrency` objects
/// at a time, and returns them ordered by object sequence whatever the order
/// the fetches complete in. The first failure abandons the remaining fetches.
async fn fetch_footers(
    io: &dyn WalObjectIo,
    objects: Vec<ListedObject>,
    concurrency: usize,
) -> Result<Vec<(ListedObject, Vec<FooterEntry>)>> {
    let mut footers = futures::stream::iter(objects)
        .map(|object| async move {
            let footer = fetch_footer(io, &object).await?;
            Ok((object, footer))
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    footers.sort_unstable_by_key(|(object, _)| object.object_seq);
    Ok(footers)
}

/// Reads the header, trailer and footer of `object` and verifies them: the
/// header must carry the sequence of the key, the trailer must be well formed,
/// the footer must match the checksum the trailer holds and its segments must
/// tile the object body.
///
/// A short object is read whole. Otherwise the header and a window at the end
/// of the object are read concurrently, and the footer is read separately only
/// when it starts before the window.
async fn fetch_footer(io: &dyn WalObjectIo, object: &ListedObject) -> Result<Vec<FooterEntry>> {
    let ListedObject {
        object_seq, size, ..
    } = *object;
    let invalid = |source: Error| {
        InvalidWalObjectSnafu {
            path: object.path.clone(),
        }
        .into_error(source)
    };
    let object_len = usize::try_from(size)
        .ok()
        .filter(|len| *len >= MIN_OBJECT_LEN)
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!(
                "truncated object, expected at least {MIN_OBJECT_LEN} bytes, actual {size}"
            ),
        })
        .map_err(invalid)?;

    let window = object_len.min(RECOVERY_TAIL_WINDOW);
    let tail_start = object_len - window;
    let (head, tail) = if tail_start == 0 {
        let bytes = io.get(object_seq).await?;
        (bytes.clone(), bytes)
    } else {
        futures::try_join!(
            io.get_range(object_seq, 0, HEADER_LEN as u64),
            io.get_range(object_seq, tail_start as u64, window as u64)
        )?
    };
    if head.len() < HEADER_LEN || tail.len() != window {
        return Err(invalid(
            CorruptedWalObjectSnafu {
                reason: format!(
                    "object holds fewer bytes than the listed {size}, head {} bytes, tail {} bytes",
                    head.len(),
                    tail.len()
                ),
            }
            .build(),
        ));
    }

    let (trailer, footer_range) =
        locate_footer(object_seq, object_len, &head, &tail).map_err(invalid)?;
    let footer = if footer_range.start >= tail_start {
        tail.slice(footer_range.start - tail_start..footer_range.end - tail_start)
    } else {
        io.get_range(
            object_seq,
            footer_range.start as u64,
            footer_range.len() as u64,
        )
        .await?
    };
    let footer = decode_footer(&footer, trailer).map_err(invalid)?;
    verify_segment_ranges(&footer, footer_range.start).map_err(invalid)?;
    Ok(footer)
}

/// Verifies the header and trailer of the object `object_seq` of `object_len`
/// bytes from its first bytes `head` and its last bytes `tail`, and returns
/// the trailer with the range the footer occupies in the object.
fn locate_footer(
    object_seq: u64,
    object_len: usize,
    head: &[u8],
    tail: &[u8],
) -> Result<(FixedTrailer, Range<usize>)> {
    let header = decode_header(head)?;
    ensure!(
        header.object_seq == object_seq,
        CorruptedWalObjectSnafu {
            reason: format!(
                "header sequence {} does not match key sequence {object_seq}",
                header.object_seq
            ),
        }
    );
    let trailer = decode_trailer(&tail[tail.len() - TRAILER_LEN..])?;
    let footer_range = footer_range(trailer, object_len)?;
    Ok((trailer, footer_range))
}

fn durable_entry_ids(catalog: &ObjectCatalog) -> HashMap<RegionId, EntryId> {
    let mut entry_ids = HashMap::new();
    for (_, footer) in catalog.objects_in_order() {
        for entry in footer {
            entry_ids
                .entry(entry.region_id)
                .and_modify(|current: &mut EntryId| *current = (*current).max(entry.max_entry_id))
                .or_insert(entry.max_entry_id);
        }
    }
    entry_ids
}

/// Object access of the store, so tests can inject failures.
#[async_trait::async_trait]
pub(crate) trait WalObjectIo: Send + Sync {
    async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult>;

    async fn get(&self, object_seq: u64) -> Result<Bytes>;

    async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes>;

    async fn list(&self) -> Result<Vec<ListedObject>>;

    fn object_path(&self, object_seq: u64) -> String;
}

#[async_trait::async_trait]
impl WalObjectIo for ObjectStoreIo {
    async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
        ObjectStoreIo::put_if_absent(self, object_seq, content).await
    }

    async fn get(&self, object_seq: u64) -> Result<Bytes> {
        ObjectStoreIo::get(self, object_seq).await
    }

    async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
        ObjectStoreIo::get_range(self, object_seq, offset, len).await
    }

    async fn list(&self) -> Result<Vec<ListedObject>> {
        ObjectStoreIo::list(self).await
    }

    fn object_path(&self, object_seq: u64) -> String {
        ObjectStoreIo::object_path(self, object_seq)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use common_base::readable_size::ReadableSize;
    use common_error::ext::{ErrorExt, RetryHint};
    use object_store::services::Memory;
    use store_api::logstore::entry::{MultiplePartEntry, MultiplePartHeader};
    use tokio::time::timeout;

    use super::*;
    use crate::error::WalObjectStoreSnafu;
    use crate::object_store_wal::batch::{POSITION_LIMIT, entry_id};
    use crate::object_store_wal::format::{FOOTER_ENTRY_LEN, decode_object};

    const PREFIX: &str = "wal/datanodes/1/epochs/2";
    const WAIT: Duration = Duration::from_secs(30);

    fn memory_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap()
    }

    fn config(flush_interval: Duration, max_batch_bytes: u64) -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            storage_provider: String::new(),
            flush_interval,
            max_batch_bytes: ReadableSize(max_batch_bytes),
            ..Default::default()
        }
    }

    /// Every append reaches the size limit, so it is persisted on its own.
    fn eager() -> ObjectStoreWalConfig {
        config(Duration::from_secs(3600), 1)
    }

    /// Nothing is persisted until a test seals the open batch.
    fn manual() -> ObjectStoreWalConfig {
        config(Duration::from_secs(3600), u64::MAX)
    }

    async fn open(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
    ) -> Arc<ObjectStoreLogStore> {
        ObjectStoreLogStore::try_new(object_store, config, 1, 2)
            .await
            .unwrap()
    }

    fn region(number: u32) -> RegionId {
        RegionId::new(1, number)
    }

    fn provider(region_id: RegionId) -> Provider {
        Provider::object_store_provider(region_id, PREFIX.to_string())
    }

    fn latest(store: &ObjectStoreLogStore, region_id: RegionId) -> EntryId {
        store.latest_entry_id(&provider(region_id)).unwrap()
    }

    /// The id of the entry at `position` of its region in object `object_seq`.
    fn id(object_seq: u64, position: u64) -> EntryId {
        entry_id(object_seq, position)
    }

    fn entry(store: &ObjectStoreLogStore, region_id: RegionId, data: &str) -> Entry {
        store
            .entry(data.as_bytes().to_vec(), 0, region_id, &provider(region_id))
            .unwrap()
    }

    async fn append(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        data: &str,
    ) -> Result<AppendBatchResponse> {
        store
            .append_batch(vec![entry(store, region_id, data)])
            .await
    }

    fn spawn_append_batch(
        store: &Arc<ObjectStoreLogStore>,
        entries: Vec<Entry>,
    ) -> tokio::task::JoinHandle<Result<AppendBatchResponse>> {
        let store = store.clone();
        tokio::spawn(async move { store.append_batch(entries).await })
    }

    /// Appends `count` single-entry batches, waiting for each to be admitted
    /// before the next is sent, so they are admitted in order.
    async fn spawn_appends(
        store: &Arc<ObjectStoreLogStore>,
        region_id: RegionId,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<AppendBatchResponse>>> {
        let admitted = *store.admitted_appends.borrow();
        let mut handles = Vec::with_capacity(count);
        for index in 1..=count {
            let entries = vec![entry(store, region_id, &format!("a{index}"))];
            handles.push(spawn_append_batch(store, entries));
            store
                .wait_for_admitted_appends(admitted + index)
                .await
                .unwrap();
        }
        handles
    }

    async fn object_seqs(io: &dyn WalObjectIo) -> Vec<u64> {
        io.list()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.object_seq)
            .collect()
    }

    fn unwrap_shared(error: &Error) -> &Error {
        match error {
            Error::ObjectStoreWal { source, .. } => source,
            other => panic!("expected a shared error, actual {other:?}"),
        }
    }

    fn assert_stopped(error: &Error) {
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
    }

    /// Waits for the next create that `ParkedIo` parked.
    async fn next_create(
        parked: &mut mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
    ) -> (u64, oneshot::Sender<bool>) {
        timeout(WAIT, parked.recv()).await.unwrap().unwrap()
    }

    /// Waits for `count` parked creates and returns their releases by
    /// sequence.
    async fn parked_creates(
        parked: &mut mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        count: usize,
    ) -> HashMap<u64, oneshot::Sender<bool>> {
        let mut releases = HashMap::new();
        for _ in 0..count {
            let (object_seq, release) = next_create(parked).await;
            releases.insert(object_seq, release);
        }
        releases
    }

    async fn open_over(
        io: Arc<dyn WalObjectIo>,
        config: &ObjectStoreWalConfig,
    ) -> Arc<ObjectStoreLogStore> {
        ObjectStoreLogStore::open(io, config, PREFIX.to_string())
            .await
            .unwrap()
    }

    async fn open_parking_creates(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
    ) -> (
        Arc<ObjectStoreLogStore>,
        Arc<ParkedIo>,
        mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
    ) {
        let (io, parked) = ParkedIo::parking_creates(object_store);
        (open_over(io.clone(), config).await, io, parked)
    }

    /// Round-trips a command through the actor, so an assertion that
    /// something did not happen runs after the actor handled every command
    /// sent before. The open batch must be empty, as under the eager config.
    async fn round_trip_actor(store: &ObjectStoreLogStore) {
        store.seal_open_batch().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_rejects_invalid_config() {
        for config in [
            config(Duration::from_millis(9), 1),
            config(Duration::from_secs(1), 0),
            ObjectStoreWalConfig {
                prefix: "/absolute".to_string(),
                ..config(Duration::from_secs(1), 1)
            },
        ] {
            let error = ObjectStoreLogStore::try_new(memory_store(), &config, 1, 2)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(error, Error::InvalidWalObjectStore { .. }),
                "unexpected error for {config:?}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_store_recovers_only_its_node_and_generation() {
        let object_store = memory_store();
        let config = ObjectStoreWalConfig::default();
        let identities = [(1, 2), (3, 2), (1, 4)];
        for (index, (node_id, generation)) in identities.iter().enumerate() {
            let encoded = encode_object(
                Header {
                    object_seq: 0,
                    writer_instance: [0; 16],
                },
                &[Record {
                    region_id: region(1),
                    entry_id: index as u64 + 1,
                    payload: Bytes::from_static(b"entry"),
                }],
            )
            .unwrap();
            ObjectStoreIo::new(
                object_store.clone(),
                config.node_prefix(*node_id, *generation),
            )
            .unwrap()
            .put_if_absent(0, encoded.bytes)
            .await
            .unwrap();
        }
        // A root-level object must not be part of any node's recovery.
        let root_io = ObjectStoreIo::new(object_store.clone(), &config.prefix).unwrap();
        root_io
            .put_if_absent(0, Bytes::from_static(b"invalid"))
            .await
            .unwrap();
        for (index, (node_id, generation)) in identities.iter().enumerate() {
            let store =
                ObjectStoreLogStore::try_new(object_store.clone(), &config, *node_id, *generation)
                    .await
                    .unwrap();
            for (other_index, (other_node, other_generation)) in identities.iter().enumerate() {
                let provider = Provider::object_store_provider(
                    region(1),
                    config.node_prefix(*other_node, *other_generation),
                );
                if index == other_index {
                    assert_eq!(store.latest_entry_id(&provider).unwrap(), index as u64 + 1);
                } else {
                    assert!(matches!(
                        store.latest_entry_id(&provider),
                        Err(Error::MismatchedWalPrefix { .. })
                    ));
                }
            }
            let root_provider = Provider::object_store_provider(region(1), config.prefix.clone());
            assert!(matches!(
                store.latest_entry_id(&root_provider),
                Err(Error::MismatchedWalPrefix { .. })
            ));
            store.stop().await.unwrap();
        }
    }

    /// Rebuilds the catalog by decoding whole objects as a recovery oracle.
    async fn recover_by_decoding(
        io: &dyn WalObjectIo,
    ) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
        let mut catalog = ObjectCatalog::default();
        for ListedObject {
            object_seq, path, ..
        } in io.list().await?
        {
            let bytes = io.get(object_seq).await?;
            decode_object(&bytes)
                .and_then(|decoded| {
                    ensure!(
                        decoded.header.object_seq == object_seq,
                        CorruptedWalObjectSnafu {
                            reason: format!(
                                "header sequence {} does not match key sequence {object_seq}",
                                decoded.header.object_seq
                            ),
                        }
                    );
                    catalog.insert_object(object_seq, decoded.footer)
                })
                .with_context(|_| InvalidWalObjectSnafu { path })?;
        }
        finish_recovery(catalog)
    }

    fn catalog_contents(catalog: &ObjectCatalog) -> Vec<(u64, Vec<FooterEntry>)> {
        catalog
            .objects_in_order()
            .map(|(object_seq, footer)| (object_seq, footer.to_vec()))
            .collect()
    }

    async fn populate(object_store: &ObjectStore, objects: usize, regions: u32) {
        for object in 0..objects {
            let records = (1..=regions)
                .filter(|number| !(object + *number as usize).is_multiple_of(3))
                .map(|number| Record {
                    region_id: region(number),
                    entry_id: id(object as u64, 1),
                    payload: Bytes::from(format!("o{object}-r{number}")),
                })
                .collect::<Vec<_>>();
            put_records(object_store, object as u64, &records).await;
        }
    }

    async fn put_records(object_store: &ObjectStore, object_seq: u64, records: &[Record]) {
        let encoded = encode_object(
            Header {
                object_seq,
                writer_instance: [0; 16],
            },
            records,
        )
        .unwrap();
        ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .put_if_absent(object_seq, encoded.bytes)
            .await
            .unwrap();
    }

    fn object_path(object_store: &ObjectStore, object_seq: u64) -> String {
        ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .object_path(object_seq)
    }

    async fn corrupt_object(
        object_store: &ObjectStore,
        path: &str,
        corrupt: impl FnOnce(&mut Vec<u8>),
    ) {
        let mut bytes = object_store.read(path).await.unwrap().to_vec();
        corrupt(&mut bytes);
        object_store.write(path, bytes).await.unwrap();
    }

    fn footer_of(bytes: &[u8]) -> (FixedTrailer, Vec<FooterEntry>) {
        let trailer = decode_trailer(&bytes[bytes.len() - TRAILER_LEN..]).unwrap();
        let footer =
            decode_footer(&bytes[footer_range(trailer, bytes.len()).unwrap()], trailer).unwrap();
        (trailer, footer)
    }

    fn assert_invalid_object(error: &Error, path: &str, reason: &str) {
        match error {
            Error::InvalidWalObject {
                path: actual,
                source,
                ..
            } => {
                assert_eq!(path, actual);
                match &**source {
                    Error::CorruptedWalObject { reason: actual, .. } => assert!(
                        actual.contains(reason),
                        "expected reason to contain {reason:?}, actual {actual:?}"
                    ),
                    other => panic!("expected a corrupted object error, actual {other:?}"),
                }
            }
            other => panic!("expected an invalid object error, actual {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_store_footer_recovery_matches_full_decode() {
        let object_store = memory_store();
        populate(&object_store, 40, 5).await;
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();

        let (catalog, next_object_seq, durable) = recover(&io).await.unwrap();
        let (expected_catalog, expected_next_object_seq, expected_durable) =
            recover_by_decoding(&io).await.unwrap();

        assert_eq!(40, catalog_contents(&catalog).len());
        assert_eq!(
            catalog_contents(&expected_catalog),
            catalog_contents(&catalog)
        );
        assert_eq!(expected_next_object_seq, next_object_seq);
        assert_eq!(40, next_object_seq);
        assert_eq!(expected_durable, durable);
        assert_eq!(5, durable.len());

        let store = open(object_store, &eager()).await;
        for number in 1..=5 {
            let region_id = region(number);
            // The objects `populate` gave the region one entry each.
            let objects = (0..40u64)
                .filter(|object| !(object + number as u64).is_multiple_of(3))
                .collect::<Vec<_>>();
            assert_eq!(id(*objects.last().unwrap(), 1), durable[&region_id]);
            assert_eq!(durable[&region_id], latest(&store, region_id));
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_corrupted_trailer_version_and_footer() {
        type Corrupt = fn(&mut Vec<u8>);
        let cases: &[(&str, Corrupt)] = &[
            ("truncated object", |bytes| {
                bytes.truncate(MIN_OBJECT_LEN - 1)
            }),
            ("invalid header magic", |bytes| bytes[0] ^= 1),
            ("invalid footer range", |bytes| {
                let start = bytes.len() - TRAILER_LEN;
                bytes[start..start + 8].copy_from_slice(&0u64.to_be_bytes());
            }),
            ("overflows the object", |bytes| {
                let start = bytes.len() - TRAILER_LEN;
                bytes[start..start + 8].copy_from_slice(&u64::MAX.to_be_bytes());
            }),
            ("invalid trailer magic", |bytes| {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }),
            ("unsupported format version 2", |bytes| {
                bytes[8..10].copy_from_slice(&2u16.to_be_bytes());
            }),
            ("footer checksum mismatch", |bytes| {
                let (trailer, _) = footer_of(bytes);
                bytes[trailer.footer_offset as usize] ^= 1;
            }),
        ];
        for (reason, corrupt) in cases {
            let object_store = memory_store();
            populate(&object_store, 3, 2).await;
            let path = object_path(&object_store, 1);
            corrupt_object(&object_store, &path, *corrupt).await;

            let error = ObjectStoreLogStore::try_new(object_store, &eager(), 1, 2)
                .await
                .unwrap_err();
            assert_invalid_object(&error, &path, reason);
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_header_sequence_mismatch() {
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let encoded = encode_object(
            Header {
                object_seq: 0,
                writer_instance: [0; 16],
            },
            &[Record {
                region_id: region(1),
                entry_id: 1,
                payload: Bytes::from_static(b"a1"),
            }],
        )
        .unwrap();
        io.put_if_absent(5, encoded.bytes).await.unwrap();

        let error = ObjectStoreLogStore::try_new(object_store, &eager(), 1, 2)
            .await
            .unwrap_err();
        assert_invalid_object(
            &error,
            &io.object_path(5),
            "header sequence 0 does not match key sequence 5",
        );
    }

    /// Overwrites the byte range of footer entry `index` and refreshes the
    /// footer checksum, so the footer is intact but describes the wrong bytes.
    fn rewrite_segment_range(bytes: &mut [u8], index: usize, offset: u64, len: u64) {
        let trailer_start = bytes.len() - TRAILER_LEN;
        let (trailer, _) = footer_of(bytes);
        let footer = footer_range(trailer, bytes.len()).unwrap();
        let entry = footer.start + 4 + index * FOOTER_ENTRY_LEN;
        bytes[entry + 28..entry + 36].copy_from_slice(&offset.to_be_bytes());
        bytes[entry + 36..entry + 44].copy_from_slice(&len.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[footer]);
        bytes[trailer_start + 16..trailer_start + 20].copy_from_slice(&checksum.to_be_bytes());
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_segment_ranges_that_do_not_tile_the_object() {
        type Corrupt = fn(&mut Vec<u8>, &[FooterEntry]);
        let cases: [(&str, Corrupt); 5] = [
            ("overflows the object", |bytes, footer| {
                rewrite_segment_range(bytes, 0, u64::MAX, footer[0].segment_len);
            }),
            ("invalid segment range", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset + 1, second.segment_len);
            }),
            ("invalid segment range", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset - 1, second.segment_len);
            }),
            ("invalid segment range", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset, second.segment_len + 1);
            }),
            ("segments end at", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset, second.segment_len - 1);
            }),
        ];
        for (reason, corrupt) in cases {
            let object_store = memory_store();
            populate(&object_store, 2, 3).await;
            let path = object_path(&object_store, 1);
            corrupt_object(&object_store, &path, |bytes| {
                let (_, footer) = footer_of(bytes);
                assert_eq!(2, footer.len());
                corrupt(bytes, &footer);
                // The footer itself still verifies.
                footer_of(bytes);
            })
            .await;

            let error = ObjectStoreLogStore::try_new(object_store.clone(), &eager(), 1, 2)
                .await
                .unwrap_err();
            assert_invalid_object(&error, &path, reason);
            let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
            let error = recover_by_decoding(&io).await.unwrap_err();
            assert_invalid_object(&error, &path, reason);
        }
    }
    #[tokio::test]
    async fn test_store_recovery_fetches_a_footer_longer_than_the_tail_window() {
        let object_store = memory_store();
        let regions = (RECOVERY_TAIL_WINDOW / FOOTER_ENTRY_LEN + 100) as u32;
        let records = (1..=regions)
            .map(|number| Record {
                region_id: region(number),
                entry_id: 1,
                payload: Bytes::from_static(b"wide"),
            })
            .collect::<Vec<_>>();
        put_records(&object_store, 0, &records).await;
        put_object(&object_store, 1, region(1), &[id(1, 1)]).await;

        let (io, reads) = RecordingIo::over(object_store.clone());
        let objects = io.list().await.unwrap();
        let wide = &objects[0];
        let (trailer, _) = footer_of(&object_store.read(&wide.path).await.unwrap().to_vec());
        assert!(trailer.footer_len > RECOVERY_TAIL_WINDOW as u64);

        let (catalog, next_object_seq, durable) = recover(io.as_ref()).await.unwrap();
        let (expected_catalog, expected_next_object_seq, expected_durable) =
            recover_by_decoding(io.as_ref()).await.unwrap();
        assert_eq!(
            catalog_contents(&expected_catalog),
            catalog_contents(&catalog)
        );
        assert_eq!(expected_next_object_seq, next_object_seq);
        assert_eq!(expected_durable, durable);
        assert_eq!(regions as usize, durable.len());

        // The wide object took the header, the tail window and the footer;
        // the narrow one was read whole.
        let mut wide_reads = reads
            .lock()
            .unwrap()
            .iter()
            .filter(|(object_seq, _, _)| *object_seq == wide.object_seq)
            .map(|(_, offset, len)| (*offset, *len))
            .collect::<Vec<_>>();
        wide_reads.sort_unstable();
        assert_eq!(
            vec![
                (0, HEADER_LEN as u64),
                (trailer.footer_offset, trailer.footer_len),
                (
                    wide.size - RECOVERY_TAIL_WINDOW as u64,
                    RECOVERY_TAIL_WINDOW as u64
                ),
            ],
            wide_reads
        );
        assert!(
            reads
                .lock()
                .unwrap()
                .iter()
                .all(|(object_seq, _, _)| *object_seq == wide.object_seq)
        );

        let store = open(object_store, &eager()).await;
        assert_eq!(id(1, 1), latest(&store, region(1)));
        assert_eq!(1, latest(&store, region(regions)));
        store.stop().await.unwrap();
    }

    async fn put_object(
        object_store: &ObjectStore,
        object_seq: u64,
        region_id: RegionId,
        entry_ids: &[EntryId],
    ) {
        let records = entry_ids
            .iter()
            .map(|entry_id| Record {
                region_id,
                entry_id: *entry_id,
                payload: Bytes::from(format!("e{entry_id}")),
            })
            .collect::<Vec<_>>();
        let encoded = encode_object(
            Header {
                object_seq,
                writer_instance: [0; 16],
            },
            &records,
        )
        .unwrap();
        ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .put_if_absent(object_seq, encoded.bytes)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_resumes_sequence_and_durable_ids() {
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let (_, next, durable) = recover(&io).await.unwrap();
        assert_eq!(0, next);
        assert!(durable.is_empty());

        put_object(&object_store, 2, region(1), &[1, id(5, 7)]).await;
        put_object(&object_store, 4, region(2), &[8]).await;
        let (_, next, durable) = recover(&io).await.unwrap();
        assert_eq!(6, next);
        assert_eq!(
            HashMap::from([(region(1), id(5, 7)), (region(2), 8)]),
            durable
        );
        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(id(5, 7), latest(&store, region(1)));
        assert_eq!(8, latest(&store, region(2)));
        assert_eq!(0, latest(&store, region(3)));

        // The sequence resumes above the object the largest id names, so the
        // first new id of every region is greater than every old one.
        let response = store
            .append_batch(vec![
                entry(&store, region(1), "a"),
                entry(&store, region(2), "b"),
            ])
            .await
            .unwrap();
        assert_eq!(
            HashMap::from([(region(1), id(6, 1)), (region(2), id(6, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![2, 4, 6], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            expected_entries(
                region(1),
                &[
                    (1, "e1"),
                    (id(5, 7), &format!("e{}", id(5, 7))),
                    (id(6, 1), "a")
                ]
            ),
            read_entries(&store, region(1), 0).await
        );
        store.stop().await.unwrap();

        // The sequence continues after the new object on restart.
        let store = open(object_store, &eager()).await;
        assert_eq!(id(6, 1), latest(&store, region(1)));
        let response = append(&store, region(2), "b2").await.unwrap();
        assert_eq!(
            HashMap::from([(region(2), id(7, 1))]),
            response.last_entry_ids
        );
        assert_eq!(
            expected_entries(region(2), &[(8, "e8"), (id(6, 1), "b"), (id(7, 1), "b2")]),
            read_entries(&store, region(2), 0).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_rejects_exhausted_sequence() {
        for (object_seq, entry_id) in [(u64::MAX, 1), (OBJECT_SEQ_LIMIT - 1, 1), (0, u64::MAX)] {
            let object_store = memory_store();
            put_object(&object_store, object_seq, region(1), &[entry_id]).await;
            let error = ObjectStoreLogStore::try_new(object_store, &eager(), 1, 2)
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::WalObjectSequenceExhausted { .. }),
                "{error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_conflicting_entry_ranges() {
        for entries in [[1, 2], [2, 3]] {
            let object_store = memory_store();
            put_object(&object_store, 0, region(1), &[1, 2]).await;
            put_object(&object_store, 1, region(1), &entries).await;
            let error = ObjectStoreLogStore::try_new(object_store.clone(), &eager(), 1, 2)
                .await
                .unwrap_err();
            assert_invalid_object(&error, &object_path(&object_store, 1), "entry");
        }
    }

    #[tokio::test]
    async fn test_store_rejects_foreign_providers() {
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        let other = Provider::object_store_provider(region_id, "other/prefix".to_string());
        let raft = Provider::raft_engine_provider(region_id.as_u64());
        for foreign in [&other, &raft] {
            let entry = Entry::Naive(NaiveEntry {
                provider: foreign.clone(),
                region_id,
                entry_id: 0,
                data: b"a1".to_vec(),
            });
            let errors = [
                store.append_batch(vec![entry]).await.unwrap_err(),
                store.latest_entry_id(foreign).unwrap_err(),
                store.read(foreign, 0, None).await.err().unwrap(),
                store.create_namespace(foreign).await.unwrap_err(),
                store.delete_namespace(foreign).await.unwrap_err(),
                store.obsolete(foreign, region_id, 1).await.unwrap_err(),
                store.obsolete_all(foreign, region_id).await.unwrap_err(),
                store.entry(Vec::new(), 1, region_id, foreign).unwrap_err(),
            ];
            for error in errors {
                if foreign == &raft {
                    assert!(matches!(error, Error::InvalidProvider { .. }), "{error:?}");
                } else {
                    assert!(
                        matches!(&error,
                        Error::MismatchedWalPrefix { expected, actual, .. }
                        if expected == PREFIX && actual == "other/prefix"),
                        "{error:?}"
                    );
                }
            }
        }
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_stop_is_idempotent() {
        let store = open(memory_store(), &eager()).await;
        let (first, second) = tokio::join!(store.stop(), store.stop());
        first.unwrap();
        second.unwrap();
        timeout(WAIT, store.command_tx.closed()).await.unwrap();
        store.stop().await.unwrap();
        assert!(store.stopped.load(Ordering::Acquire));
        // Even an empty append fails once stop began.
        assert_stopped(&store.append_batch(Vec::new()).await.unwrap_err());
    }

    /// Builds a store whose commands the test receives instead of an actor.
    fn store_without_actor() -> (ObjectStoreLogStore, mpsc::Receiver<Command>) {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        let store = ObjectStoreLogStore {
            prefix: PREFIX.to_string(),
            io: Arc::new(ObjectStoreIo::new(memory_store(), PREFIX).unwrap()),
            catalog: Arc::default(),
            obsolete_entry_ids: ObsoleteEntryIds::default(),
            terminal_error: Arc::default(),
            stopped: Arc::new(AtomicBool::new(false)),
            command_tx,
            admitted_appends: watch::channel(0).1,
            creates_held: watch::channel(false).0,
            creates_fail: Arc::default(),
        };
        (store, command_rx)
    }

    #[tokio::test]
    async fn test_store_stop_with_actor_gone() {
        let (store, command_rx) = store_without_actor();
        drop(command_rx);
        store.stop().await.unwrap();
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_actor_exits_when_the_store_is_dropped() {
        let store = open(memory_store(), &eager()).await;
        let io = Arc::downgrade(&store.io);
        let stopped = Arc::downgrade(&store.stopped);
        drop(store);
        timeout(WAIT, async {
            while stopped.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(io.upgrade().is_none());
    }

    #[tokio::test]
    async fn test_store_retains_first_terminal_error() {
        let store = open(memory_store(), &eager()).await;
        let first = set_terminal(
            &store.terminal_error,
            CorruptedWalObjectSnafu { reason: "first" }.build(),
        );
        let second = set_terminal(
            &store.terminal_error,
            CorruptedWalObjectSnafu { reason: "second" }.build(),
        );
        assert!(Arc::ptr_eq(&first, &second));
        let region_id = region(1);
        let provider = provider(region_id);
        let errors = [
            store.append_batch(Vec::new()).await.unwrap_err(),
            store.latest_entry_id(&provider).unwrap_err(),
            store.read(&provider, 0, None).await.err().unwrap(),
            store.create_namespace(&provider).await.unwrap_err(),
            store.delete_namespace(&provider).await.unwrap_err(),
            store.list_namespaces().await.unwrap_err(),
            store.obsolete(&provider, region_id, 1).await.unwrap_err(),
            store.obsolete_all(&provider, region_id).await.unwrap_err(),
            store
                .entry(Vec::new(), 1, region_id, &provider)
                .unwrap_err(),
        ];
        for error in errors {
            match error {
                Error::ObjectStoreWal { source, .. } => assert!(Arc::ptr_eq(&first, &source)),
                error => panic!("{error:?}"),
            }
        }
        assert!(store.obsolete_entry_ids.lock().unwrap().is_empty());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_bounds_concurrency_and_orders_by_sequence() {
        let object_store = memory_store();
        populate(&object_store, 16, 3).await;
        let (io, mut parked) = ParkedIo::over(object_store);
        let objects = io.list().await.unwrap();

        let mut fetch = {
            let io = io.clone();
            tokio::spawn(async move {
                fetch_footers(io.as_ref(), objects, RECOVERY_CONCURRENCY)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(object, _)| object.object_seq)
                    .collect::<Vec<_>>()
            })
        };
        let mut wave = Vec::new();
        for _ in 0..RECOVERY_CONCURRENCY {
            wave.push(timeout(WAIT, parked.recv()).await.unwrap().unwrap());
        }
        assert_eq!(
            (0..8).collect::<Vec<_>>(),
            wave.iter()
                .map(|(object_seq, _)| *object_seq)
                .collect::<Vec<_>>()
        );
        // No ninth fetch can start while eight are parked.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(parked.try_recv().is_err());

        // Complete the initial wave in reverse order, parking each replacement.
        let mut replacements = Vec::new();
        for (expected_next, (_, release)) in (8..16).zip(wave.into_iter().rev()) {
            release.send(true).unwrap();
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            assert_eq!(expected_next, object_seq);
            assert!(!fetch.is_finished());
            replacements.push(release);
        }
        for release in replacements {
            release.send(true).unwrap();
        }
        assert_eq!(
            (0..16).collect::<Vec<u64>>(),
            timeout(WAIT, &mut fetch).await.unwrap().unwrap()
        );
        assert_eq!(8, io.max_in_flight.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_store_recovery_abandons_pending_fetches_on_failure() {
        let object_store = memory_store();
        populate(&object_store, 16, 3).await;
        let (io, mut parked) = ParkedIo::over(object_store.clone());
        let fetch = {
            let io = io.clone();
            tokio::spawn(async move {
                ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await
            })
        };
        let mut wave = Vec::new();
        for _ in 0..RECOVERY_CONCURRENCY {
            wave.push(timeout(WAIT, parked.recv()).await.unwrap().unwrap());
        }
        let (failed_seq, release) = wave.pop().unwrap();
        release.send(false).unwrap();
        let error = timeout(WAIT, fetch).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
            if path == &io.object_path(failed_seq))
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());
        for (_, release) in wave {
            assert!(release.is_closed());
        }
        assert!(parked.try_recv().is_err());
        let store = open(object_store, &eager()).await;
        assert_eq!(id(15, 1), latest(&store, region(1)));
        assert_eq!(16, store.catalog.read().unwrap().next_object_seq().unwrap());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_reads_only_header_and_tail_of_large_object() {
        let object_store = memory_store();
        put_records(
            &object_store,
            0,
            &[Record {
                region_id: region(1),
                entry_id: 1,
                payload: Bytes::from(vec![0; RECOVERY_TAIL_WINDOW * 2]),
            }],
        )
        .await;
        let path = object_path(&object_store, 0);
        corrupt_object(&object_store, &path, |bytes| bytes[HEADER_LEN + 20] ^= 1).await;
        let (io, reads) = RecordingIo::over(object_store);
        let object = io.list().await.unwrap().remove(0);
        let store = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
            .await
            .unwrap();
        assert_eq!(1, latest(&store, region(1)));
        let mut reads = reads.lock().unwrap().clone();
        reads.sort_unstable();
        assert_eq!(
            vec![
                (0, 0, HEADER_LEN as u64),
                (
                    0,
                    object.size - RECOVERY_TAIL_WINDOW as u64,
                    RECOVERY_TAIL_WINDOW as u64
                )
            ],
            reads
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_short_read() {
        let object_store = memory_store();
        put_object(&object_store, 0, region(1), &[1]).await;
        let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        let mut object = io.list().await.unwrap().remove(0);
        object.size += 1;
        let error = fetch_footer(&io, &object).await.unwrap_err();
        assert_invalid_object(&error, &object.path, "fewer bytes than the listed");
    }

    async fn read_entries(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        start: EntryId,
    ) -> Vec<Entry> {
        store
            .read(&provider(region_id), start, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    fn expected_entries(region_id: RegionId, entries: &[(EntryId, &str)]) -> Vec<Entry> {
        entries
            .iter()
            .map(|(entry_id, data)| {
                Entry::Naive(NaiveEntry {
                    provider: provider(region_id),
                    region_id,
                    entry_id: *entry_id,
                    data: data.as_bytes().to_vec(),
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn test_store_reads_only_region_segments_in_entry_order() {
        let object_store = memory_store();
        let mut expected_ranges = HashMap::<_, Vec<_>>::new();
        for (seq, first) in [(1, 10), (3, 30), (7, 70)] {
            let records = [region(2), region(1)]
                .into_iter()
                .flat_map(|region_id| {
                    (first..first + 3).map(move |entry_id| Record {
                        region_id,
                        entry_id,
                        payload: Bytes::from(format!("r{}-e{entry_id}", region_id.region_number())),
                    })
                })
                .collect::<Vec<_>>();
            put_records(&object_store, seq, &records).await;
            let bytes = object_store
                .read(&object_path(&object_store, seq))
                .await
                .unwrap()
                .to_vec();
            for entry in footer_of(&bytes).1 {
                expected_ranges.entry(entry.region_id).or_default().push((
                    seq,
                    entry.segment_offset,
                    entry.segment_len,
                ));
            }
        }
        let (io, reads) = RecordingIo::over(object_store);
        let store = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
            .await
            .unwrap();
        for number in [1, 2] {
            reads.lock().unwrap().clear();
            let entries = read_entries(&store, region(number), 11).await;
            let ids = [11, 12, 30, 31, 32, 70, 71, 72];
            let expected = ids
                .into_iter()
                .map(|entry_id| {
                    Entry::Naive(NaiveEntry {
                        provider: provider(region(number)),
                        region_id: region(number),
                        entry_id,
                        data: format!("r{number}-e{entry_id}").into_bytes(),
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(expected, entries);
            assert_eq!(expected_ranges[&region(number)], *reads.lock().unwrap());
        }
        reads.lock().unwrap().clear();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&store, region(1), 73).await
        );
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&store, region(3), 0).await
        );
        assert!(reads.lock().unwrap().is_empty());
        assert_eq!(
            vec![provider(region(1)), provider(region(2))],
            store.list_namespaces().await.unwrap()
        );
        store.create_namespace(&provider(region(3))).await.unwrap();
        store.delete_namespace(&provider(region(1))).await.unwrap();
        assert_eq!(
            vec![provider(region(1)), provider(region(2))],
            store.list_namespaces().await.unwrap()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_obsolete_hides_entries_from_read_only() {
        let object_store = memory_store();
        put_object(&object_store, 0, region(1), &[10, 11, 12]).await;
        put_object(&object_store, 1, region(1), &[20]).await;
        put_object(&object_store, 2, region(2), &[10]).await;
        let store = open(object_store.clone(), &eager()).await;
        let p = provider(region(1));
        let all = expected_entries(
            region(1),
            &[(10, "e10"), (11, "e11"), (12, "e12"), (20, "e20")],
        );
        store.obsolete(&p, region(1), 9).await.unwrap();
        assert_eq!(all, read_entries(&store, region(1), 0).await);
        store.obsolete(&p, region(1), 11).await.unwrap();
        let remaining = expected_entries(region(1), &[(12, "e12"), (20, "e20")]);
        assert_eq!(remaining, read_entries(&store, region(1), 0).await);
        assert_eq!(
            expected_entries(region(1), &[(20, "e20")]),
            read_entries(&store, region(1), 20).await
        );
        assert_eq!(20, latest(&store, region(1)));
        store.obsolete(&p, region(1), 10).await.unwrap();
        assert_eq!(remaining, read_entries(&store, region(1), 0).await);
        store.obsolete_all(&p, region(1)).await.unwrap();
        store.obsolete(&p, region(1), 0).await.unwrap();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&store, region(1), 0).await
        );
        assert_eq!(20, latest(&store, region(1)));
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&store, region(2), 0).await
        );
        store.stop().await.unwrap();
        let reopened = open(object_store, &eager()).await;
        assert_eq!(all, read_entries(&reopened, region(1), 0).await);
        reopened.obsolete(&p, region(1), 20).await.unwrap();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&reopened, region(1), 0).await
        );
        assert_eq!(20, latest(&reopened, region(1)));
        reopened.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_entry_and_watermarks_reject_another_region() {
        let store = open(memory_store(), &eager()).await;
        let p = provider(region(2));
        let errors = [
            store
                .entry(b"payload".to_vec(), 7, region(1), &p)
                .unwrap_err(),
            store.obsolete(&p, region(1), 7).await.unwrap_err(),
            store.obsolete_all(&p, region(1)).await.unwrap_err(),
        ];
        for error in errors {
            assert!(
                matches!(&error, Error::MismatchedWalRegion { region_id, reason, .. }
                if *region_id == region(1) && reason == &format!("provider belongs to region {}", region(2)))
            );
            assert_eq!(
                common_error::status_code::StatusCode::InvalidArguments,
                error.status_code()
            );
        }
        assert!(store.obsolete_entry_ids.lock().unwrap().is_empty());
        assert_eq!(
            expected_entries(region(2), &[(7, "payload")]),
            vec![store.entry(b"payload".to_vec(), 7, region(2), &p).unwrap()]
        );
        assert!(store.list_namespaces().await.unwrap().is_empty());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_corrupted_segment_fails_the_read_that_decodes_it() {
        let object_store = memory_store();
        put_records(
            &object_store,
            0,
            &[
                Record {
                    region_id: region(1),
                    entry_id: 1,
                    payload: Bytes::from_static(b"a1"),
                },
                Record {
                    region_id: region(2),
                    entry_id: 1,
                    payload: Bytes::from_static(b"b1"),
                },
            ],
        )
        .await;
        put_object(&object_store, 1, region(2), &[10]).await;
        let path = object_path(&object_store, 0);
        let bytes = object_store.read(&path).await.unwrap().to_vec();
        let footer = footer_of(&bytes).1;
        let corrupt = &footer[1];
        corrupt_object(&object_store, &path, |bytes| {
            bytes[(corrupt.segment_offset + corrupt.segment_len - 1) as usize] ^= 1;
        })
        .await;
        let (io, reads) = RecordingIo::over(object_store);
        let store = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
            .await
            .unwrap();
        assert_eq!(1, latest(&store, region(1)));
        assert_eq!(10, latest(&store, region(2)));
        reads.lock().unwrap().clear();
        assert_eq!(
            expected_entries(region(1), &[(1, "a1")]),
            read_entries(&store, region(1), 0).await
        );
        assert_eq!(
            vec![(0, footer[0].segment_offset, footer[0].segment_len)],
            *reads.lock().unwrap()
        );
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&store, region(2), 10).await
        );
        reads.lock().unwrap().clear();
        let error = store
            .read(&provider(region(2)), 0, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_invalid_object(
            &error,
            &path,
            &format!("segment of region {} checksum mismatch", region(2)),
        );
        assert_eq!(
            vec![(0, corrupt.segment_offset, corrupt.segment_len)],
            *reads.lock().unwrap()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_assigns_ids_from_the_object_sequence() {
        let store = open(memory_store(), &manual()).await;
        let region_one = region(1);
        let region_two = region(2);

        // Appends admitted into one batch share its object; the regions take
        // their own positions in admission order, and every append learns
        // the last id of each of its regions.
        let first = spawn_append_batch(
            &store,
            vec![
                entry(&store, region_one, "a1"),
                entry(&store, region_two, "b1"),
                entry(&store, region_one, "a2"),
            ],
        );
        store.wait_for_admitted_appends(1).await.unwrap();
        let second = spawn_append_batch(&store, vec![entry(&store, region_two, "b2")]);
        store.wait_for_admitted_appends(2).await.unwrap();
        assert!(!first.is_finished());
        assert_eq!(0, latest(&store, region_one));
        store.seal_open_batch().await.unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(0, 2)), (region_two, id(0, 1))]),
            first.await.unwrap().unwrap().last_entry_ids
        );
        assert_eq!(
            HashMap::from([(region_two, id(0, 2))]),
            second.await.unwrap().unwrap().last_entry_ids
        );

        // The next object starts every region at position one again.
        let third = spawn_append_batch(
            &store,
            vec![
                entry(&store, region_two, "b3"),
                entry(&store, region_one, "a3"),
            ],
        );
        store.wait_for_admitted_appends(3).await.unwrap();
        store.seal_open_batch().await.unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 1)), (region_two, id(1, 1))]),
            third.await.unwrap().unwrap().last_entry_ids
        );
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            expected_entries(region_one, &[(1, "a1"), (2, "a2"), (id(1, 1), "a3")]),
            read_entries(&store, region_one, 1).await
        );
        assert_eq!(
            expected_entries(region_two, &[(2, "b2"), (id(1, 1), "b3")]),
            read_entries(&store, region_two, 2).await
        );
        assert_eq!(id(1, 1), latest(&store, region_one));
        assert_eq!(id(1, 1), latest(&store, region_two));
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_seals_when_a_region_exhausts_its_positions() {
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);
        let entries_of = |count: u64| {
            (0..count)
                .map(|_| entry(&store, region_id, ""))
                .collect::<Vec<_>>()
        };

        // The open batch holds every position of the region.
        let full = spawn_append_batch(&store, entries_of(POSITION_LIMIT - 1));
        store.wait_for_admitted_appends(1).await.unwrap();
        assert!(object_seqs(store.io.as_ref()).await.is_empty());

        // The next entry of the region seals the batch and opens the next
        // object; another region would still have fit.
        let next = spawn_append_batch(&store, vec![entry(&store, region_id, "next")]);
        store.wait_for_admitted_appends(2).await.unwrap();
        let response = timeout(WAIT, full).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(0, POSITION_LIMIT - 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        // An append that alone runs past the range fits no object: it seals
        // the open batch like any append the batch cannot take, then it is
        // refused without poisoning the store.
        let error = store
            .append_batch(entries_of(POSITION_LIMIT))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::WalEntryPositionExhausted { .. }),
            "unexpected error: {error:?}"
        );
        let response = timeout(WAIT, next).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        let after = spawn_append_batch(&store, vec![entry(&store, region_id, "after")]);
        store.wait_for_admitted_appends(3).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, after).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 1, 2], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "next"), (id(2, 1), "after")]),
            read_entries(&store, region_id, id(1, 1)).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_flushes_at_the_minimum_interval() {
        // An append is acknowledged once its object is durable, so a completed
        // append proves the tick after it sealed the batch. The appends are
        // spaced far apart, so every one of them lands in its own tick and
        // the ticks in between find an empty batch.
        let interval = MIN_FLUSH_INTERVAL;
        let store = open(memory_store(), &config(interval, u64::MAX)).await;
        let data = ["a1", "a2", "a3"];
        for (index, data) in data.iter().enumerate() {
            tokio::time::sleep(interval * 5).await;
            timeout(WAIT, append(&store, region(1), data))
                .await
                .unwrap()
                .unwrap();
            let expected_seqs = (0..=index as u64).collect::<Vec<_>>();
            assert_eq!(expected_seqs, object_seqs(store.io.as_ref()).await);
        }

        // Ticks with an empty open batch do not create objects.
        tokio::time::sleep(interval * 5).await;
        let seqs = object_seqs(store.io.as_ref()).await;
        assert_eq!(vec![0, 1, 2], seqs);
        for object_seq in seqs {
            let bytes = store.io.get(object_seq).await.unwrap();
            let decoded = decode_object(&bytes).unwrap();
            assert_eq!(1, decoded.records.len(), "object {object_seq} is empty");
        }
        assert_eq!(
            expected_entries(
                region(1),
                &[(id(0, 1), "a1"), (id(1, 1), "a2"), (id(2, 1), "a3")]
            ),
            read_entries(&store, region(1), 1).await
        );
    }

    #[tokio::test]
    async fn test_store_rejects_entries_of_another_region() {
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);

        let entry = Entry::Naive(NaiveEntry {
            provider: provider(region_id),
            region_id: region(2),
            entry_id: 0,
            data: Vec::new(),
        });
        let error = store.append_batch(vec![entry]).await.unwrap_err();
        assert!(
            matches!(error, Error::MismatchedWalRegion { .. }),
            "unexpected error: {error:?}"
        );
        let incomplete = Entry::MultiplePart(MultiplePartEntry {
            provider: provider(region_id),
            region_id,
            entry_id: 0,
            headers: vec![MultiplePartHeader::First],
            parts: vec![b"a1".to_vec()],
        });
        let error = store.append_batch(vec![incomplete]).await.unwrap_err();
        assert!(
            matches!(error, Error::IncompleteWalEntry { region_id: actual, .. } if actual == region_id),
            "unexpected error: {error:?}"
        );
        assert_eq!(
            common_error::status_code::StatusCode::InvalidArguments,
            error.status_code()
        );
        assert!(
            store
                .append_batch(Vec::new())
                .await
                .unwrap()
                .last_entry_ids
                .is_empty()
        );
        // Nothing was admitted.
        store.seal_open_batch().await.unwrap();
        assert!(object_seqs(store.io.as_ref()).await.is_empty());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_pipelines_creates_and_acknowledges_in_sequence_order() {
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 2).await;

        // At most the limit of creates run at a time; the rest wait for a slot.
        let mut releases = parked_creates(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        assert_eq!(
            (0..MAX_IN_FLIGHT_CREATES as u64).collect::<BTreeSet<_>>(),
            releases.keys().copied().collect::<BTreeSet<_>>()
        );
        round_trip_actor(&store).await;
        assert!(parked.try_recv().is_err());
        assert!(appends.iter().all(|append| !append.is_finished()));

        // Objects 2 and 1 become durable before object 0 and free a slot
        // each, but nothing is acknowledged ahead of object 0.
        for (released, expected_next) in [(2, 4), (1, 5)] {
            releases.remove(&released).unwrap().send(true).unwrap();
            let (object_seq, release) = next_create(&mut parked).await;
            assert_eq!(expected_next, object_seq);
            releases.insert(object_seq, release);
        }
        round_trip_actor(&store).await;
        assert_eq!(vec![1, 2], object_seqs(io.as_ref()).await);
        assert!(appends.iter().all(|append| !append.is_finished()));
        assert_eq!(0, latest(&store, region_id));

        // Object 0 releases the acknowledgements of objects 0 to 2.
        releases.remove(&0).unwrap().send(true).unwrap();
        let mut appends = appends.into_iter();
        for object_seq in 0..3 {
            let response = timeout(WAIT, appends.next().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(id(2, 1), latest(&store, region_id));

        // Object 5 before object 4: the append of object 5 waits for it.
        releases.remove(&3).unwrap().send(true).unwrap();
        releases.remove(&5).unwrap().send(true).unwrap();
        let fourth = timeout(WAIT, appends.next().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(3, 1))]),
            fourth.last_entry_ids
        );
        let appends = appends.collect::<Vec<_>>();
        round_trip_actor(&store).await;
        assert!(appends.iter().all(|append| !append.is_finished()));
        releases.remove(&4).unwrap().send(true).unwrap();
        for (append, object_seq) in appends.into_iter().zip(4..) {
            let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(
            MAX_IN_FLIGHT_CREATES,
            io.max_in_flight.load(Ordering::SeqCst)
        );
        assert_eq!(vec![0, 1, 2, 3, 4, 5], object_seqs(io.as_ref()).await);
        assert_eq!(
            expected_entries(
                region_id,
                &[
                    (id(0, 1), "a1"),
                    (id(1, 1), "a2"),
                    (id(2, 1), "a3"),
                    (id(3, 1), "a4"),
                    (id(4, 1), "a5"),
                    (id(5, 1), "a6")
                ]
            ),
            read_entries(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_transient_failure_rolls_back_batches_that_were_not_created() {
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        // Every slot is taken; the last batch waits for one.
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
        let mut releases = parked_creates(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        round_trip_actor(&store).await;
        assert!(parked.try_recv().is_err());

        // Object 0 fails while the others are in flight: nothing is decided
        // until they complete, and the freed slots start no create. None of
        // the later objects is created either: every batch fails and the
        // sequence rolls back to object 0.
        releases.remove(&0).unwrap().send(false).unwrap();
        round_trip_actor(&store).await;
        assert!(appends.iter().all(|append| !append.is_finished()));
        for release in releases.into_values() {
            release.send(false).unwrap();
        }
        for append in appends {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
        }
        assert!(parked.try_recv().is_err());
        assert!(object_seqs(io.as_ref()).await.is_empty());
        assert_eq!(0, latest(&store, region_id));

        // The retried entries take the same sequences and ids.
        let retries = spawn_appends(&store, region_id, 2).await;
        for (_, release) in parked_creates(&mut parked, 2).await {
            release.send(true).unwrap();
        }
        for (retry, object_seq) in retries.into_iter().zip(0..) {
            let response = timeout(WAIT, retry).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
        assert_eq!(
            expected_entries(region_id, &[(id(0, 1), "a1"), (id(1, 1), "a2")]),
            read_entries(&store, region_id, 1).await
        );

        // Objects 2 and 3 may still exist from the failed creates, so the
        // floor cannot skip sequence 2 until the last of them is reconciled.
        let error = store
            .obsolete(&provider(region(2)), region(2), id(9, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 2, .. }
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_transient_failure_before_a_durable_object_poisons() {
        let object_store = memory_store();
        let (store, io, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let region_id = region(1);
        let third = entry(&store, region_id, "a3");
        let appends = spawn_appends(&store, region_id, 2).await;
        let mut releases = parked_creates(&mut parked, 2).await;

        // Object 1 is durable while object 0 failed: object 1 cannot be
        // rolled back, so the store poisons itself and acknowledges neither.
        releases.remove(&0).unwrap().send(false).unwrap();
        releases.remove(&1).unwrap().send(true).unwrap();
        for append in appends {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(
                    unwrap_shared(&error),
                    Error::WalObjectHistoryGap {
                        object_seq: 0,
                        later_object_seq: 1,
                        ..
                    }
                ),
                "unexpected error: {error:?}"
            );
        }
        assert_eq!(vec![1], object_seqs(io.as_ref()).await);
        let error = store.latest_entry_id(&provider(region_id)).unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectHistoryGap { .. }),
            "unexpected error: {error:?}"
        );
        let error = store.append_batch(vec![third]).await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectHistoryGap { .. }),
            "unexpected error: {error:?}"
        );
        store.stop().await.unwrap();

        // Recovery indexes the durable object: its entries were never
        // acknowledged, but they replay like those of a crash between the
        // creation of an object and its acknowledgement.
        let store = open(object_store, &eager()).await;
        assert_eq!(id(1, 1), latest(&store, region_id));
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "a2")]),
            read_entries(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_permanent_failure_before_a_durable_object_poisons() {
        let object_store = memory_store();
        let (store, _, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let region_id = region(1);
        let foreign = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        foreign
            .put_if_absent(0, Bytes::from_static(b"foreign"))
            .await
            .unwrap();
        let appends = spawn_appends(&store, region_id, 2).await;
        let mut releases = parked_creates(&mut parked, 2).await;

        // Object 1 is created; object 0 conflicts with the foreign object.
        releases.remove(&1).unwrap().send(true).unwrap();
        timeout(WAIT, async {
            while object_seqs(&foreign).await != [0, 1] {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        round_trip_actor(&store).await;
        assert!(appends.iter().all(|append| !append.is_finished()));
        releases.remove(&0).unwrap().send(true).unwrap();
        for append in appends {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
        let error = store.latest_entry_id(&provider(region_id)).unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_obsolete_raises_the_sequence_floor() {
        let store = open(memory_store(), &manual()).await;
        let region_one = region(1);
        let region_two = region(2);
        let mut admitted = 0;
        let mut spawn_append = |region_id, data: &str| {
            admitted += 1;
            (
                spawn_append_batch(&store, vec![entry(&store, region_id, data)]),
                admitted,
            )
        };
        let (first, count) = spawn_append(region_one, "a1");
        store.wait_for_admitted_appends(count).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, first).await.unwrap().unwrap().unwrap();
        assert_eq!(HashMap::from([(region_one, 1)]), response.last_entry_ids);
        let (second, count) = spawn_append(region_one, "a2");
        store.wait_for_admitted_appends(count).await.unwrap();

        // A durable watermark names an object below the next sequence and
        // changes nothing: the open batch goes on under sequence 1.
        store
            .obsolete(&provider(region_one), region_one, 1)
            .await
            .unwrap();
        let (third, count) = spawn_append(region_one, "a3");
        store.wait_for_admitted_appends(count).await.unwrap();

        // A watermark that names a later object, as one inherited from
        // another prefix does, cannot move the sequence while the open batch
        // handed out ids under it: neither the sequence nor the watermark
        // moves.
        let error = store
            .obsolete(&provider(region_two), region_two, id(3, 7))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 1, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );

        // Once the batch is durable the sequence moves above that object, so
        // the region's next id is greater than the watermark.
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, second).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 1))]),
            response.last_entry_ids
        );
        let response = timeout(WAIT, third).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 2))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        store
            .obsolete(&provider(region_two), region_two, id(3, 7))
            .await
            .unwrap();
        assert_eq!(
            Some(&id(3, 7)),
            store.obsolete_entry_ids.lock().unwrap().get(&region_two)
        );
        let (fourth, count) = spawn_append(region_two, "b1");
        store.wait_for_admitted_appends(count).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, fourth).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(4, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 1, 4], object_seqs(store.io.as_ref()).await);
        // The watermark hides nothing of this prefix; the region has no
        // entry at or below it.
        assert_eq!(
            expected_entries(region_two, &[(id(4, 1), "b1")]),
            read_entries(&store, region_two, 1).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_floor_does_not_skip_an_object_left_by_a_failed_create() {
        let (io, _) = RecordingIo::over(memory_store());
        let store = open_over(io.clone(), &eager()).await;
        let region_one = region(1);
        let region_two = region(2);

        // Object 0 is written, but its create is reported as failed: the
        // object exists and is not indexed.
        io.fail_after_next_put.store(true, Ordering::Relaxed);
        append(&store, region_one, "a1").await.unwrap_err();
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(0, latest(&store, region_one));

        // A floor from another region may not skip sequence 0: a later
        // object would carry the retry, and recovery would index both.
        let error = store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 0, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );

        // The identical retry reconciles sequence 0; now the floor applies.
        let response = append(&store, region_one, "a1").await.unwrap();
        assert_eq!(HashMap::from([(region_one, 1)]), response.last_entry_ids);
        store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap();
        let response = append(&store, region_two, "b1").await.unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(6, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 6], object_seqs(io.as_ref()).await);
        store.stop().await.unwrap();

        // Recovery indexes the same objects the store did: one copy of "a1".
        let store = open_over(io, &eager()).await;
        assert_eq!(
            expected_entries(region_one, &[(1, "a1")]),
            read_entries(&store, region_one, 0).await
        );
        assert_eq!(
            expected_entries(region_two, &[(id(6, 1), "b1")]),
            read_entries(&store, region_two, 0).await
        );
        assert_eq!(1, latest(&store, region_one));
    }

    #[tokio::test]
    async fn test_store_floor_is_measured_against_a_rollback_not_the_next_sequence() {
        let (store, _, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_one = region(1);
        let region_two = region(2);
        let pending = spawn_append_batch(&store, vec![entry(&store, region_one, "a1")]);
        let (_, release) = next_create(&mut parked).await;

        // Object 0 is in flight and the next sequence is 1, but a failure
        // rolls it back to 0: a watermark naming object 0 is not accepted.
        let error = store
            .obsolete(&provider(region_two), region_two, 1)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 0, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );
        release.send(false).unwrap();
        timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err();

        // The other region takes id 1 under sequence 0 and can read it back.
        let write = spawn_append_batch(&store, vec![entry(&store, region_two, "b1")]);
        next_create(&mut parked).await.1.send(true).unwrap();
        let response = timeout(WAIT, write).await.unwrap().unwrap().unwrap();
        assert_eq!(HashMap::from([(region_two, 1)]), response.last_entry_ids);
        assert_eq!(1, latest(&store, region_two));
        assert_eq!(
            expected_entries(region_two, &[(1, "b1")]),
            read_entries(&store, region_two, 0).await
        );
    }

    #[tokio::test]
    async fn test_store_obsolete_queued_behind_stop_records_the_watermark() {
        let (store, mut command_rx) = store_without_actor();
        let region_id = region(1);
        let provider_one = provider(region_id);

        // The actor exits with the command still queued and never answers
        // it: nothing is assigned an id any more, so the watermark alone holds.
        let (result, ()) = tokio::join!(store.obsolete(&provider_one, region_id, 1), async {
            let command = timeout(WAIT, command_rx.recv()).await.unwrap();
            assert!(matches!(
                command,
                Some(Command::Obsolete { entry_id: 1, .. })
            ));
        });
        result.unwrap();
        assert_eq!(
            Some(&1),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );

        // The same holds for a call that finds the actor gone.
        drop(command_rx);
        store
            .obsolete(&provider(region(2)), region(2), id(1, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&id(1, 1)),
            store.obsolete_entry_ids.lock().unwrap().get(&region(2))
        );
    }

    #[tokio::test]
    async fn test_store_hooks_hold_and_fail_creates() {
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        store.hold_creates();
        let held = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        store.wait_for_admitted_appends(1).await.unwrap();
        round_trip_actor(&store).await;
        assert!(!held.is_finished());
        assert!(object_seqs(store.io.as_ref()).await.is_empty());
        store.release_creates();
        timeout(WAIT, held).await.unwrap().unwrap().unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        store.fail_creates();
        let error = append(&store, region_id, "a2").await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        store.begin_stop();
        assert_stopped(&append(&store, region_id, "a3").await.unwrap_err());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_rollback_and_poison_drop_the_open_batch() {
        let object_store = memory_store();
        let (store, io, mut parked) = open_parking_creates(object_store.clone(), &manual()).await;
        let region_id = region(1);
        let spawn_seal = || {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };

        // Object 0 is in flight and a2 waits in the open batch under
        // sequence 1 when the create fails: both roll back.
        let sealed = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        store.wait_for_admitted_appends(1).await.unwrap();
        let seal = spawn_seal();
        let (_, release) = next_create(&mut parked).await;
        let open = spawn_append_batch(&store, vec![entry(&store, region_id, "a2")]);
        store.wait_for_admitted_appends(2).await.unwrap();
        release.send(false).unwrap();
        for append in [sealed, open] {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                "unexpected error: {error:?}"
            );
        }
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();

        // The next entry takes the first id of sequence 0 again, alone.
        let retry = spawn_append_batch(&store, vec![entry(&store, region_id, "b1")]);
        store.wait_for_admitted_appends(3).await.unwrap();
        let seal = spawn_seal();
        next_create(&mut parked).await.1.send(true).unwrap();
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap();
        let response = timeout(WAIT, retry).await.unwrap().unwrap().unwrap();
        assert_eq!(HashMap::from([(region_id, 1)]), response.last_entry_ids);
        assert_eq!(
            expected_entries(region_id, &[(1, "b1")]),
            read_entries(&store, region_id, 0).await
        );

        // Object 1 conflicts while c2 waits in the open batch: both fail.
        let sealed = spawn_append_batch(&store, vec![entry(&store, region_id, "c1")]);
        store.wait_for_admitted_appends(4).await.unwrap();
        let seal = spawn_seal();
        let (_, release) = next_create(&mut parked).await;
        let open = spawn_append_batch(&store, vec![entry(&store, region_id, "c2")]);
        store.wait_for_admitted_appends(5).await.unwrap();
        ObjectStoreIo::new(object_store, PREFIX)
            .unwrap()
            .put_if_absent(1, Bytes::from_static(b"foreign"))
            .await
            .unwrap();
        release.send(true).unwrap();
        for append in [sealed, open] {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_poisons_once_the_last_sequence_is_taken() {
        let object_store = memory_store();
        let last = OBJECT_SEQ_LIMIT - 1;
        put_object(&object_store, last - 1, region(1), &[id(last - 1, 1)]).await;
        let store = open(object_store, &eager()).await;
        let next = entry(&store, region(1), "next");

        // The batch at the last sequence is created and acknowledged, and the
        // store is poisoned as soon as it takes that sequence.
        let response = append(&store, region(1), "last").await.unwrap();
        assert_eq!(
            HashMap::from([(region(1), id(last, 1))]),
            response.last_entry_ids
        );
        for error in [
            store.latest_entry_id(&provider(region(1))).unwrap_err(),
            store.append_batch(vec![next]).await.unwrap_err(),
        ] {
            assert!(
                matches!(
                    unwrap_shared(&error),
                    Error::WalObjectSequenceExhausted { last_object_seq, .. } if *last_object_seq == last
                ),
                "unexpected error: {error:?}"
            );
        }
        store.stop().await.unwrap();
    }

    /// Starts `stop` and waits until it has set the stopped flag.
    async fn begin_spawned_stop(
        store: &Arc<ObjectStoreLogStore>,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        stop
    }

    #[tokio::test]
    async fn test_store_stop_during_failed_flush_reports_stopped() {
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let pending = spawn_append_batch(&store, vec![entry(&store, region(1), "a1")]);
        let (_, release) = next_create(&mut parked).await;
        let stop = begin_spawned_stop(&store).await;
        assert!(!stop.is_finished());

        // The create fails after stop began: its waiter learns of the stop.
        release.send(false).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        assert_stopped(&timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err());
        assert!(object_seqs(io.as_ref()).await.is_empty());
        assert_stopped(&append(&store, region(1), "a2").await.unwrap_err());
    }

    #[tokio::test]
    async fn test_store_stop_with_several_creates_in_flight() {
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        // Four creates are in flight, the fifth batch waits for a slot.
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
        let mut releases = parked_creates(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        assert!(parked.try_recv().is_err());

        let stop = begin_spawned_stop(&store).await;
        assert!(!stop.is_finished());
        // Nothing is assigned an id after stop began, so a watermark needs no
        // floor even while creates are in flight.
        store
            .obsolete(&provider(region(2)), region(2), id(9, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&id(9, 1)),
            store.obsolete_entry_ids.lock().unwrap().get(&region(2))
        );

        // The creates in flight run to completion in any order and are
        // acknowledged; the batch that never started learns of the stop.
        for object_seq in [2, 0, 3, 1] {
            releases.remove(&object_seq).unwrap().send(true).unwrap();
        }
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        let mut appends = appends.into_iter();
        for object_seq in 0..MAX_IN_FLIGHT_CREATES as u64 {
            let response = timeout(WAIT, appends.next().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        let error = timeout(WAIT, appends.next().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_stopped(&error);
        // No create was started after stop began.
        assert!(parked.try_recv().is_err());
        assert_eq!(vec![0, 1, 2, 3], object_seqs(io.as_ref()).await);
        assert_eq!(id(3, 1), latest(&store, region_id));
        timeout(WAIT, store.command_tx.closed()).await.unwrap();
    }

    #[tokio::test]
    async fn test_store_stop_during_conflicting_flush_reports_stopped_and_poisons() {
        let object_store = memory_store();
        let (store, _, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let region_id = region(1);
        let pending = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        let (_, release) = next_create(&mut parked).await;
        let stop = begin_spawned_stop(&store).await;
        // The sequence is taken by different content before the create runs.
        ObjectStoreIo::new(object_store, PREFIX)
            .unwrap()
            .put_if_absent(0, Bytes::from_static(b"foreign"))
            .await
            .unwrap();

        release.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        assert_stopped(&timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err());
        for error in [
            store.latest_entry_id(&provider(region_id)).unwrap_err(),
            store
                .read(&provider(region_id), 1, None)
                .await
                .err()
                .unwrap(),
        ] {
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
    }

    fn injected_failure<T>(operation: &'static str, path: String) -> Result<T> {
        Err(
            object_store::Error::new(object_store::ErrorKind::Unexpected, "injected failure")
                .set_temporary(),
        )
        .context(WalObjectStoreSnafu { operation, path })
    }

    /// Object access whose whole-object reads, or conditional creates, park
    /// until the test releases them, counting how many are in flight. A
    /// release of false fails the operation with a transient error before it
    /// reaches the object store.
    struct ParkedIo {
        inner: ObjectStoreIo,
        parked: mpsc::UnboundedSender<(u64, oneshot::Sender<bool>)>,
        park_creates: bool,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl ParkedIo {
        fn over(
            object_store: ObjectStore,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            Self::new(object_store, false)
        }

        fn parking_creates(
            object_store: ObjectStore,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            Self::new(object_store, true)
        }

        fn new(
            object_store: ObjectStore,
            park_creates: bool,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            let (parked, parked_rx) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                    parked,
                    park_creates,
                    in_flight: AtomicUsize::new(0),
                    max_in_flight: AtomicUsize::new(0),
                }),
                parked_rx,
            )
        }

        /// Parks until the test releases `object_seq` and returns whether the
        /// operation proceeds.
        async fn park(&self, object_seq: u64) -> bool {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            let (release, released) = oneshot::channel();
            self.parked.send((object_seq, release)).unwrap();
            let proceed = released.await.unwrap();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            proceed
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for ParkedIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            if self.park_creates && !self.park(object_seq).await {
                return injected_failure("write", self.object_path(object_seq));
            }
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            if !self.park_creates && !self.park(object_seq).await {
                return injected_failure("read", self.object_path(object_seq));
            }
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    type RangeReads = Arc<Mutex<Vec<(u64, u64, u64)>>>;

    /// Object access that records every range read as (sequence, offset,
    /// length) and, on request, reports the next conditional create as failed
    /// after it wrote the object.
    struct RecordingIo {
        inner: ObjectStoreIo,
        reads: RangeReads,
        fail_after_next_put: AtomicBool,
    }

    impl RecordingIo {
        fn over(object_store: ObjectStore) -> (Arc<Self>, RangeReads) {
            let reads = RangeReads::default();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                reads: reads.clone(),
                fail_after_next_put: AtomicBool::new(false),
            };
            (Arc::new(io), reads)
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for RecordingIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            let result = self.inner.put_if_absent(object_seq, content).await?;
            if self.fail_after_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure("write", self.object_path(object_seq));
            }
            Ok(result)
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.reads.lock().unwrap().push((object_seq, offset, len));
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }
}
