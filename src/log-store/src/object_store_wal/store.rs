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

//! Object store WAL construction, recovery and region reads.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use common_wal::config::object_store::ObjectStoreWalConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::{ObjectStoreProvider, Provider};
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream, WalIndex};
use store_api::storage::RegionId;
use tokio::sync::{mpsc, oneshot};

use crate::error::{
    CorruptedWalObjectSnafu, Error, InvalidProviderSnafu, InvalidWalObjectSnafu,
    InvalidWalObjectStoreSnafu, MismatchedWalPrefixSnafu, MismatchedWalRegionSnafu,
    ObjectStoreWalSnafu, Result, UnsupportedObjectStoreWalOperationSnafu,
    WalObjectSequenceExhaustedSnafu,
};
use crate::object_store_wal::batch::OBJECT_SEQ_LIMIT;
use crate::object_store_wal::catalog::ObjectCatalog;
use crate::object_store_wal::format::{
    ChainLink, FixedTrailer, FooterEntry, HEADER_LEN, Header, MIN_OBJECT_LEN, TRAILER_LEN,
    decode_footer, decode_header, decode_segment, decode_trailer, encode_object, footer_range,
    verify_segment_ranges,
};
use crate::object_store_wal::io::{ListedObject, ObjectStoreIo, PutResult};

const COMMAND_BUFFER: usize = 1024;
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
/// Number of objects whose footers recovery fetches at a time.
const RECOVERY_CONCURRENCY: usize = 8;
/// Bytes recovery reads from the end of an object in one request. The window
/// holds the trailer and the footer of an object with up to 1364 regions of
/// 48 bytes each, so a second request for the footer is rare.
const RECOVERY_TAIL_WINDOW: usize = 64 * 1024;

/// A log store over the immutable WAL objects under one prefix.
pub(crate) struct ObjectStoreLogStore {
    prefix: String,
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    obsolete_entry_ids: ObsoleteEntryIds,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    command_tx: mpsc::Sender<Command>,
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
    /// recovering the catalog from the objects that already exist and writing
    /// the object that starts the epoch of this instance. Recovery fails on the
    /// first corrupted or conflicting object.
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

        positive_bytes(config.max_batch_bytes.as_bytes(), "max batch bytes")?;
        let Recovered {
            mut catalog,
            next_object_seq,
            durable_entry_ids,
            tip,
            max_epoch,
        } = recover(io.as_ref()).await?;
        let writer_instance = uuid::Uuid::new_v4().into_bytes();
        let epoch = max_epoch.checked_add(1).context(CorruptedWalObjectSnafu {
            reason: "an object carries the largest epoch, no later epoch fits",
        })?;
        let start = start_epoch(io.as_ref(), next_object_seq, tip, writer_instance, epoch).await?;
        catalog
            .insert_object(start.object_seq, Vec::new())
            .with_context(|_| InvalidWalObjectSnafu {
                path: io.object_path(start.object_seq),
            })?;
        let catalog = Arc::new(RwLock::new(catalog));
        let terminal_error = TerminalError::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        let actor = Actor {
            terminal_error: terminal_error.clone(),
            stopped: stopped.clone(),
            command_rx,
            issued_entry_ids: durable_entry_ids,
            next_object_seq: start
                .object_seq
                .checked_add(1)
                .filter(|next_object_seq| *next_object_seq < OBJECT_SEQ_LIMIT),
            writer_instance,
            epoch,
            chain_tip: start,
            stop: Vec::new(),
        };
        common_runtime::spawn_global(actor.run());
        Ok(Arc::new(Self {
            prefix,
            io,
            catalog,
            obsolete_entry_ids: ObsoleteEntryIds::default(),
            terminal_error,
            stopped,
            command_tx,
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

#[async_trait::async_trait]
impl LogStore for ObjectStoreLogStore {
    type Error = Error;

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

    async fn append_batch(&self, _entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        UnsupportedObjectStoreWalOperationSnafu.fail()
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

    /// Records a memory-only watermark. The caller re-establishes it after a restart.
    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        self.check_region(provider, region_id)?;
        record_obsolete(&self.obsolete_entry_ids, region_id, entry_id);
        Ok(())
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

enum Command {
    Stop {
        response: oneshot::Sender<Result<()>>,
    },
}

struct Actor {
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    command_rx: mpsc::Receiver<Command>,
    issued_entry_ids: HashMap<RegionId, EntryId>,
    next_object_seq: Option<u64>,
    writer_instance: [u8; 16],
    epoch: u64,
    /// The object a newly sealed batch extends.
    chain_tip: ChainLink,
    stop: Vec<oneshot::Sender<Result<()>>>,
}

impl Actor {
    async fn run(mut self) {
        while let Some(Command::Stop { response }) = self.command_rx.recv().await {
            self.handle_stop(response);
            if self.finish_stop() {
                return;
            }
        }
    }

    fn handle_stop(&mut self, response: oneshot::Sender<Result<()>>) {
        self.stop.push(response);
    }

    fn finish_stop(&mut self) -> bool {
        if !self.is_stopped() || self.stop.is_empty() {
            return false;
        }
        for response in self.stop.drain(..) {
            let _ = response.send(Ok(()));
        }
        true
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

/// What recovery rebuilt from the objects under a prefix.
#[derive(Debug)]
struct Recovered {
    catalog: ObjectCatalog,
    /// Sequence above every present object and every id the catalog holds.
    next_object_seq: u64,
    durable_entry_ids: HashMap<RegionId, EntryId>,
    /// The object the next object extends, `None` on an empty prefix.
    tip: Option<ChainLink>,
    /// Largest epoch any present object carries, zero on an empty prefix.
    max_epoch: u64,
}

/// An object as recovery fetched it: its key, header and footer.
struct FetchedObject {
    object: ListedObject,
    header: Header,
    footer: Vec<FooterEntry>,
}

/// Rebuilds the catalog from object headers and footers,
/// so recovery costs a few small reads per object however large the objects
/// are. Segments are not read; a segment checksum is verified by the read that
/// decodes it. Footers are fetched for up to [`RECOVERY_CONCURRENCY`] objects
/// at a time and indexed in sequence order, so the catalog checks the entry
/// ranges of every object against its predecessors like a sequential replay.
/// Only the objects on the chain [`select_chain`] picks are indexed.
async fn recover(io: &dyn WalObjectIo) -> Result<Recovered> {
    let objects = io.list().await?;
    finish_recovery(fetch_footers(io, objects, RECOVERY_CONCURRENCY).await?)
}

/// Indexes the chain of `objects`, which are ordered by sequence. Objects off
/// the chain are orphans: they are not indexed, but no later object takes
/// their sequences.
fn finish_recovery(objects: Vec<FetchedObject>) -> Result<Recovered> {
    let headers = objects
        .iter()
        .map(|fetched| (fetched.object.object_seq, &fetched.header))
        .collect::<BTreeMap<_, _>>();
    let chain = select_chain(&headers);
    let tip = chain.last().map(|object_seq| ChainLink {
        object_seq: *object_seq,
        writer_instance: headers[object_seq].writer_instance,
    });
    let max_epoch = headers
        .values()
        .map(|header| header.epoch)
        .max()
        .unwrap_or(0);
    let after_listed = match headers.last_key_value() {
        None => 0,
        Some((&last_object_seq, _)) => last_object_seq
            .checked_add(1)
            .context(WalObjectSequenceExhaustedSnafu { last_object_seq })?,
    };
    let chain = chain.into_iter().collect::<HashSet<_>>();

    let mut catalog = ObjectCatalog::default();
    for FetchedObject { object, footer, .. } in objects {
        if chain.contains(&object.object_seq) {
            catalog
                .insert_object(object.object_seq, footer)
                .with_context(|_| InvalidWalObjectSnafu { path: object.path })?;
        }
    }
    let next_object_seq = catalog.next_object_seq()?.max(after_listed);
    ensure!(
        next_object_seq < OBJECT_SEQ_LIMIT,
        WalObjectSequenceExhaustedSnafu {
            last_object_seq: next_object_seq - 1,
        }
    );
    let durable_entry_ids = durable_entry_ids(&catalog);
    Ok(Recovered {
        catalog,
        next_object_seq,
        durable_entry_ids,
        tip,
        max_epoch,
    })
}

/// Returns, in sequence order, the objects on the chain that ends at the tip:
/// the complete object with the largest epoch, then the largest sequence.
///
/// An object is complete when every link on its chain holds. A link holds when
/// the object starts a chain, or when its predecessor is present and carries
/// the writer instance the link records, or when its predecessor is missing
/// and lies below every present object, which is where objects are collected.
/// A create that was reported as failed may still leave its object, but every
/// object written after that failure links past it, and every instance writes
/// under an epoch above every object present when it opened, so neither such
/// an object nor a late object of an earlier instance ends the chosen chain.
fn select_chain(headers: &BTreeMap<u64, &Header>) -> Vec<u64> {
    let Some(&lowest) = headers.keys().next() else {
        return Vec::new();
    };
    // A predecessor precedes its successor, so one pass in sequence order
    // settles every object.
    let mut complete = HashMap::with_capacity(headers.len());
    for (&object_seq, header) in headers {
        let holds = match header.prev {
            None => true,
            Some(link) if link.object_seq >= object_seq => false,
            Some(link) => match headers.get(&link.object_seq) {
                Some(prev) => {
                    prev.writer_instance == link.writer_instance && complete[&link.object_seq]
                }
                None => link.object_seq < lowest,
            },
        };
        complete.insert(object_seq, holds);
    }
    let Some(tip) = headers
        .iter()
        .filter(|(object_seq, _)| complete[*object_seq])
        .max_by_key(|(object_seq, header)| (header.epoch, **object_seq))
        .map(|(object_seq, _)| *object_seq)
    else {
        return Vec::new();
    };
    let mut chain = vec![tip];
    while let Some(link) = headers[chain.last().expect("the chain holds the tip")].prev
        && headers.contains_key(&link.object_seq)
    {
        chain.push(link.object_seq);
    }
    chain.reverse();
    chain
}

/// Writes the empty object that starts `epoch` at `object_seq`, linked to the
/// recovered `tip`, and returns it as the tip later objects extend. An object
/// an earlier instance left at that sequence after recovery listed the prefix
/// has a lower epoch and never ends a chain, so the start object moves past
/// it. An object of an equal or later epoch belongs to another writer of the
/// prefix, and the conflict fails the open, as does a create whose outcome is
/// unknown: the next open counts the object in either case.
async fn start_epoch(
    io: &dyn WalObjectIo,
    mut object_seq: u64,
    tip: Option<ChainLink>,
    writer_instance: [u8; 16],
    epoch: u64,
) -> Result<ChainLink> {
    loop {
        ensure!(
            object_seq < OBJECT_SEQ_LIMIT,
            WalObjectSequenceExhaustedSnafu {
                last_object_seq: OBJECT_SEQ_LIMIT - 1,
            }
        );
        let header = Header {
            object_seq,
            writer_instance,
            epoch,
            prev: tip,
        };
        match io
            .put_if_absent(object_seq, encode_object(header, &[])?.bytes)
            .await
        {
            Ok(_) => {
                return Ok(ChainLink {
                    object_seq,
                    writer_instance,
                });
            }
            Err(error @ Error::WalObjectConflict { .. }) => {
                let head = io.get_range(object_seq, 0, HEADER_LEN as u64).await?;
                let existing = decode_header(&head).with_context(|_| InvalidWalObjectSnafu {
                    path: io.object_path(object_seq),
                })?;
                if existing.epoch >= epoch {
                    return Err(error);
                }
                object_seq += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Fetches and verifies the headers and footers of `objects`, up to
/// `concurrency` objects at a time, and returns them ordered by object sequence
/// whatever the order the fetches complete in. The first failure abandons the
/// remaining fetches.
async fn fetch_footers(
    io: &dyn WalObjectIo,
    objects: Vec<ListedObject>,
    concurrency: usize,
) -> Result<Vec<FetchedObject>> {
    let mut fetched = futures::stream::iter(objects)
        .map(|object| async move {
            let (header, footer) = fetch_footer(io, &object).await?;
            Ok(FetchedObject {
                object,
                header,
                footer,
            })
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    fetched.sort_unstable_by_key(|fetched| fetched.object.object_seq);
    Ok(fetched)
}

/// Reads the header, trailer and footer of `object` and verifies them: the
/// header must carry the sequence of the key, the trailer must be well formed,
/// the footer must match the checksum the trailer holds and its segments must
/// tile the object body.
///
/// A short object is read whole. Otherwise the header and a window at the end
/// of the object are read concurrently, and the footer is read separately only
/// when it starts before the window.
async fn fetch_footer(
    io: &dyn WalObjectIo,
    object: &ListedObject,
) -> Result<(Header, Vec<FooterEntry>)> {
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

    let (header, trailer, footer_range) =
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
    Ok((header, footer))
}

/// Verifies the header and trailer of the object `object_seq` of `object_len`
/// bytes from its first bytes `head` and its last bytes `tail`, and returns
/// the header and the trailer with the range the footer occupies in the object.
fn locate_footer(
    object_seq: u64,
    object_len: usize,
    head: &[u8],
    tail: &[u8],
) -> Result<(Header, FixedTrailer, Range<usize>)> {
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
    Ok((header, trailer, footer_range))
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
    use tokio::time::timeout;

    use super::*;
    use crate::error::WalObjectStoreSnafu;
    use crate::object_store_wal::batch::{OBJECT_SEQ_LIMIT, entry_id};
    use crate::object_store_wal::format::{
        FOOTER_ENTRY_LEN, Header, Record, decode_object, encode_object,
    };

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

    fn eager() -> ObjectStoreWalConfig {
        config(Duration::from_millis(10), 1)
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
            let io = ObjectStoreIo::new(
                object_store.clone(),
                config.node_prefix(*node_id, *generation),
            )
            .unwrap();
            put_fixture(
                &io,
                0,
                &[Record {
                    region_id: region(1),
                    entry_id: index as u64 + 1,
                    payload: Bytes::from_static(b"entry"),
                }],
            )
            .await;
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
    async fn recover_by_decoding(io: &dyn WalObjectIo) -> Result<Recovered> {
        let mut objects = Vec::new();
        for object in io.list().await? {
            let bytes = io.get(object.object_seq).await?;
            let decoded = decode_object(&bytes)
                .and_then(|decoded| {
                    ensure!(
                        decoded.header.object_seq == object.object_seq,
                        CorruptedWalObjectSnafu {
                            reason: format!(
                                "header sequence {} does not match key sequence {}",
                                decoded.header.object_seq, object.object_seq
                            ),
                        }
                    );
                    Ok(decoded)
                })
                .with_context(|_| InvalidWalObjectSnafu {
                    path: object.path.clone(),
                })?;
            objects.push(FetchedObject {
                object,
                header: decoded.header,
                footer: decoded.footer,
            });
        }
        finish_recovery(objects)
    }

    fn assert_same_recovery(expected: &Recovered, actual: &Recovered) {
        assert_eq!(
            catalog_contents(&expected.catalog),
            catalog_contents(&actual.catalog)
        );
        assert_eq!(expected.next_object_seq, actual.next_object_seq);
        assert_eq!(expected.durable_entry_ids, actual.durable_entry_ids);
        assert_eq!(expected.tip, actual.tip);
        assert_eq!(expected.max_epoch, actual.max_epoch);
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
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        put_fixture(&io, object_seq, records).await;
    }

    /// Writer instance of the objects tests write without a store.
    const FIXTURE_WRITER: [u8; 16] = *b"fixture-writer!!";

    /// The header of a fixture object at `object_seq`, which extends the
    /// present object right below it under the epoch of that object, so the
    /// fixtures a test writes in sequence order form one chain.
    async fn fixture_header(io: &ObjectStoreIo, object_seq: u64) -> Header {
        let below = io
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.object_seq)
            .filter(|seq| *seq < object_seq)
            .max();
        let prev = match below {
            Some(seq) => Some(decode_header(&io.get(seq).await.unwrap()).unwrap()),
            None => None,
        };
        Header {
            object_seq,
            writer_instance: FIXTURE_WRITER,
            epoch: prev.as_ref().map_or(1, |prev| prev.epoch),
            prev: prev.map(|prev| ChainLink {
                object_seq: prev.object_seq,
                writer_instance: prev.writer_instance,
            }),
        }
    }

    async fn put_fixture(io: &ObjectStoreIo, object_seq: u64, records: &[Record]) {
        let header = fixture_header(io, object_seq).await;
        put_header(io, header, records).await;
    }

    async fn put_header(io: &ObjectStoreIo, header: Header, records: &[Record]) {
        let object_seq = header.object_seq;
        let encoded = encode_object(header, records).unwrap();
        io.put_if_absent(object_seq, encoded.bytes).await.unwrap();
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

        let recovered = recover(&io).await.unwrap();
        let expected = recover_by_decoding(&io).await.unwrap();

        assert_eq!(40, catalog_contents(&recovered.catalog).len());
        assert_same_recovery(&expected, &recovered);
        assert_eq!(40, recovered.next_object_seq);
        let durable = recovered.durable_entry_ids;
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
            ("unsupported format version 3", |bytes| {
                bytes[8..10].copy_from_slice(&3u16.to_be_bytes());
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
            fixture_header(&io, 0).await,
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

        let recovered = recover(io.as_ref()).await.unwrap();
        let expected = recover_by_decoding(io.as_ref()).await.unwrap();
        assert_same_recovery(&expected, &recovered);
        assert_eq!(regions as usize, recovered.durable_entry_ids.len());

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
        put_records(object_store, object_seq, &records).await;
    }

    #[tokio::test]
    async fn test_store_resumes_sequence_and_durable_ids() {
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let recovered = recover(&io).await.unwrap();
        assert_eq!(0, recovered.next_object_seq);
        assert!(recovered.durable_entry_ids.is_empty());

        put_object(&object_store, 2, region(1), &[1, id(5, 7)]).await;
        put_object(&object_store, 4, region(2), &[8]).await;
        let recovered = recover(&io).await.unwrap();
        assert_eq!(6, recovered.next_object_seq);
        assert_eq!(
            HashMap::from([(region(1), id(5, 7)), (region(2), 8)]),
            recovered.durable_entry_ids
        );
        let store = open(object_store, &eager()).await;
        assert_eq!(id(5, 7), latest(&store, region(1)));
        assert_eq!(8, latest(&store, region(2)));
        assert_eq!(0, latest(&store, region(3)));
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
            let errors = [
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
    }

    #[tokio::test]
    async fn test_store_stop_with_actor_gone() {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        drop(command_rx);
        let store = ObjectStoreLogStore {
            prefix: PREFIX.to_string(),
            io: Arc::new(ObjectStoreIo::new(memory_store(), PREFIX).unwrap()),
            catalog: Arc::default(),
            obsolete_entry_ids: ObsoleteEntryIds::default(),
            terminal_error: Arc::default(),
            stopped: Arc::new(AtomicBool::new(false)),
            command_tx,
        };
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
                    .map(|fetched| fetched.object.object_seq)
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
        // Sequence 16 holds the object that started the epoch of the store.
        assert_eq!(17, store.catalog.read().unwrap().next_object_seq().unwrap());
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

    #[tokio::test]
    async fn test_store_append_is_unsupported() {
        let store = open(memory_store(), &eager()).await;
        let error = store.append_batch(Vec::new()).await.unwrap_err();
        assert!(matches!(
            error,
            Error::UnsupportedObjectStoreWalOperation { .. }
        ));
        assert_eq!(
            common_error::status_code::StatusCode::Unsupported,
            error.status_code()
        );
        store.stop().await.unwrap();
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
        reopened
            .obsolete(&p, region(1), EntryId::MAX)
            .await
            .unwrap();
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

    fn chain_header(object_seq: u64, writer: u8, epoch: u64, prev: Option<(u64, u8)>) -> Header {
        Header {
            object_seq,
            writer_instance: [writer; 16],
            epoch,
            prev: prev.map(|(object_seq, writer)| ChainLink {
                object_seq,
                writer_instance: [writer; 16],
            }),
        }
    }

    fn chain_of(headers: &[Header]) -> Vec<u64> {
        select_chain(
            &headers
                .iter()
                .map(|header| (header.object_seq, header))
                .collect(),
        )
    }

    #[test]
    fn test_store_selects_the_chain_of_the_latest_complete_object() {
        let (a, b) = (1, 2);
        assert!(chain_of(&[]).is_empty());
        // A linear chain is replayed whole.
        assert_eq!(
            vec![0, 1, 2],
            chain_of(&[
                chain_header(0, a, 1, None),
                chain_header(1, a, 1, Some((0, a))),
                chain_header(2, a, 1, Some((1, a))),
            ])
        );
        // Object 1 was reported as failed and object 2 links past it.
        assert_eq!(
            vec![0, 2],
            chain_of(&[
                chain_header(0, a, 1, None),
                chain_header(1, a, 1, Some((0, a))),
                chain_header(2, a, 1, Some((0, a))),
            ])
        );
        // Object 3 extends object 2, which never landed, so object 1 is the
        // tip although object 3 has a higher sequence.
        assert_eq!(
            vec![0, 1],
            chain_of(&[
                chain_header(0, a, 1, None),
                chain_header(1, a, 1, Some((0, a))),
                chain_header(3, a, 1, Some((2, a))),
            ])
        );
        // Object 1 names a predecessor its writer never wrote at sequence 0.
        assert_eq!(
            vec![0],
            chain_of(&[
                chain_header(0, a, 1, None),
                chain_header(1, b, 2, Some((0, b))),
            ])
        );
        // Instance A left object 2 behind after instance B, a later epoch,
        // acknowledged object 1: the later epoch ends the chain.
        assert_eq!(
            vec![0, 1],
            chain_of(&[
                chain_header(0, a, 1, None),
                chain_header(1, b, 2, Some((0, a))),
                chain_header(2, a, 1, Some((0, a))),
            ])
        );
        // A predecessor below every present object was collected.
        assert_eq!(
            vec![3, 4],
            chain_of(&[
                chain_header(3, a, 1, Some((2, a))),
                chain_header(4, a, 1, Some((3, a))),
            ])
        );
        // A missing predecessor above a present object breaks the link.
        assert_eq!(
            vec![1],
            chain_of(&[
                chain_header(1, a, 1, None),
                chain_header(3, a, 1, Some((2, a))),
            ])
        );
        // A predecessor at or above the object itself never holds.
        assert!(chain_of(&[chain_header(5, a, 1, Some((5, a)))]).is_empty());
        // Instance A's tip 3 was collected after instance B extended it. Late
        // objects of A that extend object 3 hold their links, below or above
        // B's tip, but carry an earlier epoch.
        assert_eq!(
            vec![4, 6],
            chain_of(&[
                chain_header(4, b, 2, Some((3, a))),
                chain_header(5, a, 1, Some((3, a))),
                chain_header(6, b, 2, Some((4, b))),
                chain_header(7, a, 1, Some((3, a))),
            ])
        );
    }

    #[tokio::test]
    async fn test_store_recovery_indexes_only_the_chain() {
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let record = |entry_id: EntryId, data: &'static str| Record {
            region_id: region(1),
            entry_id,
            payload: Bytes::from_static(data.as_bytes()),
        };
        put_fixture(&io, 0, &[record(1, "a")]).await;
        let first = fixture_header(&io, 1).await;
        // Object 1 was reported as failed; object 2 reused its row sequences
        // and links past it.
        put_header(&io, first.clone(), &[record(id(1, 1), "old")]).await;
        let second = Header {
            object_seq: 2,
            ..first.clone()
        };
        put_header(&io, second, &[record(id(2, 1), "new")]).await;

        for restart in 0..2 {
            let store = open(object_store.clone(), &eager()).await;
            assert_eq!(
                expected_entries(region(1), &[(1, "a"), (id(2, 1), "new")]),
                read_entries(&store, region(1), 0).await
            );
            assert_eq!(id(2, 1), latest(&store, region(1)));
            store.stop().await.unwrap();
            // Every open starts an epoch above every present object, and the
            // first restart's start object extends the chain the second
            // restart replays.
            let start = decode_header(&io.get(3 + restart).await.unwrap()).unwrap();
            assert_eq!(2 + restart, start.epoch);
            assert_eq!(Some(2 + restart), start.prev.map(|prev| prev.object_seq));
        }
        assert_eq!(vec![0, 1, 2, 3, 4], object_seqs(&io).await);
    }

    #[tokio::test]
    async fn test_store_recovery_never_reuses_the_sequence_of_an_orphan() {
        let io = ObjectStoreIo::new(memory_store(), PREFIX).unwrap();
        put_fixture(&io, 0, &[]).await;
        // Object 2 extends object 1, which never landed.
        let orphan = Header {
            prev: Some(ChainLink {
                object_seq: 1,
                writer_instance: FIXTURE_WRITER,
            }),
            ..fixture_header(&io, 2).await
        };
        put_header(&io, orphan, &[]).await;
        let recovered = recover(&io).await.unwrap();
        assert_eq!(Some(0), recovered.tip.map(|tip| tip.object_seq));
        assert_eq!(3, recovered.next_object_seq);
    }

    #[tokio::test]
    async fn test_store_recovery_keeps_the_later_epoch_over_a_late_object() {
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let record = |entry_id: EntryId, data: &'static str| Record {
            region_id: region(1),
            entry_id,
            payload: Bytes::from_static(data.as_bytes()),
        };
        let (a, b) = (1, 2);
        put_header(&io, chain_header(0, a, 1, None), &[record(1, "a")]).await;
        // Instance B acknowledged object 1 before the create instance A issued
        // for object 2 landed.
        put_header(
            &io,
            chain_header(1, b, 2, Some((0, a))),
            &[record(id(1, 1), "b")],
        )
        .await;
        put_header(
            &io,
            chain_header(2, a, 1, Some((0, a))),
            &[record(id(2, 1), "late")],
        )
        .await;

        let store = open(object_store, &eager()).await;
        assert_eq!(
            expected_entries(region(1), &[(1, "a"), (id(1, 1), "b")]),
            read_entries(&store, region(1), 0).await
        );
        store.stop().await.unwrap();
        let start = decode_header(&io.get(3).await.unwrap()).unwrap();
        assert_eq!(3, start.epoch);
        assert_eq!(
            Some(ChainLink {
                object_seq: 1,
                writer_instance: [b; 16],
            }),
            start.prev
        );
    }

    #[tokio::test]
    async fn test_store_open_starts_the_first_epoch_on_an_empty_prefix() {
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        store.stop().await.unwrap();
        let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        let start = decode_header(&io.get(0).await.unwrap()).unwrap();
        assert_eq!(1, start.epoch);
        assert_eq!(None, start.prev);
        assert_eq!(vec![0], object_seqs(&io).await);
    }

    #[tokio::test]
    async fn test_store_start_object_moves_past_an_earlier_epoch_only() {
        for (epoch, moves) in [(1, true), (2, false)] {
            let object_store = memory_store();
            let fixtures = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
            put_fixture(&fixtures, 0, &[]).await;
            // An object lands at the sequence of the start object after
            // recovery listed the prefix.
            let late = encode_object(chain_header(1, 9, epoch, None), &[])
                .unwrap()
                .bytes;
            let io = Arc::new(RacingIo {
                inner: ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap(),
                late: Mutex::new(Some((1, late))),
            });
            let result = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await;
            if moves {
                result.unwrap().stop().await.unwrap();
                let start = decode_header(&fixtures.get(2).await.unwrap()).unwrap();
                assert_eq!(2, start.epoch);
                assert_eq!(Some(0), start.prev.map(|prev| prev.object_seq));
            } else {
                let error = result.unwrap_err();
                assert!(
                    matches!(&error, Error::WalObjectConflict { path, .. } if path == &fixtures.object_path(1)),
                    "unexpected error: {error:?}"
                );
                assert_eq!(vec![0, 1], object_seqs(&fixtures).await);
            }
        }
    }

    async fn object_seqs(io: &ObjectStoreIo) -> Vec<u64> {
        io.list()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.object_seq)
            .collect()
    }

    /// Object access whose first create lets another object land at a given
    /// sequence first, as a create issued before recovery would.
    struct RacingIo {
        inner: ObjectStoreIo,
        late: Mutex<Option<(u64, Bytes)>>,
    }

    #[async_trait::async_trait]
    impl WalObjectIo for RacingIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            let late = self.late.lock().unwrap().take();
            if let Some((late_seq, late)) = late {
                self.inner.put_if_absent(late_seq, late).await?;
            }
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
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

    struct ParkedIo {
        inner: ObjectStoreIo,
        parked: mpsc::UnboundedSender<(u64, oneshot::Sender<bool>)>,
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
            let (parked, parked_rx) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                    parked,
                    in_flight: AtomicUsize::new(0),
                    max_in_flight: AtomicUsize::new(0),
                }),
                parked_rx,
            )
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for ParkedIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            let (release, released) = oneshot::channel();
            self.parked.send((object_seq, release)).unwrap();
            let success = released.await.unwrap();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if !success {
                return Err(object_store::Error::new(
                    object_store::ErrorKind::Unexpected,
                    "injected failure",
                )
                .set_temporary())
                .context(WalObjectStoreSnafu {
                    operation: "read",
                    path: self.object_path(object_seq),
                });
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

    /// Object access that records every range read as (sequence, offset, length).
    struct RecordingIo {
        inner: ObjectStoreIo,
        reads: RangeReads,
    }

    impl RecordingIo {
        fn over(object_store: ObjectStore) -> (Arc<Self>, RangeReads) {
            let reads = RangeReads::default();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                reads: reads.clone(),
            };
            (Arc::new(io), reads)
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for RecordingIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            self.inner.put_if_absent(object_seq, content).await
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
