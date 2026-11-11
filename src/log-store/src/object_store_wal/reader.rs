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

//! Read-only access to another node's immutable WAL objects.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use async_stream::try_stream;
use common_cache::Cache;
use futures::StreamExt;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::Provider;
use store_api::logstore::{EntryId, SendableEntryStream};
use store_api::storage::RegionId;
use tokio::sync::Semaphore;

use crate::error::{
    CorruptedWalObjectSnafu, Error, InvalidWalEntryRangeSnafu, InvalidWalObjectSnafu,
    InvalidWalObjectStoreSnafu, MissingSourceWalHistorySnafu, ObjectStoreWalSnafu, Result,
};
use crate::object_store_wal::batch::{
    OBJECT_SEQ_LIMIT, POSITION_BITS, POSITION_LIMIT, entry_id, sequence_floor,
};
use crate::object_store_wal::catalog::ObjectCatalog;
use crate::object_store_wal::format::{FooterEntry, Header};
use crate::object_store_wal::history;
use crate::object_store_wal::index;
use crate::object_store_wal::io::{self, ListedObject, ObjectStoreIo, WalObjectRead};
use crate::object_store_wal::prefetch;

/// Node-wide limit on concurrent remote object index reads. Startup recovery
/// keeps its existing, separate concurrency setting.
const FOOTER_CONCURRENCY: usize = 16;

/// Upper bound of a remote WAL recovery range. A reported final entry must be
/// present, whereas a LIST boundary need not contain entries of this region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReadUpperBound {
    /// The durable last entry reported by the source on downgrade.
    LastEntry(EntryId),
    /// A conservative bound obtained by listing the source prefix.
    Listed(EntryId),
}

impl WalReadUpperBound {
    fn entry_id(self) -> EntryId {
        match self {
            Self::LastEntry(id) | Self::Listed(id) => id,
        }
    }
}

/// A normalized WAL node prefix: `<root>/datanodes/<node>/epochs/<generation>`.
/// Construct it from a persisted source prefix or
/// [`ObjectStoreWalConfig::node_prefix`](common_wal::config::object_store::ObjectStoreWalConfig::node_prefix).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeWalPrefix(String);

impl NodeWalPrefix {
    /// Validates the node layout and removes a trailing slash.
    pub fn try_new(prefix: &str) -> Result<Self> {
        let prefix = io::normalize_prefix(prefix)?;
        let mut parts = prefix.rsplitn(5, '/');
        let generation = parts.next();
        let epochs = parts.next();
        let node = parts.next();
        let datanodes = parts.next();
        let root = parts.next();
        let is_id = |part: Option<&str>| {
            part.is_some_and(|part| {
                (part == "0" || !part.starts_with('0'))
                    && part.bytes().all(|byte| byte.is_ascii_digit())
                    && part.parse::<u64>().is_ok()
            })
        };
        ensure!(
            is_id(generation)
                && epochs == Some("epochs")
                && is_id(node)
                && datanodes == Some("datanodes")
                && root.is_some(),
            InvalidWalObjectStoreSnafu {
                reason: format!("expected a WAL node prefix, got {prefix:?}"),
            }
        );
        Ok(Self(prefix))
    }

    /// Returns the normalized prefix used in object keys and providers.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Readers shared by source prefix on one destination node and one WAL backend.
/// Keep this registry for the node's lifetime, and hold each returned reader
/// while any recovery range still references that source. Dropping the last
/// reader releases its cached indexes; no background task or writer is started.
pub struct ObjectStoreWalReaders {
    store: ObjectStore,
    readers: Mutex<HashMap<NodeWalPrefix, Weak<ObjectStoreWalReader>>>,
    footer_budget: Arc<Semaphore>,
}

impl ObjectStoreWalReaders {
    /// Creates an empty registry. Construction performs no storage requests.
    pub fn new(store: ObjectStore) -> Self {
        Self {
            store,
            readers: Mutex::new(HashMap::new()),
            footer_budget: Arc::new(Semaphore::new(FOOTER_CONCURRENCY)),
        }
    }

    /// Returns the shared reader for a full node prefix, without probing writes
    /// or listing objects. Only read/list permissions are needed on the backend.
    pub fn reader(&self, prefix: &NodeWalPrefix) -> Result<Arc<ObjectStoreWalReader>> {
        let mut readers = self.readers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(reader) = readers.get(prefix).and_then(Weak::upgrade) {
            return Ok(reader);
        }
        readers.retain(|_, reader| reader.strong_count() != 0);
        let reader = Arc::new(ObjectStoreWalReader {
            io: Arc::new(ObjectStoreIo::new_readonly(
                self.store.clone(),
                prefix.as_str(),
            )?),
            indexes: CachedObject::cache(),
            footer_budget: self.footer_budget.clone(),
            prefetch_budget: Arc::new(Semaphore::new(prefetch::READ_PREFETCH_BYTES)),
        });
        readers.insert(prefix.clone(), Arc::downgrade(&reader));
        Ok(reader)
    }
}

/// Reads remote WAL ranges without claiming an epoch or modifying the source.
/// The reader never honors missing-history or corrupted-segment skip options.
/// It does not own region roles, manifests, release positions or replay tasks.
pub struct ObjectStoreWalReader {
    io: Arc<dyn WalObjectRead>,
    /// Immutable indexes keyed by listed metadata, sharing loads within this prefix.
    indexes: Cache<ListedObject, Arc<CachedObject>>,
    /// Footer-load concurrency shared by all readers in the node's registry.
    footer_budget: Arc<Semaphore>,
    /// Prefetched bytes shared by reads of this source prefix only.
    prefetch_budget: Arc<Semaphore>,
}

/// Cached metadata for an immutable WAL object, without recovery release records.
struct CachedObject {
    object: ListedObject,
    header: Header,
    footer: Vec<FooterEntry>,
}

impl CachedObject {
    /// Bounds retained index weight per source prefix. Eviction is asynchronous;
    /// active reads can retain their own snapshots beyond this capacity.
    fn cache() -> Cache<ListedObject, Arc<Self>> {
        Cache::new(
            "object_store_wal_indexes",
            64 * 1024 * 1024,
            |key: &ListedObject, value: &Arc<Self>| {
                let bytes = std::mem::size_of::<ListedObject>()
                    + key.path.capacity()
                    + std::mem::size_of::<Self>()
                    + value.object.path.capacity()
                    + value.footer.capacity() * std::mem::size_of::<FooterEntry>();
                u32::try_from(bytes).unwrap_or(u32::MAX)
            },
            |_, _, _| {},
        )
    }
}

impl ObjectStoreWalReader {
    /// The normalized node prefix used by every entry's provider.
    pub fn prefix(&self) -> &str {
        self.io.prefix()
    }

    /// Lists keys only, returning a bound at or above every entry in the listed
    /// objects. An empty prefix is an error, not an empty tail. No object index
    /// or data is read on this readiness path. The bound covers the whole prefix;
    /// `region_id` identifies the recovery request in missing-history errors.
    pub async fn list_upper_bound(&self, region_id: RegionId) -> Result<EntryId> {
        let objects = self.io.list().await?;
        let seq = objects
            .iter()
            .map(|object| object.object_seq)
            .max()
            .with_context(|| MissingSourceWalHistorySnafu {
                region_id,
                prefix: self.prefix().to_string(),
                storage: self.io.storage(),
                reason: format!(
                    "empty WAL prefix {} while determining takeover bound",
                    self.prefix()
                ),
            })?;
        ensure!(
            seq < OBJECT_SEQ_LIMIT,
            CorruptedWalObjectSnafu {
                reason: format!(
                    "listed key {} has sequence {seq} outside the entry-id range",
                    self.io.object_path(seq)
                ),
            }
        );
        Ok(entry_id(seq, POSITION_LIMIT - 1))
    }

    /// Prepares a fresh view and streams only this region's `(after, upper]`.
    /// Call after the recovery range has been registered in the manifest. Each
    /// call (including a retry) starts a new LIST; cached indexes never replace
    /// that LIST. Previously indexed, still-listed immutable objects are reused.
    ///
    /// The store writing the prefix may collect listed objects before their
    /// indexes are read. An object gone below the sequence at which the chain
    /// of the read stops (a reclaim boundary, or the object of `after` when it
    /// is higher in the `LastEntry` mode) is skipped, as startup recovery skips
    /// it. One gone at or above it, on the chain or not, fails the read; a
    /// retry lists the prefix again.
    ///
    /// Errors can occur after entries have been yielded. The caller must finish
    /// the stream successfully before publishing recovery progress. In the
    /// `LastEntry` mode success also proves that the reported entry was read.
    pub async fn read(
        &self,
        region_id: RegionId,
        after: EntryId,
        upper: WalReadUpperBound,
    ) -> Result<SendableEntryStream<'static, Entry, Error>> {
        let upto = upper.entry_id();
        ensure!(
            after < upto,
            InvalidWalEntryRangeSnafu {
                region_id,
                start_entry_id: after,
                end_entry_id: upto,
            }
        );
        let catalog = self.prepare(region_id, after, upper).await?;
        let objects = catalog
            .objects_for_entry_range(region_id, after + 1, upto)?
            .into_iter()
            .map(|(seq, entry)| (seq, entry.clone()))
            .collect();
        let mut segments =
            prefetch::segments(self.io.clone(), objects, self.prefetch_budget.clone());
        let provider = Provider::object_store_provider(region_id, self.prefix().to_string());
        Ok(Box::pin(try_stream! {
            let mut reached_upto = false;
            while let Some((_, _, fetched)) = segments.next().await {
                let entries = fetched?.into_iter()
                    .filter(|record| record.entry_id > after && record.entry_id <= upto)
                    .map(|record| {
                        reached_upto |= record.entry_id == upto;
                        Entry::Naive(NaiveEntry {
                            provider: provider.clone(), region_id,
                            entry_id: record.entry_id, data: record.payload.into(),
                        })
                    }).collect::<Vec<_>>();
                if !entries.is_empty() { yield entries; }
            }
            if matches!(upper, WalReadUpperBound::LastEntry(_)) && !reached_upto {
                Err(CorruptedWalObjectSnafu {
                    reason: format!("WAL recovery of region {region_id} did not reach reported entry {upto}"),
                }.build())?;
            }
        }))
    }

    async fn prepare(
        &self,
        region_id: RegionId,
        after: EntryId,
        upper: WalReadUpperBound,
    ) -> Result<ObjectCatalog> {
        let start = match upper {
            WalReadUpperBound::LastEntry(_) => after >> POSITION_BITS,
            WalReadUpperBound::Listed(_) => 0,
        };
        let listed = self.io.list_from(start).await?;
        let present = listed.iter().cloned().collect::<HashSet<_>>();
        // Each read owns its LIST snapshot. Drop cached indexes outside it,
        // accepting a refetch for later reads with a lower watermark. In-flight
        // loads may still insert old indexes; capacity also bounds retention.
        let _ = self
            .indexes
            .invalidate_if(move |object, _| !present.contains(object));
        ensure!(
            !listed.is_empty(),
            MissingSourceWalHistorySnafu {
                region_id,
                prefix: self.prefix().to_string(),
                storage: self.io.storage(),
                reason: format!("no WAL objects from sequence {start} in {}", self.prefix()),
            }
        );
        if let WalReadUpperBound::Listed(upto) = upper {
            ensure!(
                listed
                    .iter()
                    .any(|object| object.object_seq >= upto >> POSITION_BITS),
                CorruptedWalObjectSnafu {
                    reason: format!(
                        "WAL listing of {} does not reach recorded bound {upto}",
                        self.prefix()
                    ),
                }
            );
        }
        let (objects, vanished) = index::fetch_listed(
            futures::stream::iter(listed.into_iter().map(Ok)),
            FOOTER_CONCURRENCY,
            |object| async move {
                let seq = object.object_seq;
                // Include listing metadata in the key so a load for a changed
                // object cannot join an older in-flight load of the same sequence.
                let cached = self
                    .indexes
                    .try_get_with(&object, async {
                        // Waiters share the loader without consuming footer permits.
                        let _permit = self.footer_budget.acquire().await.map_err(|_| {
                            CorruptedWalObjectSnafu {
                                reason: "remote WAL footer budget closed".to_string(),
                            }
                            .build()
                        })?;
                        let fetched = index::fetch_footer(self.io.as_ref(), &object).await?;
                        Ok::<_, Error>(Arc::new(CachedObject {
                            object: object.clone(),
                            header: fetched.header,
                            footer: fetched.footer,
                        }))
                    })
                    .await
                    .context(ObjectStoreWalSnafu)?;
                Ok::<_, Error>((seq, cached))
            },
        )
        .await?;
        let objects = objects.into_iter().collect::<BTreeMap<_, _>>();
        let headers = objects
            .iter()
            .map(|(seq, object)| (*seq, &object.header))
            .collect::<BTreeMap<_, _>>();
        let chain = match upper {
            WalReadUpperBound::LastEntry(last) => {
                // A later writer may already have reclaimed the oldest part.
                // The reported L itself must still exist and be replayed.
                let boundary = headers
                    .values()
                    .map(|header| header.reclaim_boundary)
                    .max()
                    .unwrap_or(0)
                    .max(start);
                index::ensure_vanished_below(vanished, boundary)?;
                index::chain_from(&headers, last >> POSITION_BITS, boundary).with_context(|| {
                    CorruptedWalObjectSnafu {
                        reason: format!(
                            "missing WAL chain ending at reported entry {last} in {}",
                            self.prefix()
                        ),
                    }
                })?
            }
            WalReadUpperBound::Listed(_) => {
                let chain = index::select_chain(&headers);
                let boundary = chain.last().map_or(0, |tip| headers[tip].reclaim_boundary);
                index::ensure_vanished_below(vanished, boundary)?;
                let tip = chain.last().with_context(|| CorruptedWalObjectSnafu {
                    reason: format!("no complete WAL chain in {}", self.prefix()),
                })?;
                let max_epoch = headers
                    .values()
                    .map(|header| header.epoch)
                    .max()
                    .unwrap_or(0);
                ensure!(
                    headers[tip].epoch == max_epoch,
                    CorruptedWalObjectSnafu {
                        reason: format!(
                            "WAL chain in {} would fall back from epoch {max_epoch} to {}",
                            self.prefix(),
                            headers[tip].epoch
                        ),
                    }
                );
                chain
            }
        };
        let mut catalog = ObjectCatalog::default();
        let mut history_end = 0;
        for seq in chain {
            let object = &objects[&seq];
            history_end = history_end.max(
                object
                    .footer
                    .iter()
                    .map(|entry| sequence_floor(entry.max_entry_id))
                    .max()
                    .unwrap_or(0),
            );
            // Keep only this region in the per-read catalog. All regions share
            // the source's cached footers, rather than copying the entire node
            // index into every concurrent region recovery.
            let footer = object
                .footer
                .iter()
                .filter(|entry| entry.region_id == region_id)
                .cloned()
                .collect();
            catalog
                .insert_object(seq, footer)
                .with_context(|_| InvalidWalObjectSnafu {
                    path: object.object.path.clone(),
                })?;
        }
        if matches!(upper, WalReadUpperBound::Listed(_))
            && let Some(reason) =
                history::missing_history(&catalog, region_id, after, false, history_end)
        {
            return MissingSourceWalHistorySnafu {
                region_id,
                prefix: self.prefix().to_string(),
                storage: self.io.storage(),
                reason,
            }
            .fail();
        }
        Ok(catalog)
    }
}

#[cfg(test)]
mod tests;
