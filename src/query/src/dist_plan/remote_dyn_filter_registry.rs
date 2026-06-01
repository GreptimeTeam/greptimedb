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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
use common_query::request::DynFilterPayload;
use common_runtime::spawn_global;
use common_telemetry::{debug, warn};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use session::query_id::QueryId;
use store_api::storage::RegionId;
use tokio::sync::{Notify, watch};

use crate::dist_plan::FilterId;
use crate::region_query::RegionQueryHandlerRef;

const REMOTE_DYN_FILTER_UPDATE_PAYLOAD_MAX_BYTES: usize = 64 * 1024;
const REMOTE_DYN_FILTER_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

/// Routing metadata for a remote dynamic filter subscriber.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subscriber {
    region_id: RegionId,
}

impl Subscriber {
    pub fn new(region_id: RegionId) -> Self {
        Self { region_id }
    }

    pub fn region_id(&self) -> RegionId {
        self.region_id
    }
}

/// Result of registering a remote dynamic filter entry.
#[derive(Debug, Clone)]
pub enum EntryRegistration {
    Inserted(Arc<DynFilterEntry>),
    /// The filter already existed; this contains the previously registered entry.
    Existing(Arc<DynFilterEntry>),
}

/// Result of registering a subscriber under an existing filter entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriberRegistration {
    Added,
    Duplicate,
    MissingFilter,
}

/// A registered query-local remote dynamic filter entry.
///
/// The frontend query owns the strong DataFusion filter handle until the query finishes; the
/// registry only keeps a weak reference for later updates.
#[derive(Debug)]
pub struct DynFilterEntry {
    filter_id: FilterId,
    alive_dyn_filter: Weak<DynamicFilterPhysicalExpr>,
    subscribers: RwLock<HashSet<Subscriber>>,
    last_sent_generation: AtomicU64,
    unregistered: AtomicBool,
    fanout_started: AtomicBool,
    subscriber_changed: Notify,
}

#[derive(Debug)]
struct QueryDynFilterRegistryInner {
    entries: HashMap<FilterId, Arc<DynFilterEntry>>,
}

impl DynFilterEntry {
    pub fn new(filter_id: FilterId, alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>) -> Self {
        Self {
            filter_id,
            alive_dyn_filter: Arc::downgrade(&alive_dyn_filter),
            subscribers: RwLock::new(HashSet::new()),
            last_sent_generation: AtomicU64::new(0),
            unregistered: AtomicBool::new(false),
            fanout_started: AtomicBool::new(false),
            subscriber_changed: Notify::new(),
        }
    }

    pub fn filter_id(&self) -> &FilterId {
        &self.filter_id
    }

    pub fn upgrade_alive_dyn_filter(&self) -> Option<Arc<DynamicFilterPhysicalExpr>> {
        self.alive_dyn_filter.upgrade()
    }

    pub fn subscribers(&self) -> Vec<Subscriber> {
        self.subscribers.read().unwrap().iter().cloned().collect()
    }

    pub fn register_subscriber(&self, subscriber: Subscriber) -> bool {
        let mut subscribers = self.subscribers.write().unwrap();
        subscribers.insert(subscriber)
    }

    fn mark_generation_sent(&self, generation: u64) -> bool {
        let mut current = self.last_sent_generation.load(Ordering::SeqCst);
        loop {
            if generation <= current {
                return false;
            }

            match self.last_sent_generation.compare_exchange(
                current,
                generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
    }

    fn try_mark_unregistered(&self) -> bool {
        self.unregistered
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn reactivate_for_new_subscriber(&self) {
        self.last_sent_generation.store(0, Ordering::SeqCst);
        self.unregistered.store(false, Ordering::SeqCst);
        self.subscriber_changed.notify_one();
    }

    fn mark_fanout_started(&self) -> bool {
        !self.fanout_started.swap(true, Ordering::SeqCst)
    }
}

/// Query-scoped registry that owns all remote dynamic filters for one query.
#[derive(Debug)]
pub struct QueryDynFilterRegistry {
    query_id: QueryId,
    active_streams: AtomicUsize,
    fanout_started: AtomicBool,
    registry_changed: Notify,
    lifecycle_tx: watch::Sender<bool>,
    inner: RwLock<QueryDynFilterRegistryInner>,
}

impl QueryDynFilterRegistry {
    pub fn new(query_id: QueryId) -> Self {
        let (lifecycle_tx, _) = watch::channel(false);
        Self {
            query_id,
            active_streams: AtomicUsize::new(0),
            fanout_started: AtomicBool::new(false),
            registry_changed: Notify::new(),
            lifecycle_tx,
            inner: RwLock::new(QueryDynFilterRegistryInner {
                entries: HashMap::new(),
            }),
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    fn acquire_stream(&self) {
        if self.active_streams.fetch_add(1, Ordering::SeqCst) == 0 {
            let _ = self.lifecycle_tx.send(false);
        }
    }

    fn release_stream(&self) {
        if self.active_streams.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ = self.lifecycle_tx.send(true);
            self.registry_changed.notify_one();
        }
    }

    fn active_stream_count(&self) -> usize {
        self.active_streams.load(Ordering::SeqCst)
    }

    pub fn entry_count(&self) -> usize {
        self.inner.read().unwrap().entries.len()
    }

    pub fn entries(&self) -> Vec<Arc<DynFilterEntry>> {
        self.inner
            .read()
            .unwrap()
            .entries
            .values()
            .cloned()
            .collect()
    }

    pub fn remote_dyn_filter(&self, filter_id: &FilterId) -> Option<Arc<DynFilterEntry>> {
        self.inner.read().unwrap().entries.get(filter_id).cloned()
    }

    pub fn register_remote_dyn_filter(
        &self,
        filter_id: FilterId,
        alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> EntryRegistration {
        let mut inner = self.inner.write().unwrap();
        if let Some(existing) = inner.entries.get(&filter_id) {
            return EntryRegistration::Existing(existing.clone());
        }

        let entry = Arc::new(DynFilterEntry::new(filter_id.clone(), alive_dyn_filter));
        inner.entries.insert(filter_id, entry.clone());
        self.registry_changed.notify_one();
        EntryRegistration::Inserted(entry)
    }

    pub fn register_subscriber(
        &self,
        filter_id: &FilterId,
        subscriber: Subscriber,
    ) -> SubscriberRegistration {
        let Some(entry) = self.inner.read().unwrap().entries.get(filter_id).cloned() else {
            return SubscriberRegistration::MissingFilter;
        };

        if entry.register_subscriber(subscriber) {
            // A newly added subscriber has not seen the latest snapshot yet. Reset the
            // entry-level generation watermark so the next fanout tick resends the current
            // snapshot to all subscribers; receivers treat duplicate generations idempotently.
            entry.reactivate_for_new_subscriber();
            SubscriberRegistration::Added
        } else {
            SubscriberRegistration::Duplicate
        }
    }

    /// Starts one query-scoped producer fanout task if it has not already been started.
    ///
    /// The supervisor starts one async watcher per registered source-side
    /// [`DynamicFilterPhysicalExpr`]. Each watcher waits on DataFusion's
    /// `wait_update()`/`wait_complete()` notifications and sends best-effort
    /// update/unregister RPCs to subscribed datanodes. The supervisor exits once
    /// all remote scan streams release their registry leases.
    pub fn ensure_fanout_task(self: &Arc<Self>, region_query_handler: RegionQueryHandlerRef) {
        self.registry_changed.notify_one();
        if self.fanout_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let registry = self.clone();
        let _handle = spawn_global(async move {
            registry.run_fanout(region_query_handler).await;
        });
    }

    async fn run_fanout(self: Arc<Self>, region_query_handler: RegionQueryHandlerRef) {
        let mut lifecycle_rx = self.lifecycle_tx.subscribe();

        loop {
            for entry in self.entries() {
                self.ensure_entry_fanout_task(entry, region_query_handler.clone());
            }

            if *lifecycle_rx.borrow() || self.active_stream_count() == 0 {
                break;
            }

            tokio::select! {
                _ = self.registry_changed.notified() => {}
                result = lifecycle_rx.changed() => {
                    if result.is_err() || *lifecycle_rx.borrow() {
                        break;
                    }
                }
            }
        }

        self.unregister_all_once(&region_query_handler).await;
    }

    fn ensure_entry_fanout_task(
        self: &Arc<Self>,
        entry: Arc<DynFilterEntry>,
        region_query_handler: RegionQueryHandlerRef,
    ) {
        if !entry.mark_fanout_started() {
            return;
        }

        let registry = self.clone();
        let _handle = spawn_global(async move {
            registry.run_entry_fanout(entry, region_query_handler).await;
        });
    }

    async fn run_entry_fanout(
        self: Arc<Self>,
        entry: Arc<DynFilterEntry>,
        region_query_handler: RegionQueryHandlerRef,
    ) {
        let mut lifecycle_rx = self.lifecycle_tx.subscribe();
        let mut is_complete = false;
        let mut reconcile_interval = tokio::time::interval(REMOTE_DYN_FILTER_RECONCILE_INTERVAL);

        loop {
            if *lifecycle_rx.borrow() || self.active_stream_count() == 0 {
                break;
            }

            let Some(filter) = entry.upgrade_alive_dyn_filter() else {
                self.unregister_entry_once(&region_query_handler, &entry)
                    .await;
                return;
            };

            self.fanout_snapshot(&region_query_handler, &entry, &filter, is_complete)
                .await;

            if is_complete {
                tokio::select! {
                    _ = entry.subscriber_changed.notified() => {}
                    result = lifecycle_rx.changed() => {
                        if result.is_err() || *lifecycle_rx.borrow() {
                            break;
                        }
                    }
                }
                continue;
            }

            tokio::select! {
                _ = filter.wait_update() => {}
                _ = filter.wait_complete() => {
                    is_complete = true;
                }
                // `wait_update()` subscribes when called, so an update that happens between
                // the post-send generation check and the wait call can be missed. This
                // low-frequency reconcile tick bounds that race by periodically re-reading
                // `snapshot_generation()` and coalescing to the latest snapshot.
                _ = reconcile_interval.tick() => {}
                _ = entry.subscriber_changed.notified() => {}
                result = lifecycle_rx.changed() => {
                    if result.is_err() || *lifecycle_rx.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn fanout_snapshot(
        &self,
        region_query_handler: &RegionQueryHandlerRef,
        entry: &DynFilterEntry,
        filter: &DynamicFilterPhysicalExpr,
        is_complete: bool,
    ) {
        let Some((generation, current)) = current_stable_snapshot(filter).await else {
            return;
        };

        if !is_complete && !entry.mark_generation_sent(generation) {
            return;
        }

        if is_complete {
            let _ = entry.mark_generation_sent(generation);
        }

        let payload = match DynFilterPayload::from_datafusion_expr(
            &current,
            REMOTE_DYN_FILTER_UPDATE_PAYLOAD_MAX_BYTES,
        ) {
            Ok(DynFilterPayload::Datafusion(payload)) => payload,
            Ok(_) => {
                warn!("Ignored unsupported remote dynamic filter producer payload");
                return;
            }
            Err(error) => {
                warn!(error; "Failed to encode remote dynamic filter producer snapshot");
                return;
            }
        };

        self.fanout_update(
            region_query_handler,
            entry,
            generation,
            is_complete,
            payload,
        )
        .await;
    }

    async fn fanout_update(
        &self,
        region_query_handler: &RegionQueryHandlerRef,
        entry: &DynFilterEntry,
        generation: u64,
        is_complete: bool,
        payload: Vec<u8>,
    ) {
        let query_id = self.query_id.to_string();
        let filter_id = entry.filter_id().to_string();

        for subscriber in entry.subscribers() {
            let update = RemoteDynFilterUpdate {
                filter_id: filter_id.clone(),
                payload: payload.clone(),
                generation,
                is_complete,
            };

            if let Err(error) = region_query_handler
                .handle_remote_dyn_filter_update(subscriber.region_id(), query_id.clone(), update)
                .await
            {
                warn!(error; "Failed to fan out remote dynamic filter update");
            }
        }
    }

    async fn unregister_all_once(&self, region_query_handler: &RegionQueryHandlerRef) {
        for entry in self.entries() {
            self.unregister_entry_once(region_query_handler, &entry)
                .await;
        }
    }

    async fn unregister_entry_once(
        &self,
        region_query_handler: &RegionQueryHandlerRef,
        entry: &DynFilterEntry,
    ) {
        if !entry.try_mark_unregistered() {
            return;
        }

        let query_id = self.query_id.to_string();
        let filter_id = entry.filter_id().to_string();

        for subscriber in entry.subscribers() {
            let unregister = RemoteDynFilterUnregister {
                filter_id: filter_id.clone(),
            };

            if let Err(error) = region_query_handler
                .handle_remote_dyn_filter_unregister(
                    subscriber.region_id(),
                    query_id.clone(),
                    unregister,
                )
                .await
            {
                warn!(error; "Failed to fan out remote dynamic filter unregister");
            }
        }

        debug!("Remote dynamic filter producer unregistered subscribers");
    }
}

async fn current_stable_snapshot(
    filter: &DynamicFilterPhysicalExpr,
) -> Option<(u64, Arc<dyn PhysicalExpr>)> {
    loop {
        let before = filter.snapshot_generation();
        let current = match filter.current() {
            Ok(current) => current,
            Err(error) => {
                warn!(error; "Failed to read remote dynamic filter producer snapshot");
                return None;
            }
        };
        let after = filter.snapshot_generation();

        if before == after {
            return Some((after, current));
        }

        tokio::task::yield_now().await;
    }
}

/// Stream-scoped lease that keeps a query registry alive.
///
/// Production code owns registries through this lease; the manager only keeps a weak index.
#[derive(Debug)]
pub struct RemoteDynFilterRegistryLease {
    registry_manager: Arc<DynFilterRegistryManager>,
    /// Always `Some` while the lease is alive.
    ///
    /// `Option` lets `Drop` release the strong `Arc` before pruning the weak index.
    registry: Option<Arc<QueryDynFilterRegistry>>,
}

impl RemoteDynFilterRegistryLease {
    fn new(
        registry_manager: Arc<DynFilterRegistryManager>,
        registry: Arc<QueryDynFilterRegistry>,
    ) -> Self {
        registry.acquire_stream();
        Self {
            registry_manager,
            registry: Some(registry),
        }
    }

    pub fn registry(&self) -> &QueryDynFilterRegistry {
        self.registry
            .as_deref()
            .expect("remote dyn filter registry lease must hold a registry")
    }

    pub fn ensure_fanout_task(&self, region_query_handler: RegionQueryHandlerRef) {
        self.registry
            .as_ref()
            .expect("remote dyn filter registry lease must hold a registry")
            .ensure_fanout_task(region_query_handler);
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(
            self.registry.as_ref().unwrap(),
            other.registry.as_ref().unwrap(),
        )
    }
}

impl Drop for RemoteDynFilterRegistryLease {
    fn drop(&mut self) {
        let Some(registry) = self.registry.take() else {
            return;
        };
        let query_id = registry.query_id();
        let registry_weak = Arc::downgrade(&registry);

        registry.release_stream();

        // Release this lease before pruning; concurrent drops must not observe each other's stream refs.
        drop(registry);

        let _ = self
            .registry_manager
            .remove_if_inactive_registry(&query_id, &registry_weak);
    }
}

/// Query-engine manager for query-scoped remote dynamic filter registries.
///
/// Weak index only; active streams own registries through [`RemoteDynFilterRegistryLease`].
#[derive(Debug, Default)]
pub struct DynFilterRegistryManager {
    registries: RwLock<HashMap<QueryId, Weak<QueryDynFilterRegistry>>>,
}

impl DynFilterRegistryManager {
    #[cfg(test)]
    fn get(&self, query_id: &QueryId) -> Option<Arc<QueryDynFilterRegistry>> {
        let (registry, stale_entry) = {
            let registries = self.registries.read().unwrap();
            let registry = registries.get(query_id)?;

            (registry.upgrade(), registry.clone())
        };

        if registry.is_none() {
            self.remove_stale_entry(query_id, &stale_entry);
        }

        registry
    }

    #[cfg(test)]
    fn remove(&self, query_id: &QueryId) -> Option<Weak<QueryDynFilterRegistry>> {
        self.registries.write().unwrap().remove(query_id)
    }

    fn remove_if_inactive_registry(
        &self,
        query_id: &QueryId,
        registry_weak: &Weak<QueryDynFilterRegistry>,
    ) -> Option<Weak<QueryDynFilterRegistry>> {
        let mut registries = self
            .registries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = registries.get(query_id)?;

        // `ptr_eq` protects a newer registry for the same query id. In 03b the fanout
        // task can briefly hold a strong `Arc`, so stream lifecycle uses active_streams
        // instead of relying only on `Weak::upgrade().is_none()`.
        let inactive = match current.upgrade() {
            Some(registry) => registry.active_stream_count() == 0,
            None => true,
        };
        if current.ptr_eq(registry_weak) && inactive {
            registries.remove(query_id)
        } else {
            None
        }
    }

    #[cfg(test)]
    fn remove_stale_entry(
        &self,
        query_id: &QueryId,
        stale_registry: &Weak<QueryDynFilterRegistry>,
    ) {
        let mut registries = self.registries.write().unwrap();
        let Some(current) = registries.get(query_id) else {
            return;
        };

        if current.ptr_eq(stale_registry) && current.upgrade().is_none() {
            registries.remove(query_id);
        }
    }

    /// Acquires the stream-owned registry lease for `query_id`.
    ///
    /// Returns a lease holding a strong registry reference.
    pub fn acquire_lease(self: &Arc<Self>, query_id: QueryId) -> RemoteDynFilterRegistryLease {
        let registry = self.get_or_init(query_id);
        RemoteDynFilterRegistryLease::new(self.clone(), registry)
    }

    fn get_or_init(&self, query_id: QueryId) -> Arc<QueryDynFilterRegistry> {
        let mut registries = self.registries.write().unwrap();

        if let Some(registry) = registries.get(&query_id).and_then(Weak::upgrade) {
            return registry;
        }

        let registry = Arc::new(QueryDynFilterRegistry::new(query_id));
        registries.insert(query_id, Arc::downgrade(&registry));
        registry
    }

    #[cfg(test)]
    pub fn registry_count(&self) -> usize {
        // Test snapshot helper; lifecycle decisions use lease-owned Arcs and weak pruning.
        self.registries
            .read()
            .unwrap()
            .values()
            .filter(|registry| registry.strong_count() > 0)
            .count()
    }

    #[cfg(test)]
    fn weak_entry_count(&self) -> usize {
        self.registries.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
    use async_trait::async_trait;
    use common_query::request::QueryRequest;
    use datafusion_physical_expr::expressions::{Column, lit};
    use session::ReadPreference;
    use uuid::Uuid;

    use super::*;
    use crate::dist_plan::{FilterFingerprint, RemoteDynFilterProducerId};
    use crate::error::Result as QueryResult;
    use crate::region_query::RegionQueryHandler;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedUpdate {
        region_id: RegionId,
        query_id: String,
        filter_id: String,
        generation: u64,
        is_complete: bool,
        payload: Vec<u8>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedUnregister {
        region_id: RegionId,
        query_id: String,
        filter_id: String,
    }

    #[derive(Default)]
    struct RecordingRegionQueryHandler {
        updates: Mutex<Vec<RecordedUpdate>>,
        unregisters: Mutex<Vec<RecordedUnregister>>,
        block_next_update: AtomicBool,
        update_blocked: Notify,
        release_update: Notify,
    }

    impl RecordingRegionQueryHandler {
        fn updates(&self) -> Vec<RecordedUpdate> {
            self.updates.lock().unwrap().clone()
        }

        fn unregisters(&self) -> Vec<RecordedUnregister> {
            self.unregisters.lock().unwrap().clone()
        }

        fn block_next_update(&self) {
            self.block_next_update.store(true, Ordering::SeqCst);
        }

        async fn wait_for_blocked_update(&self) {
            self.update_blocked.notified().await;
        }

        fn release_blocked_update(&self) {
            self.release_update.notify_one();
        }

        async fn wait_for_update_count(&self, expected: usize) {
            for _ in 0..300 {
                if self.updates().len() >= expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for {expected} remote dyn filter updates");
        }

        async fn wait_for_unregister_count(&self, expected: usize) {
            for _ in 0..300 {
                if self.unregisters().len() >= expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for {expected} remote dyn filter unregisters");
        }
    }

    #[async_trait]
    impl RegionQueryHandler for RecordingRegionQueryHandler {
        async fn do_get(
            &self,
            _read_preference: ReadPreference,
            _request: QueryRequest,
        ) -> QueryResult<common_recordbatch::SendableRecordBatchStream> {
            unreachable!("remote dyn filter registry tests should not execute remote queries")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            region_id: RegionId,
            query_id: String,
            update: RemoteDynFilterUpdate,
        ) -> QueryResult<()> {
            let should_block = self.block_next_update.swap(false, Ordering::SeqCst);
            self.updates.lock().unwrap().push(RecordedUpdate {
                region_id,
                query_id,
                filter_id: update.filter_id,
                generation: update.generation,
                is_complete: update.is_complete,
                payload: update.payload,
            });
            if should_block {
                self.update_blocked.notify_one();
                self.release_update.notified().await;
            }
            Ok(())
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            region_id: RegionId,
            query_id: String,
            unregister: RemoteDynFilterUnregister,
        ) -> QueryResult<()> {
            self.unregisters.lock().unwrap().push(RecordedUnregister {
                region_id,
                query_id,
                filter_id: unregister.filter_id,
            });
            Ok(())
        }
    }

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    fn test_filter_id(producer_ordinal: u32) -> FilterId {
        FilterId::new(
            RemoteDynFilterProducerId::new(42),
            producer_ordinal,
            FilterFingerprint::new(0xabc),
        )
    }

    fn test_dyn_filter(names: &[&str]) -> Arc<DynamicFilterPhysicalExpr> {
        let children = names
            .iter()
            .enumerate()
            .map(|(index, name)| Arc::new(Column::new(name, index)) as _)
            .collect();

        Arc::new(DynamicFilterPhysicalExpr::new(children, lit(true) as _))
    }

    #[test]
    fn registry_manager_returns_same_registry_for_same_query() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);
        let first = manager.acquire_lease(query_id);
        let second = manager.acquire_lease(query_id);

        assert!(first.ptr_eq(&second));
        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);
    }

    #[test]
    fn registry_manager_removes_registry_for_query() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let lease = manager.acquire_lease(query_id);

        assert!(
            manager
                .remove(&query_id)
                .unwrap()
                .ptr_eq(&Arc::downgrade(lease.registry.as_ref().unwrap()))
        );
        assert!(manager.get(&query_id).is_none());
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);
    }

    #[test]
    fn registry_manager_lease_waits_for_last_query_scoped_stream() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let first = manager.acquire_lease(query_id);
        let second = manager.acquire_lease(query_id);

        assert!(first.ptr_eq(&second));
        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);
        drop(first);
        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);

        drop(second);
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);
    }

    #[test]
    fn registry_manager_lease_does_not_remove_reacquired_registry() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let first = manager.acquire_lease(query_id);
        drop(first);
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);

        let second = manager.acquire_lease(query_id);

        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);
        drop(second);
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);
    }

    #[test]
    fn registry_manager_concurrent_final_lease_drop_cleans_weak_entry() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);
        let first = manager.acquire_lease(query_id);
        let second = manager.acquire_lease(query_id);
        let barrier = Arc::new(Barrier::new(3));

        let first_barrier = barrier.clone();
        let first_drop = thread::spawn(move || {
            first_barrier.wait();
            drop(first);
        });

        let second_barrier = barrier.clone();
        let second_drop = thread::spawn(move || {
            second_barrier.wait();
            drop(second);
        });

        barrier.wait();
        first_drop.join().unwrap();
        second_drop.join().unwrap();

        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);
    }

    #[test]
    fn registry_manager_concurrent_first_acquire_shares_registry() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);
        let worker_count = 8;
        let barrier = Arc::new(Barrier::new(worker_count + 1));

        let handles = (0..worker_count)
            .map(|_| {
                let manager = manager.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    manager.acquire_lease(query_id)
                })
            })
            .collect::<Vec<_>>();

        barrier.wait();
        let leases = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        let first = leases.first().unwrap();
        assert!(leases.iter().all(|lease| first.ptr_eq(lease)));
        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);

        drop(leases);
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);
    }

    #[test]
    fn registry_manager_drop_racing_acquire_does_not_leave_stale_entry() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        for _ in 0..64 {
            let old_lease = manager.acquire_lease(query_id);
            let barrier = Arc::new(Barrier::new(3));

            let drop_barrier = barrier.clone();
            let drop_thread = thread::spawn(move || {
                drop_barrier.wait();
                drop(old_lease);
            });

            let acquire_manager = manager.clone();
            let acquire_barrier = barrier.clone();
            let acquire_thread = thread::spawn(move || {
                acquire_barrier.wait();
                acquire_manager.acquire_lease(query_id)
            });

            barrier.wait();
            drop_thread.join().unwrap();
            let new_lease = acquire_thread.join().unwrap();

            assert_eq!(manager.registry_count(), 1);
            assert_eq!(manager.weak_entry_count(), 1);
            drop(new_lease);
            assert_eq!(manager.registry_count(), 0);
            assert_eq!(manager.weak_entry_count(), 0);
        }
    }

    #[test]
    fn registry_manager_old_drop_cannot_remove_replacement_registry() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);
        let old_lease = manager.acquire_lease(query_id);
        let old_registry = Arc::downgrade(old_lease.registry.as_ref().unwrap());

        drop(old_lease);
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);

        let replacement_lease = manager.acquire_lease(query_id);
        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);

        assert!(
            manager
                .remove_if_inactive_registry(&query_id, &old_registry)
                .is_none(),
            "old registry cleanup must not remove the replacement weak entry"
        );
        assert_eq!(manager.registry_count(), 1);
        assert_eq!(manager.weak_entry_count(), 1);

        drop(replacement_lease);
        assert_eq!(manager.registry_count(), 0);
        assert_eq!(manager.weak_entry_count(), 0);
    }

    #[test]
    fn registry_stores_filter_and_deduplicates_subscribers() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let entry = match registry.register_remote_dyn_filter(filter_id.clone(), filter.clone()) {
            EntryRegistration::Inserted(entry) => entry,
            other => panic!("unexpected registration result: {other:?}"),
        };

        assert_eq!(entry.filter_id(), &filter_id);
        assert_eq!(registry.entry_count(), 1);

        let subscriber = Subscriber::new(RegionId::new(1024, 1));
        assert_eq!(
            registry.register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );
        assert_eq!(
            registry.register_subscriber(&filter_id, subscriber),
            SubscriberRegistration::Duplicate
        );
        assert_eq!(entry.subscribers().len(), 1);
    }

    #[tokio::test]
    async fn fanout_sends_changed_generations_to_subscribers() {
        let query_id = test_query_id(1);
        let registry = Arc::new(QueryDynFilterRegistry::new(query_id));
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let entry = match registry.register_remote_dyn_filter(filter_id.clone(), filter.clone()) {
            EntryRegistration::Inserted(entry) => entry,
            other => panic!("unexpected registration result: {other:?}"),
        };
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            registry.register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        let handler_ref = handler.clone() as RegionQueryHandlerRef;

        registry
            .fanout_snapshot(&handler_ref, &entry, filter.as_ref(), false)
            .await;
        let updates = handler.updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].region_id, subscriber.region_id());
        assert_eq!(updates[0].query_id, query_id.to_string());
        assert_eq!(updates[0].filter_id, filter_id.to_string());
        assert_eq!(updates[0].generation, filter.snapshot_generation());
        assert!(!updates[0].is_complete);
        assert!(!updates[0].payload.is_empty());

        registry
            .fanout_snapshot(&handler_ref, &entry, filter.as_ref(), false)
            .await;
        assert_eq!(handler.updates().len(), 1);

        filter.update(lit(false) as _).unwrap();
        registry
            .fanout_snapshot(&handler_ref, &entry, filter.as_ref(), false)
            .await;
        let updates = handler.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[1].generation, filter.snapshot_generation());

        let second_subscriber = Subscriber::new(RegionId::new(1024, 8));
        assert_eq!(
            registry.register_subscriber(&filter_id, second_subscriber.clone()),
            SubscriberRegistration::Added
        );
        registry
            .fanout_snapshot(&handler_ref, &entry, filter.as_ref(), false)
            .await;
        let updates = handler.updates();
        assert_eq!(updates.len(), 4);
        assert!(
            updates[2..]
                .iter()
                .any(|update| update.region_id == subscriber.region_id())
        );
        assert!(
            updates[2..]
                .iter()
                .any(|update| update.region_id == second_subscriber.region_id())
        );
        assert_eq!(entry.subscribers().len(), 2);
    }

    #[tokio::test]
    async fn fanout_task_waits_for_dynamic_filter_notifications() {
        let query_id = test_query_id(3);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry = manager.get(&query_id).unwrap();
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = registry.register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            registry.register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        registry.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);

        handler.wait_for_update_count(1).await;
        let initial_generation = handler.updates()[0].generation;

        filter.update(lit(false) as _).unwrap();
        handler.wait_for_update_count(2).await;
        let updates = handler.updates();
        assert!(updates[1].generation > initial_generation);
        assert_eq!(updates[1].region_id, subscriber.region_id());
        assert_eq!(updates[1].filter_id, filter_id.to_string());

        filter.mark_complete();
        handler.wait_for_update_count(3).await;
        let updates = handler.updates();
        assert!(updates[2].is_complete);

        drop(lease);
        handler.wait_for_unregister_count(1).await;
        let unregisters = handler.unregisters();
        assert_eq!(unregisters[0].region_id, subscriber.region_id());
        assert_eq!(unregisters[0].filter_id, filter_id.to_string());
    }

    #[tokio::test]
    async fn reconcile_tick_catches_update_while_fanout_is_in_flight() {
        let query_id = test_query_id(4);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry = manager.get(&query_id).unwrap();
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = registry.register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            registry.register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        handler.block_next_update();
        registry.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);

        handler.wait_for_blocked_update().await;
        let initial_generation = handler.updates()[0].generation;

        // Update while the first RPC is still in-flight. A pure `wait_update()` loop can
        // subscribe after this update and miss the edge; the reconcile tick must still
        // observe `snapshot_generation() > last_sent_generation` and fan out the latest
        // snapshot.
        filter.update(lit(false) as _).unwrap();
        handler.release_blocked_update();

        handler.wait_for_update_count(2).await;
        let updates = handler.updates();
        assert!(updates[1].generation > initial_generation);
        assert_eq!(updates[1].region_id, subscriber.region_id());
        assert_eq!(updates[1].filter_id, filter_id.to_string());

        drop(lease);
        handler.wait_for_unregister_count(1).await;
    }

    #[tokio::test]
    async fn unregister_fanout_is_idempotent() {
        let query_id = test_query_id(2);
        let registry = QueryDynFilterRegistry::new(query_id);
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = registry.register_remote_dyn_filter(filter_id.clone(), filter);
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            registry.register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        let handler_ref = handler.clone() as RegionQueryHandlerRef;

        registry.unregister_all_once(&handler_ref).await;
        registry.unregister_all_once(&handler_ref).await;

        let unregisters = handler.unregisters();
        assert_eq!(unregisters.len(), 1);
        assert_eq!(unregisters[0].region_id, subscriber.region_id());
        assert_eq!(unregisters[0].query_id, query_id.to_string());
        assert_eq!(unregisters[0].filter_id, filter_id.to_string());
    }
}
