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
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
use common_query::request::{DynFilterPayload, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES};
use common_runtime::spawn_global;
use common_telemetry::{debug, warn};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use session::query_id::QueryId;
use store_api::storage::RegionId;
use tokio::sync::{Notify, watch};

use crate::dist_plan::FilterId;
use crate::metrics;
use crate::region_query::RegionQueryHandlerRef;

const REMOTE_DYN_FILTER_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
/// Bound best-effort RDF control RPCs so one bad subscriber cannot stall fanout.
const REMOTE_DYN_FILTER_CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Region subscribed to a remote dynamic filter.
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

/// A registered query-local producer filter and its region subscribers.
#[derive(Debug)]
pub struct DynFilterEntry {
    filter_id: FilterId,
    producer_filter: Weak<DynamicFilterPhysicalExpr>,
    subscribers: RwLock<HashSet<Subscriber>>,
    state: Mutex<DynFilterEntryState>,
    subscriber_changed: Notify,
}

#[derive(Debug, Default)]
struct DynFilterEntryState {
    last_sent_generation: u64,
    unregistered: bool,
    fanout_started: bool,
}

#[derive(Debug)]
struct QueryDynFilterRegistryInner {
    entries: HashMap<FilterId, Arc<DynFilterEntry>>,
}

impl DynFilterEntry {
    pub fn new(filter_id: FilterId, producer_filter: Arc<DynamicFilterPhysicalExpr>) -> Self {
        Self {
            filter_id,
            producer_filter: Arc::downgrade(&producer_filter),
            subscribers: RwLock::new(HashSet::new()),
            state: Mutex::new(DynFilterEntryState::default()),
            subscriber_changed: Notify::new(),
        }
    }

    pub fn filter_id(&self) -> &FilterId {
        &self.filter_id
    }

    pub fn upgrade_producer_filter(&self) -> Option<Arc<DynamicFilterPhysicalExpr>> {
        self.producer_filter.upgrade()
    }

    pub fn subscribers(&self) -> Vec<Subscriber> {
        self.subscribers.read().unwrap().iter().cloned().collect()
    }

    pub fn register_subscriber(&self, subscriber: Subscriber) -> bool {
        let mut subscribers = self.subscribers.write().unwrap();
        subscribers.insert(subscriber)
    }

    fn mark_generation_sent(&self, generation: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if generation <= state.last_sent_generation {
            return false;
        }

        state.last_sent_generation = generation;
        true
    }

    fn try_mark_unregistered(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.unregistered {
            return false;
        }

        state.unregistered = true;
        true
    }

    fn reactivate_for_new_subscriber(&self) {
        {
            let mut state = self.state.lock().unwrap();
            // Reset generation/unregister state so late subscribers get the current snapshot.
            state.last_sent_generation = 0;
            state.unregistered = false;
        }
        self.subscriber_changed.notify_one();
    }

    fn mark_fanout_started(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.fanout_started {
            return false;
        }

        state.fanout_started = true;
        true
    }

    #[cfg(test)]
    pub(crate) fn fanout_started_for_test(&self) -> bool {
        self.state.lock().unwrap().fanout_started
    }
}

/// Query-scoped registry that owns all remote dynamic filters for one query.
#[derive(Debug)]
pub struct QueryDynFilterRegistry {
    query_id: QueryId,
    lifecycle_tx: watch::Sender<()>,
    inner: RwLock<QueryDynFilterRegistryInner>,
}

impl QueryDynFilterRegistry {
    pub fn new(query_id: QueryId) -> Self {
        // Close-only lifecycle signal; dropping the registry closes it for watchers.
        let (lifecycle_tx, _) = watch::channel(());
        Self {
            query_id,
            lifecycle_tx,
            inner: RwLock::new(QueryDynFilterRegistryInner {
                entries: HashMap::new(),
            }),
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
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
        producer_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> EntryRegistration {
        let mut inner = self.inner.write().unwrap();
        if let Some(existing) = inner.entries.get(&filter_id) {
            return EntryRegistration::Existing(existing.clone());
        }

        let entry = Arc::new(DynFilterEntry::new(filter_id.clone(), producer_filter));
        inner.entries.insert(filter_id, entry.clone());
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
            // New subscribers need the current snapshot; existing subscribers may see a duplicate.
            entry.reactivate_for_new_subscriber();
            SubscriberRegistration::Added
        } else {
            SubscriberRegistration::Duplicate
        }
    }

    /// Starts missing producer fanout watchers for the registry's entries.
    ///
    /// Watchers do not hold the registry alive; dropping the registry closes their lifecycle channel.
    pub fn ensure_fanout_task(self: &Arc<Self>, region_query_handler: RegionQueryHandlerRef) {
        for entry in self.entries() {
            ensure_entry_fanout_task(
                self.query_id,
                entry,
                region_query_handler.clone(),
                self.lifecycle_tx.subscribe(),
            );
        }
    }

    #[cfg(test)]
    async fn fanout_snapshot(
        &self,
        region_query_handler: &RegionQueryHandlerRef,
        entry: &DynFilterEntry,
        filter: &DynamicFilterPhysicalExpr,
        is_complete: bool,
    ) {
        let mut lifecycle_rx = self.lifecycle_tx.subscribe();
        fanout_snapshot_for_query(
            self.query_id,
            region_query_handler,
            entry,
            filter,
            is_complete,
            &mut lifecycle_rx,
            REMOTE_DYN_FILTER_CONTROL_RPC_TIMEOUT,
        )
        .await;
    }

    #[cfg(test)]
    async fn unregister_all_once(&self, region_query_handler: &RegionQueryHandlerRef) {
        for entry in self.entries() {
            unregister_entry_once_for_query(region_query_handler, self.query_id, &entry).await;
        }
    }
}

fn ensure_entry_fanout_task(
    query_id: QueryId,
    entry: Arc<DynFilterEntry>,
    region_query_handler: RegionQueryHandlerRef,
    lifecycle_rx: watch::Receiver<()>,
) {
    if !entry.mark_fanout_started() {
        return;
    }

    let _handle = spawn_global(async move {
        run_entry_fanout(query_id, entry, region_query_handler, lifecycle_rx).await;
    });
}

async fn run_entry_fanout(
    query_id: QueryId,
    entry: Arc<DynFilterEntry>,
    region_query_handler: RegionQueryHandlerRef,
    mut lifecycle_rx: watch::Receiver<()>,
) {
    let mut is_complete = false;
    // Start reconcile after one interval and skip missed ticks; it is only a coalescing fallback.
    let mut reconcile_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + REMOTE_DYN_FILTER_RECONCILE_INTERVAL,
        REMOTE_DYN_FILTER_RECONCILE_INTERVAL,
    );
    reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let Some(filter) = entry.upgrade_producer_filter() else {
            unregister_entry_once_for_query(&region_query_handler, query_id, &entry).await;
            return;
        };

        if !fanout_snapshot_for_query(
            query_id,
            &region_query_handler,
            &entry,
            &filter,
            is_complete,
            &mut lifecycle_rx,
            REMOTE_DYN_FILTER_CONTROL_RPC_TIMEOUT,
        )
        .await
        {
            break;
        }

        if is_complete {
            tokio::select! {
                _ = entry.subscriber_changed.notified() => {}
                result = lifecycle_rx.changed() => {
                    if result.is_err() {
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
            // `wait_update()` can miss an update sent while an RPC is in-flight.
            // Re-read periodically to coalesce to the latest generation.
            _ = reconcile_interval.tick() => {}
            _ = entry.subscriber_changed.notified() => {}
            result = lifecycle_rx.changed() => {
                if result.is_err() {
                    break;
                }
            }
        }
    }

    unregister_entry_once_for_query(&region_query_handler, query_id, &entry).await;
}

async fn fanout_snapshot_for_query(
    query_id: QueryId,
    region_query_handler: &RegionQueryHandlerRef,
    entry: &DynFilterEntry,
    filter: &DynamicFilterPhysicalExpr,
    is_complete: bool,
    lifecycle_rx: &mut watch::Receiver<()>,
    control_rpc_timeout: Duration,
) -> bool {
    let Some((generation, current)) = current_stable_snapshot(filter, lifecycle_rx).await else {
        return true;
    };

    // The entry-global watermark advances before best-effort fanout. A timed-out
    // subscriber may miss this generation; later/complete snapshots supersede it,
    // and RDF only prunes.
    if !is_complete && !entry.mark_generation_sent(generation) {
        return true;
    }

    if is_complete {
        let _ = entry.mark_generation_sent(generation);
    }

    let payload =
        match DynFilterPayload::from_datafusion_expr(&current, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES)
        {
            Ok(DynFilterPayload::Datafusion(payload)) => {
                metrics::REMOTE_DYN_FILTER_ENCODE_TOTAL
                    .with_label_values(&["success"])
                    .inc();
                metrics::REMOTE_DYN_FILTER_PAYLOAD_BYTES.observe(payload.len() as f64);
                payload
            }
            Ok(_) => {
                metrics::REMOTE_DYN_FILTER_ENCODE_TOTAL
                    .with_label_values(&["unsupported"])
                    .inc();
                warn!("Ignored unsupported remote dynamic filter producer payload");
                return true;
            }
            Err(error) => {
                metrics::REMOTE_DYN_FILTER_ENCODE_TOTAL
                    .with_label_values(&["error"])
                    .inc();
                warn!(error; "Failed to encode remote dynamic filter producer snapshot");
                return true;
            }
        };

    fanout_update_for_query(
        query_id,
        region_query_handler,
        entry,
        generation,
        is_complete,
        payload,
        lifecycle_rx,
        control_rpc_timeout,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fanout_update_for_query(
    query_id: QueryId,
    region_query_handler: &RegionQueryHandlerRef,
    entry: &DynFilterEntry,
    generation: u64,
    is_complete: bool,
    payload: Vec<u8>,
    lifecycle_rx: &mut watch::Receiver<()>,
    control_rpc_timeout: Duration,
) -> bool {
    let query_id = query_id.to_string();
    let filter_id = entry.filter_id().to_string();

    for subscriber in entry.subscribers() {
        let update = RemoteDynFilterUpdate {
            filter_id: filter_id.clone(),
            payload: payload.clone(),
            generation,
            is_complete,
        };

        match await_control_rpc_or_lifecycle_close(
            lifecycle_rx,
            format!(
                "update query_id={} filter_id={} region_id={}",
                query_id,
                filter_id,
                subscriber.region_id()
            ),
            region_query_handler.handle_remote_dyn_filter_update(
                subscriber.region_id(),
                query_id.clone(),
                update,
            ),
            control_rpc_timeout,
        )
        .await
        {
            ControlRpcResult::Ok(result) => {
                if let Err(error) = result {
                    metrics::REMOTE_DYN_FILTER_UPDATE_RPC_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                    warn!(
                        error;
                        "Failed to fan out remote dynamic filter update, query_id={}, filter_id={}, region_id={}",
                        query_id,
                        filter_id,
                        subscriber.region_id()
                    );
                } else {
                    metrics::REMOTE_DYN_FILTER_UPDATE_RPC_TOTAL
                        .with_label_values(&["success"])
                        .inc();
                }
            }
            ControlRpcResult::TimedOut => {
                metrics::REMOTE_DYN_FILTER_UPDATE_RPC_TOTAL
                    .with_label_values(&["timeout"])
                    .inc();
            }
            ControlRpcResult::LifecycleClosed => {
                metrics::REMOTE_DYN_FILTER_UPDATE_RPC_TOTAL
                    .with_label_values(&["cancelled"])
                    .inc();
                return false;
            }
        }
    }

    true
}

async fn unregister_entry_once_for_query(
    region_query_handler: &RegionQueryHandlerRef,
    query_id: QueryId,
    entry: &DynFilterEntry,
) {
    if !entry.try_mark_unregistered() {
        return;
    }

    let query_id = query_id.to_string();
    let filter_id = entry.filter_id().to_string();

    for subscriber in entry.subscribers() {
        let unregister = RemoteDynFilterUnregister {
            filter_id: filter_id.clone(),
        };

        let Some(result) = await_control_rpc_timeout(
            format!(
                "unregister query_id={} filter_id={} region_id={}",
                query_id,
                filter_id,
                subscriber.region_id()
            ),
            region_query_handler.handle_remote_dyn_filter_unregister(
                subscriber.region_id(),
                query_id.clone(),
                unregister,
            ),
        )
        .await
        else {
            continue;
        };

        if let Err(error) = result {
            warn!(
                error;
                "Failed to fan out remote dynamic filter unregister, query_id={}, filter_id={}, region_id={}",
                query_id,
                filter_id,
                subscriber.region_id()
            );
        }
    }

    debug!("Remote dynamic filter producer unregistered subscribers");
}

enum ControlRpcResult<T> {
    Ok(T),
    TimedOut,
    LifecycleClosed,
}

async fn await_control_rpc_or_lifecycle_close<T>(
    lifecycle_rx: &mut watch::Receiver<()>,
    operation: String,
    rpc: impl Future<Output = T>,
    control_rpc_timeout: Duration,
) -> ControlRpcResult<T> {
    if lifecycle_rx.has_changed().is_err() {
        return ControlRpcResult::LifecycleClosed;
    }

    tokio::select! {
        biased;
        result = lifecycle_rx.changed() => {
            if result.is_err() {
                debug!("Cancelled remote dynamic filter control RPC after lifecycle close");
            }
            ControlRpcResult::LifecycleClosed
        }
        result = rpc => ControlRpcResult::Ok(result),
        _ = tokio::time::sleep(control_rpc_timeout) => {
            warn!("Timed out remote dynamic filter control RPC: {}", operation);
            ControlRpcResult::TimedOut
        }
    }
}

async fn await_control_rpc_timeout<T>(
    operation: String,
    rpc: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        result = rpc => Some(result),
        _ = tokio::time::sleep(REMOTE_DYN_FILTER_CONTROL_RPC_TIMEOUT) => {
            warn!("Timed out remote dynamic filter control RPC: {}", operation);
            None
        }
    }
}

async fn current_stable_snapshot(
    filter: &DynamicFilterPhysicalExpr,
    lifecycle_rx: &mut watch::Receiver<()>,
) -> Option<(u64, Arc<dyn PhysicalExpr>)> {
    loop {
        if lifecycle_rx.has_changed().is_err() {
            return None;
        }

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

        tokio::select! {
            biased;
            result = lifecycle_rx.changed() => {
                if result.is_err() {
                    return None;
                }
            }
            _ = tokio::task::yield_now() => {}
        }
    }
}

/// Stream-scoped lease that keeps a query registry alive.
///
/// Stream leases own registry lifecycle; the manager only keeps a weak index.
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

        // Release this lease before pruning; concurrent drops must not observe each other's strong refs.
        drop(registry);

        let _ = self
            .registry_manager
            .remove_if_dropped_registry(&query_id, &registry_weak);
    }
}

/// Query-engine manager for query-scoped remote dynamic filter registries.
///
/// Weak index only; stream leases own registries through [`RemoteDynFilterRegistryLease`].
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

    fn remove_if_dropped_registry(
        &self,
        query_id: &QueryId,
        dropped_registry: &Weak<QueryDynFilterRegistry>,
    ) -> Option<Weak<QueryDynFilterRegistry>> {
        let mut registries = self
            .registries
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = registries.get(query_id)?;

        // `ptr_eq` protects a newer registry for the same query id; `upgrade` ensures it is dead.
        if current.ptr_eq(dropped_registry) && current.upgrade().is_none() {
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

    async fn wait_for_registry_drop(registry: Weak<QueryDynFilterRegistry>) {
        for _ in 0..300 {
            if registry.upgrade().is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for remote dyn filter registry drop");
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
                .remove_if_dropped_registry(&query_id, &old_registry)
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
        let registry_weak = Arc::downgrade(lease.registry.as_ref().unwrap());
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = lease
            .registry()
            .register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);

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

        wait_for_registry_drop(registry_weak).await;
    }

    #[tokio::test]
    async fn repeated_ensure_fanout_task_keeps_single_watcher() {
        let query_id = test_query_id(6);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry_weak = Arc::downgrade(lease.registry.as_ref().unwrap());
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let entry = match lease
            .registry()
            .register_remote_dyn_filter(filter_id.clone(), filter.clone())
        {
            EntryRegistration::Inserted(entry) => entry,
            other => panic!("unexpected registration result: {other:?}"),
        };
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);

        assert!(entry.fanout_started_for_test());
        handler.wait_for_update_count(1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(handler.updates().len(), 1);

        filter.update(lit(false) as _).unwrap();
        handler.wait_for_update_count(2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(handler.updates().len(), 2);

        drop(lease);
        handler.wait_for_unregister_count(1).await;
        wait_for_registry_drop(registry_weak).await;
    }

    #[tokio::test]
    async fn fanout_task_resends_complete_snapshot_to_late_subscriber() {
        let query_id = test_query_id(7);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry_weak = Arc::downgrade(lease.registry.as_ref().unwrap());
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = lease
            .registry()
            .register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let first_subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, first_subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);
        handler.wait_for_update_count(1).await;

        filter.mark_complete();
        handler.wait_for_update_count(2).await;
        assert!(handler.updates()[1].is_complete);

        let late_subscriber = Subscriber::new(RegionId::new(1024, 8));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, late_subscriber.clone()),
            SubscriberRegistration::Added
        );

        handler.wait_for_update_count(4).await;
        let updates = handler.updates();
        assert!(
            updates[2..].iter().any(
                |update| update.region_id == first_subscriber.region_id() && update.is_complete
            )
        );
        assert!(
            updates[2..]
                .iter()
                .any(|update| update.region_id == late_subscriber.region_id()
                    && update.is_complete)
        );

        drop(lease);
        handler.wait_for_unregister_count(1).await;
        wait_for_registry_drop(registry_weak).await;
    }

    #[tokio::test]
    async fn fanout_task_unregisters_when_producer_filter_is_dropped() {
        let query_id = test_query_id(8);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry_weak = Arc::downgrade(lease.registry.as_ref().unwrap());
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = lease
            .registry()
            .register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);
        handler.wait_for_update_count(1).await;

        drop(filter);
        handler.wait_for_unregister_count(1).await;
        let unregisters = handler.unregisters();
        assert_eq!(unregisters[0].region_id, subscriber.region_id());
        assert_eq!(unregisters[0].filter_id, filter_id.to_string());

        drop(lease);
        wait_for_registry_drop(registry_weak).await;
    }

    #[tokio::test]
    async fn reconcile_tick_catches_update_while_fanout_is_in_flight() {
        let query_id = test_query_id(4);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry_weak = Arc::downgrade(lease.registry.as_ref().unwrap());
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = lease
            .registry()
            .register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        handler.block_next_update();
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);

        handler.wait_for_blocked_update().await;
        let initial_generation = handler.updates()[0].generation;

        // Update before the watcher can subscribe again; reconcile must catch it.
        filter.update(lit(false) as _).unwrap();
        handler.release_blocked_update();

        handler.wait_for_update_count(2).await;
        let updates = handler.updates();
        assert!(updates[1].generation > initial_generation);
        assert_eq!(updates[1].region_id, subscriber.region_id());
        assert_eq!(updates[1].filter_id, filter_id.to_string());

        drop(lease);
        handler.wait_for_unregister_count(1).await;
        wait_for_registry_drop(registry_weak).await;
    }

    #[tokio::test]
    async fn fanout_task_unregisters_after_lifecycle_close_during_blocked_update() {
        let query_id = test_query_id(5);
        let manager = Arc::new(DynFilterRegistryManager::default());
        let lease = manager.acquire_lease(query_id);
        let registry_weak = Arc::downgrade(lease.registry.as_ref().unwrap());
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let _ = lease
            .registry()
            .register_remote_dyn_filter(filter_id.clone(), filter.clone());
        let subscriber = Subscriber::new(RegionId::new(1024, 7));
        assert_eq!(
            lease
                .registry()
                .register_subscriber(&filter_id, subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        handler.block_next_update();
        lease.ensure_fanout_task(handler.clone() as RegionQueryHandlerRef);

        handler.wait_for_blocked_update().await;
        drop(lease);

        handler.wait_for_unregister_count(1).await;
        let unregisters = handler.unregisters();
        assert_eq!(unregisters[0].region_id, subscriber.region_id());
        assert_eq!(unregisters[0].filter_id, filter_id.to_string());
        wait_for_registry_drop(registry_weak).await;
    }

    #[tokio::test]
    async fn update_timeout_does_not_stop_fanout_for_other_subscribers() {
        let query_id = test_query_id(9);
        let registry = QueryDynFilterRegistry::new(query_id);
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(1);
        let entry = match registry.register_remote_dyn_filter(filter_id.clone(), filter.clone()) {
            EntryRegistration::Inserted(entry) => entry,
            other => panic!("unexpected registration result: {other:?}"),
        };
        let first_subscriber = Subscriber::new(RegionId::new(1024, 7));
        let second_subscriber = Subscriber::new(RegionId::new(1024, 8));
        assert_eq!(
            registry.register_subscriber(&filter_id, first_subscriber.clone()),
            SubscriberRegistration::Added
        );
        assert_eq!(
            registry.register_subscriber(&filter_id, second_subscriber.clone()),
            SubscriberRegistration::Added
        );

        let handler = Arc::new(RecordingRegionQueryHandler::default());
        let handler_ref = handler.clone() as RegionQueryHandlerRef;
        let mut lifecycle_rx = registry.lifecycle_tx.subscribe();
        handler.block_next_update();
        assert!(
            fanout_snapshot_for_query(
                query_id,
                &handler_ref,
                &entry,
                filter.as_ref(),
                false,
                &mut lifecycle_rx,
                Duration::from_millis(100),
            )
            .await
        );

        handler.wait_for_blocked_update().await;
        // Fanout is serial and the blocked RPC stays blocked; the second update proves
        // timeout continued to the next subscriber.
        handler.wait_for_update_count(2).await;

        let initial_updates = handler.updates();
        assert_eq!(
            initial_updates.len(),
            2,
            "the healthy subscriber must still receive the update after another subscriber times out"
        );
        assert!(
            initial_updates
                .iter()
                .any(|update| update.region_id == first_subscriber.region_id())
        );
        assert!(
            initial_updates
                .iter()
                .any(|update| update.region_id == second_subscriber.region_id())
        );

        filter.update(lit(false) as _).unwrap();
        assert!(
            fanout_snapshot_for_query(
                query_id,
                &handler_ref,
                &entry,
                filter.as_ref(),
                false,
                &mut lifecycle_rx,
                Duration::from_millis(100),
            )
            .await
        );
        handler.wait_for_update_count(4).await;
        let updates = handler.updates();
        assert!(
            updates[2..]
                .iter()
                .any(|update| update.region_id == first_subscriber.region_id())
        );
        assert!(
            updates[2..]
                .iter()
                .any(|update| update.region_id == second_subscriber.region_id())
        );

        registry.unregister_all_once(&handler_ref).await;
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
