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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use session::query_id::QueryId;
use store_api::storage::RegionId;

use crate::dist_plan::FilterId;

/// Lifecycle state for a query-scoped remote dynamic filter registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryState {
    Active,
    // TODO(remote-dyn-filter): Subtask 04+ should wire query finish/cancel hooks to move a
    // registry into Closing, then drive the cleanup tail (final unregister/complete RPCs,
    // watcher shutdown, and any in-flight control-path draining) before mark_closed().
    Closing,
    Closed,
}

/// Routing metadata for a remote dynamic filter subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    Existing(Arc<DynFilterEntry>),
    RejectedByState(RegistryState),
}

/// Result of registering a subscriber under an existing filter entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriberRegistration {
    Added,
    Duplicate,
    MissingFilter,
    RejectedByState(RegistryState),
}

/// A registered query-local remote dynamic filter entry.
///
/// This stores the alive DataFusion filter handle together with the subscriber fanout metadata
/// and the registry-owned watcher bookkeeping that later subtasks will drive.
// TODO(remote-dyn-filter): Revisit whether this filter-level entry should stay this rich once
// the real watcher/fanout loop lands. Some fields may move to query-level shared runtime state.
#[derive(Debug)]
pub struct DynFilterEntry {
    filter_id: FilterId,
    alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
    last_epoch: AtomicU64,
    last_observed_generation: AtomicU64,
    subscribers: RwLock<Vec<Subscriber>>,
    // TODO(remote-dyn-filter): This watcher bookkeeping is only a subtask-03 skeleton
    // placeholder for the later wait_update/fanout wiring. Revisit whether filter-level
    // watcher state is still the right shape once the real async cleanup/update loop lands.
    watcher_started: AtomicBool,
}

#[derive(Debug)]
struct QueryDynFilterRegistryInner {
    state: RegistryState,
    entries: HashMap<FilterId, Arc<DynFilterEntry>>,
}

impl DynFilterEntry {
    pub fn new(filter_id: FilterId, alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>) -> Self {
        // TODO(remote-dyn-filter): When real watcher/update scheduling lands, confirm that seeding
        // the observed generation here is still the right initialization point.
        let last_observed_generation = alive_dyn_filter.snapshot_generation();

        Self {
            filter_id,
            alive_dyn_filter,
            last_epoch: AtomicU64::new(0),
            last_observed_generation: AtomicU64::new(last_observed_generation),
            subscribers: RwLock::new(Vec::new()),
            watcher_started: AtomicBool::new(false),
        }
    }

    pub fn filter_id(&self) -> &FilterId {
        &self.filter_id
    }

    pub fn alive_dyn_filter(&self) -> Arc<DynamicFilterPhysicalExpr> {
        self.alive_dyn_filter.clone()
    }

    pub fn last_epoch(&self) -> u64 {
        self.last_epoch.load(Ordering::SeqCst)
    }

    pub fn set_last_epoch(&self, epoch: u64) {
        // TODO(remote-dyn-filter): Later subtasks should centralize epoch advancement with the
        // actual unary update dispatch path so this does not drift from sent update ordering.
        self.last_epoch.store(epoch, Ordering::SeqCst);
    }

    pub fn last_observed_generation(&self) -> u64 {
        self.last_observed_generation.load(Ordering::SeqCst)
    }

    pub fn set_last_observed_generation(&self, generation: u64) {
        // TODO(remote-dyn-filter): Later subtasks should update this only after the watcher has
        // consumed a new alive filter snapshot and decided whether to emit a remote update.
        self.last_observed_generation
            .store(generation, Ordering::SeqCst);
    }

    pub fn subscribers(&self) -> Vec<Subscriber> {
        self.subscribers.read().unwrap().clone()
    }

    pub fn register_subscriber(&self, subscriber: Subscriber) -> bool {
        let mut subscribers = self.subscribers.write().unwrap();
        if subscribers.contains(&subscriber) {
            return false;
        }

        subscribers.push(subscriber);
        true
    }

    pub fn start_watcher_if_needed(&self) -> bool {
        // TODO(remote-dyn-filter): Replace this placeholder gate with the real async watcher task
        // launch point once wait_update/fanout wiring exists. Re-evaluate whether the gate still
        // belongs on each filter entry or should move to query-level dispatch state.
        self.watcher_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn watcher_started(&self) -> bool {
        self.watcher_started.load(Ordering::SeqCst)
    }

    pub fn mark_watcher_stopped(&self) {
        // TODO(remote-dyn-filter): Hook this into the real watcher shutdown path during the
        // Closing cleanup tail so the registry only reaches Closed after watcher teardown.
        self.watcher_started.store(false, Ordering::SeqCst);
    }
}

/// Query-scoped registry that owns all remote dynamic filters for one query.
#[derive(Debug)]
pub struct QueryDynFilterRegistry {
    query_id: QueryId,
    inner: RwLock<QueryDynFilterRegistryInner>,
}

impl QueryDynFilterRegistry {
    pub fn new(query_id: QueryId) -> Self {
        Self {
            query_id,
            inner: RwLock::new(QueryDynFilterRegistryInner {
                state: RegistryState::Active,
                entries: HashMap::new(),
            }),
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn state(&self) -> RegistryState {
        self.inner.read().unwrap().state
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
        // TODO(remote-dyn-filter): Subtask 05 should call this from the MergeScan bridge after it
        // identifies a remote-propagatable alive dyn filter for the current query.
        let mut inner = self.inner.write().unwrap();
        if inner.state != RegistryState::Active {
            return EntryRegistration::RejectedByState(inner.state);
        }

        if let Some(existing) = inner.entries.get(&filter_id) {
            return EntryRegistration::Existing(existing.clone());
        }

        let entry = Arc::new(DynFilterEntry::new(filter_id.clone(), alive_dyn_filter));
        inner.entries.insert(filter_id, entry.clone());
        EntryRegistration::Inserted(entry)
    }

    pub fn register_subscriber(
        &self,
        filter_id: &FilterId,
        subscriber: Subscriber,
    ) -> SubscriberRegistration {
        // TODO(remote-dyn-filter): Later subtasks should route remote subscriber metadata into
        // this method when MergeScan builds the query_id + filter_id fanout map.
        let entry = {
            let inner = self.inner.read().unwrap();
            if inner.state != RegistryState::Active {
                return SubscriberRegistration::RejectedByState(inner.state);
            }

            let Some(entry) = inner.entries.get(filter_id) else {
                return SubscriberRegistration::MissingFilter;
            };

            entry.clone()
        };

        let inner = self.inner.read().unwrap();
        if inner.state != RegistryState::Active {
            return SubscriberRegistration::RejectedByState(inner.state);
        }

        let Some(current_entry) = inner.entries.get(filter_id) else {
            return SubscriberRegistration::MissingFilter;
        };
        if !Arc::ptr_eq(current_entry, &entry) {
            return SubscriberRegistration::MissingFilter;
        }

        if entry.register_subscriber(subscriber) {
            SubscriberRegistration::Added
        } else {
            SubscriberRegistration::Duplicate
        }
    }

    pub fn begin_closing(&self) -> RegistryState {
        let mut inner = self.inner.write().unwrap();
        match inner.state {
            RegistryState::Active => {
                // TODO(remote-dyn-filter): Closing is where the later cleanup tail starts. After
                // this transition, new registrations stay rejected while existing entries remain
                // available for final unregister/complete fanout and watcher shutdown.
                inner.state = RegistryState::Closing;
                RegistryState::Closing
            }
            RegistryState::Closing | RegistryState::Closed => inner.state,
        }
    }

    pub fn mark_closed(&self) {
        // TODO(remote-dyn-filter): Call this only after Closing cleanup finishes (final control
        // RPCs sent, watchers stopped, and any short tail work drained). The manager removes the
        // registry from its query map after this point.
        self.inner.write().unwrap().state = RegistryState::Closed;
    }
}

/// Query-engine manager for query-scoped remote dynamic filter registries.
#[derive(Debug, Default)]
pub struct DynFilterRegistryManager {
    registries: RwLock<HashMap<QueryId, Arc<QueryDynFilterRegistry>>>,
}

impl DynFilterRegistryManager {
    pub fn get(&self, query_id: &QueryId) -> Option<Arc<QueryDynFilterRegistry>> {
        self.registries.read().unwrap().get(query_id).cloned()
    }

    pub fn get_or_init(&self, query_id: QueryId) -> Arc<QueryDynFilterRegistry> {
        // TODO(remote-dyn-filter): Subtask 04 should wire query-engine runtime ownership through
        // this entry point so query_id-scoped registries live with distributed query execution.
        let mut registries = self.registries.write().unwrap();

        registries
            .entry(query_id)
            .or_insert_with(|| Arc::new(QueryDynFilterRegistry::new(query_id)))
            .clone()
    }

    pub fn begin_closing(&self, query_id: &QueryId) -> Option<Arc<QueryDynFilterRegistry>> {
        // TODO(remote-dyn-filter): Query finish/cancel hooks should call this to start the cleanup
        // tail, not remove the registry immediately.
        let registry = self.get(query_id)?;
        registry.begin_closing();
        Some(registry)
    }

    pub fn reap_closed(&self, query_id: &QueryId) -> bool {
        // TODO(remote-dyn-filter): Cleanup code should call this only after mark_closed(). If a
        // later implementation needs a retained closed-tail window, expand here instead of adding
        // ad-hoc removal at call sites.
        let mut registries = self.registries.write().unwrap();
        let Some(registry) = registries.get(query_id) else {
            return false;
        };
        if registry.state() != RegistryState::Closed {
            return false;
        }

        registries.remove(query_id);
        true
    }

    pub fn registry_count(&self) -> usize {
        self.registries.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_physical_expr::expressions::{Column, lit};
    use uuid::Uuid;

    use super::*;
    use crate::dist_plan::FilterFingerprint;

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    fn test_filter_id(region_id: RegionId, producer_ordinal: u32) -> FilterId {
        FilterId::new(region_id, producer_ordinal, FilterFingerprint::new(0xabc))
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
        let manager = DynFilterRegistryManager::default();
        let query_id = test_query_id(1);
        let first = manager.get_or_init(query_id);
        let second = manager.get_or_init(query_id);

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(manager.registry_count(), 1);
    }

    #[test]
    fn registry_stores_filter_and_deduplicates_subscribers() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));
        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(RegionId::new(1024, 7), 1);
        let entry = match registry.register_remote_dyn_filter(filter_id.clone(), filter.clone()) {
            EntryRegistration::Inserted(entry) => entry,
            other => panic!("unexpected registration result: {other:?}"),
        };

        assert_eq!(entry.filter_id(), &filter_id);
        assert_eq!(
            entry.last_observed_generation(),
            filter.snapshot_generation()
        );
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

    #[test]
    fn registry_lifecycle_rejects_new_work_after_closing() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));

        assert_eq!(registry.state(), RegistryState::Active);
        assert_eq!(registry.begin_closing(), RegistryState::Closing);
        assert_eq!(registry.state(), RegistryState::Closing);

        let filter = test_dyn_filter(&["host"]);
        let filter_id = test_filter_id(RegionId::new(1024, 7), 1);
        assert!(matches!(
            registry.register_remote_dyn_filter(filter_id, filter),
            EntryRegistration::RejectedByState(RegistryState::Closing)
        ));

        registry.mark_closed();
        assert_eq!(registry.state(), RegistryState::Closed);
    }

    #[test]
    fn registered_filter_starts_watcher_once() {
        let entry = DynFilterEntry::new(
            test_filter_id(RegionId::new(1024, 7), 1),
            test_dyn_filter(&["host"]),
        );

        assert!(entry.start_watcher_if_needed());
        assert!(entry.watcher_started());
        assert!(!entry.start_watcher_if_needed());

        entry.mark_watcher_stopped();
        assert!(!entry.watcher_started());
    }

    #[test]
    fn manager_reaps_closed_registry() {
        let manager = DynFilterRegistryManager::default();
        let query_id = test_query_id(1);
        let registry = manager.get_or_init(query_id);
        let _ = registry.register_remote_dyn_filter(
            test_filter_id(RegionId::new(1024, 7), 1),
            test_dyn_filter(&["host"]),
        );

        registry.mark_closed();

        assert!(manager.reap_closed(&query_id));
        assert_eq!(manager.registry_count(), 0);
        assert!(manager.get(&query_id).is_none());
    }
}
