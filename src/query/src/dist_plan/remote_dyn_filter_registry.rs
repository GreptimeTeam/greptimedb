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
use std::sync::{Arc, RwLock, Weak};

use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use session::query_id::QueryId;
use store_api::storage::RegionId;

use crate::dist_plan::FilterId;

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
/// This stores the alive DataFusion filter handle together with minimal subscriber state.
#[derive(Debug)]
pub struct DynFilterEntry {
    filter_id: FilterId,
    alive_dyn_filter: Weak<DynamicFilterPhysicalExpr>,
    subscribers: RwLock<HashSet<Subscriber>>,
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
        alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> EntryRegistration {
        let mut inner = self.inner.write().unwrap();
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
        let Some(entry) = self.inner.read().unwrap().entries.get(filter_id).cloned() else {
            return SubscriberRegistration::MissingFilter;
        };

        if entry.register_subscriber(subscriber) {
            SubscriberRegistration::Added
        } else {
            SubscriberRegistration::Duplicate
        }
    }
}

/// Stream-scoped lease that keeps a query registry alive.
///
/// The manager only stores a weak index. Holding this lease is the production path for owning a
/// strong registry reference; this keeps cleanup tied to stream lifetime instead of a hand-rolled
/// active-stream counter.
#[derive(Debug)]
pub struct RemoteDynFilterRegistryLease {
    registry_manager: Arc<DynFilterRegistryManager>,
    /// Always `Some` while the lease is alive.
    ///
    /// This is wrapped in `Option` only so `Drop` can `take()` and release the strong `Arc` before
    /// checking whether the manager's weak index became dead. Without this explicit drop order,
    /// the field would remain alive until after `Drop::drop` returns.
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

        // Release this lease's strong reference before checking whether the manager's weak entry is
        // dead. If two leases drop concurrently, checking `Arc::strong_count` before the fields are
        // dropped can make both drops observe each other and both skip cleanup.
        drop(registry);

        let _ = self
            .registry_manager
            .remove_if_dropped_registry(&query_id, &registry_weak);
    }
}

/// Query-engine manager for query-scoped remote dynamic filter registries.
///
/// This is an index, not an owner: the map stores `Weak` pointers and active streams own the
/// registry through [`RemoteDynFilterRegistryLease`]. Keep production access lease-based so
/// dropping a lease releases one strong owner before the manager prunes a dead weak entry.
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

        // Compare `Weak` handles rather than raw pointers. The weak control block stays alive while
        // either this drop's local weak handle or the manager's weak entry exists. `ptr_eq` prevents
        // an old lease drop from removing a different registry installed for the same query id, and
        // `upgrade().is_none()` ensures we only prune dead entries.
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
    /// This is the only production API that returns a live registry handle. A concurrent final
    /// lease drop cannot remove the map entry between lookup and ownership transfer because the
    /// upgraded `Arc` returned by `get_or_init` is already a strong owner before the manager lock is
    /// released.
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
        // Snapshot helper for tests. Lifecycle decisions must not depend on this count; cleanup uses
        // the lease-owned `Arc` and weak-entry pruning instead.
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
    use std::sync::Barrier;
    use std::thread;

    use datafusion_physical_expr::expressions::{Column, lit};
    use uuid::Uuid;

    use super::*;
    use crate::dist_plan::{FilterFingerprint, RemoteDynFilterProducerId};

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
}
