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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use session::query_id::QueryId;
use store_api::storage::RegionId;

use crate::dist_plan::FilterId;

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
    subscribers: RwLock<Vec<Subscriber>>,
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
            subscribers: RwLock::new(Vec::new()),
        }
    }

    pub fn filter_id(&self) -> &FilterId {
        &self.filter_id
    }

    pub fn upgrade_alive_dyn_filter(&self) -> Option<Arc<DynamicFilterPhysicalExpr>> {
        self.alive_dyn_filter.upgrade()
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
}

/// Query-scoped registry that owns all remote dynamic filters for one query.
#[derive(Debug)]
pub struct QueryDynFilterRegistry {
    query_id: QueryId,
    active_streams: AtomicUsize,
    inner: RwLock<QueryDynFilterRegistryInner>,
}

impl QueryDynFilterRegistry {
    pub fn new(query_id: QueryId) -> Self {
        Self {
            query_id,
            active_streams: AtomicUsize::new(0),
            inner: RwLock::new(QueryDynFilterRegistryInner {
                entries: HashMap::new(),
            }),
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    fn acquire_stream(&self) {
        self.active_streams.fetch_add(1, Ordering::SeqCst);
    }

    fn release_stream(&self) {
        self.active_streams.fetch_sub(1, Ordering::SeqCst);
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

#[derive(Debug)]
pub struct RemoteDynFilterRegistryLease {
    registry_manager: Arc<DynFilterRegistryManager>,
    registry: Arc<QueryDynFilterRegistry>,
}

impl RemoteDynFilterRegistryLease {
    fn new(
        registry_manager: Arc<DynFilterRegistryManager>,
        registry: Arc<QueryDynFilterRegistry>,
    ) -> Self {
        registry.acquire_stream();
        Self {
            registry_manager,
            registry,
        }
    }
}

impl Drop for RemoteDynFilterRegistryLease {
    fn drop(&mut self) {
        self.registry.release_stream();
        let _ = self
            .registry_manager
            .remove_if_inactive(&self.registry.query_id(), &self.registry);
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

    #[cfg(test)]
    fn remove(&self, query_id: &QueryId) -> Option<Arc<QueryDynFilterRegistry>> {
        self.registries.write().unwrap().remove(query_id)
    }

    fn remove_if_inactive(
        &self,
        query_id: &QueryId,
        registry: &Arc<QueryDynFilterRegistry>,
    ) -> Option<Arc<QueryDynFilterRegistry>> {
        let mut registries = self.registries.write().unwrap();
        let current = registries.get(query_id)?;

        if Arc::ptr_eq(current, registry) && registry.active_stream_count() == 0 {
            registries.remove(query_id)
        } else {
            None
        }
    }

    pub fn acquire_lease(self: &Arc<Self>, query_id: QueryId) -> RemoteDynFilterRegistryLease {
        let registry = self.get_or_init(query_id);
        RemoteDynFilterRegistryLease::new(self.clone(), registry)
    }

    pub fn get_or_init(&self, query_id: QueryId) -> Arc<QueryDynFilterRegistry> {
        let mut registries = self.registries.write().unwrap();

        registries
            .entry(query_id)
            .or_insert_with(|| Arc::new(QueryDynFilterRegistry::new(query_id)))
            .clone()
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
    use crate::dist_plan::{FilterFingerprint, ProducerScopeId};

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    fn test_filter_id(region_id: RegionId, producer_ordinal: u32) -> FilterId {
        FilterId::new(
            region_id,
            ProducerScopeId::new(42),
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
        let manager = DynFilterRegistryManager::default();
        let query_id = test_query_id(1);
        let first = manager.get_or_init(query_id);
        let second = manager.get_or_init(query_id);

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(manager.registry_count(), 1);
    }

    #[test]
    fn registry_manager_removes_registry_for_query() {
        let manager = DynFilterRegistryManager::default();
        let query_id = test_query_id(1);

        let registry = manager.get_or_init(query_id);

        assert!(Arc::ptr_eq(&manager.remove(&query_id).unwrap(), &registry));
        assert!(manager.get(&query_id).is_none());
        assert_eq!(manager.registry_count(), 0);
    }

    #[test]
    fn registry_manager_lease_waits_for_last_query_scoped_stream() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let first = manager.acquire_lease(query_id);
        let second = manager.acquire_lease(query_id);

        assert_eq!(manager.registry_count(), 1);
        drop(first);
        assert_eq!(manager.registry_count(), 1);

        drop(second);
        assert_eq!(manager.registry_count(), 0);
    }

    #[test]
    fn registry_manager_lease_does_not_remove_reacquired_registry() {
        let manager = Arc::new(DynFilterRegistryManager::default());
        let query_id = test_query_id(1);

        let first = manager.acquire_lease(query_id);
        drop(first);
        let second = manager.acquire_lease(query_id);

        assert_eq!(manager.registry_count(), 1);
        drop(second);
        assert_eq!(manager.registry_count(), 0);
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
