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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex, OnceLock};

use common_query::request::{
    DynFilterPayload, INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterReg,
    InitialDynFilterRegs,
};
use common_telemetry::warn;
use dashmap::DashMap;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion_common::Result as DataFusionResult;
use session::context::QueryContextRef;
use session::query_id::QueryId;
use store_api::storage::RegionId;

pub(super) const REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES: usize = 64 * 1024;

type QueryRemoteDynFilterRegs = HashMap<RemoteDynFilterId, RegisteredDynFilter>;

#[derive(Debug, Default)]
pub(super) struct RemoteDynFilterRegistry {
    // Keep cross-query concurrency while making each query's RDF state machine a
    // single critical section. RDF count per query is small
    queries: DashMap<QueryId, Arc<Mutex<QueryRemoteDynFilterRegs>>>,
}

impl RemoteDynFilterRegistry {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn get_or_insert_query(&self, query_id: QueryId) -> Arc<Mutex<QueryRemoteDynFilterRegs>> {
        self.queries
            .entry(query_id)
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone()
    }

    fn get_query(&self, query_id: &QueryId) -> Option<Arc<Mutex<QueryRemoteDynFilterRegs>>> {
        self.queries
            .get(query_id)
            .map(|query_regs| query_regs.clone())
    }

    fn remove_query_if_empty(
        &self,
        query_id: &QueryId,
        expected: &Arc<Mutex<QueryRemoteDynFilterRegs>>,
    ) {
        // Protect against stale cleanup of an old per-query state: remove the
        // outer entry only if it still points to the same inner mutex and that
        // inner map is still empty.
        self.queries.remove_if(query_id, |_, query_regs| {
            Arc::ptr_eq(query_regs, expected) && query_regs.lock().unwrap().is_empty()
        });
    }

    #[cfg(test)]
    pub(super) fn get(
        &self,
        query_id: &QueryId,
    ) -> Option<HashMap<RemoteDynFilterId, RegisteredDynFilter>> {
        self.get_query(query_id)
            .map(|query_regs| query_regs.lock().unwrap().clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct RemoteDynFilterId(String);

impl RemoteDynFilterId {
    pub(super) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Display for RemoteDynFilterId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteDynFilterUpdateOutcome {
    MissingRegistration,
    Buffered,
    Applied,
    Idempotent,
    Stale,
    AlreadyComplete,
    PayloadTooLarge,
    DecodeFailed,
}

#[derive(Debug, Clone)]
struct PendingDynFilterUpdate {
    payload: Vec<u8>,
    generation: u64,
    is_complete: bool,
}

impl PendingDynFilterUpdate {
    fn from_initial_reg(reg: &InitialDynFilterReg) -> Option<Self> {
        let snapshot = reg.initial_snapshot.as_ref()?;
        match &snapshot.payload {
            DynFilterPayload::Datafusion(payload) => Some(Self {
                payload: payload.clone(),
                generation: snapshot.generation,
                is_complete: snapshot.is_complete,
            }),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct RemoteDynFilterEpochState {
    generation: Option<u64>,
    is_complete: bool,
}

#[derive(Debug)]
struct RemoteDynFilterState {
    filter: Arc<DynamicFilterPhysicalExpr>,
    input_schema: SchemaRef,
    epoch: Mutex<RemoteDynFilterEpochState>,
}

impl RemoteDynFilterState {
    fn new(filter: Arc<DynamicFilterPhysicalExpr>, input_schema: SchemaRef) -> Self {
        Self {
            filter,
            input_schema,
            epoch: Mutex::new(RemoteDynFilterEpochState {
                generation: None,
                is_complete: false,
            }),
        }
    }

    fn filter(&self) -> Arc<DynamicFilterPhysicalExpr> {
        self.filter.clone()
    }

    fn apply_update(
        &self,
        payload: &[u8],
        generation: u64,
        is_complete: bool,
    ) -> RemoteDynFilterUpdateOutcome {
        if !validate_update_payload_size(payload) {
            return RemoteDynFilterUpdateOutcome::PayloadTooLarge;
        }

        let mut epoch = self.epoch.lock().unwrap();
        if let Some(current_generation) = epoch.generation {
            if generation < current_generation {
                return RemoteDynFilterUpdateOutcome::Stale;
            }

            if generation == current_generation {
                if is_complete && !epoch.is_complete {
                    self.filter.mark_complete();
                    epoch.is_complete = true;
                    return RemoteDynFilterUpdateOutcome::Applied;
                }
                return RemoteDynFilterUpdateOutcome::Idempotent;
            }
        }

        if epoch.is_complete {
            return RemoteDynFilterUpdateOutcome::AlreadyComplete;
        }

        let expr = match decode_update_payload(payload, self.input_schema.as_ref()) {
            Ok(expr) => expr,
            Err(error) => {
                warn!(error; "Failed to decode remote dynamic filter update payload");
                return RemoteDynFilterUpdateOutcome::DecodeFailed;
            }
        };

        if let Err(error) = self.filter.update(expr) {
            warn!(error; "Failed to apply remote dynamic filter update");
            return RemoteDynFilterUpdateOutcome::DecodeFailed;
        }

        epoch.generation = Some(generation);
        if is_complete {
            self.filter.mark_complete();
            epoch.is_complete = true;
        }

        RemoteDynFilterUpdateOutcome::Applied
    }
}

#[derive(Debug, Clone)]
pub(super) struct RegisteredDynFilter {
    pub(super) filter_id: RemoteDynFilterId,
    pub(super) child_exprs_datafusion_proto: Vec<Vec<u8>>,
    pub(super) subscriber_regions: HashSet<RegionId>,
    runtime: Option<Arc<RemoteDynFilterState>>,
    pending_update: Option<PendingDynFilterUpdate>,
}

impl RegisteredDynFilter {
    fn new(
        filter_id: RemoteDynFilterId,
        child_exprs_datafusion_proto: Vec<Vec<u8>>,
        pending_update: Option<PendingDynFilterUpdate>,
        region_id: RegionId,
    ) -> Self {
        let mut subscriber_regions = HashSet::new();
        subscriber_regions.insert(region_id);

        Self {
            filter_id,
            child_exprs_datafusion_proto,
            subscriber_regions,
            runtime: None,
            pending_update,
        }
    }

    fn apply_initial_snapshot(
        &mut self,
        reg: &InitialDynFilterReg,
    ) -> RemoteDynFilterUpdateOutcome {
        let Some(snapshot) = PendingDynFilterUpdate::from_initial_reg(reg) else {
            return RemoteDynFilterUpdateOutcome::Idempotent;
        };

        self.apply_or_buffer_update(&snapshot.payload, snapshot.generation, snapshot.is_complete)
    }

    fn register_subscriber(&mut self, region_id: RegionId) -> bool {
        if !self.subscriber_regions.insert(region_id) {
            warn!(
                "Duplicate remote dynamic filter subscriber region, filter_id: {}, region_id: {}",
                self.filter_id, region_id
            );
            return false;
        }

        true
    }

    fn has_subscribers(&self) -> bool {
        !self.subscriber_regions.is_empty()
    }

    fn should_drop_after_remove(&mut self, region_id: RegionId) -> bool {
        self.subscriber_regions.remove(&region_id);
        !self.has_subscribers()
    }

    fn buffer_update(
        &mut self,
        payload: &[u8],
        generation: u64,
        is_complete: bool,
    ) -> RemoteDynFilterUpdateOutcome {
        if !validate_update_payload_size(payload) {
            return RemoteDynFilterUpdateOutcome::PayloadTooLarge;
        }

        if let Some(pending) = self.pending_update.as_mut() {
            if generation < pending.generation {
                return RemoteDynFilterUpdateOutcome::Stale;
            }

            if generation == pending.generation {
                pending.is_complete |= is_complete;
                return RemoteDynFilterUpdateOutcome::Idempotent;
            }

            if pending.is_complete {
                return RemoteDynFilterUpdateOutcome::AlreadyComplete;
            }
        }

        self.pending_update = Some(PendingDynFilterUpdate {
            payload: payload.to_vec(),
            generation,
            is_complete,
        });
        RemoteDynFilterUpdateOutcome::Buffered
    }

    fn apply_or_buffer_update(
        &mut self,
        payload: &[u8],
        generation: u64,
        is_complete: bool,
    ) -> RemoteDynFilterUpdateOutcome {
        if let Some(runtime) = &self.runtime {
            return runtime.apply_update(payload, generation, is_complete);
        }

        self.buffer_update(payload, generation, is_complete)
    }

    fn decode_children(
        &self,
        input_schema: &Schema,
    ) -> DataFusionResult<Vec<Arc<dyn PhysicalExpr>>> {
        InitialDynFilterReg::new(
            self.filter_id.to_string(),
            self.child_exprs_datafusion_proto.clone(),
        )
        .decode_children(
            remote_dyn_filter_task_context(),
            input_schema,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
        )
    }

    fn dyn_filter(&mut self, input_schema: &Schema) -> Option<Arc<dyn PhysicalExpr>> {
        let children = match self.decode_children(input_schema) {
            Ok(children) => children,
            Err(error) => {
                warn!(error; "Failed to decode remote dynamic filter initial children");
                return None;
            }
        };

        let runtime = match &self.runtime {
            Some(runtime) => runtime.clone(),
            None => {
                let filter = Arc::new(DynamicFilterPhysicalExpr::new(children.clone(), lit(true)));
                let runtime = Arc::new(RemoteDynFilterState::new(
                    filter,
                    Arc::new(input_schema.clone()),
                ));
                if let Some(pending) = self.pending_update.take() {
                    let outcome = runtime.apply_update(
                        &pending.payload,
                        pending.generation,
                        pending.is_complete,
                    );
                    if matches!(outcome, RemoteDynFilterUpdateOutcome::DecodeFailed) {
                        warn!(
                            "Dropped buffered remote dynamic filter update after decode failure, filter_id: {}, generation: {}",
                            self.filter_id, pending.generation
                        );
                    }
                }
                self.runtime = Some(runtime.clone());
                runtime
            }
        };

        match runtime.filter().with_new_children(children) {
            Ok(expr) => Some(expr),
            Err(error) => {
                warn!(error; "Failed to remap remote dynamic filter children for scan");
                None
            }
        }
    }

    fn deactivate(&self) {
        if let Some(runtime) = &self.runtime {
            runtime.filter.mark_complete();
        }
    }
}

pub(super) fn initial_dyn_filter_regs_from_query_ctx(
    query_ctx: &QueryContextRef,
) -> Option<InitialDynFilterRegs> {
    let registrations =
        query_ctx.extension(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)?;
    match InitialDynFilterRegs::from_extension_value(registrations) {
        Ok(registrations) => match registrations.validate_default_bounds() {
            Ok(()) => Some(registrations),
            Err(error) => {
                warn!(error; "Initial remote dyn filter registrations exceeded Task 03 bounds");
                None
            }
        },
        Err(error) => {
            warn!(error; "Failed to decode initial remote dyn filter registrations from query context");
            None
        }
    }
}

pub(super) fn register_initial_dyn_filter_regs(
    regs_by_query: &RemoteDynFilterRegistry,
    query_id: &QueryId,
    region_id: RegionId,
    regs: &InitialDynFilterRegs,
) -> Vec<RemoteDynFilterId> {
    if regs.is_empty() {
        return Vec::new();
    }

    if let Err(error) = regs.validate_default_bounds() {
        warn!(error; "Ignored invalid initial dyn filter registrations for query_id {}", query_id);
        return Vec::new();
    }

    let query_regs = regs_by_query.get_or_insert_query(*query_id);
    let mut query_regs = query_regs.lock().unwrap();
    let mut registered_filter_ids = Vec::with_capacity(regs.regs.len());

    for reg in &regs.regs {
        let filter_id = RemoteDynFilterId::new(reg.filter_id.clone());
        match query_regs.entry(filter_id.clone()) {
            Entry::Occupied(mut entry) => {
                let registered = entry.get_mut();
                if registered.child_exprs_datafusion_proto != reg.child_exprs_datafusion_proto {
                    warn!(
                        "Remote dynamic filter registration reused filter_id with different children, query_id: {}, filter_id: {}, region_id: {}",
                        query_id, filter_id, region_id
                    );
                }
                if registered.register_subscriber(region_id) {
                    registered_filter_ids.push(filter_id);
                }
                let _ = registered.apply_initial_snapshot(reg);
            }
            Entry::Vacant(entry) => {
                entry.insert(RegisteredDynFilter::new(
                    filter_id.clone(),
                    reg.child_exprs_datafusion_proto.clone(),
                    PendingDynFilterUpdate::from_initial_reg(reg),
                    region_id,
                ));
                registered_filter_ids.push(filter_id);
            }
        }
    }

    registered_filter_ids
}

pub(super) fn remote_dyn_filter_exprs_for_initial_regs(
    regs_by_query: &RemoteDynFilterRegistry,
    query_id: &QueryId,
    initial_regs: &InitialDynFilterRegs,
    input_schema: &Schema,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let Some(query_regs) = regs_by_query.get_query(query_id) else {
        return Vec::new();
    };

    let mut query_regs = query_regs.lock().unwrap();
    initial_regs
        .regs
        .iter()
        .filter_map(|reg| {
            let filter_id = RemoteDynFilterId::new(reg.filter_id.clone());
            let registered = query_regs.get_mut(&filter_id)?;
            registered.dyn_filter(input_schema)
        })
        .collect()
}

pub(super) fn apply_remote_dyn_filter_update(
    regs_by_query: &RemoteDynFilterRegistry,
    query_id: &QueryId,
    filter_id: &RemoteDynFilterId,
    payload: &[u8],
    generation: u64,
    is_complete: bool,
) -> RemoteDynFilterUpdateOutcome {
    if !validate_update_payload_size(payload) {
        warn!(
            "Ignored oversized remote dynamic filter update, query_id: {}, filter_id: {}, payload_size: {}, max_payload_size: {}",
            query_id,
            filter_id,
            payload.len(),
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES
        );
        return RemoteDynFilterUpdateOutcome::PayloadTooLarge;
    }

    let Some(query_regs) = regs_by_query.get_query(query_id) else {
        warn!(
            "Ignored remote dynamic filter update without query registration, query_id: {}, filter_id: {}",
            query_id, filter_id
        );
        return RemoteDynFilterUpdateOutcome::MissingRegistration;
    };

    let mut query_regs = query_regs.lock().unwrap();
    let Some(registered) = query_regs.get_mut(filter_id) else {
        warn!(
            "Ignored remote dynamic filter update without filter registration, query_id: {}, filter_id: {}",
            query_id, filter_id
        );
        return RemoteDynFilterUpdateOutcome::MissingRegistration;
    };

    registered.apply_or_buffer_update(payload, generation, is_complete)
}

pub(super) fn unregister_remote_dyn_filter(
    regs_by_query: &RemoteDynFilterRegistry,
    query_id: &QueryId,
    filter_id: &RemoteDynFilterId,
) -> RemoteDynFilterUpdateOutcome {
    let Some(query_regs) = regs_by_query.get_query(query_id) else {
        warn!(
            "Ignored remote dynamic filter unregister without query registration, query_id: {}, filter_id: {}",
            query_id, filter_id
        );
        return RemoteDynFilterUpdateOutcome::MissingRegistration;
    };

    let (registered, should_remove_query) = {
        let mut locked = query_regs.lock().unwrap();
        let Some(registered) = locked.remove(filter_id) else {
            warn!(
                "Ignored remote dynamic filter unregister without filter registration, query_id: {}, filter_id: {}",
                query_id, filter_id
            );
            return RemoteDynFilterUpdateOutcome::MissingRegistration;
        };
        let should_remove_query = locked.is_empty();
        (registered, should_remove_query)
    };

    registered.deactivate();
    if should_remove_query {
        regs_by_query.remove_query_if_empty(query_id, &query_regs);
    }

    RemoteDynFilterUpdateOutcome::Applied
}

pub(super) fn remove_initial_dyn_filter_regs(
    regs_by_query: &RemoteDynFilterRegistry,
    query_id: &QueryId,
    region_id: RegionId,
    filter_ids: &[RemoteDynFilterId],
) {
    if filter_ids.is_empty() {
        return;
    }

    let Some(query_regs) = regs_by_query.get_query(query_id) else {
        return;
    };

    let (removed_filters, should_remove_query) = {
        let mut locked = query_regs.lock().unwrap();
        let mut removed_filters = Vec::new();

        for filter_id in filter_ids {
            let should_remove_filter = locked
                .get_mut(filter_id)
                .map(|registered| registered.should_drop_after_remove(region_id))
                .unwrap_or(false);

            if should_remove_filter && let Some(registered) = locked.remove(filter_id) {
                removed_filters.push(registered);
            }
        }

        let should_remove_query = locked.is_empty();
        (removed_filters, should_remove_query)
    };

    for registered in removed_filters {
        registered.deactivate();
    }

    if should_remove_query {
        regs_by_query.remove_query_if_empty(query_id, &query_regs);
    }
}

fn decode_update_payload(
    payload: &[u8],
    input_schema: &Schema,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    DynFilterPayload::Datafusion(payload.to_vec()).decode_datafusion_expr(
        remote_dyn_filter_task_context(),
        input_schema,
        REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
    )
}

fn remote_dyn_filter_task_context() -> &'static TaskContext {
    static TASK_CONTEXT: OnceLock<TaskContext> = OnceLock::new();

    TASK_CONTEXT.get_or_init(|| {
        // RDF payloads can contain DataFusion built-in scalar functions. For
        // example, multi-column join dynamic filters use `struct(...) IN (...)`.
        // `TaskContext::default()` has an empty function registry and cannot
        // decode those expressions.
        let session_state = SessionStateBuilder::new().with_default_features().build();
        TaskContext::from(&session_state)
    })
}

fn validate_update_payload_size(payload: &[u8]) -> bool {
    payload.len() <= REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_dyn_filter_same_query_reuses_one_inner_lock() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = QueryId::new();

        let first = regs_by_query.get_or_insert_query(query_id);
        let second = regs_by_query.get_or_insert_query(query_id);

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn remote_dyn_filter_stale_query_remove_does_not_remove_new_query_state() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = QueryId::new();
        let stale_query_regs = regs_by_query.get_or_insert_query(query_id);

        regs_by_query.remove_query_if_empty(&query_id, &stale_query_regs);
        assert!(regs_by_query.get_query(&query_id).is_none());

        let new_query_regs = regs_by_query.get_or_insert_query(query_id);
        let filter_id = RemoteDynFilterId::new("filter-1");
        let region_id = RegionId::new(1024, 7);
        new_query_regs.lock().unwrap().insert(
            filter_id.clone(),
            RegisteredDynFilter::new(filter_id.clone(), vec![], None, region_id),
        );

        regs_by_query.remove_query_if_empty(&query_id, &stale_query_regs);

        let current_query_regs = regs_by_query.get_query(&query_id).unwrap();
        assert!(Arc::ptr_eq(&current_query_regs, &new_query_regs));
        assert_eq!(current_query_regs.lock().unwrap().len(), 1);
    }

    #[test]
    fn remote_dyn_filter_register_uses_entry_to_merge_same_filter_subscribers() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = QueryId::new();
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 2);
        assert!(registered.subscriber_regions.contains(&first_region_id));
        assert!(registered.subscriber_regions.contains(&second_region_id));
    }
}
