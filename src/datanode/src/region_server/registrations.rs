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
    DecodedDynFilterExprs, DynFilterPayload, INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
    InitialDynFilterReg, InitialDynFilterRegs,
};
use common_telemetry::warn;
use dashmap::DashMap;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::ColumnarValue;
use session::context::QueryContextRef;
use session::query_id::QueryId;
use store_api::storage::RegionId;

pub(super) const REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES: usize = 64 * 1024;

/// A [`PhysicalExpr`] wrapper that prevents Mito scan from downcasting the
/// inner expression as a [`DynamicFilterPhysicalExpr`].
///
/// Mito scan pushes filters into its predicate by downcasting each
/// `PhysicalExpr` to `DynamicFilterPhysicalExpr`.  This wrapper ensures
/// the inner exact filter (e.g. a Bloom probe) stays in the [`FilterExec`]
/// for row-level evaluation only, while the separately-constructed pushdown
/// `DynamicFilterPhysicalExpr` is pushed into scan for pruning.
///
/// [`PhysicalExpr`]: datafusion::physical_plan::PhysicalExpr
/// [`DynamicFilterPhysicalExpr`]: datafusion::physical_plan::expressions::DynamicFilterPhysicalExpr
#[derive(Debug, Clone)]
struct NonPushdownDynFilterExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl NonPushdownDynFilterExpr {
    fn new(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { inner }
    }
}

impl Display for NonPushdownDynFilterExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "non_pushdown({})", self.inner)
    }
}

impl std::hash::Hash for NonPushdownDynFilterExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.dyn_hash(state);
    }
}

impl PartialEq for NonPushdownDynFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.dyn_eq(other.inner.as_any())
    }
}

impl Eq for NonPushdownDynFilterExpr {}

impl PhysicalExpr for NonPushdownDynFilterExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let new_inner = self.inner.clone().with_new_children(children)?;
        Ok(Arc::new(Self { inner: new_inner }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "non_pushdown({})", self.inner)
    }
}

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
    pub(super) fn inspect_query<R>(
        &self,
        query_id: &QueryId,
        inspect: impl FnOnce(&QueryRemoteDynFilterRegs) -> R,
    ) -> Option<R> {
        self.get_query(query_id)
            .map(|query_regs| inspect(&query_regs.lock().unwrap()))
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
    payload: DynFilterPayload,
    generation: u64,
    is_complete: bool,
}

impl PendingDynFilterUpdate {
    fn from_initial_reg(reg: &InitialDynFilterReg) -> Option<Self> {
        let snapshot = reg.initial_snapshot.as_ref()?;
        // Accept any payload variant for pending; FE may send non-Datafusion in the future.
        Some(Self {
            payload: snapshot.payload.clone(),
            generation: snapshot.generation,
            is_complete: snapshot.is_complete,
        })
    }
}

#[derive(Debug)]
struct RemoteDynFilterEpochState {
    generation: Option<u64>,
    is_complete: bool,
}

#[derive(Debug)]
struct RemoteDynFilterState {
    pushdown_filter: Arc<DynamicFilterPhysicalExpr>,
    exact_filter: Arc<DynamicFilterPhysicalExpr>,
    input_schema: SchemaRef,
    registered_children: Vec<Arc<dyn PhysicalExpr>>,
    epoch: Mutex<RemoteDynFilterEpochState>,
}

impl RemoteDynFilterState {
    fn new(
        pushdown_filter: Arc<DynamicFilterPhysicalExpr>,
        exact_filter: Arc<DynamicFilterPhysicalExpr>,
        input_schema: SchemaRef,
        registered_children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            pushdown_filter,
            exact_filter,
            input_schema,
            registered_children,
            epoch: Mutex::new(RemoteDynFilterEpochState {
                generation: None,
                is_complete: false,
            }),
        }
    }

    fn pushdown_filter(&self) -> Arc<DynamicFilterPhysicalExpr> {
        self.pushdown_filter.clone()
    }

    fn exact_filter(&self) -> Arc<DynamicFilterPhysicalExpr> {
        self.exact_filter.clone()
    }

    fn decode_update_payload(
        &self,
        payload: &DynFilterPayload,
    ) -> std::result::Result<DecodedDynFilterExprs, RemoteDynFilterUpdateOutcome> {
        if !validate_update_payload_size(payload) {
            return Err(RemoteDynFilterUpdateOutcome::PayloadTooLarge);
        }

        payload
            .decode_placement_aware(
                remote_dyn_filter_task_context(),
                self.input_schema.as_ref(),
                &self.registered_children,
                REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            )
            .map_err(|error| {
                warn!(error; "Failed to decode remote dynamic filter update payload");
                RemoteDynFilterUpdateOutcome::DecodeFailed
            })
    }

    fn apply_update(
        &self,
        payload: &DynFilterPayload,
        generation: u64,
        is_complete: bool,
    ) -> RemoteDynFilterUpdateOutcome {
        let mut epoch = self.epoch.lock().unwrap();
        if let Some(current_generation) = epoch.generation {
            if generation < current_generation {
                return RemoteDynFilterUpdateOutcome::Stale;
            }

            if generation == current_generation {
                if is_complete && !epoch.is_complete {
                    if let Err(outcome) = self.decode_update_payload(payload) {
                        return outcome;
                    }
                    self.pushdown_filter.mark_complete();
                    self.exact_filter.mark_complete();
                    epoch.is_complete = true;
                    return RemoteDynFilterUpdateOutcome::Applied;
                }
                return RemoteDynFilterUpdateOutcome::Idempotent;
            }
        }

        if epoch.is_complete {
            return RemoteDynFilterUpdateOutcome::AlreadyComplete;
        }

        // Decode placement-aware parts before mutating state
        let decoded = match self.decode_update_payload(payload) {
            Ok(decoded) => decoded,
            Err(outcome) => return outcome,
        };

        // Update pushdown wrapper first, then exact wrapper second.
        // Generation is only advanced after both succeed.
        if let Err(error) = self.pushdown_filter.update(decoded.pushdown_expr) {
            warn!(error; "Failed to apply pushdown remote dynamic filter update");
            return RemoteDynFilterUpdateOutcome::DecodeFailed;
        }

        let exact_expr = decoded.exact_expr.unwrap_or_else(|| lit(true));
        if let Err(error) = self.exact_filter.update(exact_expr) {
            warn!(error; "Failed to apply exact remote dynamic filter update");
            return RemoteDynFilterUpdateOutcome::DecodeFailed;
        }

        epoch.generation = Some(generation);
        if is_complete {
            self.pushdown_filter.mark_complete();
            self.exact_filter.mark_complete();
            epoch.is_complete = true;
        }

        RemoteDynFilterUpdateOutcome::Applied
    }
}

#[derive(Debug)]
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
        payload: &DynFilterPayload,
        generation: u64,
        is_complete: bool,
    ) -> RemoteDynFilterUpdateOutcome {
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

        if !validate_update_payload_size(payload) {
            return RemoteDynFilterUpdateOutcome::PayloadTooLarge;
        }

        self.pending_update = Some(PendingDynFilterUpdate {
            payload: payload.clone(),
            generation,
            is_complete,
        });
        RemoteDynFilterUpdateOutcome::Buffered
    }

    fn apply_or_buffer_update(
        &mut self,
        payload: &DynFilterPayload,
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

    fn dyn_filters(&mut self, input_schema: &Schema) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
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
                let pushdown_filter =
                    Arc::new(DynamicFilterPhysicalExpr::new(children.clone(), lit(true)));
                let exact_filter =
                    Arc::new(DynamicFilterPhysicalExpr::new(children.clone(), lit(true)));
                let runtime = Arc::new(RemoteDynFilterState::new(
                    pushdown_filter,
                    exact_filter,
                    Arc::new(input_schema.clone()),
                    children.clone(),
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

        let pushdown_expr = match runtime
            .pushdown_filter()
            .with_new_children(children.clone())
        {
            Ok(expr) => expr,
            Err(error) => {
                warn!(error; "Failed to remap pushdown dyn filter children for scan");
                return None;
            }
        };

        let exact_inner = match runtime.exact_filter().with_new_children(children) {
            Ok(expr) => expr,
            Err(error) => {
                warn!(error; "Failed to remap exact dyn filter children for scan");
                return None;
            }
        };

        // Wrap the exact filter in a non-pushdown marker so Mito scan cannot
        // downcast it as DynamicFilterPhysicalExpr and push it into scan.
        let exact_non_pushdown: Arc<dyn PhysicalExpr> =
            Arc::new(NonPushdownDynFilterExpr::new(exact_inner));

        Some(vec![pushdown_expr, exact_non_pushdown])
    }

    fn deactivate(&self) {
        if let Some(runtime) = &self.runtime {
            runtime.pushdown_filter.mark_complete();
            runtime.exact_filter.mark_complete();
        }
    }
}

impl Drop for RegisteredDynFilter {
    fn drop(&mut self) {
        self.deactivate();
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
            registered.dyn_filters(input_schema)
        })
        .flatten()
        .collect()
}

pub(super) fn apply_remote_dyn_filter_update(
    regs_by_query: &RemoteDynFilterRegistry,
    query_id: &QueryId,
    filter_id: &RemoteDynFilterId,
    payload: &DynFilterPayload,
    generation: u64,
    is_complete: bool,
) -> RemoteDynFilterUpdateOutcome {
    if !validate_update_payload_size(payload) {
        warn!(
            "Ignored oversized remote dynamic filter update, query_id: {}, filter_id: {}, payload_size: {}, max_payload_size: {}",
            query_id,
            filter_id,
            payload.encoded_payload_bytes(),
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

    drop(registered);
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

    drop(removed_filters);
    if should_remove_query {
        regs_by_query.remove_query_if_empty(query_id, &query_regs);
    }
}

fn validate_update_payload_size(payload: &DynFilterPayload) -> bool {
    payload.encoded_payload_bytes() <= REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::request::InitialDynFilterSnapshot;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_expr::expressions::{Column, lit};
    use datafusion::physical_plan::joins::join_hash_map::{JoinHashMapType, JoinHashMapU32};
    use datafusion::physical_plan::joins::{HashTableLookupExpr, Map, SeededRandomState};

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

        let query_regs = regs_by_query.get_query(&query_id).unwrap();
        let query_regs = query_regs.lock().unwrap();
        assert_eq!(query_regs.len(), 1);
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 2);
        assert!(registered.subscriber_regions.contains(&first_region_id));
        assert!(registered.subscriber_regions.contains(&second_region_id));
    }

    // ── NonPushdownDynFilterExpr tests ──────────────────────────────

    fn empty_arrow_schema() -> ::datafusion::arrow::datatypes::Schema {
        ::datafusion::arrow::datatypes::Schema::empty()
    }

    #[test]
    fn non_pushdown_wrapper_hides_dynamic_filter_and_delegates() {
        let inner =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true))) as Arc<dyn PhysicalExpr>;
        let wrapper: Arc<dyn PhysicalExpr> = Arc::new(NonPushdownDynFilterExpr::new(inner));

        let any = Arc::clone(&wrapper) as Arc<dyn std::any::Any + Send + Sync>;
        assert!(any.downcast_ref::<DynamicFilterPhysicalExpr>().is_none());
        assert!(any.downcast_ref::<NonPushdownDynFilterExpr>().is_some());

        let schema = empty_arrow_schema();
        assert_eq!(wrapper.data_type(&schema).unwrap(), DataType::Boolean);
        assert!(!wrapper.nullable(&schema).unwrap());
        assert_eq!(wrapper.children().len(), 0);

        let display = format!("{}", wrapper);
        assert!(
            display.starts_with("non_pushdown("),
            "expected non_pushdown prefix, got: {display}"
        );
    }

    // ── dyn_filters returns two exprs for Datafusion payload ───────

    fn test_remote_query_id() -> QueryId {
        QueryId::new()
    }

    #[test]
    fn dyn_filters_returns_two_exprs_for_datafusion_initial_snapshot() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);

        let payload = DynFilterPayload::from_datafusion_expr(&lit(true), 1024).unwrap();
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-df", vec![])
                .with_initial_snapshot(InitialDynFilterSnapshot::new(payload, 1, false)),
        ]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let exprs = remote_dyn_filter_exprs_for_initial_regs(
            &regs_by_query,
            &query_id,
            &regs,
            &empty_arrow_schema(),
        );

        // Two exprs: pushdown (DynamicFilterPhysicalExpr) + exact non-pushdown
        assert_eq!(exprs.len(), 2);

        let _ = pushdown_dyn_filter(&exprs[0]);
        let _ = exact_dyn_filter(&exprs[1]);
    }

    // ── Bloom placement-aware decode test ───────────────────────────

    fn one_column_arrow_schema() -> ::datafusion::arrow::datatypes::Schema {
        ::datafusion::arrow::datatypes::Schema::new(vec![Field::new("id", DataType::Int32, false)])
    }

    fn test_lookup_expr(child: Arc<dyn PhysicalExpr>, hashes: Vec<u64>) -> Arc<dyn PhysicalExpr> {
        let mut hash_map = JoinHashMapU32::with_capacity(hashes.len().max(1));
        hash_map.update_from_iter(Box::new(hashes.iter().enumerate()), 0);
        let map = Arc::new(Map::HashMap(Box::new(hash_map)));
        Arc::new(HashTableLookupExpr::new(
            vec![child],
            SeededRandomState::with_seeds(1, 2, 3, 4),
            map,
            "lookup".to_string(),
        ))
    }

    fn pushdown_dyn_filter(expr: &Arc<dyn PhysicalExpr>) -> Arc<DynamicFilterPhysicalExpr> {
        let any = Arc::clone(expr) as Arc<dyn std::any::Any + Send + Sync>;
        any.downcast::<DynamicFilterPhysicalExpr>()
            .expect("pushdown must be DynamicFilterPhysicalExpr")
    }

    fn exact_dyn_filter(expr: &Arc<dyn PhysicalExpr>) -> Arc<DynamicFilterPhysicalExpr> {
        let any = Arc::clone(expr) as Arc<dyn std::any::Any + Send + Sync>;
        assert!(any.downcast_ref::<DynamicFilterPhysicalExpr>().is_none());
        let wrapper = any
            .downcast_ref::<NonPushdownDynFilterExpr>()
            .expect("exact must be NonPushdownDynFilterExpr");
        let inner = Arc::clone(&wrapper.inner) as Arc<dyn std::any::Any + Send + Sync>;
        inner
            .downcast::<DynamicFilterPhysicalExpr>()
            .expect("exact wrapper must contain DynamicFilterPhysicalExpr")
    }

    #[test]
    fn bloom_dyn_filters_pushdown_is_dynamic_filter_and_exact_contains_bloom_probe() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let schema = one_column_arrow_schema();
        let id_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let children = vec![Arc::clone(&id_col)];
        let lookup = test_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);

        let payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &lookup,
            &children,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            &schema,
        )
        .unwrap();
        assert!(matches!(payload, DynFilterPayload::JoinHashBloom(_)));

        let reg = InitialDynFilterReg::from_filter_id_and_children("filter-bloom", &children)
            .unwrap()
            .with_initial_snapshot(InitialDynFilterSnapshot::new(payload, 1, false));
        let regs = InitialDynFilterRegs::new(vec![reg]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let exprs =
            remote_dyn_filter_exprs_for_initial_regs(&regs_by_query, &query_id, &regs, &schema);

        assert_eq!(exprs.len(), 2);

        // Pushdown wrapper: DynamicFilterPhysicalExpr with residual (lit(true) for lookup-only)
        let pushdown = pushdown_dyn_filter(&exprs[0]);
        let pushdown_current = format!("{}", pushdown.current().unwrap());
        assert!(
            pushdown_current.contains("true"),
            "pushdown should be residual (lit(true) for lookup-only), got: {pushdown_current}"
        );

        // Exact wrapper: NonPushdownDynFilterExpr containing bloom_probe
        let _ = exact_dyn_filter(&exprs[1]);
        let exact_display = format!("{}", exprs[1]);
        assert!(
            exact_display.contains("bloom_probe"),
            "exact wrapper display must contain bloom_probe, got: {exact_display}"
        );
        assert!(
            exact_display.contains("non_pushdown("),
            "exact wrapper display must have non_pushdown prefix, got: {exact_display}"
        );
    }

    #[test]
    fn datafusion_update_after_bloom_clears_exact_to_true() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let schema = one_column_arrow_schema();
        let id_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let children = vec![Arc::clone(&id_col)];
        let lookup = test_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);

        let bloom_payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &lookup,
            &children,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            &schema,
        )
        .unwrap();
        assert!(matches!(bloom_payload, DynFilterPayload::JoinHashBloom(_)));

        let reg = InitialDynFilterReg::from_filter_id_and_children("filter-x", &children)
            .unwrap()
            .with_initial_snapshot(InitialDynFilterSnapshot::new(bloom_payload, 1, false));
        let regs = InitialDynFilterRegs::new(vec![reg]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        // Install wrappers
        let exprs =
            remote_dyn_filter_exprs_for_initial_regs(&regs_by_query, &query_id, &regs, &schema);
        assert_eq!(exprs.len(), 2);

        let pushdown = pushdown_dyn_filter(&exprs[0]);
        let exact_dynamic = exact_dyn_filter(&exprs[1]);

        // Assert bloom is installed
        let exact_display = format!("{}", exprs[1]);
        assert!(
            exact_display.contains("bloom_probe"),
            "pre-update: exact should contain bloom_probe"
        );

        // Now send a Datafusion payload update (higher generation)
        let df_payload = DynFilterPayload::from_datafusion_expr(&lit(true), 1024).unwrap();
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-x"),
            &df_payload,
            2, // higher generation
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);

        // Pushdown should now be the Datafusion expression
        let pushdown_current = format!("{}", pushdown.current().unwrap());
        assert!(
            pushdown_current.contains("true"),
            "post-DF-update: pushdown should be true, got: {pushdown_current}"
        );

        // Re-fetch exprs to check exact was cleared to true
        let exprs_after =
            remote_dyn_filter_exprs_for_initial_regs(&regs_by_query, &query_id, &regs, &schema);
        assert_eq!(exprs_after.len(), 2);
        let exact_after_display = format!("{}", exprs_after[1]);
        assert!(
            !exact_after_display.contains("bloom_probe"),
            "post-DF-update: exact must NOT contain bloom_probe, got: {exact_after_display}"
        );
        assert_eq!(format!("{}", exact_dynamic.current().unwrap()), "true");
    }

    #[test]
    fn oversized_update_after_bloom_does_not_mutate_state() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let schema = one_column_arrow_schema();
        let id_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let children = vec![Arc::clone(&id_col)];
        let lookup = test_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);

        let bloom_payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &lookup,
            &children,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            &schema,
        )
        .unwrap();
        let reg = InitialDynFilterReg::from_filter_id_and_children("filter-oversized", &children)
            .unwrap()
            .with_initial_snapshot(InitialDynFilterSnapshot::new(
                bloom_payload.clone(),
                1,
                false,
            ));
        let regs = InitialDynFilterRegs::new(vec![reg]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let exprs =
            remote_dyn_filter_exprs_for_initial_regs(&regs_by_query, &query_id, &regs, &schema);
        assert_eq!(exprs.len(), 2);

        let exact_dynamic = exact_dyn_filter(&exprs[1]);
        let pre_oversized = format!("{}", exact_dynamic.current().unwrap());
        assert!(
            pre_oversized.contains("bloom_probe"),
            "pre-oversized: exact should contain bloom_probe, got: {pre_oversized}"
        );

        // Send oversized update — must NOT mutate state
        let oversized =
            DynFilterPayload::Datafusion(vec![0; REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES + 1]);
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-oversized"),
            &oversized,
            2,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::PayloadTooLarge);

        // Exact should still contain bloom_probe (not cleared)
        let post_oversized = format!("{}", exact_dynamic.current().unwrap());
        assert!(
            post_oversized.contains("bloom_probe"),
            "post-oversized: exact must still contain bloom_probe, got: {post_oversized}"
        );

        // The oversized generation must not become the runtime watermark.
        let valid_gen1_outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-oversized"),
            &bloom_payload,
            1, // same as installed
            false,
        );
        assert_eq!(valid_gen1_outcome, RemoteDynFilterUpdateOutcome::Idempotent);

        let stale_outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-oversized"),
            &bloom_payload,
            0,
            false,
        );
        assert_eq!(stale_outcome, RemoteDynFilterUpdateOutcome::Stale);
    }

    #[test]
    fn decode_failed_update_does_not_mutate_state() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let schema = one_column_arrow_schema();
        let id_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let children = vec![Arc::clone(&id_col)];
        let lookup = test_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);

        let bloom_payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &lookup,
            &children,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            &schema,
        )
        .unwrap();
        let reg = InitialDynFilterReg::from_filter_id_and_children("filter-decode-fail", &children)
            .unwrap()
            .with_initial_snapshot(InitialDynFilterSnapshot::new(
                bloom_payload.clone(),
                1,
                false,
            ));
        let regs = InitialDynFilterRegs::new(vec![reg]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let exprs =
            remote_dyn_filter_exprs_for_initial_regs(&regs_by_query, &query_id, &regs, &schema);
        assert_eq!(exprs.len(), 2);

        let exact_dynamic = exact_dyn_filter(&exprs[1]);
        assert!(format!("{}", exact_dynamic.current().unwrap()).contains("bloom_probe"));

        // Send an undecodable Datafusion payload (garbage bytes)
        let garbage = DynFilterPayload::Datafusion(vec![0, 1, 2, 3]);
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-decode-fail"),
            &garbage,
            2,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::DecodeFailed);

        // Exact should NOT be cleared — state is unchanged
        assert!(format!("{}", exact_dynamic.current().unwrap()).contains("bloom_probe"));
    }

    #[test]
    fn same_generation_invalid_complete_update_does_not_mark_complete() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let schema = one_column_arrow_schema();
        let id_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let children = vec![Arc::clone(&id_col)];
        let lookup = test_lookup_expr(Arc::clone(&id_col), vec![10, 20, 30]);

        let bloom_payload = DynFilterPayload::from_datafusion_expr_with_registered_children(
            &lookup,
            &children,
            REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
            &schema,
        )
        .unwrap();
        let reg = InitialDynFilterReg::from_filter_id_and_children("filter-complete", &children)
            .unwrap()
            .with_initial_snapshot(InitialDynFilterSnapshot::new(
                bloom_payload.clone(),
                1,
                false,
            ));
        let regs = InitialDynFilterRegs::new(vec![reg]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let exprs =
            remote_dyn_filter_exprs_for_initial_regs(&regs_by_query, &query_id, &regs, &schema);
        assert_eq!(exprs.len(), 2);

        let pushdown = pushdown_dyn_filter(&exprs[0]);
        let exact_dynamic = exact_dyn_filter(&exprs[1]);
        assert!(!pushdown.is_complete());
        assert!(!exact_dynamic.is_complete());
        assert!(format!("{}", exact_dynamic.current().unwrap()).contains("bloom_probe"));

        let garbage = DynFilterPayload::Datafusion(vec![0, 1, 2, 3]);
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-complete"),
            &garbage,
            1,
            true,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::DecodeFailed);

        assert!(!pushdown.is_complete());
        assert!(!exact_dynamic.is_complete());
        assert!(format!("{}", exact_dynamic.current().unwrap()).contains("bloom_probe"));

        let valid_outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-complete"),
            &bloom_payload,
            2,
            false,
        );
        assert_eq!(valid_outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(!pushdown.is_complete());
        assert!(!exact_dynamic.is_complete());
    }

    #[test]
    fn deactivate_marks_both_pushdown_and_exact_filters_complete() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-d", vec![])]);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let exprs = remote_dyn_filter_exprs_for_initial_regs(
            &regs_by_query,
            &query_id,
            &regs,
            &empty_arrow_schema(),
        );
        assert_eq!(exprs.len(), 2);

        let pushdown = pushdown_dyn_filter(&exprs[0]);
        let exact_dynamic = exact_dyn_filter(&exprs[1]);

        // Deactivate (via drop of RegisteredDynFilter)
        let _ = unregister_remote_dyn_filter(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-d"),
        );

        // After deactivation, the filter registration should be gone.
        assert!(regs_by_query.get_query(&query_id).is_none());
        assert!(pushdown.is_complete());
        assert!(exact_dynamic.is_complete());
    }
}
