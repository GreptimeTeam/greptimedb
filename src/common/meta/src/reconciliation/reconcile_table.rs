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

pub(crate) mod reconcile_regions;
pub(crate) mod reconciliation_end;
pub(crate) mod reconciliation_start;
pub(crate) mod resolve_column_metadata;
pub(crate) mod update_table_info;

use std::fmt::Debug;

use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, EventContext, EventTrigger, LockKey,
    Procedure, Result as ProcedureResult, Status,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::storage::TableId;
use table::metadata::TableMeta;
use table::table_name::TableName;
use tonic::async_trait;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::lock_key::{CatalogLock, SchemaLock, TableNameLock};
use crate::metrics;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::event::{
    RECONCILE_TABLE_EVENT_TYPE, ReconcileTableEvent, ReconciliationLocator,
};
use crate::reconciliation::reconcile_table::reconciliation_start::ReconciliationStart;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::{
    Context, ReconcileTableMetrics, build_table_meta_from_column_metadatas,
};

pub struct ReconcileTableContext {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub persistent_ctx: PersistentContext,
    pub volatile_ctx: VolatileContext,
}

#[derive(Debug, Clone, Copy)]
enum TablePhase {
    Start,
    ResolveColumnMetadata,
    ReconcileRegions,
    UpdateTableInfo,
}

impl TablePhase {
    const fn as_event_value(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::ResolveColumnMetadata => "resolve_column_metadata",
            Self::ReconcileRegions => "reconcile_regions",
            Self::UpdateTableInfo => "update_table_info",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TableMetadataState {
    Consistent,
    Inconsistent,
}

impl TableMetadataState {
    const fn as_event_value(self) -> &'static str {
        match self {
            Self::Consistent => "consistent",
            Self::Inconsistent => "inconsistent",
        }
    }
}

#[derive(Debug, Default)]
struct ReconcileTableResultSummary {
    metadata_state: Option<TableMetadataState>,
    resolution_strategy_applied: Option<ResolveStrategy>,
    resolved_column_count: Option<usize>,
    scanned_region_count: usize,
    updated_region_count: usize,
    table_info_updated: bool,
    last_completed_phase: Option<TablePhase>,
}

impl ReconcileTableResultSummary {
    fn record_scanned_regions(&mut self, scanned_region_count: usize) {
        self.scanned_region_count = scanned_region_count;
    }

    fn mark_start_completed(&mut self) {
        self.last_completed_phase = Some(TablePhase::Start);
    }

    fn record_metadata_state(&mut self, metadata_state: TableMetadataState) {
        self.metadata_state = Some(metadata_state);
    }

    fn record_resolution_strategy(&mut self, resolve_strategy: ResolveStrategy) {
        self.resolution_strategy_applied = Some(resolve_strategy);
    }

    fn record_resolved_columns(
        &mut self,
        metadata_state: TableMetadataState,
        resolution_strategy_applied: Option<ResolveStrategy>,
        resolved_column_count: Option<usize>,
    ) {
        self.metadata_state = Some(metadata_state);
        self.resolution_strategy_applied = resolution_strategy_applied;
        self.resolved_column_count = resolved_column_count;
        self.last_completed_phase = Some(TablePhase::ResolveColumnMetadata);
    }

    fn record_updated_regions(&mut self, updated_region_count: usize) {
        self.updated_region_count = updated_region_count;
    }

    fn mark_region_phase_completed(&mut self) {
        self.last_completed_phase = Some(TablePhase::ReconcileRegions);
    }

    fn mark_table_info_updated(&mut self) {
        self.table_info_updated = true;
    }

    fn mark_table_info_phase_completed(&mut self) {
        self.last_completed_phase = Some(TablePhase::UpdateTableInfo);
    }

    fn metadata_state(&self) -> Option<&'static str> {
        self.metadata_state.map(TableMetadataState::as_event_value)
    }

    fn last_completed_phase(&self) -> Option<&'static str> {
        self.last_completed_phase.map(TablePhase::as_event_value)
    }
}

impl ReconcileTableContext {
    /// Creates a new [`ReconcileTableContext`] with the given [`Context`] and [`PersistentContext`].
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            node_manager: ctx.node_manager,
            table_metadata_manager: ctx.table_metadata_manager,
            cache_invalidator: ctx.cache_invalidator,
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
        }
    }

    /// Returns the physical table name.
    pub(crate) fn table_name(&self) -> &TableName {
        &self.persistent_ctx.table_name
    }

    /// Returns the physical table id.
    pub(crate) fn table_id(&self) -> TableId {
        self.persistent_ctx.table_id
    }

    /// Builds a [`TableMeta`] from the provided [`ColumnMetadata`]s.
    pub(crate) fn build_table_meta(
        &self,
        column_metadatas: &[ColumnMetadata],
    ) -> Result<TableMeta> {
        // Safety: The table info value is set in `ReconciliationStart` state.
        let table_info_value = self.persistent_ctx.table_info_value.as_ref().unwrap();
        let table_id = self.table_id();
        let table_ref = self.table_name().table_ref();
        let name_to_ids = table_info_value.table_info.name_to_ids();
        let table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_info_value.table_info.meta,
            name_to_ids,
            column_metadatas,
        )?;

        Ok(table_meta)
    }

    /// Returns a mutable reference to the metrics.
    pub(crate) fn mut_metrics(&mut self) -> &mut ReconcileTableMetrics {
        &mut self.volatile_ctx.metrics
    }

    /// Returns a reference to the metrics.
    pub(crate) fn metrics(&self) -> &ReconcileTableMetrics {
        &self.volatile_ctx.metrics
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    pub(crate) table_id: TableId,
    pub(crate) table_name: TableName,
    pub(crate) resolve_strategy: ResolveStrategy,
    /// The table info value.
    /// The value will be set in `ReconciliationStart` state.
    pub(crate) table_info_value: Option<DeserializedValueWithBytes<TableInfoValue>>,
    // The physical table route.
    // The value will be set in `ReconciliationStart` state.
    pub(crate) physical_table_route: Option<PhysicalTableRouteValue>,
    // Whether the procedure is a subprocedure.
    pub(crate) is_subprocedure: bool,
}

impl PersistentContext {
    pub(crate) fn new(
        table_id: TableId,
        table_name: TableName,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            table_id,
            table_name,
            resolve_strategy,
            table_info_value: None,
            physical_table_route: None,
            is_subprocedure,
        }
    }
}

#[derive(Default)]
pub(crate) struct VolatileContext {
    pub(crate) table_meta: Option<TableMeta>,
    pub(crate) metrics: ReconcileTableMetrics,
    result_summary: ReconcileTableResultSummary,
}

pub struct ReconcileTableProcedure {
    pub context: ReconcileTableContext,
    state: Box<dyn State>,
}

impl ReconcileTableProcedure {
    /// Creates a new [`ReconcileTableProcedure`] with the given [`Context`] and [`PersistentContext`].
    pub fn new(
        ctx: Context,
        table_id: TableId,
        table_name: TableName,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        let persistent_ctx =
            PersistentContext::new(table_id, table_name, resolve_strategy, is_subprocedure);
        let context = ReconcileTableContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconciliationStart);
        Self { context, state }
    }
}

impl ReconcileTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileTable";

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileTableContext::new(ctx, persistent_ctx);
        Ok(Self { context, state })
    }
}

#[derive(Debug, Serialize)]
struct ProcedureData<'a> {
    state: &'a dyn State,
    persistent_ctx: &'a PersistentContext,
}

#[derive(Debug, Deserialize)]
struct ProcedureDataOwned {
    state: Box<dyn State>,
    persistent_ctx: PersistentContext,
}

#[async_trait]
impl Procedure for ReconcileTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;

        let procedure_name = Self::TYPE_NAME;
        let step = state.name();
        let _timer = metrics::METRIC_META_RECONCILIATION_PROCEDURE
            .with_label_values(&[procedure_name, step])
            .start_timer();
        match state.next(&mut self.context, _ctx).await {
            Ok((next, status)) => {
                *state = next;
                Ok(status)
            }
            Err(e) => {
                if e.is_retry_later() {
                    metrics::METRIC_META_RECONCILIATION_PROCEDURE_ERROR
                        .with_label_values(&[procedure_name, step, metrics::ERROR_TYPE_RETRYABLE])
                        .inc();
                    Err(ProcedureError::retry_later(e))
                } else {
                    metrics::METRIC_META_RECONCILIATION_PROCEDURE_ERROR
                        .with_label_values(&[procedure_name, step, metrics::ERROR_TYPE_EXTERNAL])
                        .inc();
                    Err(ProcedureError::external(e))
                }
            }
        }
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = ProcedureData {
            state: self.state.as_ref(),
            persistent_ctx: &self.context.persistent_ctx,
        };
        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.context.table_name().table_ref();

        if self.context.persistent_ctx.is_subprocedure {
            // The catalog and schema are already locked by the parent procedure.
            // Only lock the table name.
            return LockKey::new(vec![
                TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
            ]);
        }

        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ])
    }

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        if !ctx.event_type_filter.allows(RECONCILE_TABLE_EVENT_TYPE) {
            return None;
        }

        let persistent_ctx = &self.context.persistent_ctx;
        let table_name = &persistent_ctx.table_name;
        let locator = ReconciliationLocator::physical_table(
            &table_name.catalog_name,
            &table_name.schema_name,
            &table_name.table_name,
            persistent_ctx.table_id,
        );
        let event = match ctx.trigger {
            EventTrigger::Submitted => ReconcileTableEvent::table_submitted(
                locator,
                persistent_ctx.resolve_strategy,
                persistent_ctx.is_subprocedure,
            ),
            EventTrigger::Succeeded => {
                Self::result_event(locator, &self.context.volatile_ctx.result_summary, true)
            }
            EventTrigger::Failed | EventTrigger::Poisoned => {
                Self::result_event(locator, &self.context.volatile_ctx.result_summary, false)
            }
            _ => ReconcileTableEvent::table_lifecycle(locator),
        };
        Some(Box::new(event))
    }
}

impl ReconcileTableProcedure {
    fn result_event(
        locator: ReconciliationLocator,
        summary: &ReconcileTableResultSummary,
        complete: bool,
    ) -> ReconcileTableEvent {
        ReconcileTableEvent::table_result(
            locator,
            complete,
            summary.metadata_state(),
            summary.resolution_strategy_applied,
            summary.resolved_column_count,
            summary.scanned_region_count,
            summary.updated_region_count,
            summary.table_info_updated,
            summary.last_completed_phase(),
        )
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_table_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileTableContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_event_recorder::{EventTypeFilter, EventTypeFilterRef};
    use common_procedure::{
        ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
        RetryPhase,
    };
    use common_procedure_test::MockContextProvider;
    use serde_json::{Value, json};
    use store_api::storage::RegionId;

    use super::*;
    use crate::ddl::test_util::datanode_handler::PartialSuccessDatanodeHandler;
    use crate::key::DeserializedValueWithBytes;
    use crate::key::table_info::TableInfoValue;
    use crate::key::table_route::PhysicalTableRouteValue;
    use crate::key::test_utils::new_test_table_info_with_name;
    use crate::peer::Peer;
    use crate::reconciliation::reconcile_table::reconcile_regions::ReconcileRegions;
    use crate::reconciliation::utils::build_column_metadata_from_table_info;
    use crate::rpc::router::{Region, RegionRoute};
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    struct TableEventHarness {
        procedure_id: ProcedureId,
        lifecycle_state: ProcedureState,
        event_type_filter: EventTypeFilterRef,
    }

    impl TableEventHarness {
        fn all() -> Self {
            Self {
                procedure_id: ProcedureId::random(),
                lifecycle_state: ProcedureState::Running,
                event_type_filter: Arc::new(EventTypeFilter::All),
            }
        }

        fn selected(event_types: impl IntoIterator<Item = &'static str>) -> Self {
            Self {
                event_type_filter: Arc::new(EventTypeFilter::Only(
                    event_types.into_iter().map(str::to_string).collect(),
                )),
                ..Self::all()
            }
        }

        fn event(
            &self,
            procedure: &dyn Procedure,
            trigger: EventTrigger,
        ) -> Option<Box<dyn common_event_recorder::Event>> {
            procedure.event(&EventContext {
                procedure_id: self.procedure_id,
                lifecycle_state: &self.lifecycle_state,
                trigger,
                event_type_filter: self.event_type_filter.clone(),
                event_context: None,
            })
        }
    }

    #[test]
    fn table_submitted_events_cover_root_and_child_intent() {
        let events = TableEventHarness::all();
        let root = test_procedure(false);
        let child = test_procedure(true);
        for (procedure, is_subprocedure) in [(&root, false), (&child, true)] {
            let submitted = events.event(procedure, EventTrigger::Submitted).unwrap();
            assert_eq!(submitted.event_type(), RECONCILE_TABLE_EVENT_TYPE);
            assert_eq!(
                submitted.json_payload().unwrap(),
                json!({
                    "version": 1,
                    "resolve_strategy": "use_latest",
                    "is_subprocedure": is_subprocedure,
                })
            );
        }
    }

    #[test]
    fn table_non_terminal_lifecycle_events_have_null_payloads() {
        let events = TableEventHarness::all();
        let mut procedure = test_procedure(true);
        procedure.context.volatile_ctx.result_summary = populated_summary();

        for trigger in [
            EventTrigger::Recovered,
            EventTrigger::ChildSubmitted {
                procedure_id: ProcedureId::random(),
                outcome: ChildSubmissionOutcome::Accepted,
            },
            EventTrigger::Retrying {
                phase: RetryPhase::Execute,
                attempt: 2,
            },
            EventTrigger::RollingBack,
        ] {
            assert_eq!(
                events
                    .event(&procedure, trigger)
                    .unwrap()
                    .json_payload()
                    .unwrap(),
                Value::Null
            );
        }
    }

    #[test]
    fn table_terminal_events_report_bounded_results() {
        let events = TableEventHarness::all();
        let mut procedure = test_procedure(true);
        procedure.context.volatile_ctx.result_summary = populated_summary();

        for (trigger, complete) in [
            (EventTrigger::Succeeded, true),
            (EventTrigger::Failed, false),
            (EventTrigger::Poisoned, false),
        ] {
            assert_eq!(
                events
                    .event(&procedure, trigger)
                    .unwrap()
                    .json_payload()
                    .unwrap(),
                json!({
                    "version": 1,
                    "complete": complete,
                    "metadata_state": "inconsistent",
                    "resolution_strategy_applied": "use_latest",
                    "resolved_column_count": 3,
                    "scanned_region_count": 2,
                    "updated_region_count": 1,
                    "table_info_updated": true,
                    "last_completed_phase": "update_table_info",
                })
            );
        }
    }

    #[test]
    fn table_event_filtering_uses_the_reconciliation_event_type() {
        let procedure = test_procedure(false);
        assert!(
            TableEventHarness::selected([RECONCILE_TABLE_EVENT_TYPE])
                .event(&procedure, EventTrigger::Submitted)
                .is_some()
        );
        assert!(
            TableEventHarness::selected(["create_table"])
                .event(&procedure, EventTrigger::Submitted)
                .is_none()
        );
        assert!(
            TableEventHarness::selected([])
                .event(&procedure, EventTrigger::Submitted)
                .is_none()
        );
    }

    #[test]
    fn table_result_summary_is_not_persisted() {
        let events = TableEventHarness::all();
        let mut procedure = test_procedure(false);
        procedure.context.volatile_ctx.result_summary = populated_summary();
        let value: Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
        assert!(value["persistent_ctx"].get("result_summary").is_none());

        let loaded = ReconcileTableProcedure::from_json(
            test_context(),
            &serde_json::to_string(&value).unwrap(),
        )
        .unwrap();
        assert_eq!(
            events
                .event(&loaded, EventTrigger::Succeeded)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": true,
                "metadata_state": null,
                "resolution_strategy_applied": null,
                "resolved_column_count": null,
                "scanned_region_count": 0,
                "updated_region_count": 0,
                "table_info_updated": false,
                "last_completed_phase": null,
            })
        );
    }

    #[test]
    fn table_partial_summary_preserves_completed_work_without_advancing_phases() {
        let events = TableEventHarness::all();
        let mut procedure = test_procedure(false);
        let summary = &mut procedure.context.volatile_ctx.result_summary;
        summary.record_scanned_regions(2);

        assert_eq!(
            events
                .event(&procedure, EventTrigger::Failed)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "metadata_state": null,
                "resolution_strategy_applied": null,
                "resolved_column_count": null,
                "scanned_region_count": 2,
                "updated_region_count": 0,
                "table_info_updated": false,
                "last_completed_phase": null,
            })
        );

        let summary = &mut procedure.context.volatile_ctx.result_summary;
        summary.mark_start_completed();
        summary.record_metadata_state(TableMetadataState::Inconsistent);
        assert_eq!(
            events
                .event(&procedure, EventTrigger::Failed)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "metadata_state": "inconsistent",
                "resolution_strategy_applied": null,
                "resolved_column_count": null,
                "scanned_region_count": 2,
                "updated_region_count": 0,
                "table_info_updated": false,
                "last_completed_phase": "start",
            })
        );

        let summary = &mut procedure.context.volatile_ctx.result_summary;
        summary.record_resolved_columns(
            TableMetadataState::Inconsistent,
            Some(ResolveStrategy::UseLatest),
            Some(3),
        );
        summary.record_updated_regions(1);
        assert_eq!(
            events
                .event(&procedure, EventTrigger::Failed)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "metadata_state": "inconsistent",
                "resolution_strategy_applied": "use_latest",
                "resolved_column_count": 3,
                "scanned_region_count": 2,
                "updated_region_count": 1,
                "table_info_updated": false,
                "last_completed_phase": "resolve_column_metadata",
            })
        );
    }

    #[tokio::test]
    async fn table_mixed_region_outcome_preserves_partial_result() {
        let ddl_context = new_ddl_context(Arc::new(MockDatanodeManager::new(
            PartialSuccessDatanodeHandler { retryable: false },
        )));
        let context = Context {
            node_manager: ddl_context.node_manager,
            table_metadata_manager: ddl_context.table_metadata_manager,
            cache_invalidator: ddl_context.cache_invalidator,
        };
        let mut procedure = ReconcileTableProcedure::new(
            context,
            42,
            TableName::new("greptime", "public", "metrics"),
            ResolveStrategy::UseLatest,
            false,
        );

        let mut table_info = new_test_table_info_with_name(42, "metrics");
        table_info.meta.column_ids = vec![0, 1, 2];
        let name_to_ids = table_info.name_to_ids().unwrap();
        let column_metadatas = build_column_metadata_from_table_info(
            table_info.meta.schema.column_schemas(),
            &table_info.meta.primary_key_indices,
            &name_to_ids,
        )
        .unwrap();
        let region_ids = [RegionId::new(42, 1), RegionId::new(42, 2)];
        procedure.context.persistent_ctx.table_info_value = Some(
            DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info)),
        );
        procedure.context.persistent_ctx.physical_table_route =
            Some(PhysicalTableRouteValue::new(vec![
                RegionRoute {
                    region: Region::new_test(region_ids[0]),
                    leader_peer: Some(Peer::empty(1)),
                    ..Default::default()
                },
                RegionRoute {
                    region: Region::new_test(region_ids[1]),
                    leader_peer: Some(Peer::empty(2)),
                    ..Default::default()
                },
            ]));

        let summary = &mut procedure.context.volatile_ctx.result_summary;
        summary.record_scanned_regions(region_ids.len());
        summary.mark_start_completed();
        summary.record_resolved_columns(
            TableMetadataState::Inconsistent,
            Some(ResolveStrategy::UseLatest),
            Some(column_metadatas.len()),
        );
        procedure.state = Box::new(ReconcileRegions::new(column_metadatas, region_ids.to_vec()));

        let procedure_ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
            event_context: None,
        };
        assert!(procedure.execute(&procedure_ctx).await.is_err());

        assert_eq!(
            TableEventHarness::all()
                .event(&procedure, EventTrigger::Failed)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "metadata_state": "inconsistent",
                "resolution_strategy_applied": "use_latest",
                "resolved_column_count": 3,
                "scanned_region_count": 2,
                "updated_region_count": 1,
                "table_info_updated": false,
                "last_completed_phase": "resolve_column_metadata",
            })
        );
    }

    fn populated_summary() -> ReconcileTableResultSummary {
        ReconcileTableResultSummary {
            metadata_state: Some(TableMetadataState::Inconsistent),
            resolution_strategy_applied: Some(ResolveStrategy::UseLatest),
            resolved_column_count: Some(3),
            scanned_region_count: 2,
            updated_region_count: 1,
            table_info_updated: true,
            last_completed_phase: Some(TablePhase::UpdateTableInfo),
        }
    }

    fn test_procedure(is_subprocedure: bool) -> ReconcileTableProcedure {
        ReconcileTableProcedure::new(
            test_context(),
            42,
            TableName::new("greptime", "public", "metrics"),
            ResolveStrategy::UseLatest,
            is_subprocedure,
        )
    }

    fn test_context() -> Context {
        let ddl_context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        Context {
            node_manager: ddl_context.node_manager,
            table_metadata_manager: ddl_context.table_metadata_manager,
            cache_invalidator: ddl_context.cache_invalidator,
        }
    }
}
