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
pub(crate) mod resolve_table_metadatas;
pub(crate) mod update_table_infos;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, EventContext, EventTrigger, LockKey,
    Procedure, Result as ProcedureResult, Status,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::storage::{RegionNumber, TableId};
use table::metadata::TableInfo;
use table::table_name::TableName;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::metrics;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::event::{
    RECONCILE_LOGICAL_TABLES_EVENT_TYPE, ReconcileLogicalTablesEvent, ReconciliationLocator,
};
use crate::reconciliation::reconcile_logical_tables::reconciliation_start::ReconciliationStart;
use crate::reconciliation::utils::{Context, ReconcileLogicalTableMetrics};

pub struct ReconcileLogicalTablesContext {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub persistent_ctx: PersistentContext,
    pub volatile_ctx: VolatileContext,
}

#[derive(Debug, Clone, Copy)]
enum LogicalTablesPhase {
    Start,
    ResolveTableMetadatas,
    ReconcileRegions,
    UpdateTableInfos,
}

impl LogicalTablesPhase {
    const fn as_event_value(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::ResolveTableMetadatas => "resolve_table_metadatas",
            Self::ReconcileRegions => "reconcile_regions",
            Self::UpdateTableInfos => "update_table_infos",
        }
    }
}

#[derive(Debug, Default)]
struct ReconcileLogicalTablesResultSummary {
    metadata_consistent_table_count: usize,
    metadata_inconsistent_table_count: usize,
    missing_region_table_count: usize,
    resolved_column_count: usize,
    scanned_region_count: usize,
    created_region_table_count: usize,
    created_region_count: usize,
    updated_table_info_count: usize,
    last_completed_phase: Option<LogicalTablesPhase>,
}

impl ReconcileLogicalTablesResultSummary {
    fn mark_start_completed(&mut self) {
        self.last_completed_phase = Some(LogicalTablesPhase::Start);
    }

    fn begin_resolution(&mut self) {
        self.metadata_consistent_table_count = 0;
        self.metadata_inconsistent_table_count = 0;
        self.missing_region_table_count = 0;
        self.resolved_column_count = 0;
        self.scanned_region_count = 0;
        self.created_region_table_count = 0;
        self.created_region_count = 0;
        self.updated_table_info_count = 0;
        self.last_completed_phase = Some(LogicalTablesPhase::Start);
    }

    fn record_scanned_regions(&mut self, count: usize) {
        self.scanned_region_count += count;
    }

    fn record_missing_region_table(&mut self) {
        self.missing_region_table_count += 1;
    }

    fn record_consistent_table(&mut self, resolved_column_count: usize) {
        self.metadata_consistent_table_count += 1;
        self.resolved_column_count += resolved_column_count;
    }

    fn record_inconsistent_table(&mut self) {
        self.metadata_inconsistent_table_count += 1;
    }

    fn mark_resolution_completed(&mut self) {
        self.last_completed_phase = Some(LogicalTablesPhase::ResolveTableMetadatas);
    }

    fn begin_region_reconciliation(&mut self) {
        self.created_region_table_count = 0;
        self.created_region_count = 0;
        self.updated_table_info_count = 0;
        self.last_completed_phase = Some(LogicalTablesPhase::ResolveTableMetadatas);
    }

    fn mark_regions_reconciled(&mut self, table_count: usize, region_count: usize) {
        self.created_region_table_count = table_count;
        self.created_region_count = region_count;
        self.last_completed_phase = Some(LogicalTablesPhase::ReconcileRegions);
    }

    fn record_created_regions(&mut self, table_count: usize, region_count: usize) {
        self.created_region_table_count = table_count;
        self.created_region_count = region_count;
    }

    fn begin_table_info_update(&mut self) {
        self.updated_table_info_count = 0;
        self.last_completed_phase = Some(LogicalTablesPhase::ReconcileRegions);
    }

    fn record_updated_table_infos(&mut self, count: usize) {
        self.updated_table_info_count += count;
    }

    fn mark_table_info_update_completed(&mut self) {
        self.last_completed_phase = Some(LogicalTablesPhase::UpdateTableInfos);
    }

    fn last_completed_phase(&self) -> Option<&'static str> {
        self.last_completed_phase
            .map(LogicalTablesPhase::as_event_value)
    }
}

impl ReconcileLogicalTablesContext {
    /// Creates a new [`ReconcileLogicalTablesContext`] with the given [`Context`] and [`PersistentContext`].
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

    /// Returns a mutable reference to the metrics.
    pub(crate) fn mut_metrics(&mut self) -> &mut ReconcileLogicalTableMetrics {
        &mut self.volatile_ctx.metrics
    }

    /// Returns a reference to the metrics.
    pub(crate) fn metrics(&self) -> &ReconcileLogicalTableMetrics {
        &self.volatile_ctx.metrics
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    pub(crate) table_id: TableId,
    pub(crate) table_name: TableName,
    // The logical tables need to be reconciled.
    // The logical tables belongs to the physical table.
    pub(crate) logical_tables: Vec<TableName>,
    // The logical table ids.
    // The value will be set in `ReconciliationStart` state.
    pub(crate) logical_table_ids: Vec<TableId>,
    /// The table info value.
    /// The value will be set in `ReconciliationStart` state.
    pub(crate) table_info_value: Option<DeserializedValueWithBytes<TableInfoValue>>,
    // The physical table route.
    // The value will be set in `ReconciliationStart` state.
    pub(crate) physical_table_route: Option<PhysicalTableRouteValue>,
    // The table infos to be updated.
    // The value will be set in `ResolveTableMetadatas` state.
    pub(crate) update_table_infos: Vec<(TableId, Vec<ColumnMetadata>)>,
    // The table infos to be created.
    // The value will be set in `ResolveTableMetadatas` state.
    pub(crate) create_tables: Vec<(TableId, TableInfo)>,
    // Whether the procedure is a subprocedure.
    pub(crate) is_subprocedure: bool,
}

impl PersistentContext {
    pub(crate) fn new(
        table_id: TableId,
        table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
        is_subprocedure: bool,
    ) -> Self {
        let (logical_table_ids, logical_tables) = logical_tables.into_iter().unzip();

        Self {
            table_id,
            table_name,
            logical_tables,
            logical_table_ids,
            table_info_value: None,
            physical_table_route: None,
            update_table_infos: vec![],
            create_tables: vec![],
            is_subprocedure,
        }
    }
}

#[derive(Default)]
pub(crate) struct VolatileContext {
    pub(crate) metrics: ReconcileLogicalTableMetrics,
    result_summary: ReconcileLogicalTablesResultSummary,
    missing_regions_by_table: HashMap<TableId, Vec<RegionNumber>>,
}

pub struct ReconcileLogicalTablesProcedure {
    pub context: ReconcileLogicalTablesContext,
    state: Box<dyn State>,
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

impl ReconcileLogicalTablesProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileLogicalTables";

    pub fn new(
        ctx: Context,
        table_id: TableId,
        table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
        is_subprocedure: bool,
    ) -> Self {
        let persistent_ctx =
            PersistentContext::new(table_id, table_name, logical_tables, is_subprocedure);
        let context = ReconcileLogicalTablesContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconciliationStart);
        Self { context, state }
    }

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileLogicalTablesContext::new(ctx, persistent_ctx);
        Ok(Self { context, state })
    }
}

#[async_trait]
impl Procedure for ReconcileLogicalTablesProcedure {
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

        let mut table_ids = self
            .context
            .persistent_ctx
            .logical_table_ids
            .iter()
            .map(|t| TableLock::Write(*t).into())
            .collect::<Vec<_>>();
        table_ids.sort_unstable();
        table_ids.push(TableLock::Read(self.context.table_id()).into());
        if self.context.persistent_ctx.is_subprocedure {
            // The catalog and schema are already locked by the parent procedure.
            // Only lock the table name.
            return LockKey::new(table_ids);
        }
        let mut keys = vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
        ];
        keys.extend(table_ids);
        LockKey::new(keys)
    }

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        if !ctx
            .event_type_filter
            .allows(RECONCILE_LOGICAL_TABLES_EVENT_TYPE)
        {
            return None;
        }

        let persistent_ctx = &self.context.persistent_ctx;
        let result_summary = &self.context.volatile_ctx.result_summary;
        let locators = Self::event_locators(persistent_ctx);
        let event = match ctx.trigger {
            EventTrigger::Submitted => {
                ReconcileLogicalTablesEvent::submitted(locators, persistent_ctx.is_subprocedure)
            }
            EventTrigger::Succeeded => Self::result_event(locators, result_summary, true),
            EventTrigger::Failed | EventTrigger::Poisoned => {
                Self::result_event(locators, result_summary, false)
            }
            _ => ReconcileLogicalTablesEvent::lifecycle(locators),
        };
        Some(Box::new(event))
    }
}

impl ReconcileLogicalTablesProcedure {
    fn event_locators(persistent_ctx: &PersistentContext) -> Vec<ReconciliationLocator> {
        persistent_ctx
            .logical_table_ids
            .iter()
            .zip(&persistent_ctx.logical_tables)
            .map(|(table_id, table_name)| {
                ReconciliationLocator::logical_table(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                    &table_name.table_name,
                    *table_id,
                    persistent_ctx.table_id,
                )
            })
            .collect()
    }

    fn result_event(
        locators: Vec<ReconciliationLocator>,
        summary: &ReconcileLogicalTablesResultSummary,
        complete: bool,
    ) -> ReconcileLogicalTablesEvent {
        ReconcileLogicalTablesEvent::result(
            locators,
            complete,
            summary.metadata_consistent_table_count,
            summary.metadata_inconsistent_table_count,
            summary.missing_region_table_count,
            summary.resolved_column_count,
            summary.scanned_region_count,
            summary.created_region_table_count,
            summary.created_region_count,
            summary.updated_table_info_count,
            summary.last_completed_phase(),
        )
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_logical_tables_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::region::RegionResponse;
    use api::v1::region::region_request::Body;
    use api::v1::value::ValueData;
    use common_event_recorder::{EventTypeFilter, EventTypeFilterRef};
    use common_procedure::{
        ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
        RetryPhase,
    };
    use common_procedure_test::MockContextProvider;
    use serde_json::{Value, json};
    use store_api::metadata::RegionMetadata;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc;

    use super::*;
    use crate::ddl::test_util::datanode_handler::DatanodeWatcher;
    use crate::error;
    use crate::key::table_route::PhysicalTableRouteValue;
    use crate::key::test_utils::new_test_table_info_with_name;
    use crate::peer::Peer;
    use crate::reconciliation::event::RECONCILE_TABLE_EVENT_TYPE;
    use crate::reconciliation::reconcile_logical_tables::reconcile_regions::ReconcileRegions;
    use crate::rpc::router::{Region, RegionRoute};
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    struct LogicalTablesEventHarness {
        procedure_id: ProcedureId,
        lifecycle_state: ProcedureState,
        event_type_filter: EventTypeFilterRef,
    }

    impl LogicalTablesEventHarness {
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
    fn logical_table_submitted_events_cover_root_and_child_intent() {
        let events = LogicalTablesEventHarness::all();
        let root = test_procedure(false);
        let child = test_procedure(true);
        for (procedure, is_subprocedure) in [(&root, false), (&child, true)] {
            let submitted = events.event(procedure, EventTrigger::Submitted).unwrap();
            assert_eq!(submitted.event_type(), RECONCILE_LOGICAL_TABLES_EVENT_TYPE);
            assert_eq!(
                submitted.json_payload().unwrap(),
                json!({
                    "version": 1,
                    "logical_table_count": 2,
                    "is_subprocedure": is_subprocedure,
                })
            );
            let rows = submitted.extra_rows().unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0]
                    .values
                    .iter()
                    .map(|value| value.value_data.clone())
                    .collect::<Vec<_>>(),
                vec![
                    Some(ValueData::StringValue("greptime".to_string())),
                    Some(ValueData::StringValue("public".to_string())),
                    Some(ValueData::StringValue("cpu".to_string())),
                    Some(ValueData::U32Value(43)),
                    Some(ValueData::U32Value(42)),
                ]
            );
            assert_eq!(
                rows[1]
                    .values
                    .iter()
                    .map(|value| value.value_data.clone())
                    .collect::<Vec<_>>(),
                vec![
                    Some(ValueData::StringValue("greptime".to_string())),
                    Some(ValueData::StringValue("public".to_string())),
                    Some(ValueData::StringValue("memory".to_string())),
                    Some(ValueData::U32Value(44)),
                    Some(ValueData::U32Value(42)),
                ]
            );
        }
    }

    #[test]
    fn logical_table_non_terminal_lifecycle_events_have_null_payloads() {
        let events = LogicalTablesEventHarness::all();
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
    fn logical_table_terminal_events_report_bounded_results() {
        let events = LogicalTablesEventHarness::all();
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
                expected_populated_payload(complete)
            );
        }
    }

    #[test]
    fn logical_table_event_filtering_uses_the_reconciliation_event_type() {
        let procedure = test_procedure(false);
        assert!(
            LogicalTablesEventHarness::selected([RECONCILE_LOGICAL_TABLES_EVENT_TYPE])
                .event(&procedure, EventTrigger::Submitted)
                .is_some()
        );
        assert!(
            LogicalTablesEventHarness::selected([RECONCILE_TABLE_EVENT_TYPE])
                .event(&procedure, EventTrigger::Submitted)
                .is_none()
        );
        assert!(
            LogicalTablesEventHarness::selected([])
                .event(&procedure, EventTrigger::Submitted)
                .is_none()
        );
    }

    #[test]
    fn logical_table_result_summary_is_not_persisted() {
        let events = LogicalTablesEventHarness::all();
        let mut procedure = test_procedure(false);
        procedure.context.volatile_ctx.result_summary = populated_summary();
        let value: Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
        assert!(value["persistent_ctx"].get("result_summary").is_none());

        let loaded = ReconcileLogicalTablesProcedure::from_json(
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
                "processed_table_count": 0,
                "metadata_consistent_table_count": 0,
                "metadata_inconsistent_table_count": 0,
                "missing_region_table_count": 0,
                "resolved_column_count": 0,
                "scanned_region_count": 0,
                "created_region_table_count": 0,
                "created_region_count": 0,
                "updated_table_info_count": 0,
                "last_completed_phase": null,
            })
        );
    }

    #[test]
    fn logical_table_partial_summary_preserves_completed_work_without_advancing_phases() {
        let events = LogicalTablesEventHarness::all();
        let mut procedure = test_procedure(false);
        let summary = &mut procedure.context.volatile_ctx.result_summary;
        summary.mark_start_completed();
        summary.begin_resolution();
        summary.record_scanned_regions(2);
        summary.record_missing_region_table();
        summary.record_consistent_table(3);
        assert_eq!(
            events
                .event(&procedure, EventTrigger::Failed)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "processed_table_count": 2,
                "metadata_consistent_table_count": 1,
                "metadata_inconsistent_table_count": 0,
                "missing_region_table_count": 1,
                "resolved_column_count": 3,
                "scanned_region_count": 2,
                "created_region_table_count": 0,
                "created_region_count": 0,
                "updated_table_info_count": 0,
                "last_completed_phase": "start",
            })
        );
    }

    #[test]
    fn logical_table_summary_accumulates_committed_table_info_chunks() {
        let mut summary = ReconcileLogicalTablesResultSummary::default();
        summary.mark_regions_reconciled(0, 0);
        summary.begin_table_info_update();
        summary.record_updated_table_infos(2);
        summary.record_updated_table_infos(1);

        assert_eq!(summary.updated_table_info_count, 3);
        assert_eq!(
            summary.last_completed_phase(),
            Some(LogicalTablesPhase::ReconcileRegions.as_event_value())
        );
    }

    #[tokio::test]
    async fn logical_table_mixed_region_outcome_preserves_partial_result() {
        let (tx, _rx) = mpsc::channel(8);
        let handler = DatanodeWatcher::new(tx).with_handler(|_peer, request| {
            match request.body.as_ref().unwrap() {
                Body::Creates(request) => {
                    assert_eq!(request.requests.len(), 1);
                    let region_number =
                        RegionId::from_u64(request.requests[0].region_id).region_number();
                    if region_number == 1 {
                        Ok(RegionResponse::new(0))
                    } else {
                        error::UnexpectedSnafu {
                            err_msg: "mock error",
                        }
                        .fail()
                    }
                }
                _ => unreachable!(),
            }
        });
        let ddl_context = new_ddl_context(Arc::new(MockDatanodeManager::new(handler)));
        let context = Context {
            node_manager: ddl_context.node_manager,
            table_metadata_manager: ddl_context.table_metadata_manager,
            cache_invalidator: ddl_context.cache_invalidator,
        };
        let mut procedure = ReconcileLogicalTablesProcedure::new(
            context,
            42,
            TableName::new("greptime", "public", "physical_metrics"),
            vec![
                (43, TableName::new("greptime", "public", "cpu")),
                (44, TableName::new("greptime", "public", "memory")),
            ],
            false,
        );
        procedure.context.persistent_ctx.create_tables = vec![
            (43, new_test_table_info_with_name(43, "cpu")),
            (44, new_test_table_info_with_name(44, "memory")),
        ];
        procedure.context.persistent_ctx.physical_table_route =
            Some(PhysicalTableRouteValue::new(vec![
                RegionRoute {
                    region: Region::new_test(store_api::storage::RegionId::new(42, 1)),
                    leader_peer: Some(Peer::empty(1)),
                    ..Default::default()
                },
                RegionRoute {
                    region: Region::new_test(store_api::storage::RegionId::new(42, 2)),
                    leader_peer: Some(Peer::empty(1)),
                    ..Default::default()
                },
            ]));
        let summary = &mut procedure.context.volatile_ctx.result_summary;
        summary.mark_start_completed();
        summary.record_scanned_regions(4);
        summary.record_missing_region_table();
        summary.record_missing_region_table();
        summary.mark_resolution_completed();
        procedure
            .context
            .volatile_ctx
            .missing_regions_by_table
            .insert(43, vec![1]);
        procedure
            .context
            .volatile_ctx
            .missing_regions_by_table
            .insert(44, vec![1, 2]);
        procedure.state = Box::new(ReconcileRegions);

        let procedure_ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
            event_context: None,
        };
        assert!(procedure.execute(&procedure_ctx).await.is_err());
        assert!(
            procedure
                .context
                .volatile_ctx
                .missing_regions_by_table
                .get(&43)
                .is_some_and(Vec::is_empty)
        );
        assert_eq!(
            procedure
                .context
                .volatile_ctx
                .missing_regions_by_table
                .get(&44)
                .map(Vec::as_slice),
            Some(&[2][..])
        );

        assert_eq!(
            LogicalTablesEventHarness::all()
                .event(&procedure, EventTrigger::Failed)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": false,
                "processed_table_count": 2,
                "metadata_consistent_table_count": 0,
                "metadata_inconsistent_table_count": 0,
                "missing_region_table_count": 2,
                "resolved_column_count": 0,
                "scanned_region_count": 4,
                "created_region_table_count": 1,
                "created_region_count": 2,
                "updated_table_info_count": 0,
                "last_completed_phase": "resolve_table_metadatas",
            })
        );
    }

    #[tokio::test]
    async fn logical_table_region_reconciliation_rebuilds_missing_regions_after_recovery() {
        let mut procedure = ReconcileLogicalTablesProcedure::new(
            test_context(),
            42,
            TableName::new("greptime", "public", "physical_metrics"),
            vec![(43, TableName::new("greptime", "public", "cpu"))],
            false,
        );
        procedure.context.persistent_ctx.create_tables =
            vec![(43, new_test_table_info_with_name(43, "cpu"))];
        procedure.context.persistent_ctx.physical_table_route =
            Some(PhysicalTableRouteValue::new(vec![RegionRoute {
                region: Region::new_test(RegionId::new(42, 1)),
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            }]));
        procedure.state = Box::new(ReconcileRegions);
        let dumped = procedure.dump().unwrap();

        let (tx, _rx) = mpsc::channel(4);
        let handler = DatanodeWatcher::new(tx).with_handler(|_peer, request| {
            match request.body.as_ref().unwrap() {
                Body::ListMetadata(request) => Ok(RegionResponse::from_metadata(
                    serde_json::to_vec(&vec![None::<RegionMetadata>; request.region_ids.len()])
                        .unwrap(),
                )),
                Body::Creates(_) => Ok(RegionResponse::new(0)),
                _ => unreachable!(),
            }
        });
        let ddl_context = new_ddl_context(Arc::new(MockDatanodeManager::new(handler)));
        let context = Context {
            node_manager: ddl_context.node_manager,
            table_metadata_manager: ddl_context.table_metadata_manager,
            cache_invalidator: ddl_context.cache_invalidator,
        };
        let mut recovered = ReconcileLogicalTablesProcedure::from_json(context, &dumped).unwrap();
        let procedure_ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
            event_context: None,
        };

        recovered.execute(&procedure_ctx).await.unwrap();

        let summary = &recovered.context.volatile_ctx.result_summary;
        assert_eq!(summary.scanned_region_count, 1);
        assert_eq!(summary.created_region_table_count, 1);
        assert_eq!(summary.created_region_count, 1);
        assert_eq!(
            summary.last_completed_phase(),
            Some(LogicalTablesPhase::ReconcileRegions.as_event_value())
        );
        assert!(recovered.context.persistent_ctx.create_tables.is_empty());
        assert!(
            recovered
                .context
                .volatile_ctx
                .missing_regions_by_table
                .is_empty()
        );
    }

    fn populated_summary() -> ReconcileLogicalTablesResultSummary {
        ReconcileLogicalTablesResultSummary {
            metadata_consistent_table_count: 3,
            metadata_inconsistent_table_count: 1,
            missing_region_table_count: 2,
            resolved_column_count: 12,
            scanned_region_count: 18,
            created_region_table_count: 2,
            created_region_count: 6,
            updated_table_info_count: 1,
            last_completed_phase: Some(LogicalTablesPhase::UpdateTableInfos),
        }
    }

    fn expected_populated_payload(complete: bool) -> Value {
        json!({
            "version": 1,
            "complete": complete,
            "processed_table_count": 6,
            "metadata_consistent_table_count": 3,
            "metadata_inconsistent_table_count": 1,
            "missing_region_table_count": 2,
            "resolved_column_count": 12,
            "scanned_region_count": 18,
            "created_region_table_count": 2,
            "created_region_count": 6,
            "updated_table_info_count": 1,
            "last_completed_phase": "update_table_infos",
        })
    }

    fn test_procedure(is_subprocedure: bool) -> ReconcileLogicalTablesProcedure {
        ReconcileLogicalTablesProcedure::new(
            test_context(),
            42,
            TableName::new("greptime", "public", "physical_metrics"),
            vec![
                (43, TableName::new("greptime", "public", "cpu")),
                (44, TableName::new("greptime", "public", "memory")),
            ],
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
