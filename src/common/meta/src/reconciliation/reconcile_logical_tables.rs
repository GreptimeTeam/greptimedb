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
use store_api::storage::TableId;
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
        let locators = Self::event_locators(persistent_ctx);
        let event = match ctx.trigger {
            EventTrigger::Submitted => {
                ReconcileLogicalTablesEvent::submitted(locators, persistent_ctx.is_subprocedure)
            }
            EventTrigger::Succeeded => self.result_event(locators, true),
            EventTrigger::Failed | EventTrigger::Poisoned => self.result_event(locators, false),
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
        &self,
        locators: Vec<ReconciliationLocator>,
        complete: bool,
    ) -> ReconcileLogicalTablesEvent {
        let metrics = self.context.metrics();
        ReconcileLogicalTablesEvent::result(
            locators,
            complete,
            self.context.persistent_ctx.logical_table_ids.len(),
            metrics.column_metadata_consistent_count,
            metrics.column_metadata_inconsistent_count,
            metrics.create_tables_count,
            metrics.update_table_info_count,
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

    use api::v1::value::ValueData;
    use common_event_recorder::{EventTypeFilter, EventTypeFilterRef};
    use common_procedure::{
        ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
        RetryPhase,
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::reconciliation::event::RECONCILE_TABLE_EVENT_TYPE;
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
        procedure.context.volatile_ctx.metrics = populated_metrics();

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
    fn logical_table_terminal_events_report_existing_state_and_metrics() {
        let events = LogicalTablesEventHarness::all();
        let mut procedure = test_procedure(true);
        procedure.context.volatile_ctx.metrics = populated_metrics();

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
    fn logical_table_recovery_preserves_locators_and_resets_metrics() {
        let events = LogicalTablesEventHarness::all();
        let mut procedure = test_procedure(false);
        procedure.context.volatile_ctx.metrics = populated_metrics();
        let original_dump = procedure.dump().unwrap();
        procedure.context.volatile_ctx.metrics = ReconcileLogicalTableMetrics::default();
        assert_eq!(procedure.dump().unwrap(), original_dump);

        let loaded =
            ReconcileLogicalTablesProcedure::from_json(test_context(), &original_dump).unwrap();
        assert_eq!(loaded.dump().unwrap(), original_dump);
        assert_eq!(
            events
                .event(&loaded, EventTrigger::Recovered)
                .unwrap()
                .extra_rows()
                .unwrap(),
            events
                .event(&procedure, EventTrigger::Submitted)
                .unwrap()
                .extra_rows()
                .unwrap(),
        );
        assert_eq!(
            events
                .event(&loaded, EventTrigger::Succeeded)
                .unwrap()
                .json_payload()
                .unwrap(),
            json!({
                "version": 1,
                "complete": true,
                "processed_table_count": 2,
                "metadata_consistent_table_count": 0,
                "metadata_inconsistent_table_count": 0,
                "create_table_count": 0,
                "update_table_info_count": 0,
            })
        );
    }

    #[test]
    fn logical_table_failure_before_resolution_reports_requested_tables() {
        let procedure = test_procedure(false);
        let payload = LogicalTablesEventHarness::all()
            .event(&procedure, EventTrigger::Failed)
            .unwrap()
            .json_payload()
            .unwrap();
        assert_eq!(
            payload,
            json!({
                "version": 1,
                "complete": false,
                "processed_table_count": 2,
                "metadata_consistent_table_count": 0,
                "metadata_inconsistent_table_count": 0,
                "create_table_count": 0,
                "update_table_info_count": 0,
            })
        );
    }

    fn populated_metrics() -> ReconcileLogicalTableMetrics {
        ReconcileLogicalTableMetrics {
            column_metadata_consistent_count: 3,
            column_metadata_inconsistent_count: 1,
            create_tables_count: 2,
            update_table_info_count: 4,
            ..Default::default()
        }
    }

    fn expected_populated_payload(complete: bool) -> Value {
        json!({
            "version": 1,
            "complete": complete,
            "processed_table_count": 2,
            "metadata_consistent_table_count": 3,
            "metadata_inconsistent_table_count": 1,
            "create_table_count": 2,
            "update_table_info_count": 4,
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
