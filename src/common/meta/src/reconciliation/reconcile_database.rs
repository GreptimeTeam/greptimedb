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

pub(crate) mod end;
pub(crate) mod reconcile_logical_tables;
pub(crate) mod reconcile_tables;
pub(crate) mod start;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Instant;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, EventContext, EventTrigger, LockKey,
    Procedure, Result as ProcedureResult, Status,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::TableId;
use table::table_name::TableName;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::TableMetadataManagerRef;
use crate::key::table_name::TableNameValue;
use crate::lock_key::{CatalogLock, SchemaLock};
use crate::metrics;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::event::{
    RECONCILE_DATABASE_EVENT_TYPE, ReconciliationEvent, ReconciliationLocator,
};
use crate::reconciliation::reconcile_database::start::ReconcileDatabaseStart;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::{
    Context, ReconcileDatabaseMetrics, SubprocedureMeta, wait_for_inflight_subprocedures,
};
pub(crate) const DEFAULT_PARALLELISM: usize = 64;

pub(crate) struct ReconcileDatabaseContext {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    persistent_ctx: PersistentContext,
    volatile_ctx: VolatileContext,
}

impl ReconcileDatabaseContext {
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            node_manager: ctx.node_manager,
            table_metadata_manager: ctx.table_metadata_manager,
            cache_invalidator: ctx.cache_invalidator,
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
        }
    }

    /// Waits for inflight subprocedures to complete.
    async fn wait_for_inflight_subprocedures(
        &mut self,
        procedure_ctx: &ProcedureContext,
        phase: DatabasePhase,
    ) -> Result<()> {
        if !self.volatile_ctx.inflight_subprocedures.is_empty() {
            let result = wait_for_inflight_subprocedures(
                procedure_ctx,
                &self.volatile_ctx.inflight_subprocedures,
                self.persistent_ctx.fail_fast,
            )
            .await?;

            // Collects result into metrics
            let metrics: ReconcileDatabaseMetrics = result.into();
            self.volatile_ctx.inflight_subprocedures.clear();
            self.persistent_ctx.result_summary.record(phase, &metrics);
            self.volatile_ctx.metrics += metrics;
        }

        Ok(())
    }

    /// Returns the immutable metrics.
    pub(crate) fn metrics(&self) -> &ReconcileDatabaseMetrics {
        &self.volatile_ctx.metrics
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DatabasePhase {
    Start,
    PhysicalTables,
    LogicalTables,
}

impl DatabasePhase {
    const fn as_event_value(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::PhysicalTables => "physical_tables",
            Self::LogicalTables => "logical_tables",
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ReconcileDatabasePhaseSummary {
    succeeded_table_count: usize,
    failed_table_count: usize,
    succeeded_subprocedure_count: usize,
    failed_subprocedure_count: usize,
}

impl ReconcileDatabasePhaseSummary {
    fn record(&mut self, metrics: &ReconcileDatabaseMetrics) {
        self.succeeded_table_count += metrics.succeeded_tables;
        self.failed_table_count += metrics.failed_tables;
        self.succeeded_subprocedure_count += metrics.succeeded_procedures;
        self.failed_subprocedure_count += metrics.failed_procedures;
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ReconcileDatabaseResultSummary {
    physical_tables: ReconcileDatabasePhaseSummary,
    logical_tables: ReconcileDatabasePhaseSummary,
    last_completed_phase: Option<DatabasePhase>,
}

impl ReconcileDatabaseResultSummary {
    fn record(&mut self, phase: DatabasePhase, metrics: &ReconcileDatabaseMetrics) {
        match phase {
            DatabasePhase::PhysicalTables => self.physical_tables.record(metrics),
            DatabasePhase::LogicalTables => self.logical_tables.record(metrics),
            DatabasePhase::Start => {}
        }
    }

    fn mark_phase_completed(&mut self, phase: DatabasePhase) {
        self.last_completed_phase = Some(phase);
    }

    fn reset_replayed_physical_tables(&mut self) {
        self.physical_tables = Default::default();
        self.logical_tables = Default::default();
        self.last_completed_phase = Some(DatabasePhase::Start);
    }

    fn reset_replayed_logical_tables(&mut self) {
        self.logical_tables = Default::default();
        self.last_completed_phase = Some(DatabasePhase::PhysicalTables);
    }

    fn succeeded_table_count(&self) -> usize {
        self.physical_tables.succeeded_table_count + self.logical_tables.succeeded_table_count
    }

    fn failed_table_count(&self) -> usize {
        self.physical_tables.failed_table_count + self.logical_tables.failed_table_count
    }

    fn succeeded_subprocedure_count(&self) -> usize {
        self.physical_tables.succeeded_subprocedure_count
            + self.logical_tables.succeeded_subprocedure_count
    }

    fn failed_subprocedure_count(&self) -> usize {
        self.physical_tables.failed_subprocedure_count
            + self.logical_tables.failed_subprocedure_count
    }

    fn last_completed_phase(&self) -> Option<&'static str> {
        self.last_completed_phase.map(DatabasePhase::as_event_value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    catalog: String,
    schema: String,
    fail_fast: bool,
    parallelism: usize,
    resolve_strategy: ResolveStrategy,
    is_subprocedure: bool,
    #[serde(default)]
    result_summary: ReconcileDatabaseResultSummary,
}

impl PersistentContext {
    pub fn new(
        catalog: String,
        schema: String,
        fail_fast: bool,
        parallelism: usize,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            catalog,
            schema,
            fail_fast,
            parallelism,
            resolve_strategy,
            is_subprocedure,
            result_summary: ReconcileDatabaseResultSummary::default(),
        }
    }
}

pub(crate) struct VolatileContext {
    /// Stores pending physical tables.
    pending_tables: Vec<(TableId, TableName)>,
    /// Stores pending logical tables associated with each physical table.
    ///
    /// - Key: Table ID of the physical table.
    /// - Value: Vector of (TableId, TableName) tuples representing logical tables belonging to the physical table.
    pending_logical_tables: HashMap<TableId, Vec<(TableId, TableName)>>,
    /// Stores inflight subprocedures.
    inflight_subprocedures: Vec<SubprocedureMeta>,
    /// Stores the stream of tables.
    tables: Option<BoxStream<'static, Result<(String, TableNameValue)>>>,
    /// The metrics of reconciling database.
    metrics: ReconcileDatabaseMetrics,
    /// The start time of the reconciliation.
    start_time: Instant,
}

impl Default for VolatileContext {
    fn default() -> Self {
        Self {
            pending_tables: vec![],
            pending_logical_tables: HashMap::new(),
            inflight_subprocedures: vec![],
            tables: None,
            metrics: ReconcileDatabaseMetrics::default(),
            start_time: Instant::now(),
        }
    }
}

pub struct ReconcileDatabaseProcedure {
    pub context: ReconcileDatabaseContext,
    state: Box<dyn State>,
}

impl ReconcileDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileDatabase";

    pub fn new(
        ctx: Context,
        catalog: String,
        schema: String,
        fail_fast: bool,
        parallelism: usize,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        let persistent_ctx = PersistentContext::new(
            catalog,
            schema,
            fail_fast,
            parallelism,
            resolve_strategy,
            is_subprocedure,
        );
        let context = ReconcileDatabaseContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconcileDatabaseStart);
        Self { context, state }
    }

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileDatabaseContext::new(ctx, persistent_ctx);
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
impl Procedure for ReconcileDatabaseProcedure {
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

    fn recover(&mut self) -> ProcedureResult<()> {
        use crate::reconciliation::reconcile_database::reconcile_logical_tables::ReconcileLogicalTables;
        use crate::reconciliation::reconcile_database::reconcile_tables::ReconcileTables;

        if self.state.as_any().is::<ReconcileDatabaseStart>() {
            self.context.persistent_ctx.result_summary = Default::default();
        } else if self.state.as_any().is::<ReconcileTables>() {
            self.context
                .persistent_ctx
                .result_summary
                .reset_replayed_physical_tables();
        } else if self.state.as_any().is::<ReconcileLogicalTables>() {
            self.context
                .persistent_ctx
                .result_summary
                .reset_replayed_logical_tables();
        }
        Ok(())
    }

    fn lock_key(&self) -> LockKey {
        let catalog = &self.context.persistent_ctx.catalog;
        let schema = &self.context.persistent_ctx.schema;
        // If the procedure is a subprocedure, only lock the schema.
        if self.context.persistent_ctx.is_subprocedure {
            return LockKey::new(vec![SchemaLock::write(catalog, schema).into()]);
        }

        LockKey::new(vec![
            CatalogLock::Read(catalog).into(),
            SchemaLock::write(catalog, schema).into(),
        ])
    }

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        if !ctx.event_type_filter.allows(RECONCILE_DATABASE_EVENT_TYPE) {
            return None;
        }

        let persistent_ctx = &self.context.persistent_ctx;
        let locator =
            ReconciliationLocator::database(&persistent_ctx.catalog, &persistent_ctx.schema);
        let event = match ctx.trigger {
            EventTrigger::Submitted => ReconciliationEvent::database_submitted(
                locator,
                persistent_ctx.resolve_strategy,
                persistent_ctx.fail_fast,
                persistent_ctx.parallelism,
                persistent_ctx.is_subprocedure,
            ),
            EventTrigger::Succeeded => Self::result_event(locator, persistent_ctx, true),
            EventTrigger::Failed | EventTrigger::Poisoned => {
                Self::result_event(locator, persistent_ctx, false)
            }
            _ => ReconciliationEvent::database_lifecycle(locator),
        };
        Some(Box::new(event))
    }
}

impl ReconcileDatabaseProcedure {
    fn result_event(
        locator: ReconciliationLocator,
        persistent_ctx: &PersistentContext,
        complete: bool,
    ) -> ReconciliationEvent {
        let summary = &persistent_ctx.result_summary;
        ReconciliationEvent::database_result(
            locator,
            complete,
            summary.succeeded_table_count(),
            summary.failed_table_count(),
            summary.succeeded_subprocedure_count(),
            summary.failed_subprocedure_count(),
            summary.last_completed_phase(),
        )
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_database_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileDatabaseContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use common_event_recorder::{EventTypeFilter, EventTypeFilterRef};
    use common_procedure::{
        ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
        RetryPhase,
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::reconciliation::event::RECONCILE_CATALOG_EVENT_TYPE;
    use crate::reconciliation::reconcile_database::reconcile_logical_tables::ReconcileLogicalTables;
    use crate::reconciliation::reconcile_database::reconcile_tables::ReconcileTables;
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    #[test]
    fn database_event_hook_covers_root_child_filters_and_lifecycle_payloads() {
        let root = test_procedure(false);
        let child = test_procedure(true);
        let running = ProcedureState::Running;
        for (procedure, is_subprocedure) in [(&root, false), (&child, true)] {
            let submitted =
                event_for(procedure, EventTrigger::Submitted, all_events(), &running).unwrap();
            assert_eq!(submitted.event_type(), RECONCILE_DATABASE_EVENT_TYPE);
            assert_eq!(
                submitted.json_payload().unwrap(),
                json!({
                    "version": 1,
                    "resolve_strategy": "use_metasrv",
                    "fail_fast": false,
                    "parallelism": 8,
                    "is_subprocedure": is_subprocedure,
                })
            );
        }

        let mut procedure = test_procedure(true);
        procedure.context.persistent_ctx.result_summary = ReconcileDatabaseResultSummary {
            physical_tables: ReconcileDatabasePhaseSummary {
                succeeded_table_count: 2,
                failed_table_count: 1,
                succeeded_subprocedure_count: 2,
                failed_subprocedure_count: 1,
            },
            logical_tables: ReconcileDatabasePhaseSummary {
                succeeded_table_count: 3,
                failed_table_count: 2,
                succeeded_subprocedure_count: 1,
                failed_subprocedure_count: 1,
            },
            last_completed_phase: Some(DatabasePhase::LogicalTables),
        };

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
                event_for(&procedure, trigger, all_events(), &running)
                    .unwrap()
                    .json_payload()
                    .unwrap(),
                Value::Null
            );
        }

        for (trigger, complete) in [
            (EventTrigger::Succeeded, true),
            (EventTrigger::Failed, false),
            (EventTrigger::Poisoned, false),
        ] {
            assert_eq!(
                event_for(&procedure, trigger, all_events(), &running)
                    .unwrap()
                    .json_payload()
                    .unwrap(),
                json!({
                    "version": 1,
                    "complete": complete,
                    "processed_table_count": 8,
                    "succeeded_table_count": 5,
                    "failed_table_count": 3,
                    "succeeded_subprocedure_count": 3,
                    "failed_subprocedure_count": 2,
                    "last_completed_phase": "logical_tables",
                })
            );
        }

        assert!(
            event_for(
                &procedure,
                EventTrigger::Submitted,
                selected_events(RECONCILE_DATABASE_EVENT_TYPE),
                &running,
            )
            .is_some()
        );
        assert!(
            event_for(
                &procedure,
                EventTrigger::Submitted,
                selected_events(RECONCILE_CATALOG_EVENT_TYPE),
                &running,
            )
            .is_none()
        );
        assert!(
            event_for(
                &procedure,
                EventTrigger::Submitted,
                Arc::new(EventTypeFilter::Only(HashSet::new())),
                &running,
            )
            .is_none()
        );
    }

    #[test]
    fn database_old_json_without_result_summary_still_deserializes() {
        let procedure = test_procedure(false);
        let mut value: Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
        value["persistent_ctx"]
            .as_object_mut()
            .unwrap()
            .remove("result_summary");

        let loaded = ReconcileDatabaseProcedure::from_json(
            test_context(),
            &serde_json::to_string(&value).unwrap(),
        )
        .unwrap();
        assert_eq!(
            event_for(
                &loaded,
                EventTrigger::Succeeded,
                all_events(),
                &ProcedureState::Running,
            )
            .unwrap()
            .json_payload()
            .unwrap(),
            json!({
                "version": 1,
                "complete": true,
                "processed_table_count": 0,
                "succeeded_table_count": 0,
                "failed_table_count": 0,
                "succeeded_subprocedure_count": 0,
                "failed_subprocedure_count": 0,
                "last_completed_phase": null,
            })
        );
    }

    #[test]
    fn database_recovery_resets_only_the_phase_that_will_be_replayed() {
        let mut procedure = test_procedure(false);
        procedure.context.persistent_ctx.result_summary = populated_summary();
        procedure.state = Box::new(ReconcileTables);
        procedure.recover().unwrap();
        assert_eq!(
            summary_counts(&procedure.context.persistent_ctx.result_summary),
            (0, 0, 0, 0)
        );
        assert!(matches!(
            procedure
                .context
                .persistent_ctx
                .result_summary
                .last_completed_phase,
            Some(DatabasePhase::Start)
        ));

        procedure.context.persistent_ctx.result_summary = populated_summary();
        procedure.state = Box::new(ReconcileLogicalTables);
        procedure.recover().unwrap();
        assert_eq!(
            summary_counts(&procedure.context.persistent_ctx.result_summary),
            (2, 1, 2, 1)
        );
        assert!(matches!(
            procedure
                .context
                .persistent_ctx
                .result_summary
                .last_completed_phase,
            Some(DatabasePhase::PhysicalTables)
        ));
    }

    fn populated_summary() -> ReconcileDatabaseResultSummary {
        ReconcileDatabaseResultSummary {
            physical_tables: ReconcileDatabasePhaseSummary {
                succeeded_table_count: 2,
                failed_table_count: 1,
                succeeded_subprocedure_count: 2,
                failed_subprocedure_count: 1,
            },
            logical_tables: ReconcileDatabasePhaseSummary {
                succeeded_table_count: 3,
                failed_table_count: 2,
                succeeded_subprocedure_count: 1,
                failed_subprocedure_count: 1,
            },
            last_completed_phase: Some(DatabasePhase::LogicalTables),
        }
    }

    fn summary_counts(summary: &ReconcileDatabaseResultSummary) -> (usize, usize, usize, usize) {
        (
            summary.succeeded_table_count(),
            summary.failed_table_count(),
            summary.succeeded_subprocedure_count(),
            summary.failed_subprocedure_count(),
        )
    }

    fn test_procedure(is_subprocedure: bool) -> ReconcileDatabaseProcedure {
        ReconcileDatabaseProcedure::new(
            test_context(),
            "greptime".to_string(),
            "public".to_string(),
            false,
            8,
            ResolveStrategy::UseMetasrv,
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

    fn event_for(
        procedure: &dyn Procedure,
        trigger: EventTrigger,
        event_type_filter: EventTypeFilterRef,
        lifecycle_state: &ProcedureState,
    ) -> Option<Box<dyn common_event_recorder::Event>> {
        procedure.event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state,
            trigger,
            event_type_filter,
            event_context: None,
        })
    }

    fn all_events() -> EventTypeFilterRef {
        Arc::new(EventTypeFilter::All)
    }

    fn selected_events(event_type: &str) -> EventTypeFilterRef {
        Arc::new(EventTypeFilter::Only(HashSet::from([
            event_type.to_string()
        ])))
    }
}
