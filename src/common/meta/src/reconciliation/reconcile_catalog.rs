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

use std::any::Any;
use std::fmt::Debug;
use std::time::Instant;

use common_procedure::error::FromJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, EventContext, EventTrigger, LockKey,
    Procedure, Result as ProcedureResult, Status,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::TableMetadataManagerRef;
use crate::lock_key::CatalogLock;
use crate::metrics;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::event::{
    RECONCILE_CATALOG_EVENT_TYPE, ReconciliationEvent, ReconciliationLocator,
};
use crate::reconciliation::reconcile_catalog::start::ReconcileCatalogStart;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::{
    Context, ReconcileCatalogMetrics, SubprocedureMeta, wait_for_inflight_subprocedures,
};

pub(crate) mod end;
pub(crate) mod reconcile_databases;
pub(crate) mod start;

pub(crate) struct ReconcileCatalogContext {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    persistent_ctx: PersistentContext,
    volatile_ctx: VolatileContext,
}

impl ReconcileCatalogContext {
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            node_manager: ctx.node_manager,
            table_metadata_manager: ctx.table_metadata_manager,
            cache_invalidator: ctx.cache_invalidator,
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
        }
    }

    pub(crate) async fn wait_for_inflight_subprocedure(
        &mut self,
        procedure_ctx: &ProcedureContext,
    ) -> Result<()> {
        if let Some(subprocedure) = self.volatile_ctx.inflight_subprocedure.take() {
            let subprocedures = [subprocedure];
            let result = wait_for_inflight_subprocedures(
                procedure_ctx,
                &subprocedures,
                self.persistent_ctx.fast_fail,
            )
            .await?;
            let metrics: ReconcileCatalogMetrics = result.into();
            self.persistent_ctx.result_summary.record(&metrics);
            self.volatile_ctx.metrics += metrics;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CatalogPhase {
    Start,
    Databases,
}

impl CatalogPhase {
    const fn as_event_value(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Databases => "databases",
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ReconcileCatalogResultSummary {
    succeeded_database_count: usize,
    failed_database_count: usize,
    last_completed_phase: Option<CatalogPhase>,
}

impl ReconcileCatalogResultSummary {
    fn record(&mut self, metrics: &ReconcileCatalogMetrics) {
        self.succeeded_database_count += metrics.succeeded_databases;
        self.failed_database_count += metrics.failed_databases;
    }

    fn mark_phase_completed(&mut self, phase: CatalogPhase) {
        self.last_completed_phase = Some(phase);
    }

    fn reset_replayed_databases(&mut self) {
        self.succeeded_database_count = 0;
        self.failed_database_count = 0;
        self.last_completed_phase = Some(CatalogPhase::Start);
    }

    fn last_completed_phase(&self) -> Option<&'static str> {
        self.last_completed_phase.map(CatalogPhase::as_event_value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    catalog: String,
    fast_fail: bool,
    resolve_strategy: ResolveStrategy,
    parallelism: usize,
    #[serde(default)]
    result_summary: ReconcileCatalogResultSummary,
}

impl PersistentContext {
    pub fn new(
        catalog: String,
        fast_fail: bool,
        resolve_strategy: ResolveStrategy,
        parallelism: usize,
    ) -> Self {
        Self {
            catalog,
            fast_fail,
            resolve_strategy,
            parallelism,
            result_summary: ReconcileCatalogResultSummary::default(),
        }
    }
}

pub(crate) struct VolatileContext {
    /// Stores the stream of catalogs.
    schemas: Option<BoxStream<'static, Result<String>>>,
    /// Stores the inflight subprocedure.
    inflight_subprocedure: Option<SubprocedureMeta>,
    /// Stores the metrics of reconciling catalog.
    metrics: ReconcileCatalogMetrics,
    /// The start time of the reconciliation.
    start_time: Instant,
}

impl Default for VolatileContext {
    fn default() -> Self {
        Self {
            schemas: None,
            inflight_subprocedure: None,
            metrics: Default::default(),
            start_time: Instant::now(),
        }
    }
}

pub struct ReconcileCatalogProcedure {
    pub context: ReconcileCatalogContext,
    state: Box<dyn State>,
}

impl ReconcileCatalogProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileCatalog";

    pub fn new(
        ctx: Context,
        catalog: String,
        fast_fail: bool,
        resolve_strategy: ResolveStrategy,
        parallelism: usize,
    ) -> Self {
        let persistent_ctx =
            PersistentContext::new(catalog, fast_fail, resolve_strategy, parallelism);
        let context = ReconcileCatalogContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconcileCatalogStart);
        Self { context, state }
    }

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileCatalogContext::new(ctx, persistent_ctx);
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

#[async_trait::async_trait]
impl Procedure for ReconcileCatalogProcedure {
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
        serde_json::to_string(&data).context(FromJsonSnafu)
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        use crate::reconciliation::reconcile_catalog::reconcile_databases::ReconcileDatabases;

        if self.state.as_any().is::<ReconcileCatalogStart>() {
            self.context.persistent_ctx.result_summary = Default::default();
        } else if self.state.as_any().is::<ReconcileDatabases>() {
            self.context
                .persistent_ctx
                .result_summary
                .reset_replayed_databases();
        }
        Ok(())
    }

    fn lock_key(&self) -> LockKey {
        let catalog = &self.context.persistent_ctx.catalog;

        LockKey::new(vec![CatalogLock::Write(catalog).into()])
    }

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        if !ctx.event_type_filter.allows(RECONCILE_CATALOG_EVENT_TYPE) {
            return None;
        }

        let persistent_ctx = &self.context.persistent_ctx;
        let locator = ReconciliationLocator::catalog(&persistent_ctx.catalog);
        let event = match ctx.trigger {
            EventTrigger::Submitted => ReconciliationEvent::catalog_submitted(
                locator,
                persistent_ctx.resolve_strategy,
                persistent_ctx.fast_fail,
                persistent_ctx.parallelism,
            ),
            EventTrigger::Succeeded => Self::result_event(locator, persistent_ctx, true),
            EventTrigger::Failed | EventTrigger::Poisoned => {
                Self::result_event(locator, persistent_ctx, false)
            }
            _ => ReconciliationEvent::catalog_lifecycle(locator),
        };
        Some(Box::new(event))
    }
}

impl ReconcileCatalogProcedure {
    fn result_event(
        locator: ReconciliationLocator,
        persistent_ctx: &PersistentContext,
        complete: bool,
    ) -> ReconciliationEvent {
        let summary = &persistent_ctx.result_summary;
        ReconciliationEvent::catalog_result(
            locator,
            complete,
            summary.succeeded_database_count,
            summary.failed_database_count,
            summary.last_completed_phase(),
        )
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_catalog_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileCatalogContext,
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
    use crate::reconciliation::event::RECONCILE_DATABASE_EVENT_TYPE;
    use crate::reconciliation::reconcile_catalog::reconcile_databases::ReconcileDatabases;
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    #[test]
    fn catalog_event_hook_covers_filters_and_lifecycle_payloads() {
        let mut procedure = test_procedure();
        procedure.context.persistent_ctx.result_summary = ReconcileCatalogResultSummary {
            succeeded_database_count: 2,
            failed_database_count: 1,
            last_completed_phase: Some(CatalogPhase::Databases),
        };
        let running = ProcedureState::Running;

        let submitted =
            event_for(&procedure, EventTrigger::Submitted, all_events(), &running).unwrap();
        assert_eq!(submitted.event_type(), RECONCILE_CATALOG_EVENT_TYPE);
        assert_eq!(
            submitted.json_payload().unwrap(),
            json!({
                "version": 1,
                "resolve_strategy": "use_latest",
                "fail_fast": false,
                "parallelism": 8,
            })
        );

        for trigger in [
            EventTrigger::Recovered,
            EventTrigger::ChildSubmitted {
                procedure_id: ProcedureId::random(),
                outcome: ChildSubmissionOutcome::Accepted,
            },
            EventTrigger::Retrying {
                phase: RetryPhase::Execute,
                attempt: 1,
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
                    "processed_database_count": 3,
                    "succeeded_database_count": 2,
                    "failed_database_count": 1,
                    "last_completed_phase": "databases",
                })
            );
        }

        assert!(
            event_for(
                &procedure,
                EventTrigger::Submitted,
                selected_events(RECONCILE_CATALOG_EVENT_TYPE),
                &running,
            )
            .is_some()
        );
        assert!(
            event_for(
                &procedure,
                EventTrigger::Submitted,
                selected_events(RECONCILE_DATABASE_EVENT_TYPE),
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
    fn catalog_old_json_without_result_summary_still_deserializes() {
        let procedure = test_procedure();
        let mut value: Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
        value["persistent_ctx"]
            .as_object_mut()
            .unwrap()
            .remove("result_summary");

        let loaded = ReconcileCatalogProcedure::from_json(
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
                "processed_database_count": 0,
                "succeeded_database_count": 0,
                "failed_database_count": 0,
                "last_completed_phase": null,
            })
        );
    }

    #[test]
    fn catalog_recovery_resets_the_replayed_database_phase() {
        let mut procedure = test_procedure();
        procedure.state = Box::new(ReconcileDatabases);
        procedure.context.persistent_ctx.result_summary = ReconcileCatalogResultSummary {
            succeeded_database_count: 4,
            failed_database_count: 1,
            last_completed_phase: Some(CatalogPhase::Databases),
        };

        procedure.recover().unwrap();

        let summary = &procedure.context.persistent_ctx.result_summary;
        assert_eq!(summary.succeeded_database_count, 0);
        assert_eq!(summary.failed_database_count, 0);
        assert!(matches!(
            summary.last_completed_phase,
            Some(CatalogPhase::Start)
        ));
    }

    fn test_procedure() -> ReconcileCatalogProcedure {
        ReconcileCatalogProcedure::new(
            test_context(),
            "greptime".to_string(),
            false,
            ResolveStrategy::UseLatest,
            8,
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
