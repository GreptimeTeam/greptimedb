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

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::debug;
use common_telemetry::tracing::warn;
use datafusion_expr::{DmlStatement, LogicalPlan};
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY,
    FLOW_SINK_TABLE_ID,
};
use snafu::ResultExt;
use table::metadata::TableId;

use crate::Error;
use crate::batching_mode::state::CheckpointMode;
use crate::batching_mode::table_creator::QueryType;
use crate::batching_mode::task::BatchingTask;
use crate::batching_mode::utils::{
    analyze_incremental_aggregate_plan, get_table_info_df_schema,
    rewrite_incremental_aggregate_with_sink_merge,
};
use crate::error::{ExternalSnafu, UnexpectedSnafu};

impl BatchingTask {
    async fn sink_table_id(&self) -> Result<TableId, Error> {
        let table = self
            .config
            .catalog_manager
            .table(
                &self.config.sink_table_name[0],
                &self.config.sink_table_name[1],
                &self.config.sink_table_name[2],
                None,
            )
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?
            .ok_or_else(|| {
                UnexpectedSnafu {
                    reason: format!(
                        "Flow {} cannot build incremental extensions because sink table {:?} was not found",
                        self.config.flow_id, self.config.sink_table_name
                    ),
                }
                .build()
            })?;
        Ok(table.table_info().table_id())
    }

    /// For incremental-mode SQL queries, attempt to prepare an executable plan
    /// that is safe for incremental scan extensions.
    ///
    /// Returns `Some(plan)` when incremental extensions are safe, and `None`
    /// when the caller should execute the original plan without incremental
    /// extensions. The returned plan may be either a rewritten
    /// delta-LEFT-JOIN-sink merge plan or the original plan. In particular,
    /// plain GROUP BY queries with no aggregate merge columns are incremental
    /// safe without a rewrite, so they return `Some(original_plan)`.
    pub(super) async fn prepare_plan_for_incremental(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>, Error> {
        let is_incremental_sql = {
            let state = self.state.read().unwrap();
            if state.is_incremental_disabled() {
                return Ok(None);
            }
            state.checkpoint_mode() == CheckpointMode::Incremental
                && matches!(self.config.query_type, QueryType::Sql)
        };

        if !is_incremental_sql {
            return Ok(None);
        }

        // Extract inner query plan from the DML wrapper.
        // Non-DML or non-SQL plans bypass the rewrite and keep checkpoint mode;
        // non-aggregate TQL or non-INSERT plans do not need incremental scan extensions.
        let inner_plan = match plan {
            LogicalPlan::Dml(dml) => dml.input.as_ref().clone(),
            _ => return Ok(None),
        };

        // Analyze the plan for incremental rewritability.
        // Incremental reads currently require aggregate / group-by plans that
        // can be rewritten into a delta-left-join-sink merge. Non-aggregate SQL
        // (projection, filter, or other non-aggregate shapes) stays full-snapshot
        // until separately supported, and incremental mode is permanently
        // disabled for this flow.
        let Some(analysis) = analyze_incremental_aggregate_plan(&inner_plan)? else {
            warn!(
                "Flow {} incremental mode but plan is not an aggregate query; \
                 permanently disabling incremental for this flow",
                self.config.flow_id
            );
            self.state.write().unwrap().disable_incremental();
            return Ok(None);
        };

        if !analysis.unsupported_exprs.is_empty() {
            warn!(
                "Flow {} incremental aggregate contains unsupported expressions {:?}; \
                 permanently disabling incremental for this flow",
                self.config.flow_id, analysis.unsupported_exprs
            );
            self.state.write().unwrap().disable_incremental();
            return Ok(None);
        }

        // Plain GROUP BY without aggregate expressions has no values to
        // merge between delta and sink. The incremental delta scan emits
        // changed groups, and sink primary-key write semantics make this
        // idempotent; no explicit left-join rewrite is needed.
        if analysis.merge_columns.is_empty() {
            return Ok(Some(plan.clone()));
        }

        // Fetch sink table for the merge rewrite.
        // Transient errors (catalog, schema, filter, or rewrite) should not
        // permanently disable incremental mode. Instead, we fall back to a
        // full-snapshot plan for this round while keeping incremental retryable.
        let sink_table = match get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await
        {
            Ok((table, _)) => table,
            Err(err) => {
                warn!(
                    "Flow {} failed to fetch sink table for incremental rewrite; \
                     falling back to full snapshot for this round: {:?}",
                    self.config.flow_id, err
                );
                self.state.write().unwrap().mark_full_snapshot();
                return Ok(None);
            }
        };
        let rewritten_inner = match rewrite_incremental_aggregate_with_sink_merge(
            &inner_plan,
            &analysis,
            sink_table,
            &self.config.sink_table_name,
            None,
        )
        .await
        {
            Ok(plan) => plan,
            Err(err) => {
                warn!(
                    "Flow {} failed to rewrite incremental aggregate with sink merge; \
                     falling back to full snapshot for this round: {:?}",
                    self.config.flow_id, err
                );
                self.state.write().unwrap().mark_full_snapshot();
                return Ok(None);
            }
        };

        // Reconstruct DML plan with the rewritten inner plan
        let rewritten = match plan {
            LogicalPlan::Dml(dml) => LogicalPlan::Dml(DmlStatement::new(
                dml.table_name.clone(),
                dml.target.clone(),
                dml.op.clone(),
                Arc::new(rewritten_inner),
            )),
            _ => unreachable!("already matched Dml above"),
        };

        debug!(
            "Flow {} rewrote incremental SQL aggregate query with sink merge",
            self.config.flow_id
        );

        Ok(Some(rewritten))
    }

    pub(super) async fn build_flow_query_extensions(
        &self,
        incremental_safe: bool,
        can_advance_checkpoints: bool,
    ) -> Result<Vec<(&'static str, String)>, Error> {
        let mut extensions = vec![("flow.return_region_seq", "true".to_string())];

        let incremental_checkpoints_json = {
            let state = self.state.read().unwrap();
            if incremental_safe
                && can_advance_checkpoints
                && !state.is_incremental_disabled()
                && state.checkpoint_mode() == CheckpointMode::Incremental
                && !state.checkpoints().is_empty()
            {
                Some(serde_json::to_string(state.checkpoints()).map_err(|err| {
                    UnexpectedSnafu {
                        reason: format!("Failed to serialize checkpoint map: {err}"),
                    }
                    .build()
                })?)
            } else {
                None
            }
        };

        if let Some(checkpoints_json) = incremental_checkpoints_json {
            let sink_table_id = self.sink_table_id().await?;
            extensions.push((FLOW_SINK_TABLE_ID, sink_table_id.to_string()));
            extensions.push((
                FLOW_INCREMENTAL_MODE,
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ));
            extensions.push((FLOW_INCREMENTAL_AFTER_SEQS, checkpoints_json));
        }

        Ok(extensions)
    }
}
