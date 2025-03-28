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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use api::v1::flow::FlowResponse;
use common_error::ext::BoxedError;
use common_meta::ddl::create_flow::FlowType;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::key::table_info::TableInfoManager;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use itertools::Itertools;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::{RawTableMeta, TableId};
use table::requests::AUTO_CREATE_TABLE_KEY;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, RwLock};
use tokio::time::Instant;

use super::frontend_client::FrontendClient;
use crate::adapter::{CreateFlowArgs, FlowId, TableName, AUTO_CREATED_PLACEHOLDER_TS_COL};
use crate::error::{
    DatafusionSnafu, DatatypesSnafu, ExternalSnafu, FlowAlreadyExistSnafu, InternalSnafu,
    InvalidRequestSnafu, TableNotFoundMetaSnafu, TableNotFoundSnafu, TimeSnafu, UnexpectedSnafu,
};
use crate::metrics::{METRIC_FLOW_RULE_ENGINE_QUERY_TIME, METRIC_FLOW_RULE_ENGINE_SLOW_QUERY};
use crate::recording_rules::time_window::{find_time_window_expr, TimeWindowExpr};
use crate::recording_rules::utils::{
    df_plan_to_sql, sql_to_df_plan, AddAutoColumnRewriter, AddFilterRewriter, FindGroupByFinalName,
};
use crate::Error;

/// TODO(discord9): make those constants configurable
/// The default rule engine query timeout is 10 minutes
pub const DEFAULT_RULE_ENGINE_QUERY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// will output a warn log for any query that runs for more that 1 minutes, and also every 1 minutes when that query is still running
pub const SLOW_QUERY_THRESHOLD: Duration = Duration::from_secs(60);

/// The minimum duration between two queries execution by recording rule
const MIN_REFRESH_DURATION: Duration = Duration::new(5, 0);

#[derive(Clone)]
pub struct RecordingRuleTask {
    pub flow_id: FlowId,
    query: String,
    plan: LogicalPlan,
    pub time_window_expr: Option<TimeWindowExpr>,
    /// in seconds
    pub expire_after: Option<i64>,
    sink_table_name: [String; 3],
    source_table_names: HashSet<[String; 3]>,
    table_meta: TableMetadataManagerRef,
    state: Arc<RwLock<RecordingRuleState>>,
}

impl RecordingRuleTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        flow_id: FlowId,
        query: &str,
        plan: LogicalPlan,
        time_window_expr: Option<TimeWindowExpr>,
        expire_after: Option<i64>,
        sink_table_name: [String; 3],
        source_table_names: Vec<[String; 3]>,
        query_ctx: QueryContextRef,
        table_meta: TableMetadataManagerRef,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            flow_id,
            query: query.to_string(),
            plan,
            time_window_expr,
            expire_after,
            sink_table_name,
            source_table_names: source_table_names.into_iter().collect(),
            table_meta,
            state: Arc::new(RwLock::new(RecordingRuleState::new(query_ctx, shutdown_rx))),
        }
    }
}

#[derive(Debug)]
pub struct RecordingRuleState {
    query_ctx: QueryContextRef,
    /// last query complete time
    last_update_time: Instant,
    /// last time query duration
    last_query_duration: Duration,
    /// Dirty Time windows need to be updated
    /// mapping of `start -> end` and non-overlapping
    dirty_time_windows: DirtyTimeWindows,
    exec_state: ExecState,
    shutdown_rx: oneshot::Receiver<()>,
}
impl RecordingRuleState {
    pub fn new(query_ctx: QueryContextRef, shutdown_rx: oneshot::Receiver<()>) -> Self {
        Self {
            query_ctx,
            last_update_time: Instant::now(),
            last_query_duration: Duration::from_secs(0),
            dirty_time_windows: Default::default(),
            exec_state: ExecState::Idle,
            shutdown_rx,
        }
    }

    /// called after last query is done
    /// `is_succ` indicate whether the last query is successful
    pub fn after_query_exec(&mut self, elapsed: Duration, _is_succ: bool) {
        self.exec_state = ExecState::Idle;
        self.last_query_duration = elapsed;
        self.last_update_time = Instant::now();
    }

    /// wait for at least `last_query_duration`, at most `max_timeout` to start next query
    pub fn get_next_start_query_time(&self, max_timeout: Option<Duration>) -> Instant {
        let next_duration = max_timeout
            .unwrap_or(self.last_query_duration)
            .min(self.last_query_duration);
        let next_duration = next_duration.max(MIN_REFRESH_DURATION);

        self.last_update_time + next_duration
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirtyTimeWindows {
    windows: BTreeMap<Timestamp, Option<Timestamp>>,
}

impl DirtyTimeWindows {
    /// Time window merge distance
    const MERGE_DIST: i32 = 3;

    /// Maximum number of filters allowed in a single query
    const MAX_FILTER_NUM: usize = 20;

    /// Add lower bounds to the dirty time windows. Upper bounds are ignored.
    ///
    /// # Arguments
    ///
    /// * `lower_bounds` - An iterator of lower bounds to be added.
    pub fn add_lower_bounds(&mut self, lower_bounds: impl Iterator<Item = Timestamp>) {
        for lower_bound in lower_bounds {
            let entry = self.windows.entry(lower_bound);
            entry.or_insert(None);
        }
    }

    /// Generate all filter expressions consuming all time windows
    pub fn gen_filter_exprs(
        &mut self,
        col_name: &str,
        expire_lower_bound: Option<Timestamp>,
        window_size: chrono::Duration,
        task_ctx: &RecordingRuleTask,
    ) -> Result<Option<datafusion_expr::Expr>, Error> {
        debug!(
            "expire_lower_bound: {:?}, window_size: {:?}",
            expire_lower_bound.map(|t| t.to_iso8601_string()),
            window_size
        );
        self.merge_dirty_time_windows(window_size, expire_lower_bound)?;

        if self.windows.len() > Self::MAX_FILTER_NUM {
            let first_time_window = self.windows.first_key_value();
            let last_time_window = self.windows.last_key_value();
            warn!(
                "Flow id = {:?}, too many time windows: {}, only the first {} are taken for this query, the group by expression might be wrong. Time window expr={:?}, expire_after={:?}, first_time_window={:?}, last_time_window={:?}, the original query: {:?}",
                task_ctx.flow_id,
                self.windows.len(),
                Self::MAX_FILTER_NUM,
                task_ctx.time_window_expr,
                task_ctx.expire_after,
                first_time_window,
                last_time_window,
                task_ctx.query
            );
        }

        // get the first `MAX_FILTER_NUM` time windows
        let nth = self
            .windows
            .iter()
            .nth(Self::MAX_FILTER_NUM)
            .map(|(key, _)| *key);
        let first_nth = {
            if let Some(nth) = nth {
                let mut after = self.windows.split_off(&nth);
                std::mem::swap(&mut self.windows, &mut after);

                after
            } else {
                std::mem::take(&mut self.windows)
            }
        };

        let mut expr_lst = vec![];
        for (start, end) in first_nth.into_iter() {
            debug!(
                "Time window start: {:?}, end: {:?}",
                start.to_iso8601_string(),
                end.map(|t| t.to_iso8601_string())
            );

            use datafusion_expr::{col, lit};
            let lower = to_df_literal(start)?;
            let upper = end.map(to_df_literal).transpose()?;
            let expr = if let Some(upper) = upper {
                col(col_name)
                    .gt_eq(lit(lower))
                    .and(col(col_name).lt(lit(upper)))
            } else {
                col(col_name).gt_eq(lit(lower))
            };
            expr_lst.push(expr);
        }
        let expr = expr_lst.into_iter().reduce(|a, b| a.and(b));
        Ok(expr)
    }

    /// Merge time windows that overlaps or get too close
    pub fn merge_dirty_time_windows(
        &mut self,
        window_size: chrono::Duration,
        expire_lower_bound: Option<Timestamp>,
    ) -> Result<(), Error> {
        let mut new_windows = BTreeMap::new();

        let mut prev_tw = None;
        for (lower_bound, upper_bound) in std::mem::take(&mut self.windows) {
            // filter out expired time window
            if let Some(expire_lower_bound) = expire_lower_bound {
                if lower_bound < expire_lower_bound {
                    continue;
                }
            }

            let Some(prev_tw) = &mut prev_tw else {
                prev_tw = Some((lower_bound, upper_bound));
                continue;
            };

            let std_window_size = window_size.to_std().map_err(|e| {
                InternalSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;

            // if cur.lower - prev.upper <= window_size * 2, merge
            let prev_upper = prev_tw
                .1
                .unwrap_or(prev_tw.0.add_duration(std_window_size).context(TimeSnafu)?);
            prev_tw.1 = Some(prev_upper);

            let cur_upper = upper_bound.unwrap_or(
                lower_bound
                    .add_duration(std_window_size)
                    .context(TimeSnafu)?,
            );

            if lower_bound
                .sub(&prev_upper)
                .map(|dist| dist <= window_size * Self::MERGE_DIST)
                .unwrap_or(false)
            {
                prev_tw.1 = Some(cur_upper);
            } else {
                new_windows.insert(prev_tw.0, prev_tw.1);
                *prev_tw = (lower_bound, Some(cur_upper));
            }
        }

        if let Some(prev_tw) = prev_tw {
            new_windows.insert(prev_tw.0, prev_tw.1);
        }

        self.windows = new_windows;

        Ok(())
    }
}

fn to_df_literal(value: Timestamp) -> Result<datafusion_common::ScalarValue, Error> {
    let value = Value::from(value);
    let value = value
        .try_to_scalar_value(&value.data_type())
        .with_context(|_| DatatypesSnafu {
            extra: format!("Failed to convert to scalar value: {}", value),
        })?;
    Ok(value)
}

#[derive(Debug, Clone)]
enum ExecState {
    Idle,
    Executing,
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_merge_dirty_time_windows() {
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((1 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(chrono::Duration::seconds(5 * 60), None)
            .unwrap();
        // just enough to merge
        assert_eq!(
            dirty.windows,
            BTreeMap::from([(
                Timestamp::new_second(0),
                Some(Timestamp::new_second(
                    (2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60
                ))
            )])
        );

        // separate time window
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(chrono::Duration::seconds(5 * 60), None)
            .unwrap();
        // just enough to merge
        assert_eq!(
            BTreeMap::from([
                (
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(5 * 60))
                ),
                (
                    Timestamp::new_second((2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                    Some(Timestamp::new_second(
                        (3 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60
                    ))
                )
            ]),
            dirty.windows
        );

        // overlapping
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(chrono::Duration::seconds(5 * 60), None)
            .unwrap();
        // just enough to merge
        assert_eq!(
            BTreeMap::from([(
                Timestamp::new_second(0),
                Some(Timestamp::new_second(
                    (1 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60
                ))
            ),]),
            dirty.windows
        );

        // expired
        let mut dirty = DirtyTimeWindows::default();
        dirty.add_lower_bounds(
            vec![
                Timestamp::new_second(0),
                Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
            ]
            .into_iter(),
        );
        dirty
            .merge_dirty_time_windows(
                chrono::Duration::seconds(5 * 60),
                Some(Timestamp::new_second(
                    (DirtyTimeWindows::MERGE_DIST as i64) * 6 * 60,
                )),
            )
            .unwrap();
        // just enough to merge
        assert_eq!(BTreeMap::from([]), dirty.windows);
    }
}
