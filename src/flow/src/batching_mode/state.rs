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

//! Batching mode task state, which changes frequently

use std::collections::BTreeMap;
use std::time::Duration;

use common_telemetry::debug;
use common_telemetry::tracing::warn;
use common_time::Timestamp;
use datatypes::value::Value;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::batching_mode::task::BatchingTask;
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::batching_mode::MIN_REFRESH_DURATION;
use crate::error::{DatatypesSnafu, InternalSnafu, TimeSnafu, UnexpectedSnafu};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_SIZE,
    METRIC_FLOW_BATCHING_ENGINE_STALLED_WINDOW_SIZE,
};
use crate::{Error, FlowId};

/// The state of the [`BatchingTask`].
#[derive(Debug)]
pub struct TaskState {
    /// Query context
    pub(crate) query_ctx: QueryContextRef,
    /// last query complete time
    last_update_time: Instant,
    /// last time query duration
    last_query_duration: Duration,
    /// Dirty Time windows need to be updated
    /// mapping of `start -> end` and non-overlapping
    pub(crate) dirty_time_windows: DirtyTimeWindows,
    exec_state: ExecState,
    /// Shutdown receiver
    pub(crate) shutdown_rx: oneshot::Receiver<()>,
    /// Task handle
    pub(crate) task_handle: Option<tokio::task::JoinHandle<()>>,
}
impl TaskState {
    pub fn new(query_ctx: QueryContextRef, shutdown_rx: oneshot::Receiver<()>) -> Self {
        Self {
            query_ctx,
            last_update_time: Instant::now(),
            last_query_duration: Duration::from_secs(0),
            dirty_time_windows: Default::default(),
            exec_state: ExecState::Idle,
            shutdown_rx,
            task_handle: None,
        }
    }

    /// called after last query is done
    /// `is_succ` indicate whether the last query is successful
    pub fn after_query_exec(&mut self, elapsed: Duration, _is_succ: bool) {
        self.exec_state = ExecState::Idle;
        self.last_query_duration = elapsed;
        self.last_update_time = Instant::now();
    }

    /// Compute the next query delay based on the time window size or the last query duration.
    /// Aiming to avoid too frequent queries. But also not too long delay.
    ///
    /// next wait time is calculated as:
    /// last query duration, capped by [max(min_run_interval, time_window_size), max_timeout],
    /// note at most wait for `max_timeout`.
    ///
    /// if current the dirty time range is longer than one query can handle,
    /// execute immediately to faster clean up dirty time windows.
    ///
    pub fn get_next_start_query_time(
        &self,
        flow_id: FlowId,
        time_window_size: &Option<Duration>,
        max_timeout: Option<Duration>,
    ) -> Instant {
        // = last query duration, capped by [max(min_run_interval, time_window_size), max_timeout], note at most `max_timeout`
        let lower = time_window_size.unwrap_or(MIN_REFRESH_DURATION);
        let next_duration = self.last_query_duration.max(lower);
        let next_duration = if let Some(max_timeout) = max_timeout {
            next_duration.min(max_timeout)
        } else {
            next_duration
        };

        let cur_dirty_window_size = self.dirty_time_windows.window_size();
        // compute how much time range can be handled in one query
        let max_query_update_range = (*time_window_size)
            .unwrap_or_default()
            .mul_f64(DirtyTimeWindows::MAX_FILTER_NUM as f64);
        // if dirty time range is more than one query can handle, execute immediately
        // to faster clean up dirty time windows
        if cur_dirty_window_size < max_query_update_range {
            self.last_update_time + next_duration
        } else {
            // if dirty time windows can't be clean up in one query, execute immediately to faster
            // clean up dirty time windows
            debug!(
                "Flow id = {}, still have too many {} dirty time window({:?}), execute immediately",
                flow_id,
                self.dirty_time_windows.windows.len(),
                self.dirty_time_windows.windows
            );
            Instant::now()
        }
    }
}

/// For keep recording of dirty time windows, which is time window that have new data inserted
/// since last query.
#[derive(Debug, Clone, Default)]
pub struct DirtyTimeWindows {
    /// windows's `start -> end` and non-overlapping
    /// `end` is exclusive(and optional)
    windows: BTreeMap<Timestamp, Option<Timestamp>>,
}

impl DirtyTimeWindows {
    /// Time window merge distance
    ///
    /// TODO(discord9): make those configurable
    pub const MERGE_DIST: i32 = 3;

    /// Maximum number of filters allowed in a single query
    pub const MAX_FILTER_NUM: usize = 20;

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

    pub fn window_size(&self) -> Duration {
        let mut ret = Duration::from_secs(0);
        for (start, end) in &self.windows {
            if let Some(end) = end {
                if let Some(duration) = end.sub(start) {
                    ret += duration.to_std().unwrap_or_default();
                }
            }
        }
        ret
    }

    pub fn add_window(&mut self, start: Timestamp, end: Option<Timestamp>) {
        self.windows.insert(start, end);
    }

    /// Clean all dirty time windows, useful when can't found time window expr
    pub fn clean(&mut self) {
        self.windows.clear();
    }

    /// Number of dirty windows.
    pub fn len(&self) -> usize {
        self.windows.len()
    }

    /// Generate all filter expressions consuming all time windows
    ///
    /// there is two limits:
    /// - shouldn't return a too long time range(<=`window_size * window_cnt`), so that the query can be executed in a reasonable time
    /// - shouldn't return too many time range exprs, so that the query can be parsed properly instead of causing parser to overflow
    pub fn gen_filter_exprs(
        &mut self,
        col_name: &str,
        expire_lower_bound: Option<Timestamp>,
        window_size: chrono::Duration,
        window_cnt: usize,
        flow_id: FlowId,
        task_ctx: Option<&BatchingTask>,
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

            if let Some(task_ctx) = task_ctx {
                warn!(
                "Flow id = {:?}, too many time windows: {}, only the first {} are taken for this query, the group by expression might be wrong. Time window expr={:?}, expire_after={:?}, first_time_window={:?}, last_time_window={:?}, the original query: {:?}",
                task_ctx.config.flow_id,
                self.windows.len(),
                Self::MAX_FILTER_NUM,
                task_ctx.config.time_window_expr,
                task_ctx.config.expire_after,
                first_time_window,
                last_time_window,
                task_ctx.config.query
            );
            } else {
                warn!("Flow id = {:?}, too many time windows: {}, only the first {} are taken for this query, the group by expression might be wrong. first_time_window={:?}, last_time_window={:?}",
                flow_id,
                self.windows.len(),
                Self::MAX_FILTER_NUM,
                first_time_window,
                last_time_window
                )
            }
        }

        // get the first `window_cnt` time windows
        let max_time_range = window_size * window_cnt as i32;

        let mut to_be_query = BTreeMap::new();
        let mut new_windows = self.windows.clone();
        let mut cur_time_range = chrono::Duration::zero();
        for (idx, (start, end)) in self.windows.iter().enumerate() {
            let first_end = start
                .add_duration(window_size.to_std().unwrap())
                .context(TimeSnafu)?;
            let end = end.unwrap_or(first_end);

            // if time range is too long, stop
            if cur_time_range >= max_time_range {
                break;
            }

            // if we have enough time windows, stop
            if idx >= window_cnt {
                break;
            }

            let Some(x) = end.sub(start) else {
                continue;
            };
            if cur_time_range + x <= max_time_range {
                to_be_query.insert(*start, Some(end));
                new_windows.remove(start);
                cur_time_range += x;
            } else {
                // too large a window, split it
                // split at window_size * times
                let surplus = max_time_range - cur_time_range;
                if surplus.num_seconds() < window_size.num_seconds() {
                    // Skip splitting if surplus is smaller than window_size
                    break;
                }
                let times = surplus.num_seconds() / window_size.num_seconds();

                let split_offset = window_size * times as i32;
                let split_at = start
                    .add_duration(split_offset.to_std().unwrap())
                    .context(TimeSnafu)?;
                to_be_query.insert(*start, Some(split_at));

                // remove the original window
                new_windows.remove(start);
                new_windows.insert(split_at, Some(end));
                cur_time_range += split_offset;
                break;
            }
        }

        self.windows = new_windows;

        METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_CNT
            .with_label_values(&[flow_id.to_string().as_str()])
            .observe(to_be_query.len() as f64);

        let full_time_range = to_be_query
            .iter()
            .fold(chrono::Duration::zero(), |acc, (start, end)| {
                if let Some(end) = end {
                    acc + end.sub(start).unwrap_or(chrono::Duration::zero())
                } else {
                    acc
                }
            })
            .num_seconds() as f64;
        METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_SIZE
            .with_label_values(&[flow_id.to_string().as_str()])
            .observe(full_time_range);

        let stalled_time_range =
            self.windows
                .iter()
                .fold(chrono::Duration::zero(), |acc, (start, end)| {
                    if let Some(end) = end {
                        acc + end.sub(start).unwrap_or(chrono::Duration::zero())
                    } else {
                        acc
                    }
                });

        METRIC_FLOW_BATCHING_ENGINE_STALLED_WINDOW_SIZE
            .with_label_values(&[flow_id.to_string().as_str()])
            .observe(stalled_time_range.num_seconds() as f64);

        let mut expr_lst = vec![];
        for (start, end) in to_be_query.into_iter() {
            // align using time window exprs
            let (start, end) = if let Some(ctx) = task_ctx {
                let Some(time_window_expr) = &ctx.config.time_window_expr else {
                    UnexpectedSnafu {
                        reason: "time_window_expr is not set",
                    }
                    .fail()?
                };
                self.align_time_window(start, end, time_window_expr)?
            } else {
                (start, end)
            };
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
        let expr = expr_lst.into_iter().reduce(|a, b| a.or(b));
        Ok(expr)
    }

    fn align_time_window(
        &self,
        start: Timestamp,
        end: Option<Timestamp>,
        time_window_expr: &TimeWindowExpr,
    ) -> Result<(Timestamp, Option<Timestamp>), Error> {
        let align_start = time_window_expr.eval(start)?.0.context(UnexpectedSnafu {
            reason: format!(
                "Failed to align start time {:?} with time window expr {:?}",
                start, time_window_expr
            ),
        })?;
        let align_end = end
            .and_then(|end| {
                time_window_expr
                    .eval(end)
                    // if after aligned, end is the same, then use end(because it's already aligned) else use aligned end
                    .map(|r| if r.0 == Some(end) { r.0 } else { r.1 })
                    .transpose()
            })
            .transpose()?;
        Ok((align_start, align_end))
    }

    /// Merge time windows that overlaps or get too close
    ///
    /// TODO(discord9): not merge and prefer to send smaller time windows? how?
    pub fn merge_dirty_time_windows(
        &mut self,
        window_size: chrono::Duration,
        expire_lower_bound: Option<Timestamp>,
    ) -> Result<(), Error> {
        if self.windows.is_empty() {
            return Ok(());
        }

        let mut new_windows = BTreeMap::new();

        let std_window_size = window_size.to_std().map_err(|e| {
            InternalSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        // previous time window
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

            // if cur.lower - prev.upper <= window_size * MERGE_DIST, merge
            // this also deal with overlap windows because cur.lower > prev.lower is always true
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
    use session::context::QueryContext;

    use super::*;
    use crate::batching_mode::time_window::find_time_window_expr;
    use crate::batching_mode::utils::sql_to_df_plan;
    use crate::test_utils::create_test_query_engine;

    #[test]
    fn test_merge_dirty_time_windows() {
        let testcases = vec![
            // just enough to merge
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((1 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                ],
                (chrono::Duration::seconds(5 * 60), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(
                        (2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60,
                    )),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:25:00' AS TIMESTAMP)))",
                )
            ),
            // separate time window
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                ],
                (chrono::Duration::seconds(5 * 60), None),
                BTreeMap::from([
                    (
                        Timestamp::new_second(0),
                        Some(Timestamp::new_second(5 * 60)),
                    ),
                    (
                        Timestamp::new_second((2 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                        Some(Timestamp::new_second(
                            (3 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60,
                        )),
                    ),
                ]),
                Some(
                    "(((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:05:00' AS TIMESTAMP))) OR ((ts >= CAST('1970-01-01 00:25:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:30:00' AS TIMESTAMP))))",
                )
            ),
            // overlapping
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                ],
                (chrono::Duration::seconds(5 * 60), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(
                        (1 + DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60,
                    )),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:20:00' AS TIMESTAMP)))",
                )
            ),
            // complex overlapping
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 3),
                    Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 3 * 2),
                ],
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(
                        (DirtyTimeWindows::MERGE_DIST as i64) * 7
                    )),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:00:21' AS TIMESTAMP)))",
                )
            ),
            // split range
            (
                Vec::from_iter((0..20).map(|i|Timestamp::new_second(i*3)).chain(std::iter::once(
                    Timestamp::new_second(60 + 3 * (DirtyTimeWindows::MERGE_DIST as i64 + 1)),
                ))),
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([
                (
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(
                        60
                    )),
                ),
                (
                    Timestamp::new_second(60 + 3 * (DirtyTimeWindows::MERGE_DIST as i64 + 1)),
                    Some(Timestamp::new_second(
                        60 + 3 * (DirtyTimeWindows::MERGE_DIST as i64 + 1) + 3
                    )),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:01:00' AS TIMESTAMP)))",
                )
            ),
            // split 2 min into 1 min
            (
                Vec::from_iter((0..40).map(|i|Timestamp::new_second(i*3))),
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([
                (
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(
                        40 * 3
                    )),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:01:00' AS TIMESTAMP)))",
                )
            ),
            // split 3s + 1min into 3s + 57s
            (
                Vec::from_iter(std::iter::once(Timestamp::new_second(0)).chain((0..40).map(|i|Timestamp::new_second(20+i*3)))),
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([
                (
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(
                        3
                    )),
                ),(
                    Timestamp::new_second(20),
                    Some(Timestamp::new_second(
                        140
                    )),
                )]),
                Some(
                    "(((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:00:03' AS TIMESTAMP))) OR ((ts >= CAST('1970-01-01 00:00:20' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:01:17' AS TIMESTAMP))))",
                )
            ),
            // expired
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((DirtyTimeWindows::MERGE_DIST as i64) * 5 * 60),
                ],
                (
                    chrono::Duration::seconds(5 * 60),
                    Some(Timestamp::new_second(
                        (DirtyTimeWindows::MERGE_DIST as i64) * 6 * 60,
                    )),
                ),
                BTreeMap::from([]),
                None
            ),
        ];
        // let len = testcases.len();
        // let testcases = testcases[(len - 2)..(len - 1)].to_vec();
        for (lower_bounds, (window_size, expire_lower_bound), expected, expected_filter_expr) in
            testcases
        {
            let mut dirty = DirtyTimeWindows::default();
            dirty.add_lower_bounds(lower_bounds.into_iter());
            dirty
                .merge_dirty_time_windows(window_size, expire_lower_bound)
                .unwrap();
            assert_eq!(expected, dirty.windows);
            let filter_expr = dirty
                .gen_filter_exprs(
                    "ts",
                    expire_lower_bound,
                    window_size,
                    DirtyTimeWindows::MAX_FILTER_NUM,
                    0,
                    None,
                )
                .unwrap();

            let unparser = datafusion::sql::unparser::Unparser::default();
            let to_sql = filter_expr
                .as_ref()
                .map(|e| unparser.expr_to_sql(e).unwrap().to_string());
            assert_eq!(expected_filter_expr, to_sql.as_deref());
        }
    }

    #[tokio::test]
    async fn test_align_time_window() {
        type TimeWindow = (Timestamp, Option<Timestamp>);
        struct TestCase {
            sql: String,
            aligns: Vec<(TimeWindow, TimeWindow)>,
        }
        let testcases: Vec<TestCase> = vec![TestCase{
            sql: "SELECT date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window;".to_string(),
            aligns: vec![
                ((Timestamp::new_second(3), None), (Timestamp::new_second(0), None)),
                ((Timestamp::new_second(8), None), (Timestamp::new_second(5), None)),
                ((Timestamp::new_second(8), Some(Timestamp::new_second(10))), (Timestamp::new_second(5), Some(Timestamp::new_second(10)))),
                ((Timestamp::new_second(8), Some(Timestamp::new_second(9))), (Timestamp::new_second(5), Some(Timestamp::new_second(10)))),
            ],
        }];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for TestCase { sql, aligns } in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), &sql, true)
                .await
                .unwrap();

            let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
                &plan,
                query_engine.engine_state().catalog_manager().clone(),
                ctx.clone(),
            )
            .await
            .unwrap();

            let time_window_expr = time_window_expr
                .map(|expr| {
                    TimeWindowExpr::from_expr(
                        &expr,
                        &column_name,
                        &df_schema,
                        &query_engine.engine_state().session_state(),
                    )
                })
                .transpose()
                .unwrap()
                .unwrap();

            let dirty = DirtyTimeWindows::default();
            for (before_align, expected_after_align) in aligns {
                let after_align = dirty
                    .align_time_window(before_align.0, before_align.1, &time_window_expr)
                    .unwrap();
                assert_eq!(expected_after_align, after_align);
            }
        }
    }
}
