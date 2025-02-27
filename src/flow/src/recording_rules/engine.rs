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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common_meta::ddl::create_flow::FlowType;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use datafusion_common::tree_node::TreeNode;
use datatypes::value::Value;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, RwLock};
use tokio::time::Instant;

use super::frontend_client::FrontendClient;
use super::{df_plan_to_sql, AddFilterRewriter};
use crate::adapter::{CreateFlowArgs, FlowId};
use crate::error::{DatafusionSnafu, DatatypesSnafu, FlowAlreadyExistSnafu, UnexpectedSnafu};
use crate::metrics::{METRIC_FLOW_RULE_ENGINE_QUERY_TIME, METRIC_FLOW_RULE_ENGINE_SLOW_QUERY};
use crate::recording_rules::{find_plan_time_window_bound, sql_to_df_plan};
use crate::Error;

/// TODO(discord9): make those constants configurable
/// The default rule engine query timeout is 10 minutes
pub const DEFAULT_RULE_ENGINE_QUERY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// will output a warn log for any query that runs for more that 1 minutes, and also every 1 minutes when that query is still running
pub const SLOW_QUERY_THRESHOLD: Duration = Duration::from_secs(60);

/// TODO(discord9): determine how to configure refresh rate
pub struct RecordingRuleEngine {
    tasks: RwLock<BTreeMap<FlowId, RecordingRuleTask>>,
    shutdown_txs: RwLock<BTreeMap<FlowId, oneshot::Sender<()>>>,
    frontend_client: Arc<FrontendClient>,
    engine: QueryEngineRef,
}

impl RecordingRuleEngine {
    pub fn new(frontend_client: Arc<FrontendClient>, engine: QueryEngineRef) -> Self {
        Self {
            tasks: Default::default(),
            shutdown_txs: Default::default(),
            frontend_client,
            engine,
        }
    }
}

const MIN_REFRESH_DURATION: Duration = Duration::new(5, 0);

impl RecordingRuleEngine {
    pub async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids: _,
            create_if_not_exists,
            or_replace,
            expire_after,
            comment: _,
            sql,
            flow_options,
            query_ctx,
        } = args;

        // or replace logic
        {
            let is_exist = self.tasks.read().await.contains_key(&flow_id);
            match (create_if_not_exists, or_replace, is_exist) {
                // if replace, ignore that old flow exists
                (_, true, true) => {
                    info!("Replacing flow with id={}", flow_id);
                }
                (false, false, true) => FlowAlreadyExistSnafu { id: flow_id }.fail()?,
                // already exists, and not replace, return None
                (true, false, true) => {
                    info!("Flow with id={} already exists, do nothing", flow_id);
                    return Ok(None);
                }

                // continue as normal
                (_, _, false) => (),
            }
        }

        let flow_type = flow_options.get(FlowType::FLOW_TYPE_KEY);

        ensure!(
            flow_type == Some(&FlowType::RecordingRule.to_string()) || flow_type.is_none(),
            UnexpectedSnafu {
                reason: format!("Flow type is not RecordingRule nor None, got {flow_type:?}")
            }
        );

        let Some(query_ctx) = query_ctx else {
            UnexpectedSnafu {
                reason: "Query context is None".to_string(),
            }
            .fail()?
        };

        let (tx, rx) = oneshot::channel();
        let task = RecordingRuleTask::new(
            flow_id,
            &sql,
            expire_after,
            sink_table_name,
            Arc::new(query_ctx),
            rx,
        );

        let task_inner = task.clone();
        let engine = self.engine.clone();
        let frontend = self.frontend_client.clone();

        // TODO(discord9): also save handle & use time wheel or what for better
        let _handle = common_runtime::spawn_global(async move {
            match task_inner.start_executing(engine, frontend).await {
                Ok(()) => info!("Flow {} shutdown", task_inner.flow_id),
                Err(err) => common_telemetry::error!(
                    "Flow {} encounter unrecoverable error: {err:?}",
                    task_inner.flow_id
                ),
            }
        });

        // TODO(discord9): deal with replace logic
        let replaced_old_task_opt = self.tasks.write().await.insert(flow_id, task);
        drop(replaced_old_task_opt);

        self.shutdown_txs.write().await.insert(flow_id, tx);

        Ok(Some(flow_id))
    }

    pub async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        if self.tasks.write().await.remove(&flow_id).is_none() {
            warn!("Flow {flow_id} not found in tasks")
        }
        let Some(tx) = self.shutdown_txs.write().await.remove(&flow_id) else {
            UnexpectedSnafu {
                reason: format!("Can't found shutdown tx for flow {flow_id}"),
            }
            .fail()?
        };
        if tx.send(()).is_err() {
            warn!("Fail to shutdown flow {flow_id} due to receiver already dropped, maybe flow {flow_id} is already dropped?")
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RecordingRuleTask {
    flow_id: FlowId,
    query: String,
    /// in seconds
    expire_after: Option<i64>,
    sink_table_name: [String; 3],
    state: Arc<RwLock<RecordingRuleState>>,
}

impl RecordingRuleTask {
    pub fn new(
        flow_id: FlowId,
        query: &str,
        expire_after: Option<i64>,
        sink_table_name: [String; 3],
        query_ctx: QueryContextRef,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            flow_id,
            query: query.to_string(),
            expire_after,
            sink_table_name,
            state: Arc::new(RwLock::new(RecordingRuleState::new(query_ctx, shutdown_rx))),
        }
    }
}
impl RecordingRuleTask {
    /// This should be called in a new tokio task
    pub async fn start_executing(
        &self,
        engine: QueryEngineRef,
        frontend_client: Arc<FrontendClient>,
    ) -> Result<(), Error> {
        // only first query don't need upper bound
        let mut is_first = true;

        loop {
            // FIXME(discord9): test if need upper bound also works
            let new_query = self
                .gen_query_with_time_window(engine.clone(), false)
                .await?;

            let insert_into = format!(
                "INSERT INTO {}.{}.{} {}",
                self.sink_table_name[0],
                self.sink_table_name[1],
                self.sink_table_name[2],
                new_query
            );

            if is_first {
                is_first = false;
            }

            let instant = Instant::now();
            let flow_id = self.flow_id;
            let db_client = frontend_client.get_database_client().await?;
            let peer_addr = db_client.peer.addr;
            debug!(
                "Executing flow {flow_id}(expire_after={:?} secs) on {:?} with query {}",
                self.expire_after, peer_addr, &insert_into
            );

            let timer = METRIC_FLOW_RULE_ENGINE_QUERY_TIME
                .with_label_values(&[flow_id.to_string().as_str()])
                .start_timer();

            let res = db_client.database.sql(&insert_into).await;
            drop(timer);

            let elapsed = instant.elapsed();
            if let Ok(res1) = &res {
                debug!(
                    "Flow {flow_id} executed, result: {res1:?}, elapsed: {:?}",
                    elapsed
                );
            } else if let Err(res) = &res {
                warn!(
                    "Failed to execute Flow {flow_id} on frontend {}, result: {res:?}, elapsed: {:?} with query: {}",
                    peer_addr, elapsed, &insert_into
                );
            }

            // record slow query
            if elapsed >= SLOW_QUERY_THRESHOLD {
                warn!(
                    "Flow {flow_id} on frontend {} executed for {:?} before complete, query: {}",
                    peer_addr, elapsed, &insert_into
                );
                METRIC_FLOW_RULE_ENGINE_SLOW_QUERY
                    .with_label_values(&[flow_id.to_string().as_str(), &insert_into, &peer_addr])
                    .observe(elapsed.as_secs_f64());
            }

            self.state
                .write()
                .await
                .after_query_exec(elapsed, res.is_ok());

            let sleep_until = {
                let mut state = self.state.write().await;
                match state.shutdown_rx.try_recv() {
                    Ok(()) => break Ok(()),
                    Err(TryRecvError::Closed) => {
                        warn!("Unexpected shutdown flow {flow_id}, shutdown anyway");
                        break Ok(());
                    }
                    Err(TryRecvError::Empty) => (),
                }
                state.get_next_start_query_time(None)
            };
            tokio::time::sleep_until(sleep_until).await;
        }
    }

    async fn gen_query_with_time_window(
        &self,
        engine: QueryEngineRef,
        need_upper_bound: bool,
    ) -> Result<String, Error> {
        let query_ctx = self.state.read().await.query_ctx.clone();
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let low_bound = self
            .expire_after
            .map(|e| since_the_epoch.as_secs() - e as u64);

        let Some(low_bound) = low_bound else {
            return Ok(self.query.clone());
        };

        let low_bound = Timestamp::new_second(low_bound as i64);

        let plan = sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.query, true).await?;

        let (col_name, lower, upper) =
            find_plan_time_window_bound(&plan, low_bound, query_ctx.clone(), engine.clone())
                .await?;

        let new_sql = {
            let to_df_literal = |value| -> Result<_, Error> {
                let value = Value::from(value);
                let value = value
                    .try_to_scalar_value(&value.data_type())
                    .with_context(|_| DatatypesSnafu {
                        extra: format!("Failed to convert to scalar value: {}", value),
                    })?;
                Ok(value)
            };
            let lower = lower.map(to_df_literal).transpose()?;
            let upper = upper.map(to_df_literal).transpose()?.and_then(|u| {
                if need_upper_bound {
                    Some(u)
                } else {
                    None
                }
            });
            let expr = {
                use datafusion_expr::{col, lit};
                match (lower, upper) {
                    (Some(l), Some(u)) => col(&col_name)
                        .gt_eq(lit(l))
                        .and(col(&col_name).lt_eq(lit(u))),
                    (Some(l), None) => col(&col_name).gt_eq(lit(l)),
                    (None, Some(u)) => col(&col_name).lt(lit(u)),
                    // no time window, direct return
                    (None, None) => return Ok(self.query.clone()),
                }
            };

            let mut add_filter = AddFilterRewriter::new(expr);
            // make a not optimized plan for clearer unparse
            let plan =
                sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.query, false).await?;
            let plan = plan
                .clone()
                .rewrite(&mut add_filter)
                .with_context(|_| DatafusionSnafu {
                    context: format!("Failed to rewrite plan {plan:?}"),
                })?
                .data;
            df_plan_to_sql(&plan)?
        };

        Ok(new_sql)
    }
}

#[derive(Debug)]
pub struct RecordingRuleState {
    query_ctx: QueryContextRef,
    /// last query complete time
    last_update_time: Instant,
    /// last time query duration
    last_query_duration: Duration,
    exec_state: ExecState,
    shutdown_rx: oneshot::Receiver<()>,
}

impl RecordingRuleState {
    pub fn new(query_ctx: QueryContextRef, shutdown_rx: oneshot::Receiver<()>) -> Self {
        Self {
            query_ctx,
            last_update_time: Instant::now(),
            last_query_duration: Duration::from_secs(0),
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

#[derive(Debug, Clone)]
enum ExecState {
    Idle,
    Executing,
}
