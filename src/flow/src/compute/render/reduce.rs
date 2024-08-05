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
use std::ops::Range;

use datatypes::data_type::ConcreteDataType;
use datatypes::value::{ListValue, Value};
use hydroflow::scheduled::graph_ext::GraphExt;
use hydroflow::scheduled::port::{PortCtx, SEND};
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};

use crate::compute::render::{Context, SubgraphArg};
use crate::compute::state::Scheduler;
use crate::compute::types::{Arranged, Collection, CollectionBundle, ErrCollector, Toff};
use crate::error::{Error, PlanSnafu};
use crate::expr::error::{DataAlreadyExpiredSnafu, DataTypeSnafu, InternalSnafu};
use crate::expr::{AggregateExpr, EvalError, ScalarExpr};
use crate::plan::{AccumulablePlan, AggrWithIndex, KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{self, DiffRow, KeyValDiffRow, RelationType, Row};
use crate::utils::{ArrangeHandler, ArrangeReader, ArrangeWriter, KeyExpiryManager};

impl<'referred, 'df> Context<'referred, 'df> {
    const REDUCE: &'static str = "reduce";
    /// render `Plan::Reduce` into executable dataflow
    // There is a false positive in using `Vec<ScalarExpr>` as key due to `Value` have `bytes` variant
    #[allow(clippy::mutable_key_type)]
    pub fn render_reduce(
        &mut self,
        input: Box<TypedPlan>,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
        output_type: RelationType,
    ) -> Result<CollectionBundle, Error> {
        let input = self.render_plan(*input)?;
        // first assembly key&val that's ((Row, Row), tick, diff)
        // Then stream kvs through a reduce operator

        // the output is concat from key and val
        let output_key_arity = key_val_plan.key_plan.output_arity();

        // TODO(discord9): config global expire time from self
        let arrange_handler = self.compute_state.new_arrange(None);

        if let (Some(time_index), Some(expire_after)) =
            (output_type.time_index, self.compute_state.expire_after())
        {
            let expire_man =
                KeyExpiryManager::new(Some(expire_after), Some(ScalarExpr::Column(time_index)));
            arrange_handler.write().set_expire_state(expire_man);
        }

        // reduce need full arrangement to be able to query all keys
        let arrange_handler_inner = arrange_handler.clone_full_arrange().context(PlanSnafu {
            reason: "No write is expected at this point",
        })?;

        let distinct_input = self.add_accum_distinct_input_arrange(&reduce_plan);

        let reduce_arrange = ReduceArrange {
            output_arrange: arrange_handler_inner,
            distinct_input,
        };

        let now = self.compute_state.current_time_ref();

        let err_collector = self.err_collector.clone();

        // TODO(discord9): better way to schedule future run
        let scheduler = self.compute_state.get_scheduler();
        let scheduler_inner = scheduler.clone();

        let (out_send_port, out_recv_port) = self.df.make_edge::<_, Toff>(Self::REDUCE);

        let subgraph = self.df.add_subgraph_in_out(
            Self::REDUCE,
            input.collection.into_inner(),
            out_send_port,
            move |_ctx, recv, send| {
                // mfp only need to passively receive updates from recvs
                let data = recv
                    .take_inner()
                    .into_iter()
                    .flat_map(|v| v.into_iter())
                    .collect_vec();

                reduce_subgraph(
                    &reduce_arrange,
                    data,
                    &key_val_plan,
                    &reduce_plan,
                    SubgraphArg {
                        now: *now.borrow(),
                        err_collector: &err_collector,
                        scheduler: &scheduler_inner,
                        send,
                    },
                );
            },
        );

        scheduler.set_cur_subgraph(subgraph);

        // by default the key of output arrange
        let arranged = BTreeMap::from([(
            (0..output_key_arity).map(ScalarExpr::Column).collect_vec(),
            Arranged::new(arrange_handler),
        )]);

        let bundle = CollectionBundle {
            collection: Collection::from_port(out_recv_port),
            arranged,
        };
        Ok(bundle)
    }

    /// Contrast to it name, it's for adding distinct input for
    /// accumulable reduce plan with distinct input,
    /// like `select COUNT(DISTINCT col) from table`
    ///
    /// The return value is optional a list of arrangement, which is created for distinct input, and should be the
    /// same length as the distinct aggregation in accumulable reduce plan
    fn add_accum_distinct_input_arrange(
        &mut self,
        reduce_plan: &ReducePlan,
    ) -> Option<Vec<ArrangeHandler>> {
        match reduce_plan {
            ReducePlan::Distinct => None,
            ReducePlan::Accumulable(AccumulablePlan { distinct_aggrs, .. }) => {
                (!distinct_aggrs.is_empty()).then(|| {
                    std::iter::repeat_with(|| {
                        let arr = self.compute_state.new_arrange(None);
                        arr.set_full_arrangement(true);
                        arr
                    })
                    .take(distinct_aggrs.len())
                    .collect()
                })
            }
        }
    }
}

/// All arrange(aka state) used in reduce operator
pub struct ReduceArrange {
    /// The output arrange of reduce operator
    output_arrange: ArrangeHandler,
    /// The distinct input arrangement for accumulable reduce plan
    /// only used when accumulable reduce plan has distinct aggregation
    distinct_input: Option<Vec<ArrangeHandler>>,
}

/// split a row into key and val by evaluate the key and val plan
fn split_row_to_key_val(
    row: Row,
    sys_time: repr::Timestamp,
    diff: repr::Diff,
    key_val_plan: &KeyValPlan,
    row_buf: &mut Row,
) -> Result<Option<KeyValDiffRow>, EvalError> {
    if let Some(key) = key_val_plan
        .key_plan
        .evaluate_into(&mut row.inner.clone(), row_buf)?
    {
        // val_plan is not supported to carry any filter predicate,
        let val = key_val_plan
            .val_plan
            .evaluate_into(&mut row.inner.clone(), row_buf)?
            .context(InternalSnafu {
                reason: "val_plan should not contain any filter predicate",
            })?;
        Ok(Some(((key, val), sys_time, diff)))
    } else {
        Ok(None)
    }
}

/// split a row into key and val by evaluate the key and val plan
fn batch_split_rows_to_key_val(
    rows: impl IntoIterator<Item = DiffRow>,
    key_val_plan: KeyValPlan,
    err_collector: ErrCollector,
) -> impl IntoIterator<Item = KeyValDiffRow> {
    let mut row_buf = Row::new(vec![]);
    rows.into_iter().filter_map(
        move |(mut row, sys_time, diff): DiffRow| -> Option<KeyValDiffRow> {
            err_collector.run(|| {
                let len = row.len();
                if let Some(key) = key_val_plan
                    .key_plan
                    .evaluate_into(&mut row.inner, &mut row_buf)?
                {
                    // reuse the row as buffer
                    row.inner.resize(len, Value::Null);
                    // val_plan is not supported to carry any filter predicate,
                    let val = key_val_plan
                        .val_plan
                        .evaluate_into(&mut row.inner, &mut row_buf)?
                        .context(InternalSnafu {
                            reason: "val_plan should not contain any filter predicate",
                        })?;
                    Ok(Some(((key, val), sys_time, diff)))
                } else {
                    Ok(None)
                }
            })?
        },
    )
}

/// reduce subgraph, reduce the input data into a single row
/// output is concat from key and val
fn reduce_subgraph(
    ReduceArrange {
        output_arrange: arrange,
        distinct_input,
    }: &ReduceArrange,
    data: impl IntoIterator<Item = DiffRow>,
    key_val_plan: &KeyValPlan,
    reduce_plan: &ReducePlan,
    SubgraphArg {
        now,
        err_collector,
        scheduler,
        send,
    }: SubgraphArg,
) {
    let key_val = batch_split_rows_to_key_val(data, key_val_plan.clone(), err_collector.clone());
    // from here for distinct reduce and accum reduce, things are drastically different
    // for distinct reduce the arrange store the output,
    // but for accum reduce the arrange store the accum state, and output is
    // evaluated from the accum state if there is need to update
    match reduce_plan {
        ReducePlan::Distinct => reduce_distinct_subgraph(
            arrange,
            key_val,
            SubgraphArg {
                now,
                err_collector,
                scheduler,
                send,
            },
        ),
        ReducePlan::Accumulable(accum_plan) => reduce_accum_subgraph(
            arrange,
            distinct_input,
            key_val,
            accum_plan,
            SubgraphArg {
                now,
                err_collector,
                scheduler,
                send,
            },
        ),
    };
}

/// return distinct rows(distinct by row's key) from the input, but do not update the arrangement
///
/// if the same key already exist, we only preserve the oldest value(It make sense for distinct input over key)
fn eval_distinct_core(
    arrange: ArrangeReader,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
) -> Vec<KeyValDiffRow> {
    let _ = err_collector;

    // note that we also need to keep track of the distinct rows inside the current input
    // hence the `inner_map` to keeping track of the distinct rows
    let mut inner_map = BTreeMap::new();
    kv.into_iter()
        .filter_map(|((key, val), ts, diff)| {
            // first check inner_map, then check the arrangement to make sure getting the newest value
            let old_val = inner_map
                .get(&key)
                .cloned()
                .or_else(|| arrange.get(now, &key));

            let new_key_val = match (old_val, diff) {
                // a new distinct row
                (None, 1) => Some(((key, val), ts, diff)),
                // if diff from newest value, also do update
                (Some(old_val), diff) if old_val.0 == val && old_val.2 != diff => {
                    Some(((key, val), ts, diff))
                }
                _ => None,
            };

            if let Some(((k, v), t, d)) = new_key_val.clone() {
                // update the inner_map, so later updates can be checked against it
                inner_map.insert(k, (v, t, d));
            }
            new_key_val
        })
        .collect_vec()
}

/// eval distinct reduce plan, output the distinct, and update the arrangement
///
/// This function is extracted because also want to use it to update distinct input of accumulable reduce plan
fn update_reduce_distinct_arrange(
    arrange: &ArrangeHandler,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
) -> impl Iterator<Item = DiffRow> {
    let result_updates = eval_distinct_core(arrange.read(), kv, now, err_collector);

    err_collector.run(|| {
        arrange.write().apply_updates(now, result_updates)?;
        Ok(())
    });

    // Deal with output:

    // 1. Read all updates that were emitted between the last time this arrangement had updates and the current time.
    let from = arrange.read().last_compaction_time();
    let from = from.unwrap_or(repr::Timestamp::MIN);
    let range = (
        std::ops::Bound::Excluded(from),
        std::ops::Bound::Included(now),
    );
    let output_kv = arrange.read().get_updates_in_range(range);

    // 2. Truncate all updates stored in arrangement within that range.
    let run_compaction = || {
        arrange.write().compact_to(now)?;
        Ok(())
    };
    err_collector.run(run_compaction);

    // 3. Output the updates.
    // output is concat from key and val
    output_kv.into_iter().map(|((mut key, v), ts, diff)| {
        key.extend(v.into_iter());
        (key, ts, diff)
    })
}

/// eval distinct reduce plan, output the distinct, and update the arrangement
///
/// invariant: it'is assumed `kv`'s time is always <= now,
/// since it's from a Collection Bundle, where future inserts are stored in arrange
fn reduce_distinct_subgraph(
    arrange: &ArrangeHandler,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    SubgraphArg {
        now,
        err_collector,
        scheduler: _,
        send,
    }: SubgraphArg,
) {
    let ret = update_reduce_distinct_arrange(arrange, kv, now, err_collector).collect_vec();

    // no future updates should exist here
    if arrange.read().get_next_update_time(&now).is_some() {
        err_collector.push_err(
            InternalSnafu {
                reason: "No future updates should exist in the reduce distinct arrangement",
            }
            .build(),
        );
    }

    send.give(ret);
}

/// eval accumulable reduce plan by eval aggregate function and reduce the result
///
/// TODO(discord9): eval distinct by adding distinct input arrangement
///
/// invariant: it'is assumed `kv`'s time is always <= now,
/// since it's from a Collection Bundle, where future inserts are stored in arrange
///
/// the data being send is just new rows that represent the new output after given input is processed
///
/// i.e: for example before new updates comes in, the output of query `SELECT sum(number), count(number) FROM table`
/// is (10,2(), and after new updates comes in, the output is (15,3), then the new row being send is ((15, 3), now, 1)
///
/// while it will also update key -> accums's value, for example if it is empty before, it will become something like
/// |offset| accum for sum | accum for count |
/// where offset is a single value holding the end offset of each accumulator
/// and the rest is the actual accumulator values which could be multiple values
fn reduce_accum_subgraph(
    arrange: &ArrangeHandler,
    distinct_input: &Option<Vec<ArrangeHandler>>,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    accum_plan: &AccumulablePlan,
    SubgraphArg {
        now,
        err_collector,
        scheduler,
        send,
    }: SubgraphArg,
) {
    let AccumulablePlan {
        full_aggrs,
        simple_aggrs,
        distinct_aggrs,
    } = accum_plan;
    let mut key_to_vals = BTreeMap::<Row, Vec<(Row, repr::Diff)>>::new();

    for ((key, val), _tick, diff) in kv {
        // it is assumed that value is in order of insertion
        let vals = key_to_vals.entry(key).or_default();
        vals.push((val, diff));
    }

    let mut all_updates = Vec::with_capacity(key_to_vals.len());
    let mut all_outputs = Vec::with_capacity(key_to_vals.len());
    // lock the arrange for write for the rest of function body
    // so to prevent wired race condition since we are going to update the arrangement by write after read
    // TODO(discord9): consider key-based lock
    let mut arrange = arrange.write();
    for (key, value_diffs) in key_to_vals {
        if let Some(expire_man) = &arrange.get_expire_state() {
            let mut is_expired = false;
            err_collector.run(|| {
                if let Some(expired) = expire_man.get_expire_duration(now, &key)? {
                    is_expired = true;
                    // expired data is ignored in computation, and a simple warning is logged
                    common_telemetry::warn!(
                        "Data already expired: {}",
                        DataAlreadyExpiredSnafu {
                            expired_by: expired,
                        }
                        .build()
                    );
                    Ok(())
                } else {
                    Ok(())
                }
            });
            if is_expired {
                // errors already collected, we can just continue to next key
                continue;
            }
        }
        let col_diffs = {
            let row_len = value_diffs[0].0.len();
            let res = err_collector.run(|| get_col_diffs(value_diffs, row_len));
            match res {
                Some(res) => res,
                // TODO(discord9): consider better error handling other than
                // just skip the row and logging error
                None => continue,
            }
        };
        let (accums, _, _) = arrange.get(now, &key).unwrap_or_default();

        let accums = accums.inner;

        // deser accums from offsets
        let accum_ranges = {
            let res = err_collector
                .run(|| from_val_to_slice_idx(accums.first().cloned(), full_aggrs.len()));
            if let Some(res) = res {
                res
            } else {
                // errors is collected, we can just continue and send error back through `err_collector`
                continue;
            }
        };

        let mut accum_output = AccumOutput::new();
        eval_simple_aggrs(
            simple_aggrs,
            &accums,
            &accum_ranges,
            &col_diffs,
            &mut accum_output,
            err_collector,
        );

        // for distinct input
        eval_distinct_aggrs(
            distinct_aggrs,
            distinct_input,
            &accums,
            &accum_ranges,
            &col_diffs,
            &mut accum_output,
            SubgraphArg {
                now,
                err_collector,
                scheduler,
                send,
            },
        );

        // get and append results
        err_collector.run(|| {
            let (new_accums, res_val_row) = accum_output.into_accum_output()?;

            // construct the updates and save it
            all_updates.push(((key.clone(), Row::new(new_accums)), now, 1));
            let mut key_val = key;
            key_val.extend(res_val_row);
            all_outputs.push((key_val, now, 1));
            Ok(())
        });
    }
    err_collector.run(|| {
        arrange.apply_updates(now, all_updates)?;
        arrange.compact_to(now)
    });

    // for all arranges involved, schedule next time this subgraph should run
    // no future updates should exist here
    let all_arrange_used = distinct_input
        .iter()
        .flatten()
        .map(|d| d.write())
        .chain(std::iter::once(arrange));
    check_no_future_updates(all_arrange_used, err_collector, now);

    send.give(all_outputs);
}

fn get_col_diffs(
    value_diffs: Vec<(Row, repr::Diff)>,
    row_len: usize,
) -> Result<Vec<Vec<(Value, i64)>>, EvalError> {
    ensure!(
        value_diffs.iter().all(|(row, _)| row.len() == row_len),
        InternalSnafu {
            reason: "value_diffs should have rows with equal length"
        }
    );
    let ret = (0..row_len)
        .map(|i| {
            value_diffs
                .iter()
                .map(|(row, diff)| (row.get(i).cloned().unwrap(), *diff))
                .collect_vec()
        })
        .collect_vec();
    Ok(ret)
}

/// Eval simple aggregate functions with no distinct input
fn eval_simple_aggrs(
    simple_aggrs: &Vec<AggrWithIndex>,
    accums: &[Value],
    accum_ranges: &[Range<usize>],
    col_diffs: &[Vec<(Value, i64)>],
    accum_output: &mut AccumOutput,
    err_collector: &ErrCollector,
) {
    for AggrWithIndex {
        expr,
        input_idx,
        output_idx,
    } in simple_aggrs
    {
        let cur_accum_range = accum_ranges[*output_idx].clone(); // range of current accum
        let cur_old_accum = accums
            .get(cur_accum_range)
            .unwrap_or_default()
            .iter()
            .cloned();
        let cur_col_diff = col_diffs[*input_idx].iter().cloned();

        // actual eval aggregation function
        if let Some((res, new_accum)) =
            err_collector.run(|| expr.func.eval_diff_accumulable(cur_old_accum, cur_col_diff))
        {
            accum_output.insert_accum(*output_idx, new_accum);
            accum_output.insert_output(*output_idx, res);
        } // else just collect error and continue
    }
}

/// Accumulate the output of aggregation functions
///
/// The accum is a map from index to the accumulator of the aggregation function
///
/// The output is a map from index to the output of the aggregation function
#[derive(Debug)]
struct AccumOutput {
    accum: BTreeMap<usize, Vec<Value>>,
    output: BTreeMap<usize, Value>,
}

impl AccumOutput {
    fn new() -> Self {
        Self {
            accum: BTreeMap::new(),
            output: BTreeMap::new(),
        }
    }

    fn insert_accum(&mut self, idx: usize, v: Vec<Value>) {
        self.accum.insert(idx, v);
    }

    fn insert_output(&mut self, idx: usize, v: Value) {
        self.output.insert(idx, v);
    }

    /// return (accums, output)
    fn into_accum_output(self) -> Result<(Vec<Value>, Vec<Value>), EvalError> {
        ensure!(
            !self.accum.is_empty() && self.accum.len() == self.output.len(),
            InternalSnafu {
                reason: format!("Accum and output should have the non-zero and same length, found accum.len() = {}, output.len() = {}", self.accum.len(), self.output.len())
            }
        );
        // make output vec from output map
        if let Some(kv) = self.accum.last_key_value() {
            ensure!(
                *kv.0 == self.accum.len() - 1,
                InternalSnafu {
                    reason: "Accum should be a continuous range"
                }
            );
        }
        if let Some(kv) = self.output.last_key_value() {
            ensure!(
                *kv.0 == self.output.len() - 1,
                InternalSnafu {
                    reason: "Output should be a continuous range"
                }
            );
        }

        let accums = self.accum.into_values().collect_vec();
        let new_accums = from_accums_to_offsetted_accum(accums);
        let output = self.output.into_values().collect_vec();
        Ok((new_accums, output))
    }
}

/// Eval distinct aggregate functions with distinct input arrange
fn eval_distinct_aggrs(
    distinct_aggrs: &Vec<AggrWithIndex>,
    distinct_input: &Option<Vec<ArrangeHandler>>,
    accums: &[Value],
    accum_ranges: &[Range<usize>],
    col_diffs: &[Vec<(Value, i64)>],
    accum_output: &mut AccumOutput,
    SubgraphArg {
        now,
        err_collector,
        scheduler: _,
        send: _,
    }: SubgraphArg,
) {
    for AggrWithIndex {
        expr,
        input_idx,
        output_idx,
    } in distinct_aggrs
    {
        let cur_accum_range = accum_ranges[*output_idx].clone(); // range of current accum
        let cur_old_accum = accums
            .get(cur_accum_range)
            .unwrap_or_default()
            .iter()
            .cloned();
        let cur_col_diff = col_diffs[*input_idx].iter().cloned();
        // first filter input with distinct
        let input_arrange = distinct_input
            .as_ref()
            .and_then(|v| v[*input_idx].clone_full_arrange())
            .expect("A full distinct input arrangement should exist");
        let kv = cur_col_diff.map(|(v, d)| ((Row::new(vec![v]), Row::empty()), now, d));
        let col_diff_distinct =
            update_reduce_distinct_arrange(&input_arrange, kv, now, err_collector).map(
                |(row, _ts, diff)| (row.get(0).expect("Row should not be empty").clone(), diff),
            );
        let col_diff_distinct = {
            let res = col_diff_distinct.collect_vec();
            res.into_iter()
        };
        // actual eval aggregation function
        let (res, new_accum) = expr
            .func
            .eval_diff_accumulable(cur_old_accum, col_diff_distinct)
            .unwrap();
        accum_output.insert_accum(*output_idx, new_accum);
        accum_output.insert_output(*output_idx, res);
    }
}

fn check_no_future_updates<'a>(
    all_arrange_used: impl IntoIterator<Item = ArrangeWriter<'a>>,
    err_collector: &ErrCollector,
    now: repr::Timestamp,
) {
    for arrange in all_arrange_used {
        if arrange.get_next_update_time(&now).is_some() {
            err_collector.push_err(
                InternalSnafu {
                    reason: "No future updates should exist in the reduce distinct arrangement",
                }
                .build(),
            );
        }
    }
}

/// convert a list of accumulators to a vector of values with first value as offset of end of each accumulator
fn from_accums_to_offsetted_accum(new_accums: Vec<Vec<Value>>) -> Vec<Value> {
    let offset = new_accums
        .iter()
        .map(|v| v.len() as u64)
        .scan(1, |state, x| {
            *state += x;
            Some(*state)
        })
        .map(Value::from)
        .collect::<Vec<_>>();
    let first = ListValue::new(offset, ConcreteDataType::uint64_datatype());
    let first = Value::List(first);
    // construct new_accums

    std::iter::once(first)
        .chain(new_accums.into_iter().flatten())
        .collect::<Vec<_>>()
}

/// Convert a value to a list of slice index
fn from_val_to_slice_idx(
    value: Option<Value>,
    expected_len: usize,
) -> Result<Vec<Range<usize>>, EvalError> {
    let offset_end = if let Some(value) = value {
        let list = value
            .as_list()
            .with_context(|_| DataTypeSnafu {
                msg: "Accum's first element should be a list",
            })?
            .context(InternalSnafu {
                reason: "Accum's first element should be a list",
            })?;
        let ret: Vec<usize> = list
            .items()
            .iter()
            .map(|v| {
                v.as_u64().map(|j| j as usize).context(InternalSnafu {
                    reason: "End offset should be a list of u64",
                })
            })
            .try_collect()?;
        ensure!(
            ret.len() == expected_len,
            InternalSnafu {
                reason: "Offset List should have the same length as full_aggrs"
            }
        );
        Ok(ret)
    } else {
        Ok(vec![1usize; expected_len])
    }?;
    let accum_ranges = (0..expected_len)
        .map(|idx| {
            if idx == 0 {
                // note that the first element is the offset list
                debug_assert!(
                    offset_end[0] >= 1,
                    "Offset should be at least 1: {:?}",
                    &offset_end
                );
                1..offset_end[0]
            } else {
                offset_end[idx - 1]..offset_end[idx]
            }
        })
        .collect_vec();
    Ok(accum_ranges)
}

// mainly for reduce's test
// TODO(discord9): add tests for accum ser/de
#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;

    use common_time::{DateTime, Interval, Timestamp};
    use datatypes::data_type::{ConcreteDataType, ConcreteDataType as CDT};
    use hydroflow::scheduled::graph::Hydroflow;

    use super::*;
    use crate::compute::render::test::{get_output_handle, harness_test_ctx, run_and_check};
    use crate::compute::state::DataflowState;
    use crate::expr::{self, AggregateFunc, BinaryFunc, GlobalId, MapFilterProject, UnaryFunc};
    use crate::repr::{ColumnType, RelationType};

    /// SELECT sum(number) FROM numbers_with_ts GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00')
    /// input table columns: number, ts
    /// expected: sum(number), window_start, window_end
    #[test]
    fn test_tumble_group_by() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);
        const START: i64 = 1625097600000;
        let rows = vec![
            (1u32, START + 1000),
            (2u32, START + 1500),
            (3u32, START + 2000),
            (1u32, START + 2500),
            (2u32, START + 3000),
            (3u32, START + 3500),
        ];
        let rows = rows
            .into_iter()
            .map(|(number, ts)| {
                (
                    Row::new(vec![number.into(), Timestamp::new_millisecond(ts).into()]),
                    1,
                    1,
                )
            })
            .collect_vec();

        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::datetime_datatype(), false), // window start
                ColumnType::new(CDT::datetime_datatype(), false), // window end
            ])
            .into_unnamed(),
            // TODO(discord9): mfp indirectly ref to key columns
            /*
            .with_key(vec![1])
            .with_time_index(Some(0)),*/
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![
                                    ColumnType::new(ConcreteDataType::uint32_datatype(), false),
                                    ColumnType::new(ConcreteDataType::datetime_datatype(), false),
                                ])
                                .into_unnamed(),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                1_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                1_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .project(vec![0, 1])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(CDT::datetime_datatype(), false), // window start
                            ColumnType::new(CDT::datetime_datatype(), false), // window end
                            ColumnType::new(CDT::uint64_datatype(), true),    //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .into_unnamed(),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(3),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![4, 5, 6])
                    .unwrap(),
            },
        };

        let bundle = ctx.render_plan(expected).unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([(
            1,
            vec![
                (
                    Row::new(vec![
                        3u64.into(),
                        Timestamp::new_millisecond(START + 1000).into(),
                        Timestamp::new_millisecond(START + 2000).into(),
                    ]),
                    1,
                    1,
                ),
                (
                    Row::new(vec![
                        4u64.into(),
                        Timestamp::new_millisecond(START + 2000).into(),
                        Timestamp::new_millisecond(START + 3000).into(),
                    ]),
                    1,
                    1,
                ),
                (
                    Row::new(vec![
                        5u64.into(),
                        Timestamp::new_millisecond(START + 3000).into(),
                        Timestamp::new_millisecond(START + 4000).into(),
                    ]),
                    1,
                    1,
                ),
            ],
        )]);
        run_and_check(&mut state, &mut df, 1..2, expected, output);
    }

    /// select avg(number) from number;
    #[test]
    fn test_avg_eval() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1u32.into()]), 1, 1),
            (Row::new(vec![2u32.into()]), 1, 1),
            (Row::new(vec![3u32.into()]), 1, 1),
            (Row::new(vec![1u32.into()]), 1, 1),
            (Row::new(vec![2u32.into()]), 1, 1),
            (Row::new(vec![3u32.into()]), 1, 1),
        ];
        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);

        let aggr_exprs = vec![
            AggregateExpr {
                func: AggregateFunc::SumUInt32,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(1).call_binary(
                ScalarExpr::Literal(Value::from(0u32), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(ScalarExpr::Column(0).call_binary(
                ScalarExpr::Column(1).call_unary(UnaryFunc::Cast(CDT::uint64_datatype())),
                BinaryFunc::DivUInt64,
            )),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::uint64_datatype())),
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![ColumnType::new(
                                    ConcreteDataType::int64_datatype(),
                                    false,
                                )])
                                .into_unnamed(),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .project(vec![])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .project(vec![0])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 0, 1),
                            ],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(ConcreteDataType::uint32_datatype(), true),
                            ColumnType::new(ConcreteDataType::int64_datatype(), true),
                        ])
                        .into_unnamed(),
                    ),
                ),
                mfp: MapFilterProject::new(2)
                    .map(vec![
                        avg_expr,
                        // TODO(discord9): optimize mfp so to remove indirect ref
                        ScalarExpr::Column(2),
                    ])
                    .unwrap()
                    .project(vec![3])
                    .unwrap(),
            },
        };

        let bundle = ctx.render_plan(expected).unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([(1, vec![(Row::new(vec![2u64.into()]), 1, 1)])]);
        run_and_check(&mut state, &mut df, 1..2, expected, output);
    }

    /// SELECT DISTINCT col FROM table
    ///
    /// table schema:
    /// | name | type  |
    /// |------|-------|
    /// | col  | Int64 |
    #[test]
    fn test_basic_distinct() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![2i64.into()]), 2, 1),
            (Row::new(vec![3i64.into()]), 3, 1),
            (Row::new(vec![1i64.into()]), 4, 1),
            (Row::new(vec![2i64.into()]), 5, 1),
            (Row::new(vec![3i64.into()]), 6, 1),
        ];
        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        let typ = RelationType::new(vec![ColumnType::new_nullable(
            ConcreteDataType::int64_datatype(),
        )]);
        let key_val_plan = KeyValPlan {
            key_plan: MapFilterProject::new(1).project([0]).unwrap().into_safe(),
            val_plan: MapFilterProject::new(1).project([]).unwrap().into_safe(),
        };
        let reduce_plan = ReducePlan::Distinct;
        let bundle = ctx
            .render_reduce(
                Box::new(input_plan.with_types(typ.into_unnamed())),
                key_val_plan,
                reduce_plan,
                RelationType::empty(),
            )
            .unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([(
            6,
            vec![
                (Row::new(vec![1i64.into()]), 1, 1),
                (Row::new(vec![2i64.into()]), 2, 1),
                (Row::new(vec![3i64.into()]), 3, 1),
            ],
        )]);
        run_and_check(&mut state, &mut df, 6..7, expected, output);
    }

    /// SELECT SUM(col) FROM table
    ///
    /// table schema:
    /// | name | type  |
    /// |------|-------|
    /// | col  | Int64 |
    #[test]
    fn test_basic_reduce_accum() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![2i64.into()]), 2, 1),
            (Row::new(vec![3i64.into()]), 3, 1),
            (Row::new(vec![1i64.into()]), 4, 1),
            (Row::new(vec![2i64.into()]), 5, 1),
            (Row::new(vec![3i64.into()]), 6, 1),
        ];
        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        let typ = RelationType::new(vec![ColumnType::new_nullable(
            ConcreteDataType::int64_datatype(),
        )]);
        let key_val_plan = KeyValPlan {
            key_plan: MapFilterProject::new(1).project([]).unwrap().into_safe(),
            val_plan: MapFilterProject::new(1).project([0]).unwrap().into_safe(),
        };

        let simple_aggrs = vec![AggrWithIndex::new(
            AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            0,
            0,
        )];
        let accum_plan = AccumulablePlan {
            full_aggrs: vec![AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            }],
            simple_aggrs,
            distinct_aggrs: vec![],
        };

        let reduce_plan = ReducePlan::Accumulable(accum_plan);
        let bundle = ctx
            .render_reduce(
                Box::new(input_plan.with_types(typ.into_unnamed())),
                key_val_plan,
                reduce_plan,
                RelationType::empty(),
            )
            .unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([
            (1, vec![(Row::new(vec![1i64.into()]), 1, 1)]),
            (2, vec![(Row::new(vec![3i64.into()]), 2, 1)]),
            (3, vec![(Row::new(vec![6i64.into()]), 3, 1)]),
            (4, vec![(Row::new(vec![7i64.into()]), 4, 1)]),
            (5, vec![(Row::new(vec![9i64.into()]), 5, 1)]),
            (6, vec![(Row::new(vec![12i64.into()]), 6, 1)]),
        ]);
        run_and_check(&mut state, &mut df, 1..7, expected, output);
    }

    /// SELECT SUM(DISTINCT col) FROM table
    ///
    /// table schema:
    /// | name | type  |
    /// |------|-------|
    /// | col  | Int64 |
    ///
    /// this test include even more insert/delete case to cover all case for eval_distinct_core
    #[test]
    fn test_delete_reduce_distinct_accum() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            // same tick
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![1i64.into()]), 1, -1),
            // next tick
            (Row::new(vec![1i64.into()]), 2, 1),
            (Row::new(vec![1i64.into()]), 3, -1),
            // repeat in same tick
            (Row::new(vec![1i64.into()]), 4, 1),
            (Row::new(vec![1i64.into()]), 4, -1),
            (Row::new(vec![1i64.into()]), 4, 1),
        ];
        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        let typ = RelationType::new(vec![ColumnType::new_nullable(
            ConcreteDataType::int64_datatype(),
        )]);
        let key_val_plan = KeyValPlan {
            key_plan: MapFilterProject::new(1).project([]).unwrap().into_safe(),
            val_plan: MapFilterProject::new(1).project([0]).unwrap().into_safe(),
        };

        let distinct_aggrs = vec![AggrWithIndex::new(
            AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            0,
            0,
        )];
        let accum_plan = AccumulablePlan {
            full_aggrs: vec![AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: true,
            }],
            simple_aggrs: vec![],
            distinct_aggrs,
        };

        let reduce_plan = ReducePlan::Accumulable(accum_plan);
        let bundle = ctx
            .render_reduce(
                Box::new(input_plan.with_types(typ.into_unnamed())),
                key_val_plan,
                reduce_plan,
                RelationType::empty(),
            )
            .unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([
            (1, vec![(Row::new(vec![0i64.into()]), 1, 1)]),
            (2, vec![(Row::new(vec![1i64.into()]), 2, 1)]),
            (3, vec![(Row::new(vec![0i64.into()]), 3, 1)]),
            (4, vec![(Row::new(vec![1i64.into()]), 4, 1)]),
        ]);
        run_and_check(&mut state, &mut df, 1..7, expected, output);
    }

    /// SELECT SUM(DISTINCT col) FROM table
    ///
    /// table schema:
    /// | name | type  |
    /// |------|-------|
    /// | col  | Int64 |
    ///
    /// this test include insert and delete which should cover all case for eval_distinct_core
    #[test]
    fn test_basic_reduce_distinct_accum() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![1i64.into()]), 1, -1),
            (Row::new(vec![2i64.into()]), 2, 1),
            (Row::new(vec![3i64.into()]), 3, 1),
            (Row::new(vec![1i64.into()]), 4, 1),
            (Row::new(vec![2i64.into()]), 5, 1),
            (Row::new(vec![3i64.into()]), 6, 1),
            (Row::new(vec![1i64.into()]), 7, 1),
        ];
        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        let typ = RelationType::new(vec![ColumnType::new_nullable(
            ConcreteDataType::int64_datatype(),
        )]);
        let key_val_plan = KeyValPlan {
            key_plan: MapFilterProject::new(1).project([]).unwrap().into_safe(),
            val_plan: MapFilterProject::new(1).project([0]).unwrap().into_safe(),
        };

        let distinct_aggrs = vec![AggrWithIndex::new(
            AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            0,
            0,
        )];
        let accum_plan = AccumulablePlan {
            full_aggrs: vec![AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: true,
            }],
            simple_aggrs: vec![],
            distinct_aggrs,
        };

        let reduce_plan = ReducePlan::Accumulable(accum_plan);
        let bundle = ctx
            .render_reduce(
                Box::new(input_plan.with_types(typ.into_unnamed())),
                key_val_plan,
                reduce_plan,
                RelationType::empty(),
            )
            .unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([
            (1, vec![(Row::new(vec![0i64.into()]), 1, 1)]),
            (2, vec![(Row::new(vec![2i64.into()]), 2, 1)]),
            (3, vec![(Row::new(vec![5i64.into()]), 3, 1)]),
            (4, vec![(Row::new(vec![6i64.into()]), 4, 1)]),
            (5, vec![(Row::new(vec![6i64.into()]), 5, 1)]),
            (6, vec![(Row::new(vec![6i64.into()]), 6, 1)]),
            (7, vec![(Row::new(vec![6i64.into()]), 7, 1)]),
        ]);
        run_and_check(&mut state, &mut df, 1..7, expected, output);
    }

    /// SELECT SUM(col), SUM(DISTINCT col) FROM table
    ///
    /// table schema:
    /// | name | type  |
    /// |------|-------|
    /// | col  | Int64 |
    #[test]
    fn test_composite_reduce_distinct_accum() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![2i64.into()]), 2, 1),
            (Row::new(vec![3i64.into()]), 3, 1),
            (Row::new(vec![1i64.into()]), 4, 1),
            (Row::new(vec![2i64.into()]), 5, 1),
            (Row::new(vec![3i64.into()]), 6, 1),
            (Row::new(vec![1i64.into()]), 7, 1),
        ];
        let collection = ctx.render_constant(rows.clone());
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        let typ = RelationType::new(vec![ColumnType::new_nullable(
            ConcreteDataType::int64_datatype(),
        )]);
        let key_val_plan = KeyValPlan {
            key_plan: MapFilterProject::new(1).project([]).unwrap().into_safe(),
            val_plan: MapFilterProject::new(1).project([0]).unwrap().into_safe(),
        };
        let simple_aggrs = vec![AggrWithIndex::new(
            AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            0,
            0,
        )];
        let distinct_aggrs = vec![AggrWithIndex::new(
            AggregateExpr {
                func: AggregateFunc::SumInt64,
                expr: ScalarExpr::Column(0),
                distinct: true,
            },
            0,
            1,
        )];
        let accum_plan = AccumulablePlan {
            full_aggrs: vec![
                AggregateExpr {
                    func: AggregateFunc::SumInt64,
                    expr: ScalarExpr::Column(0),
                    distinct: false,
                },
                AggregateExpr {
                    func: AggregateFunc::SumInt64,
                    expr: ScalarExpr::Column(0),
                    distinct: true,
                },
            ],
            simple_aggrs,
            distinct_aggrs,
        };

        let reduce_plan = ReducePlan::Accumulable(accum_plan);
        let bundle = ctx
            .render_reduce(
                Box::new(input_plan.with_types(typ.into_unnamed())),
                key_val_plan,
                reduce_plan,
                RelationType::empty(),
            )
            .unwrap();

        let output = get_output_handle(&mut ctx, bundle);
        drop(ctx);
        let expected = BTreeMap::from([
            (1, vec![(Row::new(vec![1i64.into(), 1i64.into()]), 1, 1)]),
            (2, vec![(Row::new(vec![3i64.into(), 3i64.into()]), 2, 1)]),
            (3, vec![(Row::new(vec![6i64.into(), 6i64.into()]), 3, 1)]),
            (4, vec![(Row::new(vec![7i64.into(), 6i64.into()]), 4, 1)]),
            (5, vec![(Row::new(vec![9i64.into(), 6i64.into()]), 5, 1)]),
            (6, vec![(Row::new(vec![12i64.into(), 6i64.into()]), 6, 1)]),
            (7, vec![(Row::new(vec![13i64.into(), 6i64.into()]), 7, 1)]),
        ]);
        run_and_check(&mut state, &mut df, 1..7, expected, output);
    }
}
