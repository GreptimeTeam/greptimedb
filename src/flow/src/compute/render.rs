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

//! In this file, `render` means convert a static `Plan` into a Executable Dataflow
//!
//! And the [`Context`] is the environment for the render process, it contains all the necessary information for the render process

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::rc::Rc;

use datatypes::data_type::ConcreteDataType;
use datatypes::value::{ListValue, Value};
use hydroflow::futures::SinkExt;
use hydroflow::lattices::cc_traits::Get;
use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use hydroflow::scheduled::port::{PortCtx, SEND};
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};

use super::state::Scheduler;
use crate::adapter::error::{Error, EvalSnafu, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu};
use crate::compute::state::DataflowState;
use crate::compute::types::{Arranged, Collection, CollectionBundle, ErrCollector, Toff};
use crate::expr::error::{DataTypeSnafu, InternalSnafu};
use crate::expr::{
    self, EvalError, GlobalId, LocalId, MapFilterProject, MfpPlan, SafeMfpPlan, ScalarExpr,
};
use crate::plan::{AccumulablePlan, KeyValPlan, Plan, ReducePlan};
use crate::repr::{self, DiffRow, KeyValDiffRow, Row};
use crate::utils::{ArrangeHandler, ArrangeReader, ArrangeWriter, Arrangement};

mod map;
mod reduce;

/// The Context for build a Operator with id of `GlobalId`
pub struct Context<'referred, 'df> {
    pub id: GlobalId,
    pub df: &'referred mut Hydroflow<'df>,
    pub compute_state: &'referred mut DataflowState,
    /// a list of all collections being used in the operator
    pub input_collection: BTreeMap<GlobalId, CollectionBundle>,
    /// used by `Get`/`Let` Plan for getting/setting local variables
    ///
    /// TODO(discord9): consider if use Vec<(LocalId, CollectionBundle)> instead
    local_scope: Vec<BTreeMap<LocalId, CollectionBundle>>,
    // Collect all errors in this operator's evaluation
    err_collector: ErrCollector,
}

impl<'referred, 'df> Drop for Context<'referred, 'df> {
    fn drop(&mut self) {
        for bundle in std::mem::take(&mut self.input_collection)
            .into_values()
            .chain(
                std::mem::take(&mut self.local_scope)
                    .into_iter()
                    .flat_map(|v| v.into_iter())
                    .map(|(_k, v)| v),
            )
        {
            bundle.collection.into_inner().drop(self.df);
            drop(bundle.arranged);
        }
        // The automatically generated "drop glue" which recursively calls the destructors of all the fields (including the now empty `input_collection`)
    }
}

impl<'referred, 'df> Context<'referred, 'df> {
    pub fn insert_global(&mut self, id: GlobalId, collection: CollectionBundle) {
        self.input_collection.insert(id, collection);
    }

    pub fn insert_local(&mut self, id: LocalId, collection: CollectionBundle) {
        if let Some(last) = self.local_scope.last_mut() {
            last.insert(id, collection);
        } else {
            let first = BTreeMap::from([(id, collection)]);
            self.local_scope.push(first);
        }
    }
}

impl<'referred, 'df> Context<'referred, 'df> {
    /// Interpret and execute plan
    ///
    /// return the output of this plan
    pub fn render_plan(&mut self, plan: Plan) -> Result<CollectionBundle, Error> {
        match plan {
            Plan::Constant { rows } => Ok(self.render_constant(rows)),
            Plan::Get { id } => self.get_by_id(id),
            Plan::Let { id, value, body } => self.eval_let(id, value, body),
            Plan::Mfp { input, mfp } => {
                self.render_map_filter_project_into_executable_dataflow(input, mfp)
            }
            Plan::Reduce {
                input,
                key_val_plan,
                reduce_plan,
            } => self.render_reduce(input, key_val_plan, reduce_plan),
            Plan::Join { .. } => NotImplementedSnafu {
                reason: "Join is still WIP".to_string(),
            }
            .fail(),
            Plan::Union { .. } => NotImplementedSnafu {
                reason: "Union is still WIP".to_string(),
            }
            .fail(),
        }
    }

    /// render Constant, will only emit the `rows` once.
    pub fn render_constant(&mut self, mut rows: Vec<DiffRow>) -> CollectionBundle {
        let (send_port, recv_port) = self.df.make_edge::<_, Toff>("constant");

        self.df
            .add_subgraph_source("Constant", send_port, move |_ctx, send_port| {
                if rows.is_empty() {
                    return;
                }
                send_port.give(std::mem::take(&mut rows));
            });

        CollectionBundle::from_collection(Collection::from_port(recv_port))
    }

    pub fn get_by_id(&mut self, id: expr::Id) -> Result<CollectionBundle, Error> {
        let ret = match id {
            expr::Id::Local(local) => {
                let bundle = self
                    .local_scope
                    .iter()
                    .rev()
                    .find_map(|scope| scope.get(&local))
                    .with_context(|| InvalidQuerySnafu {
                        reason: format!("Local variable {:?} not found", local),
                    })?;
                bundle.clone(self.df)
            }
            expr::Id::Global(id) => {
                let bundle = self
                    .input_collection
                    .get(&id)
                    .with_context(|| InvalidQuerySnafu {
                        reason: format!("Collection {:?} not found", id),
                    })?;
                bundle.clone(self.df)
            }
        };
        Ok(ret)
    }

    /// Eval `Let` operator, useful for assigning a value to a local variable
    pub fn eval_let(
        &mut self,
        id: LocalId,
        value: Box<Plan>,
        body: Box<Plan>,
    ) -> Result<CollectionBundle, Error> {
        let value = self.render_plan(*value)?;

        self.local_scope.push(Default::default());
        self.insert_local(id, value);
        let ret = self.render_plan(*body)?;
        Ok(ret)
    }

    /// render `Plan::Reduce` into executable dataflow
    // There is a false positive in using `Vec<ScalarExpr>` as key due to `Value` have `bytes` variant
    #[allow(clippy::mutable_key_type)]
    pub fn render_reduce(
        &mut self,
        input: Box<Plan>,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
    ) -> Result<CollectionBundle, Error> {
        let input = self.render_plan(*input)?;
        // first assembly key&val that's ((Row, Row), tick, diff)
        // Then stream kvs through a reduce operator

        // the output is concat from key and val
        let output_key_arity = key_val_plan.key_plan.output_arity();

        // TODO: config global expire time from self
        let arrange_handler = self.compute_state.new_arrange(None);
        // reduce need full arrangement to be able to query all keys
        let arrange_handler_inner =
            arrange_handler
                .clone_full_arrange()
                .with_context(|| PlanSnafu {
                    reason: "No write is expected at this point",
                })?;

        let distinct_input = self.add_accum_distinct_input_arrange(&reduce_plan);

        let reduce_arrange = ReduceArrange {
            output_arrange: arrange_handler_inner,
            distinct_input,
        };

        let now = self.compute_state.current_time_ref();

        let err_collector = self.err_collector.clone();

        let (out_send_port, out_recv_port) = self.df.make_edge::<_, Toff>("reduce");

        let _subgraph = self.df.add_subgraph_in_out(
            "reduce",
            input.collection.into_inner(),
            out_send_port,
            move |_ctx, recv, send| {
                // mfp only need to passively receive updates from recvs
                let data = recv.take_inner().into_iter().flat_map(|v| v.into_iter());

                reduce_subgraph(
                    &reduce_arrange,
                    data,
                    &key_val_plan,
                    &reduce_plan,
                    *now.borrow(),
                    &err_collector,
                    send,
                );
            },
        );

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
    fn add_accum_distinct_input_arrange(
        &mut self,
        reduce_plan: &ReducePlan,
    ) -> Option<Vec<ArrangeHandler>> {
        match reduce_plan {
            ReducePlan::Distinct => None,
            ReducePlan::Accumulable(AccumulablePlan { distinct_aggrs, .. }) => {
                if distinct_aggrs.is_empty() {
                    None
                } else {
                    Some(
                        (0..distinct_aggrs.len())
                            .map(|_| self.compute_state.new_arrange(None))
                            .collect(),
                    )
                }
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
            .with_context(|| InternalSnafu {
                reason: "val_plan should not contain any filter predicate",
            })?;
        Ok(Some(((key, val), sys_time, diff)))
    } else {
        Ok(None)
    }
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
    now: repr::Timestamp,
    err_collector: &ErrCollector,
    send: &PortCtx<SEND, Toff>,
) {
    let mut row_buf = Row::empty();
    let key_val = data.into_iter().filter_map(|(row, sys_time, diff)| {
        // error is collected and then ignored
        err_collector
            .run(|| split_row_to_key_val(row, sys_time, diff, key_val_plan, &mut row_buf))
            .flatten()
    });
    // from here for distinct reduce and accum reduce, things are drastically different
    // for distinct reduce the arrange store the output,
    // but for accum reduce the arrange store the accum state, and output is
    // evaluated from the accum state if there is need to update
    match reduce_plan {
        ReducePlan::Distinct => {
            reduce_distinct_subgraph(arrange, key_val, now, err_collector, send)
        }
        ReducePlan::Accumulable(accum_plan) => reduce_accum_subgraph(
            arrange,
            distinct_input,
            key_val,
            accum_plan,
            now,
            err_collector,
            send,
        ),
    };
}

fn eval_distinct_core(
    arrange: ArrangeReader,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
) -> Vec<KeyValDiffRow> {
    // TODO: check if anything could go wrong
    let _ = err_collector;
    kv.into_iter()
        .filter_map(|((key, val), ts, diff)| {
            let old_val = arrange.get(now, &key);
            match (old_val, diff) {
                // a new distinct row
                (None, 1) => Some(((key, val), ts, diff)),
                // delete old_val
                (Some(old_val), -1) if old_val.0 == val => Some(((key, val), ts, -1)),
                _ => None,
            }
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
    let from = arrange.read().last_compaction_time().map(|n| n + 1);
    let from = from.unwrap_or(repr::Timestamp::MIN);
    let output_kv = arrange.read().get_updates_in_range(from..=now);

    // 2. Truncate all updates stored in arrangement within that range.
    let run_compaction = || {
        arrange.write().compaction_to(now)?;
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
fn reduce_distinct_subgraph(
    arrange: &ArrangeHandler,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
    send: &PortCtx<SEND, Toff>,
) {
    send.give(update_reduce_distinct_arrange(arrange, kv, now, err_collector).collect_vec());
    // no need to schedule since reduce is not suppose to recv or send any future updates
    // TODO: rethink if this is the correct behavior
}

/// eval accumulable reduce plan by eval aggregate function and reduce the result
///
/// TODO: eval distinct by adding distinct input arrangement
fn reduce_accum_subgraph(
    arrange: &ArrangeHandler,
    distinct_input: &Option<Vec<ArrangeHandler>>,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    accum_plan: &AccumulablePlan,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
    send: &PortCtx<SEND, Toff>,
) {
    let AccumulablePlan {
        full_aggrs,
        simple_aggrs,
        distinct_aggrs,
    } = accum_plan;
    let mut key2vals = BTreeMap::<Row, Vec<(Row, repr::Diff)>>::new();

    for ((key, val), _tick, diff) in kv {
        // it is assumed that value is in order of insertion
        let vals = key2vals.entry(key).or_default();
        vals.push((val, diff));
    }

    let mut all_updates = Vec::with_capacity(key2vals.len());
    let mut all_ouputs = Vec::with_capacity(key2vals.len());

    // lock the arrange for write
    let mut arrange = arrange.write();
    for (key, value_diffs) in key2vals {
        let col_diffs = {
            let row_len = value_diffs[0].0.len();
            // split value_diffs into columns
            (0..row_len)
                .map(|i| {
                    value_diffs
                        .iter()
                        .map(|(row, diff)| (row.get(i).cloned().unwrap(), *diff))
                        .collect_vec()
                })
                .collect_vec()
        };
        let (accums, _, _) = arrange.get(now, &key).unwrap_or_default();
        let accums = accums.inner;

        // deser accums from offsets
        let accum_ranges = {
            let res = err_collector
                .run(|| from_val_to_slice_idx(accums.get(0).cloned(), full_aggrs.len()));
            if let Some(res) = res {
                res
            } else {
                // ignore for whatever reason
                continue;
            }
        };
        let mut new_accums = vec![None; full_aggrs.len()];
        let mut res_val_row = vec![None; full_aggrs.len()];

        for (output_idx, input_idx, aggr_fn) in simple_aggrs {
            let cur_accum_range = accum_ranges[*output_idx].clone(); // range of current accum
            let cur_old_accum = accums[cur_accum_range].iter().cloned();
            let cur_col_diff = col_diffs[*input_idx].iter().cloned();

            // actual eval aggregation function
            let (res, new_accum) = aggr_fn
                .func
                .eval_diff_accumulable(cur_old_accum, cur_col_diff)
                .unwrap();

            new_accums[*output_idx] = Some(new_accum);
            res_val_row[*output_idx] = Some(res);
        }

        // for distinct input
        for (output_idx, input_idx, aggr_fn) in distinct_aggrs {
            let cur_accum_range = accum_ranges[*output_idx].clone(); // range of current accum
            let cur_old_accum = accums[cur_accum_range].iter().cloned();
            let cur_col_diff = col_diffs[*input_idx].iter().cloned();
            // first filter input with distinct
            let input_arrange = distinct_input
                .as_ref()
                .and_then(|v| v[*input_idx].clone_full_arrange())
                .unwrap();
            let kv = cur_col_diff.map(|(v, d)| ((Row::new(vec![v]), Row::empty()), now, d));
            let col_diff_distinct =
                update_reduce_distinct_arrange(&input_arrange, kv, now, err_collector).map(
                    |(row, _ts, diff)| (row.get(0).expect("Row should not be empty").clone(), diff),
                );

            // actual eval aggregation function
            let (res, new_accum) = aggr_fn
                .func
                .eval_diff_accumulable(cur_old_accum, col_diff_distinct)
                .unwrap();

            new_accums[*output_idx] = Some(new_accum);
            res_val_row[*output_idx] = Some(res);
        }

        // remove `Option` from `res_val_row` and new_accums, if there is still None, report error
        err_collector.run(|| {
            let res_val_row = res_val_row
                .into_iter()
                .map(|v| {
                    v.with_context(|| InternalSnafu {
                        reason: "Output of aggregation function should not be None",
                    })
                })
                .try_collect::<_, Vec<Value>, _>()?;
            let new_accums = new_accums
                .into_iter()
                .map(|v| {
                    v.with_context(|| InternalSnafu {
                        reason: "Accumulator of aggregation function should not be None",
                    })
                })
                .try_collect::<_, Vec<Vec<Value>>, _>()?;

            let new_accums = from_accums_to_offseted_accum(new_accums);

            // construct the updates and save it
            all_updates.push(((key.clone(), Row::new(new_accums)), now, 1));
            let mut key_val = key;
            key_val.extend(res_val_row);
            all_ouputs.push((key_val, now, 1));
            Ok(())
        });
    }
    err_collector.run(|| {
        arrange.apply_updates(now, all_updates)?;
        arrange.compaction_to(now)
    });
    send.give(all_ouputs);
}

/// convert a list of accumulators to a vector of values with first value as offset
fn from_accums_to_offseted_accum(new_accums: Vec<Vec<Value>>) -> Vec<Value> {
    let offset = new_accums
        .iter()
        .map(|v| Value::from((v.len() + 1) as u64))
        .collect::<Vec<_>>();
    let first = ListValue::new(Some(Box::new(offset)), ConcreteDataType::uint64_datatype());
    let first = Value::List(first);
    // construct new_accums

    std::iter::once(first)
        .chain(new_accums.into_iter().flatten())
        .collect::<Vec<_>>()
}

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
            .with_context(|| InternalSnafu {
                reason: "Accum's first element should be a list",
            })?;
        if let Some(res) = list.items().as_ref().map(|v| {
            v.iter()
                .map(|i| {
                    i.as_u64()
                        .map(|j| j as usize)
                        .with_context(|| InternalSnafu {
                            reason: "End offset should be a list of u64",
                        })
                })
                .try_collect::<_, Vec<usize>, _>()
        }) {
            let ret = res?;
            ensure!(
                ret.len() == expected_len,
                InternalSnafu {
                    reason: "Offset List should have the same length as full_aggrs"
                }
            );
            Ok(ret)
        } else {
            InternalSnafu {
                reason: "End offset should be a list of u64",
            }
            .fail()
        }
    } else {
        Ok(vec![0usize; expected_len])
    }?;
    let accum_ranges = (0..expected_len)
        .map(|idx| {
            if idx == 0 {
                // note that the first element is the offset list
                1..offset_end[0]
            } else {
                offset_end[idx - 1]..offset_end[idx]
            }
        })
        .collect_vec();
    Ok(accum_ranges)
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;

    use common_time::DateTime;
    use datatypes::data_type::ConcreteDataType;
    use hydroflow::scheduled::graph::Hydroflow;
    use hydroflow::scheduled::graph_ext::GraphExt;
    use hydroflow::scheduled::handoff::VecHandoff;

    use super::*;
    use crate::expr::BinaryFunc;
    use crate::repr::Row;

    fn harness_test_ctx<'r, 'h>(
        df: &'r mut Hydroflow<'h>,
        state: &'r mut DataflowState,
    ) -> Context<'r, 'h> {
        let err_collector = state.get_err_collector();
        Context {
            id: GlobalId::User(0),
            df,
            compute_state: state,
            input_collection: BTreeMap::new(),
            local_scope: Default::default(),
            err_collector,
        }
    }

    /// test if temporal filter works properly
    /// namely: if mfp operator can schedule a delete at the correct time
    #[test]
    fn test_render_mfp_with_temporal() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![2i64.into()]), 2, 1),
            (Row::new(vec![3i64.into()]), 3, 1),
        ];
        let collection = ctx.render_constant(rows);
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        // temporal filter: now <= col(0) < now + 4
        let mfp = MapFilterProject::new(1)
            .filter(vec![
                ScalarExpr::Column(0)
                    .call_unary(expr::UnaryFunc::Cast(ConcreteDataType::datetime_datatype()))
                    .call_binary(
                        ScalarExpr::CallUnmaterializable(expr::UnmaterializableFunc::Now),
                        BinaryFunc::Gte,
                    ),
                ScalarExpr::Column(0)
                    .call_binary(
                        ScalarExpr::literal(4i64.into(), ConcreteDataType::int64_datatype()),
                        BinaryFunc::SubInt64,
                    )
                    .call_unary(expr::UnaryFunc::Cast(ConcreteDataType::datetime_datatype()))
                    .call_binary(
                        ScalarExpr::CallUnmaterializable(expr::UnmaterializableFunc::Now),
                        BinaryFunc::Lt,
                    ),
            ])
            .unwrap();

        let mut bundle = ctx
            .render_map_filter_project_into_executable_dataflow(Box::new(input_plan), mfp)
            .unwrap();
        let collection = bundle.collection;
        let _arranged = bundle.arranged.pop_first().unwrap().1;
        let output = Rc::new(RefCell::new(vec![]));
        let output_inner = output.clone();
        let _subgraph = ctx.df.add_subgraph_sink(
            "test_render_constant",
            collection.into_inner(),
            move |_ctx, recv| {
                let data = recv.take_inner();
                let res = data.into_iter().flat_map(|v| v.into_iter()).collect_vec();
                output_inner.borrow_mut().clear();
                output_inner.borrow_mut().extend(res);
            },
        );
        // drop ctx here to simulate actual process of compile first, run later scenario
        drop(ctx);
        // expected output at given time
        let expected_output = BTreeMap::from([
            (
                0, // time
                vec![
                    (Row::new(vec![1i64.into()]), 0, 1),
                    (Row::new(vec![2i64.into()]), 0, 1),
                    (Row::new(vec![3i64.into()]), 0, 1),
                ],
            ),
            (
                2, // time
                vec![(Row::new(vec![1i64.into()]), 2, -1)],
            ),
            (
                3, // time
                vec![(Row::new(vec![2i64.into()]), 3, -1)],
            ),
            (
                4, // time
                vec![(Row::new(vec![3i64.into()]), 4, -1)],
            ),
        ]);

        for now in 0i64..5 {
            state.set_current_ts(now);
            state.run_available_with_schedule(&mut df);
            assert!(state.get_err_collector().inner.borrow().is_empty());
            if let Some(expected) = expected_output.get(&now) {
                assert_eq!(*output.borrow(), *expected);
            } else {
                assert_eq!(*output.borrow(), vec![]);
            };
            output.borrow_mut().clear();
        }
    }

    /// test if mfp operator without temporal filter works properly
    /// that is it filter the rows correctly
    #[test]
    fn test_render_mfp() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1.into()]), 1, 1),
            (Row::new(vec![2.into()]), 2, 1),
            (Row::new(vec![3.into()]), 3, 1),
        ];
        let collection = ctx.render_constant(rows);
        ctx.insert_global(GlobalId::User(1), collection);
        let input_plan = Plan::Get {
            id: expr::Id::Global(GlobalId::User(1)),
        };
        // filter: col(0)>1
        let mfp = MapFilterProject::new(1)
            .filter(vec![ScalarExpr::Column(0).call_binary(
                ScalarExpr::literal(1.into(), ConcreteDataType::int32_datatype()),
                BinaryFunc::Gt,
            )])
            .unwrap();
        let bundle = ctx
            .render_map_filter_project_into_executable_dataflow(Box::new(input_plan), mfp)
            .unwrap();
        let collection = bundle.collection.clone(ctx.df);

        ctx.df.add_subgraph_sink(
            "test_render_constant",
            collection.into_inner(),
            move |_ctx, recv| {
                let data = recv.take_inner();
                let res = data.into_iter().flat_map(|v| v.into_iter()).collect_vec();
                assert_eq!(
                    res,
                    vec![
                        (Row::new(vec![2.into()]), 0, 1),
                        (Row::new(vec![3.into()]), 0, 1),
                    ]
                )
            },
        );
        drop(ctx);

        df.run_available();
    }

    /// test if constant operator works properly
    /// that is it only emit once, not multiple times
    #[test]
    fn test_render_constant() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::empty(), 1, 1),
            (Row::empty(), 2, 1),
            (Row::empty(), 3, 1),
        ];
        let collection = ctx.render_constant(rows);
        let collection = collection.collection.clone(ctx.df);
        let cnt = Rc::new(RefCell::new(0));
        let cnt_inner = cnt.clone();
        ctx.df.add_subgraph_sink(
            "test_render_constant",
            collection.into_inner(),
            move |_ctx, recv| {
                let data = recv.take_inner();
                *cnt_inner.borrow_mut() += data.iter().map(|v| v.len()).sum::<usize>();
            },
        );
        ctx.df.run_available();
        assert_eq!(*cnt.borrow(), 3);
        ctx.df.run_available();
        assert_eq!(*cnt.borrow(), 3);
    }

    /// a simple example to show how to use source and sink
    #[test]
    fn example_source_sink() {
        let mut df = Hydroflow::new();
        let (send_port, recv_port) = df.make_edge::<_, VecHandoff<i32>>("test_handoff");
        df.add_subgraph_source("test_handoff_source", send_port, move |_ctx, send| {
            for i in 0..10 {
                send.give(vec![i]);
            }
        });

        let sum = Rc::new(RefCell::new(0));
        let sum_move = sum.clone();
        let sink = df.add_subgraph_sink("test_handoff_sink", recv_port, move |_ctx, recv| {
            let data = recv.take_inner();
            *sum_move.borrow_mut() += data.iter().sum::<i32>();
        });

        df.run_available();
        assert_eq!(sum.borrow().to_owned(), 45);
        df.schedule_subgraph(sink);
        df.run_available();

        assert_eq!(sum.borrow().to_owned(), 45);
    }
}
