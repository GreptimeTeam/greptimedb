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
use std::rc::Rc;

use hydroflow::lattices::cc_traits::Get;
use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use hydroflow::scheduled::port::{PortCtx, SEND};
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};

use super::state::Scheduler;
use crate::adapter::error::{Error, EvalSnafu, InvalidQuerySnafu, PlanSnafu};
use crate::compute::state::DataflowState;
use crate::compute::types::{Arranged, Collection, CollectionBundle, ErrCollector, Toff};
use crate::expr::error::InternalSnafu;
use crate::expr::{
    self, EvalError, GlobalId, LocalId, MapFilterProject, MfpPlan, SafeMfpPlan, ScalarExpr,
};
use crate::plan::{KeyValPlan, Plan, ReducePlan};
use crate::repr::{self, DiffRow, KeyValDiffRow, Row};
use crate::utils::{ArrangeHandler, ArrangeReader, Arrangement};

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

// There is a false positive in using `Vec<ScalarExpr>` as key
#[allow(clippy::mutable_key_type)]
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
            Plan::Reduce { .. } => todo!(),
            Plan::Join { .. } => todo!(),
            Plan::Union { .. } => todo!(),
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

    /// render MapFilterProject, will only emit the `rows` once. Assume all incoming row's sys time being `now`` and ignore the row's stated sys time
    /// TODO(discord9): schedule mfp operator to run when temporal filter need
    ///
    /// `MapFilterProject`(`mfp` for short) is scheduled to run when there is enough amount of input updates
    /// ***or*** when a future update in it's output buffer(a `Arrangement`) is supposed to emit now.
    pub fn render_map_filter_project_into_executable_dataflow(
        &mut self,
        input: Box<Plan>,
        mfp: MapFilterProject,
    ) -> Result<CollectionBundle, Error> {
        let input = self.render_plan(*input)?;
        // TODO(discord9): consider if check if contain temporal to determine if
        // need arrange or not, or does this added complexity worth it
        let (out_send_port, out_recv_port) = self.df.make_edge::<_, Toff>("mfp");

        let output_arity = mfp.output_arity();

        // default to have a arrange with only future updates, so it can be empty if no temporal filter is applied
        // as stream only sends current updates and etc.
        let arrange = Arrangement::new();
        let arrange_handler = ArrangeHandler::from(arrange);
        let arrange_handler_inner =
            arrange_handler
                .clone_future_only()
                .with_context(|| PlanSnafu {
                    reason: "No write is expected at this point",
                })?;

        // This closure capture following variables:
        let mfp_plan = MfpPlan::create_from(mfp)?;
        let now = self.compute_state.current_time_ref();

        let err_collector = self.err_collector.clone();

        // TODO(discord9): better way to schedule future run
        let scheduler = self.compute_state.get_scheduler();
        let scheduler_inner = scheduler.clone();

        let subgraph = self.df.add_subgraph_in_out(
            "mfp",
            input.collection.into_inner(),
            out_send_port,
            move |_ctx, recv, send| {
                // mfp only need to passively receive updates from recvs
                let data = recv.take_inner().into_iter().flat_map(|v| v.into_iter());

                mfp_subgraph(
                    &arrange_handler_inner,
                    data,
                    &mfp_plan,
                    *now.borrow(),
                    &err_collector,
                    &scheduler_inner,
                    send,
                );
            },
        );

        // register current subgraph in scheduler for future scheduling
        scheduler.set_cur_subgraph(subgraph);

        let arranged = BTreeMap::from([(
            (0..output_arity).map(ScalarExpr::Column).collect_vec(),
            Arranged::new(arrange_handler),
        )]);

        let bundle = CollectionBundle {
            collection: Collection::from_port(out_recv_port),
            arranged,
        };
        Ok(bundle)
    }

    /// render `Plan::Reduce` into executable dataflow
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
        let arrange = Arrangement::new();
        let arrange_handler = ArrangeHandler::from(arrange);
        // reduce need full arrangement to be able to query all keys
        let arrange_handler_inner =
            arrange_handler
                .clone_full_arrange()
                .with_context(|| PlanSnafu {
                    reason: "No write is expected at this point",
                })?;

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
                    &arrange_handler_inner,
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
}

/// reduce subgraph, reduce the input data into a single row
/// output is concat from key and val
fn reduce_subgraph(
    arrange: &ArrangeHandler,
    data: impl IntoIterator<Item = DiffRow>,
    key_val_plan: &KeyValPlan,
    reduce_plan: &ReducePlan,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
    send: &PortCtx<SEND, Toff>,
) {
    let run_reduce = || {
        let mut row_buf = Row::empty();
        let key_val = data.into_iter().filter_map(|(row, sys_time, diff)| {
            // error is collected and then ignored
            err_collector
                .run(|| {
                    if let Some(key) = key_val_plan
                        .key_plan
                        .evaluate_into(&mut row.inner.clone(), &mut row_buf)?
                    {
                        // val_plan is not supported to carry any filter predicate,
                        let val = key_val_plan
                            .val_plan
                            .evaluate_into(&mut row.inner.clone(), &mut row_buf)?
                            .with_context(|| InternalSnafu {
                                reason: "val_plan should not contain any filter predicate",
                            })?;
                        Ok(Some(((key, val), sys_time, diff)))
                    } else {
                        Ok(None)
                    }
                })
                .flatten()
        });
        let resul_updates = match reduce_plan {
            ReducePlan::Distinct => {
                eval_reduce_distinct(&arrange.read(), key_val, now, err_collector)
            }
            ReducePlan::Accumulable(_) => todo!(),
        };
        arrange.write().apply_updates(now, resul_updates)?;
        Ok(())
    };
    err_collector.run(run_reduce);

    // Deal with output:

    // 1. Read all updates that were emitted between the last time this arrangement had updates and the current time.
    let from = arrange.read().last_compaction_time().map(|n| n + 1);
    let from = from.unwrap_or(repr::Timestamp::MIN);
    let output_kv = arrange.read().get_updates_in_range(from..=now);

    // 2. Output the updates.
    // output is concat from key and val
    let output = output_kv
        .into_iter()
        .map(|((mut key, v), ts, diff)| {
            key.extend(v.into_iter());
            (key, ts, diff)
        })
        .collect_vec();
    send.give(output);

    // 3. Truncate all updates stored in arrangement within that range.
    let run_compaction = || {
        arrange.write().compaction_to(now)?;
        Ok(())
    };
    err_collector.run(run_compaction);

    // no need to schedule since reduce is not suppose to recv or send any future updates
    // TODO: rethink if this is the correct behavior
}

/// eval distinct reduce plan, and output all updates(which should be insert of new distinct row or deletion of old distinct row)
fn eval_reduce_distinct(
    arrange: &ArrangeReader,
    kv: impl IntoIterator<Item = KeyValDiffRow>,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
) -> Vec<KeyValDiffRow> {
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
        .collect()
}

fn mfp_subgraph(
    arrange: &ArrangeHandler,
    input: impl IntoIterator<Item = DiffRow>,
    mfp_plan: &MfpPlan,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
    scheduler: &Scheduler,
    send: &PortCtx<SEND, Toff>,
) {
    let run_mfp = || {
        let all_updates = eval_mfp_core(input, mfp_plan, now, err_collector);
        arrange.write().apply_updates(now, all_updates)?;
        Ok(())
    };
    err_collector.run(run_mfp);

    // Deal with output:
    // 1. Read all updates that were emitted between the last time this arrangement had updates and the current time.
    // 2. Output the updates.
    // 3. Truncate all updates within that range.

    let from = arrange.read().last_compaction_time().map(|n| n + 1);
    let from = from.unwrap_or(repr::Timestamp::MIN);
    let output_kv = arrange.read().get_updates_in_range(from..=now);
    // the output is expected to be key -> empty val
    let output = output_kv
        .into_iter()
        .map(|((key, _v), ts, diff)| (key, ts, diff))
        .collect_vec();
    send.give(output);
    let run_compaction = || {
        arrange.write().compaction_to(now)?;
        Ok(())
    };
    err_collector.run(run_compaction);

    // schedule the next time this operator should run
    if let Some(i) = arrange.read().get_next_update_time(&now) {
        scheduler.schedule_at(i)
    }
}

/// The core of evaluating MFP operator, given a MFP and a input, evaluate the MFP operator,
/// return the output updates **And** possibly any number of errors that occurred during the evaluation
fn eval_mfp_core(
    input: impl IntoIterator<Item = DiffRow>,
    mfp_plan: &MfpPlan,
    now: repr::Timestamp,
    err_collector: &ErrCollector,
) -> Vec<KeyValDiffRow> {
    let mut all_updates = Vec::new();
    for (mut row, _sys_time, diff) in input.into_iter() {
        // this updates is expected to be only zero to two rows
        let updates = mfp_plan.evaluate::<EvalError>(&mut row.inner, now, diff);
        // TODO(discord9): refactor error handling
        // Expect error in a single row to not interrupt the whole evaluation
        let updates = updates
            .filter_map(|r| match r {
                Ok((key, ts, diff)) => Some(((key, Row::empty()), ts, diff)),
                Err((err, _ts, _diff)) => {
                    err_collector.push_err(err);
                    None
                }
            })
            .collect_vec();

        all_updates.extend(updates);
    }
    all_updates
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
