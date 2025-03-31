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

use std::collections::BTreeMap;

use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::graph_ext::GraphExt;
use hydroflow::scheduled::port::{PortCtx, SEND};
use itertools::Itertools;
use snafu::OptionExt;

use super::state::Scheduler;
use crate::compute::state::DataflowState;
use crate::compute::types::{Collection, CollectionBundle, ErrCollector, Toff};
use crate::error::{Error, InvalidQuerySnafu, NotImplementedSnafu};
use crate::expr::{self, Batch, GlobalId, LocalId};
use crate::plan::{Plan, TypedPlan};
use crate::repr::{self, DiffRow, RelationType};

mod map;
mod reduce;
mod src_sink;

/// The Context for build a Operator with id of `GlobalId`
pub struct Context<'referred, 'df> {
    pub id: GlobalId,
    pub df: &'referred mut Hydroflow<'df>,
    pub compute_state: &'referred mut DataflowState,
    /// a list of all collections being used in the operator
    ///
    /// TODO(discord9): remove extra clone by counting usage and remove it on last usage?
    pub input_collection: BTreeMap<GlobalId, CollectionBundle>,
    /// used by `Get`/`Let` Plan for getting/setting local variables
    ///
    /// TODO(discord9): consider if use Vec<(LocalId, CollectionBundle)> instead
    pub local_scope: Vec<BTreeMap<LocalId, CollectionBundle>>,
    /// a list of all collections being used in the operator
    ///
    /// TODO(discord9): remove extra clone by counting usage and remove it on last usage?
    pub input_collection_batch: BTreeMap<GlobalId, CollectionBundle<Batch>>,
    /// used by `Get`/`Let` Plan for getting/setting local variables
    ///
    /// TODO(discord9): consider if use Vec<(LocalId, CollectionBundle)> instead
    pub local_scope_batch: Vec<BTreeMap<LocalId, CollectionBundle<Batch>>>,
    // Collect all errors in this operator's evaluation
    pub err_collector: ErrCollector,
}

impl Drop for Context<'_, '_> {
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

        for bundle in std::mem::take(&mut self.input_collection_batch)
            .into_values()
            .chain(
                std::mem::take(&mut self.local_scope_batch)
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

impl Context<'_, '_> {
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

    pub fn insert_global_batch(&mut self, id: GlobalId, collection: CollectionBundle<Batch>) {
        self.input_collection_batch.insert(id, collection);
    }

    pub fn insert_local_batch(&mut self, id: LocalId, collection: CollectionBundle<Batch>) {
        if let Some(last) = self.local_scope_batch.last_mut() {
            last.insert(id, collection);
        } else {
            let first = BTreeMap::from([(id, collection)]);
            self.local_scope_batch.push(first);
        }
    }
}

impl Context<'_, '_> {
    /// Like `render_plan` but in Batch Mode
    pub fn render_plan_batch(&mut self, plan: TypedPlan) -> Result<CollectionBundle<Batch>, Error> {
        match plan.plan {
            Plan::Constant { rows } => Ok(self.render_constant_batch(rows, &plan.schema.typ)),
            Plan::Get { id } => self.get_batch_by_id(id),
            Plan::Let { id, value, body } => self.eval_batch_let(id, value, body),
            Plan::Mfp { input, mfp } => self.render_mfp_batch(input, mfp, &plan.schema.typ),
            Plan::Reduce {
                input,
                key_val_plan,
                reduce_plan,
            } => self.render_reduce_batch(input, &key_val_plan, &reduce_plan, &plan.schema.typ),
            Plan::Join { .. } => NotImplementedSnafu {
                reason: "Join is still WIP",
            }
            .fail(),
            Plan::Union { .. } => NotImplementedSnafu {
                reason: "Union is still WIP",
            }
            .fail(),
        }
    }

    /// Interpret plan to dataflow and prepare them for execution
    ///
    /// return the output handler of this plan
    pub fn render_plan(&mut self, plan: TypedPlan) -> Result<CollectionBundle, Error> {
        match plan.plan {
            Plan::Constant { rows } => Ok(self.render_constant(rows)),
            Plan::Get { id } => self.get_by_id(id),
            Plan::Let { id, value, body } => self.eval_let(id, value, body),
            Plan::Mfp { input, mfp } => self.render_mfp(input, mfp),
            Plan::Reduce {
                input,
                key_val_plan,
                reduce_plan,
            } => self.render_reduce(input, key_val_plan, reduce_plan, plan.schema.typ),
            Plan::Join { .. } => NotImplementedSnafu {
                reason: "Join is still WIP",
            }
            .fail(),
            Plan::Union { .. } => NotImplementedSnafu {
                reason: "Union is still WIP",
            }
            .fail(),
        }
    }

    /// render Constant, take all rows that have a timestamp not greater than the current time
    /// This function is primarily used for testing
    /// Always assume input is sorted by timestamp
    pub fn render_constant_batch(
        &mut self,
        rows: Vec<DiffRow>,
        output_type: &RelationType,
    ) -> CollectionBundle<Batch> {
        let (send_port, recv_port) = self.df.make_edge::<_, Toff<Batch>>("constant_batch");
        let mut per_time: BTreeMap<repr::Timestamp, Vec<DiffRow>> = Default::default();
        for (key, group) in &rows.into_iter().chunk_by(|(_row, ts, _diff)| *ts) {
            per_time.entry(key).or_default().extend(group);
        }

        let now = self.compute_state.current_time_ref();
        // TODO(discord9): better way to schedule future run
        let scheduler = self.compute_state.get_scheduler();
        let scheduler_inner = scheduler.clone();
        let err_collector = self.err_collector.clone();

        let output_type = output_type.clone();

        let subgraph_id =
            self.df
                .add_subgraph_source("ConstantBatch", send_port, move |_ctx, send_port| {
                    // find the first timestamp that is greater than now
                    // use filter_map

                    let mut after = per_time.split_off(&(*now.borrow() + 1));
                    // swap
                    std::mem::swap(&mut per_time, &mut after);
                    let not_great_than_now = after;

                    not_great_than_now.into_iter().for_each(|(_ts, rows)| {
                        err_collector.run(|| {
                            let rows = rows.into_iter().map(|(row, _ts, _diff)| row).collect();
                            let batch = Batch::try_from_rows_with_types(
                                rows,
                                &output_type
                                    .column_types
                                    .iter()
                                    .map(|ty| ty.scalar_type().clone())
                                    .collect_vec(),
                            )?;
                            send_port.give(vec![batch]);
                            Ok(())
                        });
                    });
                    // schedule the next run
                    if let Some(next_run_time) = per_time.keys().next().copied() {
                        scheduler_inner.schedule_at(next_run_time);
                    }
                });
        scheduler.set_cur_subgraph(subgraph_id);

        CollectionBundle::from_collection(Collection::from_port(recv_port))
    }

    /// render Constant, take all rows that have a timestamp not greater than the current time
    ///
    /// Always assume input is sorted by timestamp
    pub fn render_constant(&mut self, rows: Vec<DiffRow>) -> CollectionBundle {
        let (send_port, recv_port) = self.df.make_edge::<_, Toff>("constant");
        let mut per_time: BTreeMap<repr::Timestamp, Vec<DiffRow>> = Default::default();
        for (key, group) in &rows.into_iter().chunk_by(|(_row, ts, _diff)| *ts) {
            per_time.entry(key).or_default().extend(group);
        }

        let now = self.compute_state.current_time_ref();
        // TODO(discord9): better way to schedule future run
        let scheduler = self.compute_state.get_scheduler();
        let scheduler_inner = scheduler.clone();

        let subgraph_id =
            self.df
                .add_subgraph_source("Constant", send_port, move |_ctx, send_port| {
                    // find the first timestamp that is greater than now
                    // use filter_map

                    let mut after = per_time.split_off(&(*now.borrow() + 1));
                    // swap
                    std::mem::swap(&mut per_time, &mut after);
                    let not_great_than_now = after;

                    not_great_than_now.into_iter().for_each(|(_ts, rows)| {
                        send_port.give(rows);
                    });
                    // schedule the next run
                    if let Some(next_run_time) = per_time.keys().next().copied() {
                        scheduler_inner.schedule_at(next_run_time);
                    }
                });
        scheduler.set_cur_subgraph(subgraph_id);

        CollectionBundle::from_collection(Collection::from_port(recv_port))
    }

    pub fn get_batch_by_id(&mut self, id: expr::Id) -> Result<CollectionBundle<Batch>, Error> {
        let ret = match id {
            expr::Id::Local(local) => {
                let bundle = self
                    .local_scope_batch
                    .iter()
                    .rev()
                    .find_map(|scope| scope.get(&local))
                    .with_context(|| InvalidQuerySnafu {
                        reason: format!("Local variable {:?} not found", local),
                    })?;
                bundle.clone(self.df)
            }
            expr::Id::Global(id) => {
                let bundle =
                    self.input_collection_batch
                        .get(&id)
                        .with_context(|| InvalidQuerySnafu {
                            reason: format!("Collection {:?} not found", id),
                        })?;
                bundle.clone(self.df)
            }
        };
        Ok(ret)
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
    pub fn eval_batch_let(
        &mut self,
        id: LocalId,
        value: Box<TypedPlan>,
        body: Box<TypedPlan>,
    ) -> Result<CollectionBundle<Batch>, Error> {
        let value = self.render_plan_batch(*value)?;

        self.local_scope_batch.push(Default::default());
        self.insert_local_batch(id, value);
        let ret = self.render_plan_batch(*body)?;
        Ok(ret)
    }

    /// Eval `Let` operator, useful for assigning a value to a local variable
    pub fn eval_let(
        &mut self,
        id: LocalId,
        value: Box<TypedPlan>,
        body: Box<TypedPlan>,
    ) -> Result<CollectionBundle, Error> {
        let value = self.render_plan(*value)?;

        self.local_scope.push(Default::default());
        self.insert_local(id, value);
        let ret = self.render_plan(*body)?;
        Ok(ret)
    }
}

/// The Common argument for all `Subgraph` in the render process
struct SubgraphArg<'a, T = Toff> {
    now: repr::Timestamp,
    err_collector: &'a ErrCollector,
    scheduler: &'a Scheduler,
    send: &'a PortCtx<SEND, T>,
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::rc::Rc;

    use hydroflow::scheduled::graph::Hydroflow;
    use hydroflow::scheduled::graph_ext::GraphExt;
    use hydroflow::scheduled::handoff::VecHandoff;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::repr::Row;
    pub fn run_and_check(
        state: &mut DataflowState,
        df: &mut Hydroflow,
        time_range: std::ops::Range<i64>,
        expected: BTreeMap<i64, Vec<DiffRow>>,
        output: Rc<RefCell<Vec<DiffRow>>>,
    ) {
        for now in time_range {
            state.set_current_ts(now);
            state.run_available_with_schedule(df);
            if !state.get_err_collector().is_empty() {
                panic!(
                    "Errors occur: {:?}",
                    state.get_err_collector().get_all_blocking()
                )
            }
            assert!(state.get_err_collector().is_empty());
            if let Some(expected) = expected.get(&now) {
                assert_eq!(*output.borrow(), *expected, "at ts={}", now);
            } else {
                assert_eq!(*output.borrow(), vec![], "at ts={}", now);
            };
            output.borrow_mut().clear();
        }
    }

    pub fn get_output_handle(
        ctx: &mut Context,
        mut bundle: CollectionBundle,
    ) -> Rc<RefCell<Vec<DiffRow>>> {
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
        output
    }

    pub fn harness_test_ctx<'r, 'h>(
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
            input_collection_batch: BTreeMap::new(),
            local_scope_batch: Default::default(),
            err_collector,
        }
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
        let res_subgraph_id = ctx.df.add_subgraph_sink(
            "test_render_constant",
            collection.into_inner(),
            move |_ctx, recv| {
                let data = recv.take_inner();
                *cnt_inner.borrow_mut() += data.iter().map(|v| v.len()).sum::<usize>();
            },
        );
        ctx.compute_state.set_current_ts(2);
        ctx.compute_state.run_available_with_schedule(ctx.df);
        assert_eq!(*cnt.borrow(), 2);

        ctx.compute_state.set_current_ts(3);
        ctx.compute_state.run_available_with_schedule(ctx.df);
        // to get output
        ctx.df.schedule_subgraph(res_subgraph_id);
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

    #[test]
    fn test_tee_auto_schedule() {
        use hydroflow::scheduled::handoff::TeeingHandoff as Toff;
        let mut df = Hydroflow::new();
        let (send_port, recv_port) = df.make_edge::<_, Toff<i32>>("test_handoff");
        let source = df.add_subgraph_source("test_handoff_source", send_port, move |_ctx, send| {
            for i in 0..10 {
                send.give(vec![i]);
            }
        });
        let teed_recv_port = recv_port.tee(&mut df);

        let sum = Rc::new(RefCell::new(0));
        let sum_move = sum.clone();
        let _sink = df.add_subgraph_sink("test_handoff_sink", teed_recv_port, move |_ctx, recv| {
            let data = recv.take_inner();
            *sum_move.borrow_mut() += data.iter().flat_map(|i| i.iter()).sum::<i32>();
        });
        drop(recv_port);

        df.run_available();
        assert_eq!(sum.borrow().to_owned(), 45);

        df.schedule_subgraph(source);
        df.run_available();

        assert_eq!(sum.borrow().to_owned(), 90);
    }
}
