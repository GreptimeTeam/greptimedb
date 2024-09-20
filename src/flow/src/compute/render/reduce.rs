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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use common_telemetry::trace;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::DataType;
use datatypes::value::{ListValue, Value};
use datatypes::vectors::{BooleanVector, NullVector};
use hydroflow::scheduled::graph_ext::GraphExt;
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};

use crate::compute::render::{Context, SubgraphArg};
use crate::compute::types::{Arranged, Collection, CollectionBundle, ErrCollector, Toff};
use crate::error::{Error, NotImplementedSnafu, PlanSnafu};
use crate::expr::error::{ArrowSnafu, DataAlreadyExpiredSnafu, DataTypeSnafu, InternalSnafu};
use crate::expr::{Accum, Accumulator, Batch, EvalError, ScalarExpr, VectorDiff};
use crate::plan::{AccumulablePlan, AggrWithIndex, KeyValPlan, ReducePlan, TypedPlan};
use crate::repr::{self, DiffRow, KeyValDiffRow, RelationType, Row};
use crate::utils::{ArrangeHandler, ArrangeReader, ArrangeWriter, KeyExpiryManager};

impl<'referred, 'df> Context<'referred, 'df> {
    const REDUCE_BATCH: &'static str = "reduce_batch";
    /// Like `render_reduce`, but for batch mode, and only barebone implementation
    /// no support for distinct aggregation for now
    // There is a false positive in using `Vec<ScalarExpr>` as key due to `Value` have `bytes` variant
    #[allow(clippy::mutable_key_type)]
    pub fn render_reduce_batch(
        &mut self,
        input: Box<TypedPlan>,
        key_val_plan: &KeyValPlan,
        reduce_plan: &ReducePlan,
        output_type: &RelationType,
    ) -> Result<CollectionBundle<Batch>, Error> {
        let accum_plan = if let ReducePlan::Accumulable(accum_plan) = reduce_plan {
            if !accum_plan.distinct_aggrs.is_empty() {
                NotImplementedSnafu {
                    reason: "Distinct aggregation is not supported in batch mode",
                }
                .fail()?
            }
            accum_plan.clone()
        } else {
            NotImplementedSnafu {
                reason: "Only accumulable reduce plan is supported in batch mode",
            }
            .fail()?
        };

        let input = self.render_plan_batch(*input)?;

        // first assembly key&val to separate key and val columns(since this is batch mode)
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
        let key_val_plan = key_val_plan.clone();

        let now = self.compute_state.current_time_ref();

        let err_collector = self.err_collector.clone();

        // TODO(discord9): better way to schedule future run
        let scheduler = self.compute_state.get_scheduler();

        let scheduler_inner = scheduler.clone();

        let (out_send_port, out_recv_port) =
            self.df.make_edge::<_, Toff<Batch>>(Self::REDUCE_BATCH);

        let subgraph = self.df.add_subgraph_in_out(
            Self::REDUCE_BATCH,
            input.collection.into_inner(),
            out_send_port,
            move |_ctx, recv, send| {
                let now = *(now.borrow());
                let arrange = arrange_handler_inner.clone();
                // mfp only need to passively receive updates from recvs
                let src_data = recv
                    .take_inner()
                    .into_iter()
                    .flat_map(|v| v.into_iter())
                    .collect_vec();

                reduce_batch_subgraph(
                    &arrange,
                    src_data,
                    &key_val_plan,
                    &accum_plan,
                    SubgraphArg {
                        now,
                        err_collector: &err_collector,
                        scheduler: &scheduler_inner,
                        send,
                    },
                )
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

fn from_accum_values_to_live_accums(
    accums: Vec<Value>,
    len: usize,
) -> Result<Vec<Vec<Value>>, EvalError> {
    let accum_ranges = from_val_to_slice_idx(accums.first().cloned(), len)?;
    let mut accum_list = vec![];
    for range in accum_ranges.iter() {
        accum_list.push(accums.get(range.clone()).unwrap_or_default().to_vec());
    }
    Ok(accum_list)
}

/// All arrange(aka state) used in reduce operator
pub struct ReduceArrange {
    /// The output arrange of reduce operator
    output_arrange: ArrangeHandler,
    /// The distinct input arrangement for accumulable reduce plan
    /// only used when accumulable reduce plan has distinct aggregation
    distinct_input: Option<Vec<ArrangeHandler>>,
}

fn batch_split_by_key_val(
    batch: &Batch,
    key_val_plan: &KeyValPlan,
    err_collector: &ErrCollector,
) -> (Batch, Batch) {
    let row_count = batch.row_count();
    let mut key_batch = Batch::empty();
    let mut val_batch = Batch::empty();

    err_collector.run(|| {
        if key_val_plan.key_plan.output_arity() != 0 {
            key_batch = key_val_plan.key_plan.eval_batch_into(&mut batch.clone())?;
        }

        if key_val_plan.val_plan.output_arity() != 0 {
            val_batch = key_val_plan.val_plan.eval_batch_into(&mut batch.clone())?;
        }
        Ok(())
    });

    // deal with empty key or val
    if key_batch.row_count() == 0 && key_batch.column_count() == 0 {
        key_batch.set_row_count(row_count);
    }

    if val_batch.row_count() == 0 && val_batch.column_count() == 0 {
        val_batch.set_row_count(row_count);
    }

    (key_batch, val_batch)
}

/// split a row into key and val by evaluate the key and val plan
fn split_rows_to_key_val(
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

fn reduce_batch_subgraph(
    arrange: &ArrangeHandler,
    src_data: impl IntoIterator<Item = Batch>,
    key_val_plan: &KeyValPlan,
    accum_plan: &AccumulablePlan,
    SubgraphArg {
        now,
        err_collector,
        scheduler: _,
        send,
    }: SubgraphArg<Toff<Batch>>,
) {
    let mut key_to_many_vals = BTreeMap::<Row, Vec<Batch>>::new();
    let mut input_row_count = 0;
    let mut input_batch_count = 0;

    for batch in src_data {
        input_batch_count += 1;
        input_row_count += batch.row_count();
        err_collector.run(|| {
            let (key_batch, val_batch) =
                batch_split_by_key_val(&batch, key_val_plan, err_collector);
            ensure!(
                key_batch.row_count() == val_batch.row_count(),
                InternalSnafu {
                    reason: format!(
                        "Key and val batch should have the same row count, found {} and {}",
                        key_batch.row_count(),
                        val_batch.row_count()
                    )
                }
            );

            let mut distinct_keys = BTreeSet::new();
            for row_idx in 0..key_batch.row_count() {
                let key_row = key_batch.get_row(row_idx)?;
                let key_row = Row::new(key_row);

                if distinct_keys.contains(&key_row) {
                    continue;
                } else {
                    distinct_keys.insert(key_row.clone());
                }
            }

            // TODO: here reduce numbers of eq to minimal by keeping slicing key/val batch
            for key_row in distinct_keys {
                let key_scalar_value = {
                    let mut key_scalar_value = Vec::with_capacity(key_row.len());
                    for key in key_row.iter() {
                        let v =
                            key.try_to_scalar_value(&key.data_type())
                                .context(DataTypeSnafu {
                                    msg: "can't convert key values to datafusion value",
                                })?;
                        let arrow_value =
                            v.to_scalar().context(crate::expr::error::DatafusionSnafu {
                                context: "can't convert key values to arrow value",
                            })?;
                        key_scalar_value.push(arrow_value);
                    }
                    key_scalar_value
                };

                // first compute equal from separate columns
                let eq_results = key_scalar_value
                    .into_iter()
                    .zip(key_batch.batch().iter())
                    .map(|(key, col)| {
                        // TODO(discord9): this takes half of the cpu! And this is redundant amount of `eq`!
                        arrow::compute::kernels::cmp::eq(&key, &col.to_arrow_array().as_ref() as _)
                    })
                    .try_collect::<_, Vec<_>, _>()
                    .context(ArrowSnafu {
                        context: "Failed to compare key values",
                    })?;

                // then combine all equal results to finally found equal key rows
                let opt_eq_mask = eq_results
                    .into_iter()
                    .fold(None, |acc, v| match acc {
                        Some(Ok(acc)) => Some(arrow::compute::kernels::boolean::and(&acc, &v)),
                        Some(Err(_)) => acc,
                        None => Some(Ok(v)),
                    })
                    .transpose()
                    .context(ArrowSnafu {
                        context: "Failed to combine key comparison results",
                    })?;

                let key_eq_mask = if let Some(eq_mask) = opt_eq_mask {
                    BooleanVector::from(eq_mask)
                } else {
                    // if None, meaning key_batch's column number is zero, which means
                    // the key is empty, so we just return a mask of all true
                    // meaning taking all values
                    BooleanVector::from(vec![true; key_batch.row_count()])
                };
                // TODO: both slice and mutate remaining batch

                let cur_val_batch = val_batch.filter(&key_eq_mask)?;

                key_to_many_vals
                    .entry(key_row)
                    .or_default()
                    .push(cur_val_batch);
            }

            Ok(())
        });
    }

    trace!(
        "Reduce take {} batches, {} rows",
        input_batch_count,
        input_row_count
    );

    // write lock the arrange for the rest of the function body
    // to prevent wired race condition
    let mut arrange = arrange.write();
    let mut all_arrange_updates = Vec::with_capacity(key_to_many_vals.len());

    let mut all_output_dict = BTreeMap::new();

    for (key, val_batches) in key_to_many_vals {
        err_collector.run(|| -> Result<(), _> {
            let (accums, _, _) = arrange.get(now, &key).unwrap_or_default();
            let accum_list =
                from_accum_values_to_live_accums(accums.unpack(), accum_plan.simple_aggrs.len())?;

            let mut accum_output = AccumOutput::new();
            for AggrWithIndex {
                expr,
                input_idx,
                output_idx,
            } in accum_plan.simple_aggrs.iter()
            {
                let cur_accum_value = accum_list.get(*output_idx).cloned().unwrap_or_default();
                let mut cur_accum = if cur_accum_value.is_empty() {
                    Accum::new_accum(&expr.func.clone())?
                } else {
                    Accum::try_into_accum(&expr.func, cur_accum_value)?
                };

                for val_batch in val_batches.iter() {
                    // if batch is empty, input null instead
                    let cur_input = val_batch
                        .batch()
                        .get(*input_idx)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(NullVector::new(val_batch.row_count())));
                    let len = cur_input.len();
                    cur_accum.update_batch(&expr.func, VectorDiff::from(cur_input))?;

                    trace!("Reduce accum after take {} rows: {:?}", len, cur_accum);
                }
                let final_output = cur_accum.eval(&expr.func)?;
                trace!("Reduce accum final output: {:?}", final_output);
                accum_output.insert_output(*output_idx, final_output);

                let cur_accum_value = cur_accum.into_state();
                accum_output.insert_accum(*output_idx, cur_accum_value);
            }

            let (new_accums, res_val_row) = accum_output.into_accum_output()?;

            let arrange_update = ((key.clone(), Row::new(new_accums)), now, 1);
            all_arrange_updates.push(arrange_update);

            all_output_dict.insert(key, Row::from(res_val_row));

            Ok(())
        });
    }

    err_collector.run(|| {
        arrange.apply_updates(now, all_arrange_updates)?;
        arrange.compact_to(now)
    });
    // release the lock
    drop(arrange);

    // this output part is not supposed to be resource intensive
    // (because for every batch there wouldn't usually be as many output row?),
    // so we can do some costly operation here
    let output_types = all_output_dict.first_entry().map(|entry| {
        entry
            .key()
            .iter()
            .chain(entry.get().iter())
            .map(|v| v.data_type())
            .collect::<Vec<ConcreteDataType>>()
    });

    if let Some(output_types) = output_types {
        err_collector.run(|| {
            let column_cnt = output_types.len();
            let row_cnt = all_output_dict.len();

            let mut output_builder = output_types
                .into_iter()
                .map(|t| t.create_mutable_vector(row_cnt))
                .collect_vec();

            for (key, val) in all_output_dict {
                for (i, v) in key.into_iter().chain(val.into_iter()).enumerate() {
                    output_builder
                    .get_mut(i)
                    .context(InternalSnafu{
                        reason: format!(
                            "Output builder should have the same length as the row, expected at most {} but got {}", 
                            column_cnt - 1,
                            i
                        )
                    })?
                    .try_push_value_ref(v.as_value_ref())
                    .context(DataTypeSnafu {
                        msg: "Failed to push value",
                    })?;
                }
            }

            let output_columns = output_builder
                .into_iter()
                .map(|mut b| b.to_vector())
                .collect_vec();

            let output_batch = Batch::try_new(output_columns, row_cnt)?;

            trace!("Reduce output batch: {:?}", output_batch);

            send.give(vec![output_batch]);

            Ok(())
        });
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
    SubgraphArg {
        now,
        err_collector,
        scheduler,
        send,
    }: SubgraphArg,
) {
    let key_val = split_rows_to_key_val(data, key_val_plan.clone(), err_collector.clone());
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
        if self.accum.is_empty() && self.output.is_empty() {
            return Ok((vec![], vec![]));
        }
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

    use common_time::{DateTime, Interval, Timestamp};
    use datatypes::data_type::{ConcreteDataType, ConcreteDataType as CDT};
    use hydroflow::scheduled::graph::Hydroflow;

    use super::*;
    use crate::compute::render::test::{get_output_handle, harness_test_ctx, run_and_check};
    use crate::compute::state::DataflowState;
    use crate::expr::{
        self, AggregateExpr, AggregateFunc, BinaryFunc, GlobalId, MapFilterProject, UnaryFunc,
    };
    use crate::plan::Plan;
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

    /// Batch Mode Reduce Evaluation
    /// SELECT SUM(col) FROM table
    ///
    /// table schema:
    /// | name | type  |
    /// |------|-------|
    /// | col  | Int64 |
    #[test]
    fn test_basic_batch_reduce_accum() {
        let mut df = Hydroflow::new();
        let mut state = DataflowState::default();
        let now = state.current_time_ref();
        let mut ctx = harness_test_ctx(&mut df, &mut state);

        let rows = vec![
            (Row::new(vec![1i64.into()]), 1, 1),
            (Row::new(vec![2i64.into()]), 2, 1),
            (Row::new(vec![3i64.into()]), 3, 1),
            (Row::new(vec![1i64.into()]), 4, 1),
            (Row::new(vec![2i64.into()]), 5, 1),
            (Row::new(vec![3i64.into()]), 6, 1),
        ];
        let input_plan = Plan::Constant { rows: rows.clone() };

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
            .render_reduce_batch(
                Box::new(input_plan.with_types(typ.into_unnamed())),
                &key_val_plan,
                &reduce_plan,
                &RelationType::empty(),
            )
            .unwrap();

        {
            let now_inner = now.clone();
            let expected = BTreeMap::<i64, Vec<i64>>::from([
                (1, vec![1i64]),
                (2, vec![3i64]),
                (3, vec![6i64]),
                (4, vec![7i64]),
                (5, vec![9i64]),
                (6, vec![12i64]),
            ]);
            let collection = bundle.collection;
            ctx.df
                .add_subgraph_sink("test_sink", collection.into_inner(), move |_ctx, recv| {
                    let now = *now_inner.borrow();
                    let data = recv.take_inner();
                    let res = data.into_iter().flat_map(|v| v.into_iter()).collect_vec();

                    if let Some(expected) = expected.get(&now) {
                        let batch = expected.iter().map(|v| Value::from(*v)).collect_vec();
                        let batch = Batch::try_from_rows(vec![batch.into()]).unwrap();
                        assert_eq!(res.first(), Some(&batch));
                    }
                });
            drop(ctx);

            for now in 1..7 {
                state.set_current_ts(now);
                state.run_available_with_schedule(&mut df);
                if !state.get_err_collector().is_empty() {
                    panic!(
                        "Errors occur: {:?}",
                        state.get_err_collector().get_all_blocking()
                    )
                }
            }
        }
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
