use std::collections::BTreeMap;

use common_telemetry::logging;
use datatypes::value::Value;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::{Collection, Hashable};
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use crate::compute::context::{Arrangement, ArrangementFlavor, CollectionBundle};
use crate::compute::plan::{
    convert_indexes_to_skips, AccumulablePlan, BucketedPlan, KeyValPlan, ReducePlan,
};
use crate::compute::render::error::MaybeValidatingRow;
use crate::compute::typedefs::{ErrValSpine, RowKeySpine, RowSpine};
use crate::compute::Context;
use crate::expr::{AggregateFunc, ScalarExpr};
use crate::repr::{Diff, Row};
use crate::storage::errors::{DataflowError, EvalError};
use crate::util::{CollectionExt, ReduceExt};

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders a `Plan::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
    #[allow(clippy::type_complexity)]
    pub fn render_reduce(
        &mut self,
        input: CollectionBundle<G, Row, T>,
        key_val_plan: KeyValPlan,
        reduce_plan: ReducePlan,
        input_key: Option<Vec<ScalarExpr>>,
    ) -> CollectionBundle<G, Row, T> {
        input.scope().region_named("Reduce", |inner| {
            let KeyValPlan {
                mut key_plan,
                mut val_plan,
            } = key_val_plan;
            let key_arity = key_plan.projection.len();
            let mut row_buf = Row::default();
            let mut row_mfp = Row::default();
            let (key_val_input, err_input): (
                timely::dataflow::Stream<_, (Result<(Row, Row), DataflowError>, _, _)>,
                _,
            ) = input
                .enter_region(inner)
                .flat_map(input_key.map(|k| (k, None)), || {
                    // Determine the columns we'll need from the row.
                    let mut demand = Vec::new();
                    demand.extend(key_plan.demand());
                    demand.extend(val_plan.demand());
                    demand.sort();
                    demand.dedup();
                    // remap column references to the subset we use.
                    let mut demand_map = BTreeMap::new();
                    for column in demand.iter() {
                        demand_map.insert(*column, demand_map.len());
                    }
                    let demand_map_len = demand_map.len();
                    key_plan.permute(demand_map.clone(), demand_map_len);
                    val_plan.permute(demand_map, demand_map_len);
                    let skips = convert_indexes_to_skips(demand);
                    move |row_parts, time, diff| {
                        let mut row_iter = row_parts
                            .iter()
                            .flat_map(|row| (**row).to_owned().into_iter());
                        let mut datums_local = Vec::new();
                        // Unpack only the demanded columns.
                        for skip in skips.iter() {
                            datums_local.push(row_iter.nth(*skip).unwrap());
                        }

                        // Evaluate the key expressions.
                        let key = match key_plan.evaluate_into(&mut datums_local, &mut row_mfp) {
                            Err(e) => {
                                return Some((Err(DataflowError::from(e)), time.clone(), *diff))
                            }
                            Ok(key) => key.expect("Row expected as no predicate was used"),
                        };
                        // Evaluate the value expressions.
                        // The prior evaluation may have left additional columns we should delete.
                        datums_local.truncate(skips.len());
                        let val = match val_plan.evaluate_iter(&mut datums_local) {
                            Err(e) => {
                                return Some((Err(DataflowError::from(e)), time.clone(), *diff))
                            }
                            Ok(val) => val.expect("Row expected as no predicate was used"),
                        };
                        row_buf.clear();
                        row_buf.extend(val);
                        let row = row_buf.clone();
                        Some((Ok((key, row)), time.clone(), *diff))
                    }
                });

            // Demux out the potential errors from key and value selector evaluation.
            let (ok, mut err) = key_val_input
                .as_collection()
                .consolidate_stream()
                .flat_map_fallible("OkErrDemux", Some);

            err = err.concat(&err_input);

            // Render the reduce plan
            self.render_reduce_plan(reduce_plan, ok, err, key_arity)
                .leave_region()
        })
    }

    /// Render a dataflow based on the provided plan.
    ///
    /// The output will be an arrangements that looks the same as if
    /// we just had a single reduce operator computing everything together, and
    /// this arrangement can also be re-used.
    fn render_reduce_plan<S>(
        &self,
        plan: ReducePlan,
        collection: Collection<S, (Row, Row), Diff>,
        err_input: Collection<S, DataflowError, Diff>,
        key_arity: usize,
    ) -> CollectionBundle<S, Row, T>
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let mut errors = Vec::default();
        let arrangement = self.render_reduce_plan_inner(plan, collection, &mut errors, key_arity);
        CollectionBundle::from_columns(
            0..key_arity,
            ArrangementFlavor::Local(
                arrangement,
                err_input
                    .concatenate(errors)
                    .arrange_named("Arrange bundle err"),
            ),
        )
    }

    fn render_reduce_plan_inner<S>(
        &self,
        plan: ReducePlan,
        collection: Collection<S, (Row, Row), Diff>,
        errors: &mut Vec<Collection<S, DataflowError, Diff>>,
        key_arity: usize,
    ) -> Arrangement<S, Row>
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let arrangement: Arrangement<S, Row> = match plan {
            // If we have no aggregations or just a single type of reduction, we
            // can go ahead and render them directly.
            ReducePlan::Distinct => {
                let (arranged_output, errs) = self.build_distinct(collection);
                errors.push(errs);
                arranged_output
            }
            ReducePlan::Accumulable(expr) => {
                let (arranged_output, errs) = self.build_accumulable(collection, expr);
                errors.push(errs);
                arranged_output
            }

            // TODO(discord9): impl Distinct&Accumulate first
            _ => todo!(),
        };
        todo!()
    }

    /// Build the dataflow to compute the set of distinct keys.
    fn build_distinct<S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
    ) -> (Arrangement<S, Row>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let (output, errors) = collection
            .arrange_named::<RowSpine<_, _, _, _>>("Arranged DistinctBy")
            .reduce_pair::<_, RowSpine<_, _, _, _>, _, ErrValSpine<_, _, _>>(
                "DistinctBy",
                "DistinctByErrorCheck",
                |_key: &Row, _input: &[(&Row, i64)], output: &mut Vec<(Row, i64)>| {
                    // We're pushing an empty row here because the key is implicitly added by the
                    // arrangement, and the permutation logic takes care of using the key part of the
                    // output.
                    output.push((Row::default(), 1));
                },
                move |key, input: &[(_, Diff)], output| {
                    for (_, count) in input.iter() {
                        if count.is_positive() {
                            continue;
                        }
                        let message = "Non-positive multiplicity in DistinctBy";
                        output.push((EvalError::Internal(message.to_string()).into(), 1));
                        return;
                    }
                },
            );
        (
            output,
            errors.as_collection(|_k: &Row, v: &DataflowError| v.clone()),
        )
    }

    /// Build the dataflow to compute and arrange multiple hierarchical aggregations
    /// on non-monotonic inputs.
    ///
    /// This function renders a single reduction tree that computes aggregations with
    /// a priority queue implemented with a series of reduce operators that partition
    /// the input into buckets, and compute the aggregation over very small buckets
    /// and feed the results up to larger buckets.
    ///
    /// Note that this implementation currently ignores the distinct bit because we
    /// currently only perform min / max hierarchically and the reduction tree
    /// efficiently suppresses non-distinct updates.
    fn build_bucketed<S>(
        &self,
        input: Collection<S, (Row, Row), Diff>,
        BucketedPlan {
            aggr_funcs,
            skips,
            buckets,
        }: BucketedPlan,
    ) -> (
        Arrangement<S, Row>,
        Option<Collection<S, DataflowError, Diff>>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let mut err_output: Option<Collection<S, _, _>> = None;
        let arranged_output = input.scope().region_named("ReduceHierarchical", |inner| {
            let input = input.enter(inner);

            // Gather the relevant values into a vec of rows ordered by aggregation_index
            let mut row_buf = Row::default();
            let input = input.map(move |(key, row)| {
                let mut values = Vec::with_capacity(skips.len());
                let mut row_iter = row.iter();
                for skip in skips.iter() {
                    row_buf.packer().push(row_iter.nth(*skip).unwrap().clone());
                    values.push(row_buf.clone());
                }

                (key, values)
            });

            // Repeatedly apply hierarchical reduction with a progressively coarser key.
            let mut stage = input.map(move |(key, values)| ((key, values.hashed()), values));
            let mut validating = true;
            for b in buckets.into_iter() {
                let input = stage.map(move |((key, hash), values)| ((key, hash % b), values));

                // We only want the first stage to perform validation of whether invalid accumulations
                // were observed in the input. Subsequently, we will either produce an error in the error
                // stream or produce correct data in the output stream.
                let negated_output = if validating {
                    let (oks, errs) = self
                        .build_bucketed_negated_output::<_, Result<Vec<Row>, (Row, u64)>>(
                            &input,
                            aggr_funcs.clone(),
                        )
                        .map_fallible(
                            "Checked Invalid Accumulations",
                            |(key, result)| match result {
                                Err((key, _)) => {
                                    let message = format!(
                                        "Invalid data in source, saw non-positive accumulation \
                                         for key {key:?} in hierarchical mins-maxes aggregate"
                                    );
                                    Err(EvalError::Internal(message).into())
                                }
                                Ok(values) => Ok((key, values)),
                            },
                        );
                    validating = false;
                    err_output = Some(errs.leave_region());
                    oks
                } else {
                    self.build_bucketed_negated_output::<_, Vec<Row>>(&input, aggr_funcs.clone())
                };

                stage = negated_output
                    .negate()
                    .concat(&input)
                    .consolidate_named::<RowKeySpine<_, _, _>>(
                        "Consolidated MinsMaxesHierarchical",
                    );
            }

            // Discard the hash from the key and return to the format of the input data.
            let partial = stage.map(|((key, _hash), values)| (key, values));

            // Build a series of stages for the reduction
            // Arrange the final result into (key, Row)
            let arranged =
                partial.arrange_named::<RowSpine<_, Vec<Row>, _, _>>("Arrange ReduceMinsMaxes");
            // Note that we would prefer to use `mz_timely_util::reduce::ReduceExt::reduce_pair` here,
            // but we then wouldn't be able to do this error check conditionally.  See its documentation
            // for the rationale around using a second reduction here.
            if validating {
                let errs = arranged
                    .reduce_abelian::<_, ErrValSpine<_, _, _>>(
                        "ReduceMinsMaxes Error Check",
                        move |_key, source, target| {
                            // Negative counts would be surprising, but until we are 100% certain we wont
                            // see them, we should report when we do. We may want to bake even more info
                            // in here in the future.
                            for (val, count) in source.iter() {
                                if count.is_positive() {
                                    continue;
                                }

                                let message = "Non-positive accumulation in ReduceMinsMaxes";
                                logging::error!("{message}: val={val:?}, count={count}");
                                target.push((EvalError::Internal(message.to_string()).into(), 1));
                                return;
                            }
                        },
                    )
                    .as_collection(|_, v| v.clone());
                err_output = Some(errs.leave_region());
            }
            arranged
                .reduce_abelian::<_, RowSpine<_, _, _, _>>("ReduceMinsMaxes", {
                    let mut row_buf = Row::default();
                    move |_key, source: &[(&Vec<Row>, Diff)], target: &mut Vec<(Row, Diff)>| {
                        let mut row_packer = row_buf.packer();
                        for (aggr_index, func) in aggr_funcs.iter().enumerate() {
                            let iter = source
                                .iter()
                                .map(|(values, _cnt)| values[aggr_index].iter().next().unwrap());
                            row_packer.push(func.eval(iter.cloned()));
                        }
                        target.push((row_buf.clone(), 1));
                    }
                })
                .leave_region()
        });
        (arranged_output, err_output)
    }

    /// Build the dataflow for one stage of a reduction tree for multiple hierarchical
    /// aggregates.
    ///
    /// `buckets` indicates the number of buckets in this stage. We do some non
    /// obvious trickery here to limit the memory usage per layer by internally
    /// holding only the elements that were rejected by this stage. However, the
    /// output collection maintains the `((key, bucket), (passing value)` for this
    /// stage.
    /// `validating` indicates whether we want this stage to perform error detection
    /// for invalid accumulations. Once a stage is clean of such errors, subsequent
    /// stages can skip validation.
    fn build_bucketed_negated_output<S, R>(
        &self,
        input: &Collection<S, ((Row, u64), Vec<Row>), Diff>,
        aggrs: Vec<AggregateFunc>,
    ) -> Collection<S, ((Row, u64), R), Diff>
    where
        S: Scope<Timestamp = G::Timestamp>,
        R: MaybeValidatingRow<Vec<Row>, (Row, u64)>,
    {
        let arranged_input = input
            .arrange_named::<RowSpine<_, Vec<Row>, _, _>>("Arranged MinsMaxesHierarchical input");

        arranged_input
            .reduce_abelian::<_, RowSpine<_, _, _, _>>(
                "Reduced Fallibly MinsMaxesHierarchical",
                move |key, source, target| {
                    if let Some(err) = R::into_error() {
                        // Should negative accumulations reach us, we should loudly complain.
                        for (value, count) in source.iter() {
                            if count.is_positive() {
                                continue;
                            }
                            logging::error!(
                                "Non-positive accumulation in MinsMaxesHierarchical
                                key={key:?}, value={value:?}, count={count}"
                            );
                            // After complaining, output an error here so that we can eventually
                            // report it in an error stream.
                            target.push((err(key.clone()), -1));
                            return;
                        }
                    }
                    let mut output = Vec::with_capacity(aggrs.len());
                    for (aggr_index, func) in aggrs.iter().enumerate() {
                        let iter = source.iter().map(|(values, _cnt)| {
                            values[aggr_index].iter().next().unwrap().clone()
                        });
                        output.push(Row::pack([func.eval(iter)]));
                    }
                    // We only want to arrange the parts of the input that are not part of the output.
                    // More specifically, we want to arrange it so that `input.concat(&output.negate())`
                    // gives us the intended value of this aggregate function. Also we assume that regardless
                    // of the multiplicity of the final result in the input, we only want to have one copy
                    // in the output.
                    target.push((R::ok(output), -1));
                    target.extend(
                        source
                            .iter()
                            .map(|(values, cnt)| (R::ok((*values).clone()), *cnt)),
                    );
                },
            )
            .as_collection(|k, v| (k.clone(), v.clone()))
    }

    /// Build the dataflow to compute and arrange multiple accumulable aggregations.
    ///
    /// The incoming values are moved to the update's "difference" field, at which point
    /// they can be accumulated in place. The `count` operator promotes the accumulated
    /// values to data, at which point a final map applies operator-specific logic to
    /// yield the final aggregate.
    fn build_accumulable<S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
        AccumulablePlan {
            full_aggrs,
            simple_aggrs,
            distinct_aggrs,
        }: AccumulablePlan,
    ) -> (Arrangement<S, Row>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        // we must have called this function with something to reduce
        if full_aggrs.is_empty() || simple_aggrs.len() + distinct_aggrs.len() != full_aggrs.len() {
            panic!(
                "Incorrect numbers of aggregates in accummulable reduction rendering: {}",
                &format!(
                    "full_aggrs={}, simple_aggrs={}, distinct_aggrs={}",
                    full_aggrs.len(),
                    simple_aggrs.len(),
                    distinct_aggrs.len(),
                ),
            );
        }

        // Some of the aggregations may have the `distinct` bit set, which means that they'll
        // need to be extracted from `collection` and be subjected to `distinct` with `key`.
        // Other aggregations can be directly moved in to the `diff` field.
        //
        // In each case, the resulting collection should have `data` shaped as `(key, ())`
        // and a `diff` that is a vector with length `3 * aggrs.len()`. The three values are
        // generally the count, and then two aggregation-specific values. The size could be
        // reduced if we want to specialize for the aggregations.

        let float_scale = f64::from(1 << 24);

        // Instantiate a default vector for diffs with the correct types at each
        // position.
        let zero_diffs: (Vec<_>, Diff) = (
            full_aggrs
                .iter()
                .map(|f| match f.func {
                    AggregateFunc::Any | AggregateFunc::All => Accum::Bool {
                        trues: 0,
                        falses: 0,
                    },
                    AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Accum::Float {
                        accum: 0,
                        pos_infs: 0,
                        neg_infs: 0,
                        nans: 0,
                        non_nulls: 0,
                    },
                    _ => Accum::SimpleNumber {
                        accum: 0,
                        non_nulls: 0,
                    },
                })
                .collect(),
            0,
        );

        let mut to_aggregate = Vec::new();
        if !simple_aggrs.is_empty() {
            // First, collect all non-distinct aggregations in one pass.
            let easy_cases = collection.explode_one({
                let zero_diffs = zero_diffs.clone();
                move |(key, row)| {
                    let mut diffs = zero_diffs.clone();

                    for (accumulable_index, datum_index, aggr) in simple_aggrs.iter() {
                        // Try to unpack only the datums we need
                        let datum = row.get(*datum_index).unwrap().clone();
                        diffs.0[*accumulable_index] =
                            Accum::value_to_accumulator(datum.clone(), &aggr.func);
                        diffs.1 = 1;
                    }
                    ((key, ()), diffs)
                }
            });
            to_aggregate.push(easy_cases);
        }

        // Next, collect all aggregations that require distinctness.
        for (accumulable_index, datum_index, aggr) in distinct_aggrs.into_iter() {
            let mut row_buf = Row::default();
            let collection = {
                let arranged: Arranged<S, _> = collection
                    .map(move |(key, row)| {
                        let value = row.get(datum_index).unwrap();
                        row_buf.packer().push(value.clone());
                        (key, row_buf.clone())
                    })
                    .map(|k| (k, ()))
                    .arrange_named::<RowKeySpine<(Row, Row), <G as ScopeParent>::Timestamp, Diff>>(
                        "Arranged Accumulable",
                    );
                // note `arranged` for convenient of type-infer with r-a
                // first distinct, then reduce
                arranged
                    .reduce_abelian::<_, RowKeySpine<_, _, _>>(
                        "Reduced Accumulable",
                        move |_k, _s, t: &mut Vec<((), i64)>| t.push(((), 1)),
                    )
                    .as_collection(|k, _| k.clone())
                    .explode_one({
                        let zero_diffs = zero_diffs.clone();
                        move |(key, row)| {
                            let datum = row.iter().next().unwrap();
                            let mut diffs = zero_diffs.clone();
                            diffs.0[accumulable_index] =
                                Accum::value_to_accumulator(datum.clone(), &aggr.func);
                            diffs.1 = 1;
                            ((key, ()), diffs)
                        }
                    })
            };
            to_aggregate.push(collection);
        }

        // now concatenate, if necessary, multiple aggregations
        let collection = if to_aggregate.len() == 1 {
            to_aggregate.remove(0)
        } else {
            differential_dataflow::collection::concatenate(&mut collection.scope(), to_aggregate)
        };

        // reduce is done, convert accumulators to values

        let err_full_aggrs = full_aggrs.clone();
        let (arranged_output, arranged_errs) = collection
            .arrange_named::<RowKeySpine<_, _, (Vec<Accum>, Diff)>>("ArrangeAccumulable")
            .reduce_pair::<_, RowSpine<_, _, _, _>, _, ErrValSpine<_, _, _>>(
                "ReduceAccumulable",
                "AccumulableErrorCheck",
                {
                    let mut row_buf = Row::default();
                    move |_key: &Row,
                          input: &[(&(), (Vec<Accum>, Diff))],
                          output: &mut Vec<(Row, i64)>| {
                        let (ref accums, total) = input[0].1;
                        let mut row_packer = row_buf.packer();

                        for (aggr, accum) in full_aggrs.iter().zip(accums) {
                            // The finished value depends on the aggregation function in a variety of ways.
                            // For all aggregates but count, if only null values were
                            // accumulated, then the output is null.
                            let value = if total > 0
                                && accum.is_zero()
                                && aggr.func != AggregateFunc::Count
                            {
                                Value::Null
                            } else {
                                accum.accum_to_value(&aggr.func, total)
                            };

                            row_packer.push(value);
                        }
                        output.push((row_buf.clone(), 1));
                    }
                },
                move |key, input, output| {
                    let (ref accums, total) = input[0].1;
                    for (aggr, accum) in err_full_aggrs.iter().zip(accums) {
                        // We first test here if inputs without net-positive records are present,
                        // producing an error to the logs and to the query output if that is the case.
                        if total == 0 && !accum.is_zero() {
                            logging::error!(
                                "Net-zero records with non-zero accumulation in ReduceAccumulable: aggr={:?}, accum={:?}", aggr, &accum
                            );
                            let message = format!(
                                "Invalid data in source, saw net-zero records for key {key:?} \
                                 with non-zero accumulation in accumulable aggregate"
                            );
                            output.push((EvalError::Internal(message).into(), 1));
                        }
                        match (&aggr.func, &accum) {
                            (AggregateFunc::SumUInt16, Accum::SimpleNumber { accum, .. })
                            | (AggregateFunc::SumUInt32, Accum::SimpleNumber { accum, .. })
                            | (AggregateFunc::SumUInt64, Accum::SimpleNumber { accum, .. }) => {
                                if accum.is_negative() {
                                    logging::error!(
                                    "Invalid negative unsigned aggregation in ReduceAccumulable aggr={aggr:?}, accum={accum:?}",
                                );
                                    let message = format!(
                                        "Invalid data in source, saw negative accumulation with \
                                         unsigned type for key {key:?}"
                                    );
                                    output.push((EvalError::Internal(message).into(), 1));
                                }
                            }
                            _ => (), // no more errors to check for at this point!
                        }
                    }
                },
            );
        (
            arranged_output,
            arranged_errs.as_collection(|_key, error| error.clone()),
        )
    }
}

/// Accumulates values for the various types of accumulable aggregations.
///
/// We assume that there are not more than 2^32 elements for the aggregation.
/// Thus we can perform a summation over i32 in an i64 accumulator
/// and not worry about exceeding its bounds.
///
/// The float accumulator performs accumulation in fixed point arithmetic. The fixed
/// point representation has less precision than a double. It is entirely possible
/// that the values of the accumulator overflow, thus we have to use wrapping arithmetic
/// to preserve group guarantees.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum Accum {
    /// Accumulates boolean values.
    Bool {
        /// The number of `true` values observed.
        trues: Diff,
        /// The number of `false` values observed.
        falses: Diff,
    },
    /// Accumulates simple numeric values.
    SimpleNumber {
        /// The accumulation of all non-NULL values observed.
        accum: i128,
        /// The number of non-NULL values observed.
        non_nulls: Diff,
    },
    /// Accumulates float values.
    Float {
        /// Accumulates non-special float values, mapped to a fixed precision i128 domain to
        /// preserve associativity and commutativity
        accum: i128,
        /// Counts +inf
        pos_infs: Diff,
        /// Counts -inf
        neg_infs: Diff,
        /// Counts NaNs
        nans: Diff,
        /// Counts non-NULL values
        non_nulls: Diff,
    },
}

impl Accum {
    /// For storing floating number in fixed point representation, we need to scale
    const FLOAT_SCALE: f64 = 16777216.0;
    /// Initialize a accumulator from a datum.
    fn value_to_accumulator(datum: Value, aggr: &AggregateFunc) -> Self {
        match aggr {
            AggregateFunc::Count => Accum::SimpleNumber {
                accum: 0, // unused for AggregateFunc::Count
                non_nulls: if datum.is_null() { 0 } else { 1 },
            },
            AggregateFunc::Any | AggregateFunc::All => match datum {
                Value::Boolean(true) => Accum::Bool {
                    trues: 1,
                    falses: 0,
                },
                Value::Null => Accum::Bool {
                    trues: 0,
                    falses: 0,
                },
                Value::Boolean(false) => Accum::Bool {
                    trues: 0,
                    falses: 1,
                },
                x => panic!("Invalid argument to AggregateFunc::Any: {x:?}"),
            },
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => {
                let n = match datum {
                    Value::Float32(n) => f64::from(*n),
                    Value::Float64(n) => *n,
                    Value::Null => 0f64,
                    x => panic!("Invalid argument to AggregateFunc::{aggr:?}: {x:?}"),
                };

                let nans = Diff::from(n.is_nan());
                let pos_infs = Diff::from(n == f64::INFINITY);
                let neg_infs = Diff::from(n == f64::NEG_INFINITY);
                let non_nulls = Diff::from(datum != Value::Null);

                // Map the floating point value onto a fixed precision domain
                // All special values should map to zero, since they are tracked separately
                let accum = if nans > 0 || pos_infs > 0 || neg_infs > 0 {
                    0
                } else {
                    // This operation will truncate to i128::MAX if out of range.
                    // TODO(benesch): rewrite to avoid `as`.
                    #[allow(clippy::as_conversions)]
                    {
                        (n * Self::FLOAT_SCALE) as i128
                    }
                };

                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls,
                }
            }
            _ => {
                // Other accumulations need to disentangle the accumulable
                // value from its NULL-ness, which is not quite as easily
                // accumulated.
                match datum {
                    Value::Int16(i) => Accum::SimpleNumber {
                        accum: i128::from(i),
                        non_nulls: 1,
                    },
                    Value::Int32(i) => Accum::SimpleNumber {
                        accum: i128::from(i),
                        non_nulls: 1,
                    },
                    Value::Int64(i) => Accum::SimpleNumber {
                        accum: i128::from(i),
                        non_nulls: 1,
                    },
                    Value::UInt16(u) => Accum::SimpleNumber {
                        accum: i128::from(u),
                        non_nulls: 1,
                    },
                    Value::UInt32(u) => Accum::SimpleNumber {
                        accum: i128::from(u),
                        non_nulls: 1,
                    },
                    Value::UInt64(u) => Accum::SimpleNumber {
                        accum: i128::from(u),
                        non_nulls: 1,
                    },
                    Value::Timestamp(t) => Accum::SimpleNumber {
                        accum: i128::from(t.value()),
                        non_nulls: 1,
                    },
                    Value::Null => Accum::SimpleNumber {
                        accum: 0,
                        non_nulls: 0,
                    },
                    x => panic!("Accumulating non-integer or unsupported data: {x:?}"),
                }
            }
        }
    }

    fn accum_to_value(&self, func: &AggregateFunc, total: i64) -> Value {
        match (func, &self) {
            (AggregateFunc::Count, Accum::SimpleNumber { non_nulls, .. }) => {
                Value::Int64(*non_nulls)
            }
            (AggregateFunc::All, Accum::Bool { falses, trues }) => {
                // If any false, else if all true, else must be no false and some nulls.
                if *falses > 0 {
                    Value::Boolean(false)
                } else if *trues == total {
                    Value::Boolean(true)
                } else {
                    Value::Null
                }
            }
            (AggregateFunc::Any, Accum::Bool { falses, trues }) => {
                // If any true, else if all false, else must be no true and some nulls.
                if *trues > 0 {
                    Value::Boolean(true)
                } else if *falses == total {
                    Value::Boolean(false)
                } else {
                    Value::Null
                }
            }
            (AggregateFunc::SumInt16, Accum::SimpleNumber { accum, .. })
            | (AggregateFunc::SumInt32, Accum::SimpleNumber { accum, .. }) => {
                // This conversion is safe, as long as we have less than 2^32
                // summands.
                // TODO(benesch): are we guaranteed to have less than 2^32 summands?
                // If so, rewrite to avoid `as`.
                #[allow(clippy::as_conversions)]
                Value::Int64(*accum as i64)
            }
            (AggregateFunc::SumInt64, Accum::SimpleNumber { accum, .. }) => {
                Value::from(*accum as i64)
            }
            (AggregateFunc::SumUInt16, Accum::SimpleNumber { accum, .. })
            | (AggregateFunc::SumUInt32, Accum::SimpleNumber { accum, .. }) => {
                if !accum.is_negative() {
                    // Our semantics of overflow are not clearly articulated.
                    //  We adopt an unsigned
                    // wrapping behavior to match what we do above for signed types.
                    // TODO: remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    Value::UInt64(*accum as u64)
                } else {
                    // Note that we return a value here, but an error in the other
                    // operator of the reduce_pair. Therefore, we expect that this
                    // value will never be exposed as an output.
                    Value::Null
                }
            }
            (AggregateFunc::SumUInt64, Accum::SimpleNumber { accum, .. }) => {
                if !accum.is_negative() {
                    Value::UInt64(*accum as u64)
                } else {
                    // Note that we return a value here, but an error in the other
                    // operator of the reduce_pair. Therefore, we expect that this
                    // value will never be exposed as an output.
                    Value::Null
                }
            }
            (
                AggregateFunc::SumFloat32,
                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: _,
                },
            ) => {
                if *nans > 0 || (*pos_infs > 0 && *neg_infs > 0) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Value::from(f32::NAN)
                } else if *pos_infs > 0 {
                    Value::from(f32::INFINITY)
                } else if *neg_infs > 0 {
                    Value::from(f32::NEG_INFINITY)
                } else {
                    // TODO: remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    {
                        Value::from(((*accum as f64) / Self::FLOAT_SCALE) as f32)
                    }
                }
            }
            (
                AggregateFunc::SumFloat64,
                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls: _,
                },
            ) => {
                if *nans > 0 || (*pos_infs > 0 && *neg_infs > 0) {
                    // NaNs are NaNs and cases where we've seen a
                    // mixture of positive and negative infinities.
                    Value::from(f64::NAN)
                } else if *pos_infs > 0 {
                    Value::from(f64::INFINITY)
                } else if *neg_infs > 0 {
                    Value::from(f64::NEG_INFINITY)
                } else {
                    // TODO(benesch): remove potentially dangerous usage of `as`.
                    #[allow(clippy::as_conversions)]
                    {
                        Value::from((*accum as f64) / Self::FLOAT_SCALE)
                    }
                }
            }
            _ => panic!(
                "Unexpected accumulation (aggr={:?}, accum={:?})",
                func, &self
            ),
        }
    }
}

impl Semigroup for Accum {
    fn is_zero(&self) -> bool {
        match self {
            Accum::Bool { trues, falses } => trues.is_zero() && falses.is_zero(),
            Accum::SimpleNumber { accum, non_nulls } => accum.is_zero() && non_nulls.is_zero(),
            Accum::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => {
                accum.is_zero()
                    && pos_infs.is_zero()
                    && neg_infs.is_zero()
                    && nans.is_zero()
                    && non_nulls.is_zero()
            }
        }
    }

    fn plus_equals(&mut self, other: &Accum) {
        match (&mut *self, other) {
            (
                Accum::Bool { trues, falses },
                Accum::Bool {
                    trues: other_trues,
                    falses: other_falses,
                },
            ) => {
                *trues += other_trues;
                *falses += other_falses;
            }
            (
                Accum::SimpleNumber { accum, non_nulls },
                Accum::SimpleNumber {
                    accum: other_accum,
                    non_nulls: other_non_nulls,
                },
            ) => {
                *accum += other_accum;
                *non_nulls += other_non_nulls;
            }
            (
                Accum::Float {
                    accum,
                    pos_infs,
                    neg_infs,
                    nans,
                    non_nulls,
                },
                Accum::Float {
                    accum: other_accum,
                    pos_infs: other_pos_infs,
                    neg_infs: other_neg_infs,
                    nans: other_nans,
                    non_nulls: other_non_nulls,
                },
            ) => {
                *accum = accum.checked_add(*other_accum).unwrap_or_else(|| {
                    logging::warn!("Float accumulator overflow. Incorrect results possible");
                    accum.wrapping_add(*other_accum)
                });
                *pos_infs += other_pos_infs;
                *neg_infs += other_neg_infs;
                *nans += other_nans;
                *non_nulls += other_non_nulls;
            }
            (l, r) => unreachable!(
                "Accumulator::plus_equals called with non-matching variants: {l:?} vs {r:?}"
            ),
        }
    }
}

impl Multiply<Diff> for Accum {
    type Output = Accum;

    fn multiply(self, factor: &Diff) -> Accum {
        let factor = *factor;
        match self {
            Accum::Bool { trues, falses } => Accum::Bool {
                trues: trues * factor,
                falses: falses * factor,
            },
            Accum::SimpleNumber { accum, non_nulls } => Accum::SimpleNumber {
                accum: accum * i128::from(factor),
                non_nulls: non_nulls * factor,
            },
            Accum::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => Accum::Float {
                accum: accum.checked_mul(i128::from(factor)).unwrap_or_else(|| {
                    logging::warn!("Float accumulator overflow. Incorrect results possible");
                    accum.wrapping_mul(i128::from(factor))
                }),
                pos_infs: pos_infs * factor,
                neg_infs: neg_infs * factor,
                nans: nans * factor,
                non_nulls: non_nulls * factor,
            },
        }
    }
}
