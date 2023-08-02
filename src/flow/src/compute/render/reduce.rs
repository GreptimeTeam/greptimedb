use std::collections::BTreeMap;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use crate::compute::context::CollectionBundle;
use crate::compute::plan::{convert_indexes_to_skips, KeyValPlan, ReducePlan};
use crate::compute::Context;
use crate::expr::ScalarExpr;
use crate::repr::{Diff, Row};
use crate::storage::errors::DataflowError;

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders a `Plan::Reduce` using various non-obvious techniques to
    /// minimize worst-case incremental update times and memory footprint.
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
                                return Some((
                                    Err(DataflowError::from(e)),
                                    time.clone(),
                                    diff.clone(),
                                ))
                            }
                            Ok(key) => key.expect("Row expected as no predicate was used"),
                        };
                        // Evaluate the value expressions.
                        // The prior evaluation may have left additional columns we should delete.
                        datums_local.truncate(skips.len());
                        let val = match val_plan.evaluate_iter(&mut datums_local) {
                            Err(e) => {
                                return Some((
                                    Err(DataflowError::from(e)),
                                    time.clone(),
                                    diff.clone(),
                                ))
                            }
                            Ok(val) => val.expect("Row expected as no predicate was used"),
                        };
                        row_buf.clear();
                        row_buf.extend(val);
                        let row = row_buf.clone();
                        Some((Ok((key, row)), time.clone(), diff.clone()))
                    }
                });
            /*
            // TODO(discord9): deal with errors by adding utility methods to split dataflow
            // Demux out the potential errors from key and value selector evaluation.
            let (ok) = key_val_input
                .as_collection();
                // .consolidate_stream()
                // .flat_map_fallible("OkErrDemux", Some);
            let err = err_input;
            // err = err.concat(&err_input);

            // Render the reduce plan
            self.render_reduce_plan(reduce_plan, ok, err, key_arity)
                .leave_region()*/
            todo!()
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
        todo!()
    }
}
