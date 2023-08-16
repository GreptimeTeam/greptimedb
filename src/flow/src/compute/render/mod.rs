//! for building the flow graph from PLAN
//! this is basically the last step before actually running the flow graph

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, ToStream};
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::worker::Worker as TimelyWorker;

use super::types::DataflowDescription;
use crate::compute::compute_state::ComputeState;
use crate::compute::context::CollectionBundle;
use crate::compute::plan::Plan;
use crate::compute::types::BuildDesc;
use crate::compute::Context;
use crate::expr::Id;
use crate::repr::{self, Row};
use crate::storage::errors::DataflowError;

mod reduce;

/// Assemble the "compute"  side of a dataflow, i.e. all but the sources.
///
/// This method imports sources from provided assets, and then builds the remaining
/// dataflow using "compute-local" assets like shared arrangements, and producing
/// both arrangements and sinks.
pub fn build_compute_dataflow<A: Allocate>(
    timely_worker: &mut TimelyWorker<A>,
    compute_state: &mut ComputeState,
    dataflow: DataflowDescription<Plan, ()>,
) {
    todo!()
}

pub trait RenderTimestamp: Timestamp + Lattice + Refines<repr::Timestamp> {
    /// The system timestamp component of the timestamp.
    ///
    /// This is useful for manipulating the system time, as when delaying
    /// updates for subsequent cancellation, as with monotonic reduction.
    fn system_time(&mut self) -> &mut repr::Timestamp;
    /// Effects a system delay in terms of the timestamp summary.
    fn system_delay(delay: repr::Timestamp) -> <Self as Timestamp>::Summary;
    /// The event timestamp component of the timestamp.
    fn event_time(&mut self) -> &mut repr::Timestamp;
    /// Effects an event delay in terms of the timestamp summary.
    fn event_delay(delay: repr::Timestamp) -> <Self as Timestamp>::Summary;
    /// Steps the timestamp back so that logical compaction to the output will
    /// not conflate `self` with any historical times.
    fn step_back(&self) -> Self;
}

impl RenderTimestamp for repr::Timestamp {
    fn system_time(&mut self) -> &mut repr::Timestamp {
        self
    }
    fn system_delay(delay: repr::Timestamp) -> <Self as Timestamp>::Summary {
        delay
    }
    fn event_time(&mut self) -> &mut repr::Timestamp {
        self
    }
    fn event_delay(delay: repr::Timestamp) -> <Self as Timestamp>::Summary {
        delay
    }
    fn step_back(&self) -> Self {
        self.saturating_sub(1)
    }
}

// This implementation block allows child timestamps to vary from parent timestamps.
impl<G> Context<G, Row>
where
    G: Scope,
    G::Timestamp: RenderTimestamp,
{
    /// render plan and insert into context with given GlobalId
    pub(crate) fn build_object(&mut self, object: BuildDesc<Plan>) {
        // First, transform the relation expression into a render plan.
        let bundle = self.render_plan(object.plan);
        self.insert_id(Id::Global(object.id), bundle);
    }
}

impl<S> Context<S, Row>
where
    S: Scope,
    S::Timestamp: RenderTimestamp,
{
    /// Renders a plan to a differential dataflow, producing the collection of results.
    ///
    /// The return type reflects the uncertainty about the data representation, perhaps
    /// as a stream of data, perhaps as an arrangement, perhaps as a stream of batches.
    pub fn render_plan(&mut self, plan: Plan) -> CollectionBundle<S, Row> {
        match plan {
            Plan::Constant { rows } => {
                dbg!(&rows);
                let (rows, errs) = match rows {
                    Ok(rows) => (rows, Vec::new()),
                    Err(err) => (Vec::new(), vec![err]),
                };
                let since_frontier = self.since_frontier.clone();
                let until = self.until_frontier.clone();
                let ok_collection = rows
                    .into_iter()
                    .filter_map(move |(row, mut time, diff)| {
                        dbg!(&row);
                        time.advance_by(since_frontier.borrow());
                        if !until.less_equal(&time) {
                            Some((
                                row,
                                <S::Timestamp as Refines<repr::Timestamp>>::to_inner(time),
                                diff,
                            ))
                        } else {
                            None
                        }
                    })
                    .to_stream(&mut self.scope)
                    .as_collection();
                let mut error_time: repr::Timestamp = Timestamp::minimum();
                error_time.advance_by(self.since_frontier.borrow());
                let err_collection = errs
                    .into_iter()
                    .map(move |e| {
                        (
                            DataflowError::from(e),
                            <S::Timestamp as Refines<repr::Timestamp>>::to_inner(error_time),
                            1,
                        )
                    })
                    .to_stream(&mut self.scope)
                    .as_collection();
                CollectionBundle::from_collections(ok_collection, err_collection)
            }
            Plan::Get { id, keys, plan } => {
                // Recover the collection from `self` and then apply `mfp` to it.
                // If `mfp` happens to be trivial, we can just return the collection.
                let mut collection = self
                    .lookup_id(id)
                    .unwrap_or_else(|| panic!("Get({:?}) not found at render time", id));
                match plan {
                    crate::compute::plan::GetPlan::PassArrangements => {
                        // Assert that each of `keys` are present in `collection`.
                        if !keys
                            .arranged
                            .iter()
                            .all(|(key, _, _)| collection.arranged.contains_key(key))
                        {
                            let not_included: Vec<_> = keys
                                .arranged
                                .iter()
                                .filter(|(key, _, _)| !collection.arranged.contains_key(key))
                                .map(|(key, _, _)| key)
                                .collect();
                            panic!(
                                "Those keys {:?} is not included in collections keys:{:?}",
                                not_included,
                                collection.arranged.keys().cloned().collect::<Vec<_>>()
                            );
                        }
                        assert!(keys.raw <= collection.collection.is_some());
                        // Retain only those keys we want to import.
                        collection.arranged.retain(|key, _val| {
                            keys.arranged.iter().any(|(key2, _, _)| key2 == key)
                        });
                        collection
                    }
                    crate::compute::plan::GetPlan::Arrangement(key, row, mfp) => {
                        let (oks, errs) = collection.as_collection_core(
                            mfp,
                            Some((key, row)),
                            self.until_frontier.clone(),
                        );
                        CollectionBundle::from_collections(oks, errs)
                    }
                    crate::compute::plan::GetPlan::Collection(mfp) => {
                        let (oks, errs) =
                            collection.as_collection_core(mfp, None, self.until_frontier.clone());
                        CollectionBundle::from_collections(oks, errs)
                    }
                }
            }
            Plan::Let { id, value, body } => {
                // Render `value` and bind it to `id`. Complain if this shadows an id.
                let value = self.render_plan(*value);
                let prebound = self.insert_id(Id::Local(id), value);
                assert!(prebound.is_none());

                let body = self.render_plan(*body);
                self.remove_id(Id::Local(id));
                body
            }
            Plan::Mfp {
                input,
                mfp,
                input_key_val,
            } => {
                let input = self.render_plan(*input);
                // If `mfp` is non-trivial, we should apply it and produce a collection.
                if mfp.is_identity() {
                    input
                } else {
                    let (oks, errs) =
                        input.as_collection_core(mfp, input_key_val, self.until_frontier.clone());
                    CollectionBundle::from_collections(oks, errs)
                }
            }
            Plan::Reduce {
                input,
                key_val_plan,
                plan,
                input_key,
            } => {
                let input = self.render_plan(*input);
                self.render_reduce(input, key_val_plan, plan, input_key)
            }
            _ => todo!("To be implemented"),
        }
    }
}

#[cfg(test)]
mod test {
    use differential_dataflow::input::InputSession;
    use timely::dataflow::scopes::Child;

    use super::*;
    use crate::expr::{GlobalId, LocalId};
    use crate::repr::Diff;
    #[test]
    #[allow(clippy::print_stdout)]
    fn test_constant_plan_render() {
        let build_descs = vec![BuildDesc {
            id: GlobalId::User(0),
            plan: Plan::Constant {
                rows: Ok(vec![(Row::default(), 0, 1)]),
            },
        }];
        let dataflow = DataflowDescription::<Plan, ()>::new("test".to_string());

        timely::execute_from_args(std::iter::empty::<String>(), move |worker| {
            println!("worker: {:?}", worker.index());
            let mut input = InputSession::<repr::Timestamp, Row, Diff>::new();
            worker.dataflow(|scope: &mut Child<'_, _, repr::Timestamp>| {
                let mut test_ctx = Context::<_, Row, _>::for_dataflow_in(&dataflow, scope.clone());
                for build_desc in &build_descs {
                    test_ctx.build_object(build_desc.clone());
                }
                let input_collection = input.to_collection(scope);
                let err_collection = InputSession::new().to_collection(scope);
                let input_collection =
                    CollectionBundle::from_collections(input_collection, err_collection);

                // insert collection
                test_ctx.insert_id(Id::Local(LocalId(0)), input_collection);

                let inspect = test_ctx
                    .lookup_id(Id::Global(GlobalId::User(0)))
                    .unwrap()
                    .as_specific_collection(None);
                inspect.0.inspect(|x| println!("inspect {:?}", x));
            });
            // input.insert(Row::default());
            input.update(Row::default(), 1);
            input.advance_to(1);
        })
        .expect("Computation terminated abnormally");
    }
}
