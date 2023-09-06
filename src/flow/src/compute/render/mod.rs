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

mod error;
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
                let (rows, errs) = match rows {
                    Ok(rows) => (rows, Vec::new()),
                    Err(err) => (Vec::new(), vec![err]),
                };
                let since_frontier = self.since_frontier.clone();
                let until = self.until_frontier.clone();
                let ok_collection = rows
                    .into_iter()
                    .filter_map(move |(row, mut time, diff)| {
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
    use std::any::Any;
    use std::collections::{BTreeMap, BTreeSet};
    use std::rc::Rc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;
    use differential_dataflow::input::{Input, InputSession};
    use differential_dataflow::Collection;
    use timely::dataflow::scopes::Child;
    use timely::dataflow::Stream;
    use timely::Config;

    use super::*;
    use crate::compute::plan::{
        AccumulablePlan, AvailableCollections, GetPlan, KeyValPlan, ReducePlan,
    };
    use crate::expr::{
        AggregateExpr, BinaryFunc, GlobalId, LocalId, MapFilterProject, SafeMfpPlan, ScalarExpr,
        UnaryFunc,
    };
    use crate::repr::Diff;
    type OkStream<G> = Stream<G, (Row, repr::Timestamp, Diff)>;
    type ErrStream<G> = Stream<G, (DataflowError, repr::Timestamp, Diff)>;
    type OkCollection<G> = Collection<G, Row, Diff>;
    type ErrCollection<G> = Collection<G, DataflowError, Diff>;
    /// used as a token to prevent certain resources from being dropped
    type AnyToken = Rc<dyn Any>;
    struct MockSourceToken {
        handle: InputSession<repr::Timestamp, Row, Diff>,
        err_handle: InputSession<repr::Timestamp, DataflowError, Diff>,
    }

    fn mock_input_session(input: &mut InputSession<repr::Timestamp, Row, Diff>, cnt: i64) {
        // TODO: mock a cpu usage monotonic input with timestamp
        // cpu, mem, ts
        // f32, f32, DateTime
        let schema = [
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::datetime_datatype(),
        ];
        let arrs = (0..cnt).map(|i| (i as f32 / cnt as f32, i as f32 / cnt as f32, i));
        // need more mechanism to make timestamp also timestamp here
        for (cpu, mem, ts) in arrs {
            input.update(
                Row::pack(vec![cpu.into(), mem.into(), Value::DateTime(ts.into())]),
                1,
            );
            input.advance_to(ts as u64)
        }
        input.flush();
    }

    // a simple test to see if the dataflow can be built and run
    fn exec_dataflow(
        input_id: Vec<Id>,
        dataflow: DataflowDescription<Plan>,
        sink_ids: Vec<GlobalId>,
        output_keys: Vec<Option<Vec<ScalarExpr>>>,
        input_mock_length: i64,
    ) {
        timely::execute(Config::thread(), move |worker| {
            println!("worker: {:?}", worker.index());
            let mut input = InputSession::<repr::Timestamp, Row, Diff>::new();
            worker.dataflow_named(
                "ProofOfConcept",
                |scope: &mut Child<'_, _, repr::Timestamp>| {
                    let mut test_ctx =
                        Context::<_, Row, _>::for_dataflow_in(&dataflow, scope.clone());

                    let ok_collection = input.to_collection(scope);
                    let (err_handle, err_collection) = scope.new_collection();
                    let input_collection =
                        CollectionBundle::<_, _, repr::Timestamp>::from_collections(
                            ok_collection,
                            err_collection,
                        );

                    // TODO: generate `import_sources` from `dataflow.source_imports`
                    let import_sources: Vec<_> = input_id
                        .clone()
                        .into_iter()
                        .zip(vec![input_collection])
                        .collect();

                    // import sources
                    for (id, collection) in import_sources {
                        test_ctx.insert_id(id, collection);
                    }

                    for build_desc in &dataflow.objects_to_build {
                        test_ctx.build_object(build_desc.clone());
                    }

                    dbg!(test_ctx.bindings.keys());

                    // TODO: export sinks

                    for (sink, output_key) in sink_ids.iter().zip(output_keys.iter()) {
                        let sink = *sink;
                        println!("Inspecting sink {:?}", sink.clone());
                        let inspect = test_ctx.lookup_id(Id::Global(sink)).unwrap();
                        dbg!(inspect.collection.is_some());
                        dbg!(inspect.arranged.keys());
                        let inspect = inspect.as_specific_collection(output_key.as_deref());
                        inspect
                            .0
                            .inspect(move |x| println!("inspect {:?} {:?}", sink.clone(), x));
                    }
                },
            );
            mock_input_session(&mut input, input_mock_length);
        })
        .expect("Computation terminated abnormally");
    }

    #[test]
    fn test_simple_poc_reduce_group_by() {
        // 1. build dataflow with input collection connected
        // 2. give input
        // type annotation is needed to prevent rust-analyzer to give up type deduction

        // simple give dataflow information
        // will be build by given dataflow information from other nodes later
        // key is the third column
        let place_holder =
            ScalarExpr::Literal(Ok(Value::Boolean(true)), ConcreteDataType::int64_datatype());

        let count_col = |i: usize| AggregateExpr {
            func: crate::expr::AggregateFunc::Count,
            expr: ScalarExpr::Column(i),
            distinct: false,
        };
        let sum_col = |i: usize| AggregateExpr {
            func: crate::expr::AggregateFunc::SumFloat32,
            expr: ScalarExpr::Column(i),
            distinct: false,
        };
        // equal to `SELECT minute, SUM(cpu) FROM input GROUP BY ts/300 as minute;
        // cpu, mem, ts
        // --map--> cpu, mem, ts/300
        // --reduce--> ts/300, AVG(cpu), AVG(mem)
        let cast_datetime = ScalarExpr::CallUnary {
            func: UnaryFunc::CastDatetimeToInt64,
            expr: Box::new(ScalarExpr::Column(2)),
        };
        let ts_div_5 = ScalarExpr::CallBinary {
            func: BinaryFunc::DivInt64,
            expr1: Box::new(cast_datetime),
            expr2: Box::new(ScalarExpr::Literal(
                Ok(Value::Int64(5.into())),
                ConcreteDataType::int64_datatype(),
            )),
        };
        let cast_int64_to_float32 = |i: usize| ScalarExpr::CallUnary {
            func: UnaryFunc::CastInt64ToFloat32,
            expr: Box::new(ScalarExpr::Column(i)),
        };
        let reduce_group_by_window = vec![
            // cpu, mem, ts
            // --reduce--> ts/300, SUM(cpu), SUM(mem), COUNT(cpu), COUNT(mem)
            // -- map --> ts/300, AVG(cpu), AVG(mem)
            BuildDesc {
                id: GlobalId::User(0),
                plan: Plan::Reduce {
                    input: Box::new(Plan::Get {
                        id: Id::Global(GlobalId::System(0)),
                        keys: AvailableCollections::new_raw(),
                        plan: GetPlan::Collection(
                            MapFilterProject::new(3).map([ts_div_5]).project([0, 1, 3]),
                        ),
                    }),
                    key_val_plan: KeyValPlan {
                        key_plan: SafeMfpPlan {
                            mfp: MapFilterProject::new(3).project([2]),
                        },
                        val_plan: SafeMfpPlan {
                            mfp: MapFilterProject::new(3).project([0, 1]),
                        },
                    },
                    // --reduce--> ts/300(key), SUM(cpu), SUM(mem), COUNT(cpu), COUNT(mem)
                    plan: ReducePlan::Accumulable(AccumulablePlan {
                        full_aggrs: vec![sum_col(0), sum_col(1), count_col(0), count_col(1)],
                        simple_aggrs: vec![
                            (0, 0, sum_col(0)),
                            (1, 1, sum_col(1)),
                            (2, 0, count_col(0)),
                            (3, 1, count_col(1)),
                        ],
                        distinct_aggrs: vec![],
                    }),
                    input_key: None,
                },
            },
            // 0            1           2       3           4
            // ts/300(key), SUM(cpu), SUM(mem), COUNT(cpu), COUNT(mem),
            // -- map --> AVG(cpu), AVG(mem), ts/300
            BuildDesc {
                id: GlobalId::User(1),
                plan: Plan::Get {
                    id: Id::Global(GlobalId::User(0)),
                    // not used since plan is GetPlan::Arrangement
                    keys: AvailableCollections::new_raw(),
                    plan: GetPlan::Arrangement(
                        vec![ScalarExpr::Column(0)],
                        None,
                        MapFilterProject::new(5)
                            .map([
                                ScalarExpr::CallBinary {
                                    func: BinaryFunc::DivFloat32,
                                    expr1: Box::new(ScalarExpr::Column(1)),
                                    expr2: Box::new(cast_int64_to_float32(3)),
                                },
                                ScalarExpr::CallBinary {
                                    func: BinaryFunc::DivFloat32,
                                    expr1: Box::new(ScalarExpr::Column(2)),
                                    expr2: Box::new(cast_int64_to_float32(4)),
                                },
                            ])
                            .project([0, 5, 6]),
                    ),
                },
            },
        ];
        let input_id = vec![Id::Global(GlobalId::System(0))];
        let dataflow = {
            let mut dataflow = DataflowDescription::<Plan, ()>::new("test".to_string());
            dataflow.objects_to_build = reduce_group_by_window;
            dataflow
        };
        let sink_ids = [GlobalId::User(0), GlobalId::User(1)];
        exec_dataflow(
            input_id.clone(),
            dataflow.clone(),
            sink_ids.to_vec(),
            vec![Some(vec![ScalarExpr::Column(0)]), None],
            10,
        );
    }

    #[test]
    fn test_simple_poc_reduce_count() {
        // 1. build dataflow with input collection connected
        // 2. give input
        // type annotation is needed to prevent rust-analyzer to give up type deduction

        // simple give dataflow information
        // will be build by given dataflow information from other nodes later
        // key is the third column
        let place_holder =
            ScalarExpr::Literal(Ok(Value::Boolean(true)), ConcreteDataType::int64_datatype());
        let key_plan = SafeMfpPlan {
            mfp: MapFilterProject::new(3)
                .map([place_holder.clone()])
                .project([3]),
        };
        let val_plan = SafeMfpPlan {
            mfp: MapFilterProject::new(3).project([0, 1, 2]),
        };
        let count = AggregateExpr {
            func: crate::expr::AggregateFunc::Count,
            expr: place_holder,
            distinct: false,
        };
        // equal to `SELECT COUNT(*) FROM input;`
        let reduce_group_by_window = vec![
            // count(true)
            BuildDesc {
                id: GlobalId::User(0),
                plan: Plan::Reduce {
                    input: Box::new(Plan::Get {
                        id: Id::Global(GlobalId::System(0)),
                        keys: AvailableCollections::new_raw(),
                        plan: GetPlan::Collection(MapFilterProject::new(3)),
                    }),
                    key_val_plan: KeyValPlan { key_plan, val_plan },
                    plan: ReducePlan::Accumulable(AccumulablePlan {
                        full_aggrs: vec![count.clone()],
                        simple_aggrs: vec![(0, 0, count)],
                        distinct_aggrs: vec![],
                    }),
                    input_key: None,
                },
            },
            // get second column
            BuildDesc {
                id: GlobalId::User(1),
                plan: Plan::Get {
                    id: Id::Global(GlobalId::User(0)),
                    // not used since plan is GetPlan::Arrangement
                    keys: AvailableCollections::new_raw(),
                    plan: GetPlan::Arrangement(
                        vec![ScalarExpr::Column(0)],
                        None,
                        MapFilterProject::new(2).project([1]),
                    ),
                },
            },
        ];
        let input_id = vec![Id::Global(GlobalId::System(0))];
        let dataflow = {
            let mut dataflow = DataflowDescription::<Plan, ()>::new("test".to_string());
            dataflow.objects_to_build = reduce_group_by_window;
            dataflow
        };
        let sink_ids = [GlobalId::User(1)];
        exec_dataflow(
            input_id.clone(),
            dataflow.clone(),
            sink_ids.to_vec(),
            vec![None],
            10,
        );
    }

    #[test]
    fn test_simple_poc_reduce_distinct() {
        // 1. build dataflow with input collection connected
        // 2. give input
        // type annotation is needed to prevent rust-analyzer to give up type deduction

        // simple give dataflow information
        // will be build by given dataflow information from other nodes later
        // window need date_trunc which is still WIP
        // key is the third column
        let key_plan = SafeMfpPlan {
            mfp: MapFilterProject::new(3).project([2]),
        };
        let val_plan = SafeMfpPlan {
            mfp: MapFilterProject::new(3).project([0, 1]),
        };
        // equal to `SELECT ts, COUNT(*) FROM input GROUP BY ts;`
        let reduce_plan = vec![BuildDesc {
            id: GlobalId::User(0),
            plan: Plan::Reduce {
                input: Box::new(Plan::Get {
                    id: Id::Global(GlobalId::System(0)),
                    keys: AvailableCollections::new_raw(),
                    plan: GetPlan::Collection(MapFilterProject::new(3)),
                }),
                key_val_plan: KeyValPlan { key_plan, val_plan },
                plan: ReducePlan::Distinct,
                input_key: None,
            },
        }];
        let input_id = vec![Id::Global(GlobalId::System(0))];
        let dataflow = {
            let mut dataflow = DataflowDescription::<Plan, ()>::new("test".to_string());
            dataflow.objects_to_build = reduce_plan;
            dataflow
        };
        let sink_ids = [GlobalId::User(0)];
        exec_dataflow(
            input_id.clone(),
            dataflow.clone(),
            sink_ids.to_vec(),
            vec![Some(vec![ScalarExpr::Column(0)])],
            10,
        );
    }
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
