use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::operator::{self, Operator};
use timely::dataflow::operators::generic::{InputHandle, OperatorInfo, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::{Data, ExchangeData};

use crate::util::buffer::ConsolidateBuffer;

pub trait StreamExt<G, D1>
where
    D1: Data,
    G: Scope,
{
    /// Like `timely::dataflow::operators::generic::operator::Operator::unary`,
    /// but the logic function can handle failures.
    ///
    /// Creates a new dataflow operator that partitions its input stream by a
    /// parallelization strategy `pact` and repeatedly invokes `logic`, the
    /// function returned by the function passed as `constructor`. The `logic`
    /// function can read to the input stream and write to either of two output
    /// streams, where the first output stream represents successful
    /// computations and the second output stream represents failed
    /// computations.
    fn unary_fallible<D2, E, B, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandle<G::Timestamp, D1, P::Puller>,
                    &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                    &mut OutputHandle<G::Timestamp, E, Tee<G::Timestamp, E>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Like [`timely::dataflow::operators::map::Map::flat_map`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// successful applications of `logic`, while the second returned stream
    /// will contain the failed applications.
    fn flat_map_fallible<D2, E, I, L>(&self, name: &str, logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;
}

/// Extension methods for differential [`Collection`]s.
pub trait CollectionExt<G, D1, R>
where
    G: Scope,
    R: Semigroup,
{
    /// Creates a new empty collection in `scope`.
    fn empty(scope: &G) -> Collection<G, D1, R>;

    /// Like [`Collection::map`], but `logic` is allowed to fail. The first
    /// returned collection will contain successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    fn map_fallible<D2, E, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        self.flat_map_fallible(name, move |record| Some(logic(record)))
    }

    /// Like [`Collection::flat_map`], but `logic` is allowed to fail. The first
    /// returned collection will contain the successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    fn flat_map_fallible<D2, E, I, L>(
        &self,
        name: &str,
        logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    /// Replaces each record with another, with a new difference type.
    ///
    /// This method is most commonly used to take records containing aggregatable data (e.g. numbers to be summed)
    /// and move the data into the difference component. This will allow differential dataflow to update in-place.
    fn explode_one<D2, R2, L>(&self, logic: L) -> Collection<G, D2, <R2 as Multiply<R>>::Output>
    where
        D2: differential_dataflow::Data,
        R2: Semigroup + Multiply<R>,
        <R2 as Multiply<R>>::Output: Data + Semigroup,
        L: FnMut(D1) -> (D2, R2) + 'static,
        G::Timestamp: Lattice;
}

impl<G, D1> StreamExt<G, D1> for Stream<G, D1>
where
    D1: Data,
    G: Scope,
{
    fn unary_fallible<D2, E, B, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandle<G::Timestamp, D1, P::Puller>,
                    &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                    &mut OutputHandle<G::Timestamp, E, Tee<G::Timestamp, E>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        let mut builder = OperatorBuilderRc::new(name.into(), self.scope());
        builder.set_notify(false);

        let operator_info = builder.operator_info();

        let mut input = builder.new_input(self, pact);
        let (mut ok_output, ok_stream) = builder.new_output();
        let (mut err_output, err_stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |_frontiers| {
                let mut ok_output_handle = ok_output.activate();
                let mut err_output_handle = err_output.activate();
                logic(&mut input, &mut ok_output_handle, &mut err_output_handle);
            }
        });

        (ok_stream, err_stream)
    }

    #[allow(clippy::redundant_closure)]
    fn flat_map_fallible<D2, E, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let mut storage = Vec::new();
        self.unary_fallible(Pipeline, name, move |_, _| {
            Box::new(move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session(&time);
                    let mut err_session = err_output.session(&time);
                    data.swap(&mut storage);
                    for r in storage.drain(..).flat_map(|d1| logic(d1)) {
                        match r {
                            Ok(d2) => ok_session.give(d2),
                            Err(e) => err_session.give(e),
                        }
                    }
                })
            })
        })
    }
}

impl<G, D1, R> CollectionExt<G, D1, R> for Collection<G, D1, R>
where
    G: Scope,
    G::Timestamp: Data,
    D1: Data,
    R: Semigroup,
{
    fn empty(scope: &G) -> Collection<G, D1, R> {
        operator::empty(scope).as_collection()
    }

    fn flat_map_fallible<D2, E, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let (ok_stream, err_stream) = self.inner.flat_map_fallible(name, move |(d1, t, r)| {
            logic(d1).into_iter().map(move |res| match res {
                Ok(d2) => Ok((d2, t.clone(), r.clone())),
                Err(e) => Err((e, t.clone(), r.clone())),
            })
        });
        (ok_stream.as_collection(), err_stream.as_collection())
    }

    fn explode_one<D2, R2, L>(&self, mut logic: L) -> Collection<G, D2, <R2 as Multiply<R>>::Output>
    where
        D2: differential_dataflow::Data,
        R2: Semigroup + Multiply<R>,
        <R2 as Multiply<R>>::Output: Data + Semigroup,
        L: FnMut(D1) -> (D2, R2) + 'static,
        G::Timestamp: Lattice,
    {
        self.inner
            .unary(Pipeline, "ExplodeOne", move |_, _| {
                let mut buffer = Vec::new();
                move |input, output| {
                    let mut out = ConsolidateBuffer::new(output, 0);
                    input.for_each(|time, data| {
                        data.swap(&mut buffer);
                        out.give_iterator(
                            &time,
                            buffer.drain(..).map(|(x, t, d)| {
                                let (x, d2) = logic(x);
                                (x, t, d2.multiply(&d))
                            }),
                        );
                    });
                }
            })
            .as_collection()
    }
}
