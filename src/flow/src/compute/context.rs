use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::{Collection, Data};
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::compute::typedefs::{TraceErrHandle, TraceRowHandle};
use crate::expr::{GlobalId, Id, ScalarExpr};
use crate::repr;
use crate::repr::Diff;
use crate::storage::errors::DataflowError;

// Local type definition to avoid the horror in signatures.
pub(crate) type KeyArrangement<S, K, V> =
    Arranged<S, TraceRowHandle<K, V, <S as ScopeParent>::Timestamp, Diff>>;
pub(crate) type Arrangement<S, V> = KeyArrangement<S, V, V>;
pub(crate) type ErrArrangement<S> =
    Arranged<S, TraceErrHandle<DataflowError, <S as ScopeParent>::Timestamp, Diff>>;
pub(crate) type ArrangementImport<S, V, T> = Arranged<
    S,
    TraceEnter<TraceFrontier<TraceRowHandle<V, V, T, Diff>>, <S as ScopeParent>::Timestamp>,
>;
pub(crate) type ErrArrangementImport<S, T> = Arranged<
    S,
    TraceEnter<
        TraceFrontier<TraceErrHandle<DataflowError, T, Diff>>,
        <S as ScopeParent>::Timestamp,
    >,
>;

/// Describes flavor of arrangement: local or imported trace.
#[derive(Clone)]
pub enum ArrangementFlavor<S: Scope, V: Data, T = repr::Timestamp>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// A dataflow-local arrangement.
    Local(Arrangement<S, V>, ErrArrangement<S>),
    /// An imported trace from outside the dataflow.
    ///
    /// The `GlobalId` identifier exists so that exports of this same trace
    /// can refer back to and depend on the original instance.
    Trace(
        GlobalId,
        ArrangementImport<S, V, T>,
        ErrArrangementImport<S, T>,
    ),
}

pub struct Context<S, V: Data, T = repr::Timestamp>
where
    T: Timestamp + Lattice,
    S: Scope,
    S::Timestamp: Lattice + Refines<T>,
{
    /// The scope within which all managed collections exist.
    ///
    /// It is an error to add any collections not contained in this scope.
    pub(crate) scope: S,
    /// The debug name of the dataflow associated with this context.
    pub debug_name: String,
    /// The Timely ID of the dataflow associated with this context.
    pub dataflow_id: usize,
    /// Frontier before which updates should not be emitted.
    ///
    /// We *must* apply it to sinks, to ensure correct outputs.
    /// We *should* apply it to sources and imported traces, because it improves performance.
    pub since_frontier: Antichain<T>,
    /// Frontier after which updates should not be emitted.
    /// Used to limit the amount of work done when appropriate.
    pub until_frontier: Antichain<T>,
    /// Bindings of identifiers to collections.
    pub bindings: BTreeMap<Id, CollectionBundle<S, V, T>>,
}

impl<S: Scope, V: Data> Context<S, V>
where
    S::Timestamp: Lattice + Refines<repr::Timestamp>,
{
    /// TODO" DataflowDesc & Plan & etc.
    pub fn for_dataflow_in(scope: S) -> Self {
        let dataflow_id = scope.addr()[0];
        // TODO(discord9): get since_frontier and until_frontier from dataflow_desc
        todo!()
    }
}

#[derive(Clone)]
pub struct CollectionBundle<S, V, T = repr::Timestamp>
where
    T: Timestamp + Lattice,
    S: Scope,
    S::Timestamp: Lattice + Refines<T>,
    V: Data,
{
    pub(crate) collection: Collection<S, V, Diff>,
    /// TODO: impl: 1. ScalarExpr(Could be from substrait), 2. Arrangement
    pub(crate) arranged: BTreeMap<Vec<ScalarExpr>, ArrangementFlavor<S, V, T>>,
}

#[derive(Clone)]
pub enum ArrangementFlavored {}
