use std::marker::PhantomData;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};
use timely::Data;

use crate::Diff;
pub struct Context<S, T>
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
    pub upper_frontier: Antichain<T>,
}

#[derive(Clone)]
pub struct CollectionBundle<S: Scope, V: Data, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    pub(crate) collection: Collection<S, V, Diff>,
    /// TODO: impl arranged in memory
    pub(crate) arranged: PhantomData<T>,
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum DataflowError {
    EvalError(EvalError),
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum EvalError {
    DivisionByZero,
}
