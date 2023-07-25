use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::{Collection, Data};
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::compute::render::RenderTimestamp;
use crate::compute::typedefs::{TraceErrHandle, TraceRowHandle};
use crate::expr::{GlobalId, Id, MapFilterProject, ScalarExpr};
use crate::repr;
use crate::repr::{Diff, Row};
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
    /// TODO(discord9)" DataflowDesc & Plan & etc.
    pub fn for_dataflow_in(scope: S) -> Self {
        let dataflow_id = scope.addr()[0];
        // TODO(discord9)=: get since_frontier and until_frontier from dataflow_desc
        todo!()
    }
}

impl<S: Scope, V: Data, T: Lattice> Context<S, V, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Insert a collection bundle by an identifier.
    ///
    /// This is expected to be used to install external collections (sources, indexes, other views),
    /// as well as for `Let` bindings of local collections.
    pub fn insert_id(
        &mut self,
        id: Id,
        collection: CollectionBundle<S, V, T>,
    ) -> Option<CollectionBundle<S, V, T>> {
        self.bindings.insert(id, collection)
    }

    /// Remove a collection bundle by an identifier.
    ///
    /// The primary use of this method is uninstalling `Let` bindings.
    pub fn remove_id(&mut self, id: Id) -> Option<CollectionBundle<S, V, T>> {
        self.bindings.remove(&id)
    }
    /// Melds a collection bundle to whatever exists.
    #[allow(clippy::map_entry)]
    pub fn update_id(&mut self, id: Id, collection: CollectionBundle<S, V, T>) {
        if !self.bindings.contains_key(&id) {
            self.bindings.insert(id, collection);
        } else {
            let binding = self
                .bindings
                .get_mut(&id)
                .expect("Binding verified to exist");
            if collection.collection.is_some() {
                binding.collection = collection.collection;
            }
            for (key, flavor) in collection.arranged.into_iter() {
                binding.arranged.insert(key, flavor);
            }
        }
    }
    /// Look up a collection bundle by an identifier.
    pub fn lookup_id(&self, id: Id) -> Option<CollectionBundle<S, V, T>> {
        self.bindings.get(&id).cloned()
    }
}

type ResultCollection<S, V> = (Collection<S, V, Diff>, Collection<S, DataflowError, Diff>);

#[derive(Clone)]
pub struct CollectionBundle<S, V, T = repr::Timestamp>
where
    T: Timestamp + Lattice,
    S: Scope,
    S::Timestamp: Lattice + Refines<T>,
    V: Data,
{
    pub(crate) collection: Option<ResultCollection<S, V>>,
    /// TODO(discord9): impl: 1. ScalarExpr(Could be from substrait), 2. Arrangement
    pub(crate) arranged: BTreeMap<Vec<ScalarExpr>, ArrangementFlavor<S, V, T>>,
}

impl<S: Scope, V: Data, T: Lattice> CollectionBundle<S, V, T>
where
    T: Timestamp + Lattice,
    S::Timestamp: Lattice + Refines<T>,
{
    /// Construct a new collection bundle from update streams.
    pub fn from_collections(
        oks: Collection<S, V, Diff>,
        errs: Collection<S, DataflowError, Diff>,
    ) -> Self {
        Self {
            collection: Some((oks, errs)),
            arranged: BTreeMap::default(),
        }
    }
}

impl<S, T> CollectionBundle<S, repr::Row, T>
where
    T: timely::progress::Timestamp + Lattice,
    S: Scope,
    S::Timestamp: Refines<T> + Lattice + RenderTimestamp,
{
    /// Presents `self` as a stream of updates, having been subjected to `mfp`.
    ///
    /// This operator is able to apply the logic of `mfp` early, which can substantially
    /// reduce the amount of data produced when `mfp` is non-trivial.
    ///
    /// The `key_val` argument, when present, indicates that a specific arrangement should
    /// be used, and if, in addition, the `val` component is present,
    /// that we can seek to the supplied row.
    pub fn as_collection_core(
        &self,
        mut mfp: MapFilterProject,
        key_val: Option<(Vec<ScalarExpr>, Option<Row>)>,
        until: Antichain<repr::Timestamp>,
    ) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>) {
        mfp.optimize();
        todo!()
    }
}

#[derive(Clone)]
pub enum ArrangementFlavored {}
