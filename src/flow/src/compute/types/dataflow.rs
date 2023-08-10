use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::compute::plan::Plan;
use crate::compute::types::sinks::ComputeSinkDesc;
use crate::compute::types::sources::SourceInstanceDesc;
use crate::expr::{GlobalId, ScalarExpr};
use crate::repr::{self, RelationType};

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DataflowDescription<P, S: 'static = (), T = repr::Timestamp> {
    /// Sources instantiations made available to the dataflow pair with monotonicity information.
    pub source_imports: BTreeMap<GlobalId, (SourceInstanceDesc<S>, bool)>,
    /// Indexes made available to the dataflow.
    /// (id of new index, description of index, relationtype of base source/view, monotonic)
    pub index_imports: BTreeMap<GlobalId, (IndexDesc, RelationType, bool)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order, as there may be
    /// dependencies of later objects on prior identifiers.
    pub objects_to_build: Vec<BuildDesc<P>>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view)
    pub index_exports: BTreeMap<GlobalId, (IndexDesc, RelationType)>,
    /// sinks to be created
    /// (id of new sink, description of sink)
    pub sink_exports: BTreeMap<GlobalId, ComputeSinkDesc<S, T>>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// If this is set, it should override the default setting determined by
    /// the upper bound of `since` frontiers contributing to the dataflow.
    /// It is an error for this to be set to a frontier not beyond that default.
    pub as_of: Option<Antichain<T>>,
    /// Frontier beyond which the dataflow should not execute.
    /// Specifically, updates at times greater or equal to this frontier are suppressed.
    /// This is often set to `as_of + 1` to enable "batch" computations.
    pub until: Antichain<T>,
    /// Human readable name
    pub debug_name: String,
}

/// An association of a global identifier to an expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<P = Plan> {
    pub id: GlobalId,
    pub plan: P,
}

/// An index storing processed updates so they can be queried
/// or reused in other computations
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct IndexDesc {
    /// Identity of the collection the index is on.
    pub on_id: GlobalId,
    /// Expressions to be arranged, in order of decreasing primacy.
    pub key: Vec<ScalarExpr>,
}
