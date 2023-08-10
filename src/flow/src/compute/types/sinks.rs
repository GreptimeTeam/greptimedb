use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::expr::GlobalId;
use crate::repr::{self, RelationDesc};

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeSinkDesc<S: 'static = (), T = repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: ComputeSinkConnection<S>,
    pub with_snapshot: bool,
    pub up_to: Antichain<T>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ComputeSinkConnection<S: 'static = ()> {
    // TODO(discord9): consider if ever needed
    Subscribe,
    Persist(PersistSinkConnection<S>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistSinkConnection<S> {
    pub value_desc: RelationDesc,
    pub storage_metadata: S,
}
