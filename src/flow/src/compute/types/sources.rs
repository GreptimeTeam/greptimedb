use serde::{Deserialize, Serialize};

use crate::expr::MapFilterProject;
use crate::repr::RelationType;

/// A description of an instantiation of a source.
///
/// This includes a description of the source, but additionally any
/// context-dependent options like the ability to apply filtering and
/// projection to the records as they emerge.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceDesc<M> {
    /// Arguments for this instantiation of the source.
    pub arguments: SourceInstanceArguments,
    /// Additional metadata used by the storage client of a compute instance to read it.
    pub storage_metadata: M,
    /// The relation type of this source
    pub typ: RelationType,
}

/// Per-source construction arguments.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceArguments {
    /// Linear operators to be applied record-by-record.
    pub operators: Option<MapFilterProject>,
}
