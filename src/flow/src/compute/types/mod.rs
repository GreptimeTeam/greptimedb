use serde::{Deserialize, Serialize};

use crate::expr::GlobalId;
mod dataflow;
mod sinks;
mod sources;

/// An association of a global identifier to an expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<P> {
    pub id: GlobalId,
    pub plan: P,
}
