use serde::{Deserialize, Serialize};

/// A delta query is implemented by a set of paths, one for each input.
///
/// Each delta query path responds to its input changes by repeated lookups
/// in arrangements for other join inputs. These lookups require specific
/// instructions about which expressions to use as keys. Along the way,
/// various closures are applied to filter and project as early as possible.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeltaJoinPlan {}
