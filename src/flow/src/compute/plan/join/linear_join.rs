use serde::{Deserialize, Serialize};

/// TODO: impl Join
/// A plan for the execution of a linear join.
///
/// A linear join is a sequence of stages, each of which introduces
/// a new collection. Each stage is represented by a [LinearStagePlan].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LinearJoinPlan {}
