use serde::{Deserialize, Serialize};
mod delta_join;
mod linear_join;
pub use delta_join::DeltaJoinPlan;
pub use linear_join::LinearJoinPlan;

/// TODO(discord9): impl Join
/// A complete enumeration of possible join plans to render.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum JoinPlan {
    /// A join implemented by a linear join.
    Linear(LinearJoinPlan),
    /// A join implemented by a delta join.
    Delta(DeltaJoinPlan),
}
