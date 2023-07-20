use serde::{Deserialize, Serialize};

use crate::expr::func::AggregateFunc;
use crate::expr::ScalarExpr;

/// function that might emit multiple output record for one input row
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum TableFunc {}

/// Describes an aggregation expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    pub expr: ScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    #[serde(default)]
    pub distinct: bool,
}
