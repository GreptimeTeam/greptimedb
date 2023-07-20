use serde::{Deserialize, Serialize};

use crate::expr::{Id, LocalId, ScalarExpr};

/// A compound operator that can be applied row-by-row.
///
/// This operator integrates the map, filter, and project operators.
/// It applies a sequences of map expressions, which are allowed to
/// refer to previous expressions, interleaved with predicates which
/// must be satisfied for an output to be produced. If all predicates
/// evaluate to `Datum::True` the data at the identified columns are
/// collected and produced as output in a packed `Row`.
///
/// This operator is a "builder" and its contents may contain expressions
/// that are not yet executable. For example, it may contain temporal
/// expressions in `self.expressions`, even though this is not something
/// we can directly evaluate. The plan creation methods will defensively
/// ensure that the right thing happens.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MapFilterProject {
    /// A sequence of expressions that should be appended to the row.
    ///
    /// Many of these expressions may not be produced in the output,
    /// and may only be present as common subexpressions.
    pub expressions: Vec<ScalarExpr>,
    /// Expressions that must evaluate to `Datum::True` for the output
    /// row to be produced.
    ///
    /// Each entry is prepended with a column identifier indicating
    /// the column *before* which the predicate should first be applied.
    /// Most commonly this would be one plus the largest column identifier
    /// in the predicate's support, but it could be larger to implement
    /// guarded evaluation of predicates.
    ///
    /// This list should be sorted by the first field.
    pub predicates: Vec<(usize, ScalarExpr)>,
    /// A sequence of column identifiers whose data form the output row.
    pub projection: Vec<usize>,
    /// The expected number of input columns.
    ///
    /// This is needed to ensure correct identification of newly formed
    /// columns in the output.
    pub input_arity: usize,
}
