use serde::{Deserialize, Serialize};

use crate::expr::{AggregateExpr, MapFilterProject};

/// Plan for extracting keys and values in preparation for a reduction.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KeyValPlan {
    /// Extracts the columns used as the key.
    pub key_plan: MapFilterProject,
    /// Extracts the columns used to feed the aggregations.
    pub val_plan: MapFilterProject,
}

/// TODO(discord9): Reduce Plan
/// A `ReducePlan` provides a concise description for how we will
/// execute a given reduce expression.
///
/// The provided reduce expression can have no
/// aggregations, in which case its just a `Distinct` and otherwise
/// it's composed of a combination of accumulable, hierarchical and
/// basic aggregations.
///
/// We want to try to centralize as much decision making about the
/// shape / general computation of the rendered dataflow graph
/// in this plan, and then make actually rendering the graph
/// be as simple (and compiler verifiable) as possible.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ReducePlan {
    /// Plan for not computing any aggregations, just determining the set of
    /// distinct keys.
    Distinct,
    /// Plan for computing only accumulable aggregations.
    Accumulable(AccumulablePlan),
    /// Plan for computing only hierarchical aggregations.
    Hierarchical(HierarchicalPlan),
    /// Plan for computing only basic aggregations.
    Basic(BasicPlan),
    /// Plan for computing a mix of different kinds of aggregations.
    /// We need to do extra work here to reassemble results back in the
    /// requested order.
    Collation(CollationPlan),
}

/// Plan for computing a set of accumulable aggregations.
///
/// We fuse all of the accumulable aggregations together
/// and compute them with one dataflow fragment. We need to
/// be careful to separate out the aggregations that
/// apply only to the distinct set of values. We need
/// to apply a distinct operator to those before we
/// combine them with everything else.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AccumulablePlan {
    /// All of the aggregations we were asked to compute, stored
    /// in order.
    pub full_aggrs: Vec<AggregateExpr>,
    /// All of the non-distinct accumulable aggregates.
    /// Each element represents:
    /// (index of the aggregation among accumulable aggregations,
    ///  index of the datum among inputs, aggregation expr)
    /// These will all be rendered together in one dataflow fragment.
    pub simple_aggrs: Vec<(usize, usize, AggregateExpr)>,
    /// Same as above but for all of the `DISTINCT` accumulable aggregations.
    pub distinct_aggrs: Vec<(usize, usize, AggregateExpr)>,
}

// TODO: others

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum HierarchicalPlan {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum BasicPlan {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CollationPlan {}
