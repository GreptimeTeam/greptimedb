use serde::{Deserialize, Serialize};

use crate::expr::{AggregateExpr, AggregateFunc, MapFilterProject, SafeMfpPlan};

/// This enum represents the three potential types of aggregations.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ReductionType {
    /// Accumulable functions can be subtracted from (are invertible), and associative.
    /// We can compute these results by moving some data to the diff field under arbitrary
    /// changes to inputs. Examples include sum or count.
    Accumulable,
    /// Hierarchical functions are associative, which means we can split up the work of
    /// computing them across subsets. Note that hierarchical reductions should also
    /// reduce the data in some way, as otherwise rendering them hierarchically is not
    /// worth it. Examples include min or max.
    Hierarchical,
    /// Basic, for lack of a better word, are functions that are neither accumulable
    /// nor hierarchical. Examples include jsonb_agg.
    Basic,
}

/// Plan for extracting keys and values in preparation for a reduction.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KeyValPlan {
    /// Extracts the columns used as the key.
    pub key_plan: SafeMfpPlan,
    /// Extracts the columns used to feed the aggregations.
    pub val_plan: SafeMfpPlan,
}

/// Transforms a vector containing indexes of needed columns into one containing
/// the "skips" an iterator over a Row would need to perform to see those values.
///
/// This function requires that all of the elements in `indexes` are strictly
/// increasing.
///
/// # Examples
///
/// ```
/// assert_eq!(convert_indexes_to_skips(vec![3, 6, 10, 15]), [3, 2, 3, 4])
/// ```
pub fn convert_indexes_to_skips(mut indexes: Vec<usize>) -> Vec<usize> {
    for i in 1..indexes.len() {
        assert!(
            indexes[i - 1] < indexes[i],
            "convert_indexes_to_skip needs indexes to be strictly increasing. Received: {:?}",
            indexes,
        );
    }

    for i in (1..indexes.len()).rev() {
        indexes[i] -= indexes[i - 1];
        indexes[i] -= 1;
    }

    indexes
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

// TODO(discord9): others

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum HierarchicalPlan {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum BasicPlan {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CollationPlan {}

/// Determines whether a function can be accumulated in an update's "difference" field,
/// and whether it can be subjected to recursive (hierarchical) aggregation.
///
/// Accumulable aggregations will be packed into differential dataflow's "difference" field,
/// which can be accumulated in-place using the addition operation on the type. Aggregations
/// that indicate they are accumulable will still need to provide an action that takes their
/// data and introduces it as a difference, and the post-processing when the accumulated value
/// is presented as data.
///
/// Hierarchical aggregations will be subjected to repeated aggregation on initially small but
/// increasingly large subsets of each key. This has the intended property that no invocation
/// is on a significantly large set of values (and so, no incremental update needs to reform
/// significant input data). Hierarchical aggregates can be rendered more efficiently if the
/// input stream is append-only as then we only need to retain the "currently winning" value.
/// Every hierarchical aggregate needs to supply a corresponding ReductionMonoid implementation.
fn reduction_type(func: &AggregateFunc) -> ReductionType {
    match func {
        AggregateFunc::SumInt16
        | AggregateFunc::SumInt32
        | AggregateFunc::SumInt64
        | AggregateFunc::SumUInt16
        | AggregateFunc::SumUInt32
        | AggregateFunc::SumUInt64
        | AggregateFunc::SumFloat32
        | AggregateFunc::SumFloat64
        | AggregateFunc::Count
        | AggregateFunc::Any
        | AggregateFunc::All => ReductionType::Accumulable,
        AggregateFunc::MaxInt16
        | AggregateFunc::MaxInt32
        | AggregateFunc::MaxInt64
        | AggregateFunc::MaxUInt16
        | AggregateFunc::MaxUInt32
        | AggregateFunc::MaxUInt64
        | AggregateFunc::MaxFloat32
        | AggregateFunc::MaxFloat64
        | AggregateFunc::MaxBool
        | AggregateFunc::MaxString
        | AggregateFunc::MaxDate
        | AggregateFunc::MaxTimestamp
        | AggregateFunc::MaxTimestampTz
        | AggregateFunc::MinInt16
        | AggregateFunc::MinInt32
        | AggregateFunc::MinInt64
        | AggregateFunc::MinUInt16
        | AggregateFunc::MinUInt32
        | AggregateFunc::MinUInt64
        | AggregateFunc::MinFloat32
        | AggregateFunc::MinFloat64
        | AggregateFunc::MinBool
        | AggregateFunc::MinString
        | AggregateFunc::MinDate
        | AggregateFunc::MinTimestamp
        | AggregateFunc::MinTimestampTz => ReductionType::Hierarchical,
        _ => ReductionType::Basic,
    }
}
