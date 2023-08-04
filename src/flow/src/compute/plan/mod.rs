mod join;
mod reduce;

use std::collections::BTreeMap;

use join::JoinPlan;
pub(crate) use reduce::{convert_indexes_to_skips, AccumulablePlan, KeyValPlan, ReducePlan};
use serde::{Deserialize, Serialize};

use crate::expr::{Id, LocalId, MapFilterProject, ScalarExpr, TableFunc};
use crate::repr::{self, Diff, Row};
use crate::storage::errors::EvalError;

/// The forms in which an operator's output is available;
/// it can be considered the plan-time equivalent of
/// `render::context::CollectionBundle`.
///
/// These forms are either "raw", representing an unarranged collection,
/// or "arranged", representing one that has been arranged by some key.
///
/// The raw collection, if it exists, may be consumed directly.
///
/// The arranged collections are slightly more complicated:
/// Each key here is attached to a description of how the corresponding
/// arrangement is permuted to remove value columns
/// that are redundant with key columns. Thus, the first element in each
/// tuple of `arranged` is the arrangement key; the second is the map of
/// logical output columns to columns in the key or value of the deduplicated
/// representation, and the third is a "thinning expression",
/// or list of columns to include in the value
/// when arranging.
///
/// For example, assume a 5-column collection is to be arranged by the key
/// `[Column(2), Column(0) + Column(3), Column(1)]`.
/// Then `Column(1)` and `Column(2)` in the value are redundant with the key, and
/// only columns 0, 3, and 4 need to be stored separately.
/// The thinning expression will then be `[0, 3, 4]`.
///
/// The permutation represents how to recover the
/// original values (logically `[Column(0), Column(1), Column(2), Column(3), Column(4)]`)
/// from the key and value of the arrangement, logically
/// `[Column(2), Column(0) + Column(3), Column(1), Column(0), Column(3), Column(4)]`.
/// Thus, the permutation in this case should be `{0: 3, 1: 2, 2: 0, 3: 4, 4: 5}`.
///
/// Note that this description, while true at the time of writing, is merely illustrative;
/// users of this struct should not rely on the exact strategy used for generating
/// the permutations. As long as clients apply the thinning expression
/// when creating arrangements, and permute by the hashmap when reading them,
/// the contract of the function where they are generated (`expr::permutation_for_arrangement`)
/// ensures that the correct values will be read.
#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AvailableCollections {
    /// Whether the collection exists in unarranged form.
    pub raw: bool,
    /// The set of arrangements of the collection, along with a
    /// column permutation mapping
    pub arranged: Vec<KeyWithColumnPermutation>,
}

pub type KeyWithColumnPermutation = (Vec<ScalarExpr>, BTreeMap<usize, usize>, Vec<usize>);

impl AvailableCollections {
    /// Represent a collection that has no arrangements.
    pub fn new_raw() -> Self {
        Self {
            raw: true,
            arranged: vec![],
        }
    }

    /// Represent a collection that is arranged in the
    /// specified ways.
    pub fn new_arranged(arranged: Vec<KeyWithColumnPermutation>) -> Self {
        assert!(
            !arranged.is_empty(),
            "Invariant violated: at least one collection must exist"
        );
        Self {
            raw: false,
            arranged,
        }
    }
}

/// Rendering Plan
///
/// TODO(discord9): see if we ever need to support recursive plans
pub enum Plan<T = repr::Timestamp> {
    /// A collection containing a pre-determined collection.
    Constant {
        rows: Result<Vec<(Row, T, Diff)>, EvalError>,
    },
    /// A reference to a bound collection.
    ///
    /// This is commonly either an external reference to an existing source or
    /// maintained arrangement, or an internal reference to a `Let` identifier.
    Get {
        id: Id,
        keys: AvailableCollections,
        plan: GetPlan,
    },
    /// Binds `value` to `id`, and then results in `body` with that binding.
    ///
    /// This stage has the effect of sharing `value` across multiple possible
    /// uses in `body`, and is the only mechanism we have for sharing collection
    /// information across parts of a dataflow.
    ///
    /// The binding is not available outside of `body`.
    Let {
        /// The local identifier to be used, available to `body` as `Id::Local(id)`.
        id: LocalId,
        /// The collection that should be bound to `id`.
        value: Box<Plan<T>>,
        /// The collection that results, which is allowed to contain `Get` stages
        /// that reference `Id::Local(id)`.
        body: Box<Plan<T>>,
    },
    /// Map, Filter, and Project operators.
    ///
    /// This stage contains work that we would ideally like to fuse to other plan
    /// stages, but for practical reasons cannot. For example: reduce, threshold,
    /// and topk stages are not able to absorb this operator.
    Mfp {
        /// The input collection.
        input: Box<Plan<T>>,
        /// Linear operator to apply to each record.
        mfp: MapFilterProject,
        /// Whether the input is from an arrangement, and if so,
        /// whether we can seek to a specific value therein
        input_key_val: Option<(Vec<ScalarExpr>, Option<Row>)>,
    },
    /// A variable number of output records for each input record.
    ///
    /// This stage is a bit of a catch-all for logic that does not easily fit in
    /// map stages. This includes table valued functions, but also functions of
    /// multiple arguments, and functions that modify the sign of updates.
    ///
    /// This stage allows a `MapFilterProject` operator to be fused to its output,
    /// and this can be very important as otherwise the output of `func` is just
    /// appended to the input record, for as many outputs as it has. This has the
    /// unpleasant default behavior of repeating potentially large records that
    /// are being unpacked, producing quadratic output in those cases. Instead,
    /// in these cases use a `mfp` member that projects away these large fields.
    FlatMap {
        /// The input collection.
        input: Box<Plan<T>>,
        /// The variable-record emitting function.
        func: TableFunc,
        /// Expressions that for each row prepare the arguments to `func`.
        exprs: Vec<ScalarExpr>,
        /// Linear operator to apply to each record produced by `func`.
        mfp: MapFilterProject,
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<ScalarExpr>>,
    },
    /// A multiway relational equijoin, with fused map, filter, and projection.
    ///
    /// This stage performs a multiway join among `inputs`, using the equality
    /// constraints expressed in `plan`. The plan also describes the implementation
    /// strategy we will use, and any pushed down per-record work.
    Join {
        /// An ordered list of inputs that will be joined.
        inputs: Vec<Plan<T>>,
        /// Detailed information about the implementation of the join.
        ///
        /// This includes information about the implementation strategy, but also
        /// any map, filter, project work that we might follow the join with, but
        /// potentially pushed down into the implementation of the join.
        plan: JoinPlan,
    },
    /// Aggregation by key.
    Reduce {
        /// The input collection.
        input: Box<Plan<T>>,
        /// A plan for changing input records into key, value pairs.
        key_val_plan: KeyValPlan,
        /// A plan for performing the reduce.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself. Please check
        /// out the documentation for this type for more detail.
        plan: ReducePlan,
        /// The particular arrangement of the input we expect to use,
        /// if any
        input_key: Option<Vec<ScalarExpr>>,
    },
}

/// TODO(discord9): impl GetPlan
pub enum GetPlan {
    /// Simply pass input arrangements on to the next stage.
    PassArrangements,
    /// Using the supplied key, optionally seek the row, and apply the MFP.
    Arrangement(Vec<ScalarExpr>, Option<Row>, MapFilterProject),
    /// Scan the input collection (unarranged) and apply the MFP.
    Collection(MapFilterProject),
}
