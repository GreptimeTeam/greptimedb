use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::expr::{Id, ScalarExpr};
use crate::repr::{self, Row};
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

pub enum Plan {
    Constant { rows: Result<Vec<Row>, EvalError> },
    Get { id: Id, keys: AvailableCollections },
}
