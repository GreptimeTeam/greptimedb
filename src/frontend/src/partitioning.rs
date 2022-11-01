mod columns;
mod range;

use std::fmt::Debug;

pub use datafusion_expr::Operator;
use datatypes::prelude::Value;
use store_api::storage::RegionId;

pub trait PartitionRule {
    type Error: Debug;

    fn partition_columns(&self) -> Vec<String>;

    // TODO(LFC): Unify `find_region` and `find_regions` methods when distributed read and write features are both merged into develop.
    // Or find better names since one is mainly for writes and the other is for reads.
    fn find_region(&self, values: &[Value]) -> Result<RegionId, Self::Error>;

    fn find_regions(&self, exprs: &[PartitionExpr]) -> Result<Vec<RegionId>, Self::Error>;
}

/// The right bound(exclusive) of partition range.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PartitionBound {
    Value(Value),
    // FIXME(LFC): no allow, for clippy temporarily
    #[allow(dead_code)]
    MaxValue,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PartitionExpr {
    column: String,
    op: Operator,
    value: Value,
}

impl PartitionExpr {
    pub fn value(&self) -> &Value {
        &self.value
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_partition_bound() {
        let b1 = PartitionBound::Value(1_i32.into());
        let b2 = PartitionBound::Value(100_i32.into());
        let b3 = PartitionBound::MaxValue;
        assert!(b1 < b2);
        assert!(b2 < b3);
    }
}
