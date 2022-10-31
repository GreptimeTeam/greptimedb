mod range;

use std::fmt::Debug;

pub use datafusion_expr::Operator;
use datatypes::prelude::Value;

pub(crate) type RegionId = u64;

pub(crate) type ValueList = Vec<Value>;

pub trait PartitionRule {
    type Error: Debug;

    fn partition_columns(&self) -> Vec<String>;

    // TODO(LFC): Unify `find_region` and `find_regions` methods when distributed read and write features are both merged into develop.
    // Or find better names since one is mainly for writes and the other is for reads.
    fn find_region(&self, values: &ValueList) -> Result<RegionId, Self::Error>;

    fn find_regions(&self, exprs: &[PartitionExpr]) -> Result<Vec<RegionId>, Self::Error>;
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
