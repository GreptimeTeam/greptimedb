use std::fmt::Debug;

use crate::spliter::{ColumnName, RegionId, ValueList};

pub trait PartitionRule {
    type Error: Debug;

    fn partition_columns(&self) -> Vec<ColumnName>;

    fn find_region(&self, values: &ValueList) -> std::result::Result<RegionId, Self::Error>;

    // TODO(fys): there maybe other method
}
