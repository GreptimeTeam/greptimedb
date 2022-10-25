pub mod compat;
mod projected;
mod region;
mod store;

pub use crate::schema::projected::{ProjectedSchema, ProjectedSchemaRef};
pub use crate::schema::region::{RegionSchema, RegionSchemaRef};
pub use crate::schema::store::{StoreSchema, StoreSchemaRef};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{Int64Vector, UInt64Vector, UInt8Vector};

    use super::*;
    use crate::metadata::RegionMetadata;
    use crate::read::Batch;
    use crate::test_util::descriptor_util;

    pub(crate) fn new_batch() -> Batch {
        let k0 = Int64Vector::from_slice(&[1, 2, 3]);
        let timestamp = Int64Vector::from_slice(&[4, 5, 6]);
        let v0 = Int64Vector::from_slice(&[7, 8, 9]);
        let sequences = UInt64Vector::from_slice(&[100, 100, 100]);
        let op_types = UInt8Vector::from_slice(&[0, 0, 0]);

        Batch::new(vec![
            Arc::new(k0),
            Arc::new(timestamp),
            Arc::new(v0),
            Arc::new(sequences),
            Arc::new(op_types),
        ])
    }

    pub(crate) fn new_region_schema(version: u32, num_value_columns: usize) -> RegionSchema {
        let metadata: RegionMetadata =
            descriptor_util::desc_with_value_columns("test", num_value_columns)
                .try_into()
                .unwrap();

        let columns = metadata.columns;
        RegionSchema::new(columns, version).unwrap()
    }
}
