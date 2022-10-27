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

    use datatypes::vectors::{Int64Vector, UInt64Vector, UInt8Vector, VectorRef};

    use super::*;
    use crate::metadata::RegionMetadata;
    use crate::read::Batch;
    use crate::test_util::descriptor_util;

    pub const REGION_NAME: &str = "test";

    pub(crate) fn new_batch() -> Batch {
        new_batch_with_num_values(1)
    }

    pub(crate) fn new_batch_with_num_values(num_value_columns: usize) -> Batch {
        let k0 = Int64Vector::from_slice(&[1, 2, 3]);
        let timestamp = Int64Vector::from_slice(&[4, 5, 6]);
        let mut columns: Vec<VectorRef> = vec![Arc::new(k0), Arc::new(timestamp)];

        for i in 0..num_value_columns {
            let vi = Int64Vector::from_slice(&[i as i64, i as i64, i as i64]);
            columns.push(Arc::new(vi));
        }

        let sequences = UInt64Vector::from_slice(&[100, 100, 100]);
        let op_types = UInt8Vector::from_slice(&[0, 0, 0]);

        columns.push(Arc::new(sequences));
        columns.push(Arc::new(op_types));

        Batch::new(columns)
    }

    pub(crate) fn new_region_schema(version: u32, num_value_columns: usize) -> RegionSchema {
        let metadata: RegionMetadata =
            descriptor_util::desc_with_value_columns(REGION_NAME, num_value_columns)
                .try_into()
                .unwrap();

        let columns = metadata.columns;
        RegionSchema::new(columns, version).unwrap()
    }
}
