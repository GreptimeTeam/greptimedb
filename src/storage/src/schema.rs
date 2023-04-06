// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    use datatypes::vectors::{
        Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector, VectorRef,
    };

    use crate::read::Batch;

    pub const REGION_NAME: &str = "test";

    pub(crate) fn new_batch() -> Batch {
        new_batch_with_num_values(1)
    }

    pub(crate) fn new_batch_with_num_values(num_field_columns: usize) -> Batch {
        let k0 = Int64Vector::from_slice([1, 2, 3]);
        let timestamp = TimestampMillisecondVector::from_vec(vec![4, 5, 6]);

        let mut columns: Vec<VectorRef> = vec![Arc::new(k0), Arc::new(timestamp)];

        for i in 0..num_field_columns {
            let vi = Int64Vector::from_slice([i as i64, i as i64, i as i64]);
            columns.push(Arc::new(vi));
        }

        let sequences = UInt64Vector::from_slice([100, 100, 100]);
        let op_types = UInt8Vector::from_slice([0, 0, 0]);

        columns.push(Arc::new(sequences));
        columns.push(Arc::new(op_types));

        Batch::new(columns)
    }
}
