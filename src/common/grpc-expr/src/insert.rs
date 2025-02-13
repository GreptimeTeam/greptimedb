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

use api::helper;
use api::v1::column::Values;
use common_base::BitVec;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::VectorRef;
use snafu::{ensure, ResultExt};

use crate::error::{CreateVectorSnafu, Result, UnexpectedValuesLengthSnafu};

pub(crate) fn add_values_to_builder(
    data_type: ConcreteDataType,
    values: Values,
    row_count: usize,
    null_mask: Vec<u8>,
) -> Result<VectorRef> {
    if null_mask.is_empty() {
        Ok(helper::pb_values_to_vector_ref(&data_type, values))
    } else {
        let builder = &mut data_type.create_mutable_vector(row_count);
        let values = helper::pb_values_to_values(&data_type, values);
        let null_mask = BitVec::from_vec(null_mask);
        ensure!(
            null_mask.count_ones() + values.len() == row_count,
            UnexpectedValuesLengthSnafu {
                reason: "If null_mask is not empty, the sum of the number of nulls and the length of values must be equal to row_count."
            }
        );

        let mut idx_of_values = 0;
        for idx in 0..row_count {
            match is_null(&null_mask, idx) {
                Some(true) => builder.push_null(),
                _ => {
                    builder
                        .try_push_value_ref(values[idx_of_values].as_value_ref())
                        .context(CreateVectorSnafu)?;
                    idx_of_values += 1
                }
            }
        }
        Ok(builder.to_vector())
    }
}

fn is_null(null_mask: &BitVec, idx: usize) -> Option<bool> {
    null_mask.get(idx).as_deref().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_null() {
        let null_mask = BitVec::from_slice(&[0b0000_0001, 0b0000_1000]);

        assert_eq!(Some(true), is_null(&null_mask, 0));
        assert_eq!(Some(false), is_null(&null_mask, 1));
        assert_eq!(Some(false), is_null(&null_mask, 10));
        assert_eq!(Some(true), is_null(&null_mask, 11));
        assert_eq!(Some(false), is_null(&null_mask, 12));

        assert_eq!(None, is_null(&null_mask, 16));
        assert_eq!(None, is_null(&null_mask, 99));
    }
}
