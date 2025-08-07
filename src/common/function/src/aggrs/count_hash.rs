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

//! `CountHash` / `count_hash` is a hash-based approximate distinct count function.
//!
//! It is a variant of `CountDistinct` that uses a hash function to approximate the
//! distinct count.
//! It is designed to be more efficient than `CountDistinct` for large datasets,
//! but it is not as accurate, as the hash value may be collision.

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use ahash::RandomState;
use datafusion_common::cast::as_list_array;
use datafusion_common::error::Result;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{internal_err, not_impl_err, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, TypeSignature, Volatility,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filtered_null_mask;
use datatypes::arrow;
use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, ListArray, UInt64Array,
};
use datatypes::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef};

use crate::function_registry::FunctionRegistry;

type HashValueType = u64;

// read from /dev/urandom 4047821dc6144e4b2abddf23ad4171126a52eeecd26eff2191cf673b965a7875
const RANDOM_SEED_0: u64 = 0x4047821dc6144e4b;
const RANDOM_SEED_1: u64 = 0x2abddf23ad417112;
const RANDOM_SEED_2: u64 = 0x6a52eeecd26eff21;
const RANDOM_SEED_3: u64 = 0x91cf673b965a7875;

impl CountHash {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_aggr(CountHash::udf_impl());
    }

    pub fn udf_impl() -> AggregateUDF {
        AggregateUDF::new_from_impl(CountHash {
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CountHash {
    signature: Signature,
}

impl AggregateUDFImpl for CountHash {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "count_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new_list(
            format_state_name(args.name, "count_hash"),
            Field::new_list_field(DataType::UInt64, true),
            // For count_hash accumulator, null list item stands for an
            // empty value set (i.e., all NULL value so far for that group).
            true,
        ))])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.exprs.len() > 1 {
            return not_impl_err!("count_hash with multiple arguments");
        }

        Ok(Box::new(CountHashAccumulator {
            values: HashSet::default(),
            random_state: RandomState::with_seeds(
                RANDOM_SEED_0,
                RANDOM_SEED_1,
                RANDOM_SEED_2,
                RANDOM_SEED_3,
            ),
            batch_hashes: vec![],
        }))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.exprs.len() > 1 {
            return not_impl_err!("count_hash with multiple arguments");
        }

        Ok(Box::new(CountHashGroupAccumulator::new()))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }

    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {
        SetMonotonicity::Increasing
    }
}

/// GroupsAccumulator for `count_hash` aggregate function
#[derive(Debug)]
pub struct CountHashGroupAccumulator {
    /// One HashSet per group to track distinct values
    distinct_sets: Vec<HashSet<HashValueType, RandomState>>,
    random_state: RandomState,
    batch_hashes: Vec<HashValueType>,
}

impl Default for CountHashGroupAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CountHashGroupAccumulator {
    pub fn new() -> Self {
        Self {
            distinct_sets: vec![],
            random_state: RandomState::with_seeds(
                RANDOM_SEED_0,
                RANDOM_SEED_1,
                RANDOM_SEED_2,
                RANDOM_SEED_3,
            ),
            batch_hashes: vec![],
        }
    }

    fn ensure_sets(&mut self, total_num_groups: usize) {
        if self.distinct_sets.len() < total_num_groups {
            self.distinct_sets
                .resize_with(total_num_groups, HashSet::default);
        }
    }
}

impl GroupsAccumulator for CountHashGroupAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "count_hash expects a single argument");
        self.ensure_sets(total_num_groups);

        let array = &values[0];
        self.batch_hashes.clear();
        self.batch_hashes.resize(array.len(), 0);
        let hashes = create_hashes(
            &[ArrayRef::clone(array)],
            &self.random_state,
            &mut self.batch_hashes,
        )?;

        // Use a pattern similar to accumulate_indices to process rows
        // that are not null and pass the filter
        let nulls = array.logical_nulls();

        match (nulls.as_ref(), opt_filter) {
            (None, None) => {
                // No nulls, no filter - process all rows
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    self.distinct_sets[group_idx].insert(hashes[row_idx]);
                }
            }
            (Some(nulls), None) => {
                // Has nulls, no filter
                for (row_idx, (&group_idx, is_valid)) in
                    group_indices.iter().zip(nulls.iter()).enumerate()
                {
                    if is_valid {
                        self.distinct_sets[group_idx].insert(hashes[row_idx]);
                    }
                }
            }
            (None, Some(filter)) => {
                // No nulls, has filter
                for (row_idx, (&group_idx, filter_value)) in
                    group_indices.iter().zip(filter.iter()).enumerate()
                {
                    if let Some(true) = filter_value {
                        self.distinct_sets[group_idx].insert(hashes[row_idx]);
                    }
                }
            }
            (Some(nulls), Some(filter)) => {
                // Has nulls and filter
                let iter = filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(nulls.iter())
                    .enumerate();

                for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
                    if is_valid && filter_value == Some(true) {
                        self.distinct_sets[group_idx].insert(hashes[row_idx]);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let distinct_sets: Vec<HashSet<u64, RandomState>> =
            emit_to.take_needed(&mut self.distinct_sets);

        let counts = distinct_sets
            .iter()
            .map(|set| set.len() as i64)
            .collect::<Vec<_>>();
        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(
            values.len(),
            1,
            "count_hash merge expects a single state array"
        );
        self.ensure_sets(total_num_groups);

        let list_array = as_list_array(&values[0])?;

        // For each group in the incoming batch
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if i < list_array.len() {
                let inner_array = list_array.value(i);
                let inner_array = inner_array.as_any().downcast_ref::<UInt64Array>().unwrap();
                // Add each value to our set for this group
                for j in 0..inner_array.len() {
                    if !inner_array.is_null(j) {
                        self.distinct_sets[group_idx].insert(inner_array.value(j));
                    }
                }
            }
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let distinct_sets: Vec<HashSet<u64, RandomState>> =
            emit_to.take_needed(&mut self.distinct_sets);

        let mut offsets = Vec::with_capacity(distinct_sets.len() + 1);
        offsets.push(0);
        let mut curr_len = 0i32;

        let mut value_iter = distinct_sets
            .into_iter()
            .flat_map(|set| {
                // build offset
                curr_len += set.len() as i32;
                offsets.push(curr_len);
                // convert into iter
                set.into_iter()
            })
            .peekable();
        let data_array: ArrayRef = if value_iter.peek().is_none() {
            arrow::array::new_empty_array(&DataType::UInt64) as _
        } else {
            Arc::new(UInt64Array::from_iter_values(value_iter))
        };
        let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));

        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::UInt64, true)),
            offset_buffer,
            data_array,
            None,
        );

        Ok(vec![Arc::new(list_array) as _])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // For a single hash value per row, create a list array with that value
        assert_eq!(values.len(), 1, "count_hash expects a single argument");
        let values = ArrayRef::clone(&values[0]);

        let offsets = OffsetBuffer::new(ScalarBuffer::from_iter(0..values.len() as i32 + 1));
        let nulls = filtered_null_mask(opt_filter, &values);
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::UInt64, true)),
            offsets,
            values,
            nulls,
        );

        Ok(vec![Arc::new(list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        // Base size of the struct
        let mut size = size_of::<Self>();

        // Size of the vector holding the HashSets
        size += size_of::<Vec<HashSet<HashValueType, RandomState>>>()
            + self.distinct_sets.capacity() * size_of::<HashSet<HashValueType, RandomState>>();

        // Estimate HashSet contents size more efficiently
        // Instead of iterating through all values which is expensive, use an approximation
        for set in &self.distinct_sets {
            // Base size of the HashSet
            size += set.capacity() * size_of::<HashValueType>();
        }

        size
    }
}

#[derive(Debug)]
struct CountHashAccumulator {
    values: HashSet<HashValueType, RandomState>,
    random_state: RandomState,
    batch_hashes: Vec<HashValueType>,
}

impl CountHashAccumulator {
    // calculating the size for fixed length values, taking first batch size *
    // number of batches.
    fn fixed_size(&self) -> usize {
        size_of_val(self) + (size_of::<HashValueType>() * self.values.capacity())
    }
}

impl Accumulator for CountHashAccumulator {
    /// Returns the distinct values seen so far as (one element) ListArray.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values = self.values.iter().cloned().collect::<Vec<_>>();
        let arr = Arc::new(UInt64Array::from(values)) as _;
        let list_scalar = SingleRowListArrayBuilder::new(arr).build_list_scalar();
        Ok(vec![list_scalar])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        if arr.data_type() == &DataType::Null {
            return Ok(());
        }

        self.batch_hashes.clear();
        self.batch_hashes.resize(arr.len(), 0);
        let hashes = create_hashes(
            &[ArrayRef::clone(arr)],
            &self.random_state,
            &mut self.batch_hashes,
        )?;
        for hash in hashes.as_slice() {
            self.values.insert(*hash);
        }
        Ok(())
    }

    /// Merges multiple sets of distinct values into the current set.
    ///
    /// The input to this function is a `ListArray` with **multiple** rows,
    /// where each row contains the values from a partial aggregate's phase (e.g.
    /// the result of calling `Self::state` on multiple accumulators).
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(states.len(), 1, "array_agg states must be singleton!");
        let array = &states[0];
        let list_array = array.as_list::<i32>();
        for inner_array in list_array.iter() {
            let Some(inner_array) = inner_array else {
                return internal_err!(
                    "Intermediate results of count_hash should always be non null"
                );
            };
            let hash_array = inner_array.as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..hash_array.len() {
                self.values.insert(hash_array.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        self.fixed_size()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::{Array, BooleanArray, Int32Array, Int64Array};

    use super::*;

    fn create_test_accumulator() -> CountHashAccumulator {
        CountHashAccumulator {
            values: HashSet::default(),
            random_state: RandomState::with_seeds(
                RANDOM_SEED_0,
                RANDOM_SEED_1,
                RANDOM_SEED_2,
                RANDOM_SEED_3,
            ),
            batch_hashes: vec![],
        }
    }

    #[test]
    fn test_count_hash_accumulator() -> Result<()> {
        let mut acc = create_test_accumulator();

        // Test with some data
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
            None,
        ])) as ArrayRef;
        acc.update_batch(&[array])?;
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Int64(Some(4)));

        // Test with empty data
        let mut acc = create_test_accumulator();
        let array = Arc::new(Int32Array::from(vec![] as Vec<Option<i32>>)) as ArrayRef;
        acc.update_batch(&[array])?;
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        // Test with only nulls
        let mut acc = create_test_accumulator();
        let array = Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef;
        acc.update_batch(&[array])?;
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Int64(Some(1)));

        Ok(())
    }

    #[test]
    fn test_count_hash_accumulator_merge() -> Result<()> {
        // Accumulator 1
        let mut acc1 = create_test_accumulator();
        let array1 = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef;
        acc1.update_batch(&[array1])?;
        let state1 = acc1.state()?;

        // Accumulator 2
        let mut acc2 = create_test_accumulator();
        let array2 = Arc::new(Int32Array::from(vec![Some(3), Some(4), Some(5)])) as ArrayRef;
        acc2.update_batch(&[array2])?;
        let state2 = acc2.state()?;

        // Merge state1 and state2 into a new accumulator
        let mut acc_merged = create_test_accumulator();
        let state_array1 = state1[0].to_array()?;
        let state_array2 = state2[0].to_array()?;

        acc_merged.merge_batch(&[state_array1])?;
        acc_merged.merge_batch(&[state_array2])?;

        let result = acc_merged.evaluate()?;
        // Distinct values are {1, 2, 3, 4, 5}, so count is 5
        assert_eq!(result, ScalarValue::Int64(Some(5)));

        Ok(())
    }

    fn create_test_group_accumulator() -> CountHashGroupAccumulator {
        CountHashGroupAccumulator::new()
    }

    #[test]
    fn test_count_hash_group_accumulator() -> Result<()> {
        let mut acc = create_test_group_accumulator();
        let values = Arc::new(Int32Array::from(vec![1, 2, 1, 3, 2, 4, 5])) as ArrayRef;
        let group_indices = vec![0, 1, 0, 0, 1, 2, 0];
        let total_num_groups = 3;

        acc.update_batch(&[values], &group_indices, None, total_num_groups)?;

        let result_array = acc.evaluate(EmitTo::All)?;
        let result = result_array.as_any().downcast_ref::<Int64Array>().unwrap();

        // Group 0: {1, 3, 5} -> 3
        // Group 1: {2} -> 1
        // Group 2: {4} -> 1
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), 1);

        Ok(())
    }

    #[test]
    fn test_count_hash_group_accumulator_with_filter() -> Result<()> {
        let mut acc = create_test_group_accumulator();
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
        let group_indices = vec![0, 0, 1, 1, 2, 2];
        let filter = BooleanArray::from(vec![true, false, true, true, false, true]);
        let total_num_groups = 3;

        acc.update_batch(&[values], &group_indices, Some(&filter), total_num_groups)?;

        let result_array = acc.evaluate(EmitTo::All)?;
        let result = result_array.as_any().downcast_ref::<Int64Array>().unwrap();

        // Group 0: {1} (2 is filtered out) -> 1
        // Group 1: {3, 4} -> 2
        // Group 2: {6} (5 is filtered out) -> 1
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), 1);

        Ok(())
    }

    #[test]
    fn test_count_hash_group_accumulator_merge() -> Result<()> {
        // Accumulator 1
        let mut acc1 = create_test_group_accumulator();
        let values1 = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let group_indices1 = vec![0, 0, 1, 1];
        acc1.update_batch(&[values1], &group_indices1, None, 2)?;
        // acc1 state: group 0 -> {1, 2}, group 1 -> {3, 4}
        let state1 = acc1.state(EmitTo::All)?;

        // Accumulator 2
        let mut acc2 = create_test_group_accumulator();
        let values2 = Arc::new(Int32Array::from(vec![5, 6, 1, 3])) as ArrayRef;
        // Merge into different group indices
        let group_indices2 = vec![2, 2, 0, 1];
        acc2.update_batch(&[values2], &group_indices2, None, 3)?;
        // acc2 state: group 0 -> {1}, group 1 -> {3}, group 2 -> {5, 6}

        // Merge state from acc1 into acc2
        // We will merge acc1's group 0 into acc2's group 0
        // and acc1's group 1 into acc2's group 2
        let merge_group_indices = vec![0, 2];
        acc2.merge_batch(&state1, &merge_group_indices, None, 3)?;

        let result_array = acc2.evaluate(EmitTo::All)?;
        let result = result_array.as_any().downcast_ref::<Int64Array>().unwrap();

        // Final state of acc2:
        // Group 0: {1} U {1, 2} -> {1, 2}, count = 2
        // Group 1: {3}, count = 1
        // Group 2: {5, 6} U {3, 4} -> {3, 4, 5, 6}, count = 4
        assert_eq!(result.value(0), 2);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), 4);

        Ok(())
    }

    #[test]
    fn test_size() {
        let acc = create_test_group_accumulator();
        // Just test it doesn't crash and returns a value.
        assert!(acc.size() > 0);
    }
}
