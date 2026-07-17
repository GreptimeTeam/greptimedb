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

// Derived from Apache DataFusion's `datafusion/common/src/hash_utils.rs`,
// licensed under the Apache License, Version 2.0. Frozen from DataFusion commit
// 7a84c45104d35408f00bbf880268c47193b73b28, hash_utils.rs blob
// 3be6118c55ff2045d9ebdc7ef1a1f6899c50732d.

use std::collections::HashMap;

use ahash::RandomState;
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::*;
use arrow::{downcast_dictionary_array, downcast_primitive_array};
use datafusion_common::cast::{
    as_binary_view_array, as_boolean_array, as_fixed_size_list_array, as_generic_binary_array,
    as_large_list_array, as_large_list_view_array, as_list_array, as_list_view_array, as_map_array,
    as_string_array, as_string_view_array, as_struct_array, as_union_array,
};
use datafusion_common::error::Result;
use datafusion_common::internal_err;
use itertools::Itertools;

#[cfg(test)]
pub(super) const HASH_VERSION: &str = "count_hash-ahash-v1";

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_null(random_state: &RandomState, hashes_buffer: &'_ mut [u64], mul_col: bool) {
    if mul_col {
        hashes_buffer.iter_mut().for_each(|hash| {
            // stable hash for null value
            *hash = combine_hashes(random_state.hash_one(1), *hash);
        })
    } else {
        hashes_buffer.iter_mut().for_each(|hash| {
            *hash = random_state.hash_one(1);
        })
    }
}

trait HashValue {
    fn hash_one(&self, state: &RandomState) -> u64;
}

impl<T: HashValue + ?Sized> HashValue for &T {
    fn hash_one(&self, state: &RandomState) -> u64 {
        T::hash_one(self, state)
    }
}

macro_rules! hash_value {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(self)
            }
        })+
    };
}
hash_value!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128);
hash_value!(bool, str, [u8], IntervalDayTime, IntervalMonthDayNano);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(<$i>::from_ne_bytes(self.to_ne_bytes()))
            }
        })+
    };
}
hash_float_value!((half::f16, u16), (f32, u32), (f64, u64));

/// Builds hash values of PrimitiveArray and writes them into `hashes_buffer`
/// If `rehash==true` this combines the previous hash value in the buffer
/// with the new hash using `combine_hashes`
fn hash_array_primitive<T>(
    array: &PrimitiveArray<T>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    rehash: bool,
) where
    T: ArrowPrimitiveType<Native: HashValue>,
{
    assert_eq!(
        hashes_buffer.len(),
        array.len(),
        "hashes_buffer and array should be of equal length"
    );

    if array.null_count() == 0 {
        if rehash {
            for (hash, &value) in hashes_buffer.iter_mut().zip(array.values().iter()) {
                *hash = combine_hashes(value.hash_one(random_state), *hash);
            }
        } else {
            for (hash, &value) in hashes_buffer.iter_mut().zip(array.values().iter()) {
                *hash = value.hash_one(random_state);
            }
        }
    } else if rehash {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = combine_hashes(value.hash_one(random_state), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one(random_state);
            }
        }
    }
}

/// Hashes one array into the `hashes_buffer`
/// If `rehash==true` this combines the previous hash value in the buffer
/// with the new hash using `combine_hashes`
fn hash_array<T>(array: &T, random_state: &RandomState, hashes_buffer: &mut [u64], rehash: bool)
where
    T: ArrayAccessor,
    T::Item: HashValue,
{
    assert_eq!(
        hashes_buffer.len(),
        array.len(),
        "hashes_buffer and array should be of equal length"
    );

    if array.null_count() == 0 {
        if rehash {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                let value = unsafe { array.value_unchecked(i) };
                *hash = combine_hashes(value.hash_one(random_state), *hash);
            }
        } else {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one(random_state);
            }
        }
    } else if rehash {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = combine_hashes(value.hash_one(random_state), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                let value = unsafe { array.value_unchecked(i) };
                *hash = value.hash_one(random_state);
            }
        }
    }
}

/// Hash a StringView or BytesView array
///
/// Templated to optimize inner loop based on presence of nulls and external buffers.
///
/// HAS_NULLS: do we have to check null in the inner loop
/// HAS_BUFFERS: if true, array has external buffers; if false, all strings are inlined/ less then 12 bytes
/// REHASH: if true, combining with existing hash, otherwise initializing
#[inline(never)]
fn hash_string_view_array_inner<
    T: ByteViewType,
    const HAS_NULLS: bool,
    const HAS_BUFFERS: bool,
    const REHASH: bool,
>(
    array: &GenericByteViewArray<T>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) {
    assert_eq!(
        hashes_buffer.len(),
        array.len(),
        "hashes_buffer and array should be of equal length"
    );

    let buffers = array.data_buffers();
    let view_bytes = |view_len: u32, view: u128| {
        let view = ByteView::from(view);
        let offset = view.offset as usize;
        // SAFETY: view is a valid view as it came from the array
        unsafe {
            let data = buffers.get_unchecked(view.buffer_index as usize);
            data.get_unchecked(offset..offset + view_len as usize)
        }
    };

    let hashes_and_views = hashes_buffer.iter_mut().zip(array.views().iter());
    for (i, (hash, &v)) in hashes_and_views.enumerate() {
        if HAS_NULLS && array.is_null(i) {
            continue;
        }
        let view_len = v as u32;
        // all views are inlined, no need to access external buffers
        if !HAS_BUFFERS || view_len <= 12 {
            if REHASH {
                *hash = combine_hashes(v.hash_one(random_state), *hash);
            } else {
                *hash = v.hash_one(random_state);
            }
            continue;
        }
        // view is not inlined, so we need to hash the bytes as well
        let value = view_bytes(view_len, v);
        if REHASH {
            *hash = combine_hashes(value.hash_one(random_state), *hash);
        } else {
            *hash = value.hash_one(random_state);
        }
    }
}

/// Builds hash values for array views and writes them into `hashes_buffer`
/// If `rehash==true` this combines the previous hash value in the buffer
/// with the new hash using `combine_hashes`
fn hash_generic_byte_view_array<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    rehash: bool,
) {
    // instantiate the correct version based on presence of nulls and external buffers
    match (
        array.null_count() != 0,
        !array.data_buffers().is_empty(),
        rehash,
    ) {
        // no nulls or buffers ==> hash the inlined views directly
        // don't call the inner function as Rust seems better able to inline this simpler code (2-3% faster)
        (false, false, false) => {
            for (hash, &view) in hashes_buffer.iter_mut().zip(array.views().iter()) {
                *hash = view.hash_one(random_state);
            }
        }
        (false, false, true) => {
            for (hash, &view) in hashes_buffer.iter_mut().zip(array.views().iter()) {
                *hash = combine_hashes(view.hash_one(random_state), *hash);
            }
        }
        (false, true, false) => hash_string_view_array_inner::<T, false, true, false>(
            array,
            random_state,
            hashes_buffer,
        ),
        (false, true, true) => {
            hash_string_view_array_inner::<T, false, true, true>(array, random_state, hashes_buffer)
        }
        (true, false, false) => hash_string_view_array_inner::<T, true, false, false>(
            array,
            random_state,
            hashes_buffer,
        ),
        (true, false, true) => {
            hash_string_view_array_inner::<T, true, false, true>(array, random_state, hashes_buffer)
        }
        (true, true, false) => {
            hash_string_view_array_inner::<T, true, true, false>(array, random_state, hashes_buffer)
        }
        (true, true, true) => {
            hash_string_view_array_inner::<T, true, true, true>(array, random_state, hashes_buffer)
        }
    }
}

/// Hash dictionary array with compile-time specialization for null handling.
///
/// Uses const generics to eliminate runtim branching in the hot loop:
/// - `HAS_NULL_KEYS`: Whether to check for null dictionary keys
/// - `HAS_NULL_VALUES`: Whether to check for null dictionary values
/// - `MULTI_COL`: Whether to combine with existing hash (true) or initialize (false)
#[inline(never)]
fn hash_dictionary_inner<
    K: ArrowDictionaryKeyType,
    const HAS_NULL_KEYS: bool,
    const HAS_NULL_VALUES: bool,
    const MULTI_COL: bool,
>(
    array: &DictionaryArray<K>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = array.values();
    let mut dict_hashes = vec![0; dict_values.len()];
    create_hashes(
        std::slice::from_ref(dict_values),
        random_state,
        &mut dict_hashes,
    )?;

    if HAS_NULL_KEYS {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.as_usize();
                if !HAS_NULL_VALUES || dict_values.is_valid(idx) {
                    if MULTI_COL {
                        *hash = combine_hashes(dict_hashes[idx], *hash);
                    } else {
                        *hash = dict_hashes[idx];
                    }
                }
            }
        }
    } else {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().values()) {
            let idx = key.as_usize();
            if !HAS_NULL_VALUES || dict_values.is_valid(idx) {
                if MULTI_COL {
                    *hash = combine_hashes(dict_hashes[idx], *hash);
                } else {
                    *hash = dict_hashes[idx];
                }
            }
        }
    }
    Ok(())
}

/// Hash the values in a dictionary array
fn hash_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    multi_col: bool,
) -> Result<()> {
    let has_null_keys = array.keys().null_count() != 0;
    let has_null_values = array.values().null_count() != 0;

    // Dispatcher based on null presence and multi-column mode
    // Should reduce branching within hot loops
    match (has_null_keys, has_null_values, multi_col) {
        (false, false, false) => {
            hash_dictionary_inner::<K, false, false, false>(array, random_state, hashes_buffer)
        }
        (false, false, true) => {
            hash_dictionary_inner::<K, false, false, true>(array, random_state, hashes_buffer)
        }
        (false, true, false) => {
            hash_dictionary_inner::<K, false, true, false>(array, random_state, hashes_buffer)
        }
        (false, true, true) => {
            hash_dictionary_inner::<K, false, true, true>(array, random_state, hashes_buffer)
        }
        (true, false, false) => {
            hash_dictionary_inner::<K, true, false, false>(array, random_state, hashes_buffer)
        }
        (true, false, true) => {
            hash_dictionary_inner::<K, true, false, true>(array, random_state, hashes_buffer)
        }
        (true, true, false) => {
            hash_dictionary_inner::<K, true, true, false>(array, random_state, hashes_buffer)
        }
        (true, true, true) => {
            hash_dictionary_inner::<K, true, true, true>(array, random_state, hashes_buffer)
        }
    }
}

fn hash_struct_array(
    array: &StructArray,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    let nulls = array.nulls();
    let row_len = array.len();

    // Create hashes for each row that combines the hashes over all the column at that row.
    let mut values_hashes = vec![0u64; row_len];
    create_hashes(array.columns(), random_state, &mut values_hashes)?;

    // Separate paths to avoid allocating Vec when there are no nulls
    if let Some(nulls) = nulls {
        for i in nulls.valid_indices() {
            let hash = &mut hashes_buffer[i];
            *hash = combine_hashes(*hash, values_hashes[i]);
        }
    } else {
        for i in 0..row_len {
            let hash = &mut hashes_buffer[i];
            *hash = combine_hashes(*hash, values_hashes[i]);
        }
    }

    Ok(())
}

fn hash_map_array(
    array: &MapArray,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    let nulls = array.nulls();
    let offsets = array.offsets();

    // Create hashes for each entry in each row
    let first_offset = offsets.first().copied().unwrap_or_default() as usize;
    let last_offset = offsets.last().copied().unwrap_or_default() as usize;
    let entries_len = last_offset - first_offset;

    // Only hash the entries that are actually referenced
    let mut values_hashes = vec![0u64; entries_len];
    let entries = array.entries();
    let sliced_columns: Vec<ArrayRef> = entries
        .columns()
        .iter()
        .map(|col| col.slice(first_offset, entries_len))
        .collect();
    create_hashes(&sliced_columns, random_state, &mut values_hashes)?;

    // Combine the hashes for entries on each row with each other and previous hash for that row
    // Adjust indices by first_offset since values_hashes is sliced starting from first_offset
    if let Some(nulls) = nulls {
        for (i, (start, stop)) in offsets.iter().zip(offsets.iter().skip(1)).enumerate() {
            if nulls.is_valid(i) {
                let hash = &mut hashes_buffer[i];
                for values_hash in
                    &values_hashes[start.as_usize() - first_offset..stop.as_usize() - first_offset]
                {
                    *hash = combine_hashes(*hash, *values_hash);
                }
            }
        }
    } else {
        for (i, (start, stop)) in offsets.iter().zip(offsets.iter().skip(1)).enumerate() {
            let hash = &mut hashes_buffer[i];
            for values_hash in
                &values_hashes[start.as_usize() - first_offset..stop.as_usize() - first_offset]
            {
                *hash = combine_hashes(*hash, *values_hash);
            }
        }
    }

    Ok(())
}

fn hash_list_array<OffsetSize>(
    array: &GenericListArray<OffsetSize>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()>
where
    OffsetSize: OffsetSizeTrait,
{
    // In case values is sliced, hash only the bytes used by the offsets of this ListArray
    let first_offset = array.value_offsets().first().cloned().unwrap_or_default();
    let last_offset = array.value_offsets().last().cloned().unwrap_or_default();
    let value_bytes_len = (last_offset - first_offset).as_usize();
    let mut values_hashes = vec![0u64; value_bytes_len];
    create_hashes(
        &[array
            .values()
            .slice(first_offset.as_usize(), value_bytes_len)],
        random_state,
        &mut values_hashes,
    )?;

    if array.null_count() > 0 {
        for (i, (start, stop)) in array.value_offsets().iter().tuple_windows().enumerate() {
            if array.is_valid(i) {
                let hash = &mut hashes_buffer[i];
                for values_hash in &values_hashes
                    [(*start - first_offset).as_usize()..(*stop - first_offset).as_usize()]
                {
                    *hash = combine_hashes(*hash, *values_hash);
                }
            }
        }
    } else {
        for ((start, stop), hash) in array
            .value_offsets()
            .iter()
            .tuple_windows()
            .zip(hashes_buffer.iter_mut())
        {
            for values_hash in &values_hashes
                [(*start - first_offset).as_usize()..(*stop - first_offset).as_usize()]
            {
                *hash = combine_hashes(*hash, *values_hash);
            }
        }
    }
    Ok(())
}

fn hash_list_view_array<OffsetSize>(
    array: &GenericListViewArray<OffsetSize>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()>
where
    OffsetSize: OffsetSizeTrait,
{
    let values = array.values();
    let offsets = array.value_offsets();
    let sizes = array.value_sizes();
    let nulls = array.nulls();
    let mut values_hashes = vec![0u64; values.len()];
    create_hashes(
        std::slice::from_ref(values),
        random_state,
        &mut values_hashes,
    )?;
    if let Some(nulls) = nulls {
        for (i, (offset, size)) in offsets.iter().zip(sizes.iter()).enumerate() {
            if nulls.is_valid(i) {
                let hash = &mut hashes_buffer[i];
                let start = offset.as_usize();
                let end = start + size.as_usize();
                for values_hash in &values_hashes[start..end] {
                    *hash = combine_hashes(*hash, *values_hash);
                }
            }
        }
    } else {
        for (i, (offset, size)) in offsets.iter().zip(sizes.iter()).enumerate() {
            let hash = &mut hashes_buffer[i];
            let start = offset.as_usize();
            let end = start + size.as_usize();
            for values_hash in &values_hashes[start..end] {
                *hash = combine_hashes(*hash, *values_hash);
            }
        }
    }
    Ok(())
}

fn hash_union_array(
    array: &UnionArray,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    let DataType::Union(union_fields, _mode) = array.data_type() else {
        unreachable!()
    };

    if array.is_dense() {
        // Dense union: children only contain values of their type, so they're already compact.
        // Use the default hashing approach which is efficient for dense unions.
        hash_union_array_default(array, union_fields, random_state, hashes_buffer)
    } else {
        // Sparse union: each child has the same length as the union array.
        // Optimization: only hash the elements that are actually referenced by type_ids,
        // instead of hashing all K*N elements (where K = num types, N = array length).
        hash_sparse_union_array(array, union_fields, random_state, hashes_buffer)
    }
}

/// Default hashing for union arrays - hashes all elements of each child array fully.
///
/// This approach works for both dense and sparse union arrays:
/// - Dense unions: children are compact (each child only contains values of that type)
/// - Sparse unions: children have the same length as the union array
///
/// For sparse unions with 3+ types, the optimized take/scatter approach in
/// `hash_sparse_union_array` is more efficient, but for 1-2 types or dense unions,
/// this simpler approach is preferred.
fn hash_union_array_default(
    array: &UnionArray,
    union_fields: &UnionFields,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    let mut child_hashes: HashMap<i8, Vec<u64>> = HashMap::with_capacity(union_fields.len());

    // Hash each child array fully
    for (type_id, _field) in union_fields.iter() {
        let child = array.child(type_id);
        let mut child_hash_buffer = vec![0; child.len()];
        create_hashes(
            std::slice::from_ref(child),
            random_state,
            &mut child_hash_buffer,
        )?;

        child_hashes.insert(type_id, child_hash_buffer);
    }

    // Combine hashes for each row using the appropriate child offset
    // For dense unions: value_offset points to the actual position in the child
    // For sparse unions: value_offset equals the row index
    #[expect(clippy::needless_range_loop)]
    for i in 0..array.len() {
        let type_id = array.type_id(i);
        let child_offset = array.value_offset(i);

        let child_hash = child_hashes.get(&type_id).expect("invalid type_id");
        hashes_buffer[i] = combine_hashes(hashes_buffer[i], child_hash[child_offset]);
    }

    Ok(())
}

/// Hash a sparse union array.
/// Sparse unions have child arrays with the same length as the union array.
/// For 3+ types, we optimize by only hashing the N elements that are actually used
/// (via take/scatter), instead of hashing all K*N elements.
///
/// For 1-2 types, the overhead of take/scatter outweighs the benefit, so we use
/// the default approach of hashing all children (same as dense unions).
fn hash_sparse_union_array(
    array: &UnionArray,
    union_fields: &UnionFields,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    use std::collections::HashMap;

    // For 1-2 types, the take/scatter overhead isn't worth it.
    // Fall back to the default approach (same as dense union).
    if union_fields.len() <= 2 {
        return hash_union_array_default(array, union_fields, random_state, hashes_buffer);
    }

    let type_ids = array.type_ids();

    // Group indices by type_id
    let mut indices_by_type: HashMap<i8, Vec<u32>> = HashMap::new();
    for (i, &type_id) in type_ids.iter().enumerate() {
        indices_by_type.entry(type_id).or_default().push(i as u32);
    }

    // For each type, extract only the needed elements, hash them, and scatter back
    for (type_id, _field) in union_fields.iter() {
        if let Some(indices) = indices_by_type.get(&type_id) {
            if indices.is_empty() {
                continue;
            }

            let child = array.child(type_id);
            let indices_array = UInt32Array::from(indices.clone());

            // Extract only the elements we need using take()
            let filtered = take(child.as_ref(), &indices_array, None)?;

            // Hash the filtered array
            let mut filtered_hashes = vec![0u64; filtered.len()];
            create_hashes(
                std::slice::from_ref(&filtered),
                random_state,
                &mut filtered_hashes,
            )?;

            // Scatter hashes back to correct positions
            for (hash, &idx) in filtered_hashes.iter().zip(indices.iter()) {
                hashes_buffer[idx as usize] = combine_hashes(hashes_buffer[idx as usize], *hash);
            }
        }
    }

    Ok(())
}

fn hash_fixed_list_array(
    array: &FixedSizeListArray,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    let values = array.values();
    let value_length = array.value_length() as usize;
    let nulls = array.nulls();
    let mut values_hashes = vec![0u64; values.len()];
    create_hashes(
        std::slice::from_ref(values),
        random_state,
        &mut values_hashes,
    )?;
    if let Some(nulls) = nulls {
        for i in 0..array.len() {
            if nulls.is_valid(i) {
                let hash = &mut hashes_buffer[i];
                for values_hash in &values_hashes[i * value_length..(i + 1) * value_length] {
                    *hash = combine_hashes(*hash, *values_hash);
                }
            }
        }
    } else {
        for i in 0..array.len() {
            let hash = &mut hashes_buffer[i];
            for values_hash in &values_hashes[i * value_length..(i + 1) * value_length] {
                *hash = combine_hashes(*hash, *values_hash);
            }
        }
    }
    Ok(())
}

/// Inner hash function for RunArray
#[inline(never)]
fn hash_run_array_inner<R: RunEndIndexType, const HAS_NULL_VALUES: bool, const REHASH: bool>(
    array: &RunArray<R>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    // We find the relevant runs that cover potentially sliced arrays, so we can only hash those
    // values. Then we find the runs that refer to the original runs and ensure that we apply
    // hashes correctly to the sliced, whether sliced at the start, end, or both.
    let array_offset = array.offset();
    let array_len = array.len();

    if array_len == 0 {
        return Ok(());
    }

    let run_ends = array.run_ends();
    let run_ends_values = run_ends.values();
    let values = array.values();

    let start_physical_index = array.get_start_physical_index();
    // get_end_physical_index returns the inclusive last index, but we need the exclusive range end
    // for the operations we use below.
    let end_physical_index = array.get_end_physical_index() + 1;

    let sliced_values = values.slice(
        start_physical_index,
        end_physical_index - start_physical_index,
    );
    let mut values_hashes = vec![0u64; sliced_values.len()];
    create_hashes(
        std::slice::from_ref(&sliced_values),
        random_state,
        &mut values_hashes,
    )?;

    let mut start_in_slice = 0;
    for (adjusted_physical_index, &absolute_run_end) in run_ends_values
        [start_physical_index..end_physical_index]
        .iter()
        .enumerate()
    {
        let absolute_run_end = absolute_run_end.as_usize();
        let end_in_slice = (absolute_run_end - array_offset).min(array_len);

        if HAS_NULL_VALUES && sliced_values.is_null(adjusted_physical_index) {
            start_in_slice = end_in_slice;
            continue;
        }

        let value_hash = values_hashes[adjusted_physical_index];
        let run_slice = &mut hashes_buffer[start_in_slice..end_in_slice];

        if REHASH {
            for hash in run_slice.iter_mut() {
                *hash = combine_hashes(value_hash, *hash);
            }
        } else {
            run_slice.fill(value_hash);
        }

        start_in_slice = end_in_slice;
    }

    Ok(())
}

fn hash_run_array<R: RunEndIndexType>(
    array: &RunArray<R>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    rehash: bool,
) -> Result<()> {
    let has_null_values = array.values().null_count() != 0;

    match (has_null_values, rehash) {
        (false, false) => {
            hash_run_array_inner::<R, false, false>(array, random_state, hashes_buffer)
        }
        (false, true) => hash_run_array_inner::<R, false, true>(array, random_state, hashes_buffer),
        (true, false) => hash_run_array_inner::<R, true, false>(array, random_state, hashes_buffer),
        (true, true) => hash_run_array_inner::<R, true, true>(array, random_state, hashes_buffer),
    }
}

/// Internal helper function that hashes a single array and either initializes or combines
/// the hash values in the buffer.
fn hash_single_array(
    array: &dyn Array,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    rehash: bool,
) -> Result<()> {
    downcast_primitive_array! {
        array => hash_array_primitive(array, random_state, hashes_buffer, rehash),
        DataType::Null => hash_null(random_state, hashes_buffer, rehash),
        DataType::Boolean => hash_array(&as_boolean_array(array)?, random_state, hashes_buffer, rehash),
        DataType::Utf8 => hash_array(&as_string_array(array)?, random_state, hashes_buffer, rehash),
        DataType::Utf8View => hash_generic_byte_view_array(as_string_view_array(array)?, random_state, hashes_buffer, rehash),
        DataType::LargeUtf8 => hash_array(&as_largestring_array(array), random_state, hashes_buffer, rehash),
        DataType::Binary => hash_array(&as_generic_binary_array::<i32>(array)?, random_state, hashes_buffer, rehash),
        DataType::BinaryView => hash_generic_byte_view_array(as_binary_view_array(array)?, random_state, hashes_buffer, rehash),
        DataType::LargeBinary => hash_array(&as_generic_binary_array::<i64>(array)?, random_state, hashes_buffer, rehash),
        DataType::FixedSizeBinary(_) => {
            let array: &FixedSizeBinaryArray = array.as_any().downcast_ref().unwrap();
            hash_array(&array, random_state, hashes_buffer, rehash)
        }
        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            array => hash_dictionary(array, random_state, hashes_buffer, rehash)?,
            _ => unreachable!()
        }
        DataType::Struct(_) => {
            let array = as_struct_array(array)?;
            hash_struct_array(array, random_state, hashes_buffer)?;
        }
        DataType::List(_) => {
            let array = as_list_array(array)?;
            hash_list_array(array, random_state, hashes_buffer)?;
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(array)?;
            hash_list_array(array, random_state, hashes_buffer)?;
        }
        DataType::ListView(_) => {
            let array = as_list_view_array(array)?;
            hash_list_view_array(array, random_state, hashes_buffer)?;
        }
        DataType::LargeListView(_) => {
            let array = as_large_list_view_array(array)?;
            hash_list_view_array(array, random_state, hashes_buffer)?;
        }
        DataType::Map(_, _) => {
            let array = as_map_array(array)?;
            hash_map_array(array, random_state, hashes_buffer)?;
        }
        DataType::FixedSizeList(_,_) => {
            let array = as_fixed_size_list_array(array)?;
            hash_fixed_list_array(array, random_state, hashes_buffer)?;
        }
        DataType::Union(_, _) => {
            let array = as_union_array(array)?;
            hash_union_array(array, random_state, hashes_buffer)?;
        }
        DataType::RunEndEncoded(_, _) => arrow::array::downcast_run_array! {
            array => hash_run_array(array, random_state, hashes_buffer, rehash)?,
            _ => unreachable!()
        }
        _ => {
            // This is internal because we should have caught this before.
            return internal_err!(
                "Unsupported data type in hasher: {}",
                array.data_type()
            );
        }
    }
    Ok(())
}

/// Creates hash values for every row, based on the values in the columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately.
pub(super) fn create_hashes(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
) -> Result<()> {
    for (i, array) in arrays.iter().enumerate() {
        // combine hashes with `combine_hashes` for all columns besides the first
        let rehash = i >= 1;
        hash_single_array(array.as_ref(), random_state, hashes_buffer, rehash)?;
    }
    Ok(())
}
