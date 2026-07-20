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

//! Shared native histogram field contract.
//!
//! Prom remote-write v2 stores these names as children of one Struct field, while
//! metric-engine uses the same contract to recognize native histogram tables.

use std::sync::Arc;

use datatypes::data_type::ConcreteDataType;
use datatypes::types::{StructField, StructType};
use once_cell::sync::Lazy;

pub const NATIVE_HISTOGRAM_FIELD: &str = "greptime_native_histogram";
pub const SCHEMA_FIELD: &str = "schema";
pub const ZERO_THRESHOLD_FIELD: &str = "zero_threshold";
pub const SUM_FIELD: &str = "sum";
pub const RESET_HINT_FIELD: &str = "reset_hint";
pub const START_TIMESTAMP_FIELD: &str = "start_timestamp";
pub const CUSTOM_VALUES_FIELD: &str = "custom_values";
pub const POSITIVE_SPAN_OFFSETS_FIELD: &str = "positive_span_offsets";
pub const POSITIVE_SPAN_LENGTHS_FIELD: &str = "positive_span_lengths";
pub const NEGATIVE_SPAN_OFFSETS_FIELD: &str = "negative_span_offsets";
pub const NEGATIVE_SPAN_LENGTHS_FIELD: &str = "negative_span_lengths";
pub const COUNT_U64_FIELD: &str = "count_u64";
pub const ZERO_COUNT_U64_FIELD: &str = "zero_count_u64";
pub const POSITIVE_BUCKETS_I64_FIELD: &str = "positive_buckets_i64";
pub const NEGATIVE_BUCKETS_I64_FIELD: &str = "negative_buckets_i64";
pub const COUNT_F64_FIELD: &str = "count_f64";
pub const ZERO_COUNT_F64_FIELD: &str = "zero_count_f64";
pub const POSITIVE_BUCKETS_F64_FIELD: &str = "positive_buckets_f64";
pub const NEGATIVE_BUCKETS_F64_FIELD: &str = "negative_buckets_f64";

// Keep int and float payloads in separate columns. The populated family is the
// type signal, so we don't need to persist an extra histogram-type tag.
pub const NATIVE_HISTOGRAM_FIELD_NAMES: &[&str] = &[
    SCHEMA_FIELD,
    ZERO_THRESHOLD_FIELD,
    SUM_FIELD,
    RESET_HINT_FIELD,
    START_TIMESTAMP_FIELD,
    CUSTOM_VALUES_FIELD,
    POSITIVE_SPAN_OFFSETS_FIELD,
    POSITIVE_SPAN_LENGTHS_FIELD,
    NEGATIVE_SPAN_OFFSETS_FIELD,
    NEGATIVE_SPAN_LENGTHS_FIELD,
    COUNT_U64_FIELD,
    ZERO_COUNT_U64_FIELD,
    POSITIVE_BUCKETS_I64_FIELD,
    NEGATIVE_BUCKETS_I64_FIELD,
    COUNT_F64_FIELD,
    ZERO_COUNT_F64_FIELD,
    POSITIVE_BUCKETS_F64_FIELD,
    NEGATIVE_BUCKETS_F64_FIELD,
];

static NATIVE_HISTOGRAM_VALUE_TYPE: Lazy<ConcreteDataType> = Lazy::new(|| {
    let fields = NATIVE_HISTOGRAM_FIELD_NAMES
        .iter()
        .filter_map(|name| {
            let data_type = native_histogram_field_type(name)?;
            Some(StructField::new((*name).to_string(), data_type, true))
        })
        .collect();
    ConcreteDataType::struct_datatype(StructType::new(Arc::new(fields)))
});

/// Returns the exact Greptime type for a persisted native histogram field.
pub fn native_histogram_field_type(name: &str) -> Option<ConcreteDataType> {
    match name {
        SCHEMA_FIELD | RESET_HINT_FIELD => Some(ConcreteDataType::int32_datatype()),
        ZERO_THRESHOLD_FIELD | SUM_FIELD | COUNT_F64_FIELD | ZERO_COUNT_F64_FIELD => {
            Some(ConcreteDataType::float64_datatype())
        }
        START_TIMESTAMP_FIELD => Some(ConcreteDataType::timestamp_millisecond_datatype()),
        CUSTOM_VALUES_FIELD | POSITIVE_BUCKETS_F64_FIELD | NEGATIVE_BUCKETS_F64_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::float64_datatype())),
        ),
        POSITIVE_SPAN_OFFSETS_FIELD | NEGATIVE_SPAN_OFFSETS_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype())),
        ),
        POSITIVE_SPAN_LENGTHS_FIELD | NEGATIVE_SPAN_LENGTHS_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::uint32_datatype())),
        ),
        COUNT_U64_FIELD | ZERO_COUNT_U64_FIELD => Some(ConcreteDataType::uint64_datatype()),
        POSITIVE_BUCKETS_I64_FIELD | NEGATIVE_BUCKETS_I64_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int64_datatype())),
        ),
        _ => None,
    }
}

pub fn native_histogram_value_type() -> &'static ConcreteDataType {
    &NATIVE_HISTOGRAM_VALUE_TYPE
}

pub fn is_native_histogram_value_schema(name: &str, data_type: &ConcreteDataType) -> bool {
    name == NATIVE_HISTOGRAM_FIELD && data_type == native_histogram_value_type()
}

// ---------------------------------------------------------------------------
// Stable Parquet field ids for native-histogram sub-fields.
//
// External readers resolve nested struct fields by `PARQUET:field_id`, so each
// sub-field (and list element) needs a stable positive id. The struct schema
// is fixed (always the same 18 fields). Each histogram column owns a block of
// ids (offset from a reserved base by the column's id), so several histogram
// columns in one table get disjoint sub-field ids. The reserved base is
// disjoint from user column ids and mito2 internal ids (`1 << 30`).
//
// The id is computed with checked arithmetic: the reserved base plus
// `column_id * stride` cannot always fit in a positive `i32` (a `ColumnId` is
// `u32`), so derivation returns `None` once the representable range is
// exceeded. Callers must handle `None` explicitly — the SST parquet writer
// surfaces it as an error rather than wrapping, panicking, or silently
// dropping the field id.
// ---------------------------------------------------------------------------

/// Reserved base for native-histogram struct sub-field ids.
pub const NATIVE_HISTOGRAM_SUBFIELD_ID_BASE: i32 = 0x5000_0000;

/// Number of ids reserved per histogram column (18 sub-fields + headroom for
/// list element ids), so multiple histogram columns get disjoint ids.
pub const NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE: i32 = 64;

/// Offset of list element ids within a column's id block.
pub const NATIVE_HISTOGRAM_LIST_ELEMENT_OFFSET: i32 = 32;

/// Returns the stable field id for a native-histogram struct sub-field,
/// namespaced by its parent `column_id`, or `None` if `name` is not a known
/// sub-field or the derived id overflows a positive `i32`.
pub fn native_histogram_subfield_id(column_id: i32, name: &str) -> Option<i32> {
    let idx = subfield_index(name)?;
    NATIVE_HISTOGRAM_SUBFIELD_ID_BASE
        .checked_add(column_id.checked_mul(NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE)?)
        .and_then(|v| v.checked_add(idx))
}

/// Returns the stable list `element-id` for a list-typed native-histogram
/// sub-field, namespaced by its parent `column_id`, or `None` if `name` is not
/// a known sub-field or the derived id overflows a positive `i32`.
pub fn native_histogram_list_element_id(column_id: i32, name: &str) -> Option<i32> {
    let idx = subfield_index(name)?;
    NATIVE_HISTOGRAM_SUBFIELD_ID_BASE
        .checked_add(column_id.checked_mul(NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE)?)
        .and_then(|v| v.checked_add(NATIVE_HISTOGRAM_LIST_ELEMENT_OFFSET))
        .and_then(|v| v.checked_add(idx))
}

fn subfield_index(name: &str) -> Option<i32> {
    NATIVE_HISTOGRAM_FIELD_NAMES
        .iter()
        .position(|n| *n == name)
        .map(|i| i as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subfield_ids_are_namespaced_and_disjoint() {
        // SCHEMA=0, ZERO_THRESHOLD=1, SUM=2, ..., CUSTOM_VALUES=5.
        let sum_idx = 2;
        let custom_values_idx = 5;

        // Ids are offset from the reserved base by the parent column id and
        // the sub-field index.
        assert_eq!(
            native_histogram_subfield_id(1, SUM_FIELD),
            Some(NATIVE_HISTOGRAM_SUBFIELD_ID_BASE + NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE + sum_idx,)
        );
        // List element ids additionally carry the list offset.
        assert_eq!(
            native_histogram_list_element_id(1, CUSTOM_VALUES_FIELD),
            Some(
                NATIVE_HISTOGRAM_SUBFIELD_ID_BASE
                    + NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE
                    + NATIVE_HISTOGRAM_LIST_ELEMENT_OFFSET
                    + custom_values_idx,
            )
        );
        // Different parent columns get disjoint ids for the same sub-field.
        assert_ne!(
            native_histogram_subfield_id(1, SUM_FIELD),
            native_histogram_subfield_id(7, SUM_FIELD)
        );
        // Unknown sub-field name -> None.
        assert_eq!(native_histogram_subfield_id(1, "not_a_field"), None);
    }

    #[test]
    fn subfield_ids_overflow_returns_none() {
        // `column_id` is u32-sized, but the derived id must fit in a positive
        // i32. At column_id = 12_582_912, BASE + column_id*64 == i32::MAX + 1,
        // which previously overflowed (debug panic / release wrap). Checked
        // arithmetic must yield None instead of wrapping or panicking.
        assert_eq!(native_histogram_subfield_id(12_582_912, SUM_FIELD), None);
        assert_eq!(
            native_histogram_list_element_id(12_582_912, CUSTOM_VALUES_FIELD),
            None
        );
        // One below that boundary is still representable.
        assert!(native_histogram_subfield_id(12_582_911, SUM_FIELD).is_some());
    }
}
