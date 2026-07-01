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
//! Prom remote-write v2 builds rows with these names, while metric-engine and
//! operator code use the same contract to recognize native histogram tables.

use std::collections::HashSet;
use std::sync::Arc;

use datatypes::data_type::ConcreteDataType;

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

pub fn is_native_histogram_field_schema(name: &str, data_type: &ConcreteDataType) -> bool {
    native_histogram_field_type(name).is_some_and(|expected| expected == *data_type)
}

/// Checks for the complete native histogram field set with no extras or duplicates.
pub fn is_native_histogram_field_set<'a>(
    fields: impl IntoIterator<Item = (&'a str, &'a ConcreteDataType)>,
) -> bool {
    let mut seen = HashSet::with_capacity(NATIVE_HISTOGRAM_FIELD_NAMES.len());
    for (name, data_type) in fields {
        if !is_native_histogram_field_schema(name, data_type) || !seen.insert(name) {
            return false;
        }
    }

    seen.len() == NATIVE_HISTOGRAM_FIELD_NAMES.len()
}
