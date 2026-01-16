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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use memcomparable::Serializer;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyFilter, SortField, SparsePrimaryKeyCodec,
};

/// Returns true if this is a partition column for metrics in the memtable.
pub fn is_partition_column(name: &str) -> bool {
    name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
}

#[derive(Clone)]
struct PrimaryKeyFilterInner {
    filters: Arc<Vec<SimpleFilterEvaluator>>,
    compiled_filters: Vec<CompiledPrimaryKeyFilter>,
}

impl PrimaryKeyFilterInner {
    fn new(metadata: RegionMetadataRef, filters: Arc<Vec<SimpleFilterEvaluator>>) -> Self {
        let compiled_filters = Self::compile_filters(&metadata, &filters);
        Self {
            filters,
            compiled_filters,
        }
    }

    fn compile_filters(
        metadata: &RegionMetadataRef,
        filters: &[SimpleFilterEvaluator],
    ) -> Vec<CompiledPrimaryKeyFilter> {
        if filters.is_empty() || metadata.primary_key.is_empty() {
            return Vec::new();
        }

        let mut compiled_filters = Vec::with_capacity(filters.len());
        for (filter_idx, filter) in filters.iter().enumerate() {
            if is_partition_column(filter.column_name()) {
                continue;
            }

            let Some(column) = metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            // Ignore filters that are not referencing primary key tag columns.
            if column.semantic_type != SemanticType::Tag {
                continue;
            }

            // Safety: A tag column is always in primary key.
            let Some(pk_index) = metadata.primary_key_index(column.column_id) else {
                continue;
            };

            let data_type = column.column_schema.data_type.clone();
            let fast_path = CompiledFastPath::try_new(filter, &data_type);

            compiled_filters.push(CompiledPrimaryKeyFilter {
                filter_idx,
                column_id: column.column_id,
                pk_index,
                data_type,
                fast_path,
            });
        }

        compiled_filters
    }

    fn evaluate_filters<'a>(&self, accessor: &mut impl PrimaryKeyValueAccessor<'a>) -> bool {
        if self.compiled_filters.is_empty() {
            return true;
        }

        for compiled in &self.compiled_filters {
            let filter = &self.filters[compiled.filter_idx];

            let passed = if let Some(fast_path) = &compiled.fast_path {
                let encoded_value = match accessor.encoded_value(compiled) {
                    Ok(v) => v,
                    Err(e) => {
                        common_telemetry::error!(e; "Failed to decode primary key");
                        return true;
                    }
                };
                fast_path.matches(encoded_value)
            } else {
                let value = match accessor.decode_value(compiled) {
                    Ok(v) => v,
                    Err(e) => {
                        common_telemetry::error!(e; "Failed to decode primary key");
                        return true;
                    }
                };

                // Safety: arrow schema and datatypes are constructed from the same source.
                let scalar_value = value.try_to_scalar_value(&compiled.data_type).unwrap();
                filter.evaluate_scalar(&scalar_value).unwrap_or(true)
            };

            if !passed {
                return false;
            }
        }

        true
    }
}

#[derive(Clone)]
struct CompiledPrimaryKeyFilter {
    filter_idx: usize,
    column_id: ColumnId,
    pk_index: usize,
    data_type: ConcreteDataType,
    fast_path: Option<CompiledFastPath>,
}

#[derive(Clone)]
enum CompiledFastPath {
    Eq(Vec<u8>),
    NotEq(Vec<u8>),
    Lt(Vec<u8>),
    LtEq(Vec<u8>),
    Gt(Vec<u8>),
    GtEq(Vec<u8>),
    InList(Vec<Vec<u8>>),
}

impl CompiledFastPath {
    fn try_new(filter: &SimpleFilterEvaluator, data_type: &ConcreteDataType) -> Option<Self> {
        let field = SortField::new(data_type.clone());
        // Safety guard: memcomparable defines a total order for floats that doesn't necessarily
        // match DataFusion/Arrow float comparison semantics (e.g. `-0.0 == 0.0`).
        if field.encode_data_type().is_float() {
            return None;
        }

        let encoded = |value: &Value| -> Option<Vec<u8>> {
            let mut buf = Vec::new();
            let mut serializer = Serializer::new(&mut buf);
            field
                .serialize(&mut serializer, &value.as_value_ref())
                .ok()?;
            Some(buf)
        };

        if filter.is_eq() {
            let value = filter.literal_value()?;
            return Some(Self::Eq(encoded(&value)?));
        }

        if filter.is_not_eq() {
            let value = filter.literal_value()?;
            return Some(Self::NotEq(encoded(&value)?));
        }

        if filter.is_lt() {
            let value = filter.literal_value()?;
            return Some(Self::Lt(encoded(&value)?));
        }

        if filter.is_lt_eq() {
            let value = filter.literal_value()?;
            return Some(Self::LtEq(encoded(&value)?));
        }

        if filter.is_gt() {
            let value = filter.literal_value()?;
            return Some(Self::Gt(encoded(&value)?));
        }

        if filter.is_gt_eq() {
            let value = filter.literal_value()?;
            return Some(Self::GtEq(encoded(&value)?));
        }

        if filter.is_or_eq_chain() {
            let values = filter.literal_list_values()?;
            let mut list = Vec::with_capacity(values.len());
            for value in values {
                let bytes = encoded(&value)?;
                // `NULL` never matches in equality comparisons (`NULL = x` is NULL).
                if bytes.first() == Some(&0) {
                    continue;
                }
                list.push(bytes);
            }
            return Some(Self::InList(list));
        }

        None
    }

    fn matches(&self, encoded_value: Option<&[u8]>) -> bool {
        let Some(encoded_value) = encoded_value else {
            return false;
        };

        // `NULL` never matches in comparisons (`NULL = x` and `NULL != x` are NULL).
        if encoded_value.first() == Some(&0) {
            return false;
        }

        match self {
            CompiledFastPath::Eq(encoded_literal) => {
                encoded_literal.first() != Some(&0) && encoded_value == &encoded_literal[..]
            }
            CompiledFastPath::NotEq(encoded_literal) => {
                encoded_literal.first() != Some(&0) && encoded_value != &encoded_literal[..]
            }
            CompiledFastPath::Lt(encoded_literal) => {
                encoded_literal.first() != Some(&0) && encoded_value < &encoded_literal[..]
            }
            CompiledFastPath::LtEq(encoded_literal) => {
                encoded_literal.first() != Some(&0) && encoded_value <= &encoded_literal[..]
            }
            CompiledFastPath::Gt(encoded_literal) => {
                encoded_literal.first() != Some(&0) && encoded_value > &encoded_literal[..]
            }
            CompiledFastPath::GtEq(encoded_literal) => {
                encoded_literal.first() != Some(&0) && encoded_value >= &encoded_literal[..]
            }
            CompiledFastPath::InList(encoded_literals) => {
                encoded_literals.iter().any(|lit| encoded_value == &lit[..])
            }
        }
    }
}

trait PrimaryKeyValueAccessor<'a> {
    fn encoded_value(&mut self, filter: &CompiledPrimaryKeyFilter) -> Result<Option<&'a [u8]>>;
    fn decode_value(&mut self, filter: &CompiledPrimaryKeyFilter) -> Result<Value>;
}

/// Dense primary key filter.
#[derive(Clone)]
pub struct DensePrimaryKeyFilter {
    inner: PrimaryKeyFilterInner,
    codec: DensePrimaryKeyCodec,
    offsets_buf: Vec<usize>,
}

impl DensePrimaryKeyFilter {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
        codec: DensePrimaryKeyCodec,
    ) -> Self {
        Self {
            inner: PrimaryKeyFilterInner::new(metadata, filters),
            codec,
            offsets_buf: Vec::new(),
        }
    }
}

impl PrimaryKeyFilter for DensePrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> bool {
        self.offsets_buf.clear();
        let mut accessor = DensePrimaryKeyValueAccessor {
            pk,
            codec: &self.codec,
            offsets_buf: &mut self.offsets_buf,
        };
        self.inner.evaluate_filters(&mut accessor)
    }
}

struct DensePrimaryKeyValueAccessor<'a, 'b> {
    pk: &'a [u8],
    codec: &'b DensePrimaryKeyCodec,
    offsets_buf: &'b mut Vec<usize>,
}

impl<'a> PrimaryKeyValueAccessor<'a> for DensePrimaryKeyValueAccessor<'a, '_> {
    fn encoded_value(&mut self, filter: &CompiledPrimaryKeyFilter) -> Result<Option<&'a [u8]>> {
        self.codec
            .encoded_value_at(self.pk, filter.pk_index, self.offsets_buf)
            .map(Some)
    }

    fn decode_value(&mut self, filter: &CompiledPrimaryKeyFilter) -> Result<Value> {
        self.codec
            .decode_value_at(self.pk, filter.pk_index, self.offsets_buf)
    }
}

/// Sparse primary key filter.
#[derive(Clone)]
pub struct SparsePrimaryKeyFilter {
    inner: PrimaryKeyFilterInner,
    codec: SparsePrimaryKeyCodec,
    offsets_map: HashMap<ColumnId, usize>,
}

impl SparsePrimaryKeyFilter {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
        codec: SparsePrimaryKeyCodec,
    ) -> Self {
        Self {
            inner: PrimaryKeyFilterInner::new(metadata, filters),
            codec,
            offsets_map: HashMap::new(),
        }
    }
}

impl PrimaryKeyFilter for SparsePrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> bool {
        self.offsets_map.clear();
        let mut accessor = SparsePrimaryKeyValueAccessor {
            pk,
            codec: &self.codec,
            offsets_map: &mut self.offsets_map,
        };
        self.inner.evaluate_filters(&mut accessor)
    }
}

struct SparsePrimaryKeyValueAccessor<'a, 'b> {
    pk: &'a [u8],
    codec: &'b SparsePrimaryKeyCodec,
    offsets_map: &'b mut HashMap<ColumnId, usize>,
}

impl<'a> PrimaryKeyValueAccessor<'a> for SparsePrimaryKeyValueAccessor<'a, '_> {
    fn encoded_value(&mut self, filter: &CompiledPrimaryKeyFilter) -> Result<Option<&'a [u8]>> {
        self.codec
            .encoded_value_for_column(self.pk, self.offsets_map, filter.column_id)
    }

    fn decode_value(&mut self, filter: &CompiledPrimaryKeyFilter) -> Result<Value> {
        if let Some(offset) = self
            .codec
            .has_column(self.pk, self.offsets_map, filter.column_id)
        {
            self.codec
                .decode_value_at(self.pk, offset, filter.column_id)
        } else {
            Ok(Value::Null)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use datafusion_common::Column;
    use datafusion_expr::{BinaryExpr, Expr, Literal, Operator};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::{OrderedFloat, ValueRef};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{ColumnId, RegionId};

    use super::*;
    use crate::row_converter::PrimaryKeyCodecExt;

    fn setup_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("pod", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "namespace",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "container",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    greptime_value(),
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    greptime_timestamp(),
                    ConcreteDataType::timestamp_nanosecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 5,
            })
            .primary_key(vec![1, 2, 3]);
        let metadata = builder.build().unwrap();
        Arc::new(metadata)
    }

    fn create_test_row() -> Vec<(ColumnId, ValueRef<'static>)> {
        vec![
            (1, ValueRef::String("greptime-frontend-6989d9899-22222")),
            (2, ValueRef::String("greptime-cluster")),
            (3, ValueRef::String("greptime-frontend-6989d9899-22222")),
        ]
    }

    fn create_filter(column_name: &str, value: &str) -> SimpleFilterEvaluator {
        create_filter_with_op(column_name, Operator::Eq, value)
    }

    fn create_filter_with_op<T: Literal>(
        column_name: &str,
        op: Operator,
        value: T,
    ) -> SimpleFilterEvaluator {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(column_name))),
            op,
            right: Box::new(value.lit()),
        });
        SimpleFilterEvaluator::try_new(&expr).unwrap()
    }

    fn encode_sparse_pk(
        metadata: &RegionMetadataRef,
        row: Vec<(ColumnId, ValueRef<'static>)>,
    ) -> Vec<u8> {
        let codec = SparsePrimaryKeyCodec::new(metadata);
        let mut pk = Vec::new();
        codec.encode_to_vec(row.into_iter(), &mut pk).unwrap();
        pk
    }

    fn encode_dense_pk(
        metadata: &RegionMetadataRef,
        row: Vec<(ColumnId, ValueRef<'static>)>,
    ) -> Vec<u8> {
        let codec = DensePrimaryKeyCodec::new(metadata);
        let mut pk = Vec::new();
        codec
            .encode_to_vec(row.into_iter().map(|(_, v)| v), &mut pk)
            .unwrap();
        pk
    }

    #[test]
    fn test_sparse_primary_key_filter_matches() {
        let metadata = setup_metadata();
        let filters = Arc::new(vec![create_filter(
            "pod",
            "greptime-frontend-6989d9899-22222",
        )]);
        let pk = encode_sparse_pk(&metadata, create_test_row());
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut filter = SparsePrimaryKeyFilter::new(metadata, filters, codec);
        assert!(filter.matches(&pk));
    }

    #[test]
    fn test_sparse_primary_key_filter_not_matches() {
        let metadata = setup_metadata();
        let filters = Arc::new(vec![create_filter(
            "pod",
            "greptime-frontend-6989d9899-22223",
        )]);
        let pk = encode_sparse_pk(&metadata, create_test_row());
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut filter = SparsePrimaryKeyFilter::new(metadata, filters, codec);
        assert!(!filter.matches(&pk));
    }

    #[test]
    fn test_sparse_primary_key_filter_matches_with_null() {
        let metadata = setup_metadata();
        let filters = Arc::new(vec![create_filter(
            "non-exist-label",
            "greptime-frontend-6989d9899-22222",
        )]);
        let pk = encode_sparse_pk(&metadata, create_test_row());
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut filter = SparsePrimaryKeyFilter::new(metadata, filters, codec);
        assert!(filter.matches(&pk));
    }

    #[test]
    fn test_dense_primary_key_filter_matches() {
        let metadata = setup_metadata();
        let filters = Arc::new(vec![create_filter(
            "pod",
            "greptime-frontend-6989d9899-22222",
        )]);
        let pk = encode_dense_pk(&metadata, create_test_row());
        let codec = DensePrimaryKeyCodec::new(&metadata);
        let mut filter = DensePrimaryKeyFilter::new(metadata, filters, codec);
        assert!(filter.matches(&pk));
    }

    #[test]
    fn test_dense_primary_key_filter_not_matches() {
        let metadata = setup_metadata();
        let filters = Arc::new(vec![create_filter(
            "pod",
            "greptime-frontend-6989d9899-22223",
        )]);
        let pk = encode_dense_pk(&metadata, create_test_row());
        let codec = DensePrimaryKeyCodec::new(&metadata);
        let mut filter = DensePrimaryKeyFilter::new(metadata, filters, codec);
        assert!(!filter.matches(&pk));
    }

    #[test]
    fn test_dense_primary_key_filter_matches_with_null() {
        let metadata = setup_metadata();
        let filters = Arc::new(vec![create_filter(
            "non-exist-label",
            "greptime-frontend-6989d9899-22222",
        )]);
        let pk = encode_dense_pk(&metadata, create_test_row());
        let codec = DensePrimaryKeyCodec::new(&metadata);
        let mut filter = DensePrimaryKeyFilter::new(metadata, filters, codec);
        assert!(filter.matches(&pk));
    }

    #[test]
    fn test_dense_primary_key_filter_order_ops() {
        let metadata = setup_metadata();
        let pk = encode_dense_pk(&metadata, create_test_row());
        let codec = DensePrimaryKeyCodec::new(&metadata);

        let cases = [
            (Operator::Gt, "greptime-frontend-6989d9899-22221", true),
            (Operator::GtEq, "greptime-frontend-6989d9899-22222", true),
            (Operator::Lt, "greptime-frontend-6989d9899-22223", true),
            (Operator::LtEq, "greptime-frontend-6989d9899-22221", false),
        ];

        for (op, value, expected) in cases {
            let filters = Arc::new(vec![create_filter_with_op("pod", op, value)]);
            let mut filter = DensePrimaryKeyFilter::new(metadata.clone(), filters, codec.clone());
            assert_eq!(expected, filter.matches(&pk));
        }
    }

    #[test]
    fn test_sparse_primary_key_filter_order_ops() {
        let metadata = setup_metadata();
        let pk = encode_sparse_pk(&metadata, create_test_row());
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        let cases = [
            (Operator::Gt, "greptime-frontend-6989d9899-22221", true),
            (Operator::GtEq, "greptime-frontend-6989d9899-22222", true),
            (Operator::Lt, "greptime-frontend-6989d9899-22223", true),
            (Operator::LtEq, "greptime-frontend-6989d9899-22221", false),
        ];

        for (op, value, expected) in cases {
            let filters = Arc::new(vec![create_filter_with_op("pod", op, value)]);
            let mut filter = SparsePrimaryKeyFilter::new(metadata.clone(), filters, codec.clone());
            assert_eq!(expected, filter.matches(&pk));
        }
    }

    #[test]
    fn test_dense_primary_key_filter_float_eq_fallback() {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("f", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    greptime_timestamp(),
                    ConcreteDataType::timestamp_nanosecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());

        let codec = DensePrimaryKeyCodec::new(&metadata);
        let mut pk = Vec::new();
        codec
            .encode_to_vec([ValueRef::Float64(OrderedFloat(-0.0))].into_iter(), &mut pk)
            .unwrap();

        let filters = Arc::new(vec![create_filter_with_op("f", Operator::Eq, 0.0_f64)]);
        let mut filter = DensePrimaryKeyFilter::new(metadata, filters, codec);

        assert!(filter.matches(&pk));
    }
}
