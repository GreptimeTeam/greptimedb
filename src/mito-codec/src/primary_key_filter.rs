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
use datatypes::value::Value;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyFilter, SparsePrimaryKeyCodec};

/// Returns true if this is a partition column for metrics in the memtable.
pub fn is_partition_column(name: &str) -> bool {
    name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
}

#[derive(Clone)]
struct PrimaryKeyFilterInner {
    metadata: RegionMetadataRef,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
}

impl PrimaryKeyFilterInner {
    fn evaluate_filters(
        &self,
        mut decode_fn: impl FnMut(ColumnId, &RegionMetadataRef) -> Result<Value>,
    ) -> bool {
        if self.filters.is_empty() || self.metadata.primary_key.is_empty() {
            return true;
        }

        let mut result = true;
        for filter in self.filters.iter() {
            if is_partition_column(filter.column_name()) {
                continue;
            }
            let Some(column) = self.metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            // ignore filters that are not referencing primary key columns
            if column.semantic_type != SemanticType::Tag {
                continue;
            }

            let value = match decode_fn(column.column_id, &self.metadata) {
                Ok(v) => v,
                Err(e) => {
                    common_telemetry::error!(e; "Failed to decode primary key");
                    return true;
                }
            };

            // TODO(yingwen): `evaluate_scalar()` creates temporary arrays to compare scalars. We
            // can compare the bytes directly without allocation and matching types as we use
            // comparable encoding.
            // Safety: arrow schema and datatypes are constructed from the same source.
            let scalar_value = value
                .try_to_scalar_value(&column.column_schema.data_type)
                .unwrap();
            result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
        }

        result
    }
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
            inner: PrimaryKeyFilterInner { metadata, filters },
            codec,
            offsets_buf: Vec::new(),
        }
    }
}

impl PrimaryKeyFilter for DensePrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> bool {
        self.offsets_buf.clear();
        self.inner.evaluate_filters(|column_id, metadata| {
            // index of tag column in primary key
            // Safety: A tag column is always in primary key.
            let index = metadata.primary_key_index(column_id).unwrap();
            self.codec.decode_value_at(pk, index, &mut self.offsets_buf)
        })
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
            inner: PrimaryKeyFilterInner { metadata, filters },
            codec,
            offsets_map: HashMap::new(),
        }
    }
}

impl PrimaryKeyFilter for SparsePrimaryKeyFilter {
    fn matches(&mut self, pk: &[u8]) -> bool {
        self.offsets_map.clear();
        self.inner.evaluate_filters(|column_id, _| {
            if let Some(offset) = self.codec.has_column(pk, &mut self.offsets_map, column_id) {
                self.codec.decode_value_at(pk, offset, column_id)
            } else {
                Ok(Value::Null)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datafusion_common::Column;
    use datafusion_expr::{BinaryExpr, Expr, Literal, Operator};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
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
                    "greptime_value",
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "greptime_timestamp",
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
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(column_name))),
            op: Operator::Eq,
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
}
