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

use std::collections::{BTreeMap, BTreeSet};

use common_telemetry::warn;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::InList;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use index::bloom_filter::applier::InListPredicate;
use index::Bytes;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;

use crate::cache::file_cache::FileCacheRef;
use crate::cache::index::bloom_filter_index::BloomFilterIndexCacheRef;
use crate::error::{ColumnNotFoundSnafu, ConvertValueSnafu, Result};
use crate::row_converter::SortField;
use crate::sst::index::bloom_filter::applier::BloomFilterIndexApplier;
use crate::sst::index::codec::IndexValueCodec;
use crate::sst::index::puffin_manager::PuffinManagerFactory;

pub struct BloomFilterIndexApplierBuilder<'a> {
    region_dir: String,
    object_store: ObjectStore,
    metadata: &'a RegionMetadata,
    puffin_manager_factory: PuffinManagerFactory,
    file_cache: Option<FileCacheRef>,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    predicates: BTreeMap<ColumnId, Vec<InListPredicate>>,
}

impl<'a> BloomFilterIndexApplierBuilder<'a> {
    pub fn new(
        region_dir: String,
        object_store: ObjectStore,
        metadata: &'a RegionMetadata,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        Self {
            region_dir,
            object_store,
            metadata,
            puffin_manager_factory,
            file_cache: None,
            puffin_metadata_cache: None,
            bloom_filter_index_cache: None,
            predicates: BTreeMap::default(),
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_bloom_filter_index_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }

    /// Builds the applier with given filter expressions
    pub fn build(mut self, exprs: &[Expr]) -> Result<Option<BloomFilterIndexApplier>> {
        for expr in exprs {
            self.traverse_and_collect(expr);
        }

        if self.predicates.is_empty() {
            return Ok(None);
        }

        let applier = BloomFilterIndexApplier::new(
            self.region_dir,
            self.metadata.region_id,
            self.object_store,
            self.puffin_manager_factory,
            self.predicates,
        )
        .with_file_cache(self.file_cache)
        .with_puffin_metadata_cache(self.puffin_metadata_cache)
        .with_bloom_filter_cache(self.bloom_filter_index_cache);

        Ok(Some(applier))
    }

    /// Recursively traverses expressions to collect bloom filter predicates
    fn traverse_and_collect(&mut self, expr: &Expr) {
        let res = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    self.traverse_and_collect(left);
                    self.traverse_and_collect(right);
                    Ok(())
                }
                Operator::Eq => self.collect_eq(left, right),
                _ => Ok(()),
            },
            Expr::InList(in_list) => self.collect_in_list(in_list),
            _ => Ok(()),
        };

        if let Err(err) = res {
            warn!(err; "Failed to collect bloom filter predicates, ignore it. expr: {expr}");
        }
    }

    /// Helper function to get the column id and type
    fn column_id_and_type(
        &self,
        column_name: &str,
    ) -> Result<Option<(ColumnId, ConcreteDataType)>> {
        let column = self
            .metadata
            .column_by_name(column_name)
            .context(ColumnNotFoundSnafu {
                column: column_name,
            })?;

        Ok(Some((
            column.column_id,
            column.column_schema.data_type.clone(),
        )))
    }

    /// Collects an equality expression (column = value)
    fn collect_eq(&mut self, left: &Expr, right: &Expr) -> Result<()> {
        let (col, lit) = match (left, right) {
            (Expr::Column(col), Expr::Literal(lit)) => (col, lit),
            (Expr::Literal(lit), Expr::Column(col)) => (col, lit),
            _ => return Ok(()),
        };
        if lit.is_null() {
            return Ok(());
        }
        let Some((column_id, data_type)) = self.column_id_and_type(&col.name)? else {
            return Ok(());
        };
        let value = encode_lit(lit, data_type)?;
        self.predicates
            .entry(column_id)
            .or_default()
            .push(InListPredicate {
                list: BTreeSet::from([value]),
            });

        Ok(())
    }

    /// Collects an in list expression in the form of `column IN (lit, lit, ...)`.
    fn collect_in_list(&mut self, in_list: &InList) -> Result<()> {
        // Only collect InList predicates if they reference a column
        let Expr::Column(column) = &in_list.expr.as_ref() else {
            return Ok(());
        };
        if in_list.list.is_empty() || in_list.negated {
            return Ok(());
        }

        let Some((column_id, data_type)) = self.column_id_and_type(&column.name)? else {
            return Ok(());
        };

        // Convert all non-null literals to predicates
        let predicates = in_list
            .list
            .iter()
            .filter_map(Self::nonnull_lit)
            .map(|lit| encode_lit(lit, data_type.clone()));

        // Collect successful conversions
        let mut valid_predicates = BTreeSet::new();
        for predicate in predicates {
            match predicate {
                Ok(p) => {
                    valid_predicates.insert(p);
                }
                Err(e) => warn!(e; "Failed to convert value in InList"),
            }
        }

        if !valid_predicates.is_empty() {
            self.predicates
                .entry(column_id)
                .or_default()
                .push(InListPredicate {
                    list: valid_predicates,
                });
        }

        Ok(())
    }

    /// Helper function to get non-null literal value
    fn nonnull_lit(expr: &Expr) -> Option<&ScalarValue> {
        match expr {
            Expr::Literal(lit) if !lit.is_null() => Some(lit),
            _ => None,
        }
    }
}

// TODO(ruihang): extract this and the one under inverted_index into a common util mod.
/// Helper function to encode a literal into bytes.
fn encode_lit(lit: &ScalarValue, data_type: ConcreteDataType) -> Result<Bytes> {
    let value = Value::try_from(lit.clone()).context(ConvertValueSnafu)?;
    let mut bytes = vec![];
    let field = SortField::new(data_type);
    IndexValueCodec::encode_nonnull_value(value.as_value_ref(), &field, &mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion_common::Column;
    use datatypes::schema::ColumnSchema;
    use object_store::services::Memory;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    fn test_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "column1",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "column2",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "column3",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    fn test_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn column(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn string_lit(s: impl Into<String>) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.into())))
    }

    #[test]
    fn test_build_with_exprs() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_exprs_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );
        let exprs = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(column("column1")),
            op: Operator::Eq,
            right: Box::new(string_lit("value1")),
        })];
        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let predicates = result.unwrap().predicates;
        assert_eq!(predicates.len(), 1);

        let column_predicates = predicates.get(&1).unwrap();
        assert_eq!(column_predicates.len(), 1);

        let expected = encode_lit(
            &ScalarValue::Utf8(Some("value1".to_string())),
            ConcreteDataType::string_datatype(),
        )
        .unwrap();
        assert_eq!(column_predicates[0].list, BTreeSet::from([expected]));
    }

    fn int64_lit(i: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(i)))
    }

    #[test]
    fn test_build_with_in_list() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_in_list_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![Expr::InList(InList {
            expr: Box::new(column("column2")),
            list: vec![int64_lit(1), int64_lit(2), int64_lit(3)],
            negated: false,
        })];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let predicates = result.unwrap().predicates;
        let column_predicates = predicates.get(&2).unwrap();
        assert_eq!(column_predicates.len(), 1);
        assert_eq!(column_predicates[0].list.len(), 3);
    }

    #[test]
    fn test_build_with_and_expressions() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_and_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );
        let exprs = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Eq,
                right: Box::new(string_lit("value1")),
            })),
            op: Operator::And,
            right: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column2")),
                op: Operator::Eq,
                right: Box::new(int64_lit(42)),
            })),
        })];
        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let predicates = result.unwrap().predicates;
        assert_eq!(predicates.len(), 2);
        assert!(predicates.contains_key(&1));
        assert!(predicates.contains_key(&2));
    }

    #[test]
    fn test_build_with_null_values() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_null_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Utf8(None))),
            }),
            Expr::InList(InList {
                expr: Box::new(column("column2")),
                list: vec![
                    int64_lit(1),
                    Expr::Literal(ScalarValue::Int64(None)),
                    int64_lit(3),
                ],
                negated: false,
            }),
        ];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let predicates = result.unwrap().predicates;
        assert!(!predicates.contains_key(&1)); // Null equality should be ignored
        let column2_predicates = predicates.get(&2).unwrap();
        assert_eq!(column2_predicates[0].list.len(), 2);
    }

    #[test]
    fn test_build_with_invalid_expressions() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_invalid_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );
        let exprs = vec![
            // Non-equality operator
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Gt,
                right: Box::new(string_lit("value1")),
            }),
            // Non-existent column
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("non_existent")),
                op: Operator::Eq,
                right: Box::new(string_lit("value")),
            }),
            // Negated IN list
            Expr::InList(InList {
                expr: Box::new(column("column2")),
                list: vec![int64_lit(1), int64_lit(2)],
                negated: true,
            }),
        ];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_build_with_multiple_predicates_same_column() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_multiple_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );
        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Eq,
                right: Box::new(string_lit("value1")),
            }),
            Expr::InList(InList {
                expr: Box::new(column("column1")),
                list: vec![string_lit("value2"), string_lit("value3")],
                negated: false,
            }),
        ];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let predicates = result.unwrap().predicates;
        let column_predicates = predicates.get(&1).unwrap();
        assert_eq!(column_predicates.len(), 2);
    }
}
