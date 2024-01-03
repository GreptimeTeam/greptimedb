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

mod between;
mod comparison;
mod eq_list;
mod in_list;
mod regex_match;

use std::collections::HashMap;

use api::v1::SemanticType;
use common_query::logical_plan::Expr;
use common_telemetry::warn;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr as DfExpr, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use index::inverted_index::search::index_apply::PredicatesIndexApplier;
use index::inverted_index::search::predicate::Predicate;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;

use crate::error::{BuildIndexApplierSnafu, ColumnNotFoundSnafu, ConvertValueSnafu, Result};
use crate::row_converter::SortField;
use crate::sst::index::applier::SstIndexApplier;
use crate::sst::index::codec::IndexValueCodec;

type ColumnName = String;

/// Constructs an [`SstIndexApplier`] which applies predicates to SST files during scan.
pub struct SstIndexApplierBuilder<'a> {
    /// Directory of the region, required argument for constructing [`SstIndexApplier`].
    region_dir: String,

    /// Object store, required argument for constructing [`SstIndexApplier`].
    object_store: ObjectStore,

    /// Metadata of the region, used to get metadata like column type.
    metadata: &'a RegionMetadata,

    /// Stores predicates during traversal on the Expr tree.
    output: HashMap<ColumnName, Vec<Predicate>>,
}

impl<'a> SstIndexApplierBuilder<'a> {
    /// Creates a new [`SstIndexApplierBuilder`].
    pub fn new(
        region_dir: String,
        object_store: ObjectStore,
        metadata: &'a RegionMetadata,
    ) -> Self {
        Self {
            region_dir,
            object_store,
            metadata,
            output: HashMap::default(),
        }
    }

    /// Consumes the builder to construct an [`SstIndexApplier`], optionally returned based on
    /// the expressions provided. If no predicates match, returns `None`.
    pub fn build(mut self, exprs: &[Expr]) -> Result<Option<SstIndexApplier>> {
        for expr in exprs {
            self.traverse_and_collect(expr.df_expr());
        }

        if self.output.is_empty() {
            return Ok(None);
        }

        let predicates = self.output.into_iter().collect();
        let applier = PredicatesIndexApplier::try_from(predicates);
        Ok(Some(SstIndexApplier::new(
            self.region_dir,
            self.object_store,
            Box::new(applier.context(BuildIndexApplierSnafu)?),
        )))
    }

    /// Recursively traverses expressions to collect predicates.
    /// Results are stored in `self.output`.
    fn traverse_and_collect(&mut self, expr: &DfExpr) {
        let res = match expr {
            DfExpr::Between(between) => self.collect_between(between),

            DfExpr::InList(in_list) => self.collect_inlist(in_list),
            DfExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    self.traverse_and_collect(left);
                    self.traverse_and_collect(right);
                    Ok(())
                }
                Operator::Or => self.collect_or_eq_list(left, right),
                Operator::Eq => self.collect_eq(left, right),
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    self.collect_comparison_expr(left, op, right)
                }
                Operator::RegexMatch => self.collect_regex_match(left, right),
                _ => Ok(()),
            },

            // TODO(zhongzc): support more expressions, e.g. IsNull, IsNotNull, ...
            _ => Ok(()),
        };

        if let Err(err) = res {
            warn!(err; "Failed to collect predicates, ignore it. expr: {expr}");
        }
    }

    /// Helper function to add a predicate to the output.
    fn add_predicate(&mut self, column_name: &str, predicate: Predicate) {
        match self.output.get_mut(column_name) {
            Some(predicates) => predicates.push(predicate),
            None => {
                self.output.insert(column_name.to_string(), vec![predicate]);
            }
        }
    }

    /// Helper function to get the column type of a tag column.
    /// Returns `None` if the column is not a tag column.
    fn tag_column_type(&self, column_name: &str) -> Result<Option<ConcreteDataType>> {
        let column = self
            .metadata
            .column_by_name(column_name)
            .context(ColumnNotFoundSnafu {
                column: column_name,
            })?;

        Ok((column.semantic_type == SemanticType::Tag)
            .then(|| column.column_schema.data_type.clone()))
    }

    /// Helper function to get a non-null literal.
    fn nonnull_lit(expr: &DfExpr) -> Option<&ScalarValue> {
        match expr {
            DfExpr::Literal(lit) if !lit.is_null() => Some(lit),
            _ => None,
        }
    }

    /// Helper function to get the column name of a column expression.
    fn column_name(expr: &DfExpr) -> Option<&str> {
        match expr {
            DfExpr::Column(column) => Some(&column.name),
            _ => None,
        }
    }

    /// Helper function to encode a literal into bytes.
    fn encode_lit(lit: &ScalarValue, data_type: ConcreteDataType) -> Result<Vec<u8>> {
        let value = Value::try_from(lit.clone()).context(ConvertValueSnafu)?;
        let mut bytes = vec![];
        let field = SortField::new(data_type);
        IndexValueCodec::encode_value(value.as_value_ref(), &field, &mut bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion_common::Column;
    use datafusion_expr::Between;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use index::inverted_index::search::predicate::{
        Bound, Range, RangePredicate, RegexMatchPredicate,
    };
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    pub(crate) fn test_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("c", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "d",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 4,
            })
            .primary_key(vec![1, 2]);
        builder.build().unwrap()
    }

    pub(crate) fn test_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    pub(crate) fn tag_column() -> DfExpr {
        DfExpr::Column(Column {
            relation: None,
            name: "a".to_string(),
        })
    }

    pub(crate) fn tag_column2() -> DfExpr {
        DfExpr::Column(Column {
            relation: None,
            name: "b".to_string(),
        })
    }

    pub(crate) fn field_column() -> DfExpr {
        DfExpr::Column(Column {
            relation: None,
            name: "c".to_string(),
        })
    }

    pub(crate) fn nonexistent_column() -> DfExpr {
        DfExpr::Column(Column {
            relation: None,
            name: "nonexistent".to_string(),
        })
    }

    pub(crate) fn string_lit(s: impl Into<String>) -> DfExpr {
        DfExpr::Literal(ScalarValue::Utf8(Some(s.into())))
    }

    pub(crate) fn int64_lit(i: impl Into<i64>) -> DfExpr {
        DfExpr::Literal(ScalarValue::Int64(Some(i.into())))
    }

    pub(crate) fn encoded_string(s: impl Into<String>) -> Vec<u8> {
        let mut bytes = vec![];
        IndexValueCodec::encode_value(
            Value::from(s.into()).as_value_ref(),
            &SortField::new(ConcreteDataType::string_datatype()),
            &mut bytes,
        )
        .unwrap();
        bytes
    }

    pub(crate) fn encoded_int64(s: impl Into<i64>) -> Vec<u8> {
        let mut bytes = vec![];
        IndexValueCodec::encode_value(
            Value::from(s.into()).as_value_ref(),
            &SortField::new(ConcreteDataType::int64_datatype()),
            &mut bytes,
        )
        .unwrap();
        bytes
    }

    #[test]
    fn test_collect_and_basic() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let expr = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column()),
                op: Operator::RegexMatch,
                right: Box::new(string_lit("bar")),
            })),
            op: Operator::And,
            right: Box::new(DfExpr::Between(Between {
                expr: Box::new(tag_column2()),
                negated: false,
                low: Box::new(int64_lit(123)),
                high: Box::new(int64_lit(456)),
            })),
        });

        builder.traverse_and_collect(&expr);
        let predicates = builder.output.get("a").unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "bar".to_string()
            })
        );
        let predicates = builder.output.get("b").unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        inclusive: true,
                        value: encoded_int64(123),
                    }),
                    upper: Some(Bound {
                        inclusive: true,
                        value: encoded_int64(456),
                    }),
                }
            })
        );
    }
}
