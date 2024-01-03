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

use std::collections::HashSet;

use datafusion_expr::{BinaryExpr, Expr as DfExpr, Operator};
use datatypes::data_type::ConcreteDataType;
use index::inverted_index::search::predicate::{InListPredicate, Predicate};
use index::inverted_index::Bytes;

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    /// Collects an eq expression in the form of `column = lit`.
    pub(crate) fn collect_eq(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        let Some(column_name) = Self::column_name(left).or_else(|| Self::column_name(right)) else {
            return Ok(());
        };
        let Some(lit) = Self::nonnull_lit(right).or_else(|| Self::nonnull_lit(left)) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let predicate = Predicate::InList(InListPredicate {
            list: HashSet::from_iter([Self::encode_lit(lit, data_type)?]),
        });
        self.add_predicate(column_name, predicate);
        Ok(())
    }

    /// Collects eq list in the form of `column = lit OR column = lit OR ...`.
    pub(crate) fn collect_or_eq_list(&mut self, eq_expr: &DfExpr, or_list: &DfExpr) -> Result<()> {
        let DfExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) = eq_expr
        else {
            return Ok(());
        };

        let Some(column_name) = Self::column_name(left).or_else(|| Self::column_name(right)) else {
            return Ok(());
        };
        let Some(lit) = Self::nonnull_lit(right).or_else(|| Self::nonnull_lit(left)) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let bytes = Self::encode_lit(lit, data_type.clone())?;
        let mut inlist = HashSet::from_iter([bytes]);

        if Self::collect_eq_list_inner(column_name, &data_type, or_list, &mut inlist)? {
            let predicate = Predicate::InList(InListPredicate { list: inlist });
            self.add_predicate(column_name, predicate);
        }

        Ok(())
    }

    /// Recursively collects eq list.
    ///
    /// Returns false if the expression doesn't match the form then
    /// caller can safely ignore the expression.
    fn collect_eq_list_inner(
        column_name: &str,
        data_type: &ConcreteDataType,
        expr: &DfExpr,
        inlist: &mut HashSet<Bytes>,
    ) -> Result<bool> {
        let DfExpr::BinaryExpr(BinaryExpr {
            left,
            op: op @ (Operator::Eq | Operator::Or),
            right,
        }) = expr
        else {
            return Ok(false);
        };

        if op == &Operator::Or {
            let r = Self::collect_eq_list_inner(column_name, data_type, left, inlist)?
                .then(|| Self::collect_eq_list_inner(column_name, data_type, right, inlist))
                .transpose()?
                .unwrap_or(false);
            return Ok(r);
        }

        if op == &Operator::Eq {
            let Some(name) = Self::column_name(left).or_else(|| Self::column_name(right)) else {
                return Ok(false);
            };
            if column_name != name {
                return Ok(false);
            }
            let Some(lit) = Self::nonnull_lit(right).or_else(|| Self::nonnull_lit(left)) else {
                return Ok(false);
            };

            inlist.insert(Self::encode_lit(lit, data_type.clone())?);
            return Ok(true);
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::sst::index::applier::builder::tests::{
        encoded_string, field_column, int64_lit, nonexistent_column, string_lit, tag_column,
        tag_column2, test_object_store, test_region_metadata,
    };

    #[test]
    fn test_collect_eq_basic() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        builder
            .collect_eq(&tag_column(), &string_lit("foo"))
            .unwrap();
        builder
            .collect_eq(&string_lit("bar"), &tag_column())
            .unwrap();

        let predicates = builder.output.get("a").unwrap();
        assert_eq!(predicates.len(), 2);
        assert_eq!(
            predicates[0],
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter([encoded_string("foo")])
            })
        );
        assert_eq!(
            predicates[1],
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter([encoded_string("bar")])
            })
        );
    }

    #[test]
    fn test_collect_eq_field_column() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        builder
            .collect_eq(&field_column(), &string_lit("abc"))
            .unwrap();
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_eq_nonexistent_column() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let res = builder.collect_eq(&nonexistent_column(), &string_lit("abc"));
        assert!(matches!(res, Err(Error::ColumnNotFound { .. })));
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_eq_type_mismatch() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let res = builder.collect_eq(&tag_column(), &int64_lit(1));
        assert!(matches!(res, Err(Error::FieldTypeMismatch { .. })));
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_or_eq_list_basic() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let eq_expr = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(tag_column()),
            op: Operator::Eq,
            right: Box::new(string_lit("abc")),
        });
        let or_eq_list = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column()),
                op: Operator::Eq,
                right: Box::new(string_lit("foo")),
            })),
            op: Operator::Or,
            right: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(tag_column()),
                    op: Operator::Eq,
                    right: Box::new(string_lit("bar")),
                })),
                op: Operator::Or,
                right: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(tag_column()),
                    op: Operator::Eq,
                    right: Box::new(string_lit("baz")),
                })),
            })),
        });

        builder.collect_or_eq_list(&eq_expr, &or_eq_list).unwrap();

        let predicates = builder.output.get("a").unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter([
                    encoded_string("abc"),
                    encoded_string("foo"),
                    encoded_string("bar"),
                    encoded_string("baz")
                ])
            })
        );
    }

    #[test]
    fn test_collect_or_eq_list_invalid_op() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let eq_expr = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(tag_column()),
            op: Operator::Eq,
            right: Box::new(string_lit("abc")),
        });
        let or_eq_list = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column()),
                op: Operator::Eq,
                right: Box::new(string_lit("foo")),
            })),
            op: Operator::Or,
            right: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column()),
                op: Operator::Gt, // invalid op
                right: Box::new(string_lit("foo")),
            })),
        });

        builder.collect_or_eq_list(&eq_expr, &or_eq_list).unwrap();
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_or_eq_list_multiple_columns() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let eq_expr = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(tag_column()),
            op: Operator::Eq,
            right: Box::new(string_lit("abc")),
        });
        let or_eq_list = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column()),
                op: Operator::Eq,
                right: Box::new(string_lit("foo")),
            })),
            op: Operator::Or,
            right: Box::new(DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column2()), // different column
                op: Operator::Eq,
                right: Box::new(string_lit("bar")),
            })),
        });

        builder.collect_or_eq_list(&eq_expr, &or_eq_list).unwrap();
        assert!(builder.output.is_empty());
    }
}
