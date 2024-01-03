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

use datafusion_expr::{Expr as DfExpr, Operator};
use index::inverted_index::search::predicate::{Bound, Predicate, Range, RangePredicate};
use index::inverted_index::Bytes;

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    /// Collects a comparison expression in the form of
    /// `column < lit`, `column > lit`, `column <= lit`, `column >= lit`,
    /// `lit < column`, `lit > column`, `lit <= column`, `lit >= column`.
    pub(crate) fn collect_comparison_expr(
        &mut self,
        left: &DfExpr,
        op: &Operator,
        right: &DfExpr,
    ) -> Result<()> {
        match op {
            Operator::Lt => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_column_gt_lit(right, left)
                } else {
                    self.collect_column_lt_lit(left, right)
                }
            }
            Operator::LtEq => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_column_ge_lit(right, left)
                } else {
                    self.collect_column_le_lit(left, right)
                }
            }
            Operator::Gt => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_column_lt_lit(right, left)
                } else {
                    self.collect_column_gt_lit(left, right)
                }
            }
            Operator::GtEq => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_column_le_lit(right, left)
                } else {
                    self.collect_column_ge_lit(left, right)
                }
            }
            _ => Ok(()),
        }
    }

    fn collect_column_lt_lit(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_column_cmp_lit(left, right, |value| Range {
            lower: None,
            upper: Some(Bound {
                inclusive: false,
                value,
            }),
        })
    }

    fn collect_column_gt_lit(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_column_cmp_lit(left, right, |value| Range {
            lower: Some(Bound {
                inclusive: false,
                value,
            }),
            upper: None,
        })
    }

    fn collect_column_le_lit(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_column_cmp_lit(left, right, |value| Range {
            lower: None,
            upper: Some(Bound {
                inclusive: true,
                value,
            }),
        })
    }

    fn collect_column_ge_lit(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_column_cmp_lit(left, right, |value| Range {
            lower: Some(Bound {
                inclusive: true,
                value,
            }),
            upper: None,
        })
    }

    fn collect_column_cmp_lit(
        &mut self,
        column: &DfExpr,
        literal: &DfExpr,
        range_builder: impl FnOnce(Bytes) -> Range,
    ) -> Result<()> {
        let Some(column_name) = Self::column_name(column) else {
            return Ok(());
        };
        let Some(lit) = Self::nonnull_lit(literal) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let predicate = Predicate::Range(RangePredicate {
            range: range_builder(Self::encode_lit(lit, data_type)?),
        });

        self.add_predicate(column_name, predicate);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::error::Error;
    use crate::sst::index::applier::builder::tests::{
        encoded_string, field_column, int64_lit, nonexistent_column, string_lit, tag_column,
        test_object_store, test_region_metadata,
    };

    #[test]
    fn test_collect_comparison_basic() {
        let cases = [
            (
                (&tag_column(), &Operator::Lt, &string_lit("123")),
                Range {
                    lower: None,
                    upper: Some(Bound {
                        inclusive: false,
                        value: encoded_string("123"),
                    }),
                },
            ),
            (
                (&string_lit("123"), &Operator::Lt, &tag_column()),
                Range {
                    lower: Some(Bound {
                        inclusive: false,
                        value: encoded_string("123"),
                    }),
                    upper: None,
                },
            ),
            (
                (&tag_column(), &Operator::LtEq, &string_lit("123")),
                Range {
                    lower: None,
                    upper: Some(Bound {
                        inclusive: true,
                        value: encoded_string("123"),
                    }),
                },
            ),
            (
                (&string_lit("123"), &Operator::LtEq, &tag_column()),
                Range {
                    lower: Some(Bound {
                        inclusive: true,
                        value: encoded_string("123"),
                    }),
                    upper: None,
                },
            ),
            (
                (&tag_column(), &Operator::Gt, &string_lit("123")),
                Range {
                    lower: Some(Bound {
                        inclusive: false,
                        value: encoded_string("123"),
                    }),
                    upper: None,
                },
            ),
            (
                (&string_lit("123"), &Operator::Gt, &tag_column()),
                Range {
                    lower: None,
                    upper: Some(Bound {
                        inclusive: false,
                        value: encoded_string("123"),
                    }),
                },
            ),
            (
                (&tag_column(), &Operator::GtEq, &string_lit("123")),
                Range {
                    lower: Some(Bound {
                        inclusive: true,
                        value: encoded_string("123"),
                    }),
                    upper: None,
                },
            ),
            (
                (&string_lit("123"), &Operator::GtEq, &tag_column()),
                Range {
                    lower: None,
                    upper: Some(Bound {
                        inclusive: true,
                        value: encoded_string("123"),
                    }),
                },
            ),
        ];

        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        for ((left, op, right), _) in &cases {
            builder.collect_comparison_expr(left, op, right).unwrap();
        }

        let predicates = builder.output.get("a").unwrap();
        assert_eq!(predicates.len(), cases.len());
        for ((_, expected), actual) in cases.into_iter().zip(predicates) {
            assert_eq!(
                actual,
                &Predicate::Range(RangePredicate { range: expected })
            );
        }
    }

    #[test]
    fn test_collect_comparison_type_mismatch() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let res = builder.collect_comparison_expr(&tag_column(), &Operator::Lt, &int64_lit(10));
        assert!(matches!(res, Err(Error::FieldTypeMismatch { .. })));
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_comparison_field_column() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        builder
            .collect_comparison_expr(&field_column(), &Operator::Lt, &string_lit("abc"))
            .unwrap();
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_comparison_nonexistent_column() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let res = builder.collect_comparison_expr(
            &nonexistent_column(),
            &Operator::Lt,
            &string_lit("abc"),
        );
        assert!(matches!(res, Err(Error::ColumnNotFound { .. })));
        assert!(builder.output.is_empty());
    }
}
