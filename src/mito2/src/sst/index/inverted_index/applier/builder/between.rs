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

use datafusion_expr::Between;
use index::inverted_index::search::predicate::{Bound, Predicate, Range, RangePredicate};

use crate::error::Result;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;

impl InvertedIndexApplierBuilder<'_> {
    /// Collects a `BETWEEN` expression in the form of `column BETWEEN lit AND lit`.
    pub(crate) fn collect_between(&mut self, between: &Between) -> Result<()> {
        if between.negated {
            return Ok(());
        }

        let Some(column_name) = Self::column_name(&between.expr) else {
            return Ok(());
        };
        let Some((column_id, data_type)) = self.column_id_and_type(column_name)? else {
            return Ok(());
        };
        let Some(low) = Self::nonnull_lit(&between.low) else {
            return Ok(());
        };
        let Some(high) = Self::nonnull_lit(&between.high) else {
            return Ok(());
        };

        let predicate = Predicate::Range(RangePredicate {
            range: Range {
                lower: Some(Bound {
                    inclusive: true,
                    value: Self::encode_lit(low, data_type.clone())?,
                }),
                upper: Some(Bound {
                    inclusive: true,
                    value: Self::encode_lit(high, data_type)?,
                }),
            },
        });

        self.add_predicate(column_id, predicate);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use store_api::region_request::PathType;

    use super::*;
    use crate::error::Error;
    use crate::sst::index::inverted_index::applier::builder::tests::{
        encoded_string, field_column, int64_lit, nonexistent_column, string_lit, tag_column,
        test_object_store, test_region_metadata,
    };
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    #[test]
    fn test_collect_between_basic() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_collect_between_basic_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        let between = Between {
            negated: false,
            expr: Box::new(tag_column()),
            low: Box::new(string_lit("abc")),
            high: Box::new(string_lit("def")),
        };

        builder.collect_between(&between).unwrap();

        let predicates = builder.output.get(&1).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        inclusive: true,
                        value: encoded_string("abc"),
                    }),
                    upper: Some(Bound {
                        inclusive: true,
                        value: encoded_string("def"),
                    }),
                }
            })
        );
    }

    #[test]
    fn test_collect_between_negated() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_block("test_collect_between_negated_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        let between = Between {
            negated: true,
            expr: Box::new(tag_column()),
            low: Box::new(string_lit("abc")),
            high: Box::new(string_lit("def")),
        };

        builder.collect_between(&between).unwrap();
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_between_field_column() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_block("test_collect_between_field_column_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        let between = Between {
            negated: false,
            expr: Box::new(field_column()),
            low: Box::new(string_lit("abc")),
            high: Box::new(string_lit("def")),
        };

        builder.collect_between(&between).unwrap();

        let predicates = builder.output.get(&3).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        inclusive: true,
                        value: encoded_string("abc"),
                    }),
                    upper: Some(Bound {
                        inclusive: true,
                        value: encoded_string("def"),
                    }),
                }
            })
        );
    }

    #[test]
    fn test_collect_between_type_mismatch() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_block("test_collect_between_type_mismatch_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        let between = Between {
            negated: false,
            expr: Box::new(tag_column()),
            low: Box::new(int64_lit(123)),
            high: Box::new(int64_lit(456)),
        };

        let res = builder.collect_between(&between);
        assert!(matches!(res, Err(Error::Encode { .. })));
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_between_nonexistent_column() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_block("test_collect_between_nonexistent_column_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        let between = Between {
            negated: false,
            expr: Box::new(nonexistent_column()),
            low: Box::new(string_lit("abc")),
            high: Box::new(string_lit("def")),
        };

        let res = builder.collect_between(&between);
        assert!(matches!(res, Err(Error::ColumnNotFound { .. })));
        assert!(builder.output.is_empty());
    }
}
