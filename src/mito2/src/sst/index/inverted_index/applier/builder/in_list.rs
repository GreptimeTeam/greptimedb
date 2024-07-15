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

use datafusion_expr::expr::InList;
use index::inverted_index::search::predicate::{InListPredicate, Predicate};

use crate::error::Result;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;

impl<'a> InvertedIndexApplierBuilder<'a> {
    /// Collects an in list expression in the form of `column IN (lit, lit, ...)`.
    pub(crate) fn collect_inlist(&mut self, inlist: &InList) -> Result<()> {
        if inlist.negated {
            return Ok(());
        }
        let Some(column_name) = Self::column_name(&inlist.expr) else {
            return Ok(());
        };
        let Some((column_id, data_type)) = self.tag_column_id_and_type(column_name)? else {
            return Ok(());
        };

        let mut predicate = InListPredicate {
            list: HashSet::with_capacity(inlist.list.len()),
        };
        for lit in &inlist.list {
            let Some(lit) = Self::nonnull_lit(lit) else {
                return Ok(());
            };

            predicate
                .list
                .insert(Self::encode_lit(lit, data_type.clone())?);
        }

        self.add_predicate(column_id, Predicate::InList(predicate));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::sst::index::inverted_index::applier::builder::tests::{
        encoded_string, field_column, int64_lit, nonexistent_column, string_lit, tag_column,
        test_object_store, test_region_metadata,
    };
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    #[test]
    fn test_collect_in_list_basic() {
        let (_d, facotry) = PuffinManagerFactory::new_for_test_block("test_collect_in_list_basic_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            None,
            None,
            &metadata,
            HashSet::default(),
            facotry,
        );

        let in_list = InList {
            expr: Box::new(tag_column()),
            list: vec![string_lit("foo"), string_lit("bar")],
            negated: false,
        };

        builder.collect_inlist(&in_list).unwrap();

        let predicates = builder.output.get(&1).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter([encoded_string("foo"), encoded_string("bar")])
            })
        );
    }

    #[test]
    fn test_collect_in_list_negated() {
        let (_d, facotry) =
            PuffinManagerFactory::new_for_test_block("test_collect_in_list_negated_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            None,
            None,
            &metadata,
            HashSet::default(),
            facotry,
        );

        let in_list = InList {
            expr: Box::new(tag_column()),
            list: vec![string_lit("foo"), string_lit("bar")],
            negated: true,
        };

        builder.collect_inlist(&in_list).unwrap();
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_in_list_field_column() {
        let (_d, facotry) =
            PuffinManagerFactory::new_for_test_block("test_collect_in_list_field_column_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            None,
            None,
            &metadata,
            HashSet::default(),
            facotry,
        );

        let in_list = InList {
            expr: Box::new(field_column()),
            list: vec![string_lit("foo"), string_lit("bar")],
            negated: false,
        };

        builder.collect_inlist(&in_list).unwrap();
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_in_list_type_mismatch() {
        let (_d, facotry) =
            PuffinManagerFactory::new_for_test_block("test_collect_in_list_type_mismatch_");
        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            None,
            None,
            &metadata,
            HashSet::default(),
            facotry,
        );

        let in_list = InList {
            expr: Box::new(tag_column()),
            list: vec![int64_lit(123), int64_lit(456)],
            negated: false,
        };

        let res = builder.collect_inlist(&in_list);
        assert!(matches!(res, Err(Error::FieldTypeMismatch { .. })));
        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_collect_in_list_nonexistent_column() {
        let (_d, facotry) =
            PuffinManagerFactory::new_for_test_block("test_collect_in_list_nonexistent_column_");

        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            None,
            None,
            &metadata,
            HashSet::default(),
            facotry,
        );

        let in_list = InList {
            expr: Box::new(nonexistent_column()),
            list: vec![string_lit("foo"), string_lit("bar")],
            negated: false,
        };

        let res = builder.collect_inlist(&in_list);
        assert!(matches!(res, Err(Error::ColumnNotFound { .. })));
        assert!(builder.output.is_empty());
    }
}
