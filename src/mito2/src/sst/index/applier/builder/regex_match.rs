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

use datafusion_common::ScalarValue;
use datafusion_expr::Expr as DfExpr;
use index::inverted_index::search::predicate::{Predicate, RegexMatchPredicate};

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    /// Collects a regex match expression in the form of `column ~ pattern`.
    pub(crate) fn collect_regex_match(&mut self, column: &DfExpr, pattern: &DfExpr) -> Result<()> {
        let Some(column_name) = Self::column_name(column) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };
        if !data_type.is_string() {
            return Ok(());
        }
        let DfExpr::Literal(ScalarValue::Utf8(Some(pattern))) = pattern else {
            return Ok(());
        };

        let predicate = Predicate::RegexMatch(RegexMatchPredicate {
            pattern: pattern.clone(),
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
        field_column, int64_lit, nonexistent_column, string_lit, tag_column, test_object_store,
        test_region_metadata,
    };

    #[test]
    fn test_regex_match_basic() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        builder
            .collect_regex_match(&tag_column(), &string_lit("abc"))
            .unwrap();

        let predicates = builder.output.get("a").unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "abc".to_string()
            })
        );
    }

    #[test]
    fn test_regex_match_field_column() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        builder
            .collect_regex_match(&field_column(), &string_lit("abc"))
            .unwrap();

        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_regex_match_type_mismatch() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        builder
            .collect_regex_match(&tag_column(), &int64_lit(123))
            .unwrap();

        assert!(builder.output.is_empty());
    }

    #[test]
    fn test_regex_match_type_nonexist_column() {
        let metadata = test_region_metadata();
        let mut builder =
            SstIndexApplierBuilder::new("test".to_string(), test_object_store(), &metadata);

        let res = builder.collect_regex_match(&nonexistent_column(), &string_lit("abc"));
        assert!(matches!(res, Err(Error::ColumnNotFound { .. })));
        assert!(builder.output.is_empty());
    }
}
