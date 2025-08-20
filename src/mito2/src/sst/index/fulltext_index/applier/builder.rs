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

use std::collections::BTreeMap;

use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use store_api::metadata::RegionMetadata;
use store_api::region_request::PathType;
use store_api::storage::{ColumnId, ConcreteDataType};

use crate::cache::file_cache::FileCacheRef;
use crate::cache::index::bloom_filter_index::BloomFilterIndexCacheRef;
use crate::error::Result;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
use crate::sst::index::puffin_manager::PuffinManagerFactory;

/// A request for fulltext index.
///
/// It contains all the queries and terms for a column.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FulltextRequest {
    pub queries: Vec<FulltextQuery>,
    pub terms: Vec<FulltextTerm>,
}

impl FulltextRequest {
    /// Convert terms to a query string.
    ///
    /// For example, if the terms are ["foo", "bar"], the query string will be `r#"+"foo" +"bar""#`.
    /// Need to escape the `"` in the term.
    ///
    /// `skip_lowercased` is used for the situation that lowercased terms are not indexed.
    pub fn terms_as_query(&self, skip_lowercased: bool) -> FulltextQuery {
        let mut query = String::new();
        for term in &self.terms {
            if skip_lowercased && term.col_lowered {
                continue;
            }
            // Escape the `"` in the term.
            let escaped_term = term.term.replace("\"", "\\\"");
            if query.is_empty() {
                query = format!("+\"{escaped_term}\"");
            } else {
                query.push_str(&format!(" +\"{escaped_term}\""));
            }
        }
        FulltextQuery(query)
    }
}

/// A query to be matched in fulltext index.
///
/// `query` is the query to be matched, e.g. "+foo -bar" in `SELECT * FROM t WHERE matches(text, "+foo -bar")`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FulltextQuery(pub String);

/// A term to be matched in fulltext index.
///
/// `term` is the term to be matched, e.g. "foo" in `SELECT * FROM t WHERE matches_term(text, "foo")`.
/// `col_lowered` indicates whether the column is lowercased, e.g. `col_lowered = true` when `matches_term(lower(text), "foo")`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FulltextTerm {
    pub col_lowered: bool,
    pub term: String,
}

/// `FulltextIndexApplierBuilder` is a builder for `FulltextIndexApplier`.
pub struct FulltextIndexApplierBuilder<'a> {
    table_dir: String,
    path_type: PathType,
    store: ObjectStore,
    puffin_manager_factory: PuffinManagerFactory,
    metadata: &'a RegionMetadata,
    file_cache: Option<FileCacheRef>,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    bloom_filter_cache: Option<BloomFilterIndexCacheRef>,
}

impl<'a> FulltextIndexApplierBuilder<'a> {
    /// Creates a new `FulltextIndexApplierBuilder`.
    pub fn new(
        table_dir: String,
        path_type: PathType,
        store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        metadata: &'a RegionMetadata,
    ) -> Self {
        Self {
            table_dir,
            path_type,
            store,
            puffin_manager_factory,
            metadata,
            file_cache: None,
            puffin_metadata_cache: None,
            bloom_filter_cache: None,
        }
    }

    /// Sets the file cache to be used by the `FulltextIndexApplier`.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    /// Sets the puffin metadata cache to be used by the `FulltextIndexApplier`.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    /// Sets the bloom filter cache to be used by the `FulltextIndexApplier`.
    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_cache = bloom_filter_cache;
        self
    }

    /// Builds `SstIndexApplier` from the given expressions.
    pub fn build(self, exprs: &[Expr]) -> Result<Option<FulltextIndexApplier>> {
        let mut requests = BTreeMap::new();
        for expr in exprs {
            Self::extract_requests(expr, self.metadata, &mut requests);
        }

        // Check if any requests have queries or terms
        let has_requests = requests
            .iter()
            .any(|(_, request)| !request.queries.is_empty() || !request.terms.is_empty());

        Ok(has_requests.then(|| {
            FulltextIndexApplier::new(
                self.table_dir,
                self.path_type,
                self.store,
                requests,
                self.puffin_manager_factory,
            )
            .with_file_cache(self.file_cache)
            .with_puffin_metadata_cache(self.puffin_metadata_cache)
            .with_bloom_filter_cache(self.bloom_filter_cache)
        }))
    }

    fn extract_requests(
        expr: &Expr,
        metadata: &'a RegionMetadata,
        requests: &mut BTreeMap<ColumnId, FulltextRequest>,
    ) {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                Self::extract_requests(left, metadata, requests);
                Self::extract_requests(right, metadata, requests);
            }
            Expr::ScalarFunction(func) => {
                if let Some((column_id, query)) = Self::expr_to_query(metadata, func) {
                    requests.entry(column_id).or_default().queries.push(query);
                } else if let Some((column_id, term)) = Self::expr_to_term(metadata, func) {
                    requests.entry(column_id).or_default().terms.push(term);
                }
            }
            _ => {}
        }
    }

    fn expr_to_query(
        metadata: &RegionMetadata,
        f: &ScalarFunction,
    ) -> Option<(ColumnId, FulltextQuery)> {
        if f.name() != "matches" {
            return None;
        }
        if f.args.len() != 2 {
            return None;
        }

        let Expr::Column(c) = &f.args[0] else {
            return None;
        };
        let column = metadata.column_by_name(&c.name)?;

        if column.column_schema.data_type != ConcreteDataType::string_datatype() {
            return None;
        }

        let Expr::Literal(ScalarValue::Utf8(Some(query)), _) = &f.args[1] else {
            return None;
        };

        Some((column.column_id, FulltextQuery(query.to_string())))
    }

    fn expr_to_term(
        metadata: &RegionMetadata,
        f: &ScalarFunction,
    ) -> Option<(ColumnId, FulltextTerm)> {
        if f.name() != "matches_term" {
            return None;
        }
        if f.args.len() != 2 {
            return None;
        }

        let mut lowered = false;
        let column;
        match &f.args[0] {
            Expr::Column(c) => {
                column = c;
            }
            Expr::ScalarFunction(f) => {
                let lower_arg = Self::extract_lower_arg(f)?;
                lowered = true;
                if let Expr::Column(c) = lower_arg {
                    column = c;
                } else {
                    return None;
                }
            }
            _ => return None,
        }

        let column = metadata.column_by_name(&column.name)?;
        if column.column_schema.data_type != ConcreteDataType::string_datatype() {
            return None;
        }

        let Expr::Literal(ScalarValue::Utf8(Some(term)), _) = &f.args[1] else {
            return None;
        };

        Some((
            column.column_id,
            FulltextTerm {
                col_lowered: lowered,
                term: term.to_string(),
            },
        ))
    }

    fn extract_lower_arg(lower_func: &ScalarFunction) -> Option<&Expr> {
        if lower_func.args.len() != 1 {
            return None;
        }

        if lower_func.name() != "lower" {
            return None;
        }

        if lower_func.args.len() != 1 {
            return None;
        }

        Some(&lower_func.args[0])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::function::FunctionRef;
    use common_function::function_factory::ScalarFunctionFactory;
    use common_function::scalars::matches::MatchesFunction;
    use common_function::scalars::matches_term::MatchesTermFunction;
    use datafusion::functions::string::lower;
    use datafusion_common::Column;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Literal, ScalarUDF};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    fn mock_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("text", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });

        builder.build().unwrap()
    }

    fn matches_func() -> Arc<ScalarUDF> {
        Arc::new(
            ScalarFunctionFactory::from(Arc::new(MatchesFunction) as FunctionRef)
                .provide(Default::default()),
        )
    }

    fn matches_term_func() -> Arc<ScalarUDF> {
        Arc::new(
            ScalarFunctionFactory::from(Arc::new(MatchesTermFunction) as FunctionRef)
                .provide(Default::default()),
        )
    }

    #[test]
    fn test_expr_to_query_basic() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "foo".lit()],
            func: matches_func(),
        };

        let (column_id, query) =
            FulltextIndexApplierBuilder::expr_to_query(&metadata, &func).unwrap();
        assert_eq!(column_id, 1);
        assert_eq!(query, FulltextQuery("foo".to_string()));
    }

    #[test]
    fn test_expr_to_query_wrong_num_args() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text"))],
            func: matches_func(),
        };

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &func).is_none());
    }

    #[test]
    fn test_expr_to_query_not_found_column() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("not_found")), "foo".lit()],
            func: matches_func(),
        };

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &func).is_none());
    }

    #[test]
    fn test_expr_to_query_column_wrong_data_type() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("ts")), "foo".lit()],
            func: matches_func(),
        };

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &func).is_none());
    }

    #[test]
    fn test_expr_to_query_pattern_not_string() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), 42.lit()],
            func: matches_func(),
        };

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &func).is_none());
    }

    #[test]
    fn test_expr_to_term_basic() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "foo".lit()],
            func: matches_term_func(),
        };

        let (column_id, term) =
            FulltextIndexApplierBuilder::expr_to_term(&metadata, &func).unwrap();
        assert_eq!(column_id, 1);
        assert_eq!(
            term,
            FulltextTerm {
                col_lowered: false,
                term: "foo".to_string(),
            }
        );
    }

    #[test]
    fn test_expr_to_term_with_lower() {
        let metadata = mock_metadata();

        let lower_func_expr = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text"))],
            func: lower(),
        };

        let func = ScalarFunction {
            args: vec![Expr::ScalarFunction(lower_func_expr), "foo".lit()],
            func: matches_term_func(),
        };

        let (column_id, term) =
            FulltextIndexApplierBuilder::expr_to_term(&metadata, &func).unwrap();
        assert_eq!(column_id, 1);
        assert_eq!(
            term,
            FulltextTerm {
                col_lowered: true,
                term: "foo".to_string(),
            }
        );
    }

    #[test]
    fn test_expr_to_term_wrong_num_args() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text"))],
            func: matches_term_func(),
        };

        assert!(FulltextIndexApplierBuilder::expr_to_term(&metadata, &func).is_none());
    }

    #[test]
    fn test_expr_to_term_wrong_function_name() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "foo".lit()],
            func: matches_func(), // Using 'matches' instead of 'matches_term'
        };

        assert!(FulltextIndexApplierBuilder::expr_to_term(&metadata, &func).is_none());
    }

    #[test]
    fn test_extract_lower_arg() {
        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text"))],
            func: lower(),
        };

        let arg = FulltextIndexApplierBuilder::extract_lower_arg(&func).unwrap();
        match arg {
            Expr::Column(c) => {
                assert_eq!(c.name, "text");
            }
            _ => panic!("Expected Column expression"),
        }
    }

    #[test]
    fn test_extract_lower_arg_wrong_function() {
        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text"))],
            func: matches_func(), // Not 'lower'
        };

        assert!(FulltextIndexApplierBuilder::extract_lower_arg(&func).is_none());
    }

    #[test]
    fn test_extract_requests() {
        let metadata = mock_metadata();

        // Create a matches expression
        let matches_expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "foo".lit()],
            func: matches_func(),
        });

        let mut requests = BTreeMap::new();
        FulltextIndexApplierBuilder::extract_requests(&matches_expr, &metadata, &mut requests);

        assert_eq!(requests.len(), 1);
        let request = requests.get(&1).unwrap();
        assert_eq!(request.queries.len(), 1);
        assert_eq!(request.terms.len(), 0);
        assert_eq!(request.queries[0], FulltextQuery("foo".to_string()));
    }

    #[test]
    fn test_extract_multiple_requests() {
        let metadata = mock_metadata();

        // Create a matches expression
        let matches_expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "foo".lit()],
            func: matches_func(),
        });

        // Create a matches_term expression
        let matches_term_expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "bar".lit()],
            func: matches_term_func(),
        });

        // Create a binary expression combining both
        let binary_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(matches_expr),
            op: Operator::And,
            right: Box::new(matches_term_expr),
        });

        let mut requests = BTreeMap::new();
        FulltextIndexApplierBuilder::extract_requests(&binary_expr, &metadata, &mut requests);

        assert_eq!(requests.len(), 1);
        let request = requests.get(&1).unwrap();
        assert_eq!(request.queries.len(), 1);
        assert_eq!(request.terms.len(), 1);
        assert_eq!(request.queries[0], FulltextQuery("foo".to_string()));
        assert_eq!(
            request.terms[0],
            FulltextTerm {
                col_lowered: false,
                term: "bar".to_string(),
            }
        );
    }

    #[test]
    fn test_terms_as_query() {
        // Test with empty terms
        let request = FulltextRequest::default();
        assert_eq!(request.terms_as_query(false), FulltextQuery(String::new()));
        assert_eq!(request.terms_as_query(true), FulltextQuery(String::new()));

        // Test with a single term (not lowercased)
        let mut request = FulltextRequest::default();
        request.terms.push(FulltextTerm {
            col_lowered: false,
            term: "foo".to_string(),
        });
        assert_eq!(
            request.terms_as_query(false),
            FulltextQuery("+\"foo\"".to_string())
        );
        assert_eq!(
            request.terms_as_query(true),
            FulltextQuery("+\"foo\"".to_string())
        );

        // Test with a single lowercased term and skip_lowercased=true
        let mut request = FulltextRequest::default();
        request.terms.push(FulltextTerm {
            col_lowered: true,
            term: "foo".to_string(),
        });
        assert_eq!(
            request.terms_as_query(false),
            FulltextQuery("+\"foo\"".to_string())
        );
        assert_eq!(request.terms_as_query(true), FulltextQuery(String::new())); // Should skip lowercased term

        // Test with multiple terms, mix of lowercased and not
        let mut request = FulltextRequest::default();
        request.terms.push(FulltextTerm {
            col_lowered: false,
            term: "foo".to_string(),
        });
        request.terms.push(FulltextTerm {
            col_lowered: true,
            term: "bar".to_string(),
        });
        assert_eq!(
            request.terms_as_query(false),
            FulltextQuery("+\"foo\" +\"bar\"".to_string())
        );
        assert_eq!(
            request.terms_as_query(true),
            FulltextQuery("+\"foo\"".to_string()) // Only the non-lowercased term
        );

        // Test with term containing quotes that need escaping
        let mut request = FulltextRequest::default();
        request.terms.push(FulltextTerm {
            col_lowered: false,
            term: "foo\"bar".to_string(),
        });
        assert_eq!(
            request.terms_as_query(false),
            FulltextQuery("+\"foo\\\"bar\"".to_string())
        );

        // Test with a complex mix of terms
        let mut request = FulltextRequest::default();
        request.terms.push(FulltextTerm {
            col_lowered: false,
            term: "foo".to_string(),
        });
        request.terms.push(FulltextTerm {
            col_lowered: true,
            term: "bar\"quoted\"".to_string(),
        });
        request.terms.push(FulltextTerm {
            col_lowered: false,
            term: "baz\\escape".to_string(),
        });
        assert_eq!(
            request.terms_as_query(false),
            FulltextQuery("+\"foo\" +\"bar\\\"quoted\\\"\" +\"baz\\escape\"".to_string())
        );
        assert_eq!(
            request.terms_as_query(true),
            FulltextQuery("+\"foo\" +\"baz\\escape\"".to_string()) // Skips the lowercased term
        );
    }
}
