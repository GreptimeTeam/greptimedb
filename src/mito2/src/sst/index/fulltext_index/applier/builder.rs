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

use common_base::term_token::like_probes;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{Like, ScalarFunction};
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::schema::FulltextBackend;
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
    /// Patterns of case-sensitive `LIKE` predicates, e.g. "%foo%" in `text LIKE '%foo%'`.
    pub like_patterns: Vec<String>,
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
        let has_requests = requests.iter().any(|(_, request)| {
            !request.queries.is_empty()
                || !request.terms.is_empty()
                || !request.like_patterns.is_empty()
        });

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
            Expr::Like(like) => {
                if let Some((column_id, pattern)) = Self::expr_to_like_pattern(metadata, like) {
                    requests
                        .entry(column_id)
                        .or_default()
                        .like_patterns
                        .push(pattern);
                }
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

        Some((column.column_id, FulltextQuery(query.clone())))
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
                term: term.clone(),
            },
        ))
    }

    fn expr_to_like_pattern(metadata: &RegionMetadata, like: &Like) -> Option<(ColumnId, String)> {
        // `ILIKE` uses Unicode case folding, which disagrees with the index's
        // `to_lowercase` on characters like 'ſ', so its probes could miss rows.
        if like.negated || like.case_insensitive {
            return None;
        }
        // Probes assume arrow's default `\` escape.
        if !matches!(like.escape_char, None | Some('\\')) {
            return None;
        }

        let Expr::Column(c) = like.expr.as_ref() else {
            return None;
        };
        let column = metadata.column_by_name(&c.name)?;
        if column.column_schema.data_type != ConcreteDataType::string_datatype() {
            return None;
        }
        // `LIKE` is common on unindexed columns and with patterns like '%word%' that have
        // no probe; skip them instead of opening every SST's index for nothing.
        let options = column.column_schema.fulltext_options().ok()??;
        if !options.enable || options.backend != FulltextBackend::Bloom {
            return None;
        }

        let Expr::Literal(
            ScalarValue::Utf8(Some(pattern))
            | ScalarValue::LargeUtf8(Some(pattern))
            | ScalarValue::Utf8View(Some(pattern)),
            _,
        ) = like.pattern.as_ref()
        else {
            return None;
        };
        if like_probes(pattern).is_empty() {
            return None;
        }

        Some((column.column_id, pattern.clone()))
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
    use datafusion_expr::expr::{Like, ScalarFunction};
    use datafusion_expr::{Literal, ScalarUDF};
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextOptions};
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
            ScalarFunctionFactory::from(Arc::new(MatchesFunction::default()) as FunctionRef)
                .provide(Default::default()),
        )
    }

    fn matches_term_func() -> Arc<ScalarUDF> {
        Arc::new(
            ScalarFunctionFactory::from(Arc::new(MatchesTermFunction::default()) as FunctionRef)
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

    fn mock_metadata_with_fulltext(backend: FulltextBackend) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("text", ConcreteDataType::string_datatype(), true)
                    .with_fulltext_options(FulltextOptions::new_unchecked(
                        true,
                        FulltextAnalyzer::English,
                        false,
                        backend,
                        10240,
                        0.01,
                    ))
                    .unwrap(),
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

    #[test]
    fn test_expr_to_like_pattern() {
        let like = |negated, case_insensitive, escape_char, pattern: &str| Like {
            negated,
            expr: Box::new(Expr::Column(Column::from_name("text"))),
            pattern: Box::new(pattern.lit()),
            escape_char,
            case_insensitive,
        };
        let bloom = mock_metadata_with_fulltext(FulltextBackend::Bloom);

        for escape_char in [None, Some('\\')] {
            assert_eq!(
                FulltextIndexApplierBuilder::expr_to_like_pattern(
                    &bloom,
                    &like(false, false, escape_char, "%foo bar baz%")
                ),
                Some((1, "%foo bar baz%".to_string()))
            );
        }
        // NOT LIKE, ILIKE and other escape characters can't use the probes, and
        // '%foo%' has none.
        for like in [
            like(true, false, None, "%foo bar baz%"),
            like(false, true, None, "%foo bar baz%"),
            like(false, false, Some('!'), "%foo bar baz%"),
            like(false, false, None, "%foo%"),
        ] {
            assert_eq!(
                FulltextIndexApplierBuilder::expr_to_like_pattern(&bloom, &like),
                None
            );
        }
        // Only columns with a bloom fulltext index.
        for metadata in [
            mock_metadata(),
            mock_metadata_with_fulltext(FulltextBackend::Tantivy),
        ] {
            assert_eq!(
                FulltextIndexApplierBuilder::expr_to_like_pattern(
                    &metadata,
                    &like(false, false, None, "%foo bar baz%")
                ),
                None
            );
        }
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
}
