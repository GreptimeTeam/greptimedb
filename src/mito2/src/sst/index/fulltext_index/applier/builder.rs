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

use common_base::term_token::extract_like_probes;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{Like, ScalarFunction};
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::schema::FulltextBackend;
use index::target::IndexTarget;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use store_api::metadata::RegionMetadata;
use store_api::region_request::PathType;
#[cfg(test)]
use store_api::storage::ConcreteDataType;

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
    pub likes: Vec<FulltextLike>,
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

/// A case-sensitive `LIKE` pattern to be matched in fulltext index.
///
/// `pattern` is e.g. "%foo bar%" in `SELECT * FROM t WHERE text LIKE '%foo bar%'`.
/// `col_lowered` indicates whether the column is lowercased, e.g. `lower(text) LIKE '%foo bar%'`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FulltextLike {
    pub col_lowered: bool,
    pub pattern: String,
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
            !request.queries.is_empty() || !request.terms.is_empty() || !request.likes.is_empty()
        });

        Ok(has_requests.then(|| {
            FulltextIndexApplier::new(
                self.table_dir,
                self.path_type,
                self.store,
                requests,
                self.puffin_manager_factory,
                self.metadata,
            )
            .with_file_cache(self.file_cache)
            .with_puffin_metadata_cache(self.puffin_metadata_cache)
            .with_bloom_filter_cache(self.bloom_filter_cache)
        }))
    }

    fn extract_requests(
        expr: &Expr,
        metadata: &'a RegionMetadata,
        requests: &mut BTreeMap<IndexTarget, FulltextRequest>,
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
                if let Some((target, like)) = Self::expr_to_like(metadata, like) {
                    requests.entry(target).or_default().likes.push(like);
                }
            }
            Expr::ScalarFunction(func) => {
                if let Some((target, query)) = Self::expr_to_query(metadata, func) {
                    requests.entry(target).or_default().queries.push(query);
                } else if let Some((target, term)) = Self::expr_to_term(metadata, func) {
                    requests.entry(target).or_default().terms.push(term);
                }
            }
            _ => {}
        }
    }

    fn expr_to_query(
        metadata: &RegionMetadata,
        f: &ScalarFunction,
    ) -> Option<(IndexTarget, FulltextQuery)> {
        if f.name() != "matches" {
            return None;
        }
        if f.args.len() != 2 {
            return None;
        }

        let target = Self::expr_target(metadata, &f.args[0])?;

        let Expr::Literal(
            ScalarValue::Utf8(Some(query))
            | ScalarValue::LargeUtf8(Some(query))
            | ScalarValue::Utf8View(Some(query)),
            _,
        ) = &f.args[1]
        else {
            return None;
        };

        Some((target, FulltextQuery(query.clone())))
    }

    fn expr_to_term(
        metadata: &RegionMetadata,
        f: &ScalarFunction,
    ) -> Option<(IndexTarget, FulltextTerm)> {
        if f.name() != "matches_term" {
            return None;
        }
        if f.args.len() != 2 {
            return None;
        }

        let (expr, lowered) = Self::unwrap_lower(&f.args[0]);
        let target = Self::expr_target(metadata, expr)?;

        let Expr::Literal(
            ScalarValue::Utf8(Some(term))
            | ScalarValue::LargeUtf8(Some(term))
            | ScalarValue::Utf8View(Some(term)),
            _,
        ) = &f.args[1]
        else {
            return None;
        };

        Some((
            target,
            FulltextTerm {
                col_lowered: lowered,
                term: term.clone(),
            },
        ))
    }

    fn expr_to_like(metadata: &RegionMetadata, like: &Like) -> Option<(IndexTarget, FulltextLike)> {
        // `ILIKE` uses Unicode case folding, which disagrees with the index's
        // `to_lowercase` on characters like 'ſ', so its probes could miss rows.
        if like.negated || like.case_insensitive {
            return None;
        }
        // Probes assume arrow's default `\` escape.
        if !matches!(like.escape_char, None | Some('\\')) {
            return None;
        }

        let (expr, col_lowered) = Self::unwrap_lower(&like.expr);
        let target = Self::expr_target(metadata, expr)?;
        let options = crate::sst::index::column::fulltext_target_options(metadata, &target)?;
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
        if extract_like_probes(pattern).is_empty() {
            return None;
        }

        Some((
            target,
            FulltextLike {
                col_lowered,
                pattern: pattern.clone(),
            },
        ))
    }

    fn expr_target(metadata: &RegionMetadata, expr: &Expr) -> Option<IndexTarget> {
        if let Expr::Column(column) = expr {
            let column = metadata.column_by_name(&column.name)?;
            if !column.column_schema.data_type.is_string() {
                return None;
            }
            let target = IndexTarget::ColumnId(column.column_id);
            crate::sst::index::column::fulltext_target_options(metadata, &target)?;
            return Some(target);
        }
        let (target, ty) = crate::sst::index::column::json_index_target(metadata, expr, |hint| {
            hint.fulltext_index
                .as_ref()
                .is_some_and(|options| options.enable)
        })
        .ok()??;
        ty.is_string().then_some(target)
    }

    fn unwrap_lower(expr: &Expr) -> (&Expr, bool) {
        if let Expr::ScalarFunction(func) = expr
            && let Some(inner) = Self::extract_lower_arg(func)
        {
            return (inner, true);
        }
        (expr, false)
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
    use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
    use datatypes::json::{JsonSettings, JsonTypeHint};
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextOptions};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_json_fulltext_predicate_extraction() {
        let options = FulltextOptions {
            enable: true,
            ..Default::default()
        };
        let metadata = json_metadata(Some(options));
        let target = IndexTarget::new_json_path(3, vec!["message".into()]).unwrap();
        for literal in [
            ScalarValue::Utf8(Some("error timeout".into())),
            ScalarValue::LargeUtf8(Some("error timeout".into())),
            ScalarValue::Utf8View(Some("error timeout".into())),
        ] {
            let expr = json_expr("$.message", Some(ScalarValue::Utf8View(None)));
            let query = ScalarFunction::new_udf(
                matches_func(),
                vec![expr.clone(), Expr::Literal(literal.clone(), None)],
            );
            assert_eq!(
                FulltextIndexApplierBuilder::expr_to_query(&metadata, &query)
                    .unwrap()
                    .0,
                target
            );
            for lowered in [false, true] {
                let expr = if lowered {
                    lower().call(vec![expr.clone()])
                } else {
                    expr.clone()
                };
                let term = ScalarFunction::new_udf(
                    matches_term_func(),
                    vec![expr.clone(), Expr::Literal(literal.clone(), None)],
                );
                let (actual, term) =
                    FulltextIndexApplierBuilder::expr_to_term(&metadata, &term).unwrap();
                assert_eq!(actual, target);
                assert_eq!(term.col_lowered, lowered);
                let like = Like::new(
                    false,
                    Box::new(expr),
                    Box::new(Expr::Literal(literal.clone(), None)),
                    None,
                    false,
                );
                let (actual, like) =
                    FulltextIndexApplierBuilder::expr_to_like(&metadata, &like).unwrap();
                assert_eq!(actual, target);
                assert_eq!(like.col_lowered, lowered);
            }
        }
        for expr in [
            json_expr("$.missing", None),
            json_expr("$.message", Some(ScalarValue::Int64(None))),
        ] {
            assert!(FulltextIndexApplierBuilder::expr_target(&metadata, &expr).is_none());
        }
        assert!(
            FulltextIndexApplierBuilder::expr_target(
                &json_metadata(None),
                &json_expr("$.message", None)
            )
            .is_none()
        );
    }

    #[test]
    fn test_json_fulltext_sst_compatibility() {
        let (_dir, factory) = PuffinManagerFactory::new_for_test_block("json_fulltext_plan");
        let ordinary = IndexTarget::ColumnId(1);
        let path = IndexTarget::new_json_path(3, vec!["message".into()]).unwrap();
        let other = IndexTarget::new_json_path(3, vec!["other".into()]).unwrap();
        let missing = IndexTarget::new_json_path(3, vec!["missing".into()]).unwrap();
        for backend in [FulltextBackend::Bloom, FulltextBackend::Tantivy] {
            let options = FulltextOptions {
                enable: true,
                backend: backend.clone(),
                ..Default::default()
            };
            let metadata = json_metadata(Some(options.clone()));
            let requests = BTreeMap::from([
                (ordinary.clone(), FulltextRequest::default()),
                (path.clone(), FulltextRequest::default()),
                (other.clone(), FulltextRequest::default()),
            ]);
            let applier = FulltextIndexApplier::new(
                "test".into(),
                PathType::Bare,
                object_store::ObjectStore::new(object_store::services::Memory::default()).unwrap(),
                requests,
                factory.clone(),
                &metadata,
            );
            let unchanged = applier.for_sst(&metadata).unwrap();
            assert_eq!(unchanged.predicate_key(), applier.predicate_key());
            for change in 0..7 {
                let mut changed = options.clone();
                match change {
                    0 => changed.granularity += 1,
                    1 => changed.false_positive_rate_in_10000 += 1,
                    2 => changed.analyzer = FulltextAnalyzer::Chinese,
                    3 => changed.case_sensitive = true,
                    4 => {
                        changed.backend = if backend == FulltextBackend::Bloom {
                            FulltextBackend::Tantivy
                        } else {
                            FulltextBackend::Bloom
                        }
                    }
                    5 => changed.enable = false,
                    _ => {}
                }
                let sst = json_metadata((change != 6).then_some(changed));
                let plan = applier.for_sst(&sst).unwrap();
                assert!(plan.requests.contains_key(&ordinary));
                assert!(plan.requests.contains_key(&other));
                let compatible = change < 2 || (change == 2 && backend == FulltextBackend::Bloom);
                assert_eq!(
                    plan.requests.contains_key(&path),
                    compatible,
                    "{backend:?}, change={change}"
                );
                assert_eq!(plan.predicate_key() == applier.predicate_key(), compatible);
            }
            // An unresolved query-side target must not become compatible merely
            // because the expected options map has no entry for it.
            let applier = FulltextIndexApplier::new(
                "test".into(),
                PathType::Bare,
                object_store::ObjectStore::new(object_store::services::Memory::default()).unwrap(),
                BTreeMap::from([(missing.clone(), FulltextRequest::default())]),
                factory.clone(),
                &metadata,
            );
            assert!(applier.for_sst(&metadata).is_none());
            // Missing current options are also fail-closed even if the SST has them.
            let applier = FulltextIndexApplier::new(
                "test".into(),
                PathType::Bare,
                object_store::ObjectStore::new(object_store::services::Memory::default()).unwrap(),
                BTreeMap::from([(path.clone(), FulltextRequest::default())]),
                factory.clone(),
                &json_metadata(None),
            );
            assert!(applier.for_sst(&metadata).is_none());
        }
    }

    fn json_metadata(options: Option<FulltextOptions>) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::from_existing(mock_metadata());
        let mut column =
            ColumnSchema::new("attrs", ConcreteDataType::json2(Default::default()), true);
        column.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            JsonSettings::try_new(
                vec![
                    JsonTypeHint {
                        path: vec!["message".into()],
                        data_type: ConcreteDataType::string_datatype(),
                        inverted_index: false,
                        skipping_index: None,
                        fulltext_index: options,
                    },
                    JsonTypeHint {
                        path: vec!["other".into()],
                        data_type: ConcreteDataType::string_datatype(),
                        inverted_index: false,
                        skipping_index: None,
                        fulltext_index: Some(FulltextOptions {
                            enable: true,
                            ..Default::default()
                        }),
                    },
                ],
                Some(0),
            )
            .unwrap(),
        ))));
        builder.push_column_metadata(ColumnMetadata {
            column_schema: column,
            semantic_type: SemanticType::Field,
            column_id: 3,
        });
        builder.build().unwrap()
    }

    fn json_expr(path: &str, ty: Option<ScalarValue>) -> Expr {
        use common_function::scalars::json::json_get::JsonGetWithType;
        use common_function::scalars::udf::create_udf;
        let mut args = vec![datafusion_expr::col("attrs"), datafusion_expr::lit(path)];
        if let Some(ty) = ty {
            args.push(Expr::Literal(ty, None));
        }
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(create_udf(Arc::new(JsonGetWithType::default()))),
            args,
        ))
    }

    fn mock_metadata() -> RegionMetadata {
        mock_metadata_with_fulltext(FulltextBackend::Bloom)
    }

    fn mock_metadata_without_fulltext() -> RegionMetadata {
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
    fn test_ordinary_fulltext_requests_require_enabled_index() {
        for options in [
            None,
            Some(FulltextOptions::default()),
            Some(FulltextOptions {
                enable: true,
                ..Default::default()
            }),
            Some(FulltextOptions {
                enable: true,
                backend: FulltextBackend::Tantivy,
                ..Default::default()
            }),
        ] {
            let enabled = options.as_ref().is_some_and(|options| options.enable);
            let bloom_enabled = options
                .as_ref()
                .is_some_and(|options| options.enable && options.backend == FulltextBackend::Bloom);
            let mut metadata = mock_metadata_without_fulltext();
            if let Some(options) = options {
                metadata.column_metadatas[0]
                    .column_schema
                    .set_fulltext_options(&options)
                    .unwrap();
            }
            let column = datafusion_expr::col("text");
            let expressions = [
                (
                    matches_func().call(vec![column.clone(), "error".lit()]),
                    enabled,
                ),
                (
                    matches_term_func().call(vec![column.clone(), "error".lit()]),
                    enabled,
                ),
                (
                    matches_term_func()
                        .call(vec![lower().call(vec![column.clone()]), "error".lit()]),
                    enabled,
                ),
                (column.like("%error timeout retry%".lit()), bloom_enabled),
            ];
            for (expr, expected) in expressions {
                let mut requests = BTreeMap::new();
                FulltextIndexApplierBuilder::extract_requests(&expr, &metadata, &mut requests);
                assert_eq!(!requests.is_empty(), expected, "{expr}");
            }
        }
    }

    #[test]
    fn test_fulltext_string_literal_representations() {
        let metadata = mock_metadata();
        for literal in [
            ScalarValue::Utf8(Some("foo".into())),
            ScalarValue::LargeUtf8(Some("foo".into())),
            ScalarValue::Utf8View(Some("foo".into())),
        ] {
            let args = vec![
                Expr::Column(Column::from_name("text")),
                Expr::Literal(literal, None),
            ];
            let query = ScalarFunction {
                args: args.clone(),
                func: matches_func(),
            };
            assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &query).is_some());
            let term = ScalarFunction {
                args,
                func: matches_term_func(),
            };
            assert!(FulltextIndexApplierBuilder::expr_to_term(&metadata, &term).is_some());
        }
    }

    #[test]
    fn test_expr_to_query_basic() {
        let metadata = mock_metadata();

        let func = ScalarFunction {
            args: vec![Expr::Column(Column::from_name("text")), "foo".lit()],
            func: matches_func(),
        };

        let (target, query) = FulltextIndexApplierBuilder::expr_to_query(&metadata, &func).unwrap();
        assert_eq!(target, IndexTarget::ColumnId(1));
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

        let (target, term) = FulltextIndexApplierBuilder::expr_to_term(&metadata, &func).unwrap();
        assert_eq!(target, IndexTarget::ColumnId(1));
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

        let (target, term) = FulltextIndexApplierBuilder::expr_to_term(&metadata, &func).unwrap();
        assert_eq!(target, IndexTarget::ColumnId(1));
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
        let request = requests.get(&IndexTarget::ColumnId(1)).unwrap();
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
    fn test_expr_to_like() {
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
                FulltextIndexApplierBuilder::expr_to_like(
                    &bloom,
                    &like(false, false, escape_char, "%foo bar baz%")
                ),
                Some((
                    IndexTarget::ColumnId(1),
                    FulltextLike {
                        col_lowered: false,
                        pattern: "%foo bar baz%".to_string(),
                    }
                ))
            );
        }
        let lowered = Like {
            expr: Box::new(Expr::ScalarFunction(ScalarFunction {
                args: vec![Expr::Column(Column::from_name("text"))],
                func: lower(),
            })),
            ..like(false, false, None, "%foo bar baz%")
        };
        assert_eq!(
            FulltextIndexApplierBuilder::expr_to_like(&bloom, &lowered),
            Some((
                IndexTarget::ColumnId(1),
                FulltextLike {
                    col_lowered: true,
                    pattern: "%foo bar baz%".to_string(),
                }
            ))
        );
        // NOT LIKE, ILIKE and other escape characters can't use the probes, and
        // '%foo%' has none.
        for like in [
            like(true, false, None, "%foo bar baz%"),
            like(false, true, None, "%foo bar baz%"),
            like(false, false, Some('!'), "%foo bar baz%"),
            like(false, false, None, "%foo%"),
        ] {
            assert_eq!(
                FulltextIndexApplierBuilder::expr_to_like(&bloom, &like),
                None
            );
        }
        // Only columns with a bloom fulltext index.
        for metadata in [
            mock_metadata_without_fulltext(),
            mock_metadata_with_fulltext(FulltextBackend::Tantivy),
        ] {
            assert_eq!(
                FulltextIndexApplierBuilder::expr_to_like(
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
        let request = requests.get(&IndexTarget::ColumnId(1)).unwrap();
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
