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
use datafusion_expr::Expr;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use store_api::metadata::RegionMetadata;
use store_api::storage::{ColumnId, ConcreteDataType, RegionId};

use crate::cache::file_cache::FileCacheRef;
use crate::error::Result;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
use crate::sst::index::puffin_manager::PuffinManagerFactory;

/// `FulltextIndexApplierBuilder` is a builder for `FulltextIndexApplier`.
pub struct FulltextIndexApplierBuilder<'a> {
    region_dir: String,
    region_id: RegionId,
    store: ObjectStore,
    puffin_manager_factory: PuffinManagerFactory,
    metadata: &'a RegionMetadata,
    file_cache: Option<FileCacheRef>,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
}

impl<'a> FulltextIndexApplierBuilder<'a> {
    /// Creates a new `FulltextIndexApplierBuilder`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        metadata: &'a RegionMetadata,
    ) -> Self {
        Self {
            region_dir,
            region_id,
            store,
            puffin_manager_factory,
            metadata,
            file_cache: None,
            puffin_metadata_cache: None,
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

    /// Builds `SstIndexApplier` from the given expressions.
    pub fn build(self, exprs: &[Expr]) -> Result<Option<FulltextIndexApplier>> {
        let mut queries = Vec::with_capacity(exprs.len());
        for expr in exprs {
            if let Some((column_id, query)) = Self::expr_to_query(self.metadata, expr) {
                queries.push((column_id, query));
            }
        }

        Ok((!queries.is_empty()).then(|| {
            FulltextIndexApplier::new(
                self.region_dir,
                self.region_id,
                self.store,
                queries,
                self.puffin_manager_factory,
            )
            .with_file_cache(self.file_cache)
            .with_puffin_metadata_cache(self.puffin_metadata_cache)
        }))
    }

    fn expr_to_query(metadata: &RegionMetadata, expr: &Expr) -> Option<(ColumnId, String)> {
        let Expr::ScalarFunction(f) = expr else {
            return None;
        };
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

        let Expr::Literal(ScalarValue::Utf8(Some(query))) = &f.args[1] else {
            return None;
        };

        Some((column.column_id, query.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::function_registry::FUNCTION_REGISTRY;
    use common_function::scalars::udf::create_udf;
    use datafusion_common::Column;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::ScalarUDF;
    use datatypes::schema::ColumnSchema;
    use session::context::QueryContext;
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
            create_udf(
                FUNCTION_REGISTRY.get_function("matches").unwrap(),
                QueryContext::arc(),
                Default::default(),
            )
            .into(),
        )
    }

    #[test]
    fn test_expr_to_query_basic() {
        let metadata = mock_metadata();

        let expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![
                Expr::Column(Column {
                    name: "text".to_string(),
                    relation: None,
                }),
                Expr::Literal(ScalarValue::Utf8(Some("foo".to_string()))),
            ],
            func: matches_func(),
        });

        let (column_id, query) =
            FulltextIndexApplierBuilder::expr_to_query(&metadata, &expr).unwrap();
        assert_eq!(column_id, 1);
        assert_eq!(query, "foo".to_string());
    }

    #[test]
    fn test_expr_to_query_wrong_num_args() {
        let metadata = mock_metadata();

        let expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![Expr::Column(Column {
                name: "text".to_string(),
                relation: None,
            })],
            func: matches_func(),
        });

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &expr).is_none());
    }

    #[test]
    fn test_expr_to_query_not_found_column() {
        let metadata = mock_metadata();

        let expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![
                Expr::Column(Column {
                    name: "not_found".to_string(),
                    relation: None,
                }),
                Expr::Literal(ScalarValue::Utf8(Some("foo".to_string()))),
            ],
            func: matches_func(),
        });

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &expr).is_none());
    }

    #[test]
    fn test_expr_to_query_column_wrong_data_type() {
        let metadata = mock_metadata();

        let expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![
                Expr::Column(Column {
                    name: "ts".to_string(),
                    relation: None,
                }),
                Expr::Literal(ScalarValue::Utf8(Some("foo".to_string()))),
            ],
            func: matches_func(),
        });

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &expr).is_none());
    }

    #[test]
    fn test_expr_to_query_pattern_not_string() {
        let metadata = mock_metadata();

        let expr = Expr::ScalarFunction(ScalarFunction {
            args: vec![
                Expr::Column(Column {
                    name: "text".to_string(),
                    relation: None,
                }),
                Expr::Literal(ScalarValue::Int64(Some(42))),
            ],
            func: matches_func(),
        });

        assert!(FulltextIndexApplierBuilder::expr_to_query(&metadata, &expr).is_none());
    }
}
