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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::InList;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use index::bloom_filter::applier::{BloomFilterApplier, InListPredicate, Predicate};
use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::RowGroupMetaData;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{BlobGuard, PuffinManager, PuffinReader};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::{ColumnId, RegionId};

use super::INDEX_BLOB_TYPE;
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::bloom_filter_index::{
    BloomFilterIndexCacheRef, CachedBloomFilterIndexBlobReader,
};
use crate::error::{
    ApplyBloomFilterIndexSnafu, ColumnNotFoundSnafu, ConvertValueSnafu, MetadataSnafu,
    PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result,
};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::row_converter::SortField;
use crate::sst::file::FileId;
use crate::sst::index::codec::IndexValueCodec;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::TYPE_BLOOM_FILTER_INDEX;
use crate::sst::location;

pub(crate) type BloomFilterIndexApplierRef = Arc<BloomFilterIndexApplier>;

pub struct BloomFilterIndexApplier {
    region_dir: String,
    region_id: RegionId,
    object_store: ObjectStore,
    file_cache: Option<FileCacheRef>,
    puffin_manager_factory: PuffinManagerFactory,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    filters: HashMap<ColumnId, Vec<Predicate>>,
}

impl BloomFilterIndexApplier {
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        object_store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        filters: HashMap<ColumnId, Vec<Predicate>>,
    ) -> Self {
        Self {
            region_dir,
            region_id,
            object_store,
            file_cache: None,
            puffin_manager_factory,
            puffin_metadata_cache: None,
            bloom_filter_index_cache: None,
            filters,
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }

    /// Applies bloom filter predicates to the provided SST file and returns a bitmap
    /// indicating which segments may contain matching rows
    pub async fn apply(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
        row_group_metas: &[RowGroupMetaData],
        basement: &mut BTreeMap<usize, Option<RowSelection>>,
    ) -> Result<()> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_BLOOM_FILTER_INDEX])
            .start_timer();

        for (column_id, predicates) in &self.filters {
            let mut blob = match self.cached_blob_reader(file_id, *column_id).await {
                Ok(Some(puffin_reader)) => puffin_reader,
                other => {
                    if let Err(err) = other {
                        warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                    }
                    self.remote_blob_reader(file_id, *column_id, file_size_hint)
                        .await?
                }
            };

            // Create appropriate reader based on whether we have caching enabled
            if let Some(bloom_filter_cache) = &self.bloom_filter_index_cache {
                let file_size = if let Some(file_size) = file_size_hint {
                    file_size
                } else {
                    blob.metadata().await.context(MetadataSnafu)?.content_length
                };
                let reader = CachedBloomFilterIndexBlobReader::new(
                    file_id,
                    *column_id,
                    file_size,
                    BloomFilterReaderImpl::new(blob),
                    bloom_filter_cache.clone(),
                );
                self.apply_filters(reader, predicates, row_group_metas, basement)
                    .await
                    .context(ApplyBloomFilterIndexSnafu)?;
            } else {
                let reader = BloomFilterReaderImpl::new(blob);
                self.apply_filters(reader, predicates, row_group_metas, basement)
                    .await
                    .context(ApplyBloomFilterIndexSnafu)?;
            }
        }

        Ok(())
    }

    /// Creates a blob reader from the cached index file
    async fn cached_blob_reader(
        &self,
        file_id: FileId,
        column_id: ColumnId,
    ) -> Result<Option<BlobReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(self.region_id, file_id, FileType::Puffin);
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self.puffin_manager_factory.build(file_cache.local_store());
        let puffin_file_name = file_cache.cache_file_path(index_key);

        let reader = puffin_manager
            .reader(&puffin_file_name)
            .await
            .context(PuffinBuildReaderSnafu)?
            .blob(&Self::column_blob_name(column_id))
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)?;
        Ok(Some(reader))
    }

    // TODO(ruihang): use the same util with the code in creator
    fn column_blob_name(column_id: ColumnId) -> String {
        format!("{INDEX_BLOB_TYPE}-{column_id}")
    }

    /// Creates a blob reader from the remote index file
    async fn remote_blob_reader(
        &self,
        file_id: FileId,
        column_id: ColumnId,
        file_size_hint: Option<u64>,
    ) -> Result<BlobReader> {
        let puffin_manager = self
            .puffin_manager_factory
            .build(self.object_store.clone())
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let file_path = location::index_file_path(&self.region_dir, file_id);
        puffin_manager
            .reader(&file_path)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(&Self::column_blob_name(column_id))
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)
    }

    async fn apply_filters<R: BloomFilterReader + Send + 'static>(
        &self,
        reader: R,
        predicates: &[Predicate],
        row_group_metas: &[RowGroupMetaData],
        basement: &mut BTreeMap<usize, Option<RowSelection>>,
    ) -> std::result::Result<(), index::bloom_filter::error::Error> {
        let mut applier = BloomFilterApplier::new(Box::new(reader)).await?;

        for predicate in predicates {
            match predicate {
                Predicate::InList(in_list) => {
                    applier
                        .search(&in_list.list, row_group_metas, basement)
                        .await?;
                }
            }
        }

        Ok(())
    }
}

pub struct BloomFilterIndexApplierBuilder<'a> {
    region_dir: String,
    object_store: ObjectStore,
    metadata: &'a RegionMetadata,
    puffin_manager_factory: PuffinManagerFactory,
    file_cache: Option<FileCacheRef>,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    output: HashMap<ColumnId, Vec<Predicate>>,
}

impl<'a> BloomFilterIndexApplierBuilder<'a> {
    pub fn new(
        region_dir: String,
        object_store: ObjectStore,
        metadata: &'a RegionMetadata,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        Self {
            region_dir,
            object_store,
            metadata,
            puffin_manager_factory,
            file_cache: None,
            puffin_metadata_cache: None,
            bloom_filter_index_cache: None,
            output: HashMap::default(),
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_bloom_filter_index_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }

    /// Builds the applier with given filter expressions
    pub fn build(mut self, exprs: &[Expr]) -> Result<Option<BloomFilterIndexApplier>> {
        for expr in exprs {
            self.traverse_and_collect(expr);
        }

        if self.output.is_empty() {
            return Ok(None);
        }

        let applier = BloomFilterIndexApplier::new(
            self.region_dir,
            self.metadata.region_id,
            self.object_store,
            self.puffin_manager_factory,
            self.output,
        )
        .with_file_cache(self.file_cache)
        .with_puffin_metadata_cache(self.puffin_metadata_cache)
        .with_bloom_filter_cache(self.bloom_filter_index_cache);

        Ok(Some(applier))
    }

    /// Recursively traverses expressions to collect bloom filter predicates
    fn traverse_and_collect(&mut self, expr: &Expr) {
        let res = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    self.traverse_and_collect(left);
                    self.traverse_and_collect(right);
                    Ok(())
                }
                Operator::Eq => self.collect_eq(left, right),
                _ => Ok(()),
            },
            Expr::InList(in_list) => self.collect_in_list(in_list),
            _ => Ok(()),
        };

        if let Err(err) = res {
            warn!(err; "Failed to collect bloom filter predicates, ignore it. expr: {expr}");
        }
    }

    /// Helper function to get the column id and type
    fn column_id_and_type(
        &self,
        column_name: &str,
    ) -> Result<Option<(ColumnId, ConcreteDataType)>> {
        let column = self
            .metadata
            .column_by_name(column_name)
            .context(ColumnNotFoundSnafu {
                column: column_name,
            })?;

        Ok(Some((
            column.column_id,
            column.column_schema.data_type.clone(),
        )))
    }

    /// Collects an equality expression (column = value)
    fn collect_eq(&mut self, left: &Expr, right: &Expr) -> Result<()> {
        let (col, lit) = match (left, right) {
            (Expr::Column(col), Expr::Literal(lit)) => (col, lit),
            (Expr::Literal(lit), Expr::Column(col)) => (col, lit),
            _ => return Ok(()),
        };
        if lit.is_null() {
            return Ok(());
        }
        let Some((column_id, data_type)) = self.column_id_and_type(&col.name)? else {
            return Ok(());
        };
        let value = encode_lit(lit, data_type)?;

        // Create bloom filter predicate
        let mut set = HashSet::new();
        set.insert(value);
        let predicate = Predicate::InList(InListPredicate { list: set });

        // Add to output predicates
        self.output.entry(column_id).or_default().push(predicate);

        Ok(())
    }

    /// Collects an in list expression in the form of `column IN (lit, lit, ...)`.
    fn collect_in_list(&mut self, in_list: &InList) -> Result<()> {
        // Only collect InList predicates if they reference a column
        let Expr::Column(column) = &in_list.expr.as_ref() else {
            return Ok(());
        };
        if in_list.list.is_empty() || in_list.negated {
            return Ok(());
        }

        let Some((column_id, data_type)) = self.column_id_and_type(&column.name)? else {
            return Ok(());
        };

        // Convert all non-null literals to predicates
        let predicates = in_list
            .list
            .iter()
            .filter_map(Self::nonnull_lit)
            .map(|lit| encode_lit(lit, data_type.clone()));

        // Collect successful conversions
        let mut valid_predicates = HashSet::new();
        for predicate in predicates {
            match predicate {
                Ok(p) => {
                    valid_predicates.insert(p);
                }
                Err(e) => warn!(e; "Failed to convert value in InList"),
            }
        }

        if !valid_predicates.is_empty() {
            self.output
                .entry(column_id)
                .or_default()
                .push(Predicate::InList(InListPredicate {
                    list: valid_predicates,
                }));
        }

        Ok(())
    }

    /// Helper function to get non-null literal value
    fn nonnull_lit(expr: &Expr) -> Option<&ScalarValue> {
        match expr {
            Expr::Literal(lit) if !lit.is_null() => Some(lit),
            _ => None,
        }
    }
}

// TODO(ruihang): extract this and the one under inverted_index into a common util mod.
/// Helper function to encode a literal into bytes.
fn encode_lit(lit: &ScalarValue, data_type: ConcreteDataType) -> Result<Vec<u8>> {
    let value = Value::try_from(lit.clone()).context(ConvertValueSnafu)?;
    let mut bytes = vec![];
    let field = SortField::new(data_type);
    IndexValueCodec::encode_nonnull_value(value.as_value_ref(), &field, &mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion_common::Column;
    use datatypes::schema::ColumnSchema;
    use object_store::services::Memory;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};

    use super::*;

    fn test_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "column1",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "column2",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "column3",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    fn test_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn column(name: &str) -> Expr {
        Expr::Column(Column {
            relation: None,
            name: name.to_string(),
        })
    }

    fn string_lit(s: impl Into<String>) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.into())))
    }

    #[test]
    fn test_build_with_exprs() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_exprs_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(column("column1")),
            op: Operator::Eq,
            right: Box::new(string_lit("value1")),
        })];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let filters = result.unwrap().filters;
        assert_eq!(filters.len(), 1);

        let column_predicates = filters.get(&1).unwrap();
        assert_eq!(column_predicates.len(), 1);

        let expected = encode_lit(
            &ScalarValue::Utf8(Some("value1".to_string())),
            ConcreteDataType::string_datatype(),
        )
        .unwrap();
        match &column_predicates[0] {
            Predicate::InList(p) => {
                assert_eq!(p.list.iter().next().unwrap(), &expected);
            }
        }
    }

    fn int64_lit(i: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(i)))
    }

    #[test]
    fn test_build_with_in_list() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_in_list_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![Expr::InList(InList {
            expr: Box::new(column("column2")),
            list: vec![int64_lit(1), int64_lit(2), int64_lit(3)],
            negated: false,
        })];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let filters = result.unwrap().filters;
        let column_predicates = filters.get(&2).unwrap();
        assert_eq!(column_predicates.len(), 1);

        match &column_predicates[0] {
            Predicate::InList(p) => {
                assert_eq!(p.list.len(), 3);
            }
        }
    }

    #[test]
    fn test_build_with_and_expressions() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_and_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Eq,
                right: Box::new(string_lit("value1")),
            })),
            op: Operator::And,
            right: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column2")),
                op: Operator::Eq,
                right: Box::new(int64_lit(42)),
            })),
        })];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let filters = result.unwrap().filters;
        assert_eq!(filters.len(), 2);
        assert!(filters.contains_key(&1));
        assert!(filters.contains_key(&2));
    }

    #[test]
    fn test_build_with_null_values() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_null_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Utf8(None))),
            }),
            Expr::InList(InList {
                expr: Box::new(column("column2")),
                list: vec![
                    int64_lit(1),
                    Expr::Literal(ScalarValue::Int64(None)),
                    int64_lit(3),
                ],
                negated: false,
            }),
        ];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let filters = result.unwrap().filters;
        assert!(!filters.contains_key(&1)); // Null equality should be ignored
        let column2_predicates = filters.get(&2).unwrap();
        match &column2_predicates[0] {
            Predicate::InList(p) => {
                assert_eq!(p.list.len(), 2); // Only non-null values should be included
            }
        }
    }

    #[test]
    fn test_build_with_invalid_expressions() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_invalid_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![
            // Non-equality operator
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Gt,
                right: Box::new(string_lit("value1")),
            }),
            // Non-existent column
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("non_existent")),
                op: Operator::Eq,
                right: Box::new(string_lit("value")),
            }),
            // Negated IN list
            Expr::InList(InList {
                expr: Box::new(column("column2")),
                list: vec![int64_lit(1), int64_lit(2)],
                negated: true,
            }),
        ];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_build_with_multiple_predicates_same_column() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_build_with_multiple_");
        let metadata = test_region_metadata();
        let builder = BloomFilterIndexApplierBuilder::new(
            "test".to_string(),
            test_object_store(),
            &metadata,
            factory,
        );

        let exprs = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column("column1")),
                op: Operator::Eq,
                right: Box::new(string_lit("value1")),
            }),
            Expr::InList(InList {
                expr: Box::new(column("column1")),
                list: vec![string_lit("value2"), string_lit("value3")],
                negated: false,
            }),
        ];

        let result = builder.build(&exprs).unwrap();
        assert!(result.is_some());

        let filters = result.unwrap().filters;
        let column_predicates = filters.get(&1).unwrap();
        assert_eq!(column_predicates.len(), 2);
    }
}
