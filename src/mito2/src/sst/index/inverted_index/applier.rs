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

pub mod builder;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use datatypes::data_type::ConcreteDataType;
use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReadMetrics};
use index::inverted_index::search::index_apply::{
    ApplyOutput, IndexApplier, IndexNotFoundStrategy, PredicatesIndexApplier, SearchContext,
};
use index::inverted_index::search::predicate::Predicate;
use index::target::IndexTarget;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;
use store_api::storage::ColumnId;

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::inverted_index::{CachedInvertedIndexBlobReader, InvertedIndexCacheRef};
use crate::cache::index::result_cache::PredicateKey;
use crate::error::{
    ApplyInvertedIndexSnafu, BuildIndexApplierSnafu, MetadataSnafu, PuffinBuildReaderSnafu,
    PuffinReadBlobSnafu, Result,
};
use crate::metrics::{INDEX_APPLY_ELAPSED, INDEX_APPLY_MEMORY_USAGE};
use crate::sst::file::RegionIndexId;
use crate::sst::index::inverted_index::INDEX_BLOB_TYPE;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::{TYPE_INVERTED_INDEX, trigger_index_background_download};

/// Metrics for tracking inverted index apply operations.
#[derive(Default, Clone)]
pub struct InvertedIndexApplyMetrics {
    /// Total time spent applying the index.
    pub apply_elapsed: std::time::Duration,
    /// Number of blob cache misses (0 or 1).
    pub blob_cache_miss: usize,
    /// Total size of blobs read (in bytes).
    pub blob_read_bytes: u64,
    /// Metrics for inverted index reads.
    pub inverted_index_read_metrics: InvertedIndexReadMetrics,
}

impl std::fmt::Debug for InvertedIndexApplyMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            apply_elapsed,
            blob_cache_miss,
            blob_read_bytes,
            inverted_index_read_metrics,
        } = self;

        if self.is_empty() {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;

        write!(f, "\"apply_elapsed\":\"{:?}\"", apply_elapsed)?;

        if *blob_cache_miss > 0 {
            write!(f, ", \"blob_cache_miss\":{}", blob_cache_miss)?;
        }
        if *blob_read_bytes > 0 {
            write!(f, ", \"blob_read_bytes\":{}", blob_read_bytes)?;
        }
        write!(
            f,
            ", \"inverted_index_read_metrics\":{:?}",
            inverted_index_read_metrics
        )?;

        write!(f, "}}")
    }
}

impl InvertedIndexApplyMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub fn is_empty(&self) -> bool {
        self.apply_elapsed.is_zero()
    }

    /// Merges another metrics into this one.
    pub fn merge_from(&mut self, other: &Self) {
        self.apply_elapsed += other.apply_elapsed;
        self.blob_cache_miss += other.blob_cache_miss;
        self.blob_read_bytes += other.blob_read_bytes;
        self.inverted_index_read_metrics
            .merge_from(&other.inverted_index_read_metrics);
    }
}

/// `InvertedIndexApplier` is responsible for applying predicates to the provided SST files
/// and returning the relevant row group ids for further scan.
pub(crate) struct InvertedIndexApplier {
    /// The root directory of the table.
    table_dir: String,

    /// Path type for generating file paths.
    path_type: PathType,

    /// Store responsible for accessing remote index files.
    store: ObjectStore,

    /// The cache of index files.
    file_cache: Option<FileCacheRef>,

    /// The puffin manager factory.
    puffin_manager_factory: PuffinManagerFactory,

    /// In-memory cache for inverted index.
    inverted_index_cache: Option<InvertedIndexCacheRef>,

    /// Puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,

    /// All collected predicates.
    predicates: BTreeMap<ColumnId, Vec<Predicate>>,

    /// Default apply plan built from all collected predicates.
    default_plan: SstApplyPlan,

    /// Expected predicate column types from the latest region metadata.
    expected_predicate_col_types: BTreeMap<ColumnId, ConcreteDataType>,
}

pub(crate) type InvertedIndexApplierRef = Arc<InvertedIndexApplier>;

#[derive(Clone)]
pub(crate) struct SstApplyPlan {
    pub predicate_key: PredicateKey,
    pub index_applier: Arc<PredicatesIndexApplier>,
}

impl InvertedIndexApplier {
    /// Creates a new `InvertedIndexApplier`.
    pub fn new(
        table_dir: String,
        path_type: PathType,
        store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        predicates: BTreeMap<ColumnId, Vec<Predicate>>,
        expected_predicate_col_types: BTreeMap<ColumnId, ConcreteDataType>,
    ) -> Result<Self> {
        let default_plan = Self::build_apply_plan(&predicates)?;
        INDEX_APPLY_MEMORY_USAGE.add(default_plan.index_applier.memory_usage() as i64);

        Ok(Self {
            table_dir,
            path_type,
            store,
            file_cache: None,
            puffin_manager_factory,
            inverted_index_cache: None,
            puffin_metadata_cache: None,
            predicates,
            default_plan,
            expected_predicate_col_types,
        })
    }

    /// Sets the file cache.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    /// Sets the index cache.
    pub fn with_index_cache(mut self, index_cache: Option<InvertedIndexCacheRef>) -> Self {
        self.inverted_index_cache = index_cache;
        self
    }

    /// Sets the puffin metadata cache.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    /// Applies predicates to one SST file with the provided index applier.
    ///
    /// # Arguments
    /// * `file_id` - The region file ID to apply predicates to
    /// * `file_size_hint` - Optional hint for file size to avoid extra metadata reads
    /// * `index_applier` - Inverted index applier produced by `plan_for_sst`.
    /// * `metrics` - Optional mutable reference to collect metrics on demand
    #[tracing::instrument(
        skip_all,
        fields(file_id = %file_id)
    )]
    pub async fn apply(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        index_applier: &PredicatesIndexApplier,
        mut metrics: Option<&mut InvertedIndexApplyMetrics>,
    ) -> Result<ApplyOutput> {
        let start = Instant::now();

        let context = SearchContext {
            // Encountering a non-existing column indicates that it doesn't match predicates.
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };

        let mut cache_miss = 0;
        let blob = match self.cached_blob_reader(file_id, file_size_hint).await {
            Ok(Some(puffin_reader)) => puffin_reader,
            other => {
                cache_miss += 1;
                if let Err(err) = other {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                }
                self.remote_blob_reader(file_id, file_size_hint).await?
            }
        };

        let blob_size = blob.metadata().await.context(MetadataSnafu)?.content_length;

        let result = if let Some(index_cache) = &self.inverted_index_cache {
            let mut index_reader = CachedInvertedIndexBlobReader::new(
                file_id.file_id(),
                file_id.version,
                blob_size,
                InvertedIndexBlobReader::new(blob),
                index_cache.clone(),
            );
            index_applier
                .apply(
                    context,
                    &mut index_reader,
                    metrics
                        .as_deref_mut()
                        .map(|m| &mut m.inverted_index_read_metrics),
                )
                .await
                .context(ApplyInvertedIndexSnafu)
        } else {
            let mut index_reader = InvertedIndexBlobReader::new(blob);
            index_applier
                .apply(
                    context,
                    &mut index_reader,
                    metrics
                        .as_deref_mut()
                        .map(|m| &mut m.inverted_index_read_metrics),
                )
                .await
                .context(ApplyInvertedIndexSnafu)
        };

        // Record elapsed time to histogram and collect metrics if requested
        let elapsed = start.elapsed();
        INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_INVERTED_INDEX])
            .observe(elapsed.as_secs_f64());

        if let Some(metrics) = metrics {
            metrics.apply_elapsed = elapsed;
            metrics.blob_cache_miss = cache_miss;
            metrics.blob_read_bytes = blob_size;
        }

        result
    }

    /// Creates a blob reader from the cached index file.
    async fn cached_blob_reader(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(
            file_id.region_id(),
            file_id.file_id(),
            FileType::Puffin(file_id.version),
        );
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self.puffin_manager_factory.build(
            file_cache.local_store(),
            WriteCachePathProvider::new(file_cache.clone()),
        );

        // Adds file size hint to the puffin reader to avoid extra metadata read.
        let reader = puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(INDEX_BLOB_TYPE)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)?;
        Ok(Some(reader))
    }

    /// Creates a blob reader from the remote index file.
    async fn remote_blob_reader(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<BlobReader> {
        let path_factory = RegionFilePathFactory::new(self.table_dir.clone(), self.path_type);

        // Trigger background download if file cache and file size are available
        trigger_index_background_download(
            self.file_cache.as_ref(),
            &file_id,
            file_size_hint,
            &path_factory,
            &self.store,
        );

        let puffin_manager = self
            .puffin_manager_factory
            .build(self.store.clone(), path_factory)
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(INDEX_BLOB_TYPE)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)
    }

    /// Builds a per-SST apply plan.
    ///
    /// Returns `None` when no compatible predicate remains for this SST.
    pub fn plan_for_sst(&self, sst_metadata: &RegionMetadataRef) -> Result<Option<SstApplyPlan>> {
        // Borrowed first pass: only detect whether some predicate column is
        // incompatible with this SST and whether any compatible predicate
        // remains. Nothing is cloned here, so no map is allocated when the SST
        // is fully compatible.
        let mut has_type_mismatch = false;
        let mut has_compatible_predicate = false;

        for (col_id, expected) in &self.expected_predicate_col_types {
            // A column missing from the SST is compatible: the index lookup
            // reports it as not found instead of mismatching the predicate.
            if let Some(sst_col) = sst_metadata.column_by_id(*col_id)
                && sst_col.column_schema.data_type != *expected
            {
                has_type_mismatch = true;
                continue;
            }

            // The predicate map entry is what makes a column applicable, so an
            // entry holding an empty predicate list still counts.
            if self.predicates.contains_key(col_id) {
                has_compatible_predicate = true;
            }
        }

        if !has_compatible_predicate {
            return Ok(None);
        }

        if !has_type_mismatch {
            return Ok(Some(self.default_plan.clone()));
        }

        // Slow path: build a plan from the compatible predicates only. Cloning
        // the predicate lists happens here, not on the fully compatible path.
        let compatible_predicates = self
            .expected_predicate_col_types
            .iter()
            .filter_map(|(col_id, expected)| {
                if let Some(sst_col) = sst_metadata.column_by_id(*col_id)
                    && sst_col.column_schema.data_type != *expected
                {
                    return None;
                }

                self.predicates
                    .get(col_id)
                    .map(|predicates| (*col_id, predicates.clone()))
            })
            .collect();

        let plan = Self::build_apply_plan(&compatible_predicates)?;
        Ok(Some(plan))
    }

    fn build_apply_plan(
        predicates_by_col: &BTreeMap<ColumnId, Vec<Predicate>>,
    ) -> Result<SstApplyPlan> {
        let predicates = predicates_by_col
            .iter()
            .map(|(col_id, preds)| (format!("{}", IndexTarget::ColumnId(*col_id)), preds.clone()))
            .collect();

        let index_applier =
            PredicatesIndexApplier::try_from(predicates).context(BuildIndexApplierSnafu)?;

        let predicate_key = PredicateKey::new_inverted(Arc::new(predicates_by_col.clone()));
        Ok(SstApplyPlan {
            predicate_key,
            index_applier: Arc::new(index_applier),
        })
    }
}

impl Drop for InvertedIndexApplier {
    fn drop(&mut self) {
        INDEX_APPLY_MEMORY_USAGE.sub(self.default_plan.index_applier.memory_usage() as i64);
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use futures::io::Cursor;
    use index::inverted_index::search::predicate::RegexMatchPredicate;
    use object_store::services::Memory;
    use puffin::puffin_manager::PuffinWriter;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::sst::index::RegionFileId;

    #[tokio::test]
    async fn test_plan_for_sst() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_plan_for_sst_basic_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let table_dir = "table_dir".to_string();

        let mut predicates = BTreeMap::new();
        predicates.insert(
            1,
            vec![Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "foo".to_string(),
            })],
        );
        let expected_predicate_col_types =
            BTreeMap::from_iter([(1, ConcreteDataType::string_datatype())]);

        let sst_index_applier = InvertedIndexApplier::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            predicates,
            expected_predicate_col_types,
        )
        .unwrap();
        let plan = sst_index_applier
            .plan_for_sst(&mock_region_metadata())
            .unwrap();
        assert!(plan.is_some());
    }

    #[tokio::test]
    async fn test_plan_for_sst_type_mismatch() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_plan_for_sst_type_mismatch_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let table_dir = "table_dir".to_string();

        let mut predicates = BTreeMap::new();
        predicates.insert(
            1,
            vec![Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "foo".to_string(),
            })],
        );
        // Column id 1 is String in `mock_region_metadata`, set expected type to Int64.
        let expected_predicate_col_types =
            BTreeMap::from_iter([(1, ConcreteDataType::int64_datatype())]);

        let sst_index_applier = InvertedIndexApplier::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            predicates,
            expected_predicate_col_types,
        )
        .unwrap();
        let plan = sst_index_applier
            .plan_for_sst(&mock_region_metadata())
            .unwrap();
        assert!(plan.is_none());
    }

    /// Expected outcome of [`InvertedIndexApplier::plan_for_sst`] for one case.
    enum ExpectedPlan {
        /// No compatible predicate remains, so no plan is built.
        NoPlan,
        /// The default plan is reused as is, sharing the same applier.
        DefaultPlan,
        /// A plan is built from the compatible predicates only.
        Filtered(BTreeMap<ColumnId, Vec<Predicate>>),
    }

    /// One case of [`InvertedIndexApplier::plan_for_sst`].
    struct PlanForSstCase {
        name: &'static str,
        /// Predicates collected for the query.
        predicates: BTreeMap<ColumnId, Vec<Predicate>>,
        /// Expected predicate column types from the latest region metadata.
        expected_col_types: BTreeMap<ColumnId, ConcreteDataType>,
        expected: ExpectedPlan,
    }

    fn regex_match_predicate(pattern: &str) -> Predicate {
        Predicate::RegexMatch(RegexMatchPredicate {
            pattern: pattern.to_string(),
        })
    }

    #[tokio::test]
    async fn test_plan_for_sst_cases() {
        // `mock_region_metadata` holds column 1 (String tag) and column 2
        // (TimestampMillisecond).
        let sst_metadata = mock_region_metadata();
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_plan_for_sst_cases_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();

        let cases = vec![
            PlanForSstCase {
                name: "fully compatible reuses the default plan",
                predicates: BTreeMap::from_iter([(1, vec![regex_match_predicate("foo")])]),
                expected_col_types: BTreeMap::from_iter([(1, ConcreteDataType::string_datatype())]),
                expected: ExpectedPlan::DefaultPlan,
            },
            PlanForSstCase {
                name: "missing sst column is compatible",
                predicates: BTreeMap::from_iter([(3, vec![regex_match_predicate("foo")])]),
                expected_col_types: BTreeMap::from_iter([(3, ConcreteDataType::string_datatype())]),
                expected: ExpectedPlan::DefaultPlan,
            },
            PlanForSstCase {
                name: "present empty predicate list counts as compatible",
                predicates: BTreeMap::from_iter([(1, vec![])]),
                expected_col_types: BTreeMap::from_iter([(1, ConcreteDataType::string_datatype())]),
                expected: ExpectedPlan::DefaultPlan,
            },
            PlanForSstCase {
                name: "empty predicate map yields no plan",
                predicates: BTreeMap::new(),
                expected_col_types: BTreeMap::new(),
                expected: ExpectedPlan::NoPlan,
            },
            PlanForSstCase {
                name: "expected column without predicate entry yields no plan",
                predicates: BTreeMap::new(),
                expected_col_types: BTreeMap::from_iter([(1, ConcreteDataType::string_datatype())]),
                expected: ExpectedPlan::NoPlan,
            },
            PlanForSstCase {
                name: "fully mismatched yields no plan",
                predicates: BTreeMap::from_iter([(1, vec![regex_match_predicate("foo")])]),
                expected_col_types: BTreeMap::from_iter([(1, ConcreteDataType::int64_datatype())]),
                expected: ExpectedPlan::NoPlan,
            },
            PlanForSstCase {
                name: "partial mismatch keeps compatible predicates",
                predicates: BTreeMap::from_iter([
                    (1, vec![regex_match_predicate("foo")]),
                    (2, vec![regex_match_predicate("bar")]),
                ]),
                expected_col_types: BTreeMap::from_iter([
                    (1, ConcreteDataType::string_datatype()),
                    (2, ConcreteDataType::timestamp_second_datatype()),
                ]),
                expected: ExpectedPlan::Filtered(BTreeMap::from_iter([(
                    1,
                    vec![regex_match_predicate("foo")],
                )])),
            },
            PlanForSstCase {
                name: "mismatch without predicate entry rebuilds the plan",
                predicates: BTreeMap::from_iter([(1, vec![regex_match_predicate("foo")])]),
                expected_col_types: BTreeMap::from_iter([
                    (1, ConcreteDataType::string_datatype()),
                    (2, ConcreteDataType::timestamp_second_datatype()),
                ]),
                expected: ExpectedPlan::Filtered(BTreeMap::from_iter([(
                    1,
                    vec![regex_match_predicate("foo")],
                )])),
            },
            PlanForSstCase {
                name: "mismatch keeps compatible missing column",
                predicates: BTreeMap::from_iter([
                    (2, vec![regex_match_predicate("bar")]),
                    (3, vec![regex_match_predicate("baz")]),
                ]),
                expected_col_types: BTreeMap::from_iter([
                    (2, ConcreteDataType::timestamp_second_datatype()),
                    (3, ConcreteDataType::string_datatype()),
                ]),
                expected: ExpectedPlan::Filtered(BTreeMap::from_iter([(
                    3,
                    vec![regex_match_predicate("baz")],
                )])),
            },
        ];

        for case in cases {
            let PlanForSstCase {
                name,
                predicates,
                expected_col_types,
                expected,
            } = case;

            let applier = InvertedIndexApplier::new(
                "table_dir".to_string(),
                PathType::Bare,
                object_store.clone(),
                puffin_manager_factory.clone(),
                predicates,
                expected_col_types,
            )
            .unwrap();

            let plan = applier.plan_for_sst(&sst_metadata).unwrap();

            match expected {
                ExpectedPlan::NoPlan => {
                    assert!(plan.is_none(), "case [{name}] expects no plan");
                }
                ExpectedPlan::DefaultPlan => {
                    let Some(plan) = plan else {
                        panic!("case [{name}] expects a plan");
                    };
                    assert!(
                        Arc::ptr_eq(&plan.index_applier, &applier.default_plan.index_applier),
                        "case [{name}] expects the default plan applier to be reused"
                    );
                    assert_eq!(
                        plan.predicate_key, applier.default_plan.predicate_key,
                        "case [{name}] expects the default predicate key"
                    );
                }
                ExpectedPlan::Filtered(compatible) => {
                    let Some(plan) = plan else {
                        panic!("case [{name}] expects a plan");
                    };
                    assert!(
                        !Arc::ptr_eq(&plan.index_applier, &applier.default_plan.index_applier),
                        "case [{name}] expects a plan rebuilt from compatible predicates"
                    );
                    assert_eq!(
                        plan.predicate_key,
                        PredicateKey::new_inverted(Arc::new(compatible)),
                        "case [{name}] expects only compatible predicates"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_index_applier_apply_invalid_blob_type() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_index_applier_apply_invalid_blob_type_")
                .await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let table_dir = "table_dir".to_string();

        let puffin_manager = puffin_manager_factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );
        let mut writer = puffin_manager.writer(&index_id).await.unwrap();
        writer
            .put_blob(
                "invalid_blob_type",
                Cursor::new(vec![]),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut predicates = BTreeMap::new();
        predicates.insert(
            1,
            vec![Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "foo".to_string(),
            })],
        );
        let expected_predicate_col_types =
            BTreeMap::from_iter([(1, ConcreteDataType::string_datatype())]);
        let sst_index_applier = InvertedIndexApplier::new(
            table_dir.clone(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            predicates,
            expected_predicate_col_types,
        )
        .unwrap();
        let plan = sst_index_applier
            .plan_for_sst(&mock_region_metadata())
            .unwrap()
            .unwrap();
        let res = sst_index_applier
            .apply(index_id, None, &plan.index_applier, None)
            .await;
        assert!(format!("{:?}", res.unwrap_err()).contains("Blob not found"));
    }

    fn mock_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1024, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
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
            })
            .primary_key(vec![1]);
        Arc::new(builder.build().unwrap())
    }
}
