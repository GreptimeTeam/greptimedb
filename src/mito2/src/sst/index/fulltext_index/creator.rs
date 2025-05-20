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

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_telemetry::warn;
use datatypes::schema::{FulltextAnalyzer, FulltextBackend};
use index::fulltext_index::create::{
    BloomFilterFulltextIndexCreator, FulltextIndexCreator, TantivyFulltextIndexCreator,
};
use index::fulltext_index::{Analyzer, Config};
use puffin::blob_metadata::CompressionCodec;
use puffin::puffin_manager::PutOptions;
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, ConcreteDataType, RegionId};

use crate::error::{
    CastVectorSnafu, CreateFulltextCreatorSnafu, FieldTypeMismatchSnafu, FulltextFinishSnafu,
    FulltextPushTextSnafu, IndexOptionsSnafu, OperateAbortedIndexSnafu, Result,
};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::{INDEX_BLOB_TYPE_BLOOM, INDEX_BLOB_TYPE_TANTIVY};
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};
use crate::sst::index::TYPE_FULLTEXT_INDEX;

/// `FulltextIndexer` is responsible for creating fulltext indexes for SST files.
pub struct FulltextIndexer {
    /// Creators for each column.
    creators: HashMap<ColumnId, SingleCreator>,
    /// Whether the index creation was aborted.
    aborted: bool,
    /// Statistics of index creation.
    stats: Statistics,
}

impl FulltextIndexer {
    /// Creates a new `FulltextIndexer`.
    pub async fn new(
        region_id: &RegionId,
        sst_file_id: &FileId,
        intermediate_manager: &IntermediateManager,
        metadata: &RegionMetadataRef,
        compress: bool,
        bloom_row_granularity: usize,
        mem_limit: usize,
    ) -> Result<Option<Self>> {
        let mut creators = HashMap::new();

        for column in &metadata.column_metadatas {
            let options = column
                .column_schema
                .fulltext_options()
                .context(IndexOptionsSnafu {
                    column_name: &column.column_schema.name,
                })?;

            // Relax the type constraint here as many types can be casted to string.

            let options = match options {
                Some(options) if options.enable => options,
                _ => continue,
            };

            let column_id = column.column_id;
            let intm_path = intermediate_manager.fulltext_path(region_id, sst_file_id, column_id);

            let config = Config {
                analyzer: match options.analyzer {
                    FulltextAnalyzer::English => Analyzer::English,
                    FulltextAnalyzer::Chinese => Analyzer::Chinese,
                },
                case_sensitive: options.case_sensitive,
            };

            let inner = match options.backend {
                FulltextBackend::Tantivy => {
                    let creator = TantivyFulltextIndexCreator::new(&intm_path, config, mem_limit)
                        .await
                        .context(CreateFulltextCreatorSnafu)?;
                    AltFulltextCreator::Tantivy(creator)
                }
                FulltextBackend::Bloom => {
                    let temp_file_provider = Arc::new(TempFileProvider::new(
                        IntermediateLocation::new(&metadata.region_id, sst_file_id),
                        intermediate_manager.clone(),
                    ));
                    let global_memory_usage = Arc::new(AtomicUsize::new(0));
                    let creator = BloomFilterFulltextIndexCreator::new(
                        config,
                        bloom_row_granularity,
                        temp_file_provider,
                        global_memory_usage,
                        Some(mem_limit),
                    );
                    AltFulltextCreator::Bloom(creator)
                }
            };

            creators.insert(
                column_id,
                SingleCreator {
                    column_id,
                    inner,
                    compress,
                },
            );
        }

        Ok((!creators.is_empty()).then(move || Self {
            creators,
            aborted: false,
            stats: Statistics::new(TYPE_FULLTEXT_INDEX),
        }))
    }

    /// Updates the index with the given batch.
    pub async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if let Err(update_err) = self.do_update(batch).await {
            if let Err(err) = self.do_abort().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to abort index creator, err: {err}");
                } else {
                    warn!(err; "Failed to abort index creator");
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Finalizes the index creation.
    pub async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        match self.do_finish(puffin_writer).await {
            Ok(()) => Ok((self.stats.row_count(), self.stats.byte_count())),
            Err(finish_err) => {
                if let Err(err) = self.do_abort().await {
                    if cfg!(any(test, feature = "test")) {
                        panic!("Failed to abort index creator, err: {err}");
                    } else {
                        warn!(err; "Failed to abort index creator");
                    }
                }
                Err(finish_err)
            }
        }
    }

    /// Aborts the index creation.
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }

        self.do_abort().await
    }

    /// Returns the memory usage of the index creator.
    pub fn memory_usage(&self) -> usize {
        self.creators.values().map(|c| c.inner.memory_usage()).sum()
    }

    /// Returns IDs of columns that the creator is responsible for.
    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.creators.keys().copied()
    }
}

impl FulltextIndexer {
    async fn do_update(&mut self, batch: &mut Batch) -> Result<()> {
        let mut guard = self.stats.record_update();
        guard.inc_row_count(batch.num_rows());

        for creator in self.creators.values_mut() {
            creator.update(batch).await?;
        }

        Ok(())
    }

    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let mut written_bytes = 0;
        for creator in self.creators.values_mut() {
            written_bytes += creator.finish(puffin_writer).await?;
        }

        guard.inc_byte_count(written_bytes);
        Ok(())
    }

    async fn do_abort(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.aborted = true;

        for (_, mut creator) in self.creators.drain() {
            creator.abort().await?;
        }

        Ok(())
    }
}

/// `SingleCreator` is a creator for a single column.
struct SingleCreator {
    /// Column ID.
    column_id: ColumnId,
    /// Inner creator.
    inner: AltFulltextCreator,
    /// Whether the index should be compressed.
    compress: bool,
}

impl SingleCreator {
    async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        let text_column = batch
            .fields()
            .iter()
            .find(|c| c.column_id == self.column_id);
        match text_column {
            Some(column) => {
                let data = column
                    .data
                    .cast(&ConcreteDataType::string_datatype())
                    .context(CastVectorSnafu {
                        from: column.data.data_type(),
                        to: ConcreteDataType::string_datatype(),
                    })?;

                for i in 0..batch.num_rows() {
                    let data = data.get_ref(i);
                    let text = data
                        .as_string()
                        .context(FieldTypeMismatchSnafu)?
                        .unwrap_or_default();
                    self.inner.push_text(text).await?;
                }
            }
            _ => {
                // If the column is not found in the batch, push empty text.
                // Ensure that the number of texts pushed is the same as the number of rows in the SST,
                // so that the texts are aligned with the row ids.
                for _ in 0..batch.num_rows() {
                    self.inner.push_text("").await?;
                }
            }
        }

        Ok(())
    }

    async fn finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<ByteCount> {
        let options = PutOptions {
            compression: self.compress.then_some(CompressionCodec::Zstd),
        };
        self.inner
            .finish(puffin_writer, &self.column_id, options)
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort(&self.column_id).await;
        Ok(())
    }
}

#[allow(dead_code, clippy::large_enum_variant)]
/// `AltFulltextCreator` is an alternative fulltext index creator that can be either Tantivy or BloomFilter.
enum AltFulltextCreator {
    Tantivy(TantivyFulltextIndexCreator),
    Bloom(BloomFilterFulltextIndexCreator),
}

impl AltFulltextCreator {
    async fn push_text(&mut self, text: &str) -> Result<()> {
        match self {
            Self::Tantivy(creator) => creator.push_text(text).await.context(FulltextPushTextSnafu),
            Self::Bloom(creator) => creator.push_text(text).await.context(FulltextPushTextSnafu),
        }
    }

    fn memory_usage(&self) -> usize {
        match self {
            Self::Tantivy(creator) => creator.memory_usage(),
            Self::Bloom(creator) => creator.memory_usage(),
        }
    }

    async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        column_id: &ColumnId,
        put_options: PutOptions,
    ) -> Result<ByteCount> {
        match self {
            Self::Tantivy(creator) => {
                let key = format!("{INDEX_BLOB_TYPE_TANTIVY}-{}", column_id);
                creator
                    .finish(puffin_writer, &key, put_options)
                    .await
                    .context(FulltextFinishSnafu)
            }
            Self::Bloom(creator) => {
                let key = format!("{INDEX_BLOB_TYPE_BLOOM}-{}", column_id);
                creator
                    .finish(puffin_writer, &key, put_options)
                    .await
                    .context(FulltextFinishSnafu)
            }
        }
    }

    async fn abort(&mut self, column_id: &ColumnId) {
        match self {
            Self::Tantivy(creator) => {
                if let Err(err) = creator.abort().await {
                    warn!(err; "Failed to abort the fulltext index creator in the Tantivy flavor, col_id: {:?}", column_id);
                }
            }
            Self::Bloom(creator) => {
                if let Err(err) = creator.abort().await {
                    warn!(err; "Failed to abort the fulltext index creator in the Bloom Filter flavor, col_id: {:?}", column_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_base::BitVec;
    use datatypes::data_type::DataType;
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextOptions};
    use datatypes::vectors::{UInt64Vector, UInt8Vector};
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use index::fulltext_index::search::RowId;
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use puffin::puffin_manager::{PuffinManager, PuffinWriter};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::{ConcreteDataType, RegionId};

    use super::*;
    use crate::access_layer::RegionFilePathFactory;
    use crate::read::{Batch, BatchColumn};
    use crate::sst::file::FileId;
    use crate::sst::index::fulltext_index::applier::builder::{
        FulltextQuery, FulltextRequest, FulltextTerm,
    };
    use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    async fn new_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }

    fn mock_region_metadata(backend: FulltextBackend) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "text_english_case_sensitive",
                    ConcreteDataType::string_datatype(),
                    true,
                )
                .with_fulltext_options(FulltextOptions {
                    enable: true,
                    analyzer: FulltextAnalyzer::English,
                    case_sensitive: true,
                    backend: backend.clone(),
                })
                .unwrap(),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "text_english_case_insensitive",
                    ConcreteDataType::string_datatype(),
                    true,
                )
                .with_fulltext_options(FulltextOptions {
                    enable: true,
                    analyzer: FulltextAnalyzer::English,
                    case_sensitive: false,
                    backend: backend.clone(),
                })
                .unwrap(),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "text_chinese",
                    ConcreteDataType::string_datatype(),
                    true,
                )
                .with_fulltext_options(FulltextOptions {
                    enable: true,
                    analyzer: FulltextAnalyzer::Chinese,
                    case_sensitive: false,
                    backend: backend.clone(),
                })
                .unwrap(),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 4,
            });

        Arc::new(builder.build().unwrap())
    }

    fn new_batch(
        rows: &[(
            Option<&str>, // text_english_case_sensitive
            Option<&str>, // text_english_case_insensitive
            Option<&str>, // text_chinese
        )],
    ) -> Batch {
        let mut vec_english_sensitive =
            ConcreteDataType::string_datatype().create_mutable_vector(0);
        let mut vec_english_insensitive =
            ConcreteDataType::string_datatype().create_mutable_vector(0);
        let mut vec_chinese = ConcreteDataType::string_datatype().create_mutable_vector(0);

        for (text_english_case_sensitive, text_english_case_insensitive, text_chinese) in rows {
            match text_english_case_sensitive {
                Some(s) => vec_english_sensitive.push_value_ref((*s).into()),
                None => vec_english_sensitive.push_null(),
            }
            match text_english_case_insensitive {
                Some(s) => vec_english_insensitive.push_value_ref((*s).into()),
                None => vec_english_insensitive.push_null(),
            }
            match text_chinese {
                Some(s) => vec_chinese.push_value_ref((*s).into()),
                None => vec_chinese.push_null(),
            }
        }

        let num_rows = vec_english_sensitive.len();
        Batch::new(
            vec![],
            Arc::new(UInt64Vector::from_iter_values(
                (0..num_rows).map(|n| n as u64),
            )),
            Arc::new(UInt64Vector::from_iter_values(std::iter::repeat_n(
                0, num_rows,
            ))),
            Arc::new(UInt8Vector::from_iter_values(std::iter::repeat_n(
                1, num_rows,
            ))),
            vec![
                BatchColumn {
                    column_id: 1,
                    data: vec_english_sensitive.to_vector(),
                },
                BatchColumn {
                    column_id: 2,
                    data: vec_english_insensitive.to_vector(),
                },
                BatchColumn {
                    column_id: 3,
                    data: vec_chinese.to_vector(),
                },
            ],
        )
        .unwrap()
    }

    /// Applier factory that can handle both queries and terms.
    ///
    /// It builds a fulltext index with the given data rows, and returns a function
    /// that can handle both queries and terms in a single request.
    ///
    /// The function takes two parameters:
    /// - `queries`: A list of (ColumnId, query_string) pairs for fulltext queries
    /// - `terms`: A list of (ColumnId, [(bool, String)]) for fulltext terms, where bool indicates if term is lowercased
    async fn build_fulltext_applier_factory(
        prefix: &str,
        backend: FulltextBackend,
        rows: &[(
            Option<&str>, // text_english_case_sensitive
            Option<&str>, // text_english_case_insensitive
            Option<&str>, // text_chinese
        )],
    ) -> impl Fn(
        Vec<(ColumnId, &str)>,
        Vec<(ColumnId, Vec<(bool, &str)>)>,
        Option<BitVec>,
    ) -> BoxFuture<'static, Option<BTreeSet<RowId>>> {
        let (d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let region_dir = "region0".to_string();
        let sst_file_id = FileId::random();
        let object_store = mock_object_store();
        let region_metadata = mock_region_metadata(backend.clone());
        let intm_mgr = new_intm_mgr(d.path().to_string_lossy()).await;

        let mut indexer = FulltextIndexer::new(
            &region_metadata.region_id,
            &sst_file_id,
            &intm_mgr,
            &region_metadata,
            true,
            1,
            1024,
        )
        .await
        .unwrap()
        .unwrap();

        let mut batch = new_batch(rows);
        indexer.update(&mut batch).await.unwrap();

        let puffin_manager = factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(region_dir.clone()),
        );
        let mut writer = puffin_manager.writer(&sst_file_id).await.unwrap();
        let _ = indexer.finish(&mut writer).await.unwrap();
        writer.finish().await.unwrap();

        move |queries: Vec<(ColumnId, &str)>,
              terms_requests: Vec<(ColumnId, Vec<(bool, &str)>)>,
              coarse_mask: Option<BitVec>| {
            let _d = &d;
            let region_dir = region_dir.clone();
            let object_store = object_store.clone();
            let factory = factory.clone();

            let mut requests: BTreeMap<ColumnId, FulltextRequest> = BTreeMap::new();

            // Add queries
            for (column_id, query) in queries {
                requests
                    .entry(column_id)
                    .or_default()
                    .queries
                    .push(FulltextQuery(query.to_string()));
            }

            // Add terms
            for (column_id, terms) in terms_requests {
                let fulltext_terms = terms
                    .into_iter()
                    .map(|(col_lowered, term)| FulltextTerm {
                        col_lowered,
                        term: term.to_string(),
                    })
                    .collect::<Vec<_>>();

                requests
                    .entry(column_id)
                    .or_default()
                    .terms
                    .extend(fulltext_terms);
            }

            let applier = FulltextIndexApplier::new(
                region_dir,
                region_metadata.region_id,
                object_store,
                requests,
                factory,
            );

            let backend = backend.clone();
            async move {
                match backend {
                    FulltextBackend::Tantivy => {
                        applier.apply_fine(sst_file_id, None).await.unwrap()
                    }
                    FulltextBackend::Bloom => {
                        let coarse_mask = coarse_mask.unwrap_or_default();
                        let row_groups = (0..coarse_mask.len()).map(|i| (1, coarse_mask[i]));
                        // row group id == row id
                        let resp = applier
                            .apply_coarse(sst_file_id, None, row_groups)
                            .await
                            .unwrap();
                        resp.map(|r| {
                            r.into_iter()
                                .map(|(row_group_id, _)| row_group_id as RowId)
                                .collect()
                        })
                    }
                }
            }
            .boxed()
        }
    }

    fn rows(row_ids: impl IntoIterator<Item = RowId>) -> BTreeSet<RowId> {
        row_ids.into_iter().collect()
    }

    #[tokio::test]
    async fn test_fulltext_index_basic_case_sensitive_tantivy() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_basic_case_sensitive_tantivy_",
            FulltextBackend::Tantivy,
            &[
                (Some("hello"), None, None),
                (Some("world"), None, None),
                (None, None, None),
                (Some("Hello, World"), None, None),
            ],
        )
        .await;

        let row_ids = applier_factory(vec![(1, "hello")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([0])));

        let row_ids = applier_factory(vec![(1, "world")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([1])));

        let row_ids = applier_factory(vec![(1, "Hello")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(vec![(1, "World")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(vec![], vec![(1, vec![(false, "hello")])], None).await;
        assert_eq!(row_ids, Some(rows([0])));

        let row_ids = applier_factory(vec![], vec![(1, vec![(true, "hello")])], None).await;
        assert_eq!(row_ids, None);

        let row_ids = applier_factory(vec![], vec![(1, vec![(false, "world")])], None).await;
        assert_eq!(row_ids, Some(rows([1])));

        let row_ids = applier_factory(vec![], vec![(1, vec![(true, "world")])], None).await;
        assert_eq!(row_ids, None);

        let row_ids = applier_factory(vec![], vec![(1, vec![(false, "Hello")])], None).await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(vec![], vec![(1, vec![(true, "Hello")])], None).await;
        assert_eq!(row_ids, None);

        let row_ids = applier_factory(vec![], vec![(1, vec![(false, "Hello, World")])], None).await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(vec![], vec![(1, vec![(true, "Hello, World")])], None).await;
        assert_eq!(row_ids, None);
    }

    #[tokio::test]
    async fn test_fulltext_index_basic_case_sensitive_bloom() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_basic_case_sensitive_bloom_",
            FulltextBackend::Bloom,
            &[
                (Some("hello"), None, None),
                (Some("world"), None, None),
                (None, None, None),
                (Some("Hello, World"), None, None),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "hello")])],
            Some(BitVec::from_slice(&[0b1110])), // row 0 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, None);

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([1])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "world")])],
            Some(BitVec::from_slice(&[0b1101])), // row 1 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, None);

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello")])],
            Some(BitVec::from_slice(&[0b0111])), // row 3 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "Hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, None);

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello, World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello, World")])],
            Some(BitVec::from_slice(&[0b0111])), // row 3 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "Hello, World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, None);
    }

    #[tokio::test]
    async fn test_fulltext_index_basic_case_insensitive_tantivy() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_basic_case_insensitive_tantivy_",
            FulltextBackend::Tantivy,
            &[
                (None, Some("hello"), None),
                (None, None, None),
                (None, Some("world"), None),
                (None, Some("Hello, World"), None),
            ],
        )
        .await;

        let row_ids = applier_factory(vec![(2, "hello")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![(2, "world")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(vec![(2, "Hello")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![(2, "World")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(false, "hello")])], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(true, "hello")])], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(false, "world")])], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(true, "world")])], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(false, "Hello")])], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(true, "Hello")])], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(false, "World")])], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(vec![], vec![(2, vec![(true, "World")])], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_basic_case_insensitive_bloom() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_basic_case_insensitive_bloom_",
            FulltextBackend::Bloom,
            &[
                (None, Some("hello"), None),
                (None, None, None),
                (None, Some("world"), None),
                (None, Some("Hello, World"), None),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "hello")])],
            Some(BitVec::from_slice(&[0b1110])), // row 0 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "hello")])],
            Some(BitVec::from_slice(&[0b1110])), // row 0 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "world")])],
            Some(BitVec::from_slice(&[0b1011])), // row 2 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "world")])],
            Some(BitVec::from_slice(&[0b1011])), // row 2 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "Hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "Hello")])],
            Some(BitVec::from_slice(&[0b0111])), // row 3 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([0])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "Hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "Hello")])],
            Some(BitVec::from_slice(&[0b1110])), // row 0 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "World")])],
            Some(BitVec::from_slice(&[0b0111])), // row 3 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([2])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "World")])],
            Some(BitVec::from_slice(&[0b1011])), // row 2 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_basic_chinese_tantivy() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_basic_chinese_tantivy_",
            FulltextBackend::Tantivy,
            &[
                (None, None, Some("你好")),
                (None, None, None),
                (None, None, Some("世界")),
                (None, None, Some("你好，世界")),
            ],
        )
        .await;

        let row_ids = applier_factory(vec![(3, "你好")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![(3, "世界")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(vec![], vec![(3, vec![(false, "你好")])], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![], vec![(3, vec![(false, "世界")])], None).await;
        assert_eq!(row_ids, Some(rows([2, 3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_basic_chinese_bloom() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_basic_chinese_bloom_",
            FulltextBackend::Bloom,
            &[
                (None, None, Some("你好")),
                (None, None, None),
                (None, None, Some("世界")),
                (None, None, Some("你好，世界")),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(3, vec![(false, "你好")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(3, vec![(false, "你好")])],
            Some(BitVec::from_slice(&[0b1110])), // row 0 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(3, vec![(false, "世界")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([2, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(3, vec![(false, "世界")])],
            Some(BitVec::from_slice(&[0b1011])), // row 2 is filtered out
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_terms_case_sensitive_tantivy() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_multi_terms_case_sensitive_tantivy_",
            FulltextBackend::Tantivy,
            &[
                (Some("Hello"), None, None),
                (Some("World"), None, None),
                (None, None, None),
                (Some("Hello, World"), None, None),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "hello"), (false, "world")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello"), (false, "World")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "Hello"), (false, "World")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([1, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello"), (true, "World")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "Hello"), (true, "World")])],
            None,
        )
        .await;
        assert_eq!(row_ids, None);
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_terms_case_sensitive_bloom() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_multi_terms_case_sensitive_bloom_",
            FulltextBackend::Bloom,
            &[
                (Some("Hello"), None, None),
                (Some("World"), None, None),
                (None, None, None),
                (Some("Hello, World"), None, None),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "hello"), (false, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello"), (false, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "Hello"), (false, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([1, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "Hello"), (true, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(true, "Hello"), (true, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, None);
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_terms_case_insensitive_tantivy() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_multi_terms_case_insensitive_tantivy_",
            FulltextBackend::Tantivy,
            &[
                (None, Some("hello"), None),
                (None, None, None),
                (None, Some("world"), None),
                (None, Some("Hello, World"), None),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "hello"), (false, "world")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "hello"), (false, "world")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "hello"), (true, "world")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "hello"), (true, "world")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_terms_case_insensitive_bloom() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_multi_terms_case_insensitive_bloom_",
            FulltextBackend::Bloom,
            &[
                (None, Some("hello"), None),
                (None, None, None),
                (None, Some("world"), None),
                (None, Some("Hello, World"), None),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "hello"), (false, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "hello"), (false, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(false, "hello"), (true, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(2, vec![(true, "hello"), (true, "world")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_columns_tantivy() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_multi_columns_tantivy_",
            FulltextBackend::Tantivy,
            &[
                (Some("Hello"), None, Some("你好")),
                (Some("World"), Some("world"), None),
                (None, Some("World"), Some("世界")),
                (
                    Some("Hello, World"),
                    Some("Hello, World"),
                    Some("你好，世界"),
                ),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![(1, "Hello"), (3, "你好")],
            vec![(2, vec![(false, "world")])],
            None,
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids =
            applier_factory(vec![(2, "World")], vec![(1, vec![(false, "World")])], None).await;
        assert_eq!(row_ids, Some(rows([1, 3])));
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_columns_bloom() {
        let applier_factory = build_fulltext_applier_factory(
            "test_fulltext_index_multi_columns_bloom_",
            FulltextBackend::Bloom,
            &[
                (Some("Hello"), None, Some("你好")),
                (Some("World"), Some("world"), None),
                (None, Some("World"), Some("世界")),
                (
                    Some("Hello, World"),
                    Some("Hello, World"),
                    Some("你好，世界"),
                ),
            ],
        )
        .await;

        let row_ids = applier_factory(
            vec![],
            vec![
                (1, vec![(false, "Hello")]),
                (2, vec![(false, "world")]),
                (3, vec![(false, "你好")]),
            ],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([3])));

        let row_ids = applier_factory(
            vec![],
            vec![(1, vec![(false, "World")]), (2, vec![(false, "World")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([1, 3])));
    }
}
