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
use std::path::PathBuf;

use common_telemetry::warn;
use datatypes::schema::FulltextAnalyzer;
use index::fulltext_index::create::{FulltextIndexCreator, TantivyFulltextIndexCreator};
use index::fulltext_index::{Analyzer, Config};
use puffin::blob_metadata::CompressionCodec;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, ConcreteDataType, RegionId};

use crate::error::{
    CastVectorSnafu, CreateFulltextCreatorSnafu, FieldTypeMismatchSnafu, FulltextFinishSnafu,
    FulltextOptionsSnafu, FulltextPushTextSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu,
    Result,
};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::INDEX_BLOB_TYPE;
use crate::sst::index::intermediate::IntermediateManager;
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
        mem_limit: usize,
    ) -> Result<Option<Self>> {
        let mut creators = HashMap::new();

        for column in &metadata.column_metadatas {
            let options =
                column
                    .column_schema
                    .fulltext_options()
                    .context(FulltextOptionsSnafu {
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

            let creator = TantivyFulltextIndexCreator::new(&intm_path, config, mem_limit)
                .await
                .context(CreateFulltextCreatorSnafu)?;

            creators.insert(
                column_id,
                SingleCreator {
                    column_id,
                    inner: Box::new(creator),
                    intm_path,
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
    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
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
    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
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
    inner: Box<dyn FulltextIndexCreator>,
    /// Intermediate path where the index is written to.
    intm_path: PathBuf,
    /// Whether the index should be compressed.
    compress: bool,
}

impl SingleCreator {
    async fn update(&mut self, batch: &Batch) -> Result<()> {
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
                    self.inner
                        .push_text(text)
                        .await
                        .context(FulltextPushTextSnafu)?;
                }
            }
            _ => {
                // If the column is not found in the batch, push empty text.
                // Ensure that the number of texts pushed is the same as the number of rows in the SST,
                // so that the texts are aligned with the row ids.
                for _ in 0..batch.num_rows() {
                    self.inner
                        .push_text("")
                        .await
                        .context(FulltextPushTextSnafu)?;
                }
            }
        }

        Ok(())
    }

    async fn finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<ByteCount> {
        self.inner.finish().await.context(FulltextFinishSnafu)?;

        let options = PutOptions {
            compression: self.compress.then_some(CompressionCodec::Zstd),
        };

        let key = format!("{INDEX_BLOB_TYPE}-{}", self.column_id);
        puffin_writer
            .put_dir(&key, self.intm_path.clone(), options)
            .await
            .context(PuffinAddBlobSnafu)
    }

    async fn abort(&mut self) -> Result<()> {
        if let Err(err) = self.inner.finish().await {
            warn!(err; "Failed to finish fulltext index creator, col_id: {:?}, dir_path: {:?}", self.column_id, self.intm_path);
        }
        if let Err(err) = tokio::fs::remove_dir_all(&self.intm_path).await {
            warn!(err; "Failed to remove fulltext index directory, col_id: {:?}, dir_path: {:?}", self.column_id, self.intm_path);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::data_type::DataType;
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextOptions};
    use datatypes::vectors::{UInt64Vector, UInt8Vector};
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use index::fulltext_index::search::RowId;
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use puffin::puffin_manager::PuffinManager;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::{ConcreteDataType, RegionId};

    use super::*;
    use crate::read::{Batch, BatchColumn};
    use crate::sst::file::FileId;
    use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::sst::location;

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    async fn new_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }

    fn mock_region_metadata() -> RegionMetadataRef {
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
            Arc::new(UInt64Vector::from_iter_values(
                std::iter::repeat(0).take(num_rows),
            )),
            Arc::new(UInt8Vector::from_iter_values(
                std::iter::repeat(1).take(num_rows),
            )),
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

    async fn build_applier_factory(
        prefix: &str,
        rows: &[(
            Option<&str>, // text_english_case_sensitive
            Option<&str>, // text_english_case_insensitive
            Option<&str>, // text_chinese
        )],
    ) -> impl Fn(Vec<(ColumnId, &str)>) -> BoxFuture<'static, BTreeSet<RowId>> {
        let (d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let region_dir = "region0".to_string();
        let sst_file_id = FileId::random();
        let file_path = location::index_file_path(&region_dir, sst_file_id);
        let object_store = mock_object_store();
        let region_metadata = mock_region_metadata();
        let intm_mgr = new_intm_mgr(d.path().to_string_lossy()).await;

        let mut indexer = FulltextIndexer::new(
            &region_metadata.region_id,
            &sst_file_id,
            &intm_mgr,
            &region_metadata,
            true,
            1024,
        )
        .await
        .unwrap()
        .unwrap();

        let batch = new_batch(rows);
        indexer.update(&batch).await.unwrap();

        let puffin_manager = factory.build(object_store.clone());
        let mut writer = puffin_manager.writer(&file_path).await.unwrap();
        let _ = indexer.finish(&mut writer).await.unwrap();
        writer.finish().await.unwrap();

        move |queries| {
            let _d = &d;
            let applier = FulltextIndexApplier::new(
                region_dir.clone(),
                object_store.clone(),
                queries
                    .into_iter()
                    .map(|(a, b)| (a, b.to_string()))
                    .collect(),
                factory.clone(),
            );

            async move { applier.apply(sst_file_id).await.unwrap() }.boxed()
        }
    }

    #[tokio::test]
    async fn test_fulltext_index_basic() {
        let applier_factory = build_applier_factory(
            "test_fulltext_index_basic_",
            &[
                (Some("hello"), None, Some("你好")),
                (Some("world"), Some("world"), None),
                (None, Some("World"), Some("世界")),
                (
                    Some("Hello, World"),
                    Some("Hello, World"),
                    Some("你好，世界"),
                ),
            ],
        )
        .await;

        let row_ids = applier_factory(vec![(1, "hello")]).await;
        assert_eq!(row_ids, vec![0].into_iter().collect());

        let row_ids = applier_factory(vec![(1, "world")]).await;
        assert_eq!(row_ids, vec![1].into_iter().collect());

        let row_ids = applier_factory(vec![(2, "hello")]).await;
        assert_eq!(row_ids, vec![3].into_iter().collect());

        let row_ids = applier_factory(vec![(2, "world")]).await;
        assert_eq!(row_ids, vec![1, 2, 3].into_iter().collect());

        let row_ids = applier_factory(vec![(3, "你好")]).await;
        assert_eq!(row_ids, vec![0, 3].into_iter().collect());

        let row_ids = applier_factory(vec![(3, "世界")]).await;
        assert_eq!(row_ids, vec![2, 3].into_iter().collect());
    }

    #[tokio::test]
    async fn test_fulltext_index_multi_columns() {
        let applier_factory = build_applier_factory(
            "test_fulltext_index_multi_columns_",
            &[
                (Some("hello"), None, Some("你好")),
                (Some("world"), Some("world"), None),
                (None, Some("World"), Some("世界")),
                (
                    Some("Hello, World"),
                    Some("Hello, World"),
                    Some("你好，世界"),
                ),
            ],
        )
        .await;

        let row_ids = applier_factory(vec![(1, "hello"), (3, "你好")]).await;
        assert_eq!(row_ids, vec![0].into_iter().collect());

        let row_ids = applier_factory(vec![(1, "world"), (3, "世界")]).await;
        assert_eq!(row_ids, vec![].into_iter().collect());

        let row_ids = applier_factory(vec![(2, "world"), (3, "世界")]).await;
        assert_eq!(row_ids, vec![2, 3].into_iter().collect());
    }
}
