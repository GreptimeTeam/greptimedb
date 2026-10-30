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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::sst::index::column::extract_json_type_hint_values;
use api::v1::SemanticType;
use arrow_schema::extension::ExtensionType;
use common_telemetry::warn;
use datatypes::arrow::array::{Array, LargeStringArray, StringArray};
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::Json2ExtensionType;
use datatypes::schema::{FulltextAnalyzer, FulltextBackend};
use datatypes::vectors::VectorRef;
use index::fulltext_index::create::{
    BloomFilterFulltextIndexCreator, FulltextIndexCreator, TantivyFulltextIndexCreator,
};
use index::fulltext_index::{Analyzer, Config};
use index::target::IndexTarget;
use puffin::blob_metadata::CompressionCodec;
use puffin::puffin_manager::PutOptions;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, ConcreteDataType, FileId, RegionId};

use crate::error::{
    CastVectorSnafu, ComputeArrowSnafu, CreateFulltextCreatorSnafu, DataTypeMismatchSnafu,
    FulltextFinishSnafu, FulltextPushTextSnafu, IndexOptionsSnafu, OperateAbortedIndexSnafu,
    Result,
};
use crate::read::Batch;
use crate::sst::index::TYPE_FULLTEXT_INDEX;
use crate::sst::index::fulltext_index::{INDEX_BLOB_TYPE_BLOOM, INDEX_BLOB_TYPE_TANTIVY};
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};

/// `FulltextIndexer` is responsible for creating fulltext indexes for SST files.
pub struct FulltextIndexer {
    /// Creators for each column.
    creators: HashMap<IndexTarget, SingleCreator>,
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
        include_json_paths: bool,
    ) -> Result<Option<Self>> {
        let mut creators = HashMap::new();

        for column in &metadata.column_metadatas {
            // Tag columns don't support fulltext index now.
            // If we need to support fulltext index for tag columns, we also need to parse
            // the codec and handle sparse encoding for flat format specially.
            if column.semantic_type == SemanticType::Tag {
                common_telemetry::debug!(
                    "Skip creating fulltext index for tag column {}",
                    column.column_schema.name
                );
                continue;
            }

            let options = column
                .column_schema
                .fulltext_options()
                .context(IndexOptionsSnafu {
                    column_name: &column.column_schema.name,
                })?;

            let mut targets = Vec::new();
            if let Some(options) = options.filter(|options| options.enable) {
                targets.push((IndexTarget::ColumnId(column.column_id), options));
            }
            if include_json_paths
                && column.column_schema.data_type.is_json2()
                && let Some(extension) = column
                    .column_schema
                    .extension_type::<Json2ExtensionType>()
                    .context(IndexOptionsSnafu {
                        column_name: &column.column_schema.name,
                    })?
            {
                for hint in extension.metadata().json_settings().type_hints() {
                    let Some(options) = hint.fulltext_index.as_ref().filter(|o| o.enable) else {
                        continue;
                    };
                    if let Ok(target) =
                        IndexTarget::new_json_path(column.column_id, hint.path.clone())
                    {
                        targets.push((target, options.clone()));
                    }
                }
            }
            for (target, options) in targets {
                // fulltext_path generates a fresh UUID for each target, so JSON paths
                // in the same column use separate intermediate directories.
                let intm_path =
                    intermediate_manager.fulltext_path(region_id, sst_file_id, column.column_id);

                let config = Config {
                    analyzer: match options.analyzer {
                        FulltextAnalyzer::English => Analyzer::English,
                        FulltextAnalyzer::Chinese => Analyzer::Chinese,
                    },
                    case_sensitive: options.case_sensitive,
                };

                let inner = match options.backend {
                    FulltextBackend::Tantivy => {
                        let creator =
                            TantivyFulltextIndexCreator::new(&intm_path, config, mem_limit)
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
                            options.granularity as _,
                            options.false_positive_rate(),
                            temp_file_provider,
                            global_memory_usage,
                            Some(mem_limit),
                        );
                        AltFulltextCreator::Bloom(creator)
                    }
                };

                creators.insert(
                    target.clone(),
                    SingleCreator {
                        target,
                        column_name: column.column_schema.name.clone(),
                        inner,
                        compress,
                    },
                );
            }
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

    /// Updates the fulltext index with the given flat format RecordBatch.
    pub async fn update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if batch.num_rows() == 0 {
            return Ok(());
        }

        if let Err(update_err) = self.do_update_flat(batch).await {
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
        self.creators.keys().filter_map(|target| match target {
            IndexTarget::ColumnId(id) => Some(*id),
            IndexTarget::JsonPath(_) => None,
        })
    }

    /// Returns IDs of columns indexed with the bloom backend.
    pub fn bloom_column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.creators
            .values()
            .filter(|c| matches!(c.inner, AltFulltextCreator::Bloom(_)))
            .filter_map(|c| match &c.target {
                IndexTarget::ColumnId(id) => Some(*id),
                IndexTarget::JsonPath(_) => None,
            })
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

    async fn do_update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut guard = self.stats.record_update();
        guard.inc_row_count(batch.num_rows());

        for creator in self.creators.values_mut() {
            creator.update_flat(batch).await?;
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
    /// Column or JSON path to index.
    target: IndexTarget,
    /// Column name.
    column_name: String,
    /// Inner creator.
    inner: AltFulltextCreator,
    /// Whether the index should be compressed.
    compress: bool,
}

impl SingleCreator {
    async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        let column_id = match &self.target {
            IndexTarget::ColumnId(id) => *id,
            IndexTarget::JsonPath(path) => {
                let root = batch
                    .field_col_value(path.column_id())
                    .with_context(|| crate::error::InvalidRecordBatchSnafu {
                        reason: format!("Missing JSON fulltext column {}", path.column_id()),
                    })?
                    .data
                    .to_arrow_array();
                let values = extract_json_type_hint_values(&root, path.path())?;
                return self.push_json_values(values).await;
            }
        };
        let text_column = batch.fields().iter().find(|c| c.column_id == column_id);
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
                        .try_into_string()
                        .context(DataTypeMismatchSnafu)?
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

    async fn update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        if let IndexTarget::JsonPath(path) = &self.target {
            let root = batch.column_by_name(&self.column_name).with_context(|| {
                crate::error::InvalidRecordBatchSnafu {
                    reason: format!("Missing JSON fulltext column {}", self.column_name),
                }
            })?;
            let values = extract_json_type_hint_values(root, path.path())?;
            return self.push_json_values(values).await;
        }
        // Find the column in the RecordBatch by name
        if let Some(column_array) = batch.column_by_name(&self.column_name) {
            // Convert Arrow array to string array.
            // TODO(yingwen): Use Utf8View later if possible.
            match column_array.data_type() {
                DataType::Utf8 => {
                    let string_array = column_array.as_any().downcast_ref::<StringArray>().unwrap();
                    for text_opt in string_array.iter() {
                        let text = text_opt.unwrap_or_default();
                        self.inner.push_text(text).await?;
                    }
                }
                DataType::LargeUtf8 => {
                    let large_string_array = column_array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap();
                    for text_opt in large_string_array.iter() {
                        let text = text_opt.unwrap_or_default();
                        self.inner.push_text(text).await?;
                    }
                }
                _ => {
                    // For other types, cast to Utf8 as before
                    let array = datatypes::arrow::compute::cast(column_array, &DataType::Utf8)
                        .context(ComputeArrowSnafu)?;
                    let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for text_opt in string_array.iter() {
                        let text = text_opt.unwrap_or_default();
                        self.inner.push_text(text).await?;
                    }
                }
            }
        } else {
            // If the column is not found in the batch, push empty text.
            // Ensure that the number of texts pushed is the same as the number of rows in the SST,
            // so that the texts are aligned with the row ids.
            for _ in 0..batch.num_rows() {
                self.inner.push_text("").await?;
            }
        }

        Ok(())
    }

    async fn push_json_values(&mut self, values: VectorRef) -> Result<()> {
        for row in 0..values.len() {
            let text = values
                .get_ref(row)
                .try_into_string()
                .context(DataTypeMismatchSnafu)?
                .unwrap_or_default();
            self.inner.push_text(text).await?;
        }
        Ok(())
    }

    async fn finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<ByteCount> {
        let options = PutOptions {
            compression: self.compress.then_some(CompressionCodec::Zstd),
        };
        self.inner
            .finish(puffin_writer, &self.target, options)
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort(&self.target).await;
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
        target: &IndexTarget,
        put_options: PutOptions,
    ) -> Result<ByteCount> {
        let target_key = target.encode();
        match self {
            Self::Tantivy(creator) => {
                let blob_key = format!("{INDEX_BLOB_TYPE_TANTIVY}-{target_key}");
                creator
                    .finish(puffin_writer, &blob_key, put_options)
                    .await
                    .context(FulltextFinishSnafu)
            }
            Self::Bloom(creator) => {
                let blob_key = format!("{INDEX_BLOB_TYPE_BLOOM}-{target_key}");
                creator
                    .finish(puffin_writer, &blob_key, put_options)
                    .await
                    .context(FulltextFinishSnafu)
            }
        }
    }

    async fn abort(&mut self, target: &IndexTarget) {
        match self {
            Self::Tantivy(creator) => {
                if let Err(err) = creator.abort().await {
                    warn!(err; "Failed to abort the fulltext index creator in the Tantivy flavor, target: {:?}", target);
                }
            }
            Self::Bloom(creator) => {
                if let Err(err) = creator.abort().await {
                    warn!(err; "Failed to abort the fulltext index creator in the Bloom Filter flavor, target: {:?}", target);
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
    use datatypes::vectors::{UInt8Vector, UInt64Vector};
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use index::fulltext_index::search::RowId;
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use puffin::puffin_manager::{PuffinManager, PuffinWriter};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_request::PathType;
    use store_api::storage::{ConcreteDataType, FileId, RegionId};

    use super::*;
    use crate::access_layer::RegionFilePathFactory;
    use crate::read::{Batch, BatchColumn};
    use crate::sst::file::{RegionFileId, RegionIndexId};
    use crate::sst::index::fulltext_index::applier::FulltextIndexApplier;
    use crate::sst::index::fulltext_index::applier::builder::{
        FulltextQuery, FulltextRequest, FulltextTerm,
    };
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    #[tokio::test]
    async fn test_json_fulltext_build_and_prune_both_formats() {
        use datatypes::arrow::array::{ArrayRef, StructArray};
        use datatypes::arrow::buffer::NullBuffer;
        use datatypes::arrow::datatypes::{Field, Schema};
        use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
        use datatypes::json::{JsonSettings, JsonTypeHint};
        use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
        use datatypes::vectors::{Helper, UInt8Vector, UInt64Vector};

        for backend in [FulltextBackend::Bloom, FulltextBackend::Tantivy] {
            for flat in [false, true] {
                let (dir, factory) =
                    PuffinManagerFactory::new_for_test_async("json_fulltext").await;
                let options = FulltextOptions::new(
                    true,
                    FulltextAnalyzer::English,
                    false,
                    backend.clone(),
                    1,
                    0.01,
                )
                .unwrap();
                let hints = ["message", "other/\".field"].map(|name| JsonTypeHint {
                    path: vec![name.into()],
                    data_type: ConcreteDataType::string_datatype(),
                    inverted_index: false,
                    skipping_index: None,
                    fulltext_index: Some(options.clone()),
                });
                let mut column = ColumnSchema::new(
                    "attrs",
                    ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
                    true,
                );
                column.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
                    JsonSettings::try_new(hints.to_vec(), Some(0)).unwrap(),
                ))));
                let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
                builder.push_column_metadata(ColumnMetadata {
                    column_schema: column,
                    semantic_type: SemanticType::Field,
                    column_id: 1,
                });
                builder.push_column_metadata(ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    semantic_type: SemanticType::Timestamp,
                    column_id: 2,
                });
                let metadata = Arc::new(builder.build().unwrap());
                let file_id = FileId::random();
                let manager = new_intm_mgr(dir.path().to_string_lossy()).await;
                assert!(
                    FulltextIndexer::new(
                        &metadata.region_id,
                        &file_id,
                        &manager,
                        &metadata,
                        false,
                        1024,
                        false
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
                let mut creator = FulltextIndexer::new(
                    &metadata.region_id,
                    &file_id,
                    &manager,
                    &metadata,
                    false,
                    1024,
                    true,
                )
                .await
                .unwrap()
                .unwrap();
                let messages: ArrayRef = Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    None,
                    Some("beta"),
                    Some("alpha"),
                ]));
                let other: ArrayRef = Arc::new(StringArray::from(vec![
                    Some("beta"),
                    None,
                    Some("alpha"),
                    Some(""),
                ]));
                let root: ArrayRef = Arc::new(StructArray::new(
                    vec![
                        Field::new("message", messages.data_type().clone(), true),
                        Field::new("other/\".field", other.data_type().clone(), true),
                    ]
                    .into(),
                    vec![messages, other],
                    Some(NullBuffer::from(vec![true, false, true, true])),
                ));
                for start in [0, 2] {
                    let array = root.slice(start, 2);
                    if flat {
                        let batch = RecordBatch::try_new(
                            Arc::new(Schema::new(vec![Field::new(
                                "attrs",
                                array.data_type().clone(),
                                true,
                            )])),
                            vec![array],
                        )
                        .unwrap();
                        creator.update_flat(&batch).await.unwrap();
                    } else {
                        let mut batch = Batch::new(
                            vec![],
                            Arc::new(UInt64Vector::from_vec(vec![0, 1])),
                            Arc::new(UInt64Vector::from_vec(vec![0, 0])),
                            Arc::new(UInt8Vector::from_vec(vec![1, 1])),
                            vec![BatchColumn {
                                column_id: 1,
                                data: Helper::try_into_vector(array).unwrap(),
                            }],
                        )
                        .unwrap();
                        creator.update(&mut batch).await.unwrap();
                    }
                }
                assert_eq!(creator.column_ids().count(), 0);
                assert_eq!(creator.bloom_column_ids().count(), 0);
                let store = mock_object_store();
                let puffin = factory.build(
                    store.clone(),
                    RegionFilePathFactory::new("json_test".into(), PathType::Bare),
                );
                let index_id =
                    RegionIndexId::new(RegionFileId::new(metadata.region_id, file_id), 0);
                let mut writer = puffin.writer(&index_id).await.unwrap();
                let (rows, bytes) = creator.finish(&mut writer).await.unwrap();
                assert_eq!(rows, 4);
                assert!(bytes > 0);
                writer.finish().await.unwrap();
                let cache = Arc::new(
                    crate::cache::index::bloom_filter_index::BloomFilterIndexCache::new(
                        1024 * 1024,
                        1024 * 1024,
                        1024,
                    ),
                );
                for (path, term, expected) in [
                    ("message", "alpha", vec![0, 3]),
                    ("other/\".field", "alpha", vec![2]),
                    ("message", "hidden", vec![]),
                    ("message", "absent", vec![]),
                ] {
                    let target = IndexTarget::new_json_path(1, vec![path.into()]).unwrap();
                    let request = match backend {
                        FulltextBackend::Tantivy => FulltextRequest {
                            queries: vec![FulltextQuery(term.into())],
                            ..Default::default()
                        },
                        FulltextBackend::Bloom => FulltextRequest {
                            terms: vec![FulltextTerm {
                                col_lowered: false,
                                term: term.into(),
                            }],
                            ..Default::default()
                        },
                    };
                    let applier = FulltextIndexApplier::new(
                        "json_test".into(),
                        PathType::Bare,
                        store.clone(),
                        BTreeMap::from([(target, request)]),
                        factory.clone(),
                    )
                    .with_metadata(&metadata)
                    .with_bloom_filter_cache(Some(cache.clone()));
                    assert!(applier.compatible_with_sst(&metadata));
                    for change in 0..3 {
                        let mut changed_hints = hints.to_vec();
                        for hint in &mut changed_hints {
                            let options = hint.fulltext_index.as_mut().unwrap();
                            match change {
                                0 => options.analyzer = FulltextAnalyzer::Chinese,
                                1 => options.case_sensitive = true,
                                _ => {
                                    options.backend = match backend {
                                        FulltextBackend::Bloom => FulltextBackend::Tantivy,
                                        FulltextBackend::Tantivy => FulltextBackend::Bloom,
                                    }
                                }
                            }
                        }
                        let mut changed = (*metadata).clone();
                        changed.column_metadatas[0]
                            .column_schema
                            .with_extension_type(&Json2ExtensionType::new(Arc::new(
                                JsonMetadata::new(
                                    JsonSettings::try_new(changed_hints, Some(0)).unwrap(),
                                ),
                            )));
                        assert!(!applier.compatible_with_sst(&changed));
                    }
                    assert!(!applier.compatible_with_sst(&mock_region_metadata(backend.clone())));
                    for _ in 0..2 {
                        match backend {
                            FulltextBackend::Tantivy => {
                                let rows = applier
                                    .apply_fine(index_id, None, None)
                                    .await
                                    .unwrap()
                                    .unwrap();
                                assert_eq!(
                                    rows.into_iter().collect::<Vec<_>>(),
                                    expected.iter().map(|v| *v as RowId).collect::<Vec<_>>()
                                );
                            }
                            FulltextBackend::Bloom => {
                                let ranges = applier
                                    .apply_coarse(index_id, None, [(4, true)].into_iter(), None)
                                    .await
                                    .unwrap()
                                    .unwrap();
                                let rows: Vec<_> = ranges[0].1.iter().cloned().flatten().collect();
                                // A Bloom filter may retain extra rows, but must retain every match.
                                assert!(expected.iter().all(|row| rows.contains(row)));
                                if expected.is_empty() {
                                    assert!(
                                        rows.len() < 4,
                                        "path={path}, term={term}, flat={flat}, rows={rows:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap()
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
                .with_fulltext_options(FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::English,
                    true,
                    backend.clone(),
                    1,
                    0.01,
                ))
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
                .with_fulltext_options(FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::English,
                    false,
                    backend.clone(),
                    1,
                    0.01,
                ))
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
                .with_fulltext_options(FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::Chinese,
                    false,
                    backend.clone(),
                    1,
                    0.01,
                ))
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
                Some(s) => vec_english_sensitive.push_value_ref(&(*s).into()),
                None => vec_english_sensitive.push_null(),
            }
            match text_english_case_insensitive {
                Some(s) => vec_english_insensitive.push_value_ref(&(*s).into()),
                None => vec_english_insensitive.push_null(),
            }
            match text_chinese {
                Some(s) => vec_chinese.push_value_ref(&(*s).into()),
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
        let table_dir = "table0".to_string();
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
            1024,
            true,
        )
        .await
        .unwrap()
        .unwrap();

        let mut batch = new_batch(rows);
        indexer.update(&mut batch).await.unwrap();

        let puffin_manager = factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );
        let region_file_id = RegionFileId::new(region_metadata.region_id, sst_file_id);
        let index_id = RegionIndexId::new(region_file_id, 0);
        let mut writer = puffin_manager.writer(&index_id).await.unwrap();
        let _ = indexer.finish(&mut writer).await.unwrap();
        writer.finish().await.unwrap();

        move |queries: Vec<(ColumnId, &str)>,
              terms_requests: Vec<(ColumnId, Vec<(bool, &str)>)>,
              coarse_mask: Option<BitVec>| {
            let _d = &d;
            let table_dir = table_dir.clone();
            let object_store = object_store.clone();
            let factory = factory.clone();

            let mut requests: BTreeMap<IndexTarget, FulltextRequest> = BTreeMap::new();

            // Add queries
            for (column_id, query) in queries {
                requests
                    .entry(IndexTarget::ColumnId(column_id))
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
                    .entry(IndexTarget::ColumnId(column_id))
                    .or_default()
                    .terms
                    .extend(fulltext_terms);
            }

            let applier = FulltextIndexApplier::new(
                table_dir,
                PathType::Bare,
                object_store,
                requests,
                factory,
            );

            let backend = backend.clone();
            async move {
                match backend {
                    FulltextBackend::Tantivy => {
                        applier.apply_fine(index_id, None, None).await.unwrap()
                    }
                    FulltextBackend::Bloom => {
                        let coarse_mask = coarse_mask.unwrap_or_default();
                        let row_groups = (0..coarse_mask.len()).map(|i| (1, coarse_mask[i]));
                        // row group id == row id
                        let resp = applier
                            .apply_coarse(index_id, None, row_groups, None)
                            .await
                            .unwrap();
                        resp.map(|r| {
                            r.into_iter()
                                .filter(|(_, ranges)| !ranges.is_empty())
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

        // Terms are left to the bloom backend.
        let row_ids = applier_factory(vec![], vec![(1, vec![(false, "hello")])], None).await;
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
            vec![(2, vec![(true, "hello")])],
            Some(BitVec::from_slice(&[0b1111])),
        )
        .await;
        assert_eq!(row_ids, Some(rows([0, 3])));
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

        let row_ids = applier_factory(vec![(1, "Hello"), (3, "你好")], vec![], None).await;
        assert_eq!(row_ids, Some(rows([0, 3])));

        let row_ids = applier_factory(vec![(1, "World"), (2, "World")], vec![], None).await;
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
