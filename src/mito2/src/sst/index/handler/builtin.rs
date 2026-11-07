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

//! Adapters preserving the existing built-in creators and their file-level budgets.

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::warn;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::Json2ExtensionType;
use datatypes::schema::FulltextBackend;
use index::target::IndexTarget;

use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::read::Batch;
use crate::sst::file::{FileMeta, IndexType};
use crate::sst::index::bloom_filter::creator::BloomFilterIndexer;
use crate::sst::index::fulltext_index;
use crate::sst::index::fulltext_index::creator::{FulltextIndexer, FulltextTarget};
use crate::sst::index::handler::{
    BuildContext, CurrencyContext, IndexCreator, IndexKindHandler, PlanContext, PlanEntry,
    PlannedIndex,
};
use crate::sst::index::inverted_index::creator::InvertedIndexer;
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// Configuration for one of the existing file-level built-in creators.
#[derive(Debug, Clone)]
pub(crate) enum BuiltinIndexHandler {
    Inverted(InvertedIndexConfig),
    Fulltext(FulltextIndexConfig),
    Bloom(BloomFilterConfig),
}

impl IndexKindHandler for BuiltinIndexHandler {
    fn kinds(&self) -> &[IndexKind] {
        match self {
            Self::Inverted(_) => &[IndexKind::Builtin(IndexType::InvertedIndex)],
            Self::Fulltext(_) => &[IndexKind::Builtin(IndexType::FulltextIndex)],
            Self::Bloom(_) => &[IndexKind::Builtin(IndexType::BloomFilterIndex)],
        }
    }

    fn validate(&self, _context: &PlanContext<'_>) -> Result<(), BoxedError> {
        // Built-in options retain their existing schema/build-time validation.
        // Planning here would make region creation reject inputs it previously accepted.
        Ok(())
    }

    fn plan(&self, context: &PlanContext<'_>) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
        let create = match self {
            Self::Inverted(config) => match context.build_type {
                IndexBuildType::Flush => config.create_on_flush.auto(),
                IndexBuildType::Compact => config.create_on_compaction.auto(),
                _ => true,
            },
            Self::Fulltext(config) => match context.build_type {
                IndexBuildType::Flush => config.create_on_flush.auto(),
                IndexBuildType::Compact => config.create_on_compaction.auto(),
                _ => true,
            },
            Self::Bloom(config) => match context.build_type {
                IndexBuildType::Flush => config.create_on_flush.auto(),
                IndexBuildType::Compact => config.create_on_compaction.auto(),
                _ => true,
            },
        };
        if !create {
            return Ok(None);
        }
        let kind = self.kinds()[0];
        let mut targets = Vec::new();
        let mut entries = Vec::new();
        let mut segment_row_count = None;
        let mut fulltext_targets = Vec::new();
        let mut bloom_targets = Vec::new();
        match self {
            Self::Inverted(_) => {
                targets.extend(
                    context
                        .metadata
                        .inverted_indexed_column_ids(
                            context
                                .index_options
                                .inverted_index
                                .ignore_column_ids
                                .iter(),
                        )
                        .into_iter()
                        .map(IndexTarget::ColumnId),
                );
                targets.extend(json_inverted_targets(context));
                segment_row_count =
                    NonZeroUsize::new(context.index_options.inverted_index.segment_row_count);
                if segment_row_count.is_none() {
                    warn!(
                        "Skip creating inverted index with zero segment row count, region_id: {}",
                        context.metadata.region_id
                    );
                    return Ok(None);
                }
                entries.extend(targets.iter().map(|target| PlanEntry {
                    column_id: target_column_id(target),
                    kind,
                    format_version: 0,
                }));
            }
            Self::Fulltext(_) => {
                for column in &context.metadata.column_metadatas {
                    for (target, options) in FulltextIndexer::targets(column, &context.build_type)
                        .map_err(BoxedError::new)?
                    {
                        entries.push(PlanEntry {
                            column_id: target_column_id(&target),
                            kind,
                            format_version: if options.backend == FulltextBackend::Bloom {
                                u32::from(fulltext_index::FULLTEXT_BLOOM_VERSION)
                            } else {
                                0
                            },
                        });
                        fulltext_targets.push((
                            column.column_id,
                            column.column_schema.name.clone(),
                            target,
                            options,
                        ));
                    }
                }
            }
            Self::Bloom(_) => {
                for column in &context.metadata.column_metadatas {
                    for (target, options) in
                        BloomFilterIndexer::targets(column, &context.build_type)
                            .map_err(BoxedError::new)?
                    {
                        entries.push(PlanEntry {
                            column_id: target_column_id(&target),
                            kind,
                            format_version: 0,
                        });
                        bloom_targets.push((target, options));
                    }
                }
            }
        }
        entries.sort_by_key(|entry| (entry.column_id, entry.format_version));
        entries.dedup_by(|a, b| {
            if a.column_id == b.column_id {
                b.format_version = b.format_version.max(a.format_version);
                true
            } else {
                false
            }
        });
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(BuiltinPlan {
            handler: self.clone(),
            entries,
            targets,
            segment_row_count,
            fulltext_targets,
            bloom_targets,
        })))
    }
}

#[derive(Debug)]
struct BuiltinPlan {
    handler: BuiltinIndexHandler,
    entries: Vec<PlanEntry>,
    targets: Vec<IndexTarget>,
    segment_row_count: Option<NonZeroUsize>,
    fulltext_targets: Vec<FulltextTarget>,
    bloom_targets: Vec<(IndexTarget, datatypes::schema::SkippingIndexOptions)>,
}

#[async_trait]
impl PlannedIndex for BuiltinPlan {
    fn kinds(&self) -> &[IndexKind] {
        self.handler.kinds()
    }
    fn entries(&self) -> &[PlanEntry] {
        &self.entries
    }

    async fn build(
        &self,
        context: &BuildContext<'_>,
    ) -> Result<Option<Box<dyn IndexCreator>>, BoxedError> {
        let file_id = context.output.file_id();
        match &self.handler {
            BuiltinIndexHandler::Inverted(config) => {
                let mut segment = self
                    .segment_row_count
                    .expect("validated inverted segment size");
                if let Some(row_group_size) = context.row_group_size
                    && row_group_size % segment.get() != 0
                {
                    segment = NonZeroUsize::new(row_group_size).expect("nonzero row group size");
                }
                Ok(Some(Box::new(InvertedIndexer::new(
                    file_id,
                    context.metadata,
                    context.intermediate_manager.clone(),
                    config.mem_threshold_on_create(),
                    segment,
                    self.targets.clone(),
                ))))
            }
            BuiltinIndexHandler::Fulltext(config) => FulltextIndexer::new_with_targets(
                &context.metadata.region_id,
                &file_id,
                context.intermediate_manager,
                context.metadata,
                config.compress,
                config.mem_threshold_on_create(),
                self.fulltext_targets.clone(),
            )
            .await
            .map(|creator| creator.map(|c| Box::new(c) as Box<dyn IndexCreator>))
            .map_err(BoxedError::new),
            BuiltinIndexHandler::Bloom(config) => BloomFilterIndexer::new_with_targets(
                file_id,
                context.metadata,
                context.intermediate_manager.clone(),
                config.mem_threshold_on_create(),
                self.bloom_targets.clone(),
            )
            .map(|creator| creator.map(|c| Box::new(c) as Box<dyn IndexCreator>))
            .map_err(BoxedError::new),
        }
    }

    fn is_current(&self, file: &FileMeta, context: &CurrencyContext<'_>) -> bool {
        // Preserve the established schema/version policy, including its JSON rebuild scope.
        file.is_index_consistent_with_region(
            &context.metadata.column_metadatas,
            context.index_options,
        )
    }
}

fn json_inverted_targets(context: &PlanContext<'_>) -> Vec<IndexTarget> {
    // FIXME: Rebuilds replace the whole Puffin file and can drop existing JSON path indexes.
    if !matches!(
        context.build_type,
        IndexBuildType::Flush | IndexBuildType::Compact
    ) {
        return Vec::new();
    }
    let mut targets = Vec::new();
    for column in &context.metadata.column_metadatas {
        if !column.column_schema.data_type.is_json2() {
            continue;
        }
        let Ok(Some(extension)) = column.column_schema.extension_type::<Json2ExtensionType>()
        else {
            continue;
        };
        for hint in extension.metadata().json_settings().type_hints() {
            if !hint.inverted_index
                || !(hint.data_type.is_numeric()
                    || hint.data_type.is_boolean()
                    || hint.data_type.is_string())
            {
                continue;
            }
            match IndexTarget::new_json_path(column.column_id, hint.path.clone()) {
                Ok(target) => targets.push(target),
                Err(err) => warn!(err; "Skipping invalid JSON inverted index target"),
            }
        }
    }
    targets
}

#[async_trait]
impl IndexCreator for InvertedIndexer {
    async fn update(&mut self, batch: &mut Batch) -> Result<(), BoxedError> {
        Self::update(self, batch).await.map_err(BoxedError::new)
    }
    async fn update_flat(&mut self, batch: &RecordBatch) -> Result<(), BoxedError> {
        Self::update_flat(self, batch)
            .await
            .map_err(BoxedError::new)
    }
    async fn finish(
        &mut self,
        writer: &mut SstPuffinWriter,
    ) -> Result<Vec<CompletedEntry>, BoxedError> {
        let mut columns: Vec<_> = Self::indexed_targets(self)
            .filter_map(column_target_id)
            .map(Some)
            .collect();
        let (_, bytes) = Self::finish(self, writer).await.map_err(BoxedError::new)?;
        if bytes == 0 {
            return Ok(Vec::new());
        }
        if columns.is_empty() {
            columns.push(None);
        }
        columns.sort_unstable();
        columns.dedup();
        Ok(columns
            .into_iter()
            .map(|column_id| CompletedEntry {
                column_id,
                kind: IndexKind::Builtin(IndexType::InvertedIndex),
                format_version: 0,
            })
            .collect())
    }
    async fn abort(&mut self) -> Result<(), BoxedError> {
        Self::abort(self).await.map_err(BoxedError::new)
    }
    fn memory_usage(&self) -> usize {
        Self::memory_usage(self)
    }
    fn indexed_targets(&self) -> Vec<IndexTarget> {
        Self::indexed_targets(self).cloned().collect()
    }
    fn needs_dense_pk(&self) -> bool {
        true
    }
}

#[async_trait]
impl IndexCreator for FulltextIndexer {
    async fn update(&mut self, batch: &mut Batch) -> Result<(), BoxedError> {
        Self::update(self, batch).await.map_err(BoxedError::new)
    }
    async fn update_flat(&mut self, batch: &RecordBatch) -> Result<(), BoxedError> {
        Self::update_flat(self, batch)
            .await
            .map_err(BoxedError::new)
    }
    async fn finish(
        &mut self,
        writer: &mut SstPuffinWriter,
    ) -> Result<Vec<CompletedEntry>, BoxedError> {
        let mut columns: Vec<_> = Self::indexed_targets(self)
            .filter_map(column_target_id)
            .map(Some)
            .collect();
        let bloom_columns: Vec<_> = self
            .bloom_indexed_targets()
            .filter_map(column_target_id)
            .collect();
        let (_, bytes) = Self::finish(self, writer).await.map_err(BoxedError::new)?;
        if bytes == 0 {
            return Ok(Vec::new());
        }
        if columns.is_empty() {
            columns.push(None);
        }
        columns.sort_unstable();
        columns.dedup();
        Ok(columns
            .into_iter()
            .map(|column_id| CompletedEntry {
                column_id,
                kind: IndexKind::Builtin(IndexType::FulltextIndex),
                format_version: if column_id.is_some_and(|id| bloom_columns.contains(&id)) {
                    u32::from(fulltext_index::FULLTEXT_BLOOM_VERSION)
                } else {
                    0
                },
            })
            .collect())
    }
    async fn abort(&mut self) -> Result<(), BoxedError> {
        Self::abort(self).await.map_err(BoxedError::new)
    }
    fn memory_usage(&self) -> usize {
        Self::memory_usage(self)
    }
    fn indexed_targets(&self) -> Vec<IndexTarget> {
        Self::indexed_targets(self).cloned().collect()
    }
}

#[async_trait]
impl IndexCreator for BloomFilterIndexer {
    async fn update(&mut self, batch: &mut Batch) -> Result<(), BoxedError> {
        Self::update(self, batch).await.map_err(BoxedError::new)
    }
    async fn update_flat(&mut self, batch: &RecordBatch) -> Result<(), BoxedError> {
        Self::update_flat(self, batch)
            .await
            .map_err(BoxedError::new)
    }
    async fn finish(
        &mut self,
        writer: &mut SstPuffinWriter,
    ) -> Result<Vec<CompletedEntry>, BoxedError> {
        let mut columns: Vec<_> = Self::indexed_targets(self)
            .filter_map(column_target_id)
            .map(Some)
            .collect();
        let (_, bytes) = Self::finish(self, writer).await.map_err(BoxedError::new)?;
        if bytes == 0 {
            return Ok(Vec::new());
        }
        if columns.is_empty() {
            columns.push(None);
        }
        columns.sort_unstable();
        columns.dedup();
        Ok(columns
            .into_iter()
            .map(|column_id| CompletedEntry {
                column_id,
                kind: IndexKind::Builtin(IndexType::BloomFilterIndex),
                format_version: 0,
            })
            .collect())
    }
    async fn abort(&mut self) -> Result<(), BoxedError> {
        Self::abort(self).await.map_err(BoxedError::new)
    }
    fn memory_usage(&self) -> usize {
        Self::memory_usage(self)
    }
    fn indexed_targets(&self) -> Vec<IndexTarget> {
        Self::indexed_targets(self).cloned().collect()
    }
    fn needs_dense_pk(&self) -> bool {
        true
    }
}

fn target_column_id(target: &IndexTarget) -> store_api::storage::ColumnId {
    match target {
        IndexTarget::ColumnId(id) => *id,
        IndexTarget::JsonPath(path) => path.column_id(),
    }
}

fn column_target_id(target: &IndexTarget) -> Option<store_api::storage::ColumnId> {
    match target {
        IndexTarget::ColumnId(id) => Some(*id),
        IndexTarget::JsonPath(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::options::IndexOptions;
    use crate::test_util::sst_util;

    #[test]
    fn test_builtin_validation_preserves_build_time_option_errors() {
        for (handler, key) in [
            (
                BuiltinIndexHandler::Fulltext(FulltextIndexConfig::default()),
                datatypes::schema::FULLTEXT_KEY,
            ),
            (
                BuiltinIndexHandler::Bloom(BloomFilterConfig::default()),
                datatypes::schema::SKIPPING_INDEX_KEY,
            ),
        ] {
            let mut metadata = sst_util::sst_region_metadata();
            let column = metadata
                .column_metadatas
                .iter_mut()
                .find(|column| column.semantic_type == api::v1::SemanticType::Field)
                .unwrap();
            column.column_schema =
                column
                    .column_schema
                    .clone()
                    .with_metadata(std::collections::HashMap::from([(
                        key.to_string(),
                        "invalid json".to_string(),
                    )]));
            let metadata = Arc::new(metadata);
            let options = IndexOptions::default();
            let context = PlanContext {
                metadata: &metadata,
                index_options: &options,
                build_type: IndexBuildType::Flush,
                existing_indexes: &[],
            };
            assert!(handler.validate(&context).is_ok());
            assert!(handler.plan(&context).is_err());
        }
    }
}
