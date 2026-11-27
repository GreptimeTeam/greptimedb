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

//! Planning and creator adapter for the inverted index.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::warn;
use datatypes::arrow::record_batch::RecordBatch;
use index::target::IndexTarget;

use crate::config::InvertedIndexConfig;
use crate::read::Batch;
use crate::sst::file::FileMeta;
use crate::sst::index::column::json_index_hints;
use crate::sst::index::handler::builtin::{
    builtin_is_current, normalize_entries, target_column_id,
};
use crate::sst::index::handler::{
    BuildContext, IndexCreator, PlanContext, PlanEntry, PlannedIndex,
};
use crate::sst::index::inverted_index::creator::InvertedIndexer;
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// Resolved column targets and configuration shared across output files.
#[derive(Debug)]
pub(crate) struct InvertedPlan {
    config: InvertedIndexConfig,
    entries: Vec<PlanEntry>,
    targets: Vec<IndexTarget>,
    segment_row_count: NonZeroUsize,
}

impl InvertedPlan {
    /// Resolves targets without opening SST data or creating output files.
    pub(crate) fn plan(
        config: &InvertedIndexConfig,
        context: &PlanContext<'_>,
    ) -> Result<Option<Arc<Self>>, BoxedError> {
        let create = match context.build_type {
            IndexBuildType::Flush => config.create_on_flush.auto(),
            IndexBuildType::Compact => config.create_on_compaction.auto(),
            _ => true,
        };
        if !create {
            return Ok(None);
        }
        let mut targets: Vec<_> = context
            .metadata
            .inverted_indexed_column_ids(
                context
                    .index_options
                    .inverted_index
                    .ignore_column_ids
                    .iter(),
            )
            .into_iter()
            .map(IndexTarget::ColumnId)
            .collect();
        targets.extend(json_inverted_targets(context)?);
        let Some(segment_row_count) =
            NonZeroUsize::new(context.index_options.inverted_index.segment_row_count)
        else {
            warn!(
                "Skip creating inverted index with zero segment row count, region_id: {}",
                context.metadata.region_id
            );
            return Ok(None);
        };
        let mut entries: Vec<_> = targets
            .iter()
            .map(|target| PlanEntry {
                column_id: target_column_id(target),
                kind: IndexKind::Inverted,
                format_version: 0,
            })
            .collect();
        normalize_entries(&mut entries);
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            config: config.clone(),
            entries,
            targets,
            segment_row_count,
        })))
    }
}

#[async_trait]
impl PlannedIndex for InvertedPlan {
    fn kinds(&self) -> &[IndexKind] {
        &[IndexKind::Inverted]
    }

    fn entries(&self) -> &[PlanEntry] {
        &self.entries
    }

    async fn build(
        &self,
        context: &BuildContext<'_>,
    ) -> Result<Option<Box<dyn IndexCreator>>, BoxedError> {
        // Segments may span row groups; readers split them at row group boundaries.
        Ok(Some(Box::new(InvertedIndexer::new(
            context.output.file_id(),
            context.metadata,
            context.intermediate_manager.clone(),
            self.config.mem_threshold_on_create(),
            self.segment_row_count,
            self.targets.clone(),
        ))))
    }

    fn is_current(&self, file: &FileMeta, context: &PlanContext<'_>) -> bool {
        builtin_is_current(
            file,
            context,
            IndexKind::Inverted,
            self.targets.iter().map(|target| (target, 0)),
        )
    }
}

fn json_inverted_targets(context: &PlanContext<'_>) -> Result<Vec<IndexTarget>, BoxedError> {
    let mut targets = Vec::new();
    for column in &context.metadata.column_metadatas {
        for hint in json_index_hints(
            column,
            &context.build_type,
            context.rebuild_source,
            IndexKind::Inverted,
        )
        .map_err(BoxedError::new)?
        {
            if !hint.inverted_index
                || !(hint.data_type.is_numeric()
                    || hint.data_type.is_boolean()
                    || hint.data_type.is_string())
            {
                continue;
            }
            match IndexTarget::new_json_path(column.column_id, hint.path) {
                Ok(target) => targets.push(target),
                Err(err) => warn!(err; "Skipping invalid JSON inverted index target"),
            }
        }
    }
    Ok(targets)
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
        let entries = Self::indexed_targets(self)
            .map(|target| CompletedEntry {
                target: target.clone(),
                kind: IndexKind::Inverted,
                format_version: 0,
            })
            .collect();
        let (_, bytes) = Self::finish(self, writer).await.map_err(BoxedError::new)?;
        if bytes == 0 {
            return Ok(Vec::new());
        }
        Ok(entries)
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

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
    use datatypes::json::{JsonSettings, JsonTypeHint};
    use datatypes::types::json_type::{JsonNativeType, JsonObjectType};

    use super::*;
    use crate::region::options::IndexOptions;
    use crate::test_util::sst_util;

    #[test]
    fn test_json_inverted_plan_preserves_build_scope() {
        let mut metadata = sst_util::sst_region_metadata();
        let column = &mut metadata.column_metadatas[2].column_schema;
        column.data_type = ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new()));
        column.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            JsonSettings::try_new(
                vec![JsonTypeHint {
                    path: vec!["value".into()],
                    data_type: ConcreteDataType::int64_datatype(),
                    inverted_index: true,
                    skipping_index: None,
                    fulltext_index: None,
                }],
                Some(0),
            )
            .unwrap(),
        ))));
        let metadata = Arc::new(metadata);
        let options = IndexOptions::default();
        let config = InvertedIndexConfig::default();

        for (build_type, expected_columns) in [
            (IndexBuildType::Flush, vec![0, 2]),
            (IndexBuildType::Compact, vec![0, 2]),
            (IndexBuildType::Manual, vec![0]),
            (IndexBuildType::SchemaChange, vec![0]),
        ] {
            let context = PlanContext {
                metadata: &metadata,
                index_options: &options,
                build_type: build_type.clone(),
                rebuild_source: None,
                existing_indexes: &[],
            };
            let plan = InvertedPlan::plan(&config, &context).unwrap().unwrap();
            let columns: Vec<_> = plan
                .targets
                .iter()
                .map(|target| match target {
                    IndexTarget::ColumnId(id) => *id,
                    IndexTarget::JsonPath(path) => path.column_id(),
                })
                .collect();
            assert_eq!(columns, expected_columns, "{build_type:?}");
        }
    }
}
