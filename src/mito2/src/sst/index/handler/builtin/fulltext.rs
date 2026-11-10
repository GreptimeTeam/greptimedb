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

//! Planning and creator adapter for the fulltext index.

use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use datatypes::arrow::record_batch::RecordBatch;
use index::target::IndexTarget;

use crate::config::FulltextIndexConfig;
use crate::read::Batch;
use crate::sst::index::fulltext_index;
use crate::sst::index::fulltext_index::creator::{FulltextIndexer, FulltextTarget};
use crate::sst::index::handler::builtin::column_target_id;
use crate::sst::index::handler::{BuildContext, IndexCreator, PlanContext, PlannedIndex};
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// Resolved column targets and configuration shared across output files.
#[derive(Debug)]
pub(crate) struct FulltextPlan {
    config: FulltextIndexConfig,
    targets: Vec<FulltextTarget>,
}

impl FulltextPlan {
    /// Resolves targets without opening SST data or creating output files.
    pub(crate) fn plan(
        config: &FulltextIndexConfig,
        context: &PlanContext<'_>,
    ) -> Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
        let create = match context.build_type {
            IndexBuildType::Flush => config.create_on_flush.auto(),
            IndexBuildType::Compact => config.create_on_compaction.auto(),
            _ => true,
        };
        if !create {
            return Ok(None);
        }
        let mut targets = Vec::new();
        for column in &context.metadata.column_metadatas {
            for (target, options) in
                FulltextIndexer::targets(column, &context.build_type).map_err(BoxedError::new)?
            {
                targets.push((
                    column.column_id,
                    column.column_schema.name.clone(),
                    target,
                    options,
                ));
            }
        }
        if targets.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            config: config.clone(),
            targets,
        })))
    }
}

#[async_trait]
impl PlannedIndex for FulltextPlan {
    fn kind(&self) -> IndexKind {
        IndexKind::Fulltext
    }

    async fn build(
        &self,
        context: &BuildContext<'_>,
    ) -> Result<Option<Box<dyn IndexCreator>>, BoxedError> {
        FulltextIndexer::new_with_targets(
            &context.metadata.region_id,
            &context.output.file_id(),
            context.intermediate_manager,
            context.metadata,
            self.config.compress,
            self.config.mem_threshold_on_create(),
            self.targets.clone(),
        )
        .await
        .map(|creator| creator.map(|c| Box::new(c) as Box<dyn IndexCreator>))
        .map_err(BoxedError::new)
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
                kind: IndexKind::Fulltext,
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
