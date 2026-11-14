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

//! Planning and creator adapter for the bloom index.

use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::SkippingIndexOptions;
use index::target::IndexTarget;

use crate::config::BloomFilterConfig;
use crate::read::Batch;
use crate::sst::index::bloom_filter::creator::BloomFilterIndexer;
use crate::sst::index::handler::builtin::{column_target_id, normalize_entries, target_column_id};
use crate::sst::index::handler::{
    BuildContext, IndexCreator, PlanContext, PlanEntry, PlannedIndex,
};
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// Resolved column targets and configuration shared across output files.
#[derive(Debug)]
pub(crate) struct BloomPlan {
    config: BloomFilterConfig,
    entries: Vec<PlanEntry>,
    targets: Vec<(IndexTarget, SkippingIndexOptions)>,
}

impl BloomPlan {
    /// Resolves targets without opening SST data or creating output files.
    pub(crate) fn plan(
        config: &BloomFilterConfig,
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
        let mut entries = Vec::new();
        for column in &context.metadata.column_metadatas {
            for (target, options) in
                BloomFilterIndexer::targets(column, &context.build_type).map_err(BoxedError::new)?
            {
                entries.push(PlanEntry {
                    column_id: target_column_id(&target),
                    kind: IndexKind::Bloom,
                    format_version: 0,
                });
                targets.push((target, options));
            }
        }
        normalize_entries(&mut entries);
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            config: config.clone(),
            entries,
            targets,
        })))
    }
}

#[async_trait]
impl PlannedIndex for BloomPlan {
    fn kinds(&self) -> &[IndexKind] {
        &[IndexKind::Bloom]
    }

    fn entries(&self) -> &[PlanEntry] {
        &self.entries
    }

    async fn build(
        &self,
        context: &BuildContext<'_>,
    ) -> Result<Option<Box<dyn IndexCreator>>, BoxedError> {
        BloomFilterIndexer::new_with_targets(
            context.output.file_id(),
            context.metadata,
            context.intermediate_manager.clone(),
            self.config.mem_threshold_on_create(),
            self.targets.clone(),
        )
        .map(|creator| creator.map(|c| Box::new(c) as Box<dyn IndexCreator>))
        .map_err(BoxedError::new)
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
                kind: IndexKind::Bloom,
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
