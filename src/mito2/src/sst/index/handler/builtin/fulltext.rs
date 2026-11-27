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
use datatypes::schema::FulltextBackend;
use index::target::IndexTarget;

use crate::config::FulltextIndexConfig;
use crate::read::Batch;
use crate::sst::file::FileMeta;
use crate::sst::index::fulltext_index;
use crate::sst::index::fulltext_index::creator::{FulltextIndexer, FulltextTarget};
use crate::sst::index::handler::builtin::{
    builtin_is_current, normalize_entries, target_column_id,
};
use crate::sst::index::handler::{
    BuildContext, IndexCreator, PlanContext, PlanEntry, PlannedIndex,
};
use crate::sst::index::kind::IndexKind;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::{CompletedEntry, IndexBuildType};

/// Resolved column targets and configuration shared across output files.
#[derive(Debug)]
pub(crate) struct FulltextPlan {
    config: FulltextIndexConfig,
    entries: Vec<PlanEntry>,
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
        let mut entries = Vec::new();
        for column in &context.metadata.column_metadatas {
            for (target, options) in
                FulltextIndexer::targets(column, &context.build_type, context.rebuild_source)
                    .map_err(BoxedError::new)?
            {
                entries.push(PlanEntry {
                    column_id: target_column_id(&target),
                    kind: IndexKind::Fulltext,
                    format_version: u32::from(fulltext_format_version(&options.backend)),
                });
                targets.push((
                    column.column_id,
                    column.column_schema.name.clone(),
                    target,
                    options,
                ));
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
impl PlannedIndex for FulltextPlan {
    fn kinds(&self) -> &[IndexKind] {
        &[IndexKind::Fulltext]
    }

    fn entries(&self) -> &[PlanEntry] {
        &self.entries
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

    fn is_current(&self, file: &FileMeta, context: &PlanContext<'_>) -> bool {
        builtin_is_current(
            file,
            context,
            IndexKind::Fulltext,
            self.targets
                .iter()
                .map(|(_, _, target, options)| (target, fulltext_format_version(&options.backend))),
        )
    }
}

fn fulltext_format_version(backend: &FulltextBackend) -> u8 {
    match backend {
        FulltextBackend::Bloom => fulltext_index::FULLTEXT_BLOOM_VERSION,
        FulltextBackend::Tantivy => 0,
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
        let bloom_targets: std::collections::HashSet<_> = self.bloom_indexed_targets().collect();
        let entries = Self::indexed_targets(self)
            .map(|target| CompletedEntry {
                target: target.clone(),
                kind: IndexKind::Fulltext,
                format_version: if bloom_targets.contains(target) {
                    u32::from(fulltext_index::FULLTEXT_BLOOM_VERSION)
                } else {
                    0
                },
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
}
