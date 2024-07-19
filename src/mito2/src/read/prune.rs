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

use crate::error::Result;
use crate::read::last_row::RowGroupLastRowCachedReader;
use crate::read::{Batch, BatchReader};
use crate::sst::parquet::file_range::FileRangeContextRef;
use crate::sst::parquet::reader::{ReaderMetrics, RowGroupReader};

pub enum Source {
    RowGroup(RowGroupReader),
    LastRow(RowGroupLastRowCachedReader),
}

impl Source {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::RowGroup(r) => r.next_batch().await,
            Source::LastRow(r) => r.next_batch().await,
        }
    }
}

pub struct PruneReader {
    /// Context for file ranges.
    context: FileRangeContextRef,
    source: Source,
    metrics: ReaderMetrics,
}

impl PruneReader {
    pub(crate) fn new_with_row_group_reader(
        ctx: FileRangeContextRef,
        reader: RowGroupReader,
    ) -> Self {
        Self {
            context: ctx,
            source: Source::RowGroup(reader),
            metrics: Default::default(),
        }
    }

    pub(crate) fn new_with_last_row_reader(
        ctx: FileRangeContextRef,
        reader: RowGroupLastRowCachedReader,
    ) -> Self {
        Self {
            context: ctx,
            source: Source::LastRow(reader),
            metrics: Default::default(),
        }
    }

    pub(crate) fn reset_source(&mut self, source: Source) {
        self.source = source;
    }

    pub(crate) fn metrics(&mut self) -> &ReaderMetrics {
        match &self.source {
            Source::RowGroup(r) => r.metrics(),
            Source::LastRow(_) => &self.metrics,
        }
    }

    pub(crate) async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(b) = self.source.next_batch().await? {
            match self.prune(b)? {
                Some(b) => {
                    return Ok(Some(b));
                }
                None => {
                    continue;
                }
            }
        }
        Ok(None)
    }

    /// Prunes batches by the pushed down predicate.
    fn prune(&mut self, batch: Batch) -> Result<Option<Batch>> {
        // fast path
        if self.context.filters().is_empty() {
            return Ok(Some(batch));
        }

        let num_rows_before_filter = batch.num_rows();
        let Some(batch_filtered) = self.context.precise_filter(batch)? else {
            // the entire batch is filtered out
            self.metrics.num_rows_precise_filtered += num_rows_before_filter;
            return Ok(None);
        };

        // update metric
        let filtered_rows = num_rows_before_filter - batch_filtered.num_rows();
        self.metrics.num_rows_precise_filtered += filtered_rows;

        if !batch_filtered.is_empty() {
            Ok(Some(batch_filtered))
        } else {
            Ok(None)
        }
    }
}
