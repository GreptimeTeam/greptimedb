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

//! Sync dedup reader implementation

use common_telemetry::debug;

use crate::metrics::MERGE_FILTER_ROWS_TOTAL;
use crate::read::dedup::{DedupMetrics, DedupStrategy};
use crate::read::Batch;

/// A sync version of reader that dedup sorted batches from a source based on the
/// dedup strategy.
pub(crate) struct DedupReader<R, S> {
    source: R,
    strategy: S,
    metrics: DedupMetrics,
}

impl<R, S> DedupReader<R, S> {
    /// Creates a new dedup reader.
    pub(crate) fn new(source: R, strategy: S) -> Self {
        Self {
            source,
            strategy,
            metrics: DedupMetrics::default(),
        }
    }
}

impl<R: Iterator<Item = crate::error::Result<Batch>>, S: DedupStrategy> DedupReader<R, S> {
    /// Returns the next deduplicated batch.
    fn fetch_next_batch(&mut self) -> Option<crate::error::Result<Batch>> {
        while let Some(res) = self.source.next() {
            match res {
                Ok(batch) => {
                    if let Some(batch) = self
                        .strategy
                        .push_batch(batch, &mut self.metrics)
                        .transpose()
                    {
                        return Some(batch);
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
        self.strategy.finish(&mut self.metrics).transpose()
    }
}

impl<R: Iterator<Item = crate::error::Result<Batch>>, S: DedupStrategy> Iterator
    for DedupReader<R, S>
{
    type Item = crate::error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_next_batch()
    }
}

impl<R, S> Drop for DedupReader<R, S> {
    fn drop(&mut self) {
        debug!("Sync dedup reader finished, metrics: {:?}", self.metrics);

        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["dedup"])
            .inc_by(self.metrics.num_unselected_rows as u64);
        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(self.metrics.num_unselected_rows as u64);
    }
}
