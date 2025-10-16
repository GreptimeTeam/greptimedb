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

use std::collections::HashSet;
use std::time::Instant;

use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceRange};

use crate::error;
use crate::memtable::simple_bulk_memtable::{Iter, SimpleBulkMemtable};
use crate::memtable::time_series::Values;
use crate::memtable::{BoxedBatchIterator, IterBuilder, MemScanMetrics};
use crate::read::dedup::LastNonNullIter;
use crate::region::options::MergeMode;

impl SimpleBulkMemtable {
    pub fn region_metadata(&self) -> RegionMetadataRef {
        self.region_metadata.clone()
    }

    pub(crate) fn create_iter(
        &self,
        projection: Option<&[ColumnId]>,
        sequence: Option<SequenceRange>,
    ) -> error::Result<BatchIterBuilderDeprecated> {
        let mut series = self.series.write().unwrap();

        let values = if series.is_empty() {
            None
        } else {
            Some(series.compact(&self.region_metadata)?.clone())
        };
        let projection = self.build_projection(projection);
        Ok(BatchIterBuilderDeprecated {
            region_metadata: self.region_metadata.clone(),
            values,
            projection,
            dedup: self.dedup,
            sequence,
            merge_mode: self.merge_mode,
        })
    }
}

#[derive(Clone)]
pub(crate) struct BatchIterBuilderDeprecated {
    region_metadata: RegionMetadataRef,
    values: Option<Values>,
    projection: HashSet<ColumnId>,
    sequence: Option<SequenceRange>,
    dedup: bool,
    merge_mode: MergeMode,
}

impl IterBuilder for BatchIterBuilderDeprecated {
    fn build(&self, metrics: Option<MemScanMetrics>) -> error::Result<BoxedBatchIterator> {
        let start_time = Instant::now();
        let Some(values) = self.values.clone() else {
            return Ok(Box::new(Iter { batch: None }));
        };

        let maybe_batch = values
            .to_batch(&[], &self.region_metadata, &self.projection, self.dedup)
            .and_then(|mut b| {
                b.filter_by_sequence(self.sequence)?;
                Ok(b)
            })
            .map(Some)
            .transpose();

        // Collect metrics from the batch
        if let Some(metrics) = metrics {
            let (num_rows, num_batches) = match &maybe_batch {
                Some(Ok(batch)) => (batch.num_rows(), 1),
                _ => (0, 0),
            };
            let inner = crate::memtable::MemScanMetricsData {
                total_series: 1,
                num_rows,
                num_batches,
                scan_cost: start_time.elapsed(),
            };
            metrics.merge_inner(&inner);
        }

        let iter = Iter { batch: maybe_batch };

        if self.merge_mode == MergeMode::LastNonNull {
            Ok(Box::new(LastNonNullIter::new(iter)))
        } else {
            Ok(Box::new(iter))
        }
    }
}
