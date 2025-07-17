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

use std::sync::Arc;

use itertools::Itertools;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error;
use crate::memtable::simple_bulk_memtable::{BatchIterBuilder, SimpleBulkMemtable};
use crate::memtable::{Memtable, MemtableRange, MemtableRangeContext, MemtableRanges};
use crate::read::scan_region::PredicateGroup;

impl SimpleBulkMemtable {
    pub fn region_metadata(&self) -> RegionMetadataRef {
        self.region_metadata.clone()
    }

    pub fn ranges_sequential(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> error::Result<MemtableRanges> {
        let projection = Arc::new(self.build_projection(projection));
        let values = self.series.read().unwrap().read_to_values();
        let contexts = values
            .into_iter()
            .filter_map(|v| {
                let filtered = match v
                    .to_batch(&[], &self.region_metadata, &projection, self.dedup)
                    .and_then(|mut b| {
                        b.filter_by_sequence(sequence)?;
                        Ok(b)
                    }) {
                    Ok(filtered) => filtered,
                    Err(e) => {
                        return Some(Err(e));
                    }
                };
                if filtered.is_empty() {
                    None
                } else {
                    Some(Ok(filtered))
                }
            })
            .map_ok(|batch| {
                let builder = BatchIterBuilder {
                    batch,
                    merge_mode: self.merge_mode,
                };
                Arc::new(MemtableRangeContext::new(
                    self.id,
                    Box::new(builder),
                    predicate.clone(),
                ))
            })
            .collect::<error::Result<Vec<_>>>()?;

        let ranges = contexts
            .into_iter()
            .enumerate()
            .map(|(idx, context)| (idx, MemtableRange::new(context)))
            .collect();

        Ok(MemtableRanges {
            ranges,
            stats: self.stats(),
        })
    }
}
