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

//! Context for iterating bulk memtable.

use std::collections::VecDeque;
use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::row_converter::DensePrimaryKeyCodec;
use crate::sst::parquet::file_range::RangeBase;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::SimpleFilterContext;
use crate::sst::parquet::stats::RowGroupPruningStats;

pub(crate) type BulkIterContextRef = Arc<BulkIterContext>;

pub(crate) struct BulkIterContext {
    pub(crate) base: RangeBase,
    pub(crate) predicate: Option<Predicate>,
}

impl BulkIterContext {
    pub(crate) fn new(
        region_metadata: RegionMetadataRef,
        projection: &Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) -> Self {
        let codec = DensePrimaryKeyCodec::new(&region_metadata);

        let simple_filters = predicate
            .as_ref()
            .iter()
            .flat_map(|predicate| {
                predicate
                    .exprs()
                    .iter()
                    .filter_map(|expr| SimpleFilterContext::new_opt(&region_metadata, None, expr))
            })
            .collect();

        let read_format = build_read_format(region_metadata, projection);

        Self {
            base: RangeBase {
                filters: simple_filters,
                read_format,
                codec,
                // we don't need to compat batch since all batch in memtable have the same schema.
                compat_batch: None,
            },
            predicate,
        }
    }

    /// Prunes row groups by stats.
    pub(crate) fn row_groups_to_read(&self, file_meta: &Arc<ParquetMetaData>) -> VecDeque<usize> {
        let region_meta = self.base.read_format.metadata();
        let row_groups = file_meta.row_groups();
        // expected_metadata is set to None since we always expect region metadata of memtable is up-to-date.
        let stats = RowGroupPruningStats::new(row_groups, &self.base.read_format, None);
        if let Some(predicate) = self.predicate.as_ref() {
            predicate
                .prune_with_stats(&stats, region_meta.schema.arrow_schema())
                .iter()
                .zip(0..file_meta.num_row_groups())
                .filter_map(|(selected, row_group)| {
                    if !*selected {
                        return None;
                    }
                    Some(row_group)
                })
                .collect::<VecDeque<_>>()
        } else {
            (0..file_meta.num_row_groups()).collect()
        }
    }

    pub(crate) fn read_format(&self) -> &ReadFormat {
        &self.base.read_format
    }
}

fn build_read_format(
    region_metadata: RegionMetadataRef,
    projection: &Option<&[ColumnId]>,
) -> ReadFormat {
    let read_format = if let Some(column_ids) = &projection {
        ReadFormat::new(region_metadata, column_ids.iter().copied())
    } else {
        // No projection, lists all column ids to read.
        ReadFormat::new(
            region_metadata.clone(),
            region_metadata
                .column_metadatas
                .iter()
                .map(|col| col.column_id),
        )
    };

    read_format
}
