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

use mito_codec::row_converter::{DensePrimaryKeyCodec, build_primary_key_codec};
use parquet::file::metadata::ParquetMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::sst::parquet::file_range::{PreFilterMode, RangeBase};
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::SimpleFilterContext;
use crate::sst::parquet::stats::RowGroupPruningStats;

pub(crate) type BulkIterContextRef = Arc<BulkIterContext>;

pub struct BulkIterContext {
    pub(crate) base: RangeBase,
    pub(crate) predicate: Option<Predicate>,
}

impl BulkIterContext {
    pub fn new(
        region_metadata: RegionMetadataRef,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        skip_auto_convert: bool,
    ) -> Result<Self> {
        Self::new_with_pre_filter_mode(
            region_metadata,
            projection,
            predicate,
            skip_auto_convert,
            PreFilterMode::All,
        )
    }

    pub fn new_with_pre_filter_mode(
        region_metadata: RegionMetadataRef,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        skip_auto_convert: bool,
        pre_filter_mode: PreFilterMode,
    ) -> Result<Self> {
        let codec = build_primary_key_codec(&region_metadata);

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

        let read_format = ReadFormat::new(
            region_metadata.clone(),
            projection,
            true,
            None,
            "memtable",
            skip_auto_convert,
        )?;

        let dyn_filters = predicate
            .as_ref()
            .map(|pred| pred.dyn_filters().clone())
            .unwrap_or_default();

        Ok(Self {
            base: RangeBase {
                filters: simple_filters,
                dyn_filters,
                read_format,
                prune_schema: region_metadata.schema.clone(),
                expected_metadata: Some(region_metadata),
                codec,
                // we don't need to compat batch since all batch in memtable have the same schema.
                compat_batch: None,
                compaction_projection_mapper: None,
                pre_filter_mode,
                partition_filter: None,
            },
            predicate,
        })
    }

    /// Prunes row groups by stats.
    pub(crate) fn row_groups_to_read(
        &self,
        file_meta: &Arc<ParquetMetaData>,
        skip_fields: bool,
    ) -> VecDeque<usize> {
        let region_meta = self.base.read_format.metadata();
        let row_groups = file_meta.row_groups();
        // expected_metadata is set to None since we always expect region metadata of memtable is up-to-date.
        let stats =
            RowGroupPruningStats::new(row_groups, &self.base.read_format, None, skip_fields);
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

    /// Returns the pre-filter mode.
    pub(crate) fn pre_filter_mode(&self) -> PreFilterMode {
        self.base.pre_filter_mode
    }

    /// Returns the region id.
    pub(crate) fn region_id(&self) -> store_api::storage::RegionId {
        self.base.read_format.metadata().region_id
    }
}
