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

use common_recordbatch::filter::SimpleFilterEvaluator;
use mito_codec::row_converter::build_primary_key_codec;
use parquet::file::metadata::ParquetMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::sst::parquet::file_range::{PreFilterMode, RangeBase};
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::prefilter::{CachedPrimaryKeyFilter, is_usable_primary_key_filter};
use crate::sst::parquet::reader::SimpleFilterContext;
use crate::sst::parquet::stats::RowGroupPruningStats;

pub(crate) type BulkIterContextRef = Arc<BulkIterContext>;

struct BulkFilterPlan {
    remaining_simple_filters: Vec<SimpleFilterContext>,
    pk_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
}

pub struct BulkIterContext {
    pub(crate) base: RangeBase,
    pub(crate) predicate: Option<Predicate>,
    /// Pre-extracted primary key filters for PK prefiltering.
    /// `None` if PK prefiltering is not applicable.
    pk_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
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

        let read_format = if let Some(column_ids) = projection {
            FlatReadFormat::new(
                region_metadata.clone(),
                column_ids.iter().copied(),
                None,
                "memtable",
                skip_auto_convert,
            )?
        } else {
            FlatReadFormat::new(
                region_metadata.clone(),
                region_metadata
                    .column_metadatas
                    .iter()
                    .map(|col| col.column_id),
                None,
                "memtable",
                skip_auto_convert,
            )?
        };

        let dyn_filters = predicate
            .as_ref()
            .map(|pred| pred.dyn_filters().as_ref().clone())
            .unwrap_or_default();

        let filter_plan =
            Self::build_filter_plan(&region_metadata, &read_format, predicate.as_ref());

        Ok(Self {
            base: RangeBase {
                filters: filter_plan.remaining_simple_filters,
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
            pk_filters: filter_plan.pk_filters,
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

    fn build_filter_plan(
        region_metadata: &RegionMetadataRef,
        read_format: &FlatReadFormat,
        predicate: Option<&Predicate>,
    ) -> BulkFilterPlan {
        let simple_filters: Vec<SimpleFilterContext> = predicate
            .into_iter()
            .flat_map(|predicate| {
                predicate
                    .exprs()
                    .iter()
                    .filter_map(|expr| SimpleFilterContext::new_opt(region_metadata, None, expr))
            })
            .collect();

        let metadata = read_format.metadata();
        if read_format.batch_has_raw_pk_columns() || metadata.primary_key.is_empty() {
            return BulkFilterPlan {
                remaining_simple_filters: simple_filters,
                pk_filters: None,
            };
        }

        let mut remaining_simple_filters = Vec::new();
        let mut pk_filters = Vec::new();

        for filter_ctx in simple_filters {
            let pk_filter = filter_ctx.filter().as_filter().and_then(|filter| {
                is_usable_primary_key_filter(metadata, None, filter).then(|| filter.clone())
            });

            if let Some(pk_filter) = pk_filter {
                pk_filters.push(pk_filter);
            } else {
                remaining_simple_filters.push(filter_ctx);
            }
        }

        BulkFilterPlan {
            remaining_simple_filters,
            pk_filters: (!pk_filters.is_empty()).then_some(Arc::new(pk_filters)),
        }
    }

    /// Builds a fresh PK filter for a new iterator. Returns `None` if PK
    /// prefiltering is not applicable.
    pub(crate) fn build_pk_filter(&self) -> Option<CachedPrimaryKeyFilter> {
        let pk_filters = self.pk_filters.as_ref()?;
        let metadata = self.base.read_format.metadata();
        // Parquet PK prefilter always supports the partition column.
        let inner = self
            .base
            .codec
            .primary_key_filter(metadata, Arc::clone(pk_filters), false);
        Some(CachedPrimaryKeyFilter::new(inner))
    }

    pub(crate) fn read_format(&self) -> &FlatReadFormat {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::{col, lit};
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::test_util::sst_util::{sst_region_metadata, sst_region_metadata_with_encoding};

    #[test]
    fn test_build_filter_plan_splits_pk_prefiltered_simple_filters_on_legacy_path() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "memtable",
            false,
        )
        .unwrap();
        assert!(!read_format.batch_has_raw_pk_columns());
        let predicate = Predicate::new(vec![
            col("tag_0").eq(lit("a")),
            col("field_0").gt(lit(1_u64)),
        ]);

        let plan = BulkIterContext::build_filter_plan(&metadata, &read_format, Some(&predicate));

        assert!(plan.pk_filters.is_some());
        assert_eq!(plan.pk_filters.as_ref().unwrap().len(), 1);
        assert_eq!(plan.remaining_simple_filters.len(), 1);
        let remaining_filter = plan.remaining_simple_filters[0]
            .filter()
            .as_filter()
            .unwrap();
        assert_eq!(remaining_filter.column_name(), "field_0");
    }

    #[test]
    fn test_build_filter_plan_keeps_tag_filters_in_range_base_without_pk_prefilter_support() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "memtable",
            true,
        )
        .unwrap();
        assert!(read_format.batch_has_raw_pk_columns());
        let predicate = Predicate::new(vec![col("tag_0").eq(lit("a"))]);

        let plan = BulkIterContext::build_filter_plan(&metadata, &read_format, Some(&predicate));

        assert!(plan.pk_filters.is_none());
        assert_eq!(plan.remaining_simple_filters.len(), 1);
        let remaining_filter = plan.remaining_simple_filters[0]
            .filter()
            .as_filter()
            .unwrap();
        assert_eq!(remaining_filter.column_name(), "tag_0");
    }

    #[test]
    fn test_build_filter_plan_keeps_non_pk_filters_in_range_base() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "memtable",
            true,
        )
        .unwrap();
        let predicate = Predicate::new(vec![col("field_0").gt(lit(1_u64))]);

        let plan = BulkIterContext::build_filter_plan(&metadata, &read_format, Some(&predicate));

        assert!(plan.pk_filters.is_none());
        assert_eq!(plan.remaining_simple_filters.len(), 1);
        let remaining_filter = plan.remaining_simple_filters[0]
            .filter()
            .as_filter()
            .unwrap();
        assert_eq!(remaining_filter.column_name(), "field_0");
    }
}
