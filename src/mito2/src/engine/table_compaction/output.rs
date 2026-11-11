// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;

use crate::access_layer::AccessLayer;
use crate::cache::CachedSstMeta;
use crate::engine::region_hook::VerifiedSstFileInfo;
use crate::error::{InvalidRequestSnafu, OpenDalSnafu, Result};
use crate::region::MitoRegionRef;
use crate::sst::file::FileMeta;
use crate::sst::location;
use crate::sst::parquet::metadata::{MetadataLoader, extract_primary_key_range};

/// Verifies every output, then notifies the region hook once if one is registered.
///
/// Without a hook no footer is needed after its own verification, so each one is
/// dropped before the next output is read. With a hook every footer is kept until
/// all outputs verify, because the hook observes them in one call; footers are then
/// released when this returns, before the caller takes the manifest lock. A failed
/// verification notifies nothing.
pub(crate) async fn verify_and_notify(
    region: &MitoRegionRef,
    metadata: &RegionMetadataRef,
    outputs: &[FileMeta],
) -> Result<()> {
    let Some(hook) = region.manifest_ctx.hook() else {
        for file in outputs {
            verify_file(&region.access_layer, metadata, file).await?;
        }
        return Ok(());
    };

    let mut footers = Vec::with_capacity(outputs.len());
    for file in outputs {
        footers.push(verify_file(&region.access_layer, metadata, file).await?);
    }
    let verified = outputs
        .iter()
        .zip(&footers)
        .map(|(file_meta, footer)| VerifiedSstFileInfo { file_meta, footer })
        .collect::<Vec<_>>();
    hook.on_sst_files_verified(region.region_id, metadata, &verified)
        .await;
    Ok(())
}

async fn verify_file(
    access: &AccessLayer,
    metadata: &RegionMetadataRef,
    file: &FileMeta,
) -> Result<Arc<ParquetMetaData>> {
    let region_id = metadata.region_id;
    let path = location::sst_file_path(access.table_dir(), file.file_id(), access.path_type());
    let size = access
        .object_store()
        .stat(&path)
        .await
        .context(OpenDalSnafu)?
        .content_length();
    ensure!(
        size == file.file_size,
        InvalidRequestSnafu {
            region_id,
            reason: "Output SST size mismatch"
        }
    );
    let footer = MetadataLoader::new(access.object_store().clone(), &path, size)
        .load(&mut Default::default())
        .await?;
    let cached = CachedSstMeta::try_new_with_region_metadata(&path, footer, None)?;
    ensure!(
        cached.region_metadata() == *metadata,
        InvalidRequestSnafu {
            region_id,
            reason: "Output footer schema or partition mismatch"
        }
    );
    let footer = cached.parquet_metadata();
    verify_statistics(&footer, metadata, file)?;
    if file.index_file_size > 0 {
        let path =
            location::index_file_path(access.table_dir(), file.index_id(), access.path_type());
        let index_size = access
            .object_store()
            .stat(&path)
            .await
            .context(OpenDalSnafu)?
            .content_length();
        ensure!(
            index_size == file.index_file_size,
            InvalidRequestSnafu {
                region_id,
                reason: "Output index size mismatch"
            }
        );
    } else {
        ensure!(
            file.available_indexes.is_empty() && file.indexes.is_empty(),
            InvalidRequestSnafu {
                region_id,
                reason: "Output advertises indexes without an index object",
            }
        );
    }
    Ok(footer)
}

fn verify_statistics(
    footer: &ParquetMetaData,
    metadata: &RegionMetadataRef,
    file: &FileMeta,
) -> Result<()> {
    let max_uncompressed = footer
        .row_groups()
        .iter()
        .map(|group| {
            group
                .columns()
                .iter()
                .map(|column| column.uncompressed_size() as u64)
                .sum::<u64>()
        })
        .max()
        .unwrap_or(0);
    let pk_range = extract_primary_key_range(footer, metadata);
    ensure!(
        footer.file_metadata().num_rows() > 0
            && footer.file_metadata().num_rows() as u64 == file.num_rows
            && footer.num_row_groups() as u64 == file.num_row_groups
            && max_uncompressed == file.max_row_group_uncompressed_size
            && pk_range.as_ref().map(|range| &range.0) == file.primary_key_min.as_ref()
            && pk_range.as_ref().map(|range| &range.1) == file.primary_key_max.as_ref()
            // `num_series` is the writer's caller-reported estimate derived from
            // timestamp boundaries, not a footer statistic, so it only bounds the
            // count against rows instead of being verified against the footer.
            && file.num_series <= file.num_rows,
        InvalidRequestSnafu {
            region_id: metadata.region_id,
            reason: "Output statistics differ from its footer"
        }
    );
    verify_time_range(footer, metadata, file)
}

fn verify_time_range(
    footer: &ParquetMetaData,
    metadata: &RegionMetadataRef,
    file: &FileMeta,
) -> Result<()> {
    let region_id = metadata.region_id;
    let column_name = &metadata.time_index_column().column_schema.name;
    let index = footer
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.path().parts().len() == 1 && column.name() == column_name)
        .context(InvalidRequestSnafu {
            region_id,
            reason: "Output has no timestamp column",
        })?;
    let mut minimum = i64::MAX;
    let mut maximum = i64::MIN;
    for group in footer.row_groups() {
        let Some(Statistics::Int64(stats)) = group.column(index).statistics() else {
            return InvalidRequestSnafu {
                region_id,
                reason: "Output timestamp statistics missing",
            }
            .fail();
        };
        minimum = minimum.min(*stats.min_opt().context(InvalidRequestSnafu {
            region_id,
            reason: "Output timestamp minimum missing",
        })?);
        maximum = maximum.max(*stats.max_opt().context(InvalidRequestSnafu {
            region_id,
            reason: "Output timestamp maximum missing",
        })?);
    }
    let unit = metadata.time_index_type().unit();
    ensure!(
        file.time_range.0.value() == minimum
            && file.time_range.1.value() == maximum
            && file.time_range.0.unit() == unit
            && file.time_range.1.unit() == unit,
        InvalidRequestSnafu {
            region_id,
            reason: "Output timestamp range mismatch"
        }
    );
    Ok(())
}
