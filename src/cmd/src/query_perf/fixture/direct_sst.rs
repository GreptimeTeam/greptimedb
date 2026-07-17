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

use std::collections::HashMap;
use std::fs::{self};
use std::io::{BufWriter, Write};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use async_trait::async_trait;
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, Float64Array, RecordBatch, StringDictionaryBuilder,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
use mito2::access_layer::{FilePathProvider, Metrics, WriteType};
use mito2::config::IndexConfig;
use mito2::manifest::action::{RegionCheckpoint, RegionManifest, RemovedFilesRecord};
use mito2::read::FlatSource;
use mito2::sst::file::{FileMeta, RegionFileId};
use mito2::sst::index::{Indexer, IndexerBuilder};
use mito2::sst::parquet::writer::ParquetWriter;
use mito2::sst::parquet::{SstInfo, WriteOptions};
use mito2::sst::{
    DEFAULT_WRITE_BUFFER_SIZE, FlatSchemaOptions, FormatType, to_flat_sst_arrow_schema,
};
use object_store::ObjectStore;
use object_store::services::Fs as FsBuilder;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::path_utils::region_name;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use crate::query_perf::case::*;
use crate::query_perf::error::{Error, Result};
use crate::query_perf::fixture::{DirectSstRequest, DirectSstSummary};

struct NoopIndexBuilder;

#[async_trait]
impl IndexerBuilder for NoopIndexBuilder {
    async fn build(
        &self,
        _file_id: FileId,
        _index_version: u64,
        _row_group_size: Option<usize>,
    ) -> Indexer {
        Indexer::default()
    }
}

#[derive(Clone)]
struct FixedPathProvider {
    table_dir: String,
}

impl FilePathProvider for FixedPathProvider {
    fn build_index_file_path(&self, file_id: RegionFileId) -> String {
        mito2::sst::location::index_file_path_legacy(&self.table_dir, file_id, PathType::Bare)
    }
    fn build_index_file_path_with_version(
        &self,
        index_id: mito2::sst::file::RegionIndexId,
    ) -> String {
        mito2::sst::location::index_file_path(&self.table_dir, index_id, PathType::Bare)
    }
    fn build_sst_file_path(&self, file_id: RegionFileId) -> String {
        mito2::sst::location::sst_file_path(&self.table_dir, file_id, PathType::Bare)
    }
}

fn semantic_type(semantic: ValidatedSemanticType) -> SemanticType {
    match semantic {
        ValidatedSemanticType::Tag => SemanticType::Tag,
        ValidatedSemanticType::Field => SemanticType::Field,
        ValidatedSemanticType::Timestamp => SemanticType::Timestamp,
    }
}

fn concrete_type(ty: ValidatedColumnType) -> ConcreteDataType {
    match ty {
        ValidatedColumnType::String => ConcreteDataType::string_datatype(),
        ValidatedColumnType::Float64 => ConcreteDataType::float64_datatype(),
        ValidatedColumnType::Uint64 => ConcreteDataType::uint64_datatype(),
        ValidatedColumnType::TimestampNanosecond => {
            ConcreteDataType::timestamp_nanosecond_datatype()
        }
        ValidatedColumnType::TimestampMillisecond => {
            ConcreteDataType::timestamp_millisecond_datatype()
        }
    }
}

fn build_region_metadata(table: &ValidatedTable, region_id: RegionId) -> Result<RegionMetadata> {
    let mut builder = store_api::metadata::RegionMetadataBuilder::new(region_id);
    for (idx, col) in table.columns().iter().enumerate() {
        let mut schema = ColumnSchema::new(
            col.name(),
            concrete_type(col.ty()),
            col.semantic() != ValidatedSemanticType::Timestamp,
        );
        if col.semantic() == ValidatedSemanticType::Tag {
            schema = schema.with_inverted_index(true);
        }
        if matches!(
            col.semantic(),
            ValidatedSemanticType::Tag | ValidatedSemanticType::Field
        ) {
            schema = schema
                .with_skipping_options(SkippingIndexOptions {
                    granularity: 1,
                    ..Default::default()
                })
                .map_err(|err| Error::new(format!("failed to set skipping options: {err}")))?;
        }
        if col.name() == table.time_index() {
            schema = schema.with_time_index(true);
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: schema,
            semantic_type: semantic_type(col.semantic()),
            column_id: u32::try_from(idx)
                .map_err(|_| Error::new("fixture column index exceeds u32"))?,
        });
    }
    let pk_ids = table
        .primary_key()
        .iter()
        .map(|name| {
            table
                .columns()
                .iter()
                .position(|c| c.name() == name)
                .ok_or_else(|| Error::new(format!("primary key column {name} not found")))
                .and_then(|index| {
                    u32::try_from(index).map_err(|_| Error::new("primary key index exceeds u32"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    builder.primary_key(pk_ids);
    builder.primary_key_encoding(PrimaryKeyEncoding::Dense);
    builder
        .build()
        .map_err(|err| Error::new(format!("failed to build fixture metadata: {err}")))
}

fn encode_dense_primary_key(
    table: &ValidatedTable,
    values: &HashMap<String, String>,
) -> Result<Vec<u8>> {
    let fields = table
        .primary_key()
        .iter()
        .enumerate()
        .map(|(idx, _)| -> Result<_> {
            Ok((
                u32::try_from(idx)
                    .map_err(|_| Error::new("primary key field index exceeds u32"))?,
                SortField::new(ConcreteDataType::string_datatype()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let converter = DensePrimaryKeyCodec::with_fields(fields);
    converter
        .encode(
            table
                .primary_key()
                .iter()
                .map(|name| {
                    values
                        .get(name)
                        .map(|value| datatypes::value::ValueRef::String(value.as_str()))
                        .ok_or_else(|| Error::new(format!("primary key value {name} is missing")))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
        )
        .map_err(|err| Error::new(format!("failed to encode dense primary key: {err}")))
}

fn tag_value(col: &ValidatedColumn, series: usize) -> Result<String> {
    match col.distribution() {
        Some(ValidatedDistribution::Cardinality { values, prefix }) => Ok(format!(
            "{}{}",
            prefix,
            u64::try_from(series).map_err(|_| Error::new("series index exceeds u64"))? % values
        )),
        _ => Err(Error::new(format!(
            "tag column {} requires cardinality distribution",
            col.name()
        ))),
    }
}

fn series_for_row(
    layout: &ValidatedLayout,
    sst_idx: usize,
    base_row: usize,
    row: usize,
) -> Result<usize> {
    match layout.series_layout() {
        ValidatedSeriesLayout::RoundRobin | ValidatedSeriesLayout::TimestampMajor => base_row
            .checked_add(row)
            .map(|value| value % layout.series_count())
            .ok_or_else(|| Error::new("fixture row index overflows")),
        ValidatedSeriesLayout::PerSst => Ok(sst_idx % layout.series_count()),
    }
}

fn timestamp_for_row(layout: &ValidatedLayout, base_row: usize, row: usize) -> Result<i64> {
    let logical_row = base_row
        .checked_add(row)
        .ok_or_else(|| Error::new("fixture row index overflows"))?;
    let timestamp_step = if layout.series_layout() == ValidatedSeriesLayout::TimestampMajor {
        logical_row / layout.series_count()
    } else {
        logical_row
    };

    let step = i64::try_from(timestamp_step)
        .map_err(|_| Error::new("fixture timestamp step exceeds i64"))?;
    layout
        .start_unix_nanos()
        .checked_add(
            step.checked_mul(layout.step_nanos())
                .ok_or_else(|| Error::new("fixture timestamp multiplication overflows"))?,
        )
        .ok_or_else(|| Error::new("fixture timestamp overflows"))
}

fn wave_value(min: f64, max: f64, row: usize) -> f64 {
    let span = max - min;
    let phase = (row % 1024) as f64 / 1023.0;
    min + span * (0.5 - 0.5 * (std::f64::consts::TAU * phase).cos())
}

fn generate_record_batch(
    table: &ValidatedTable,
    metadata: &RegionMetadataRef,
    layout: &ValidatedLayout,
    sst_idx: usize,
    sequence: u64,
) -> Result<RecordBatch> {
    let flat_schema = to_flat_sst_arrow_schema(metadata, &FlatSchemaOptions::default());
    let rows = layout.rows_per_sst();
    let mut columns = Vec::new();
    let base_row = sst_idx
        .checked_mul(rows)
        .ok_or_else(|| Error::new("fixture base row overflows"))?;

    for col in table.columns() {
        match (col.semantic(), col.ty()) {
            (ValidatedSemanticType::Tag, ValidatedColumnType::String) => {
                let mut b = StringDictionaryBuilder::<UInt32Type>::new();
                for row in 0..rows {
                    let series = series_for_row(layout, sst_idx, base_row, row)?;
                    let value = tag_value(col, series)?;
                    b.append_value(value);
                }
                columns.push(Arc::new(b.finish()) as ArrayRef);
            }
            (ValidatedSemanticType::Field, ValidatedColumnType::Float64) => {
                let (min, max) = match col.distribution() {
                    Some(ValidatedDistribution::DeterministicWave { min, max }) => (*min, *max),
                    _ => (0.0, 1.0),
                };
                columns.push(Arc::new(Float64Array::from(
                    (0..rows)
                        .map(|r| wave_value(min, max, base_row + r))
                        .collect::<Vec<_>>(),
                )) as ArrayRef);
            }
            (ValidatedSemanticType::Field, ValidatedColumnType::Uint64) => {
                columns.push(Arc::new(UInt64Array::from(
                    (0..rows)
                        .map(|r| {
                            base_row
                                .checked_add(r)
                                .ok_or_else(|| Error::new("fixture field ordinal overflows"))
                                .and_then(|value| {
                                    u64::try_from(value).map_err(|_| {
                                        Error::new("fixture field ordinal exceeds u64")
                                    })
                                })
                        })
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef)
            }
            (ValidatedSemanticType::Timestamp, ValidatedColumnType::TimestampNanosecond) => columns
                .push(Arc::new(TimestampNanosecondArray::from(
                    (0..rows)
                        .map(|r| timestamp_for_row(layout, base_row, r))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef),
            (ValidatedSemanticType::Timestamp, ValidatedColumnType::TimestampMillisecond) => {
                columns.push(Arc::new(TimestampMillisecondArray::from(
                    (0..rows)
                        .map(|r| {
                            timestamp_for_row(layout, base_row, r).map(|value| value / 1_000_000)
                        })
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef)
            }
            other => {
                return Err(Error::new(format!(
                    "unsupported column combination {other:?}"
                )));
            }
        }
    }

    let mut pk_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
    for row in 0..rows {
        let series = series_for_row(layout, sst_idx, base_row, row)?;
        let values = table
            .columns()
            .iter()
            .filter(|c| c.semantic() == ValidatedSemanticType::Tag)
            .map(|c| Ok((c.name().to_string(), tag_value(c, series)?)))
            .collect::<Result<HashMap<_, _>>>()?;
        pk_builder
            .append(encode_dense_primary_key(table, &values)?)
            .map_err(|err| Error::new(format!("failed to append dense primary key: {err}")))?;
    }
    columns.push(Arc::new(pk_builder.finish()));
    columns.push(Arc::new(UInt64Array::from_value(sequence, rows)));
    columns.push(Arc::new(UInt8Array::from_value(OpType::Put as u8, rows)));
    RecordBatch::try_new(flat_schema, columns)
        .map_err(|err| Error::new(format!("failed to build fixture record batch: {err}")))
}

fn file_meta_from_sst_info(
    info: &SstInfo,
    region_id: RegionId,
    file_id: FileId,
    sequence: u64,
) -> FileMeta {
    FileMeta {
        region_id,
        file_id,
        time_range: info.time_range,
        level: 0,
        file_size: info.file_size,
        max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
        available_indexes: Default::default(),
        indexes: Default::default(),
        index_file_size: 0,
        index_version: 0,
        num_rows: info.num_rows as u64,
        num_row_groups: info.num_row_groups,
        sequence: NonZeroU64::new(sequence),
        partition_expr: None,
        num_series: info.num_series,
        primary_key_min: None,
        primary_key_max: None,
    }
}

fn deterministic_file_id(seed: u64, index: usize) -> Result<FileId> {
    let seed_hi = ((seed >> 16) & 0xffff) as u16;
    let seed_lo = (seed & 0xffff) as u16;
    FileId::parse_str(&format!(
        "00000000-0000-{seed_hi:04x}-{seed_lo:04x}-{index:012x}"
    ))
    .map_err(|err| Error::new(format!("failed to construct deterministic file ID: {err}")))
}

fn remap_sst_file(
    object_store_dir: &Path,
    table_dir: &str,
    region_id: RegionId,
    old_file_id: FileId,
    new_file_id: FileId,
) -> Result<()> {
    if old_file_id == new_file_id {
        return Ok(());
    }
    let old_path = object_store_dir.join(mito2::sst::location::sst_file_path(
        table_dir,
        RegionFileId::new(region_id, old_file_id),
        PathType::Bare,
    ));
    let new_path = object_store_dir.join(mito2::sst::location::sst_file_path(
        table_dir,
        RegionFileId::new(region_id, new_file_id),
        PathType::Bare,
    ));
    if let Some(parent) = new_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            Error::new(format!(
                "failed to create remapped SST parent directory: {err}"
            ))
        })?;
    }
    fs::rename(&old_path, &new_path).map_err(|err| {
        Error::new(format!(
            "failed to remap SST file {} -> {}: {err}",
            old_path.display(),
            new_path.display()
        ))
    })?;
    Ok(())
}
pub async fn run_direct_sst(request: DirectSstRequest) -> Result<DirectSstSummary> {
    let DirectSstRequest {
        case,
        case_name,
        out_dir,
        region_id,
        table_dir,
        table,
        checkpoint_version,
        allow_large,
        dry_run,
    } = request;
    let scenario = case.direct_readable_sst()?;
    if scenario.layout().sst_count() > 1000 && !allow_large {
        return Err(Error::new("sst_count exceeds 1000; pass --allow-large"));
    }
    let seed = scenario.seed().unwrap_or(0);
    let table_index = match table.as_deref() {
        Some(name) => scenario
            .tables()
            .iter()
            .position(|table| table.name() == name)
            .ok_or_else(|| {
                Error::new(format!("--table {name} was not found in case {case_name}"))
            })?,
        None if scenario.tables().len() == 1 => 0,
        None => {
            return Err(Error::new(format!(
                "case {case_name} contains {} tables; pass --table <name> to choose one",
                scenario.tables().len()
            )));
        }
    };
    let table = &scenario.tables()[table_index];
    let region_id = RegionId::from(region_id);
    let table_dir =
        table_dir.unwrap_or_else(|| format!("data/{}/{}/", table.database(), table.name()));
    validate_relative_path("--table-dir", table_dir.trim_end_matches('/'))?;
    if table_dir.trim_end_matches('/').rsplit('/').next()
        == Some(region_name(region_id.table_id(), region_id.region_sequence()).as_str())
    {
        return Err(Error::new("--table-dir must be table-level directory"));
    }
    let region_dir =
        mito2::sst::location::region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let format = match table.sst_format() {
        ValidatedSstFormat::Flat => FormatType::Flat,
        ValidatedSstFormat::PrimaryKey => FormatType::PrimaryKey,
    };
    if dry_run {
        return Ok(DirectSstSummary {
            case: case_name,
            table: table.name().to_string(),
            database: table.database().to_string(),
            sst_count: scenario.layout().sst_count(),
            rows_per_sst: scenario.layout().rows_per_sst(),
            dry_run: true,
        });
    }

    let obj_store_dir = out_dir.join("object-store");
    let manifest_dir = out_dir.join("manifest");
    fs::create_dir_all(&obj_store_dir).map_err(|err| {
        Error::new(format!(
            "failed to create fixture object-store directory: {err}"
        ))
    })?;
    fs::create_dir_all(&manifest_dir).map_err(|err| {
        Error::new(format!(
            "failed to create fixture manifest directory: {err}"
        ))
    })?;
    let ostorage = ObjectStore::new(FsBuilder::default().root(&obj_store_dir.to_string_lossy()))
        .map_err(|err| {
            Error::new(format!(
                "failed to create filesystem object store for fixture output: {err}"
            ))
        })?
        .finish();
    let metadata: RegionMetadataRef = Arc::new(build_region_metadata(table, region_id)?);
    let mut files = HashMap::with_capacity(scenario.layout().sst_count());
    let mut next_file_index = 1;
    for i in 0..scenario.layout().sst_count() {
        let sequence = 1000_u64
            .checked_add(u64::try_from(i).map_err(|_| Error::new("SST index exceeds u64"))?)
            .ok_or_else(|| Error::new("fixture sequence overflows"))?;
        let batch = generate_record_batch(table, &metadata, scenario.layout(), i, sequence)?;
        let source =
            FlatSource::new_iter(batch.schema(), Box::new(vec![batch].into_iter().map(Ok)));
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            ostorage.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            FixedPathProvider {
                table_dir: table_dir.clone(),
            },
            &mut metrics,
        )
        .await;
        let opts = WriteOptions {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            row_group_size: scenario.layout().row_group_size(),
            max_file_size: None,
        };
        let infos = match format {
            FormatType::Flat => writer.write_all_flat(source, Some(sequence), &opts).await,
            FormatType::PrimaryKey => {
                writer
                    .write_all_flat_as_primary_key(source, Some(sequence), &opts)
                    .await
            }
        }
        .map_err(|err| {
            Error::new(format!(
                "failed to write fixture SST through Mito parquet writer: {err}"
            ))
        })?;
        for info in infos {
            let file_id = deterministic_file_id(
                seed.wrapping_add(
                    u64::try_from(table_index)
                        .map_err(|_| Error::new("table index exceeds u64"))?
                        << 16,
                ),
                next_file_index,
            )?;
            next_file_index += 1;
            remap_sst_file(&obj_store_dir, &table_dir, region_id, info.file_id, file_id)?;
            files.insert(
                file_id,
                file_meta_from_sst_info(&info, region_id, file_id, sequence),
            );
        }
    }
    let overall_seq = files
        .values()
        .filter_map(|m| m.sequence)
        .map(|s| s.get())
        .max()
        .unwrap_or(0);
    let manifest = RegionManifest {
        metadata,
        files,
        removed_files: RemovedFilesRecord::default(),
        flushed_entry_id: overall_seq,
        flushed_sequence: overall_seq,
        committed_sequence: Some(overall_seq),
        manifest_version: checkpoint_version,
        truncated_entry_id: None,
        compaction_time_window: None,
        sst_format: format,
        append_mode: table.append_mode(),
    };
    let checkpoint = RegionCheckpoint {
        last_version: checkpoint_version,
        compacted_actions: manifest.files.len(),
        checkpoint: Some(manifest.clone()),
    };
    let checkpoint_bytes = checkpoint
        .encode()
        .map_err(|err| Error::new(format!("failed to encode fixture region checkpoint: {err}")))?;
    let checkpoint_path = manifest_dir.join(format!("{checkpoint_version:020}.checkpoint"));
    fs::write(&checkpoint_path, &checkpoint_bytes)
        .map_err(|err| Error::new(format!("failed to write checkpoint file: {err}")))?;
    let checkpoint_summary = serde_json::to_vec_pretty(&serde_json::json!({ "size": checkpoint_bytes.len(), "version": checkpoint_version, "checksum": null, "extend_metadata": {} })).map_err(|err| Error::new(format!("failed to serialize checkpoint metadata: {err}")))?;
    fs::write(manifest_dir.join("_last_checkpoint"), checkpoint_summary)
        .map_err(|err| Error::new(format!("failed to write _last_checkpoint file: {err}")))?;
    let files_jsonl_path = out_dir.join("files.jsonl");
    let mut jsonl = BufWriter::new(
        fs::File::create(&files_jsonl_path)
            .map_err(|err| Error::new(format!("failed to create fixture files.jsonl: {err}")))?,
    );
    let mut entries: Vec<_> = manifest.files.iter().collect();
    entries.sort_by_key(|(id, _)| id.to_string());
    for (file_id, meta) in entries {
        let entry = serde_json::to_string(&serde_json::json!({ "file_id": file_id.to_string(), "region_id": meta.region_id.as_u64(), "object_path": mito2::sst::location::sst_file_path(&table_dir, RegionFileId::new(meta.region_id, *file_id), PathType::Bare), "time_range_start": meta.time_range.0.value(), "time_range_end": meta.time_range.1.value(), "num_rows": meta.num_rows, "num_row_groups": meta.num_row_groups, "file_size": meta.file_size, "num_series": meta.num_series, "sequence": meta.sequence.map(|s| s.get()) })).map_err(|err| Error::new(format!("failed to serialize fixture SST metadata JSONL entry: {err}")))?;
        writeln!(jsonl, "{entry}").map_err(|err| {
            Error::new(format!(
                "failed to write fixture SST metadata JSONL entry: {err}"
            ))
        })?;
    }
    jsonl
        .flush()
        .map_err(|err| Error::new(format!("failed to flush fixture files.jsonl: {err}")))?;
    let total_rows = scenario
        .layout()
        .sst_count()
        .checked_mul(scenario.layout().rows_per_sst())
        .ok_or_else(|| Error::new("fixture total rows overflows"))?;
    let summary = serde_json::to_vec_pretty(&serde_json::json!({ "case": case_name, "seed": seed, "table_index": table_index, "table": table.name(), "database": table.database(), "region_id": region_id.as_u64(), "table_dir": table_dir, "region_dir": region_dir, "sst_format": format!("{format:?}"), "sst_count": scenario.layout().sst_count(), "rows_per_sst": scenario.layout().rows_per_sst(), "row_group_size": scenario.layout().row_group_size(), "total_rows": total_rows, "checkpoint_path": checkpoint_path, "files_jsonl_path": files_jsonl_path, "readback_validated": false, "metadata_source": "synthetic" })).map_err(|err| Error::new(format!("failed to serialize fixture summary: {err}")))?;
    fs::write(out_dir.join("summary.json"), summary)
        .map_err(|err| Error::new(format!("failed to write fixture summary.json: {err}")))?;
    Ok(DirectSstSummary {
        case: case_name,
        table: table.name().to_string(),
        database: table.database().to_string(),
        sst_count: scenario.layout().sst_count(),
        rows_per_sst: scenario.layout().rows_per_sst(),
        dry_run: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_perf::case::load_case;

    const CASE: &str = r#"
[scenario]
kind = "direct_readable_sst"
[[scenario.tables]]
database = "public"
name = "unit"
engine = "mito"
primary_key = ["host"]
time_index = "ts"
[[scenario.tables.columns]]
name = "host"
type = "STRING"
semantic = "tag"
distribution = { kind = "cardinality", values = 1, prefix = "host" }
[[scenario.tables.columns]]
name = "value"
type = "DOUBLE"
semantic = "field"
[[scenario.tables.columns]]
name = "ts"
type = "TIMESTAMP(9)"
semantic = "timestamp"
[scenario.layout]
regions = 1
sst_count = 1
rows_per_sst = 1
row_group_size = 1
series_count = 1
start_unix_nanos = 0
step_nanos = 1
time_range_layout = "non_overlapping_per_sst"
series_layout = "round_robin"
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "select 1"
"#;

    #[tokio::test]
    async fn output_io_failure_returns_an_error_without_unwinding() {
        let temp = tempfile::tempdir().unwrap_or_else(|err| panic!("temporary directory: {err}"));
        let case_path = temp.path().join("case.toml");
        fs::write(&case_path, CASE).unwrap_or_else(|err| panic!("write case: {err}"));
        let out_file = temp.path().join("not-a-directory");
        fs::write(&out_file, "x").unwrap_or_else(|err| panic!("write output file: {err}"));

        let result = run_direct_sst(DirectSstRequest {
            case: load_case(&case_path).unwrap_or_else(|err| panic!("load case: {err}")),
            case_name: "unit".to_string(),
            out_dir: out_file,
            region_id: 1,
            table_dir: None,
            table: None,
            checkpoint_version: 1,
            allow_large: false,
            dry_run: false,
        })
        .await;

        assert!(result.is_err());
    }
}
