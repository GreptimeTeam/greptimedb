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

#![allow(clippy::print_stderr, clippy::print_stdout)]

//! Generic direct-readable-SST fixture generator for query performance labs.
//!
//! This is a bounded MVP: it reads one table/one region case TOML, builds
//! synthetic region metadata, writes real Mito readable SST parquet files, and
//! emits a manifest checkpoint plus validation metadata.  The first case uses
//! `sst_format = "flat"`; primary-key output is also accepted for early lab use
//! because the writer can convert the same flat input batch to primary-key SSTs.
//!
//! TODO(query-perf): add multi-region generation and seed-manifest/catalog DB
//! integration so the generated fixture can be mounted directly by a full
//! GreptimeDB open/query path without manual metadata wiring.

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use async_trait::async_trait;
use clap::Parser;
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
use serde::Deserialize;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::path_utils::region_name;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

#[derive(Parser, Debug)]
#[command(name = "query_perf_fixture")]
#[command(about = "Generate direct-readable SST fixtures for query performance cases")]
struct Args {
    /// TOML case file, for example tests/perf/query_cases/.../case.toml.
    #[arg(long, value_name = "PATH")]
    case: PathBuf,

    /// Output directory (creates object-store/, manifest/, files.jsonl, summary.json).
    #[arg(long, value_name = "DIR")]
    out_dir: PathBuf,

    /// Region ID to synthesize. Defaults to table id 1024, region 0.
    #[arg(long, default_value = "4398046511104")]
    region_id: u64,

    /// Table directory relative to object-store root. Defaults to data/{database}/{table}/.
    #[arg(long, value_name = "DIR")]
    table_dir: Option<String>,

    /// Table name to generate when the case contains multiple [[tables]].
    #[arg(long, value_name = "NAME")]
    table: Option<String>,

    /// Manifest/checkpoint version.
    #[arg(long, default_value = "1000000")]
    checkpoint_version: u64,

    /// Safety flag when sst_count exceeds 1000.
    #[arg(long)]
    allow_large: bool,

    /// Print plan only.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
struct CaseFile {
    scenario: Scenario,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum Scenario {
    #[serde(rename = "direct_readable_sst")]
    DirectReadableSst(DirectReadableSstScenario),
}

#[derive(Debug, Deserialize)]
struct DirectReadableSstScenario {
    #[serde(default)]
    seed: Option<u64>,
    tables: Vec<TableConfig>,
    layout: LayoutConfig,
}

impl Scenario {
    fn kind(&self) -> &'static str {
        match self {
            Scenario::DirectReadableSst(_) => "direct_readable_sst",
        }
    }

    fn direct_readable_sst(&self) -> &DirectReadableSstScenario {
        match self {
            Scenario::DirectReadableSst(scenario) => scenario,
        }
    }
}

#[derive(Debug, Deserialize)]
struct TableConfig {
    database: String,
    name: String,
    engine: String,
    #[serde(default)]
    append_mode: Option<bool>,
    #[serde(default)]
    sst_format: Option<String>,
    primary_key: Vec<String>,
    time_index: String,
    columns: Vec<ColumnConfig>,
}

#[derive(Debug, Deserialize)]
struct ColumnConfig {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    semantic: String,
    distribution: Option<Distribution>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum Distribution {
    #[serde(rename = "cardinality")]
    Cardinality {
        values: NonZeroUsize,
        prefix: String,
    },
    #[serde(rename = "deterministic_wave")]
    DeterministicWave { min: f64, max: f64 },
}

#[derive(Debug, Deserialize)]
struct LayoutConfig {
    regions: usize,
    sst_count: usize,
    rows_per_sst: usize,
    row_group_size: usize,
    series_count: NonZeroUsize,
    start_unix_nanos: i64,
    step_nanos: i64,
    time_range_layout: String,
    series_layout: String,
}

struct NoopIndexBuilder;

#[async_trait]
impl IndexerBuilder for NoopIndexBuilder {
    async fn build(&self, _file_id: FileId, _index_version: u64) -> Indexer {
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

fn semantic_type(s: &str) -> SemanticType {
    match s.to_ascii_lowercase().as_str() {
        "tag" => SemanticType::Tag,
        "field" => SemanticType::Field,
        "timestamp" => SemanticType::Timestamp,
        other => panic!("unsupported semantic {other}"),
    }
}

fn concrete_type(ty: &str) -> ConcreteDataType {
    match ty.to_ascii_uppercase().as_str() {
        "STRING" => ConcreteDataType::string_datatype(),
        "DOUBLE" => ConcreteDataType::float64_datatype(),
        "UINT64" => ConcreteDataType::uint64_datatype(),
        "TIMESTAMP(9)" => ConcreteDataType::timestamp_nanosecond_datatype(),
        "TIMESTAMP(3)" => ConcreteDataType::timestamp_millisecond_datatype(),
        other => panic!("unsupported column type {other}"),
    }
}

fn build_region_metadata(table: &TableConfig, region_id: RegionId) -> RegionMetadata {
    let mut builder = store_api::metadata::RegionMetadataBuilder::new(region_id);
    for (idx, col) in table.columns.iter().enumerate() {
        let mut schema = ColumnSchema::new(
            &col.name,
            concrete_type(&col.ty),
            col.semantic != "timestamp",
        );
        if col.semantic == "tag" {
            schema = schema.with_inverted_index(true);
        }
        if col.semantic == "tag" || col.semantic == "field" {
            schema = schema
                .with_skipping_options(SkippingIndexOptions {
                    granularity: 1,
                    ..Default::default()
                })
                .expect("valid skipping index options for query perf fixture");
        }
        if col.name == table.time_index {
            schema = schema.with_time_index(true);
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: schema,
            semantic_type: semantic_type(&col.semantic),
            column_id: idx as u32,
        });
    }
    let pk_ids = table
        .primary_key
        .iter()
        .map(|name| {
            table
                .columns
                .iter()
                .position(|c| c.name == *name)
                .unwrap_or_else(|| panic!("primary key column {name} not found")) as u32
        })
        .collect();
    builder.primary_key(pk_ids);
    builder.primary_key_encoding(PrimaryKeyEncoding::Dense);
    builder
        .build()
        .expect("region metadata should be valid for fixture table config")
}

fn encode_dense_primary_key(table: &TableConfig, values: &HashMap<String, String>) -> Vec<u8> {
    let fields = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            (
                idx as u32,
                SortField::new(ConcreteDataType::string_datatype()),
            )
        })
        .collect();
    let converter = DensePrimaryKeyCodec::with_fields(fields);
    converter
        .encode(
            table
                .primary_key
                .iter()
                .map(|name| datatypes::value::ValueRef::String(values[name].as_str())),
        )
        .expect("dense primary key encoding should match fixture primary key fields")
}

fn tag_value(col: &ColumnConfig, series: usize) -> String {
    match col
        .distribution
        .as_ref()
        .expect("tag distribution is required")
    {
        Distribution::Cardinality { values, prefix } => {
            format!("{}{}", prefix, series % values.get())
        }
        _ => panic!("tag column {} requires cardinality distribution", col.name),
    }
}

fn wave_value(min: f64, max: f64, row: usize) -> f64 {
    let span = max - min;
    let phase = (row % 1024) as f64 / 1023.0;
    min + span * (0.5 - 0.5 * (std::f64::consts::TAU * phase).cos())
}

fn generate_record_batch(
    table: &TableConfig,
    metadata: &RegionMetadataRef,
    layout: &LayoutConfig,
    sst_idx: usize,
    sequence: u64,
) -> RecordBatch {
    let flat_schema = to_flat_sst_arrow_schema(metadata, &FlatSchemaOptions::default());
    let rows = layout.rows_per_sst;
    let mut columns = Vec::new();
    let base_row = sst_idx * rows;

    for col in &table.columns {
        match (col.semantic.as_str(), col.ty.to_ascii_uppercase().as_str()) {
            ("tag", "STRING") => {
                let mut b = StringDictionaryBuilder::<UInt32Type>::new();
                for row in 0..rows {
                    let series = if layout.series_layout == "round_robin" {
                        (base_row + row) % layout.series_count.get()
                    } else {
                        sst_idx % layout.series_count.get()
                    };
                    let value = tag_value(col, series);
                    b.append_value(value);
                }
                columns.push(Arc::new(b.finish()) as ArrayRef);
            }
            ("field", "DOUBLE") => {
                let (min, max) = match col.distribution.as_ref() {
                    Some(Distribution::DeterministicWave { min, max }) => (*min, *max),
                    _ => (0.0, 1.0),
                };
                columns.push(Arc::new(Float64Array::from(
                    (0..rows)
                        .map(|r| wave_value(min, max, base_row + r))
                        .collect::<Vec<_>>(),
                )) as ArrayRef);
            }
            ("field", "UINT64") => columns.push(Arc::new(UInt64Array::from(
                (0..rows).map(|r| (base_row + r) as u64).collect::<Vec<_>>(),
            )) as ArrayRef),
            ("timestamp", "TIMESTAMP(9)") => columns.push(Arc::new(TimestampNanosecondArray::from(
                (0..rows)
                    .map(|r| layout.start_unix_nanos + ((base_row + r) as i64 * layout.step_nanos))
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            ("timestamp", "TIMESTAMP(3)") => {
                columns.push(Arc::new(TimestampMillisecondArray::from(
                    (0..rows)
                        .map(|r| {
                            (layout.start_unix_nanos + ((base_row + r) as i64 * layout.step_nanos))
                                / 1_000_000
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef)
            }
            other => panic!("unsupported column combination {other:?}"),
        }
    }

    let mut pk_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
    for row in 0..rows {
        let series = if layout.series_layout == "round_robin" {
            (base_row + row) % layout.series_count.get()
        } else {
            sst_idx % layout.series_count.get()
        };
        let values = table
            .columns
            .iter()
            .filter(|c| c.semantic == "tag")
            .map(|c| (c.name.clone(), tag_value(c, series)))
            .collect();
        pk_builder
            .append(encode_dense_primary_key(table, &values))
            .expect("append generated dense primary key to Arrow dictionary builder");
    }
    columns.push(Arc::new(pk_builder.finish()));
    columns.push(Arc::new(UInt64Array::from_value(sequence, rows)));
    columns.push(Arc::new(UInt8Array::from_value(OpType::Put as u8, rows)));
    RecordBatch::try_new(flat_schema, columns)
        .expect("generated fixture columns should match flat SST Arrow schema")
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

fn deterministic_file_id(seed: u64, index: usize) -> FileId {
    let seed_hi = ((seed >> 16) & 0xffff) as u16;
    let seed_lo = (seed & 0xffff) as u16;
    FileId::parse_str(&format!(
        "00000000-0000-{seed_hi:04x}-{seed_lo:04x}-{index:012x}"
    ))
    .expect("deterministic file id must be a valid UUID")
}

fn remap_sst_file(
    object_store_dir: &Path,
    table_dir: &str,
    region_id: RegionId,
    old_file_id: FileId,
    new_file_id: FileId,
) {
    if old_file_id == new_file_id {
        return;
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
        fs::create_dir_all(parent).expect("failed to create remapped SST parent directory");
    }
    fs::rename(&old_path, &new_path).unwrap_or_else(|err| {
        panic!(
            "failed to remap SST file {} -> {}: {err}",
            old_path.display(),
            new_path.display()
        )
    });
}

fn case_name_from_path(path: &Path) -> String {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .unwrap_or("query_perf_case")
        .to_string()
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let case_text = fs::read_to_string(&args.case).expect("failed to read query perf case TOML");
    let case: CaseFile = toml::from_str(&case_text).expect("failed to parse query perf case TOML");
    let case_name = case_name_from_path(&args.case);
    let scenario = case.scenario.direct_readable_sst();
    if scenario.tables.is_empty() || scenario.layout.regions != 1 {
        panic!("fixture generator supports one or more tables and exactly one region per table");
    }
    if scenario.layout.time_range_layout != "non_overlapping_per_sst" {
        panic!("MVP supports time_range_layout=non_overlapping_per_sst");
    }
    if scenario.layout.sst_count > 1000 && !args.allow_large {
        panic!("sst_count exceeds 1000; pass --allow-large");
    }
    let seed = scenario.seed.unwrap_or(0);
    let table_index = match args.table.as_deref() {
        Some(name) => scenario
            .tables
            .iter()
            .position(|table| table.name == name)
            .unwrap_or_else(|| panic!("--table {name} was not found in case {case_name}")),
        None if scenario.tables.len() == 1 => 0,
        None => panic!(
            "case {} contains {} tables; pass --table <name> to choose one",
            case_name,
            scenario.tables.len()
        ),
    };
    let table = &scenario.tables[table_index];
    if table.engine != "mito" {
        panic!("MVP supports engine=mito");
    }
    let region_id = RegionId::from(args.region_id);
    let table_dir = args
        .table_dir
        .unwrap_or_else(|| format!("data/{}/{}/", table.database, table.name));
    if Path::new(&table_dir).is_absolute() {
        panic!("--table-dir must be relative");
    }
    if table_dir.trim_end_matches('/').rsplit('/').next()
        == Some(region_name(region_id.table_id(), region_id.region_sequence()).as_str())
    {
        panic!("--table-dir must be table-level directory");
    }
    let region_dir =
        mito2::sst::location::region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let format = match table.sst_format.as_deref().unwrap_or("flat") {
        "flat" => FormatType::Flat,
        "primary_key" | "primary-key" => FormatType::PrimaryKey,
        other => panic!("unsupported sst_format {other}"),
    };
    println!(
        "query_perf_fixture case={} scenario={} table={}.{} ssts={} rows_per_sst={} format={format:?}",
        case_name,
        case.scenario.kind(),
        table.database,
        table.name,
        scenario.layout.sst_count,
        scenario.layout.rows_per_sst
    );
    println!(
        "out_dir={} table_dir={} region_dir={}",
        args.out_dir.display(),
        table_dir,
        region_dir
    );
    if args.dry_run {
        return;
    }

    let obj_store_dir = args.out_dir.join("object-store");
    let manifest_dir = args.out_dir.join("manifest");
    fs::create_dir_all(&obj_store_dir).expect("failed to create fixture object-store directory");
    fs::create_dir_all(&manifest_dir).expect("failed to create fixture manifest directory");
    let ostorage = ObjectStore::new(FsBuilder::default().root(&obj_store_dir.to_string_lossy()))
        .expect("failed to create filesystem object store for fixture output")
        .finish();
    let metadata: RegionMetadataRef = Arc::new(build_region_metadata(table, region_id));
    let mut files = HashMap::with_capacity(scenario.layout.sst_count);
    let mut next_file_index = 1;
    for i in 0..scenario.layout.sst_count {
        let sequence = 1000 + i as u64;
        let batch = generate_record_batch(table, &metadata, &scenario.layout, i, sequence);
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
            row_group_size: scenario.layout.row_group_size,
            max_file_size: None,
            parquet: Default::default(),
            parquet_policy_provider: None,
            parquet_write_operation: Default::default(),
        };
        let infos = match format {
            FormatType::Flat => writer.write_all_flat(source, Some(sequence), &opts).await,
            FormatType::PrimaryKey => {
                writer
                    .write_all_flat_as_primary_key(source, Some(sequence), &opts)
                    .await
            }
        }
        .expect("failed to write fixture SST through Mito parquet writer");
        for info in infos {
            let file_id = deterministic_file_id(
                seed.wrapping_add((table_index as u64) << 16),
                next_file_index,
            );
            next_file_index += 1;
            remap_sst_file(&obj_store_dir, &table_dir, region_id, info.file_id, file_id);
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
        manifest_version: args.checkpoint_version,
        truncated_entry_id: None,
        compaction_time_window: None,
        sst_format: format,
        append_mode: table.append_mode,
    };
    let checkpoint = RegionCheckpoint {
        last_version: args.checkpoint_version,
        compacted_actions: manifest.files.len(),
        checkpoint: Some(manifest.clone()),
    };
    let checkpoint_bytes = checkpoint
        .encode()
        .expect("failed to encode fixture region checkpoint");
    let checkpoint_path = manifest_dir.join(format!("{:020}.checkpoint", args.checkpoint_version));
    fs::write(&checkpoint_path, &checkpoint_bytes).expect("failed to write checkpoint file");
    fs::write(manifest_dir.join("_last_checkpoint"), serde_json::to_vec_pretty(&serde_json::json!({ "size": checkpoint_bytes.len(), "version": args.checkpoint_version, "checksum": null, "extend_metadata": {} })).expect("failed to serialize _last_checkpoint metadata")).expect("failed to write _last_checkpoint file");
    let files_jsonl_path = args.out_dir.join("files.jsonl");
    let mut jsonl = BufWriter::new(
        fs::File::create(&files_jsonl_path).expect("failed to create fixture files.jsonl"),
    );
    let mut entries: Vec<_> = manifest.files.iter().collect();
    entries.sort_by_key(|(id, _)| id.to_string());
    for (file_id, meta) in entries {
        writeln!(jsonl, "{}", serde_json::to_string(&serde_json::json!({ "file_id": file_id.to_string(), "region_id": meta.region_id.as_u64(), "object_path": mito2::sst::location::sst_file_path(&table_dir, RegionFileId::new(meta.region_id, *file_id), PathType::Bare), "time_range_start": meta.time_range.0.value(), "time_range_end": meta.time_range.1.value(), "num_rows": meta.num_rows, "num_row_groups": meta.num_row_groups, "file_size": meta.file_size, "num_series": meta.num_series, "sequence": meta.sequence.map(|s| s.get()) })).expect("failed to serialize fixture SST metadata JSONL entry")).expect("failed to write fixture SST metadata JSONL entry");
    }
    jsonl.flush().expect("failed to flush fixture files.jsonl");
    fs::write(args.out_dir.join("summary.json"), serde_json::to_vec_pretty(&serde_json::json!({ "case": case_name, "seed": seed, "table_index": table_index, "table": table.name, "database": table.database, "region_id": region_id.as_u64(), "table_dir": table_dir, "region_dir": region_dir, "sst_format": format!("{format:?}"), "sst_count": scenario.layout.sst_count, "rows_per_sst": scenario.layout.rows_per_sst, "row_group_size": scenario.layout.row_group_size, "total_rows": scenario.layout.sst_count * scenario.layout.rows_per_sst, "checkpoint_path": checkpoint_path, "files_jsonl_path": files_jsonl_path, "readback_validated": false, "metadata_source": "synthetic" })).expect("failed to serialize fixture summary")).expect("failed to write fixture summary.json");
    println!("Done. wrote {} SST file entries", manifest.files.len());
}
