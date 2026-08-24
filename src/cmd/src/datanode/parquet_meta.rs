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

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use parquet::basic::Compression;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use serde::Serialize;
use snafu::ResultExt;

use crate::error;

/// Display parquet file metadata.
#[derive(Debug, Parser)]
pub struct ParquetMetaCommand {
    /// Path to input parquet file.
    #[clap(long, value_name = "FILE")]
    input: PathBuf,

    /// Output format.
    #[clap(long, value_enum, default_value = "text")]
    format: MetaOutputFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetaOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Serialize)]
struct FileMetaView {
    input: String,
    num_rows: i64,
    num_row_groups: usize,
    num_columns: usize,
    key_value_metadata: BTreeMap<String, Option<String>>,
    row_groups: Vec<RowGroupMetaView>,
}

#[derive(Debug, Serialize)]
struct RowGroupMetaView {
    index: usize,
    num_rows: i64,
    uncompressed_size: i64,
    compressed_size: i64,
    compression_ratio: Option<f64>,
    data_pages: Option<usize>,
    dictionary_page_bytes: i64,
    columns: Vec<ColumnChunkMetaView>,
}

#[derive(Debug, Serialize)]
struct ColumnChunkMetaView {
    index: usize,
    path: String,
    physical_type: String,
    encodings: Vec<String>,
    compression: String,
    num_values: i64,
    uncompressed_size: i64,
    compressed_size: i64,
    compression_ratio: Option<f64>,
    data_page_offset: i64,
    dictionary_page_offset: Option<i64>,
    dictionary_page_bytes: Option<i64>,
    data_pages: Option<usize>,
    has_statistics: bool,
    column_index_offset: Option<i64>,
    column_index_length: Option<i32>,
    offset_index_offset: Option<i64>,
    offset_index_length: Option<i32>,
    bloom_filter_offset: Option<i64>,
    bloom_filter_length: Option<i32>,
}

#[derive(Debug, Clone, Copy)]
enum CompressionConfig {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

impl ParquetMetaCommand {
    pub async fn run(&self) -> error::Result<()> {
        let metadata = load_metadata_with_page_index(&self.input)?;
        let view = build_file_meta_view(&self.input, &metadata);

        match self.format {
            MetaOutputFormat::Text => print_meta_text(&view),
            MetaOutputFormat::Json => {
                let json = serde_json::to_string_pretty(&view).context(error::SerdeJsonSnafu)?;
                println!("{json}");
            }
        }

        Ok(())
    }
}

fn load_metadata_with_page_index(path: &Path) -> error::Result<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&open_file(path)?)
        .map_err(|e| parquet_error("read parquet metadata", path, e))
}

fn build_file_meta_view(path: &Path, metadata: &ParquetMetaData) -> FileMetaView {
    let key_value_metadata = metadata
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    let offset_index = metadata.offset_index();
    let row_groups = metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(row_group_idx, row_group)| {
            let columns: Vec<_> = row_group
                .columns()
                .iter()
                .enumerate()
                .map(|(column_idx, column)| {
                    let data_pages = offset_index
                        .and_then(|index| index.get(row_group_idx))
                        .and_then(|columns| columns.get(column_idx))
                        .map(|index| index.page_locations().len());
                    let dictionary_page_bytes = dictionary_page_bytes(
                        column.dictionary_page_offset(),
                        column.data_page_offset(),
                    );

                    ColumnChunkMetaView {
                        index: column_idx,
                        path: column.column_path().string(),
                        physical_type: format!("{:?}", column.column_type()),
                        encodings: column.encodings().map(|enc| format!("{enc:?}")).collect(),
                        compression: compression_to_string(column.compression()).to_string(),
                        num_values: column.num_values(),
                        uncompressed_size: column.uncompressed_size(),
                        compressed_size: column.compressed_size(),
                        compression_ratio: compression_ratio(
                            column.uncompressed_size(),
                            column.compressed_size(),
                        ),
                        data_page_offset: column.data_page_offset(),
                        dictionary_page_offset: column.dictionary_page_offset(),
                        dictionary_page_bytes,
                        data_pages,
                        has_statistics: column.statistics().is_some(),
                        column_index_offset: column.column_index_offset(),
                        column_index_length: column.column_index_length(),
                        offset_index_offset: column.offset_index_offset(),
                        offset_index_length: column.offset_index_length(),
                        bloom_filter_offset: column.bloom_filter_offset(),
                        bloom_filter_length: column.bloom_filter_length(),
                    }
                })
                .collect();
            let data_pages = if columns.iter().all(|column| column.data_pages.is_some()) {
                Some(
                    columns
                        .iter()
                        .map(|column| column.data_pages.unwrap_or_default())
                        .sum(),
                )
            } else {
                None
            };
            let dictionary_page_bytes = columns
                .iter()
                .map(|column| column.dictionary_page_bytes.unwrap_or_default())
                .sum();

            RowGroupMetaView {
                index: row_group_idx,
                num_rows: row_group.num_rows(),
                uncompressed_size: row_group.total_byte_size(),
                compressed_size: row_group.compressed_size(),
                compression_ratio: compression_ratio(
                    row_group.total_byte_size(),
                    row_group.compressed_size(),
                ),
                data_pages,
                dictionary_page_bytes,
                columns,
            }
        })
        .collect();

    FileMetaView {
        input: path.display().to_string(),
        num_rows: metadata.file_metadata().num_rows(),
        num_row_groups: metadata.num_row_groups(),
        num_columns: metadata.file_metadata().schema_descr().num_columns(),
        key_value_metadata,
        row_groups,
    }
}

fn print_meta_text(view: &FileMetaView) {
    println!("file: {}", view.input);
    println!("rows: {}", view.num_rows);
    println!("row_groups: {}", view.num_row_groups);
    println!("columns: {}", view.num_columns);
    println!("key_value_metadata: {}", view.key_value_metadata.len());
    for row_group in &view.row_groups {
        println!(
            "row_group[{}]: rows={}, uncompressed_size={}, compressed_size={}, compression_ratio={}, data_pages={}, dictionary_page_bytes={}",
            row_group.index,
            row_group.num_rows,
            row_group.uncompressed_size,
            row_group.compressed_size,
            format_ratio(row_group.compression_ratio),
            format_optional_usize(row_group.data_pages),
            row_group.dictionary_page_bytes,
        );
        for column in &row_group.columns {
            println!(
                "  column[{}] path={}: type={}, compression={}, encodings=[{}], values={}, uncompressed_size={}, compressed_size={}, compression_ratio={}, data_pages={}, dictionary_page_offset={}, dictionary_page_bytes={}, data_page_offset={}, statistics={}, column_index={}/{}, offset_index={}/{}, bloom_filter={}/{}",
                column.index,
                column.path,
                column.physical_type,
                column.compression,
                column.encodings.join(","),
                column.num_values,
                column.uncompressed_size,
                column.compressed_size,
                format_ratio(column.compression_ratio),
                format_optional_usize(column.data_pages),
                format_optional_i64(column.dictionary_page_offset),
                column
                    .dictionary_page_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                column.data_page_offset,
                column.has_statistics,
                format_optional_i64(column.column_index_offset),
                format_optional_i32(column.column_index_length),
                format_optional_i64(column.offset_index_offset),
                format_optional_i32(column.offset_index_length),
                format_optional_i64(column.bloom_filter_offset),
                format_optional_i32(column.bloom_filter_length),
            );
        }
    }
}

fn open_file(path: &Path) -> error::Result<File> {
    File::open(path).context(error::FileIoSnafu)
}

fn parquet_error(
    action: &'static str,
    path: &Path,
    error: parquet::errors::ParquetError,
) -> error::Error {
    error::IllegalConfigSnafu {
        msg: format!("{action} failed for {}: {error}", path.display()),
    }
    .build()
}

fn dictionary_page_bytes(
    dictionary_page_offset: Option<i64>,
    data_page_offset: i64,
) -> Option<i64> {
    dictionary_page_offset.map(|offset| data_page_offset.saturating_sub(offset))
}

fn compression_ratio(uncompressed: i64, compressed: i64) -> Option<f64> {
    (uncompressed > 0).then(|| compressed as f64 / uncompressed as f64)
}

fn format_ratio(ratio: Option<f64>) -> String {
    ratio
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn format_optional_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn compression_to_config(compression: Compression) -> CompressionConfig {
    match compression {
        Compression::UNCOMPRESSED => CompressionConfig::Uncompressed,
        Compression::SNAPPY => CompressionConfig::Snappy,
        Compression::GZIP(_) => CompressionConfig::Gzip,
        Compression::LZO => CompressionConfig::Lzo,
        Compression::BROTLI(_) => CompressionConfig::Brotli,
        Compression::LZ4 => CompressionConfig::Lz4,
        Compression::ZSTD(_) => CompressionConfig::Zstd,
        Compression::LZ4_RAW => CompressionConfig::Lz4Raw,
    }
}

fn compression_to_string(compression: Compression) -> &'static str {
    match compression_to_config(compression) {
        CompressionConfig::Uncompressed => "uncompressed",
        CompressionConfig::Snappy => "snappy",
        CompressionConfig::Gzip => "gzip",
        CompressionConfig::Lzo => "lzo",
        CompressionConfig::Brotli => "brotli",
        CompressionConfig::Lz4 => "lz4",
        CompressionConfig::Zstd => "zstd",
        CompressionConfig::Lz4Raw => "lz4-raw",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_meta_view_unknown_page_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("input.parquet");
        write_test_parquet(&path);

        let metadata = load_metadata_with_page_index(&path).unwrap();
        let view = build_file_meta_view(&path, &metadata);

        assert_eq!(view.num_row_groups, 1);
        assert!(view.row_groups[0].uncompressed_size > 0);
        assert!(view.row_groups[0].compressed_size > 0);
        assert!(view.row_groups[0].compression_ratio.is_some());
        assert_eq!(view.row_groups[0].columns.len(), 2);
    }

    fn write_test_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("host", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "greptime:test".to_string(),
                "value".to_string(),
            )]))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}
