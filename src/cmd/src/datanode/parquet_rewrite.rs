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

use std::fs::File;
use std::path::{Path, PathBuf};

use clap::Parser;
use datatypes::arrow::record_batch::RecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error;

/// Read and rewrite a parquet file with different writer properties.
#[derive(Debug, Parser)]
pub struct ParquetRewriteCommand {
    /// Path to input parquet file.
    #[clap(long, value_name = "FILE")]
    input: PathBuf,

    /// Path to output parquet file in rewrite mode.
    #[clap(long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Path to writer properties TOML in rewrite mode.
    #[clap(long, value_name = "FILE")]
    properties: Option<PathBuf>,

    /// Dump writer properties TOML inferred from the input parquet file.
    #[clap(long, value_name = "FILE")]
    dump_properties: Option<PathBuf>,

    /// Number of rows per record batch.
    #[clap(long)]
    batch_size: Option<usize>,

    /// Overwrite output files.
    #[clap(long, default_value_t = false)]
    overwrite: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct RewriteProperties {
    writer: WriterConfig,
    columns: Vec<ColumnConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default, deny_unknown_fields)]
struct WriterConfig {
    dictionary_enabled: Option<bool>,
    compression: Option<CompressionConfig>,
    compression_level: Option<u32>,
    encoding: Option<EncodingConfig>,
    max_row_group_row_count: Option<usize>,
    data_page_size_limit: Option<usize>,
    data_page_row_count_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct ColumnConfig {
    path: Vec<String>,
    dictionary_enabled: Option<bool>,
    compression: Option<CompressionConfig>,
    compression_level: Option<u32>,
    encoding: Option<EncodingConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum EncodingConfig {
    Plain,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    ByteStreamSplit,
}

impl ParquetRewriteCommand {
    pub async fn run(&self) -> error::Result<()> {
        match (&self.dump_properties, &self.output, &self.properties) {
            (Some(path), None, None) => self.dump_properties(path),
            (None, Some(output), Some(properties)) => self.rewrite(output, properties),
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => illegal_config(
                "use either --dump-properties or rewrite mode, not both".to_string(),
            ),
            (None, _, _) => illegal_config(
                "rewrite mode requires --output and --properties; config dump mode requires --dump-properties".to_string(),
            ),
        }
    }

    fn dump_properties(&self, path: &Path) -> error::Result<()> {
        ensure_can_write(path, self.overwrite)?;
        let metadata = load_metadata_with_page_index(&self.input)?;
        let properties = infer_rewrite_properties(&metadata);
        let content = toml::to_string_pretty(&properties).map_err(|e| {
            error::IllegalConfigSnafu {
                msg: format!("failed to serialize writer properties: {e}"),
            }
            .build()
        })?;
        std::fs::write(path, content).context(error::FileIoSnafu)?;
        println!("Wrote writer properties to {}", path.display());
        Ok(())
    }

    fn rewrite(&self, output: &Path, properties: &Path) -> error::Result<()> {
        ensure_can_write(output, self.overwrite)?;

        let props = load_rewrite_properties(properties)?;
        let input_reader = SerializedFileReader::new(open_file(&self.input)?)
            .map_err(|e| parquet_error("read source parquet metadata", &self.input, e))?;
        let key_value_metadata = input_reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned();

        let mut reader_builder = ParquetRecordBatchReaderBuilder::try_new(open_file(&self.input)?)
            .map_err(|e| parquet_error("open source parquet", &self.input, e))?;
        if let Some(batch_size) = self.batch_size {
            if batch_size == 0 {
                return illegal_config("--batch-size must be greater than 0".to_string());
            }
            reader_builder = reader_builder.with_batch_size(batch_size);
        }
        let reader = reader_builder
            .build()
            .map_err(|e| parquet_error("build parquet reader", &self.input, e))?;
        let schema = reader.schema();

        let writer_props = build_writer_properties(props, key_value_metadata)?;
        let mut writer = ArrowWriter::try_new(create_file(output)?, schema, Some(writer_props))
            .map_err(|e| parquet_error("create parquet writer", output, e))?;

        for batch in reader {
            let batch =
                batch.map_err(|e| parquet_error("read parquet batch", &self.input, e.into()))?;
            writer
                .write(&batch)
                .map_err(|e| parquet_error("write parquet batch", output, e))?;
        }
        writer
            .close()
            .map_err(|e| parquet_error("close parquet writer", output, e))?;

        println!("Wrote parquet file to {}", output.display());
        Ok(())
    }
}

fn load_metadata_with_page_index(path: &Path) -> error::Result<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&open_file(path)?)
        .map_err(|e| parquet_error("read parquet metadata", path, e))
}

fn infer_rewrite_properties(metadata: &ParquetMetaData) -> RewriteProperties {
    let first_column = metadata
        .row_groups()
        .first()
        .and_then(|row_group| row_group.columns().first());
    let compression = first_column.map(|column| compression_to_config(column.compression()));
    let max_row_group_row_count = metadata
        .row_groups()
        .first()
        .and_then(|row_group| usize::try_from(row_group.num_rows()).ok());
    let dictionary_enabled = first_column
        .map(|column| column.dictionary_page_offset().is_some())
        .or(Some(true));

    let columns = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let first_chunk = metadata
                .row_groups()
                .first()
                .and_then(|row_group| row_group.columns().get(idx));
            ColumnConfig {
                path: column.path().parts().to_vec(),
                dictionary_enabled: first_chunk
                    .map(|chunk| chunk.dictionary_page_offset().is_some()),
                compression: first_chunk.map(|chunk| compression_to_config(chunk.compression())),
                compression_level: None,
                encoding: first_chunk.and_then(infer_data_encoding),
            }
        })
        .collect();

    RewriteProperties {
        writer: WriterConfig {
            dictionary_enabled,
            compression,
            compression_level: None,
            encoding: first_column.and_then(infer_data_encoding),
            max_row_group_row_count,
            data_page_size_limit: None,
            data_page_row_count_limit: None,
            dictionary_page_size_limit: None,
        },
        columns,
    }
}

fn load_rewrite_properties(path: &Path) -> error::Result<RewriteProperties> {
    let content = std::fs::read_to_string(path).context(error::FileIoSnafu)?;
    toml::from_str(&content).map_err(|e| {
        error::IllegalConfigSnafu {
            msg: format!("failed to parse writer properties {}: {e}", path.display()),
        }
        .build()
    })
}

fn build_writer_properties(
    config: RewriteProperties,
    key_value_metadata: Option<Vec<parquet::file::metadata::KeyValue>>,
) -> error::Result<WriterProperties> {
    let mut builder = WriterProperties::builder().set_key_value_metadata(key_value_metadata);
    if let Some(dictionary_enabled) = config.writer.dictionary_enabled {
        builder = builder.set_dictionary_enabled(dictionary_enabled);
    }
    if let Some(compression) = config.writer.compression {
        builder = builder.set_compression(to_parquet_compression(
            compression,
            config.writer.compression_level,
        )?);
    }
    if let Some(encoding) = config.writer.encoding {
        builder = builder.set_encoding(to_parquet_encoding(encoding));
    }
    if let Some(max_row_group_row_count) = config.writer.max_row_group_row_count {
        if max_row_group_row_count == 0 {
            return illegal_config("max_row_group_row_count must be greater than 0".to_string());
        }
        builder = builder.set_max_row_group_row_count(Some(max_row_group_row_count));
    }
    if let Some(data_page_size_limit) = config.writer.data_page_size_limit {
        builder = builder.set_data_page_size_limit(data_page_size_limit);
    }
    if let Some(data_page_row_count_limit) = config.writer.data_page_row_count_limit {
        builder = builder.set_data_page_row_count_limit(data_page_row_count_limit);
    }
    if let Some(dictionary_page_size_limit) = config.writer.dictionary_page_size_limit {
        builder = builder.set_dictionary_page_size_limit(dictionary_page_size_limit);
    }

    for column in config.columns {
        if column.path.is_empty() {
            return illegal_config("column path must not be empty".to_string());
        }
        let path = ColumnPath::new(column.path);
        if let Some(dictionary_enabled) = column.dictionary_enabled {
            builder = builder.set_column_dictionary_enabled(path.clone(), dictionary_enabled);
        }
        if let Some(compression) = column.compression {
            builder = builder.set_column_compression(
                path.clone(),
                to_parquet_compression(compression, column.compression_level)?,
            );
        }
        if let Some(encoding) = column.encoding {
            builder = builder.set_column_encoding(path, to_parquet_encoding(encoding));
        }
    }

    Ok(builder.build())
}

fn ensure_can_write(path: &Path, overwrite: bool) -> error::Result<()> {
    if !overwrite && path.exists() {
        return illegal_config(format!(
            "{} already exists; pass --overwrite to replace it",
            path.display()
        ));
    }
    Ok(())
}

fn open_file(path: &Path) -> error::Result<File> {
    File::open(path).context(error::FileIoSnafu)
}

fn create_file(path: &Path) -> error::Result<File> {
    File::create(path).context(error::FileIoSnafu)
}

fn illegal_config<T>(msg: String) -> error::Result<T> {
    error::IllegalConfigSnafu { msg }.fail()
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

fn to_parquet_compression(
    compression: CompressionConfig,
    level: Option<u32>,
) -> error::Result<Compression> {
    Ok(match compression {
        CompressionConfig::Uncompressed => Compression::UNCOMPRESSED,
        CompressionConfig::Snappy => Compression::SNAPPY,
        CompressionConfig::Gzip => Compression::GZIP(match level {
            Some(level) => GzipLevel::try_new(level).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid gzip compression level {level}: {e}"),
                }
                .build()
            })?,
            None => GzipLevel::default(),
        }),
        CompressionConfig::Lzo => Compression::LZO,
        CompressionConfig::Brotli => Compression::BROTLI(match level {
            Some(level) => BrotliLevel::try_new(level).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid brotli compression level {level}: {e}"),
                }
                .build()
            })?,
            None => BrotliLevel::default(),
        }),
        CompressionConfig::Lz4 => Compression::LZ4,
        CompressionConfig::Zstd => Compression::ZSTD(match level {
            Some(level) => ZstdLevel::try_new(level as i32).map_err(|e| {
                error::IllegalConfigSnafu {
                    msg: format!("invalid zstd compression level {level}: {e}"),
                }
                .build()
            })?,
            None => ZstdLevel::default(),
        }),
        CompressionConfig::Lz4Raw => Compression::LZ4_RAW,
    })
}

fn to_parquet_encoding(encoding: EncodingConfig) -> Encoding {
    match encoding {
        EncodingConfig::Plain => Encoding::PLAIN,
        EncodingConfig::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
        EncodingConfig::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        EncodingConfig::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
        EncodingConfig::ByteStreamSplit => Encoding::BYTE_STREAM_SPLIT,
    }
}

fn infer_data_encoding(
    column: &parquet::file::metadata::ColumnChunkMetaData,
) -> Option<EncodingConfig> {
    let encodings: Vec<_> = column.encodings().collect();
    if encodings.contains(&Encoding::DELTA_BINARY_PACKED) {
        Some(EncodingConfig::DeltaBinaryPacked)
    } else if encodings.contains(&Encoding::DELTA_LENGTH_BYTE_ARRAY) {
        Some(EncodingConfig::DeltaLengthByteArray)
    } else if encodings.contains(&Encoding::DELTA_BYTE_ARRAY) {
        Some(EncodingConfig::DeltaByteArray)
    } else if encodings.contains(&Encoding::BYTE_STREAM_SPLIT) {
        Some(EncodingConfig::ByteStreamSplit)
    } else if encodings.contains(&Encoding::PLAIN) {
        Some(EncodingConfig::Plain)
    } else {
        None
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::metadata::KeyValue;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_dump_and_rewrite_preserves_key_value_metadata_and_disables_dictionary() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        write_test_parquet(&input, true);

        let metadata = load_metadata_with_page_index(&input).unwrap();
        let mut properties = infer_rewrite_properties(&metadata);
        properties.columns[1].dictionary_enabled = Some(false);
        let props_path = dir.path().join("props.toml");
        std::fs::write(&props_path, toml::to_string(&properties).unwrap()).unwrap();

        let command = ParquetRewriteCommand {
            input: input.clone(),
            output: Some(output.clone()),
            properties: Some(props_path),
            dump_properties: None,
            batch_size: Some(2),
            overwrite: false,
        };
        command
            .rewrite(&output, command.properties.as_ref().unwrap())
            .unwrap();

        let rewritten = load_metadata_with_page_index(&output).unwrap();
        assert_eq!(rewritten.file_metadata().num_rows(), 4);
        let key_values = rewritten
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        assert!(key_values.iter().any(|kv| kv.key == "greptime:test"));
        assert!(
            rewritten
                .row_groups()
                .iter()
                .all(|row_group| row_group.column(1).dictionary_page_offset().is_none())
        );
    }

    fn write_test_parquet(path: &Path, dictionary_enabled: bool) {
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
            .set_dictionary_enabled(dictionary_enabled)
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
