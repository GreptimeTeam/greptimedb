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
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;

use arrow::csv::reader::Format;
use arrow::csv::{self, WriterBuilder};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use common_runtime;
use common_telemetry::warn;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use snafu::ResultExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;

use crate::buffered_writer::DfRecordBatchEncoder;
use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::file_format::{self, FileFormat, stream_to_file};
use crate::share_buffer::SharedBuffer;
use crate::util::normalize_infer_schema;

const SKIP_BAD_RECORDS_BATCH_SIZE: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvFormat {
    pub has_header: bool,
    pub skip_bad_records: bool,
    pub delimiter: u8,
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
    pub timestamp_format: Option<String>,
    pub time_format: Option<String>,
    pub date_format: Option<String>,
}

impl TryFrom<&HashMap<String, String>> for CsvFormat {
    type Error = error::Error;

    fn try_from(value: &HashMap<String, String>) -> Result<Self> {
        let mut format = CsvFormat::default();
        if let Some(delimiter) = value.get(file_format::FORMAT_DELIMITER) {
            // TODO(weny): considers to support parse like "\t" (not only b'\t')
            format.delimiter = u8::from_str(delimiter).map_err(|_| {
                error::ParseFormatSnafu {
                    key: file_format::FORMAT_DELIMITER,
                    value: delimiter,
                }
                .build()
            })?;
        };
        if let Some(compression_type) = value.get(file_format::FORMAT_COMPRESSION_TYPE) {
            format.compression_type = CompressionType::from_str(compression_type)?;
        };
        if let Some(schema_infer_max_record) =
            value.get(file_format::FORMAT_SCHEMA_INFER_MAX_RECORD)
        {
            format.schema_infer_max_record =
                Some(schema_infer_max_record.parse::<usize>().map_err(|_| {
                    error::ParseFormatSnafu {
                        key: file_format::FORMAT_SCHEMA_INFER_MAX_RECORD,
                        value: schema_infer_max_record,
                    }
                    .build()
                })?);
        };
        if let Some(has_header) = value.get(file_format::FORMAT_HAS_HEADER) {
            format.has_header = parse_bool(file_format::FORMAT_HAS_HEADER, has_header)?;
        };
        if let Some(skip_bad_records) = value.get(file_format::FORMAT_SKIP_BAD_RECORDS) {
            format.skip_bad_records =
                parse_bool(file_format::FORMAT_SKIP_BAD_RECORDS, skip_bad_records)?;
        };
        if let Some(timestamp_format) = value.get(file_format::TIMESTAMP_FORMAT) {
            format.timestamp_format = Some(timestamp_format.clone());
        }
        if let Some(time_format) = value.get(file_format::TIME_FORMAT) {
            format.time_format = Some(time_format.clone());
        }
        if let Some(date_format) = value.get(file_format::DATE_FORMAT) {
            format.date_format = Some(date_format.clone());
        }
        Ok(format)
    }
}

fn parse_bool(key: &'static str, value: &str) -> Result<bool> {
    value
        .parse()
        .map_err(|_| error::ParseFormatSnafu { key, value }.build())
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            has_header: true,
            skip_bad_records: false,
            delimiter: b',',
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::Uncompressed,
            timestamp_format: None,
            time_format: None,
            date_format: None,
        }
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let meta = store
            .stat(path)
            .await
            .context(error::ReadObjectSnafu { path })?;

        let reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?
            .into_futures_async_read(0..meta.content_length())
            .await
            .context(error::ReadObjectSnafu { path })?
            .compat();

        let decoded = self.compression_type.convert_async_read(reader);

        let delimiter = self.delimiter;
        let schema_infer_max_record = self.schema_infer_max_record;
        let has_header = self.has_header;

        common_runtime::spawn_blocking_global(move || {
            let reader = SyncIoBridge::new(decoded);

            let format = Format::default()
                .with_delimiter(delimiter)
                .with_header(has_header);
            let (schema, _records_read) = format
                .infer_schema(reader, schema_infer_max_record)
                .context(error::InferSchemaSnafu)?;

            Ok(normalize_infer_schema(schema))
        })
        .await
        .context(error::JoinHandleSnafu)?
    }
}

pub async fn stream_to_csv(
    stream: SendableRecordBatchStream,
    store: ObjectStore,
    path: &str,
    threshold: usize,
    concurrency: usize,
    format: &CsvFormat,
) -> Result<usize> {
    stream_to_file(
        stream,
        store,
        path,
        threshold,
        concurrency,
        format.compression_type,
        |buffer| {
            let mut builder = WriterBuilder::new();
            if let Some(timestamp_format) = &format.timestamp_format {
                builder = builder.with_timestamp_format(timestamp_format.to_owned())
            }
            if let Some(date_format) = &format.date_format {
                builder = builder.with_date_format(date_format.to_owned())
            }
            if let Some(time_format) = &format.time_format {
                builder = builder.with_time_format(time_format.to_owned())
            }
            builder.build(buffer)
        },
    )
    .await
}

impl DfRecordBatchEncoder for csv::Writer<SharedBuffer> {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write(batch).context(error::WriteRecordBatchSnafu)
    }
}

/// Builds a CSV stream that can skip selected record-level parse/cast errors.
///
/// The decoder intentionally uses one-record batches. Arrow's CSV decoder clears
/// buffered rows before type parsing, so a failed multi-row flush cannot be
/// safely retried row by row without replaying input bytes.
pub async fn tolerant_csv_stream(
    store: &ObjectStore,
    path: &str,
    schema: SchemaRef,
    projection: Vec<usize>,
    format: &CsvFormat,
) -> Result<SendableRecordBatchStream> {
    let meta = store
        .stat(path)
        .await
        .context(error::ReadObjectSnafu { path })?;

    let reader = store
        .reader(path)
        .await
        .context(error::ReadObjectSnafu { path })?
        .into_bytes_stream(0..meta.content_length())
        .await
        .context(error::ReadObjectSnafu { path })?;

    let reader = format.compression_type.convert_stream(reader).boxed();
    tolerant_csv_stream_from_reader(
        reader,
        path,
        schema,
        projection,
        format.has_header,
        format.delimiter,
    )
}

fn tolerant_csv_stream_from_reader(
    reader: BoxStream<'static, io::Result<Bytes>>,
    path: &str,
    schema: SchemaRef,
    projection: Vec<usize>,
    has_header: bool,
    delimiter: u8,
) -> Result<SendableRecordBatchStream> {
    let projected_schema = Arc::new(
        schema
            .project(&projection)
            .context(error::InferSchemaSnafu)?,
    );
    let mut decoder = csv::ReaderBuilder::new(schema)
        .with_header(has_header)
        .with_delimiter(delimiter)
        .with_batch_size(SKIP_BAD_RECORDS_BATCH_SIZE)
        .with_projection(projection)
        .build_decoder();

    let path = path.to_string();
    let mut upstream = reader.fuse();
    let mut buffered = Bytes::new();
    let mut input_finished = false;
    let stream = futures::stream::poll_fn(move |cx| {
        loop {
            while !input_finished {
                if buffered.is_empty() {
                    match futures::ready!(upstream.poll_next_unpin(cx)) {
                        Some(Ok(bytes)) if bytes.is_empty() => continue,
                        Some(Ok(bytes)) => buffered = bytes,
                        Some(Err(error)) => return Poll::Ready(Some(Err(error.into()))),
                        None => input_finished = true,
                    }
                }

                let decoded = decoder.decode(buffered.as_ref())?;
                if decoded > 0 {
                    buffered.advance(decoded);
                    continue;
                }

                if decoder.capacity() == 0 || input_finished {
                    break;
                }

                if buffered.is_empty() {
                    continue;
                }

                return Poll::Ready(Some(Err(ArrowError::ParseError(
                    "CSV decoder made no progress while input bytes remain".to_string(),
                ))));
            }

            match decoder.flush() {
                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                Ok(None) if input_finished => return Poll::Ready(None),
                Ok(None) => continue,
                Err(error) if is_skippable_arrow_error(&error) => {
                    warn!(
                        "Skipping bad CSV record while copying from {}: {}",
                        path, error
                    );
                }
                Err(error) => return Poll::Ready(Some(Err(error))),
            }
        }
    })
    .map(|result: std::result::Result<RecordBatch, ArrowError>| result.map_err(Into::into));

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        projected_schema,
        stream,
    )))
}

pub fn is_skippable_arrow_error(error: &ArrowError) -> bool {
    matches!(
        error,
        ArrowError::ParseError(_)
            | ArrowError::CastError(_)
            | ArrowError::ComputeError(_)
            | ArrowError::InvalidArgumentError(_)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field};
    use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use common_test_util::find_workspace_path;
    use datafusion::datasource::physical_plan::{CsvSource, FileSource};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float64Vector, StringVector, UInt32Vector, VectorRef};
    use futures::TryStreamExt;

    use super::*;
    use crate::file_format::{
        FORMAT_COMPRESSION_TYPE, FORMAT_DELIMITER, FORMAT_HAS_HEADER,
        FORMAT_SCHEMA_INFER_MAX_RECORD, FORMAT_SKIP_BAD_RECORDS, FileFormat, file_to_stream,
    };
    use crate::test_util::{format_schema, test_store};

    fn test_data_root() -> String {
        find_workspace_path("/src/common/datasource/tests/csv")
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let csv = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = csv.infer_schema(&store, "simple.csv").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "c1: Utf8: NULL",
                "c2: Int64: NULL",
                "c3: Int64: NULL",
                "c4: Int64: NULL",
                "c5: Int64: NULL",
                "c6: Int64: NULL",
                "c7: Int64: NULL",
                "c8: Int64: NULL",
                "c9: Int64: NULL",
                "c10: Utf8: NULL",
                "c11: Float64: NULL",
                "c12: Float64: NULL",
                "c13: Utf8: NULL"
            ],
            formatted,
        );
    }

    #[tokio::test]
    async fn normalize_infer_schema() {
        let csv = CsvFormat {
            schema_infer_max_record: Some(3),
            ..CsvFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = csv.infer_schema(&store, "max_infer.csv").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "num: Int64: NULL",
                "str: Utf8: NULL",
                "ts: Utf8: NULL",
                "t: Utf8: NULL",
                "date: Date32: NULL"
            ],
            formatted,
        );
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let csv = CsvFormat {
            schema_infer_max_record: Some(3),
            ..CsvFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = csv
            .infer_schema(&store, "schema_infer_limit.csv")
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "a: Int64: NULL",
                "b: Float64: NULL",
                "c: Int64: NULL",
                "d: Int64: NULL"
            ],
            formatted
        );

        let csv = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = csv
            .infer_schema(&store, "schema_infer_limit.csv")
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "a: Int64: NULL",
                "b: Float64: NULL",
                "c: Int64: NULL",
                "d: Utf8: NULL"
            ],
            formatted
        );
    }

    #[test]
    fn test_try_from() {
        let map = HashMap::new();
        let format: CsvFormat = CsvFormat::try_from(&map).unwrap();

        assert_eq!(format, CsvFormat::default());

        let map = HashMap::from([
            (
                FORMAT_SCHEMA_INFER_MAX_RECORD.to_string(),
                "2000".to_string(),
            ),
            (FORMAT_COMPRESSION_TYPE.to_string(), "zstd".to_string()),
            (FORMAT_DELIMITER.to_string(), b'\t'.to_string()),
            (FORMAT_HAS_HEADER.to_string(), "false".to_string()),
        ]);
        let format = CsvFormat::try_from(&map).unwrap();

        assert_eq!(
            format,
            CsvFormat {
                compression_type: CompressionType::Zstd,
                schema_infer_max_record: Some(2000),
                delimiter: b'\t',
                has_header: false,
                skip_bad_records: false,
                timestamp_format: None,
                time_format: None,
                date_format: None
            }
        );

        let map = HashMap::from([(FORMAT_SKIP_BAD_RECORDS.to_string(), "true".to_string())]);
        let format = CsvFormat::try_from(&map).unwrap();

        assert_eq!(
            format,
            CsvFormat {
                skip_bad_records: true,
                ..CsvFormat::default()
            }
        );
    }

    #[test]
    fn test_try_from_rejects_invalid_bool_options() {
        let map = HashMap::from([(FORMAT_SKIP_BAD_RECORDS.to_string(), "yes".to_string())]);
        assert!(CsvFormat::try_from(&map).is_err());
    }

    #[tokio::test]
    async fn test_compressed_csv() {
        // Create test data
        let column_schemas = vec![
            ColumnSchema::new("id", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        ];
        let schema = Arc::new(Schema::new(column_schemas));

        // Create multiple record batches with different data
        let batch1_columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3])),
            Arc::new(StringVector::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Float64Vector::from_slice(vec![10.5, 20.3, 30.7])),
        ];
        let batch1 = RecordBatch::new(schema.clone(), batch1_columns).unwrap();

        let batch2_columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![4, 5, 6])),
            Arc::new(StringVector::from(vec!["David", "Eva", "Frank"])),
            Arc::new(Float64Vector::from_slice(vec![40.1, 50.2, 60.3])),
        ];
        let batch2 = RecordBatch::new(schema.clone(), batch2_columns).unwrap();

        let batch3_columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![7, 8, 9])),
            Arc::new(StringVector::from(vec!["Grace", "Henry", "Ivy"])),
            Arc::new(Float64Vector::from_slice(vec![70.4, 80.5, 90.6])),
        ];
        let batch3 = RecordBatch::new(schema.clone(), batch3_columns).unwrap();

        // Combine all batches into a RecordBatches collection
        let recordbatches = RecordBatches::try_new(schema, vec![batch1, batch2, batch3]).unwrap();

        // Test with different compression types
        let compression_types = vec![
            CompressionType::Gzip,
            CompressionType::Bzip2,
            CompressionType::Xz,
            CompressionType::Zstd,
        ];

        // Create a temporary file path
        let temp_dir = common_test_util::temp_dir::create_temp_dir("test_compressed_csv");
        for compression_type in compression_types {
            let format = CsvFormat {
                compression_type,
                ..CsvFormat::default()
            };

            // Use correct format without Debug formatter
            let compressed_file_name =
                format!("test_compressed_csv.{}", compression_type.file_extension());
            let compressed_file_path = temp_dir.path().join(&compressed_file_name);
            let compressed_file_path_str = compressed_file_path.to_str().unwrap();

            // Create a simple file store for testing
            let store = test_store("/");

            // Export CSV with compression
            let rows = stream_to_csv(
                Box::pin(DfRecordBatchStreamAdapter::new(recordbatches.as_stream())),
                store,
                compressed_file_path_str,
                1024,
                1,
                &format,
            )
            .await
            .unwrap();

            assert_eq!(rows, 9);

            // Verify compressed file was created and has content
            assert!(compressed_file_path.exists());
            let file_size = std::fs::metadata(&compressed_file_path).unwrap().len();
            assert!(file_size > 0);

            // Verify the file is actually compressed
            let file_content = std::fs::read(&compressed_file_path).unwrap();
            // Compressed files should not start with CSV header
            // They should have compression magic bytes
            match compression_type {
                CompressionType::Gzip => {
                    // Gzip magic bytes: 0x1f 0x8b
                    assert_eq!(file_content[0], 0x1f, "Gzip file should start with 0x1f");
                    assert_eq!(
                        file_content[1], 0x8b,
                        "Gzip file should have 0x8b as second byte"
                    );
                }
                CompressionType::Bzip2 => {
                    // Bzip2 magic bytes: 'BZ'
                    assert_eq!(file_content[0], b'B', "Bzip2 file should start with 'B'");
                    assert_eq!(
                        file_content[1], b'Z',
                        "Bzip2 file should have 'Z' as second byte"
                    );
                }
                CompressionType::Xz => {
                    // XZ magic bytes: 0xFD '7zXZ'
                    assert_eq!(file_content[0], 0xFD, "XZ file should start with 0xFD");
                }
                CompressionType::Zstd => {
                    // Zstd magic bytes: 0x28 0xB5 0x2F 0xFD
                    assert_eq!(file_content[0], 0x28, "Zstd file should start with 0x28");
                    assert_eq!(
                        file_content[1], 0xB5,
                        "Zstd file should have 0xB5 as second byte"
                    );
                }
                _ => {}
            }

            // Verify the compressed file can be decompressed and content matches original data
            let store = test_store("/");
            let schema = Arc::new(
                CsvFormat {
                    compression_type,
                    ..Default::default()
                }
                .infer_schema(&store, compressed_file_path_str)
                .await
                .unwrap(),
            );
            let csv_source = CsvSource::new(schema).with_batch_size(8192);

            let stream = file_to_stream(
                &store,
                compressed_file_path_str,
                csv_source.clone(),
                None,
                compression_type,
            )
            .await
            .unwrap();

            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            let pretty_print = arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string();
            let expected = r#"+----+---------+-------+
| id | name    | value |
+----+---------+-------+
| 1  | Alice   | 10.5  |
| 2  | Bob     | 20.3  |
| 3  | Charlie | 30.7  |
| 4  | David   | 40.1  |
| 5  | Eva     | 50.2  |
| 6  | Frank   | 60.3  |
| 7  | Grace   | 70.4  |
| 8  | Henry   | 80.5  |
| 9  | Ivy     | 90.6  |
+----+---------+-------+"#;
            assert_eq!(expected, pretty_print);
        }
    }

    #[tokio::test]
    async fn test_tolerant_csv_stream_continues_after_parse_error() {
        let temp_dir = common_test_util::temp_dir::create_temp_dir("test_tolerant_csv_stream");
        let csv_file_path = temp_dir.path().join("input.csv");
        std::fs::write(
            &csv_file_path,
            "id,name,value\n1,Alice,10.5\nbad,Bad,20.0\nworse,Bad,21.0\n2,Bob,30.5",
        )
        .unwrap();

        let store = test_store("/");
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let path = csv_file_path.to_str().unwrap();

        let stream =
            tolerant_csv_stream(&store, path, schema, vec![0, 1, 2], &CsvFormat::default())
                .await
                .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let pretty_print = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        let expected = r#"+----+-------+-------+
| id | name  | value |
+----+-------+-------+
| 1  | Alice | 10.5  |
| 2  | Bob   | 30.5  |
+----+-------+-------+"#;
        assert_eq!(expected, pretty_print);
    }

    #[tokio::test]
    async fn test_tolerant_csv_stream_fails_on_structural_csv_error() {
        let temp_dir =
            common_test_util::temp_dir::create_temp_dir("test_tolerant_csv_stream_csv_error");
        let csv_file_path = temp_dir.path().join("input.csv");
        std::fs::write(&csv_file_path, "id,name,value\n1,Alice,10.5\n2,Bob\n").unwrap();

        let store = test_store("/");
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let path = csv_file_path.to_str().unwrap();

        let stream =
            tolerant_csv_stream(&store, path, schema, vec![0, 1, 2], &CsvFormat::default())
                .await
                .unwrap();
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();

        assert!(error.to_string().contains("incorrect number of fields"));
    }
}
