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
use std::io::{self, BufReader};
use std::str::FromStr;
use std::task::Poll;

use arrow::error::ArrowError;
use arrow::json;
use arrow::json::WriterBuilder;
use arrow::json::reader::{ValueIter, infer_json_schema_from_iterator};
use arrow::json::writer::LineDelimited;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use common_runtime;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonFormat {
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
    pub timestamp_format: Option<String>,
    pub time_format: Option<String>,
    pub date_format: Option<String>,
}

impl TryFrom<&HashMap<String, String>> for JsonFormat {
    type Error = error::Error;

    fn try_from(value: &HashMap<String, String>) -> Result<Self> {
        let mut format = JsonFormat::default();
        if let Some(compression_type) = value.get(file_format::FORMAT_COMPRESSION_TYPE) {
            format.compression_type = CompressionType::from_str(compression_type)?
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
        format.timestamp_format = value.get(file_format::TIMESTAMP_FORMAT).cloned();
        format.time_format = value.get(file_format::TIME_FORMAT).cloned();
        format.date_format = value.get(file_format::DATE_FORMAT).cloned();
        Ok(format)
    }
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::Uncompressed,
            timestamp_format: None,
            time_format: None,
            date_format: None,
        }
    }
}

#[async_trait]
impl FileFormat for JsonFormat {
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

        let schema_infer_max_record = self.schema_infer_max_record;

        common_runtime::spawn_blocking_global(move || {
            let mut reader = BufReader::new(SyncIoBridge::new(decoded));

            let iter = ValueIter::new(&mut reader, schema_infer_max_record);

            let schema = infer_json_schema_from_iterator(iter).context(error::InferSchemaSnafu)?;

            Ok(normalize_infer_schema(schema))
        })
        .await
        .context(error::JoinHandleSnafu)?
    }
}

/// Returns a stream of the json file at `path` that reads the fields of `schema`.
///
/// Unlike the stream of a `JsonSource`, a string column also takes a number or a boolean, as
/// the text it has in the file. So a column whose numbers must be parsed from every digit can
/// be read as a string, see [`parse_decimal_columns`](super::decimal::parse_decimal_columns).
pub async fn json_text_stream(
    store: &ObjectStore,
    path: &str,
    schema: SchemaRef,
    format: &JsonFormat,
    batch_size: usize,
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
    json_text_stream_from_reader(reader, schema, batch_size)
}

fn json_text_stream_from_reader(
    reader: BoxStream<'static, io::Result<Bytes>>,
    schema: SchemaRef,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    let mut builder = Some(
        json::ReaderBuilder::new(schema.clone())
            .with_coerce_primitive(true)
            .with_batch_size(batch_size),
    );
    let mut decoder = None;

    let mut upstream = reader.fuse();
    let mut buffered = Bytes::new();
    let mut input_finished = false;
    let stream = futures::stream::poll_fn(move |cx| {
        if let Some(builder) = builder.take() {
            decoder = Some(builder.build_decoder()?);
        }
        let Some(decoder) = decoder.as_mut() else {
            return Poll::Ready(None);
        };

        loop {
            if input_finished {
                return Poll::Ready(decoder.flush().transpose());
            }

            if buffered.is_empty() {
                match futures::ready!(upstream.poll_next_unpin(cx)) {
                    Some(Ok(bytes)) => buffered = bytes,
                    Some(Err(error)) => return Poll::Ready(Some(Err(error.into()))),
                    None => input_finished = true,
                }
                continue;
            }

            let decoded = decoder.decode(buffered.as_ref())?;
            buffered.advance(decoded);
            // The decoder stops before the end of the input once it holds a full batch.
            if !buffered.is_empty()
                && let Some(batch) = decoder.flush()?
            {
                return Poll::Ready(Some(Ok(batch)));
            }
        }
    })
    .map(|result: std::result::Result<RecordBatch, ArrowError>| result.map_err(Into::into));

    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

pub async fn stream_to_json(
    stream: SendableRecordBatchStream,
    store: ObjectStore,
    path: &str,
    threshold: usize,
    concurrency: usize,
    format: &JsonFormat,
) -> Result<usize> {
    stream_to_file(
        stream,
        store,
        path,
        threshold,
        concurrency,
        format.compression_type,
        |buffer| {
            let mut builder = WriterBuilder::new().with_explicit_nulls(true);
            if let Some(timestamp_format) = &format.timestamp_format {
                builder = builder.with_timestamp_format(timestamp_format.to_owned());
            }
            if let Some(time_format) = &format.time_format {
                builder = builder.with_time_format(time_format.to_owned());
            }
            if let Some(date_format) = &format.date_format {
                builder = builder.with_date_format(date_format.to_owned());
            }
            builder.build::<_, LineDelimited>(buffer)
        },
    )
    .await
}

impl DfRecordBatchEncoder for json::Writer<SharedBuffer, LineDelimited> {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write(batch).context(error::WriteRecordBatchSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::AsArray;
    use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use common_test_util::find_workspace_path;
    use datafusion::datasource::physical_plan::{FileSource, JsonSource};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float64Vector, StringVector, UInt32Vector, VectorRef};
    use futures::TryStreamExt;

    use super::*;
    use crate::file_format::{
        FORMAT_COMPRESSION_TYPE, FORMAT_SCHEMA_INFER_MAX_RECORD, FileFormat, file_to_stream,
    };
    use crate::test_util::{format_schema, test_store};

    fn test_data_root() -> String {
        find_workspace_path("/src/common/datasource/tests/json")
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn test_json_text_stream() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("price", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("k", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("n", arrow_schema::DataType::Int64, true),
        ]));
        let input: &[u8] = br#"{"k":"a","price":1.2500000000000000000000000000000000000000e0,"n":1}
{"k":"b","price":0e0,"n":2,"unknown":true}
{"k":"c","price":null}
{"n":4,"price":123456789012345678.12,"k":"d"}
{"k":"e","price":"5"}"#;

        // Every size of input chunk and of batch gives the same rows.
        for chunk_size in [1, 7, input.len()] {
            for batch_size in [1, 2, 8192] {
                let chunks = input
                    .chunks(chunk_size)
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                    .collect::<Vec<_>>();
                let stream = json_text_stream_from_reader(
                    futures::stream::iter(chunks).boxed(),
                    schema.clone(),
                    batch_size,
                )
                .unwrap();
                let batches = stream.try_collect::<Vec<_>>().await.unwrap();
                assert!(batches.iter().all(|batch| batch.num_rows() <= batch_size));
                let batch = arrow::compute::concat_batches(&schema, &batches).unwrap();

                let prices = batch.column(0).as_string::<i32>();
                assert_eq!(
                    prices.iter().collect::<Vec<_>>(),
                    vec![
                        Some("1.2500000000000000000000000000000000000000e0"),
                        Some("0e0"),
                        None,
                        Some("123456789012345678.12"),
                        Some("5"),
                    ]
                );
                let keys = batch.column(1).as_string::<i32>();
                assert_eq!(
                    keys.iter().collect::<Vec<_>>(),
                    vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]
                );
                let numbers = batch
                    .column(2)
                    .as_primitive::<arrow::datatypes::Int64Type>();
                assert_eq!(
                    numbers.iter().collect::<Vec<_>>(),
                    vec![Some(1), Some(2), None, Some(4), None]
                );
            }
        }

        // A record that is cut off is an error.
        let stream = json_text_stream_from_reader(
            futures::stream::iter(vec![Ok(Bytes::from_static(br#"{"k":"a","price":1"#))]).boxed(),
            schema,
            8192,
        )
        .unwrap();
        assert!(stream.try_collect::<Vec<_>>().await.is_err());
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let json = JsonFormat::default();
        let store = test_store(&test_data_root());
        let schema = json.infer_schema(&store, "simple.json").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "a: Int64: NULL",
                "b: Float64: NULL",
                "c: Boolean: NULL",
                "d: Utf8: NULL",
            ],
            formatted
        );
    }

    #[tokio::test]
    async fn normalize_infer_schema() {
        let json = JsonFormat {
            schema_infer_max_record: Some(3),
            ..JsonFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = json.infer_schema(&store, "max_infer.json").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(vec!["num: Int64: NULL", "str: Utf8: NULL"], formatted,);
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let json = JsonFormat {
            schema_infer_max_record: Some(3),
            ..JsonFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.json")
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec!["a: Int64: NULL", "b: Float64: NULL", "c: Boolean: NULL"],
            formatted
        );
    }

    #[test]
    fn test_try_from() {
        let map = HashMap::new();
        let format = JsonFormat::try_from(&map).unwrap();

        assert_eq!(format, JsonFormat::default());

        let map = HashMap::from([
            (
                FORMAT_SCHEMA_INFER_MAX_RECORD.to_string(),
                "2000".to_string(),
            ),
            (FORMAT_COMPRESSION_TYPE.to_string(), "zstd".to_string()),
        ]);
        let format = JsonFormat::try_from(&map).unwrap();

        assert_eq!(
            format,
            JsonFormat {
                compression_type: CompressionType::Zstd,
                schema_infer_max_record: Some(2000),
                ..JsonFormat::default()
            }
        );
    }

    #[tokio::test]
    async fn test_compressed_json() {
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
        let temp_dir = common_test_util::temp_dir::create_temp_dir("test_compressed_json");
        for compression_type in compression_types {
            let format = JsonFormat {
                compression_type,
                ..JsonFormat::default()
            };

            let compressed_file_name =
                format!("test_compressed_json.{}", compression_type.file_extension());
            let compressed_file_path = temp_dir.path().join(&compressed_file_name);

            // Create a simple file store for testing
            let store = test_store(temp_dir.path().to_str().unwrap());

            // Export JSON with compression
            let rows = stream_to_json(
                Box::pin(DfRecordBatchStreamAdapter::new(recordbatches.as_stream())),
                store,
                &compressed_file_name,
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
            // Compressed files should not start with '{' (JSON character)
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
            let store = test_store(temp_dir.path().to_str().unwrap());
            let schema = Arc::new(
                JsonFormat {
                    compression_type,
                    ..Default::default()
                }
                .infer_schema(&store, &compressed_file_name)
                .await
                .unwrap(),
            );
            let json_source = JsonSource::new(schema).with_batch_size(8192);

            let stream = file_to_stream(
                &store,
                &compressed_file_name,
                json_source.clone(),
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
}
