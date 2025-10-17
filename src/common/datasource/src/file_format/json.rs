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
use std::io::BufReader;
use std::str::FromStr;

use arrow::json;
use arrow::json::reader::{ValueIter, infer_json_schema_from_iterator};
use arrow::json::writer::LineDelimited;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use common_runtime;
use datafusion::physical_plan::SendableRecordBatchStream;
use object_store::ObjectStore;
use snafu::ResultExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;

use crate::buffered_writer::DfRecordBatchEncoder;
use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::file_format::{self, FileFormat, stream_to_file};
use crate::share_buffer::SharedBuffer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonFormat {
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
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
        Ok(format)
    }
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::Uncompressed,
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

            Ok(schema)
        })
        .await
        .context(error::JoinHandleSnafu)?
    }
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
        json::LineDelimitedWriter::new,
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
    use common_test_util::find_workspace_path;

    use super::*;
    use crate::file_format::{FORMAT_COMPRESSION_TYPE, FORMAT_SCHEMA_INFER_MAX_RECORD, FileFormat};
    use crate::test_util::{format_schema, test_store};

    fn test_data_root() -> String {
        find_workspace_path("/src/common/datasource/tests/json")
            .display()
            .to_string()
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
            }
        );
    }

    #[tokio::test]
    async fn test_compressed_json_export() {
        use std::sync::Arc;

        use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
        use common_recordbatch::{RecordBatch, RecordBatches};
        use datatypes::prelude::ConcreteDataType;
        use datatypes::schema::{ColumnSchema, Schema};
        use datatypes::vectors::{Float64Vector, StringVector, UInt32Vector, VectorRef};

        // Create test data
        let column_schemas = vec![
            ColumnSchema::new("id", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        ];
        let schema = Arc::new(Schema::new(column_schemas));

        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3])),
            Arc::new(StringVector::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Float64Vector::from_slice(vec![10.5, 20.3, 30.7])),
        ];

        let recordbatch = RecordBatch::new(schema.clone(), columns).unwrap();
        let recordbatches = RecordBatches::try_new(schema, vec![recordbatch]).unwrap();

        // Test with different compression types
        let compression_types = vec![
            CompressionType::Gzip,
            CompressionType::Bzip2,
            CompressionType::Xz,
            CompressionType::Zstd,
            CompressionType::Uncompressed,
        ];

        for compression_type in compression_types {
            let format = JsonFormat {
                compression_type,
                ..JsonFormat::default()
            };

            // Create a temporary file path
            let temp_dir = std::env::temp_dir();
            let file_name = format!("test_compressed_{:?}.json", compression_type);
            let file_path = temp_dir.join(&file_name);
            let path_str = file_path.to_str().unwrap();

            // Create a simple file store for testing
            let store = test_store("/");

            // Export JSON with compression
            let rows = stream_to_json(
                Box::pin(DfRecordBatchStreamAdapter::new(recordbatches.as_stream())),
                store,
                path_str,
                1024,
                1,
                &format,
            )
            .await
            .unwrap();

            assert_eq!(rows, 3);

            // Verify file was created and has content
            assert!(file_path.exists());
            let file_size = std::fs::metadata(&file_path).unwrap().len();
            assert!(file_size > 0);

            // Clean up
            let _ = std::fs::remove_file(&file_path);
        }
    }
}
