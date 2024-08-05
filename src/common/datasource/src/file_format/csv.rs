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
use std::str::FromStr;
use std::sync::Arc;

use arrow::csv;
#[allow(deprecated)]
use arrow::csv::reader::infer_reader_schema as infer_csv_schema;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use common_runtime;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use derive_builder::Builder;
use object_store::ObjectStore;
use snafu::ResultExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;

use super::stream_to_file;
use crate::buffered_writer::DfRecordBatchEncoder;
use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::file_format::{self, open_with_decoder, FileFormat};
use crate::share_buffer::SharedBuffer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvFormat {
    pub has_header: bool,
    pub delimiter: u8,
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
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
            format.has_header = has_header.parse().map_err(|_| {
                error::ParseFormatSnafu {
                    key: file_format::FORMAT_HAS_HEADER,
                    value: has_header,
                }
                .build()
            })?;
        }
        Ok(format)
    }
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::Uncompressed,
        }
    }
}

#[derive(Debug, Clone, Builder)]
pub struct CsvConfig {
    batch_size: usize,
    file_schema: SchemaRef,
    #[builder(default = "None")]
    file_projection: Option<Vec<usize>>,
    #[builder(default = "true")]
    has_header: bool,
    #[builder(default = "b','")]
    delimiter: u8,
}

impl CsvConfig {
    fn builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new(self.file_schema.clone())
            .with_delimiter(self.delimiter)
            .with_batch_size(self.batch_size)
            .with_header(self.has_header);

        if let Some(proj) = &self.file_projection {
            builder = builder.with_projection(proj.clone());
        }

        builder
    }
}

#[derive(Debug, Clone)]
pub struct CsvOpener {
    config: Arc<CsvConfig>,
    object_store: Arc<ObjectStore>,
    compression_type: CompressionType,
}

impl CsvOpener {
    /// Return a new [`CsvOpener`]. The caller must ensure [`CsvConfig`].file_schema must correspond to the opening file.
    pub fn new(
        config: CsvConfig,
        object_store: ObjectStore,
        compression_type: CompressionType,
    ) -> Self {
        CsvOpener {
            config: Arc::new(config),
            object_store: Arc::new(object_store),
            compression_type,
        }
    }
}

impl FileOpener for CsvOpener {
    fn open(&self, meta: FileMeta) -> DataFusionResult<FileOpenFuture> {
        open_with_decoder(
            self.object_store.clone(),
            meta.location().to_string(),
            self.compression_type,
            || Ok(self.config.builder().build_decoder()),
        )
    }
}

#[allow(deprecated)]
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

            let (schema, _records_read) =
                infer_csv_schema(reader, delimiter, schema_infer_max_record, has_header)
                    .context(error::InferSchemaSnafu)?;
            Ok(schema)
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
) -> Result<usize> {
    stream_to_file(stream, store, path, threshold, concurrency, |buffer| {
        csv::Writer::new(buffer)
    })
    .await
}

impl DfRecordBatchEncoder for csv::Writer<SharedBuffer> {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write(batch).context(error::WriteRecordBatchSnafu)
    }
}

#[cfg(test)]
mod tests {

    use common_test_util::find_workspace_path;

    use super::*;
    use crate::file_format::{
        FileFormat, FORMAT_COMPRESSION_TYPE, FORMAT_DELIMITER, FORMAT_HAS_HEADER,
        FORMAT_SCHEMA_INFER_MAX_RECORD,
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
                "c10: Int64: NULL",
                "c11: Float64: NULL",
                "c12: Float64: NULL",
                "c13: Utf8: NULL"
            ],
            formatted,
        );
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let json = CsvFormat {
            schema_infer_max_record: Some(3),
            ..CsvFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = json
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

        let json = CsvFormat::default();
        let store = test_store(&test_data_root());
        let schema = json
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
            }
        );
    }
}
