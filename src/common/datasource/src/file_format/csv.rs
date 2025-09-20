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

use arrow::csv::reader::Format;
use arrow::csv::{self, WriterBuilder};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvFormat {
    pub has_header: bool,
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
            format.has_header = has_header.parse().map_err(|_| {
                error::ParseFormatSnafu {
                    key: file_format::FORMAT_HAS_HEADER,
                    value: has_header,
                }
                .build()
            })?;
        };
        if let Some(timestamp_format) = value.get(file_format::TIMESTAMP_FORMAT) {
            format.timestamp_format = Some(timestamp_format.clone());
        }
        if let Some(time_format) = value.get(file_format::TIME_FORMAT) {
            format.timestamp_format = Some(time_format.clone());
        }
        if let Some(date_format) = value.get(file_format::DATE_FORMAT) {
            format.timestamp_format = Some(date_format.clone());
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
    format: &CsvFormat,
) -> Result<usize> {
    stream_to_file(stream, store, path, threshold, concurrency, |buffer| {
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
        FORMAT_COMPRESSION_TYPE, FORMAT_DELIMITER, FORMAT_HAS_HEADER,
        FORMAT_SCHEMA_INFER_MAX_RECORD, FileFormat,
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
                timestamp_format: None,
                time_format: None,
                date_format: None
            }
        );
    }
}
