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

use std::result;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::error::Result as DatafusionResult;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ArrowWriter, parquet_to_arrow_schema};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::{
    KeyValue, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use futures::future::BoxFuture;
use object_store::{FuturesAsyncReader, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use crate::DEFAULT_WRITE_BUFFER_SIZE;
use crate::buffered_writer::{ArrowWriterCloser, DfRecordBatchEncoder};
use crate::error::{
    self, InvalidParquetTableNameMetadataSnafu, Result, WriteObjectSnafu, WriteParquetSnafu,
};
use crate::file_format::FileFormat;
use crate::share_buffer::SharedBuffer;

/// Parquet file metadata key for the source GreptimeDB table name.
///
/// The value is an opaque, unqualified GreptimeDB table name.
pub const PARQUET_TABLE_NAME_KEY: &str = "greptime.table_name";

/// Extracts the table name identity from one Parquet file's key/value metadata.
///
/// Missing metadata or a missing key represents an unknown table identity.
pub fn parquet_table_name_from_metadata(metadata: Option<&[KeyValue]>) -> Result<Option<&str>> {
    let mut table_name = None;
    for item in metadata.unwrap_or_default() {
        if item.key != PARQUET_TABLE_NAME_KEY {
            continue;
        }

        let Some(value) = item.value.as_deref() else {
            return InvalidParquetTableNameMetadataSnafu {
                reason: format!("metadata key '{PARQUET_TABLE_NAME_KEY}' has no value"),
            }
            .fail();
        };
        if value.is_empty() {
            return InvalidParquetTableNameMetadataSnafu {
                reason: format!("metadata key '{PARQUET_TABLE_NAME_KEY}' has an empty value"),
            }
            .fail();
        }
        if table_name.is_some_and(|table_name| table_name != value) {
            return InvalidParquetTableNameMetadataSnafu {
                reason: format!("metadata key '{PARQUET_TABLE_NAME_KEY}' has conflicting values"),
            }
            .fail();
        }
        table_name = Some(value);
    }

    Ok(table_name)
}

/// Extracts one consistent table identity from a batch of Parquet file metadata.
pub fn parquet_table_name_from_metadata_batch<'a>(
    metadata: impl IntoIterator<Item = Option<&'a [KeyValue]>>,
) -> Result<Option<&'a str>> {
    let mut table_name = None;
    let mut saw_missing = false;
    for metadata in metadata {
        match parquet_table_name_from_metadata(metadata)? {
            Some(value) => {
                if saw_missing || table_name.is_some_and(|table_name| table_name != value) {
                    return InvalidParquetTableNameMetadataSnafu {
                        reason: "Parquet files have inconsistent table names".to_string(),
                    }
                    .fail();
                }
                table_name = Some(value);
            }
            None => {
                if table_name.is_some() {
                    return InvalidParquetTableNameMetadataSnafu {
                        reason: "Parquet files have inconsistent table names".to_string(),
                    }
                    .fail();
                }
                saw_missing = true;
            }
        }
    }
    Ok(table_name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParquetFormat {}

#[async_trait]
impl FileFormat for ParquetFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let meta = store
            .stat(path)
            .await
            .context(error::ReadObjectSnafu { path })?;

        let mut reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?
            .into_futures_async_read(0..meta.content_length())
            .await
            .context(error::ReadObjectSnafu { path })?
            .compat();

        let metadata = reader
            .get_metadata(None)
            .await
            .context(error::ReadParquetSnafuSnafu)?;

        let file_metadata = metadata.file_metadata();
        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .context(error::ParquetToSchemaSnafu)?;

        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct DefaultParquetFileReaderFactory {
    object_store: ObjectStore,
}

/// Returns a AsyncFileReader factory
impl DefaultParquetFileReaderFactory {
    pub fn new(object_store: ObjectStore) -> Self {
        Self { object_store }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DatafusionResult<Box<dyn AsyncFileReader + Send>> {
        let path = partitioned_file.path().to_string();
        let object_store = self.object_store.clone();

        Ok(Box::new(LazyParquetFileReader::new(
            object_store,
            path,
            metadata_size_hint,
        )))
    }
}

pub struct LazyParquetFileReader {
    object_store: ObjectStore,
    reader: Option<Compat<FuturesAsyncReader>>,
    file_size: Option<u64>,
    metadata_size_hint: Option<usize>,
    path: String,
}

impl LazyParquetFileReader {
    pub fn new(object_store: ObjectStore, path: String, metadata_size_hint: Option<usize>) -> Self {
        LazyParquetFileReader {
            object_store,
            path,
            reader: None,
            file_size: None,
            metadata_size_hint,
        }
    }

    /// Must initialize the reader, or throw an error from the future.
    async fn maybe_initialize(&mut self) -> result::Result<(), object_store::Error> {
        if self.reader.is_none() {
            let meta = self.object_store.stat(&self.path).await?;
            self.file_size = Some(meta.content_length());
            let reader = self
                .object_store
                .reader(&self.path)
                .await?
                .into_futures_async_read(0..meta.content_length())
                .await?
                .compat();
            self.reader = Some(reader);
        }

        Ok(())
    }
}

impl AsyncFileReader for LazyParquetFileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        Box::pin(async move {
            self.maybe_initialize()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            // Safety: Must initialized
            self.reader.as_mut().unwrap().get_bytes(range).await
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            self.maybe_initialize()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;

            let metadata_opts = options.map(|o| o.metadata_options().clone());
            let column_index_policy =
                options.map_or(PageIndexPolicy::Skip, |o| o.column_index_policy());
            let offset_index_policy =
                options.map_or(PageIndexPolicy::Skip, |o| o.offset_index_policy());
            let metadata_reader = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .with_column_index_policy(column_index_policy)
                .with_offset_index_policy(offset_index_policy)
                .with_prefetch_hint(self.metadata_size_hint);

            let metadata = metadata_reader
                .load_and_finish(self.reader.as_mut().unwrap(), self.file_size.unwrap())
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

impl DfRecordBatchEncoder for ArrowWriter<SharedBuffer> {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write(batch).context(error::EncodeRecordBatchSnafu)
    }
}

#[async_trait]
impl ArrowWriterCloser for ArrowWriter<SharedBuffer> {
    async fn close(self) -> Result<ParquetMetaData> {
        self.close().context(error::EncodeRecordBatchSnafu)
    }
}

/// Output the stream to a parquet file.
///
/// Returns number of rows written.
pub async fn stream_to_parquet(
    stream: SendableRecordBatchStream,
    schema: datatypes::schema::SchemaRef,
    store: ObjectStore,
    path: &str,
    concurrency: usize,
) -> Result<usize> {
    stream_to_parquet_with_metadata(stream, schema, store, path, concurrency, Vec::new()).await
}

/// Output the stream to a parquet file with custom key/value footer metadata.
///
/// Returns number of rows written.
pub async fn stream_to_parquet_with_metadata(
    mut stream: SendableRecordBatchStream,
    schema: datatypes::schema::SchemaRef,
    store: ObjectStore,
    path: &str,
    concurrency: usize,
    metadata: Vec<(String, String)>,
) -> Result<usize> {
    let metadata = if metadata.is_empty() {
        None
    } else {
        Some(
            metadata
                .into_iter()
                .map(|(key, value)| KeyValue::new(key, value))
                .collect(),
        )
    };
    let write_props = column_wise_config(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_truncate_length(None)
            .set_column_index_truncate_length(None)
            .set_key_value_metadata(metadata),
        schema,
    )
    .build();
    let inner_writer = store
        .writer_with(path)
        .concurrent(concurrency)
        .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
        .await
        .map(|w| w.into_futures_async_write().compat_write())
        .context(WriteObjectSnafu { path })?;

    let mut writer = AsyncArrowWriter::try_new(inner_writer, stream.schema(), Some(write_props))
        .context(WriteParquetSnafu { path })?;
    let mut rows_written = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch.context(error::ReadRecordBatchSnafu)?;
        writer
            .write(&batch)
            .await
            .context(WriteParquetSnafu { path })?;
        rows_written += batch.num_rows();
    }
    writer.close().await.context(WriteParquetSnafu { path })?;
    Ok(rows_written)
}

/// Customizes per-column properties.
fn column_wise_config(
    mut props: WriterPropertiesBuilder,
    schema: SchemaRef,
) -> WriterPropertiesBuilder {
    // Disable dictionary for timestamp column, since for increasing timestamp column,
    // the dictionary pages will be larger than data pages.
    for col in schema.column_schemas() {
        if col.data_type.is_timestamp() {
            let path = ColumnPath::new(vec![col.name.clone()]);
            props = props
                .set_column_dictionary_enabled(path.clone(), false)
                .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED)
        }
    }
    props
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use common_test_util::find_workspace_path;
    use datafusion::parquet::arrow::async_reader::AsyncFileReader;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema as GreptimeSchema};
    use datatypes::vectors::{UInt32Vector, VectorRef};
    use parquet::file::metadata::KeyValue;

    use super::*;
    use crate::test_util::{format_schema, test_store, test_tmp_store};

    fn test_data_root() -> String {
        find_workspace_path("/src/common/datasource/tests/parquet")
            .display()
            .to_string()
    }

    fn test_record_batch_stream() -> (SendableRecordBatchStream, datatypes::schema::SchemaRef) {
        let schema = Arc::new(GreptimeSchema::new(vec![ColumnSchema::new(
            "number",
            ConcreteDataType::uint32_datatype(),
            false,
        )]));
        let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice([1, 2, 3]))];
        let batch = RecordBatch::new(schema.clone(), columns).unwrap();
        let batches = RecordBatches::try_new(schema.clone(), vec![batch]).unwrap();
        (
            Box::pin(DfRecordBatchStreamAdapter::new(batches.as_stream())),
            schema,
        )
    }

    async fn written_key_values(store: ObjectStore, path: &str) -> Option<Vec<KeyValue>> {
        let mut reader = LazyParquetFileReader::new(store, path.to_string(), None);
        reader
            .get_metadata(None)
            .await
            .unwrap()
            .file_metadata()
            .key_value_metadata()
            .cloned()
    }

    #[tokio::test]
    async fn legacy_parquet_writer_does_not_write_table_name_metadata() {
        let (store, dir) = test_tmp_store("legacy_parquet_writer_metadata");
        let path = format!("{}/output.parquet", dir.path().display());
        let (stream, schema) = test_record_batch_stream();

        assert_eq!(
            3,
            stream_to_parquet(stream, schema, store.clone(), &path, 1)
                .await
                .unwrap()
        );

        let user_key_values = written_key_values(store, &path)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|item| item.key != "ARROW:schema")
            .collect::<Vec<_>>();
        assert!(user_key_values.is_empty(), "{user_key_values:?}");
    }

    #[tokio::test]
    async fn parquet_writer_writes_table_name_metadata_to_footer() {
        let (store, dir) = test_tmp_store("parquet_writer_table_name_metadata");
        let path = format!("{}/output.parquet", dir.path().display());
        let (stream, schema) = test_record_batch_stream();

        assert_eq!(
            3,
            stream_to_parquet_with_metadata(
                stream,
                schema,
                store.clone(),
                &path,
                1,
                vec![(PARQUET_TABLE_NAME_KEY.to_string(), "my_table".to_string(),)],
            )
            .await
            .unwrap()
        );

        let table_names = written_key_values(store, &path)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|item| item.key == PARQUET_TABLE_NAME_KEY)
            .collect::<Vec<_>>();
        assert_eq!(1, table_names.len());
        assert_eq!(Some("my_table"), table_names[0].value.as_deref());
    }

    #[test]
    fn missing_parquet_table_name_is_unknown() {
        let metadata = [KeyValue::new("other".to_string(), "value".to_string())];

        assert_eq!(
            None,
            parquet_table_name_from_metadata(Some(&metadata)).unwrap()
        );
    }

    #[test]
    fn present_parquet_table_name_is_returned_as_opaque_value() {
        let metadata = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            "schema/table.with.dots".to_string(),
        )];

        assert_eq!(
            Some("schema/table.with.dots"),
            parquet_table_name_from_metadata(Some(&metadata)).unwrap()
        );
    }

    #[test]
    fn parquet_table_name_without_value_is_invalid() {
        let metadata = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            None::<String>,
        )];

        assert!(parquet_table_name_from_metadata(Some(&metadata)).is_err());
    }

    #[test]
    fn empty_parquet_table_name_is_invalid() {
        let metadata = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            String::new(),
        )];

        assert!(parquet_table_name_from_metadata(Some(&metadata)).is_err());
    }

    #[test]
    fn conflicting_parquet_table_names_are_invalid() {
        let metadata = [
            KeyValue::new(PARQUET_TABLE_NAME_KEY.to_string(), "first".to_string()),
            KeyValue::new(PARQUET_TABLE_NAME_KEY.to_string(), "second".to_string()),
        ];

        assert!(parquet_table_name_from_metadata(Some(&metadata)).is_err());
    }

    #[test]
    fn identical_duplicate_parquet_table_names_are_accepted() {
        let metadata = [
            KeyValue::new(PARQUET_TABLE_NAME_KEY.to_string(), "same".to_string()),
            KeyValue::new(PARQUET_TABLE_NAME_KEY.to_string(), "same".to_string()),
        ];

        assert_eq!(
            Some("same"),
            parquet_table_name_from_metadata(Some(&metadata)).unwrap()
        );
    }

    #[test]
    fn empty_parquet_metadata_batch_is_unknown() {
        assert_eq!(
            None,
            parquet_table_name_from_metadata_batch(std::iter::empty()).unwrap()
        );
    }

    #[test]
    fn all_missing_parquet_metadata_batch_is_unknown() {
        assert_eq!(
            None,
            parquet_table_name_from_metadata_batch([None, None]).unwrap()
        );
    }

    #[test]
    fn same_parquet_table_name_across_batch_is_returned() {
        let first = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            "same".to_string(),
        )];
        let second = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            "same".to_string(),
        )];

        assert_eq!(
            Some("same"),
            parquet_table_name_from_metadata_batch([
                Some(first.as_slice()),
                Some(second.as_slice())
            ])
            .unwrap()
        );
    }

    #[test]
    fn different_parquet_table_names_across_batch_are_invalid() {
        let first = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            "first".to_string(),
        )];
        let second = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            "second".to_string(),
        )];

        assert!(
            parquet_table_name_from_metadata_batch([
                Some(first.as_slice()),
                Some(second.as_slice())
            ])
            .is_err()
        );
    }

    #[test]
    fn invalid_parquet_table_name_in_batch_is_propagated() {
        let invalid = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            None::<String>,
        )];

        assert!(parquet_table_name_from_metadata_batch([Some(invalid.as_slice())]).is_err());
    }

    #[test]
    fn mixed_present_and_missing_parquet_table_names_are_invalid() {
        let present = [KeyValue::new(
            PARQUET_TABLE_NAME_KEY.to_string(),
            "table".to_string(),
        )];
        let missing = [KeyValue::new("other".to_string(), "value".to_string())];

        assert!(
            parquet_table_name_from_metadata_batch([
                Some(present.as_slice()),
                Some(missing.as_slice()),
            ])
            .is_err()
        );
        assert!(
            parquet_table_name_from_metadata_batch([
                Some(missing.as_slice()),
                Some(present.as_slice()),
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let json = ParquetFormat::default();
        let store = test_store(&test_data_root());
        let schema = json.infer_schema(&store, "basic.parquet").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(vec!["num: Int64: NULL", "str: Utf8: NULL"], formatted);
    }
}
