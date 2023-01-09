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

//! Parquet sst format.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_compat::CompatExt;
use async_stream::try_stream;
use async_trait::async_trait;
use datatypes::arrow::record_batch::RecordBatch;
use futures_util::{Stream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;
use table::predicate::Predicate;
use tokio::io::BufReader;

use crate::error::{
    self, NewRecordBatchSnafu, ReadObjectSnafu, ReadParquetSnafu, Result, WriteObjectSnafu,
    WriteParquetSnafu,
};
use crate::memtable::BoxedBatchIterator;
use crate::read::{Batch, BatchReader};
use crate::schema::compat::ReadAdapter;
use crate::schema::{ProjectedSchemaRef, StoreSchema};
use crate::sst;

/// Parquet sst writer.
pub struct ParquetWriter<'a> {
    file_path: &'a str,
    iter: BoxedBatchIterator,
    object_store: ObjectStore,
    max_row_group_size: usize,
}

impl<'a> ParquetWriter<'a> {
    pub fn new(
        file_path: &'a str,
        iter: BoxedBatchIterator,
        object_store: ObjectStore,
    ) -> ParquetWriter {
        ParquetWriter {
            file_path,
            iter,
            object_store,
            max_row_group_size: 4096, // TODO(hl): make this configurable
        }
    }

    pub async fn write_sst(self, _opts: &sst::WriteOptions) -> Result<()> {
        self.write_rows(None).await
    }

    /// Iterates memtable and writes rows to Parquet file.
    /// A chunk of records yielded from each iteration with a size given
    /// in config will be written to a single row group.
    async fn write_rows(self, extra_meta: Option<HashMap<String, String>>) -> Result<()> {
        let projected_schema = self.iter.schema();
        let store_schema = projected_schema.schema_to_read();
        let schema = store_schema.arrow_schema().clone();
        let object = self.object_store.object(self.file_path);

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD)
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(self.max_row_group_size)
            .set_key_value_metadata(extra_meta.map(|map| {
                map.iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            }))
            .build();

        // TODO(hl): Since OpenDAL's writer is async and ArrowWriter requires a `std::io::Write`,
        // here we use a Vec<u8> to buffer all parquet bytes in memory and write to object store
        // at a time. Maybe we should find a better way to brige ArrowWriter and OpenDAL's object.
        let mut buf = vec![];
        let mut arrow_writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(writer_props))
            .context(WriteParquetSnafu)?;
        for batch in self.iter {
            let batch = batch?;
            let arrow_batch = RecordBatch::try_new(
                schema.clone(),
                batch
                    .columns()
                    .iter()
                    .map(|v| v.to_arrow_array())
                    .collect::<Vec<_>>(),
            )
            .context(NewRecordBatchSnafu)?;
            arrow_writer
                .write(&arrow_batch)
                .context(WriteParquetSnafu)?;
        }
        arrow_writer.close().context(WriteParquetSnafu)?;
        object.write(buf).await.context(WriteObjectSnafu {
            path: object.path(),
        })?;
        Ok(())
    }
}

pub struct ParquetReader<'a> {
    file_path: &'a str,
    object_store: ObjectStore,
    projected_schema: ProjectedSchemaRef,
    predicate: Predicate,
}

impl<'a> ParquetReader<'a> {
    pub fn new(
        file_path: &str,
        object_store: ObjectStore,
        projected_schema: ProjectedSchemaRef,
        predicate: Predicate,
    ) -> ParquetReader {
        ParquetReader {
            file_path,
            object_store,
            projected_schema,
            predicate,
        }
    }

    pub async fn chunk_stream(&self) -> Result<ChunkStream> {
        let operator = self.object_store.clone();
        let reader = operator
            .object(self.file_path)
            .reader()
            .await
            .context(ReadObjectSnafu {
                path: self.file_path,
            })?
            .compat();
        let buf_reader = BufReader::new(reader);
        let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
            .await
            .context(ReadParquetSnafu {
                file: self.file_path,
            })?;
        let arrow_schema = builder.schema().clone();

        let store_schema = Arc::new(StoreSchema::try_from(arrow_schema).context(
            error::ConvertStoreSchemaSnafu {
                file: self.file_path,
            },
        )?);

        let adapter = ReadAdapter::new(store_schema.clone(), self.projected_schema.clone())?;

        let pruned_row_groups = self.predicate.prune_row_groups(
            store_schema.schema().clone(),
            builder.metadata().row_groups(),
        );

        let projection = ProjectionMask::roots(
            builder.metadata().file_metadata().schema_descr(),
            adapter.fields_to_read(),
        );

        let mut masked_stream = builder
            .with_projection(projection)
            .build()
            .context(ReadParquetSnafu {
                file: self.file_path,
            })?
            .zip(futures_util::stream::iter(pruned_row_groups.into_iter()));

        let file_name = self.file_path.to_string();
        let chunk_stream = try_stream!({
            while let Some((record_batch, valid)) = masked_stream.next().await {
                if valid {
                    yield record_batch.context(ReadParquetSnafu { file: &file_name })?
                }
            }
        });

        ChunkStream::new(adapter, Box::pin(chunk_stream))
    }
}

pub type SendableChunkStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

pub struct ChunkStream {
    adapter: ReadAdapter,
    stream: SendableChunkStream,
}

impl ChunkStream {
    pub fn new(adapter: ReadAdapter, stream: SendableChunkStream) -> Result<Self> {
        Ok(Self { adapter, stream })
    }
}

#[async_trait]
impl BatchReader for ChunkStream {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.stream
            .try_next()
            .await?
            .map(|rb| self.adapter.arrow_record_batch_to_batch(&rb))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Array, ArrayRef, UInt64Array, UInt8Array};
    use datatypes::prelude::Vector;
    use datatypes::vectors::TimestampMillisecondVector;
    use object_store::backend::fs::Builder;
    use store_api::storage::OpType;
    use tempdir::TempDir;

    use super::*;
    use crate::memtable::{
        tests as memtable_tests, DefaultMemtableBuilder, IterContext, MemtableBuilder,
    };
    use crate::schema::ProjectedSchema;

    #[tokio::test]
    async fn test_parquet_writer() {
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema);

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
            ], // keys
            &[
                (Some(1), Some(1234)),
                (Some(2), Some(1234)),
                (Some(7), Some(1234)),
                (Some(8), Some(1234)),
                (Some(9), Some(1234)),
                (Some(3), Some(1234)),
            ], // values
        );

        let dir = TempDir::new("write_parquet").unwrap();
        let path = dir.path().to_str().unwrap();
        let backend = Builder::default().root(path).build().unwrap();
        let object_store = ObjectStore::new(backend);
        let sst_file_name = "test-flush.parquet";
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, iter, object_store.clone());

        writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();

        // verify parquet file
        let reader = BufReader::new(
            object_store
                .object(sst_file_name)
                .reader()
                .await
                .unwrap()
                .compat(),
        );

        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

        let mut stream = builder.build().unwrap();
        // chunk schema: timestamp, __version, v1, __sequence, __op_type
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(6, chunk.columns().len());

        // timestamp
        assert_eq!(
            &TimestampMillisecondVector::from_slice(&[
                1000.into(),
                1000.into(),
                1001.into(),
                2002.into(),
                2003.into(),
                2003.into()
            ])
            .to_arrow_array(),
            chunk.column(0)
        );

        // version
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1, 2, 1, 1, 1, 5])) as ArrayRef),
            chunk.column(1)
        );

        // v0
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1, 2, 3, 7, 8, 9])) as Arc<dyn Array>),
            chunk.column(2)
        );

        // v1
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1234; 6])) as Arc<dyn Array>),
            chunk.column(3)
        );

        // sequence
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![10; 6])) as Arc<dyn Array>),
            chunk.column(4)
        );

        // op_type
        assert_eq!(
            &(Arc::new(UInt8Array::from(vec![1; 6])) as Arc<dyn Array>),
            chunk.column(5)
        );
    }

    #[tokio::test]
    async fn test_parquet_reader() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
            ], // keys
            &[
                (Some(1), Some(1234)),
                (Some(2), Some(1234)),
                (Some(7), Some(1234)),
                (Some(8), Some(1234)),
                (Some(9), Some(1234)),
                (Some(3), Some(1234)),
            ], // values
        );

        let dir = TempDir::new("write_parquet").unwrap();
        let path = dir.path().to_str().unwrap();
        let backend = Builder::default().root(path).build().unwrap();
        let object_store = ObjectStore::new(backend);
        let sst_file_name = "test-read.parquet";
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, iter, object_store.clone());

        writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();

        let operator = ObjectStore::new(
            object_store::backend::fs::Builder::default()
                .root(dir.path().to_str().unwrap())
                .build()
                .unwrap(),
        );

        let projected_schema = Arc::new(ProjectedSchema::new(schema, Some(vec![1])).unwrap());
        let reader = ParquetReader::new(
            "test-read.parquet",
            operator,
            projected_schema,
            Predicate::empty(),
        );

        let mut stream = reader.chunk_stream().await.unwrap();
        assert_eq!(
            6,
            stream
                .next_batch()
                .await
                .transpose()
                .unwrap()
                .unwrap()
                .num_rows()
        );
    }
}
