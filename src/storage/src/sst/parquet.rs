// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

use async_stream::try_stream;
use async_trait::async_trait;
use common_telemetry::debug;
use datatypes::arrow::array::Array;
use datatypes::arrow::chunk::Chunk;
use datatypes::arrow::datatypes::{DataType, Schema};
use datatypes::arrow::io::parquet::read::{
    infer_schema, read_columns_many_async, read_metadata_async, RowGroupDeserializer,
};
use datatypes::arrow::io::parquet::write::{
    Compression, Encoding, FileSink, Version, WriteOptions,
};
use futures::io::BufReader;
use futures::AsyncWriteExt;
use futures_util::sink::SinkExt;
use futures_util::{try_join, Stream, TryStreamExt};
use object_store::{ObjectStore, SeekableReader};
use sluice::pipe;
use snafu::ResultExt;
use table::predicate::Predicate;

use crate::error::{self, Result};
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
        let schema = store_schema.arrow_schema();
        let object = self.object_store.object(self.file_path);

        let (reader, mut writer) = pipe::pipe();

        // now all physical types use plain encoding, maybe let caller to choose encoding for each type.
        let encodings = get_encoding_for_schema(schema, |_| Encoding::Plain);
        try_join!(
            async {
                // FIXME(hl): writer size is not used in fs backend so just leave it to 0,
                // but in s3/azblob backend the Content-Length field of HTTP request is set
                // to this value.
                object
                    .write_from(0, reader)
                    .await
                    .context(error::FlushIoSnafu)
            },
            async {
                let mut sink = FileSink::try_new(
                    &mut writer,
                    // The file sink needs the `Schema` instead of a reference.
                    (**schema).clone(),
                    encodings,
                    WriteOptions {
                        write_statistics: true,
                        compression: Compression::Gzip,
                        version: Version::V2,
                    },
                )
                .context(error::WriteParquetSnafu)?;

                for batch in self.iter {
                    let batch = batch?;
                    sink.send(store_schema.batch_to_arrow_chunk(&batch))
                        .await
                        .context(error::WriteParquetSnafu)?;
                }

                if let Some(meta) = extra_meta {
                    for (k, v) in meta {
                        sink.metadata.insert(k, Some(v));
                    }
                }
                sink.close().await.context(error::WriteParquetSnafu)?;
                drop(sink);

                writer.close().await.context(error::WriteObjectSnafu {
                    path: self.file_path,
                })
            }
        )
        .map(|_| ())
    }
}

fn get_encoding_for_schema<F: Fn(&DataType) -> Encoding + Clone>(
    schema: &Schema,
    map: F,
) -> Vec<Encoding> {
    schema
        .fields
        .iter()
        .flat_map(|f| transverse(&f.data_type, map.clone()))
        .collect()
}

// TODO(hl): backport from arrow2 v0.12 (https://github.com/jorgecarleitao/arrow2/blob/f57dbd5dbc61b940a71decd5f81d0fd4c93b158d/src/io/parquet/write/mod.rs#L454-L509)
// remove it when upgrade to newer version
pub fn transverse<T, F: Fn(&DataType) -> T + Clone>(data_type: &DataType, map: F) -> Vec<T> {
    let mut encodings = vec![];
    transverse_recursive(data_type, map, &mut encodings);
    encodings
}

fn transverse_recursive<T, F: Fn(&DataType) -> T + Clone>(
    data_type: &DataType,
    map: F,
    encodings: &mut Vec<T>,
) {
    use datatypes::arrow::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | Dictionary(_) | LargeUtf8 => encodings.push(map(data_type)),
        List | FixedSizeList | LargeList => {
            let a = data_type.to_logical_type();
            if let DataType::List(inner) = a {
                transverse_recursive(&inner.data_type, map, encodings)
            } else if let DataType::LargeList(inner) = a {
                transverse_recursive(&inner.data_type, map, encodings)
            } else if let DataType::FixedSizeList(inner, _) = a {
                transverse_recursive(&inner.data_type, map, encodings)
            } else {
                unreachable!()
            }
        }
        Struct => {
            if let DataType::Struct(fields) = data_type.to_logical_type() {
                for field in fields {
                    transverse_recursive(&field.data_type, map.clone(), encodings)
                }
            } else {
                unreachable!()
            }
        }
        Union => todo!(),
        Map => todo!(),
    }
}

pub struct ParquetReader<'a> {
    file_path: &'a str,
    object_store: ObjectStore,
    projected_schema: ProjectedSchemaRef,
    predicate: Predicate,
}

type ReaderFactoryFuture<'a, R> =
    Pin<Box<dyn futures_util::Future<Output = std::io::Result<R>> + Send + 'a>>;

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

    pub async fn chunk_stream(&self, chunk_size: usize) -> Result<ChunkStream> {
        let file_path = self.file_path.to_string();
        let operator = self.object_store.clone();
        let reader_factory = move || -> ReaderFactoryFuture<SeekableReader> {
            let file_path = file_path.clone();
            let operator = operator.clone();
            Box::pin(async move { Ok(operator.object(&file_path).seekable_reader(..)) })
        };

        let file_path = self.file_path.to_string();
        let reader = reader_factory()
            .await
            .context(error::ReadParquetIoSnafu { file: &file_path })?;
        // Use BufReader to alleviate consumption bring by random seek and small IO.
        let mut buf_reader = BufReader::new(reader);
        let metadata = read_metadata_async(&mut buf_reader)
            .await
            .context(error::ReadParquetSnafu { file: &file_path })?;

        let arrow_schema =
            infer_schema(&metadata).context(error::ReadParquetSnafu { file: &file_path })?;
        let store_schema = Arc::new(
            StoreSchema::try_from(arrow_schema)
                .context(error::ConvertStoreSchemaSnafu { file: &file_path })?,
        );

        let adapter = ReadAdapter::new(store_schema.clone(), self.projected_schema.clone())?;

        let pruned_row_groups = self
            .predicate
            .prune_row_groups(store_schema.schema().clone(), &metadata.row_groups);

        let projected_fields = adapter.fields_to_read();
        let chunk_stream = try_stream!({
            for (idx, valid) in pruned_row_groups.iter().enumerate() {
                if !valid {
                    debug!("Pruned {} row groups", idx);
                    continue;
                }

                let rg = &metadata.row_groups[idx];
                let column_chunks = read_columns_many_async(
                    &reader_factory,
                    rg,
                    projected_fields.clone(),
                    Some(chunk_size),
                )
                .await
                .context(error::ReadParquetSnafu { file: &file_path })?;

                let chunks = RowGroupDeserializer::new(column_chunks, rg.num_rows() as usize, None);
                for maybe_chunk in chunks {
                    let columns_in_chunk =
                        maybe_chunk.context(error::ReadParquetSnafu { file: &file_path })?;
                    yield columns_in_chunk;
                }
            }
        });

        ChunkStream::new(adapter, Box::pin(chunk_stream))
    }
}

pub type SendableChunkStream = Pin<Box<dyn Stream<Item = Result<Chunk<Arc<dyn Array>>>> + Send>>;

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
            .map(|chunk| self.adapter.arrow_chunk_to_batch(&chunk))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Array, UInt64Array, UInt8Array};
    use datatypes::arrow::io::parquet::read::FileReader;
    use datatypes::prelude::{ScalarVector, Vector};
    use datatypes::vectors::TimestampVector;
    use object_store::backend::fs::Builder;
    use store_api::storage::OpType;
    use tempdir::TempDir;

    use super::*;
    use crate::memtable::{
        tests as memtable_tests, DefaultMemtableBuilder, IterContext, MemtableBuilder,
    };

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
        let writer = ParquetWriter::new(sst_file_name, iter, object_store);

        writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();

        // verify parquet file

        let reader = std::fs::File::open(dir.path().join(sst_file_name)).unwrap();
        let mut file_reader = FileReader::try_new(reader, None, Some(128), None, None).unwrap();

        // chunk schema: timestamp, __version, v1, __sequence, __op_type
        let chunk = file_reader.next().unwrap().unwrap();
        assert_eq!(6, chunk.arrays().len());

        // timestamp
        assert_eq!(
            TimestampVector::from_slice(&[
                1000.into(),
                1000.into(),
                1001.into(),
                2002.into(),
                2003.into(),
                2003.into()
            ])
            .to_arrow_array(),
            chunk.arrays()[0]
        );

        // version
        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[1, 2, 1, 1, 1, 5])) as Arc<dyn Array>,
            chunk.arrays()[1]
        );

        // v0
        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[1, 2, 3, 7, 8, 9])) as Arc<dyn Array>,
            chunk.arrays()[2]
        );

        // v1
        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[
                1234, 1234, 1234, 1234, 1234, 1234
            ])) as Arc<dyn Array>,
            chunk.arrays()[3]
        );

        // sequence
        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[10, 10, 10, 10, 10, 10])) as Arc<dyn Array>,
            chunk.arrays()[4]
        );

        // op_type
        assert_eq!(
            Arc::new(UInt8Array::from_slice(&[0, 0, 0, 0, 0, 0])) as Arc<dyn Array>,
            chunk.arrays()[5]
        );
    }
}
