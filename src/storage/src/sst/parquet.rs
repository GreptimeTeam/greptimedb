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
use futures::io::BufReader;
use futures_util::{Stream, TryStreamExt};
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

        todo!()
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
        // let file_path = self.file_path.to_string();
        // let operator = self.object_store.clone();
        // let reader_factory = move || -> ReaderFactoryFuture<SeekableReader> {
        //     let file_path = file_path.clone();
        //     let operator = operator.clone();
        //     Box::pin(async move { Ok(operator.object(&file_path).seekable_reader(..)) })
        // };
        //
        // let file_path = self.file_path.to_string();
        // let reader = reader_factory()
        //     .await
        //     .context(error::ReadParquetIoSnafu { file: &file_path })?;
        // // Use BufReader to alleviate consumption bring by random seek and small IO.
        // let mut buf_reader = BufReader::new(reader);
        // let metadata = read_metadata_async(&mut buf_reader)
        //     .await
        //     .context(error::ReadParquetSnafu { file: &file_path })?;
        //
        // let arrow_schema =
        //     infer_schema(&metadata).context(error::ReadParquetSnafu { file: &file_path })?;
        // let store_schema = Arc::new(
        //     StoreSchema::try_from(arrow_schema)
        //         .context(error::ConvertStoreSchemaSnafu { file: &file_path })?,
        // );
        //
        // let adapter = ReadAdapter::new(store_schema.clone(), self.projected_schema.clone())?;
        //
        // let pruned_row_groups = self
        //     .predicate
        //     .prune_row_groups(store_schema.schema().clone(), &metadata.row_groups);
        //
        // let projected_fields = adapter.fields_to_read();
        // let chunk_stream = try_stream!({
        //     for (idx, valid) in pruned_row_groups.iter().enumerate() {
        //         if !valid {
        //             debug!("Pruned {} row groups", idx);
        //             continue;
        //         }
        //
        //         let rg = &metadata.row_groups[idx];
        //         let column_chunks = read_columns_many_async(
        //             &reader_factory,
        //             rg,
        //             projected_fields.clone(),
        //             Some(chunk_size),
        //         )
        //         .await
        //         .context(error::ReadParquetSnafu { file: &file_path })?;
        //
        //         let chunks = RowGroupDeserializer::new(column_chunks, rg.num_rows() as usize, None);
        //         for maybe_chunk in chunks {
        //             let columns_in_chunk =
        //                 maybe_chunk.context(error::ReadParquetSnafu { file: &file_path })?;
        //             yield columns_in_chunk;
        //         }
        //     }
        // });
        //
        // ChunkStream::new(adapter, Box::pin(chunk_stream))
        todo!()
    }
}

pub type SendableChunkStream = Pin<Box<dyn Stream<Item = Result<Batch>> + Send>>;

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
        self.stream.try_next().await
    }
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use datatypes::arrow::array::{Array, UInt64Array, UInt8Array};
//     use datatypes::arrow::io::parquet::read::FileReader;
//     use datatypes::prelude::{ScalarVector, Vector};
//     use datatypes::vectors::TimestampVector;
//     use object_store::backend::fs::Builder;
//     use store_api::storage::OpType;
//     use tempdir::TempDir;
//
//     use super::*;
//     use crate::memtable::{
//         tests as memtable_tests, DefaultMemtableBuilder, IterContext, MemtableBuilder,
//     };
//
//     #[tokio::test]
//     async fn test_parquet_writer() {
//         let schema = memtable_tests::schema_for_test();
//         let memtable = DefaultMemtableBuilder::default().build(schema);
//
//         memtable_tests::write_kvs(
//             &*memtable,
//             10, // sequence
//             OpType::Put,
//             &[
//                 (1000, 1),
//                 (1000, 2),
//                 (2002, 1),
//                 (2003, 1),
//                 (2003, 5),
//                 (1001, 1),
//             ], // keys
//             &[
//                 (Some(1), Some(1234)),
//                 (Some(2), Some(1234)),
//                 (Some(7), Some(1234)),
//                 (Some(8), Some(1234)),
//                 (Some(9), Some(1234)),
//                 (Some(3), Some(1234)),
//             ], // values
//         );
//
//         let dir = TempDir::new("write_parquet").unwrap();
//         let path = dir.path().to_str().unwrap();
//         let backend = Builder::default().root(path).build().unwrap();
//         let object_store = ObjectStore::new(backend);
//         let sst_file_name = "test-flush.parquet";
//         let iter = memtable.iter(&IterContext::default()).unwrap();
//         let writer = ParquetWriter::new(sst_file_name, iter, object_store);
//
//         writer
//             .write_sst(&sst::WriteOptions::default())
//             .await
//             .unwrap();
//
//         // verify parquet file
//
//         let reader = std::fs::File::open(dir.path().join(sst_file_name)).unwrap();
//         let mut file_reader = FileReader::try_new(reader, None, Some(128), None, None).unwrap();
//
//         // chunk schema: timestamp, __version, v1, __sequence, __op_type
//         let chunk = file_reader.next().unwrap().unwrap();
//         assert_eq!(6, chunk.arrays().len());
//
//         // timestamp
//         assert_eq!(
//             TimestampVector::from_slice(&[
//                 1000.into(),
//                 1000.into(),
//                 1001.into(),
//                 2002.into(),
//                 2003.into(),
//                 2003.into()
//             ])
//             .to_arrow_array(),
//             chunk.arrays()[0]
//         );
//
//         // version
//         assert_eq!(
//             Arc::new(UInt64Array::from_slice(&[1, 2, 1, 1, 1, 5])) as Arc<dyn Array>,
//             chunk.arrays()[1]
//         );
//
//         // v0
//         assert_eq!(
//             Arc::new(UInt64Array::from_slice(&[1, 2, 3, 7, 8, 9])) as Arc<dyn Array>,
//             chunk.arrays()[2]
//         );
//
//         // v1
//         assert_eq!(
//             Arc::new(UInt64Array::from_slice(&[
//                 1234, 1234, 1234, 1234, 1234, 1234
//             ])) as Arc<dyn Array>,
//             chunk.arrays()[3]
//         );
//
//         // sequence
//         assert_eq!(
//             Arc::new(UInt64Array::from_slice(&[10, 10, 10, 10, 10, 10])) as Arc<dyn Array>,
//             chunk.arrays()[4]
//         );
//
//         // op_type
//         assert_eq!(
//             Arc::new(UInt8Array::from_slice(&[0, 0, 0, 0, 0, 0])) as Arc<dyn Array>,
//             chunk.arrays()[5]
//         );
//     }
// }
