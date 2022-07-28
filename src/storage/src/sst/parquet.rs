//! Parquet sst format.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datatypes::arrow::array::Array;
use datatypes::arrow::chunk::Chunk;
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::io::parquet::read::{
    infer_schema, read_columns_many_async, read_metadata_async, RowGroupDeserializer,
};
use datatypes::arrow::io::parquet::write::{
    Compression, Encoding, FileSink, Version, WriteOptions,
};
use datatypes::prelude::{ConcreteDataType, Vector};
use datatypes::schema::ColumnSchema;
use datatypes::vectors::{Helper, UInt64Vector, UInt8Vector};
use futures_util::sink::SinkExt;
use futures_util::{Stream, TryStreamExt};
use object_store::{ObjectStore, SeekableReader};
use snafu::{OptionExt, ResultExt};
use store_api::storage::consts;

use crate::error::{
    FlushIoSnafu, InvalidParquetSchemaSnafu, ReadParquetIoSnafu, ReadParquetSnafu, Result,
    SequenceColumnNotFoundSnafu, WriteParquetSnafu,
};
use crate::memtable::{BoxedBatchIterator, MemtableSchema};
use crate::metadata::ColumnMetadata;
use crate::read::{Batch, BatchReader};
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
        let schema = memtable_schema_to_arrow_schema(self.iter.schema());
        let object = self.object_store.object(self.file_path);

        // FIXME(hl): writer size is not used in fs backend so just leave it to 0,
        // but in s3/azblob backend the Content-Length field of HTTP request is set
        // to this value.
        let writer = object.writer(0).await.context(FlushIoSnafu)?;

        // now all physical types use plain encoding, maybe let caller to choose encoding for each type.
        let encodings = get_encoding_for_schema(&schema, |_| Encoding::Plain);

        let mut sink = FileSink::try_new(
            writer,
            schema,
            encodings,
            WriteOptions {
                write_statistics: true,
                compression: Compression::Gzip,
                version: Version::V2,
            },
        )
        .context(WriteParquetSnafu)?;

        for batch in self.iter {
            let batch = batch?;
            sink.send(Chunk::new(
                batch
                    .keys
                    .iter()
                    .map(|v| v.to_arrow_array())
                    .chain(std::iter::once(batch.sequences.to_arrow_array()))
                    .chain(std::iter::once(batch.value_types.to_arrow_array()))
                    .chain(batch.values.iter().map(|v| v.to_arrow_array()))
                    .collect(),
            ))
            .await
            .context(WriteParquetSnafu)?;
        }

        if let Some(meta) = extra_meta {
            for (k, v) in meta {
                sink.metadata.insert(k, Some(v));
            }
        }
        sink.close().await.context(WriteParquetSnafu)?;
        // FIXME(yingwen): Hack to workaround an [arrow2 BUG](https://github.com/jorgecarleitao/parquet2/issues/162),
        // upgrading to latest arrow2 can fixed this, but now datafusion is still using an old arrow2 version.
        sink.flush().await.context(WriteParquetSnafu)
    }
}

/// Assembles arrow schema from memtable schema info.
/// TODO(hl): implement `From<Schema>` for `MemtableSchema`/`Physical schema`
fn memtable_schema_to_arrow_schema(schema: &MemtableSchema) -> Schema {
    let col_meta_to_field: fn(&ColumnMetadata) -> Field = |col_meta| {
        Field::from(&ColumnSchema::new(
            col_meta.desc.name.clone(),
            col_meta.desc.data_type.clone(),
            col_meta.desc.is_nullable,
        ))
    };

    let fields = schema
        .row_key_columns()
        .map(col_meta_to_field)
        .chain(std::iter::once(Field::from(&ColumnSchema::new(
            consts::SEQUENCE_COLUMN_NAME,
            ConcreteDataType::uint64_datatype(),
            false,
        ))))
        .chain(std::iter::once(Field::from(&ColumnSchema::new(
            consts::VALUE_TYPE_COLUMN_NAME,
            ConcreteDataType::uint8_datatype(),
            false,
        ))))
        .chain(schema.value_columns().map(col_meta_to_field))
        .collect::<Vec<_>>();
    Schema::from(fields)
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
}

type ReaderFactoryFuture<'a, R> =
    Pin<Box<dyn futures_util::Future<Output = std::io::Result<R>> + Send + 'a>>;

pub type FieldProjection = Box<dyn Fn(&Schema) -> Vec<Field> + Send + Sync>;

impl<'a> ParquetReader<'a> {
    pub fn new(file_path: &str, object_store: ObjectStore) -> ParquetReader {
        ParquetReader {
            file_path,
            object_store,
        }
    }

    pub async fn chunk_stream(
        &self,
        projection: Option<FieldProjection>,
        chunk_size: usize,
    ) -> Result<ChunkStream> {
        let file_path = self.file_path.to_string();
        let operator = self.object_store.clone();
        let reader_factory = move || -> ReaderFactoryFuture<SeekableReader> {
            let file_path = file_path.clone();
            let operator = operator.clone();
            Box::pin(async move { Ok(operator.object(&file_path).seekable_reader(..)) })
        };

        let file_path = self.file_path.to_string();
        let mut reader = reader_factory()
            .await
            .context(ReadParquetIoSnafu { file: &file_path })?;
        let metadata = read_metadata_async(&mut reader)
            .await
            .context(ReadParquetSnafu { file: &file_path })?;
        let schema = infer_schema(&metadata).context(ReadParquetSnafu { file: &file_path })?;

        let projected_fields = projection.map_or_else(|| schema.fields.clone(), |p| p(&schema));
        let projected_schema = Schema {
            fields: projected_fields.clone(),
            metadata: schema.metadata,
        };

        let chunk_stream = try_stream!({
            for rg in metadata.row_groups {
                let column_chunks = read_columns_many_async(
                    &reader_factory,
                    &rg,
                    projected_fields.clone(),
                    Some(chunk_size),
                )
                .await
                .context(ReadParquetSnafu { file: &file_path })?;

                let chunks = RowGroupDeserializer::new(column_chunks, rg.num_rows() as usize, None);
                for maybe_chunk in chunks {
                    let columns_in_chunk =
                        maybe_chunk.context(ReadParquetSnafu { file: &file_path })?;
                    yield columns_in_chunk;
                }
            }
        });

        ChunkStream::new(projected_schema, Box::pin(chunk_stream))
    }
}

type ChunkConverter = Box<dyn Fn(Chunk<Arc<dyn Array>>) -> Result<Batch> + Send + Sync>;

pub type SendableChunkStream = Pin<Box<dyn Stream<Item = Result<Chunk<Arc<dyn Array>>>> + Send>>;

pub struct ChunkStream {
    stream: SendableChunkStream,
    converter: ChunkConverter,
}

impl ChunkStream {
    pub fn new(schema: Schema, stream: SendableChunkStream) -> Result<Self> {
        Ok(Self {
            converter: batch_converter_factory(schema)?,
            stream,
        })
    }
}

#[async_trait]
impl BatchReader for ChunkStream {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.stream
            .try_next()
            .await?
            .map(&self.converter)
            .transpose()
    }
}

// TODO(hl): Currently rowkey/values/reserved columns are identified by their position in field:
// all fields before `__sequence` column are rowkeys and fields after `__value_type` are values.
// But it would be better to persist rowkey/value columns' positions to Parquet metadata and
// parse `MemtableSchema` from metadata while building BatchReader.
fn batch_converter_factory(schema: Schema) -> Result<ChunkConverter> {
    let ts_idx = schema
        .fields
        .iter()
        .position(|f| f.name == consts::SEQUENCE_COLUMN_NAME)
        .with_context(|| SequenceColumnNotFoundSnafu {
            msg: format!("Schema: {:?}", schema),
        })?;

    let field_len = schema.fields.len();

    macro_rules! handle_err {
        ($stmt: expr, $schema: ident) => {
            $stmt.with_context(|_| InvalidParquetSchemaSnafu {
                msg: format!("Schema type error: {:?}", $schema),
            })?
        };
    }

    let converter = move |c: Chunk<Arc<dyn Array>>| {
        Ok(Batch {
            sequences: handle_err!(
                UInt64Vector::try_from_arrow_array(&c.arrays()[ts_idx].clone()),
                schema
            ),
            value_types: handle_err!(
                UInt8Vector::try_from_arrow_array(&c.arrays()[ts_idx + 1].clone()),
                schema
            ),
            keys: handle_err!(
                (0..ts_idx)
                    .into_iter()
                    .map(|i| Helper::try_into_vector(&c.arrays()[i].clone()))
                    .collect::<std::result::Result<_, _>>(),
                schema
            ),
            values: handle_err!(
                (ts_idx + 2..field_len)
                    .into_iter()
                    .map(|i| Helper::try_into_vector(&c.arrays()[i].clone()))
                    .collect::<std::result::Result<_, _>>(),
                schema
            ),
        })
    };
    Ok(Box::new(converter))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Array, Int64Array, UInt64Array, UInt8Array};
    use datatypes::arrow::io::parquet::read::FileReader;
    use object_store::backend::fs::Backend;
    use store_api::storage::ValueType;
    use tempdir::TempDir;

    use super::*;
    use crate::memtable::tests as memtable_tests;
    use crate::memtable::{DefaultMemtableBuilder, IterContext, MemtableBuilder};

    #[tokio::test]
    async fn test_parquet_writer() {
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder {}.build(1, schema);

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            ValueType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
            ], // keys
            &[Some(1), Some(2), Some(7), Some(8), Some(9), Some(3)], // values
        );

        let dir = TempDir::new("write_parquet").unwrap();
        let path = dir.path().to_str().unwrap();
        let backend = Backend::build().root(path).finish().await.unwrap();
        let object_store = ObjectStore::new(backend);
        let sst_file_name = "test-flush.parquet";
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, iter, object_store);

        writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();

        // verify parquet file

        let reader = std::fs::File::open(dir.path().join(sst_file_name)).unwrap();
        let mut file_reader = FileReader::try_new(reader, None, Some(128), None, None).unwrap();

        // chunk schema: timestamp, __version, __sequence, __value_type, v1
        let chunk = file_reader.next().unwrap().unwrap();
        assert_eq!(5, chunk.arrays().len());

        assert_eq!(
            Arc::new(Int64Array::from_slice(&[
                1000, 1000, 1001, 2002, 2003, 2003
            ])) as Arc<dyn Array>,
            chunk.arrays()[0]
        );

        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[1, 2, 1, 1, 1, 5])) as Arc<dyn Array>,
            chunk.arrays()[1]
        );

        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[10, 10, 10, 10, 10, 10])) as Arc<dyn Array>,
            chunk.arrays()[2]
        );

        assert_eq!(
            Arc::new(UInt8Array::from_slice(&[0, 0, 0, 0, 0, 0])) as Arc<dyn Array>,
            chunk.arrays()[3]
        );

        assert_eq!(
            Arc::new(UInt64Array::from_slice(&[1, 2, 3, 7, 8, 9])) as Arc<dyn Array>,
            chunk.arrays()[4]
        );
    }
}
