use datatypes::arrow::chunk::Chunk;
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::io::parquet::write::{
    Compression, Encoding, FileSink, Version, WriteOptions,
};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Vector;
use datatypes::schema::ColumnSchema;
use futures_util::sink::SinkExt;
use object_store::{backend::fs, ObjectStore};
use snafu::ResultExt;
use store_api::storage::consts::{SEQUENCE_COLUMN_NAME, VALUE_TYPE_COLUMN_NAME};
use store_api::storage::SequenceNumber;

use crate::error::{ArrowSnafu, FlushIoSnafu, Result};
use crate::memtable::{IterContext, MemtableRef, MemtableSchema};
use crate::metadata::ColumnMetadata;

pub enum Backend {
    Fs { dir: String },
}

pub struct FlushConfig {
    pub backend: Backend,
    pub row_group_size: usize,
}

pub struct FlushTask {
    config: FlushConfig,
    object_store: ObjectStore,
}

impl FlushTask {
    pub async fn try_new(config: FlushConfig) -> Result<Self> {
        let operator = match config.backend {
            Backend::Fs { ref dir } => {
                let accessor = fs::Backend::build().root(dir).finish().await.unwrap();
                ObjectStore::new(accessor)
            }
        };
        Ok(Self {
            config,
            object_store: operator,
        })
    }

    pub async fn write_rows(&self, mt: &MemtableRef, object_name: &str) -> Result<()> {
        let schema = mt.schema();

        let schema = memtable_schema_to_arrow_schema(schema);
        let object = self.object_store.object(object_name);

        // FIXME(hl): writer size is not used in fs backend so just leave it to 0,
        // but in s3/azblob backend the Content-Length field of HTTP request is set
        // to this value.
        let writer = object.writer(0).await.context(FlushIoSnafu)?;

        let encodings = get_encoding_for_schema(&schema, |_| Encoding::Plain);
        let mut streamer = FileSink::try_new(
            writer,
            schema,
            encodings,
            WriteOptions {
                write_statistics: true,
                compression: Compression::Gzip,
                version: Version::V2,
            },
        )
        .context(ArrowSnafu)?;

        let iter_ctx = IterContext {
            batch_size: 128,
            visible_sequence: SequenceNumber::MAX,
        };

        let mut iter = mt.iter(iter_ctx).unwrap();

        while let Some(batch) = iter.next().unwrap() {
            streamer
                .send(Chunk::new(
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
                .context(ArrowSnafu)?;
        }

        streamer.close().await.context(ArrowSnafu)
    }
}

/// Assembles arrow schema from memtable schema info.
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
            SEQUENCE_COLUMN_NAME,
            ConcreteDataType::uint64_datatype(),
            false,
        ))))
        .chain(std::iter::once(Field::from(&ColumnSchema::new(
            SEQUENCE_COLUMN_NAME,
            ConcreteDataType::uint64_datatype(),
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
