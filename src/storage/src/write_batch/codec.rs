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

use std::io::Cursor;
use std::sync::Arc;

use datatypes::arrow::ipc::reader::StreamReader;
use datatypes::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::Helper;
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::WriteRequest;

use crate::codec::{Decoder, Encoder};
use crate::proto::wal::MutationType;
use crate::proto::write_batch::{self, gen_columns, gen_put_data_vector};
use crate::write_batch::{
    DataCorruptedSnafu, DecodeArrowSnafu, DecodeProtobufSnafu, DecodeVectorSnafu, EncodeArrowSnafu,
    EncodeProtobufSnafu, Error as WriteBatchError, FromProtobufSnafu, MissingColumnSnafu, Mutation,
    ParseSchemaSnafu, PutData, Result, ToProtobufSnafu, WriteBatch,
};

// TODO(jiachun): We can make a comparison with protobuf, including performance, storage cost,
// CPU consumption, etc
#[derive(Default)]
pub struct WriteBatchArrowEncoder {}

impl WriteBatchArrowEncoder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Encoder for WriteBatchArrowEncoder {
    type Item = WriteBatch;
    type Error = WriteBatchError;

    fn encode(&self, item: &WriteBatch, dst: &mut Vec<u8>) -> Result<()> {
        let item_schema = item.schema();
        let arrow_schema = item_schema.arrow_schema();

        let opts = IpcWriteOptions::default();
        let mut writer = StreamWriter::try_new_with_options(dst, arrow_schema, opts)
            .context(EncodeArrowSnafu)?;

        for mutation in item.iter() {
            let rb = match mutation {
                Mutation::Put(put) => {
                    let arrays = item_schema
                        .column_schemas()
                        .iter()
                        .map(|column_schema| {
                            let vector = put.column_by_name(&column_schema.name).context(
                                MissingColumnSnafu {
                                    name: &column_schema.name,
                                },
                            )?;
                            Ok(vector.to_arrow_array())
                        })
                        .collect::<Result<Vec<_>>>()?;

                    RecordBatch::try_new(arrow_schema.clone(), arrays).context(EncodeArrowSnafu)?
                }
            };
            writer.write(&rb).context(EncodeArrowSnafu)?;
        }
        writer.finish().context(EncodeArrowSnafu)?;
        Ok(())
    }
}

pub struct WriteBatchArrowDecoder {
    mutation_types: Vec<i32>,
}

impl WriteBatchArrowDecoder {
    pub fn new(mutation_types: Vec<i32>) -> Self {
        Self { mutation_types }
    }
}

impl Decoder for WriteBatchArrowDecoder {
    type Item = WriteBatch;
    type Error = WriteBatchError;

    fn decode(&self, src: &[u8]) -> Result<WriteBatch> {
        let reader = Cursor::new(src);
        let mut reader = StreamReader::try_new(reader, None).context(DecodeArrowSnafu)?;
        let arrow_schema = reader.schema();
        let mut chunks = Vec::with_capacity(self.mutation_types.len());

        for maybe_record_batch in reader.by_ref() {
            let record_batch = maybe_record_batch.context(DecodeArrowSnafu)?;
            chunks.push(record_batch);
        }

        // check if exactly finished
        ensure!(
            reader.is_finished(),
            DataCorruptedSnafu {
                message: "Impossible, the num of data chunks is different than expected."
            }
        );

        ensure!(
            chunks.len() == self.mutation_types.len(),
            DataCorruptedSnafu {
                message: format!(
                    "expected {} mutations, but got {}",
                    self.mutation_types.len(),
                    chunks.len()
                )
            }
        );

        let schema = Arc::new(Schema::try_from(arrow_schema).context(ParseSchemaSnafu)?);
        let mut write_batch = WriteBatch::new(schema.clone());

        for (mutation_type, record_batch) in self.mutation_types.iter().zip(chunks.into_iter()) {
            match MutationType::from_i32(*mutation_type) {
                Some(MutationType::Put) => {
                    let mut put_data = PutData::with_num_columns(schema.num_columns());
                    for (column_schema, array) in schema
                        .column_schemas()
                        .iter()
                        .zip(record_batch.columns().iter())
                    {
                        let vector = Helper::try_into_vector(array).context(DecodeVectorSnafu)?;
                        put_data.add_column_by_name(&column_schema.name, vector)?;
                    }

                    write_batch.put(put_data)?;
                }
                Some(MutationType::Delete) => {
                    unimplemented!("delete mutation is not implemented")
                }
                _ => {
                    return DataCorruptedSnafu {
                        message: format!("Unexpected mutation type: {mutation_type}"),
                    }
                    .fail()
                }
            }
        }
        Ok(write_batch)
    }
}

pub struct WriteBatchProtobufEncoder {}

impl Encoder for WriteBatchProtobufEncoder {
    type Item = WriteBatch;
    type Error = WriteBatchError;

    fn encode(&self, item: &WriteBatch, dst: &mut Vec<u8>) -> Result<()> {
        let schema = item.schema().into();

        let mutations = item
            .iter()
            .map(|mtn| match mtn {
                Mutation::Put(put_data) => item
                    .schema()
                    .column_schemas()
                    .iter()
                    .map(|cs| {
                        let vector = put_data
                            .column_by_name(&cs.name)
                            .context(MissingColumnSnafu { name: &cs.name })?;
                        gen_columns(vector).context(ToProtobufSnafu)
                    })
                    .collect::<Result<Vec<_>>>(),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|columns| write_batch::Mutation {
                mutation: Some(write_batch::mutation::Mutation::Put(write_batch::Put {
                    columns,
                })),
            })
            .collect();

        let write_batch = write_batch::WriteBatch {
            schema: Some(schema),
            mutations,
        };

        write_batch.encode(dst).context(EncodeProtobufSnafu)
    }
}

pub struct WriteBatchProtobufDecoder {
    mutation_types: Vec<i32>,
}

impl WriteBatchProtobufDecoder {
    #[allow(dead_code)]
    pub fn new(mutation_types: Vec<i32>) -> Self {
        Self { mutation_types }
    }
}

impl Decoder for WriteBatchProtobufDecoder {
    type Item = WriteBatch;
    type Error = WriteBatchError;

    fn decode(&self, src: &[u8]) -> Result<WriteBatch> {
        let write_batch = write_batch::WriteBatch::decode(src).context(DecodeProtobufSnafu)?;

        let schema = write_batch.schema.context(DataCorruptedSnafu {
            message: "schema required",
        })?;

        let schema = SchemaRef::try_from(schema).context(FromProtobufSnafu {})?;

        ensure!(
            write_batch.mutations.len() == self.mutation_types.len(),
            DataCorruptedSnafu {
                message: &format!(
                    "expected {} mutations, but got {}",
                    self.mutation_types.len(),
                    write_batch.mutations.len()
                )
            }
        );

        let mutations = write_batch
            .mutations
            .into_iter()
            .map(|mtn| match mtn.mutation {
                Some(write_batch::mutation::Mutation::Put(put)) => {
                    let mut put_data = PutData::with_num_columns(put.columns.len());

                    let res = schema
                        .column_schemas()
                        .iter()
                        .map(|column| (column.name.clone(), column.data_type.clone()))
                        .zip(put.columns.into_iter())
                        .map(|((name, data_type), column)| {
                            gen_put_data_vector(data_type, column)
                                .map(|vector| (name, vector))
                                .context(FromProtobufSnafu)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .map(|(name, vector)| put_data.add_column_by_name(&name, vector))
                        .collect::<Result<Vec<_>>>();

                    res.map(|_| Mutation::Put(put_data))
                }
                Some(write_batch::mutation::Mutation::Delete(_)) => todo!(),
                _ => DataCorruptedSnafu {
                    message: "invalid mutation type",
                }
                .fail(),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut write_batch = WriteBatch::new(schema);

        mutations
            .into_iter()
            .try_for_each(|mutation| match mutation {
                Mutation::Put(put_data) => write_batch.put(put_data),
            })?;

        Ok(write_batch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{BooleanVector, TimestampMillisecondVector, UInt64Vector};
    use store_api::storage::PutOperation;

    use super::*;
    use crate::{proto, write_batch};

    fn gen_new_batch_and_types() -> (WriteBatch, Vec<i32>) {
        let mut batch = write_batch::new_test_batch();
        for i in 0..10 {
            let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));
            let boolv = Arc::new(BooleanVector::from(vec![Some(true), Some(false), None]));
            let tsv = Arc::new(TimestampMillisecondVector::from_vec(vec![i, i, i]));

            let mut put_data = PutData::new();
            put_data.add_key_column("k1", intv.clone()).unwrap();
            put_data.add_version_column(intv).unwrap();
            put_data.add_value_column("v1", boolv).unwrap();
            put_data.add_key_column("ts", tsv).unwrap();

            batch.put(put_data).unwrap();
        }

        let types = proto::wal::gen_mutation_types(&batch);

        (batch, types)
    }

    #[test]
    fn test_codec_arrow() -> Result<()> {
        let (batch, mutation_types) = gen_new_batch_and_types();

        let encoder = WriteBatchArrowEncoder::new();
        let mut dst = vec![];
        let result = encoder.encode(&batch, &mut dst);
        assert!(result.is_ok());

        let decoder = WriteBatchArrowDecoder::new(mutation_types);
        let result = decoder.decode(&dst);
        let batch2 = result?;
        assert_eq!(batch.num_rows, batch2.num_rows);

        Ok(())
    }

    #[test]
    fn test_codec_protobuf() -> Result<()> {
        let (batch, mutation_types) = gen_new_batch_and_types();

        let encoder = WriteBatchProtobufEncoder {};
        let mut dst = vec![];
        let result = encoder.encode(&batch, &mut dst);
        assert!(result.is_ok());

        let decoder = WriteBatchProtobufDecoder::new(mutation_types);
        let result = decoder.decode(&dst);
        let batch2 = result?;
        assert_eq!(batch.num_rows, batch2.num_rows);

        Ok(())
    }

    fn gen_new_batch_and_types_with_none_column() -> (WriteBatch, Vec<i32>) {
        let mut batch = write_batch::new_test_batch();
        for _ in 0..10 {
            let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));
            let tsv = Arc::new(TimestampMillisecondVector::from_vec(vec![0, 0, 0]));

            let mut put_data = PutData::new();
            put_data.add_key_column("k1", intv.clone()).unwrap();
            put_data.add_version_column(intv).unwrap();
            put_data.add_key_column("ts", tsv).unwrap();

            batch.put(put_data).unwrap();
        }

        let types = proto::wal::gen_mutation_types(&batch);

        (batch, types)
    }

    #[test]
    fn test_codec_with_none_column_arrow() -> Result<()> {
        let (batch, mutation_types) = gen_new_batch_and_types_with_none_column();

        let encoder = WriteBatchArrowEncoder::new();
        let mut dst = vec![];
        let result = encoder.encode(&batch, &mut dst);
        assert!(result.is_ok());

        let decoder = WriteBatchArrowDecoder::new(mutation_types);
        let result = decoder.decode(&dst);
        let batch2 = result?;
        assert_eq!(batch.num_rows, batch2.num_rows);

        Ok(())
    }

    #[test]
    fn test_codec_with_none_column_protobuf() -> Result<()> {
        let (batch, mutation_types) = gen_new_batch_and_types_with_none_column();

        let encoder = WriteBatchProtobufEncoder {};
        let mut dst = vec![];
        encoder.encode(&batch, &mut dst).unwrap();

        let decoder = WriteBatchProtobufDecoder::new(mutation_types);
        let result = decoder.decode(&dst);
        let batch2 = result?;
        assert_eq!(batch.num_rows, batch2.num_rows);

        Ok(())
    }
}
