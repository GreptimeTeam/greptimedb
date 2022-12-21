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

use common_recordbatch::RecordBatch;
use datatypes::arrow::ipc::reader::StreamReader;
use datatypes::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::Helper;
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{OpType, WriteRequest};

use crate::codec::{Decoder, Encoder};
use crate::error::{self, Error, Result};
use crate::proto::wal::MutationType;
use crate::proto::write_batch::{self, gen_columns, gen_put_data_vector};
use crate::schema::StoreSchema;
use crate::write_batch::{Mutation, Payload, WriteBatch};

#[derive(Default)]
pub struct PayloadEncoder {}

impl PayloadEncoder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Encoder for PayloadEncoder {
    type Item = Payload;
    type Error = Error;

    fn encode(&self, item: &Payload, dst: &mut Vec<u8>) -> Result<()> {
        let arrow_schema = item.schema.arrow_schema();

        let opts = IpcWriteOptions::default();
        let mut writer = StreamWriter::try_new_with_options(dst, arrow_schema, opts)
            .context(error::EncodeArrowSnafu)?;

        for mutation in &item.mutations {
            let rb = mutation.record_batch.df_record_batch();
            writer.write(rb).context(error::EncodeArrowSnafu)?;
        }
        writer.finish().context(error::EncodeArrowSnafu)?;

        Ok(())
    }
}

pub struct PayloadDecoder {
    mutation_types: Vec<i32>,
}

impl PayloadDecoder {
    pub fn new(mutation_types: Vec<i32>) -> Self {
        Self { mutation_types }
    }
}

impl Decoder for PayloadDecoder {
    type Item = Payload;
    type Error = Error;

    fn decode(&self, src: &[u8]) -> Result<Payload> {
        let reader = Cursor::new(src);
        let mut reader = StreamReader::try_new(reader, None).context(error::DecodeArrowSnafu)?;
        let arrow_schema = reader.schema();

        // We could let the decoder takes a schema as input if possible, then we don't
        // need to rebuild the schema here.
        let schema = Arc::new(Schema::try_from(arrow_schema).context(error::ParseSchemaSnafu)?);
        let mut mutations = Vec::with_capacity(self.mutation_types.len());

        for (record_batch, mutation_type) in reader.by_ref().zip(self.mutation_types.iter()) {
            let record_batch = record_batch.context(error::DecodeArrowSnafu)?;
            let record_batch = RecordBatch::try_from_df_record_batch(schema.clone(), record_batch)
                .context(error::CreateRecordBatchSnafu)?;
            let op_type = match MutationType::from_i32(*mutation_type) {
                Some(MutationType::Put) => OpType::Put,
                Some(MutationType::Delete) => {
                    unimplemented!("delete mutation is not implemented")
                }
                None => {
                    return error::BatchCorruptedSnafu {
                        message: format!("Unexpceted mutation type: {}", mutation_type),
                    }
                    .fail()
                }
            };
            mutations.push(Mutation {
                op_type,
                record_batch,
            });
        }

        // check if exactly finished
        ensure!(
            reader.is_finished(),
            error::BatchCorruptedSnafu {
                message: "Impossible, the num of data chunks is different than expected."
            }
        );

        ensure!(
            mutations.len() == self.mutation_types.len(),
            error::BatchCorruptedSnafu {
                message: format!(
                    "expected {} mutations, but got {}",
                    self.mutation_types.len(),
                    mutations.len()
                )
            }
        );

        Ok(Payload { schema, mutations })
    }
}

// TODO(yingwen): Remove protobuf encoder/decoder.
// pub struct WriteBatchProtobufEncoder {}

// impl Encoder for WriteBatchProtobufEncoder {
//     type Item = WriteBatch;
//     type Error = WriteBatchError;

//     fn encode(&self, item: &WriteBatch, dst: &mut Vec<u8>) -> Result<()> {
//         let schema = item.schema().into();

//         let mutations = item
//             .iter()
//             .map(|mtn| match mtn {
//                 Mutation::Put(put_data) => item
//                     .schema()
//                     .column_schemas()
//                     .iter()
//                     .map(|cs| {
//                         let vector = put_data
//                             .column_by_name(&cs.name)
//                             .context(MissingColumnSnafu { name: &cs.name })?;
//                         gen_columns(vector).context(ToProtobufSnafu)
//                     })
//                     .collect::<Result<Vec<_>>>(),
//             })
//             .collect::<Result<Vec<_>>>()?
//             .into_iter()
//             .map(|columns| write_batch::Mutation {
//                 mutation: Some(write_batch::mutation::Mutation::Put(write_batch::Put {
//                     columns,
//                 })),
//             })
//             .collect();

//         let write_batch = write_batch::WriteBatch {
//             schema: Some(schema),
//             mutations,
//         };

//         write_batch.encode(dst).context(EncodeProtobufSnafu)
//     }
// }

// pub struct WriteBatchProtobufDecoder {
//     mutation_types: Vec<i32>,
// }

// impl WriteBatchProtobufDecoder {
//     #[allow(dead_code)]
//     pub fn new(mutation_types: Vec<i32>) -> Self {
//         Self { mutation_types }
//     }
// }

// impl Decoder for WriteBatchProtobufDecoder {
//     type Item = WriteBatch;
//     type Error = WriteBatchError;

//     fn decode(&self, src: &[u8]) -> Result<WriteBatch> {
//         let write_batch = write_batch::WriteBatch::decode(src).context(DecodeProtobufSnafu)?;

//         let schema = write_batch.schema.context(DataCorruptedSnafu {
//             message: "schema required",
//         })?;

//         let schema = SchemaRef::try_from(schema).context(FromProtobufSnafu {})?;

//         ensure!(
//             write_batch.mutations.len() == self.mutation_types.len(),
//             DataCorruptedSnafu {
//                 message: &format!(
//                     "expected {} mutations, but got {}",
//                     self.mutation_types.len(),
//                     write_batch.mutations.len()
//                 )
//             }
//         );

//         let mutations = write_batch
//             .mutations
//             .into_iter()
//             .map(|mtn| match mtn.mutation {
//                 Some(write_batch::mutation::Mutation::Put(put)) => {
//                     let mut put_data = PutData::with_num_columns(put.columns.len());

//                     let res = schema
//                         .column_schemas()
//                         .iter()
//                         .map(|column| (column.name.clone(), column.data_type.clone()))
//                         .zip(put.columns.into_iter())
//                         .map(|((name, data_type), column)| {
//                             gen_put_data_vector(data_type, column)
//                                 .map(|vector| (name, vector))
//                                 .context(FromProtobufSnafu)
//                         })
//                         .collect::<Result<Vec<_>>>()?
//                         .into_iter()
//                         .map(|(name, vector)| put_data.add_column_by_name(&name, vector))
//                         .collect::<Result<Vec<_>>>();

//                     res.map(|_| Mutation::Put(put_data))
//                 }
//                 Some(write_batch::mutation::Mutation::Delete(_)) => todo!(),
//                 _ => DataCorruptedSnafu {
//                     message: "invalid mutation type",
//                 }
//                 .fail(),
//             })
//             .collect::<Result<Vec<_>>>()?;

//         let mut write_batch = WriteBatch::new(schema);

//         mutations
//             .into_iter()
//             .try_for_each(|mutation| match mutation {
//                 Mutation::Put(put_data) => write_batch.put(put_data),
//             })?;

//         Ok(write_batch)
//     }
// }

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::vectors::{BooleanVector, TimestampMillisecondVector, UInt64Vector, VectorRef};
    use store_api::storage::consts;

    use super::*;
    use crate::{proto, write_batch};

    fn gen_new_batch_and_types() -> (WriteBatch, Vec<i32>) {
        let mut batch = write_batch::new_test_batch();
        for i in 0..10 {
            let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3])) as VectorRef;
            let boolv =
                Arc::new(BooleanVector::from(vec![Some(true), Some(false), None])) as VectorRef;
            let tsv = Arc::new(TimestampMillisecondVector::from_vec(vec![i, i, i])) as VectorRef;

            let mut put_data = HashMap::new();
            put_data.insert("k1".to_string(), intv.clone());
            put_data.insert(consts::VERSION_COLUMN_NAME.to_string(), intv);
            put_data.insert("v1".to_string(), boolv);
            put_data.insert("ts".to_string(), tsv);

            batch.put(put_data).unwrap();
        }

        let types = proto::wal::gen_mutation_types(batch.payload());

        (batch, types)
    }

    #[test]
    fn test_codec_arrow() -> Result<()> {
        let (batch, mutation_types) = gen_new_batch_and_types();

        let encoder = PayloadEncoder::new();
        let mut dst = vec![];
        let result = encoder.encode(batch.payload(), &mut dst);
        assert!(result.is_ok());

        let decoder = PayloadDecoder::new(mutation_types);
        let result = decoder.decode(&dst);
        let payload = result?;
        assert_eq!(*batch.payload(), payload);

        Ok(())
    }

    fn gen_new_batch_and_types_with_none_column() -> (WriteBatch, Vec<i32>) {
        let mut batch = write_batch::new_test_batch();
        for _ in 0..10 {
            let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3])) as VectorRef;
            let tsv = Arc::new(TimestampMillisecondVector::from_vec(vec![0, 0, 0])) as VectorRef;

            let mut put_data = HashMap::with_capacity(3);
            put_data.insert("k1".to_string(), intv.clone());
            put_data.insert(consts::VERSION_COLUMN_NAME.to_string(), intv);
            put_data.insert("ts".to_string(), tsv);

            batch.put(put_data).unwrap();
        }

        let types = proto::wal::gen_mutation_types(batch.payload());

        (batch, types)
    }

    #[test]
    fn test_codec_with_none_column_arrow() -> Result<()> {
        let (batch, mutation_types) = gen_new_batch_and_types_with_none_column();

        let encoder = PayloadEncoder::new();
        let mut dst = vec![];
        let result = encoder.encode(batch.payload(), &mut dst);
        assert!(result.is_ok());

        let decoder = PayloadDecoder::new(mutation_types);
        let result = decoder.decode(&dst);
        let payload = result?;
        assert_eq!(*batch.payload(), payload);

        Ok(())
    }
}
