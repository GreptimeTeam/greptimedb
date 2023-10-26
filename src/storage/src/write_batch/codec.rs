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

use std::io::Cursor;
use std::sync::Arc;

use api::v1::OpType;
use common_recordbatch::RecordBatch;
use datatypes::arrow::ipc::reader::StreamReader;
use datatypes::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datatypes::schema::Schema;
use snafu::{ensure, ResultExt};

use crate::codec::{Decoder, Encoder};
use crate::error::{
    BatchCorruptedSnafu, CreateRecordBatchSnafu, DecodeArrowSnafu, EncodeArrowSnafu, Error,
    ParseSchemaSnafu, Result,
};
use crate::proto::wal::MutationType;
use crate::write_batch::{Mutation, Payload};

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
            .context(EncodeArrowSnafu)?;

        for mutation in &item.mutations {
            let record_batch = mutation.record_batch.df_record_batch();
            writer.write(record_batch).context(EncodeArrowSnafu)?;
        }
        writer.finish().context(EncodeArrowSnafu)?;

        Ok(())
    }
}

pub struct PayloadDecoder<'a> {
    mutation_types: &'a [i32],
}

impl<'a> PayloadDecoder<'a> {
    pub fn new(mutation_types: &'a [i32]) -> Self {
        Self { mutation_types }
    }
}

impl<'a> Decoder for PayloadDecoder<'a> {
    type Item = Payload;
    type Error = Error;

    fn decode(&self, src: &[u8]) -> Result<Payload> {
        let reader = Cursor::new(src);
        let mut reader = StreamReader::try_new(reader, None).context(DecodeArrowSnafu)?;
        let arrow_schema = reader.schema();

        // We could let the decoder takes a schema as input if possible, then we don't
        // need to rebuild the schema here.
        let schema = Arc::new(Schema::try_from(arrow_schema).context(ParseSchemaSnafu)?);
        let mut mutations = Vec::with_capacity(self.mutation_types.len());

        for (record_batch, mutation_type) in reader.by_ref().zip(self.mutation_types) {
            let record_batch = record_batch.context(DecodeArrowSnafu)?;
            let record_batch = RecordBatch::try_from_df_record_batch(schema.clone(), record_batch)
                .context(CreateRecordBatchSnafu)?;
            let op_type = match MutationType::try_from(*mutation_type) {
                Ok(MutationType::Delete) => OpType::Delete,
                Ok(MutationType::Put) => OpType::Put,
                Err(e) => {
                    return BatchCorruptedSnafu {
                        message: format!("Unexpceted decode error for mutation type: {e}"),
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
            BatchCorruptedSnafu {
                message: "The num of data chunks is different than expected."
            }
        );

        ensure!(
            mutations.len() == self.mutation_types.len(),
            BatchCorruptedSnafu {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::vectors::{BooleanVector, TimestampMillisecondVector, UInt64Vector, VectorRef};
    use store_api::storage::WriteRequest;

    use super::*;
    use crate::write_batch::WriteBatch;
    use crate::{proto, write_batch};

    fn gen_new_batch_and_types() -> (WriteBatch, Vec<i32>) {
        let mut batch = write_batch::new_test_batch();
        for i in 0..10 {
            let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
            let boolv =
                Arc::new(BooleanVector::from(vec![Some(true), Some(false), None])) as VectorRef;
            let tsv = Arc::new(TimestampMillisecondVector::from_vec(vec![i, i, i])) as VectorRef;

            let put_data = HashMap::from([
                ("k1".to_string(), intv),
                ("v1".to_string(), boolv),
                ("ts".to_string(), tsv),
            ]);

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
        encoder.encode(batch.payload(), &mut dst).unwrap();

        let decoder = PayloadDecoder::new(&mutation_types);
        let result = decoder.decode(&dst);
        let payload = result?;
        assert_eq!(*batch.payload(), payload);

        Ok(())
    }

    fn gen_new_batch_and_types_with_none_column() -> (WriteBatch, Vec<i32>) {
        let mut batch = write_batch::new_test_batch();
        for _ in 0..10 {
            let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
            let tsv = Arc::new(TimestampMillisecondVector::from_vec(vec![0, 0, 0])) as VectorRef;

            let put_data =
                HashMap::from([("k1".to_string(), intv.clone()), ("ts".to_string(), tsv)]);

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
        encoder.encode(batch.payload(), &mut dst).unwrap();

        let decoder = PayloadDecoder::new(&mutation_types);
        let result = decoder.decode(&dst);
        let payload = result?;
        assert_eq!(*batch.payload(), payload);

        Ok(())
    }
}
