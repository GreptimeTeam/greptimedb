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

//! Synchronous in-memory blocks using the narthex samples column codecs.

use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, Int64Array, UInt8Array, UInt64Array,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use super::{MAX_ROWS, codec, invalid, require};
use crate::error::{ComputeArrowSnafu, NewRecordBatchSnafu, Result};

#[derive(Debug, Clone, Default)]
pub(crate) struct NarthexBulkPart {
    blocks: Vec<NarthexBulkBlock>,
}

#[derive(Debug, Clone)]
struct NarthexBulkBlock {
    key: Bytes,
    rows: usize,
    columns: Vec<NarthexBulkColumn>,
}

#[derive(Debug, Clone)]
enum NarthexBulkColumn {
    Key,
    Numbers { encoding: u16, bytes: Bytes },
    Float { nulls: u32, bytes: Bytes },
    Operations { encoding: u16, bytes: Bytes },
}

impl NarthexBulkPart {
    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let key = batch
            .column_by_name(PRIMARY_KEY_COLUMN_NAME)
            .ok_or_else(|| invalid("narthex bulk part requires exact primary keys"))?;
        let keys = cast(key, &DataType::Binary).context(ComputeArrowSnafu)?;
        let keys = keys
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("narthex bulk key must be binary"))?;
        require(keys.null_count() == 0, "narthex bulk key must not be null")?;
        let mut start = 0;
        while start < batch.num_rows() {
            let mut end = start + 1;
            while end < batch.num_rows()
                && end - start < MAX_ROWS
                && keys.value(end) == keys.value(start)
            {
                end += 1;
            }
            let slice = batch.slice(start, end - start);
            let columns = slice
                .columns()
                .iter()
                .zip(slice.schema().fields())
                .map(|(array, field)| {
                    if field.name() == PRIMARY_KEY_COLUMN_NAME {
                        return Ok(NarthexBulkColumn::Key);
                    }
                    let column = match field.data_type() {
                        DataType::Float64 => {
                            let values = array
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .ok_or_else(|| invalid("invalid narthex bulk float column"))?;
                            let bits = values
                                .iter()
                                .map(|v| v.map(f64::to_bits))
                                .collect::<Vec<_>>();
                            NarthexBulkColumn::Float {
                                nulls: array.null_count() as u32,
                                bytes: codec::encode_float(&bits).into(),
                            }
                        }
                        DataType::Timestamp(_, _) => {
                            require(array.null_count() == 0, "null narthex bulk timestamp")?;
                            let array = cast(array, &DataType::Int64).context(ComputeArrowSnafu)?;
                            let values = array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| invalid("invalid narthex bulk timestamp"))?;
                            let (encoding, bytes) = codec::encode_time(values.values());
                            NarthexBulkColumn::Numbers {
                                encoding,
                                bytes: bytes.into(),
                            }
                        }
                        DataType::UInt64 => {
                            require(array.null_count() == 0, "null narthex bulk sequence")?;
                            let values = array
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .ok_or_else(|| invalid("invalid narthex bulk sequence"))?;
                            let (encoding, bytes) = codec::encode_sequence(values.values());
                            NarthexBulkColumn::Numbers {
                                encoding,
                                bytes: bytes.into(),
                            }
                        }
                        DataType::UInt8 => {
                            require(array.null_count() == 0, "null narthex bulk operation")?;
                            let values = array
                                .as_any()
                                .downcast_ref::<UInt8Array>()
                                .ok_or_else(|| invalid("invalid narthex bulk operation"))?;
                            let (encoding, bytes) = codec::encode_operations(values.values());
                            NarthexBulkColumn::Operations {
                                encoding,
                                bytes: bytes.into(),
                            }
                        }
                        other => {
                            return Err(invalid(format!(
                                "unsupported narthex bulk column {}: {other}",
                                field.name()
                            )));
                        }
                    };
                    Ok(column)
                })
                .collect::<Result<Vec<_>>>()?;
            self.blocks.push(NarthexBulkBlock {
                key: Bytes::copy_from_slice(keys.value(start)),
                rows: end - start,
                columns,
            });
            start = end;
        }
        Ok(())
    }

    pub(crate) fn size_bytes(&self) -> usize {
        self.blocks
            .iter()
            .map(|block| {
                block.key.len()
                    + std::mem::size_of::<NarthexBulkBlock>()
                    + block
                        .columns
                        .iter()
                        .map(|column| {
                            std::mem::size_of::<NarthexBulkColumn>()
                                + match column {
                                    NarthexBulkColumn::Key => 0,
                                    NarthexBulkColumn::Numbers { bytes, .. }
                                    | NarthexBulkColumn::Float { bytes, .. }
                                    | NarthexBulkColumn::Operations { bytes, .. } => bytes.len(),
                                }
                        })
                        .sum::<usize>()
            })
            .sum()
    }

    /// Narthex keys are separate from encoded sample columns.
    pub(crate) fn primary_keys(&self) -> crate::memtable::BoxedPrimaryKeyIterator {
        Box::new(
            self.blocks
                .iter()
                .map(|block| block.key.clone())
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    pub(crate) fn read(
        &self,
        schema: SchemaRef,
    ) -> impl Iterator<Item = Result<RecordBatch>> + Send + use<> {
        self.blocks
            .clone()
            .into_iter()
            .map(move |block| block.decode(schema.clone()))
    }
}

impl NarthexBulkBlock {
    fn decode(&self, schema: SchemaRef) -> Result<RecordBatch> {
        let columns = self
            .columns
            .iter()
            .zip(schema.fields())
            .map(|(column, field)| {
                let array: ArrayRef = match column {
                    NarthexBulkColumn::Key => {
                        let array: ArrayRef = Arc::new(BinaryArray::from_iter_values(
                            std::iter::repeat_n(self.key.as_ref(), self.rows),
                        ));
                        cast(&array, field.data_type()).context(ComputeArrowSnafu)?
                    }
                    NarthexBulkColumn::Numbers { encoding, bytes } => {
                        let values = codec::decode_numbers(bytes, *encoding, self.rows)?;
                        if matches!(field.data_type(), DataType::Timestamp(_, _)) {
                            let array: ArrayRef = Arc::new(Int64Array::from(
                                values.into_iter().map(|v| v as i64).collect::<Vec<_>>(),
                            ));
                            cast(&array, field.data_type()).context(ComputeArrowSnafu)?
                        } else {
                            Arc::new(UInt64Array::from(values))
                        }
                    }
                    NarthexBulkColumn::Float { bytes, nulls } => Arc::new(Float64Array::from(
                        codec::decode_float(bytes, self.rows, *nulls)?
                            .into_iter()
                            .map(|v| v.map(f64::from_bits))
                            .collect::<Vec<_>>(),
                    )),
                    NarthexBulkColumn::Operations { encoding, bytes } => Arc::new(
                        UInt8Array::from(codec::decode_operations(bytes, *encoding, self.rows)?),
                    ),
                };
                Ok(array)
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, columns).context(NewRecordBatchSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use datatypes::arrow::datatypes::{Field, Schema, TimeUnit};
    use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME};

    #[test]
    fn narthex_bulk_codec_preserves_keys_nulls_sequences_and_float_bits() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(PRIMARY_KEY_COLUMN_NAME, DataType::Binary, false),
            Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false),
            Field::new(OP_TYPE_COLUMN_NAME, DataType::UInt8, false),
        ]));
        let bits = [
            Some((-0.0_f64).to_bits()),
            None,
            Some(0x7ff0000000000002),
            Some(3.5_f64.to_bits()),
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(
                    bits.iter()
                        .map(|v| v.map(f64::from_bits))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMillisecondArray::from(vec![-1, -1, 2, 3])),
                Arc::new(BinaryArray::from_iter_values([
                    b"a".as_slice(),
                    b"a",
                    b"b",
                    b"b",
                ])),
                Arc::new(UInt64Array::from(vec![9, 8, 11, 10])),
                Arc::new(UInt8Array::from(vec![1, 0, 1, 1])),
            ],
        )
        .unwrap();
        let mut encoded = NarthexBulkPart::default();
        encoded.push(&batch).unwrap();
        assert_eq!(2, encoded.blocks.len());
        let decoded = encoded
            .read(schema.clone())
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            encoded.primary_keys().collect::<Vec<_>>()
        );
        let mut corrupt = encoded.clone();
        corrupt.blocks[0].columns[0] = NarthexBulkColumn::Float {
            nulls: 0,
            bytes: Bytes::new(),
        };
        assert!(corrupt.read(schema).next().unwrap().is_err());
        assert_eq!(
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            corrupt.primary_keys().collect::<Vec<_>>()
        );
        for (index, block) in decoded.iter().enumerate() {
            let expected = batch.slice(index * 2, 2);
            for column in 1..5 {
                assert_eq!(expected.column(column), block.column(column));
            }
            let values = block
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(
                bits[index * 2..index * 2 + 2],
                values
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>()
            );
        }
    }
}
