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

use std::sync::Arc;

use bytes::Buf;
use common_base::bytes::Bytes;
use common_decimal::Decimal128;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::time::Time;
use common_time::{Date, Duration, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::types::IntervalType;
use datatypes::value::ValueRef;
use memcomparable::{Deserializer, Serializer};
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{
    self, FieldTypeMismatchSnafu, NotSupportedFieldSnafu, Result, SerializeFieldSnafu,
};
use crate::key_values::KeyValue;
use crate::primary_key_filter::DensePrimaryKeyFilter;
use crate::row_converter::{
    CompositeValues, PrimaryKeyCodec, PrimaryKeyCodecExt, PrimaryKeyFilter,
};

/// Field to serialize and deserialize value in memcomparable format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    data_type: ConcreteDataType,
}

impl SortField {
    pub fn new(data_type: ConcreteDataType) -> Self {
        Self { data_type }
    }

    /// Returns the data type of the field.
    pub fn data_type(&self) -> &ConcreteDataType {
        &self.data_type
    }

    /// Returns the physical data type to encode of the field.
    ///
    /// For example, a dictionary field will be encoded as its value type.
    pub fn encode_data_type(&self) -> &ConcreteDataType {
        match &self.data_type {
            ConcreteDataType::Dictionary(dict_type) => dict_type.value_type(),
            _ => &self.data_type,
        }
    }

    pub fn estimated_size(&self) -> usize {
        Self::estimated_size_by_type(self.encode_data_type())
    }

    fn estimated_size_by_type(data_type: &ConcreteDataType) -> usize {
        match data_type {
            ConcreteDataType::Boolean(_) => 2,
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => 2,
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => 3,
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => 5,
            ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => 9,
            ConcreteDataType::Float32(_) => 5,
            ConcreteDataType::Float64(_) => 9,
            ConcreteDataType::Binary(_)
            | ConcreteDataType::Json(_)
            | ConcreteDataType::Vector(_) => 11,
            ConcreteDataType::String(_) => 11, // a non-empty string takes at least 11 bytes.
            ConcreteDataType::Date(_) => 5,
            ConcreteDataType::Timestamp(_) => 10,
            ConcreteDataType::Time(_) => 10,
            ConcreteDataType::Duration(_) => 10,
            ConcreteDataType::Interval(_) => 18,
            ConcreteDataType::Decimal128(_) => 19,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Struct(_)
            | ConcreteDataType::Dictionary(_) => 0,
        }
    }

    /// Serialize a value to the serializer.
    pub fn serialize(
        &self,
        serializer: &mut Serializer<&mut Vec<u8>>,
        value: &ValueRef,
    ) -> Result<()> {
        Self::serialize_by_type(self.encode_data_type(), serializer, value)
    }

    fn serialize_by_type(
        data_type: &ConcreteDataType,
        serializer: &mut Serializer<&mut Vec<u8>>,
        value: &ValueRef,
    ) -> Result<()> {
        macro_rules! cast_value_and_serialize {
            (
                $data_type: ident;
                $serializer: ident;
                $(
                    $ty: ident, $f: ident
                ),*
            ) => {
                match $data_type {
                $(
                    ConcreteDataType::$ty(_) => {
                        paste!{
                            value
                            .[<try_into_ $f>]()
                            .context(FieldTypeMismatchSnafu)?
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                        }
                    }
                )*
                    ConcreteDataType::Timestamp(_) => {
                        let timestamp = value.try_into_timestamp().context(FieldTypeMismatchSnafu)?;
                        timestamp
                            .map(|t|t.value())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                        let interval = value.try_into_interval_year_month().context(FieldTypeMismatchSnafu)?;
                        interval.map(|i| i.to_i32())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                        let interval = value.try_into_interval_day_time().context(FieldTypeMismatchSnafu)?;
                        interval.map(|i| i.to_i64())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                        let interval = value.try_into_interval_month_day_nano().context(FieldTypeMismatchSnafu)?;
                        interval.map(|i| i.to_i128())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::List(_) |
                    ConcreteDataType::Struct(_) |
                    ConcreteDataType::Dictionary(_) |
                    ConcreteDataType::Null(_) => {
                        return error::NotSupportedFieldSnafu {
                            data_type: $data_type.clone()
                        }.fail()
                    }
                }
            };
        }
        cast_value_and_serialize!(data_type; serializer;
            Boolean, boolean,
            Binary, binary,
            Int8, i8,
            UInt8, u8,
            Int16, i16,
            UInt16, u16,
            Int32, i32,
            UInt32, u32,
            Int64, i64,
            UInt64, u64,
            Float32, f32,
            Float64, f64,
            String, string,
            Date, date,
            Time, time,
            Duration, duration,
            Decimal128, decimal128,
            Json, binary,
            Vector, binary
        );

        Ok(())
    }

    /// Deserialize a value from the deserializer.
    pub fn deserialize<B: Buf>(&self, deserializer: &mut Deserializer<B>) -> Result<Value> {
        Self::deserialize_by_type(self.encode_data_type(), deserializer)
    }

    fn deserialize_by_type<B: Buf>(
        data_type: &ConcreteDataType,
        deserializer: &mut Deserializer<B>,
    ) -> Result<Value> {
        macro_rules! deserialize_and_build_value {
            (
                $data_type: ident;
                $serializer: ident;
                $(
                    $ty: ident, $f: ident
                ),*
            ) => {

                match $data_type {
                    $(
                        ConcreteDataType::$ty(_) => {
                            Ok(Value::from(Option::<$f>::deserialize(deserializer).context(error::DeserializeFieldSnafu)?))
                        }
                    )*
                    ConcreteDataType::Binary(_) | ConcreteDataType::Json(_) | ConcreteDataType::Vector(_) => Ok(Value::from(
                        Option::<Vec<u8>>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(Bytes::from),
                    )),
                    ConcreteDataType::Timestamp(ty) => {
                        let timestamp = Option::<i64>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(|t|ty.create_timestamp(t));
                        Ok(Value::from(timestamp))
                    }
                    ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                        let interval = Option::<i32>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(IntervalYearMonth::from_i32);
                        Ok(Value::from(interval))
                    }
                    ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                        let interval = Option::<i64>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(IntervalDayTime::from_i64);
                        Ok(Value::from(interval))
                    }
                    ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                        let interval = Option::<i128>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(IntervalMonthDayNano::from_i128);
                        Ok(Value::from(interval))
                    }
                    ConcreteDataType::List(l) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::List(l.clone()),
                    }
                    .fail(),
                    ConcreteDataType::Struct(f) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::Struct(f.clone()),
                    }
                    .fail(),
                    ConcreteDataType::Dictionary(d) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::Dictionary(d.clone()),
                    }
                    .fail(),
                    ConcreteDataType::Null(n) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::Null(n.clone()),
                    }
                    .fail(),
                }
            };
        }
        deserialize_and_build_value!(data_type; deserializer;
            Boolean, bool,
            Int8, i8,
            Int16, i16,
            Int32, i32,
            Int64, i64,
            UInt8, u8,
            UInt16, u16,
            UInt32, u32,
            UInt64, u64,
            Float32, f32,
            Float64, f64,
            String, String,
            Date, Date,
            Time, Time,
            Duration, Duration,
            Decimal128, Decimal128
        )
    }

    /// Skip deserializing this field, returns the length of it.
    pub(crate) fn skip_deserialize(
        &self,
        bytes: &[u8],
        deserializer: &mut Deserializer<&[u8]>,
    ) -> Result<usize> {
        let pos = deserializer.position();
        if bytes[pos] == 0 {
            deserializer.advance(1);
            return Ok(1);
        }

        Self::skip_deserialize_by_type(self.encode_data_type(), bytes, deserializer)
    }

    fn skip_deserialize_by_type(
        data_type: &ConcreteDataType,
        bytes: &[u8],
        deserializer: &mut Deserializer<&[u8]>,
    ) -> Result<usize> {
        let to_skip = match data_type {
            ConcreteDataType::Boolean(_) => 2,
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => 2,
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => 3,
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => 5,
            ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => 9,
            ConcreteDataType::Float32(_) => 5,
            ConcreteDataType::Float64(_) => 9,
            ConcreteDataType::Binary(_)
            | ConcreteDataType::Json(_)
            | ConcreteDataType::Vector(_) => {
                // Now the encoder encode binary as a list of bytes so we can't use
                // skip bytes.
                let pos_before = deserializer.position();
                let mut current = pos_before + 1;
                while bytes[current] == 1 {
                    current += 2;
                }
                let to_skip = current - pos_before + 1;
                deserializer.advance(to_skip);
                return Ok(to_skip);
            }
            ConcreteDataType::String(_) => {
                let pos_before = deserializer.position();
                deserializer.advance(1);
                deserializer
                    .skip_bytes()
                    .context(error::DeserializeFieldSnafu)?;
                return Ok(deserializer.position() - pos_before);
            }
            ConcreteDataType::Date(_) => 5,
            ConcreteDataType::Timestamp(_) => 9, // We treat timestamp as Option<i64>
            ConcreteDataType::Time(_) => 10,     // i64 and 1 byte time unit
            ConcreteDataType::Duration(_) => 10,
            ConcreteDataType::Interval(IntervalType::YearMonth(_)) => 5,
            ConcreteDataType::Interval(IntervalType::DayTime(_)) => 9,
            ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => 17,
            ConcreteDataType::Decimal128(_) => 19,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Struct(_)
            | ConcreteDataType::Dictionary(_) => 0,
        };
        deserializer.advance(to_skip);
        Ok(to_skip)
    }
}

impl PrimaryKeyCodecExt for DensePrimaryKeyCodec {
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        self.encode_dense(row, buffer)
    }
}

/// A memory-comparable row [`Value`] encoder/decoder.
#[derive(Clone, Debug)]
pub struct DensePrimaryKeyCodec {
    /// Primary key fields.
    ordered_primary_key_columns: Arc<Vec<(ColumnId, SortField)>>,
}

impl DensePrimaryKeyCodec {
    pub fn new(metadata: &RegionMetadata) -> Self {
        let ordered_primary_key_columns = metadata
            .primary_key_columns()
            .map(|c| {
                (
                    c.column_id,
                    SortField::new(c.column_schema.data_type.clone()),
                )
            })
            .collect::<Vec<_>>();

        Self::with_fields(ordered_primary_key_columns)
    }

    pub fn with_fields(fields: Vec<(ColumnId, SortField)>) -> Self {
        Self {
            ordered_primary_key_columns: Arc::new(fields),
        }
    }

    fn encode_dense<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        let mut serializer = Serializer::new(buffer);
        for (idx, value) in row.enumerate() {
            self.field_at(idx).serialize(&mut serializer, &value)?;
        }
        Ok(())
    }

    /// Decode primary key values from bytes.
    pub fn decode_dense(&self, bytes: &[u8]) -> Result<Vec<(ColumnId, Value)>> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = Vec::with_capacity(self.ordered_primary_key_columns.len());
        for (column_id, field) in self.ordered_primary_key_columns.iter() {
            let value = field.deserialize(&mut deserializer)?;
            values.push((*column_id, value));
        }
        Ok(values)
    }

    /// Decode primary key values from bytes without column id.
    pub fn decode_dense_without_column_id(&self, bytes: &[u8]) -> Result<Vec<Value>> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = Vec::with_capacity(self.ordered_primary_key_columns.len());
        for (_, field) in self.ordered_primary_key_columns.iter() {
            let value = field.deserialize(&mut deserializer)?;
            values.push(value);
        }
        Ok(values)
    }

    /// Returns the field at `pos`.
    ///
    /// # Panics
    /// Panics if `pos` is out of bounds.
    fn field_at(&self, pos: usize) -> &SortField {
        &self.ordered_primary_key_columns[pos].1
    }

    /// Decode value at `pos` in `bytes`.
    ///
    /// The i-th element in offsets buffer is how many bytes to skip in order to read value at `pos`.
    pub fn decode_value_at(
        &self,
        bytes: &[u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
    ) -> Result<Value> {
        let mut deserializer = Deserializer::new(bytes);
        if pos < offsets_buf.len() {
            // We computed the offset before.
            let to_skip = offsets_buf[pos];
            deserializer.advance(to_skip);
            return self.field_at(pos).deserialize(&mut deserializer);
        }

        if offsets_buf.is_empty() {
            let mut offset = 0;
            // Skip values before `pos`.
            for i in 0..pos {
                // Offset to skip before reading value i.
                offsets_buf.push(offset);
                let skip = self
                    .field_at(i)
                    .skip_deserialize(bytes, &mut deserializer)?;
                offset += skip;
            }
            // Offset to skip before reading this value.
            offsets_buf.push(offset);
        } else {
            // Offsets are not enough.
            let value_start = offsets_buf.len() - 1;
            // Advances to decode value at `value_start`.
            let mut offset = offsets_buf[value_start];
            deserializer.advance(offset);
            for i in value_start..pos {
                // Skip value i.
                let skip = self
                    .field_at(i)
                    .skip_deserialize(bytes, &mut deserializer)?;
                // Offset for the value at i + 1.
                offset += skip;
                offsets_buf.push(offset);
            }
        }

        self.field_at(pos).deserialize(&mut deserializer)
    }

    /// Returns the encoded bytes at `pos` in `bytes`.
    ///
    /// The i-th element in offsets buffer is how many bytes to skip in order to read value at
    /// `pos`.
    pub fn encoded_value_at<'a>(
        &self,
        bytes: &'a [u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
    ) -> Result<&'a [u8]> {
        let mut deserializer = Deserializer::new(bytes);

        let offset = if pos < offsets_buf.len() {
            // We computed the offset before.
            let to_skip = offsets_buf[pos];
            deserializer.advance(to_skip);
            to_skip
        } else if offsets_buf.is_empty() {
            let mut offset = 0;
            // Skip values before `pos`.
            for i in 0..pos {
                // Offset to skip before reading value i.
                offsets_buf.push(offset);
                let skip = self
                    .field_at(i)
                    .skip_deserialize(bytes, &mut deserializer)?;
                offset += skip;
            }
            // Offset to skip before reading this value.
            offsets_buf.push(offset);
            offset
        } else {
            // Offsets are not enough.
            let value_start = offsets_buf.len() - 1;
            // Advances to decode value at `value_start`.
            let mut offset = offsets_buf[value_start];
            deserializer.advance(offset);
            for i in value_start..pos {
                // Skip value i.
                let skip = self
                    .field_at(i)
                    .skip_deserialize(bytes, &mut deserializer)?;
                // Offset for the value at i + 1.
                offset += skip;
                offsets_buf.push(offset);
            }
            offset
        };

        let len = self
            .field_at(pos)
            .skip_deserialize(bytes, &mut deserializer)?;
        Ok(&bytes[offset..offset + len])
    }

    pub fn estimated_size(&self) -> usize {
        self.ordered_primary_key_columns
            .iter()
            .map(|(_, f)| f.estimated_size())
            .sum()
    }

    pub fn num_fields(&self) -> usize {
        self.ordered_primary_key_columns.len()
    }
}

impl PrimaryKeyCodec for DensePrimaryKeyCodec {
    fn encode_key_value(&self, key_value: &KeyValue, buffer: &mut Vec<u8>) -> Result<()> {
        self.encode_dense(key_value.primary_keys(), buffer)
    }

    fn encode_values(&self, values: &[(ColumnId, Value)], buffer: &mut Vec<u8>) -> Result<()> {
        self.encode_dense(values.iter().map(|(_, v)| v.as_value_ref()), buffer)
    }

    fn encode_value_refs(
        &self,
        values: &[(ColumnId, ValueRef)],
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        let iter = values.iter().map(|(_, v)| v.clone());
        self.encode_dense(iter, buffer)
    }

    fn estimated_size(&self) -> Option<usize> {
        Some(self.estimated_size())
    }

    fn num_fields(&self) -> Option<usize> {
        Some(self.num_fields())
    }

    fn encoding(&self) -> PrimaryKeyEncoding {
        PrimaryKeyEncoding::Dense
    }

    fn primary_key_filter(
        &self,
        metadata: &RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
    ) -> Box<dyn PrimaryKeyFilter> {
        Box::new(DensePrimaryKeyFilter::new(
            metadata.clone(),
            filters,
            self.clone(),
        ))
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        Ok(CompositeValues::Dense(self.decode_dense(bytes)?))
    }

    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        // TODO(weny, yinwen): avoid decoding the whole primary key.
        let mut values = self.decode_dense(bytes)?;
        Ok(values.pop().map(|(_, v)| v))
    }
}

#[cfg(test)]
mod tests {
    use common_base::bytes::StringBytes;
    use common_time::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, Timestamp};
    use datatypes::value::Value;

    use super::*;

    fn check_encode_and_decode(data_types: &[ConcreteDataType], row: Vec<Value>) {
        let encoder = DensePrimaryKeyCodec::with_fields(
            data_types
                .iter()
                .map(|t| (0, SortField::new(t.clone())))
                .collect::<Vec<_>>(),
        );

        let value_ref = row.iter().map(|v| v.as_value_ref()).collect::<Vec<_>>();

        let result = encoder.encode(value_ref.iter().cloned()).unwrap();
        let decoded = encoder.decode(&result).unwrap().into_dense();
        assert_eq!(decoded, row);
        let mut decoded = Vec::new();
        let mut offsets = Vec::new();
        // Iter two times to test offsets buffer.
        for _ in 0..2 {
            decoded.clear();
            for i in 0..data_types.len() {
                let value = encoder.decode_value_at(&result, i, &mut offsets).unwrap();
                decoded.push(value);
            }
            assert_eq!(data_types.len(), offsets.len(), "offsets: {offsets:?}");
            assert_eq!(decoded, row);
        }
    }

    #[test]
    fn test_memcmp() {
        let encoder = DensePrimaryKeyCodec::with_fields(vec![
            (0, SortField::new(ConcreteDataType::string_datatype())),
            (1, SortField::new(ConcreteDataType::int64_datatype())),
        ]);
        let values = [Value::String("abcdefgh".into()), Value::Int64(128)];
        let value_ref = values.iter().map(|v| v.as_value_ref()).collect::<Vec<_>>();
        let result = encoder.encode(value_ref.iter().cloned()).unwrap();

        let decoded = encoder.decode(&result).unwrap().into_dense();
        assert_eq!(&values, &decoded as &[Value]);
    }

    #[test]
    fn test_memcmp_timestamp() {
        check_encode_and_decode(
            &[
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            vec![
                Value::Timestamp(Timestamp::new_millisecond(42)),
                Value::Int64(43),
            ],
        );
    }

    #[test]
    fn test_memcmp_duration() {
        check_encode_and_decode(
            &[
                ConcreteDataType::duration_millisecond_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            vec![
                Value::Duration(Duration::new_millisecond(44)),
                Value::Int64(45),
            ],
        )
    }

    #[test]
    fn test_memcmp_binary() {
        check_encode_and_decode(
            &[
                ConcreteDataType::binary_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            vec![
                Value::Binary(Bytes::from("hello".as_bytes())),
                Value::Int64(43),
            ],
        );
    }

    #[test]
    fn test_memcmp_string() {
        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            vec![Value::String(StringBytes::from("hello"))],
        );

        check_encode_and_decode(&[ConcreteDataType::string_datatype()], vec![Value::Null]);

        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            vec![Value::String("".into())],
        );
        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            vec![Value::String("world".into())],
        );
    }

    #[test]
    fn test_encode_null() {
        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int32_datatype(),
            ],
            vec![Value::String(StringBytes::from("abcd")), Value::Null],
        )
    }

    #[test]
    fn test_memcmp_dictionary() {
        // Test Dictionary<i32, string>
        check_encode_and_decode(
            &[ConcreteDataType::dictionary_datatype(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype(),
            )],
            vec![Value::String("hello".into())],
        );

        // Test Dictionary<i32, i64>
        check_encode_and_decode(
            &[ConcreteDataType::dictionary_datatype(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
            )],
            vec![Value::Int64(42)],
        );

        // Test Dictionary with null value
        check_encode_and_decode(
            &[ConcreteDataType::dictionary_datatype(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype(),
            )],
            vec![Value::Null],
        );

        // Test multiple Dictionary columns
        check_encode_and_decode(
            &[
                ConcreteDataType::dictionary_datatype(
                    ConcreteDataType::int32_datatype(),
                    ConcreteDataType::string_datatype(),
                ),
                ConcreteDataType::dictionary_datatype(
                    ConcreteDataType::int16_datatype(),
                    ConcreteDataType::int64_datatype(),
                ),
            ],
            vec![Value::String("world".into()), Value::Int64(123)],
        );
    }

    #[test]
    fn test_encode_multiple_rows() {
        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            vec![
                Value::String("hello".into()),
                Value::Int64(42),
                Value::Boolean(false),
            ],
        );

        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            vec![
                Value::String("world".into()),
                Value::Int64(43),
                Value::Boolean(true),
            ],
        );

        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            vec![Value::Null, Value::Int64(43), Value::Boolean(true)],
        );

        // All types.
        check_encode_and_decode(
            &[
                ConcreteDataType::boolean_datatype(),
                ConcreteDataType::int8_datatype(),
                ConcreteDataType::uint8_datatype(),
                ConcreteDataType::int16_datatype(),
                ConcreteDataType::uint16_datatype(),
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::uint32_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::uint64_datatype(),
                ConcreteDataType::float32_datatype(),
                ConcreteDataType::float64_datatype(),
                ConcreteDataType::binary_datatype(),
                ConcreteDataType::string_datatype(),
                ConcreteDataType::date_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::time_millisecond_datatype(),
                ConcreteDataType::duration_millisecond_datatype(),
                ConcreteDataType::interval_year_month_datatype(),
                ConcreteDataType::interval_day_time_datatype(),
                ConcreteDataType::interval_month_day_nano_datatype(),
                ConcreteDataType::decimal128_default_datatype(),
                ConcreteDataType::vector_datatype(3),
                ConcreteDataType::dictionary_datatype(
                    ConcreteDataType::int32_datatype(),
                    ConcreteDataType::string_datatype(),
                ),
            ],
            vec![
                Value::Boolean(true),
                Value::Int8(8),
                Value::UInt8(8),
                Value::Int16(16),
                Value::UInt16(16),
                Value::Int32(32),
                Value::UInt32(32),
                Value::Int64(64),
                Value::UInt64(64),
                Value::Float32(1.0.into()),
                Value::Float64(1.0.into()),
                Value::Binary(b"hello"[..].into()),
                Value::String("world".into()),
                Value::Date(Date::new(10)),
                Value::Timestamp(Timestamp::new_millisecond(12)),
                Value::Time(Time::new_millisecond(13)),
                Value::Duration(Duration::new_millisecond(14)),
                Value::IntervalYearMonth(IntervalYearMonth::new(1)),
                Value::IntervalDayTime(IntervalDayTime::new(1, 15)),
                Value::IntervalMonthDayNano(IntervalMonthDayNano::new(1, 1, 15)),
                Value::Decimal128(Decimal128::from(16)),
                Value::Binary(Bytes::from(vec![0; 12])),
                Value::String("dict_value".into()),
            ],
        );
    }
}
