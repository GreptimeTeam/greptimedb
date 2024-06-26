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

use bytes::Buf;
use common_base::bytes::Bytes;
use common_decimal::Decimal128;
use common_time::time::Time;
use common_time::{Date, Duration, Interval};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::value::ValueRef;
use memcomparable::{Deserializer, Serializer};
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;

use crate::error;
use crate::error::{FieldTypeMismatchSnafu, NotSupportedFieldSnafu, Result, SerializeFieldSnafu};

/// Row value encoder/decoder.
pub trait RowCodec {
    /// Encodes rows to bytes.
    /// # Note
    /// Ensure the length of row iterator matches the length of fields.
    fn encode<'a, I>(&self, row: I) -> Result<Vec<u8>>
    where
        I: Iterator<Item = ValueRef<'a>>;

    /// Encodes rows to specific vec.
    /// # Note
    /// Ensure the length of row iterator matches the length of fields.
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>;

    /// Decode row values from bytes.
    fn decode(&self, bytes: &[u8]) -> Result<Vec<Value>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortField {
    pub(crate) data_type: ConcreteDataType,
}

impl SortField {
    pub fn new(data_type: ConcreteDataType) -> Self {
        Self { data_type }
    }

    pub fn estimated_size(&self) -> usize {
        match &self.data_type {
            ConcreteDataType::Boolean(_) => 2,
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => 2,
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => 3,
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => 5,
            ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => 9,
            ConcreteDataType::Float32(_) => 5,
            ConcreteDataType::Float64(_) => 9,
            ConcreteDataType::Binary(_) => 11,
            ConcreteDataType::String(_) => 11, // a non-empty string takes at least 11 bytes.
            ConcreteDataType::Date(_) => 5,
            ConcreteDataType::DateTime(_) => 9,
            ConcreteDataType::Timestamp(_) => 10,
            ConcreteDataType::Time(_) => 10,
            ConcreteDataType::Duration(_) => 10,
            ConcreteDataType::Interval(_) => 18,
            ConcreteDataType::Decimal128(_) => 19,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => 0,
        }
    }
}

impl SortField {
    pub(crate) fn serialize(
        &self,
        serializer: &mut Serializer<&mut Vec<u8>>,
        value: &ValueRef,
    ) -> Result<()> {
        macro_rules! cast_value_and_serialize {
            (
                $self: ident;
                $serializer: ident;
                $(
                    $ty: ident, $f: ident
                ),*
            ) => {
                match &$self.data_type {
                $(
                    ConcreteDataType::$ty(_) => {
                        paste!{
                            value
                            .[<as_ $f>]()
                            .context(FieldTypeMismatchSnafu)?
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                        }
                    }
                )*
                    ConcreteDataType::Timestamp(_) => {
                        let timestamp = value.as_timestamp().context(FieldTypeMismatchSnafu)?;
                        timestamp
                            .map(|t|t.value())
                            .serialize($serializer)
                            .context(SerializeFieldSnafu)?;
                    }
                    ConcreteDataType::List(_) |
                    ConcreteDataType::Dictionary(_) |
                    ConcreteDataType::Null(_) => {
                        return error::NotSupportedFieldSnafu {
                            data_type: $self.data_type.clone()
                        }.fail()
                    }
                }
            };
        }
        cast_value_and_serialize!(self; serializer;
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
            DateTime, datetime,
            Time, time,
            Interval, interval,
            Duration, duration,
            Decimal128, decimal128
        );

        Ok(())
    }

    fn deserialize<B: Buf>(&self, deserializer: &mut Deserializer<B>) -> Result<Value> {
        use common_time::DateTime;
        macro_rules! deserialize_and_build_value {
            (
                $self: ident;
                $serializer: ident;
                $(
                    $ty: ident, $f: ident
                ),*
            ) => {

                match &$self.data_type {
                    $(
                        ConcreteDataType::$ty(_) => {
                            Ok(Value::from(Option::<$f>::deserialize(deserializer).context(error::DeserializeFieldSnafu)?))
                        }
                    )*
                    ConcreteDataType::Binary(_) => Ok(Value::from(
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
                    ConcreteDataType::List(l) => NotSupportedFieldSnafu {
                        data_type: ConcreteDataType::List(l.clone()),
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
        deserialize_and_build_value!(self; deserializer;
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
            DateTime, DateTime,
            Interval, Interval,
            Duration, Duration,
            Decimal128, Decimal128
        )
    }

    /// Skip deserializing this field, returns the length of it.
    fn skip_deserialize(
        &self,
        bytes: &[u8],
        deserializer: &mut Deserializer<&[u8]>,
    ) -> Result<usize> {
        let pos = deserializer.position();
        if bytes[pos] == 0 {
            deserializer.advance(1);
            return Ok(1);
        }

        let to_skip = match &self.data_type {
            ConcreteDataType::Boolean(_) => 2,
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => 2,
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => 3,
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => 5,
            ConcreteDataType::Int64(_) | ConcreteDataType::UInt64(_) => 9,
            ConcreteDataType::Float32(_) => 5,
            ConcreteDataType::Float64(_) => 9,
            ConcreteDataType::Binary(_) => {
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
            ConcreteDataType::DateTime(_) => 9,
            ConcreteDataType::Timestamp(_) => 9, // We treat timestamp as Option<i64>
            ConcreteDataType::Time(_) => 10,     // i64 and 1 byte time unit
            ConcreteDataType::Duration(_) => 10,
            ConcreteDataType::Interval(_) => 18,
            ConcreteDataType::Decimal128(_) => 19,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => 0,
        };
        deserializer.advance(to_skip);
        Ok(to_skip)
    }
}

/// A memory-comparable row [Value] encoder/decoder.
#[derive(Debug)]
pub struct McmpRowCodec {
    fields: Vec<SortField>,
}

impl McmpRowCodec {
    /// Creates [McmpRowCodec] instance with all primary keys in given `metadata`.
    pub fn new_with_primary_keys(metadata: &RegionMetadata) -> Self {
        Self::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        )
    }

    pub fn new(fields: Vec<SortField>) -> Self {
        Self { fields }
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Estimated length for encoded bytes.
    pub fn estimated_size(&self) -> usize {
        self.fields.iter().map(|f| f.estimated_size()).sum()
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
            return self.fields[pos].deserialize(&mut deserializer);
        }

        if offsets_buf.is_empty() {
            let mut offset = 0;
            // Skip values before `pos`.
            for i in 0..pos {
                // Offset to skip before reading value i.
                offsets_buf.push(offset);
                let skip = self.fields[i].skip_deserialize(bytes, &mut deserializer)?;
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
                let skip = self.fields[i].skip_deserialize(bytes, &mut deserializer)?;
                // Offset for the value at i + 1.
                offset += skip;
                offsets_buf.push(offset);
            }
        }

        self.fields[pos].deserialize(&mut deserializer)
    }
}

impl RowCodec for McmpRowCodec {
    fn encode<'a, I>(&self, row: I) -> Result<Vec<u8>>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        let mut buffer = Vec::new();
        self.encode_to_vec(row, &mut buffer)?;
        Ok(buffer)
    }

    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        buffer.reserve(self.estimated_size());
        let mut serializer = Serializer::new(buffer);
        for (value, field) in row.zip(self.fields.iter()) {
            field.serialize(&mut serializer, &value)?;
        }
        Ok(())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Vec<Value>> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = Vec::with_capacity(self.fields.len());
        for f in &self.fields {
            let value = f.deserialize(&mut deserializer)?;
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use common_base::bytes::StringBytes;
    use common_time::{DateTime, Timestamp};
    use datatypes::value::Value;

    use super::*;

    fn check_encode_and_decode(data_types: &[ConcreteDataType], row: Vec<Value>) {
        let encoder = McmpRowCodec::new(
            data_types
                .iter()
                .map(|t| SortField::new(t.clone()))
                .collect::<Vec<_>>(),
        );

        let value_ref = row.iter().map(|v| v.as_value_ref()).collect::<Vec<_>>();

        let result = encoder.encode(value_ref.iter().cloned()).unwrap();
        let decoded = encoder.decode(&result).unwrap();
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
            assert_eq!(data_types.len(), offsets.len(), "offsets: {:?}", offsets);
            assert_eq!(decoded, row);
        }
    }

    #[test]
    fn test_memcmp() {
        let encoder = McmpRowCodec::new(vec![
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::int64_datatype()),
        ]);
        let values = [Value::String("abcdefgh".into()), Value::Int64(128)];
        let value_ref = values.iter().map(|v| v.as_value_ref()).collect::<Vec<_>>();
        let result = encoder.encode(value_ref.iter().cloned()).unwrap();

        let decoded = encoder.decode(&result).unwrap();
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
                ConcreteDataType::datetime_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::time_millisecond_datatype(),
                ConcreteDataType::duration_millisecond_datatype(),
                ConcreteDataType::interval_month_day_nano_datatype(),
                ConcreteDataType::decimal128_default_datatype(),
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
                Value::DateTime(DateTime::new(11)),
                Value::Timestamp(Timestamp::new_millisecond(12)),
                Value::Time(Time::new_millisecond(13)),
                Value::Duration(Duration::new_millisecond(14)),
                Value::Interval(Interval::from_month_day_nano(1, 1, 15)),
                Value::Decimal128(Decimal128::from(16)),
            ],
        );
    }
}
