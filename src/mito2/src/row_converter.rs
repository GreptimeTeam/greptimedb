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
use common_time::time::Time;
use common_time::{Date, Interval, Timestamp};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::value::ValueRef;
use memcomparable::{Deserializer, Serializer};
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};

use crate::error;
use crate::error::{
    FieldTypeMismatchSnafu, NotSupportedFieldSnafu, RowLengthMismatchSnafu, SerializeFieldSnafu,
};

/// Row value encoder/decoder.
pub trait RowCodec {
    /// Encodes rows to bytes.
    fn encode<'a, I>(&self, rows: I) -> error::Result<Vec<u8>>
    where
        I: Iterator<Item = &'a [ValueRef<'a>]>;

    /// Decode row values from bytes.
    fn decode(&self, bytes: &[u8]) -> error::Result<Vec<Vec<Value>>>;
}

pub struct SortField {
    data_type: ConcreteDataType,
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
            ConcreteDataType::Interval(_) => 18,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => 0,
        }
    }
}

impl SortField {
    fn serialize(
        &self,
        serializer: &mut Serializer<&mut Vec<u8>>,
        value: &ValueRef,
    ) -> error::Result<()> {
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
                        timestamp.map(MemComparableTimestamp::from)
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
            Interval, interval
        );

        Ok(())
    }

    fn deserialize<B: Buf>(&self, deserializer: &mut Deserializer<B>) -> error::Result<Value> {
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
                    ConcreteDataType::Timestamp(_) => {
                        let timestamp = Option::<MemComparableTimestamp>::deserialize(deserializer)
                            .context(error::DeserializeFieldSnafu)?
                            .map(Timestamp::try_from)
                            .transpose()?;
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
            Interval, Interval
        )
    }
}

/// A memory-comparable representation for [Timestamp] that is used for
/// serialization/deserialization.
#[derive(Debug, Serialize, Deserialize)]
struct MemComparableTimestamp {
    sec: i64,
    nsec: u32,
}

impl From<Timestamp> for MemComparableTimestamp {
    fn from(value: Timestamp) -> Self {
        let (sec, nsec) = value.split();
        Self { sec, nsec }
    }
}

impl TryFrom<MemComparableTimestamp> for Timestamp {
    type Error = error::Error;

    fn try_from(
        MemComparableTimestamp { sec, nsec }: MemComparableTimestamp,
    ) -> Result<Self, Self::Error> {
        Timestamp::from_splits(sec, nsec)
            .ok_or(error::DeserializeTimestampSnafu { sec, nsec }.build())
    }
}

/// A memory-comparable row [Value] encoder/decoder.
pub struct McmpRowCodec {
    fields: Vec<SortField>,
}

impl McmpRowCodec {
    pub fn new(fields: Vec<SortField>) -> Self {
        Self { fields }
    }

    /// Estimated length for encoded bytes.
    fn estimated_length(&self) -> usize {
        self.fields.iter().map(|f| f.estimated_size()).sum()
    }
}

impl RowCodec for McmpRowCodec {
    fn encode<'a, I>(&self, rows: I) -> error::Result<Vec<u8>>
    where
        I: Iterator<Item = &'a [ValueRef<'a>]>,
    {
        let mut bytes = Vec::with_capacity(self.estimated_length());
        let mut serializer = memcomparable::Serializer::new(&mut bytes);

        for row in rows {
            ensure!(
                row.len() == self.fields.len(),
                RowLengthMismatchSnafu {
                    expect: self.fields.len(),
                    actual: row.len(),
                }
            );

            for (value, field) in row.iter().zip(self.fields.iter()) {
                field.serialize(&mut serializer, value)?;
            }
        }
        Ok(bytes)
    }

    fn decode(&self, bytes: &[u8]) -> error::Result<Vec<Vec<Value>>> {
        let mut deserializer = memcomparable::Deserializer::new(bytes);
        let mut res = vec![];
        while deserializer.has_remaining() {
            let mut values = Vec::with_capacity(self.fields.len());
            for f in &self.fields {
                let value = f.deserialize(&mut deserializer)?;
                values.push(value);
            }
            res.push(values);
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use common_base::bytes::StringBytes;
    use datatypes::value::Value;

    use super::*;

    fn check_encode_and_decode(data_types: &[ConcreteDataType], rows: &[Vec<Value>]) {
        let encoder = McmpRowCodec::new(
            data_types
                .iter()
                .map(|t| SortField::new(t.clone()))
                .collect::<Vec<_>>(),
        );

        let value_ref = rows
            .iter()
            .map(|row| row.iter().map(|v| v.as_value_ref()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let result = encoder
            .encode(value_ref.iter().map(|r| r.as_slice()))
            .unwrap();
        let decoded = encoder.decode(&result).unwrap();
        assert_eq!(value_ref.len(), decoded.len());

        for i in 0..rows.len() {
            assert_eq!(&rows[i], decoded.get(i).unwrap() as &[Value]);
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
        let result = encoder.encode(std::iter::once(&value_ref as _)).unwrap();

        let decoded = encoder.decode(&result).unwrap();
        assert_eq!(1, decoded.len());
        assert_eq!(&values, decoded.get(0).unwrap() as &[Value]);
    }

    #[test]
    fn test_memcmp_timestamp() {
        check_encode_and_decode(
            &[
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            &[vec![
                Value::Timestamp(Timestamp::new_millisecond(42)),
                Value::Int64(43),
            ]],
        );
    }

    #[test]
    fn test_memcmp_binary() {
        check_encode_and_decode(
            &[
                ConcreteDataType::binary_datatype(),
                ConcreteDataType::int64_datatype(),
            ],
            &[vec![
                Value::Binary(Bytes::from("hello".as_bytes())),
                Value::Int64(43),
            ]],
        );
    }

    #[test]
    fn test_memcmp_string() {
        check_encode_and_decode(
            &[ConcreteDataType::string_datatype()],
            &[
                vec![Value::String(StringBytes::from("hello"))],
                vec![Value::Null],
                vec![Value::String("".into())],
                vec![Value::String("world".into())],
            ],
        );
    }

    #[test]
    fn test_encode_null() {
        check_encode_and_decode(
            &[
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int32_datatype(),
            ],
            &[vec![Value::String(StringBytes::from("abcd")), Value::Null]],
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
            &[
                vec![
                    Value::String("hello".into()),
                    Value::Int64(42),
                    Value::Boolean(false),
                ],
                vec![
                    Value::String("world".into()),
                    Value::Int64(43),
                    Value::Boolean(true),
                ],
                vec![Value::Null, Value::Int64(43), Value::Boolean(true)],
            ],
        );
    }
}
