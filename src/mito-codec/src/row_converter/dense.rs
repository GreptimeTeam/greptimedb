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
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;

use crate::error::{
    self, FieldTypeMismatchSnafu, NotSupportedFieldSnafu, Result, SerializeFieldSnafu,
};
use crate::key_values::KeyValue;
use crate::primary_key_filter::DensePrimaryKeyFilter;
use crate::row_converter::{
    CompositeValues, PrimaryKeyCodec, PrimaryKeyCodecExt, PrimaryKeyFilter, encoded_string_len,
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
        let remaining = bytes
            .get(pos..)
            .ok_or(memcomparable::Error::Eof)
            .context(error::DeserializeFieldSnafu)?;
        let len = Self::encoded_len(self.encode_data_type(), remaining)?;
        if len > 1 && self.encode_data_type().is_boolean() && remaining[1] > 1 {
            return Err(memcomparable::Error::InvalidBoolEncoding(remaining[1]))
                .context(error::DeserializeFieldSnafu);
        }
        deserializer.advance(len);
        Ok(len)
    }

    /// Checks field boundaries before any unchecked reads in memcomparable.
    fn encoded_len(data_type: &ConcreteDataType, bytes: &[u8]) -> Result<usize> {
        let marker = bytes
            .first()
            .copied()
            .ok_or(memcomparable::Error::Eof)
            .context(error::DeserializeFieldSnafu)?;
        match marker {
            0 => return Ok(1),
            1 => {}
            value => {
                return Err(memcomparable::Error::InvalidTagEncoding(value as usize))
                    .context(error::DeserializeFieldSnafu);
            }
        }
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
                // Binary is encoded as a sequence of bytes, not chunked strings.
                return encoded_binary_len(bytes).context(error::DeserializeFieldSnafu);
            }
            ConcreteDataType::String(_) => {
                return encoded_string_len(bytes).context(error::DeserializeFieldSnafu);
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
            | ConcreteDataType::Dictionary(_) => {
                return NotSupportedFieldSnafu {
                    data_type: data_type.clone(),
                }
                .fail();
            }
        };
        if bytes.len() < to_skip {
            return Err(memcomparable::Error::Eof).context(error::DeserializeFieldSnafu);
        }
        Ok(to_skip)
    }

    /// Decodes primitive fields straight into a reference value for column
    /// builders, avoiding owned Value construction and conversion. The caller has
    /// already checked the encoded boundary.
    fn deserialize_primitive_ref<B: Buf>(
        data_type: &ConcreteDataType,
        deserializer: &mut Deserializer<B>,
    ) -> Option<Result<ValueRef<'static>>> {
        macro_rules! decode_primitive {
            ($($variant:ident, $native:ty),* $(,)?) => {
                match data_type {
                    $(ConcreteDataType::$variant(_) => Some(
                        Option::<$native>::deserialize(deserializer)
                            .map(ValueRef::from)
                            .context(error::DeserializeFieldSnafu)
                    ),)*
                    _ => None,
                }
            };
        }
        decode_primitive!(
            Boolean, bool, Int8, i8, Int16, i16, Int32, i32, Int64, i64, UInt8, u8, UInt16, u16,
            UInt32, u32, UInt64, u64, Float32, f32, Float64, f64,
        )
    }
}

/// The Option marker has already been checked by SortField.
fn encoded_binary_len(bytes: &[u8]) -> memcomparable::Result<usize> {
    let mut current = 1;
    loop {
        match bytes.get(current).copied() {
            Some(0) => return Ok(current + 1),
            Some(1) if bytes.get(current + 1).is_some() => current += 2,
            Some(1) | None => return Err(memcomparable::Error::Eof),
            Some(marker) => return Err(memcomparable::Error::InvalidSeqEncoding(marker)),
        }
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

    /// Counts complete fields in a key encoded with a prefix of this codec's schema.
    ///
    /// Dense keys contain neither column ids nor types: callers must ensure that
    /// existing fields have the same order and types. Only EOF between fields is
    /// accepted; truncated fields and bytes beyond the schema return an error.
    /// Field boundaries are checked in all builds; full value validation is only
    /// enabled in debug builds and this crate's unit tests.
    pub fn decode_prefix_len(&self, bytes: &[u8]) -> Result<usize> {
        let mut deserializer = Deserializer::new(bytes);
        for (index, (_, field)) in self.ordered_primary_key_columns.iter().enumerate() {
            if !deserializer.has_remaining() {
                return Ok(index);
            }
            let start = deserializer.position();
            // Preserve the prefix error contract used by primary-key range mapping.
            let len = SortField::encoded_len(field.encode_data_type(), &bytes[start..]).map_err(
                |source| match source {
                    error::Error::DeserializeField { .. } => error::InvalidDensePrimaryKeySnafu {
                        reason: "truncated field or invalid encoding",
                    }
                    .build(),
                    source => source,
                },
            )?;
            // Production range mapping only needs boundaries, not allocated field values.
            #[cfg(any(debug_assertions, test))]
            field.deserialize(&mut Deserializer::new(&bytes[start..start + len]))?;
            deserializer.advance(len);
        }
        snafu::ensure!(
            !deserializer.has_remaining(),
            error::InvalidDensePrimaryKeySnafu {
                reason: "key contains bytes beyond the primary key schema",
            }
        );
        Ok(self.num_fields())
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

    /// Iterates over all source PK fields in schema order, checking each field's
    /// encoded boundary before deserializing it. Stops after the first error.
    ///
    /// Unlike positional access, this requires no offsets or value cache. Callers
    /// can consume each value immediately instead of retaining the entire key.
    pub fn decode_dense_iter<'a>(
        &'a self,
        bytes: &'a [u8],
    ) -> impl Iterator<Item = Result<(ColumnId, Value)>> + 'a {
        self.ordered_primary_key_columns.iter().scan(
            Some(Deserializer::new(bytes)),
            move |state, (id, field)| {
                let deserializer = state.as_mut()?;
                let remaining = &bytes[deserializer.position()..];
                let decoded = SortField::encoded_len(field.encode_data_type(), remaining)
                    .and_then(|_| field.deserialize(deserializer).map(|value| (*id, value)));
                if decoded.is_err() {
                    *state = None;
                }
                Some(decoded)
            },
        )
    }

    /// Returns the column ids and sort fields in encoded primary-key order.
    pub fn fields(&self) -> &[(ColumnId, SortField)] {
        &self.ordered_primary_key_columns
    }

    /// Decodes all fields in source order into a consumer, checking boundaries.
    /// Strings borrow the reusable buffer for the duration of each callback, so
    /// materializing them into column builders needs no per-value owned string.
    pub fn decode_dense_with(
        &self,
        bytes: &[u8],
        value_buf: &mut Vec<u8>,
        mut consume: impl FnMut(usize, ValueRef<'_>),
    ) -> Result<()> {
        let mut deserializer = Deserializer::new(bytes);
        for (pos, (_, field)) in self.ordered_primary_key_columns.iter().enumerate() {
            let data_type = field.encode_data_type();
            let remaining = &bytes[deserializer.position()..];
            SortField::encoded_len(data_type, remaining)?;
            if data_type.is_string() && remaining[0] != 0 {
                deserializer.advance(1);
                deserializer
                    .read_bytes_into(value_buf)
                    .context(error::DeserializeFieldSnafu)?;
                let value = std::str::from_utf8(value_buf).map_err(|err| {
                    error::InvalidDensePrimaryKeySnafu {
                        reason: format!("string is not valid UTF-8: {err}"),
                    }
                    .build()
                })?;
                consume(pos, ValueRef::String(value));
            } else if let Some(value) =
                SortField::deserialize_primitive_ref(data_type, &mut deserializer)
            {
                consume(pos, value?);
            } else {
                let value = field.deserialize(&mut deserializer)?;
                consume(pos, value.as_value_ref());
            }
        }
        Ok(())
    }

    /// Returns the field at `pos`.
    ///
    /// # Panics
    /// Panics if `pos` is out of bounds.
    fn field_at(&self, pos: usize) -> &SortField {
        &self.ordered_primary_key_columns[pos].1
    }

    /// Advances `deserializer` to the start of value at `pos`.
    ///
    /// Returns the offset of the value at `pos` in `bytes`.
    fn advance_to_value_at(
        &self,
        bytes: &[u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
        deserializer: &mut Deserializer<&[u8]>,
    ) -> Result<usize> {
        ensure!(
            pos < self.num_fields(),
            error::InvalidDensePrimaryKeySnafu {
                reason: format!(
                    "field position {pos} exceeds field count {}",
                    self.num_fields()
                ),
            }
        );
        if offsets_buf.is_empty() {
            // The schema bounds this cache. Reserve once even when the first
            // requested field is near the end of a wide key.
            offsets_buf.reserve_exact(self.num_fields());
            offsets_buf.push(0);
        }
        // Start at the requested field if cached, otherwise resume discovery
        // from the furthest known boundary.
        let value_start = pos.min(offsets_buf.len() - 1);
        let mut offset = offsets_buf[value_start];
        ensure!(
            offset <= bytes.len(),
            error::InvalidDensePrimaryKeySnafu {
                reason: "cached offset exceeds key length",
            }
        );
        deserializer.advance(offset);
        for i in value_start..pos {
            offset += self.field_at(i).skip_deserialize(bytes, deserializer)?;
            offsets_buf.push(offset);
        }
        Ok(offset)
    }

    /// Decode value at `pos` in `bytes`.
    ///
    /// The i-th element in the offsets buffer is the start of field i. The buffer
    /// must only be reused for the same encoded key and codec.
    #[inline]
    pub fn decode_value_at(
        &self,
        bytes: &[u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
    ) -> Result<Value> {
        let encoded = self.encoded_value_at(bytes, pos, offsets_buf)?;
        self.field_at(pos)
            .deserialize(&mut Deserializer::new(encoded))
    }

    /// Returns the encoded bytes at `pos` in `bytes`.
    ///
    /// The i-th element in the offsets buffer is the start of field i. The buffer
    /// must only be reused for the same encoded key and codec. Validates the
    /// framing up to this field, without validating or decoding later fields.
    #[inline]
    pub fn encoded_value_at<'a>(
        &self,
        bytes: &'a [u8],
        pos: usize,
        offsets_buf: &mut Vec<usize>,
    ) -> Result<&'a [u8]> {
        let mut deserializer = Deserializer::new(bytes);
        let offset = self.advance_to_value_at(bytes, pos, offsets_buf, &mut deserializer)?;

        let len = self
            .field_at(pos)
            .skip_deserialize(bytes, &mut deserializer)?;
        // We already found the next field's start while validating this one.
        // Reuse it instead of scanning this field again on the next lookup.
        if offsets_buf.len() == pos + 1 && pos + 1 < self.num_fields() {
            offsets_buf.push(offset + len);
        }
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

    fn as_dense(&self) -> Option<&DensePrimaryKeyCodec> {
        Some(self)
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
        let boundaries: Vec<_> = (0..=row.len())
            .map(|count| {
                encoder
                    .encode(value_ref[..count].iter().cloned())
                    .unwrap()
                    .len()
            })
            .collect();
        for end in 0..=result.len() {
            match boundaries.iter().position(|boundary| *boundary == end) {
                Some(count) => {
                    assert_eq!(count, encoder.decode_prefix_len(&result[..end]).unwrap())
                }
                None => assert!(
                    encoder.decode_prefix_len(&result[..end]).is_err(),
                    "truncated {data_types:?} at {end}"
                ),
            }
        }
        let decoded = encoder.decode(&result).unwrap().into_dense();
        assert_eq!(decoded, row);
        let mut value_buf = Vec::new();
        let mut borrowed = Vec::new();
        encoder
            .decode_dense_with(&result, &mut value_buf, |_, value| {
                borrowed.push(Value::from(value))
            })
            .unwrap();
        assert_eq!(borrowed, row);
        let sequential = encoder
            .decode_dense_iter(&result)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            sequential
                .iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>(),
            row.iter().collect::<Vec<_>>()
        );
        for end in 0..result.len() {
            assert!(
                encoder
                    .decode_dense_with(&result[..end], &mut value_buf, |_, _| {})
                    .is_err()
            );
            assert!(
                encoder
                    .decode_dense_iter(&result[..end])
                    .collect::<Result<Vec<_>>>()
                    .is_err(),
                "truncated at {end}"
            );
        }
        // Every supported type must have the same boundaries in positional and
        // full decoding. Exercise every truncation both at the requested field
        // and while skipping it to reach a later field.
        let mut offsets = Vec::new();
        let mut end = 0;
        for (pos, value) in row.iter().enumerate() {
            let encoded = encoder
                .encoded_value_at(&result, pos, &mut offsets)
                .unwrap();
            assert_eq!(
                encoder
                    .field_at(pos)
                    .deserialize(&mut Deserializer::new(encoded))
                    .unwrap(),
                *value
            );
            let start = end;
            end += encoded.len();
            for len in start..end {
                assert!(
                    encoder
                        .encoded_value_at(&result[..len], pos, &mut Vec::new())
                        .is_err()
                );
                assert!(
                    encoder
                        .decode_value_at(&result[..len], pos, &mut Vec::new())
                        .is_err()
                );
                if pos + 1 < row.len() {
                    assert!(
                        encoder
                            .encoded_value_at(&result[..len], pos + 1, &mut Vec::new())
                            .is_err()
                    );
                }
            }
        }
        assert_eq!(end, result.len());
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
    fn test_decode_prefix_len_accepts_only_complete_fields() {
        let codec = DensePrimaryKeyCodec::with_fields(vec![
            (0, SortField::new(ConcreteDataType::string_datatype())),
            (1, SortField::new(ConcreteDataType::int64_datatype())),
            (2, SortField::new(ConcreteDataType::binary_datatype())),
            (3, SortField::new(ConcreteDataType::string_datatype())),
        ]);
        let values = [
            Value::from("abcdefghijk"),
            Value::Int64(42),
            Value::Binary(vec![0, 255, 1].into()),
            Value::Null,
        ];
        let mut boundaries = vec![0];
        let mut encoded = Vec::new();
        for count in 1..=values.len() {
            encoded.clear();
            codec
                .encode_dense(
                    values[..count].iter().map(Value::as_value_ref),
                    &mut encoded,
                )
                .unwrap();
            boundaries.push(encoded.len());
            assert_eq!(count, codec.decode_prefix_len(&encoded).unwrap());
        }
        for end in 0..=encoded.len() {
            match boundaries.iter().position(|boundary| *boundary == end) {
                Some(count) => assert_eq!(count, codec.decode_prefix_len(&encoded[..end]).unwrap()),
                None => assert!(
                    codec.decode_prefix_len(&encoded[..end]).is_err(),
                    "truncated at {end}"
                ),
            }
        }
        encoded.push(0);
        assert!(codec.decode_prefix_len(&encoded).is_err());
        assert!(codec.decode_prefix_len(&[2]).is_err());
        let empty = DensePrimaryKeyCodec::with_fields(vec![]);
        assert_eq!(0, empty.decode_prefix_len(&[]).unwrap());
        assert!(empty.decode_prefix_len(&[0]).is_err());
    }

    #[test]
    fn test_prefix_len_validates_values_in_unit_tests() {
        let codec = DensePrimaryKeyCodec::with_fields(vec![(
            0,
            SortField::new(ConcreteDataType::boolean_datatype()),
        )]);
        // The field is complete, but 2 is not a boolean. Even release unit tests
        // retain value validation through cfg(test).
        assert!(codec.decode_prefix_len(&[1, 2]).is_err());
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
    fn test_encoded_value_at() {
        let data_types = [
            ConcreteDataType::string_datatype(),
            ConcreteDataType::int32_datatype(),
            ConcreteDataType::string_datatype(),
        ];
        let encoder = DensePrimaryKeyCodec::with_fields(
            data_types
                .iter()
                .enumerate()
                .map(|(idx, t)| (idx as ColumnId, SortField::new(t.clone())))
                .collect::<Vec<_>>(),
        );
        let row = [Value::String("hello".into()), Value::Int32(42), Value::Null];
        let value_ref = row.iter().map(|v| v.as_value_ref()).collect::<Vec<_>>();
        let encoded_pk = encoder.encode(value_ref.iter().cloned()).unwrap();

        let mut offsets = Vec::new();
        let mut combined = Vec::new();
        for (pos, value) in row.iter().enumerate() {
            let encoded_value = encoder
                .encoded_value_at(&encoded_pk, pos, &mut offsets)
                .unwrap();
            combined.extend_from_slice(encoded_value);

            let field = SortField::new(data_types[pos].clone());
            let mut expected = Vec::new();
            let mut serializer = Serializer::new(&mut expected);
            field
                .serialize(&mut serializer, &value.as_value_ref())
                .unwrap();
            assert_eq!(encoded_value, expected.as_slice());
        }
        assert_eq!(combined, encoded_pk);
        assert_eq!(offsets.len(), row.len());

        // Verify the offsets buffer can be reused for random access.
        for (pos, value) in row.iter().enumerate().rev() {
            let encoded_value = encoder
                .encoded_value_at(&encoded_pk, pos, &mut offsets)
                .unwrap();

            let field = SortField::new(data_types[pos].clone());
            let mut expected = Vec::new();
            let mut serializer = Serializer::new(&mut expected);
            field
                .serialize(&mut serializer, &value.as_value_ref())
                .unwrap();
            assert_eq!(encoded_value, expected.as_slice());
        }
    }

    #[test]
    fn test_positional_decode_invalid_encoding() {
        for (ty, bytes) in [
            (ConcreteDataType::int64_datatype(), vec![2]),
            (ConcreteDataType::boolean_datatype(), vec![1, 2]),
            (ConcreteDataType::binary_datatype(), vec![1, 2]),
            (ConcreteDataType::binary_datatype(), vec![1, 1, 42, 2]),
            (ConcreteDataType::string_datatype(), vec![1, 2]),
            (
                ConcreteDataType::string_datatype(),
                vec![1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                ConcreteDataType::string_datatype(),
                vec![1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 10],
            ),
        ] {
            let codec = DensePrimaryKeyCodec::with_fields(vec![
                (0, SortField::new(ty)),
                (1, SortField::new(ConcreteDataType::int64_datatype())),
            ]);
            for pos in [0, 1, 2, usize::MAX] {
                assert!(
                    codec
                        .encoded_value_at(&bytes, pos, &mut Vec::new())
                        .is_err()
                );
                assert!(codec.decode_value_at(&bytes, pos, &mut Vec::new()).is_err());
            }
            assert!(
                codec
                    .decode_dense_iter(&bytes)
                    .collect::<Result<Vec<_>>>()
                    .is_err()
            );
            assert!(
                codec
                    .decode_dense_with(&bytes, &mut Vec::new(), |_, _| {})
                    .is_err()
            );
        }
        let codec = DensePrimaryKeyCodec::with_fields(vec![
            (0, SortField::new(ConcreteDataType::string_datatype())),
            (1, SortField::new(ConcreteDataType::int64_datatype())),
        ]);
        let bytes = codec
            .encode([ValueRef::String("abcdefghijk"), ValueRef::Int64(42)].into_iter())
            .unwrap();
        for pos in [2, usize::MAX] {
            assert!(
                codec
                    .encoded_value_at(&bytes, pos, &mut Vec::new())
                    .is_err()
            );
            assert!(codec.decode_value_at(&bytes, pos, &mut Vec::new()).is_err());
        }
        for offset in [bytes.len(), bytes.len() + 1, usize::MAX] {
            for pos in [0, 1] {
                assert!(
                    codec
                        .encoded_value_at(&bytes, pos, &mut vec![offset])
                        .is_err()
                );
                assert!(
                    codec
                        .decode_value_at(&bytes, pos, &mut vec![offset])
                        .is_err()
                );
            }
        }
        let mut invalid_utf8 = bytes;
        invalid_utf8[2] = 0xff;
        assert!(
            codec
                .decode_dense_with(&invalid_utf8, &mut Vec::new(), |_, _| {})
                .is_err()
        );
        assert!(
            codec
                .decode_dense_iter(&invalid_utf8)
                .collect::<Result<Vec<_>>>()
                .is_err()
        );
        assert!(
            codec
                .decode_value_at(&invalid_utf8, 0, &mut Vec::new())
                .is_err()
        );
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
