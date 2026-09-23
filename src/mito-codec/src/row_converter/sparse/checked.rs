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
use snafu::{OptionExt, ensure};
use store_api::storage::ColumnId;

use crate::error::{InvalidSparsePrimaryKeySnafu, Result};
use crate::row_converter::encoded_string_len;
use crate::row_converter::sparse::{
    COLUMN_ID_ENCODE_SIZE, RESERVED_COLUMN_ID_TABLE_ID, RESERVED_COLUMN_ID_TSID,
    SparseOffsetsCache, TABLE_ID_VALUE_OFFSET, TAGS_START_OFFSET, TSID_VALUE_OFFSET,
};

/// A borrowing view of one sparse primary key, with checked, lazy field lookup.
///
/// The scratch cache is reset on construction and retains its capacity between keys.
/// Ordinary labels can be skipped without schema lookup: their encoding is always String.
pub struct SparsePrimaryKeyView<'a, 'b> {
    pk: &'a [u8],
    cache: &'b mut SparseOffsetsCache,
}

impl<'a, 'b> SparsePrimaryKeyView<'a, 'b> {
    /// Validates the fixed prefix and binds fresh offset discovery to this key.
    pub fn new(pk: &'a [u8], cache: &'b mut SparseOffsetsCache) -> Result<Self> {
        ensure!(
            pk.len() >= TAGS_START_OFFSET
                && pk[..COLUMN_ID_ENCODE_SIZE] == RESERVED_COLUMN_ID_TABLE_ID.to_be_bytes()
                && pk[TSID_VALUE_OFFSET - COLUMN_ID_ENCODE_SIZE..TSID_VALUE_OFFSET]
                    == RESERVED_COLUMN_ID_TSID.to_be_bytes()
                && pk[TABLE_ID_VALUE_OFFSET] == 1
                && pk[TSID_VALUE_OFFSET] == 1,
            InvalidSparsePrimaryKeySnafu {
                reason: "invalid table_id/tsid prefix"
            }
        );
        cache.clear();
        Ok(Self { pk, cache })
    }

    /// Returns the encoded value, including its null marker, or None for an absent label.
    /// The result borrows the key, not the scratch cache.
    pub fn encoded_value(&mut self, column_id: ColumnId) -> Result<Option<&'a [u8]>> {
        match column_id {
            RESERVED_COLUMN_ID_TABLE_ID => {
                return Ok(Some(
                    &self.pk[TABLE_ID_VALUE_OFFSET..TSID_VALUE_OFFSET - COLUMN_ID_ENCODE_SIZE],
                ));
            }
            RESERVED_COLUMN_ID_TSID => {
                return Ok(Some(&self.pk[TSID_VALUE_OFFSET..TAGS_START_OFFSET]));
            }
            _ => {}
        }
        if let Some(offset) = self.cache.get(column_id) {
            return encoded_label(&self.pk[offset..]).map(Some);
        }
        while !self.cache.finished && self.cache.cursor < self.pk.len() {
            let bytes = &self.pk[self.cache.cursor..];
            let id = bytes.first_chunk::<COLUMN_ID_ENCODE_SIZE>().context(
                InvalidSparsePrimaryKeySnafu {
                    reason: "truncated label column id",
                },
            )?;
            let id = u32::from_be_bytes(*id);
            let offset = self.cache.cursor + COLUMN_ID_ENCODE_SIZE;
            let value = encoded_label(&self.pk[offset..])?;
            self.cache.insert(id, offset);
            self.cache.cursor = offset + value.len();
            if id == column_id {
                return Ok(Some(value));
            }
        }
        self.cache.finished = true;
        Ok(None)
    }

    /// Extracts a UTF-8 label into a reusable buffer, validating it once.
    /// Missing and null labels return `None` without modifying the buffer;
    /// an empty label returns `Some("")`. Reserved numeric columns are rejected.
    /// The returned string borrows `buffer`, whose contents are replaced for each label.
    pub fn label<'buf>(
        &mut self,
        column_id: ColumnId,
        buffer: &'buf mut Vec<u8>,
    ) -> Result<Option<&'buf str>> {
        ensure!(
            !matches!(
                column_id,
                RESERVED_COLUMN_ID_TABLE_ID | RESERVED_COLUMN_ID_TSID
            ),
            InvalidSparsePrimaryKeySnafu {
                reason: "reserved column is not a string label"
            }
        );
        let Some(encoded) = self.encoded_value(column_id)? else {
            return Ok(None);
        };
        if encoded[0] == 0 {
            return Ok(None);
        }

        buffer.clear();
        // encoded_value checked the markers and every chunk length. Reserve once
        // before unchunking so the buffer does not grow for each 8-byte chunk.
        buffer.reserve(encoded.len() - 2);
        for chunk in encoded[2..].chunks_exact(9) {
            let len = usize::from(chunk[8]).min(8);
            buffer.extend_from_slice(&chunk[..len]);
        }
        std::str::from_utf8(buffer).map(Some).map_err(|_| {
            InvalidSparsePrimaryKeySnafu {
                reason: "label is not valid UTF-8",
            }
            .build()
        })
    }

    /// Returns the table id from the non-null prefix validated by [`Self::new`].
    pub fn table_id(&self) -> u32 {
        (&self.pk[TABLE_ID_VALUE_OFFSET + 1..]).get_u32()
    }

    /// Returns the series id from the non-null prefix validated by [`Self::new`].
    pub fn tsid(&self) -> u64 {
        (&self.pk[TSID_VALUE_OFFSET + 1..]).get_u64()
    }
}

/// Finds the end of an Option<String> without allocating or reading its payload.
/// Unlike memcomparable's skip_bytes, all advances are checked for truncated input.
fn encoded_label(bytes: &[u8]) -> Result<&[u8]> {
    let len = encoded_string_len(bytes).map_err(|error| {
        InvalidSparsePrimaryKeySnafu {
            reason: error.to_string(),
        }
        .build()
    })?;
    Ok(&bytes[..len])
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::value::ValueRef;

    use super::*;
    use crate::index::IndexValueCodec;
    use crate::row_converter::{PrimaryKeyCodec, SortField, SparsePrimaryKeyCodec};

    #[test]
    fn index_bytes_match_full_decode_across_chunks_and_lookup_orders() {
        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut cache = SparseOffsetsCache::new();
        let mut buffer = Vec::new();
        // Reusing the cache across different keys must not reuse their field offsets.
        for len in [0, 1, 7, 8, 9, 16, 24, 65] {
            let label = "x".repeat(len);
            let values = [
                (RESERVED_COLUMN_ID_TABLE_ID, ValueRef::UInt32(42)),
                (RESERVED_COLUMN_ID_TSID, ValueRef::UInt64(u64::MAX)),
                (90, ValueRef::String(&label)),
                (7, ValueRef::String("")),
                (1, ValueRef::Null),
                (3, ValueRef::String("last中文")),
            ];
            let mut pk = Vec::new();
            codec.encode_value_refs(&values, &mut pk).unwrap();
            // Exercise the offset cache's overflow storage as well as its inline entries.
            codec
                .encode_raw_tag_value((100..140).map(|id| (id, label.as_bytes())), &mut pk)
                .unwrap();
            // Writers normally omit null labels; existing encoded nulls must remain null too.
            pk.extend_from_slice(&2_u32.to_be_bytes());
            pk.push(0);
            let decoded = codec.decode(&pk).unwrap().into_sparse();
            for order in [
                [
                    RESERVED_COLUMN_ID_TABLE_ID,
                    RESERVED_COLUMN_ID_TSID,
                    90,
                    7,
                    1,
                    3,
                ],
                [
                    3,
                    1,
                    7,
                    90,
                    RESERVED_COLUMN_ID_TSID,
                    RESERVED_COLUMN_ID_TABLE_ID,
                ],
            ] {
                let mut view = SparsePrimaryKeyView::new(&pk, &mut cache).unwrap();
                for id in order.into_iter().chain([139, 100, 132, 2]) {
                    let expected = decoded.get(&id).filter(|v| !v.is_null()).map(|value| {
                        let field = SortField::new(match id {
                            RESERVED_COLUMN_ID_TABLE_ID => ConcreteDataType::uint32_datatype(),
                            RESERVED_COLUMN_ID_TSID => ConcreteDataType::uint64_datatype(),
                            _ => ConcreteDataType::string_datatype(),
                        });
                        let mut bytes = Vec::new();
                        IndexValueCodec::encode_nonnull_value(
                            value.as_value_ref(),
                            &field,
                            &mut bytes,
                        )
                        .unwrap();
                        bytes
                    });
                    let actual =
                        IndexValueCodec::encode_sparse_value(&mut view, id, &mut buffer).unwrap();
                    assert_eq!(actual, expected.as_deref(), "len={len}, column={id}");
                }
                assert!(view.encoded_value(999).unwrap().is_none());
            }
        }
    }

    #[test]
    fn reserved_ids_preserve_unsigned_values() {
        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut cache = SparseOffsetsCache::new();
        for (table_id, tsid) in [(0, 0), (42, u64::MAX), (u32::MAX, 42)] {
            let mut pk = Vec::new();
            codec.encode_internal(table_id, tsid, &mut pk).unwrap();
            let view = SparsePrimaryKeyView::new(&pk, &mut cache).unwrap();
            assert_eq!(view.table_id(), table_id);
            assert_eq!(view.tsid(), tsid);
        }
    }

    #[test]
    fn malformed_sparse_values_return_errors() {
        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut pk = Vec::new();
        codec.encode_internal(1, 2, &mut pk).unwrap();
        codec
            .encode_raw_tag_value([(1, b"0123456789".as_slice())].into_iter(), &mut pk)
            .unwrap();
        let mut cache = SparseOffsetsCache::new();
        for end in 0..pk.len() {
            // A complete reserved prefix is a valid key with no labels.
            if end == TAGS_START_OFFSET {
                continue;
            }
            let result = SparsePrimaryKeyView::new(&pk[..end], &mut cache)
                .and_then(|mut view| view.encoded_value(1));
            assert!(result.is_err(), "truncation at {end}");
        }
        for (offset, value) in [(0, 0), (4, 0), (13, 2), (26, 2), (27, 2), (36, 0), (45, 10)] {
            let mut invalid = pk.clone();
            invalid[offset] = value;
            assert!(
                SparsePrimaryKeyView::new(&invalid, &mut cache)
                    .and_then(|mut view| view.encoded_value(1))
                    .is_err()
            );
        }
        let mut invalid_utf8 = pk;
        invalid_utf8[28] = 0xff;
        let mut view = SparsePrimaryKeyView::new(&invalid_utf8, &mut cache).unwrap();
        assert!(IndexValueCodec::encode_sparse_value(&mut view, 1, &mut Vec::new()).is_err());
    }

    #[test]
    fn label_reuses_buffer_across_null_empty_invalid_and_valid_values() {
        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut cache = SparseOffsetsCache::new();
        let mut buffer = Vec::with_capacity(256);
        let ptr = buffer.as_ptr();
        for label in [
            b"1234567\xe4\xb8\xad\0\xe6\x96\x87".as_slice(),
            b"",
            b"\xff",
            b"after-error",
        ] {
            let mut pk = Vec::new();
            codec.encode_internal(42, 7, &mut pk).unwrap();
            codec
                .encode_raw_tag_value([(1, label)].into_iter(), &mut pk)
                .unwrap();
            pk.extend_from_slice(&2_u32.to_be_bytes());
            pk.push(0);
            let mut view = SparsePrimaryKeyView::new(&pk, &mut cache).unwrap();
            let actual = view.label(1, &mut buffer);
            match std::str::from_utf8(label) {
                Ok(expected) => assert_eq!(actual.unwrap(), Some(expected)),
                Err(_) => assert!(actual.is_err()),
            }
            assert_eq!(view.label(2, &mut buffer).unwrap(), None);
            assert_eq!(view.label(3, &mut buffer).unwrap(), None);
            // Null/missing values do not overwrite the scratch buffer, matching index extraction.
            assert_eq!(buffer, label);
            assert_eq!(buffer.as_ptr(), ptr);
            assert!(
                view.label(RESERVED_COLUMN_ID_TABLE_ID, &mut buffer)
                    .is_err()
            );
            assert!(view.label(RESERVED_COLUMN_ID_TSID, &mut buffer).is_err());
        }
    }
}
