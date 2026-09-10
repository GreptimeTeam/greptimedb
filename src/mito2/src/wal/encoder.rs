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

//! A cached-size, single-pass encoder for [`WalEntry`].
//!
//! # Why
//!
//! `prost` does not cache message sizes the way protobuf-C++ does. When
//! `WalEntry::encode_to_vec` runs, the encode pass recomputes the
//! `encoded_len` of every nested message each time it needs a length
//! delimiter. For the deep WAL tree
//! (`WalEntry → Mutation → Rows → Row → Value`) the length of a leaf `Value`
//! ends up being recomputed roughly once per ancestor level — i.e. ~5 times.
//! Microbenchmarks show ~87% of `encode_to_vec` is spent in these repeated
//! length walks, not in writing bytes.
//!
//! # How
//!
//! This encoder walks the tree once to compute and cache the body length of
//! every length-delimited message node (in pre-order into a flat `sizes`
//! vector), then walks it a second time to write bytes, reading each cached
//! length back via a cursor. Every node's length is computed exactly once.
//!
//! Leaf messages whose sizing is not recursively redundant (`ColumnSchema`,
//! `Value`, `WriteHint`, `BulkWalEntry`) are delegated to prost's own
//! `encoded_len`/`encode_raw`, but their (single) computed length is still
//! cached so the encode pass never recomputes it.
//!
//! The output is byte-for-byte identical to `WalEntry::encode_to_vec`; this is
//! asserted in tests and must hold to preserve WAL replay compatibility.
//!
//! # Maintenance
//!
//! This encoder hard-codes the wire layout (field tags and field order) of
//! `WalEntry`, `Mutation`, `Rows` and `Row`. If any of these messages change in
//! greptime-proto, this file MUST be updated to match:
//! - **Adding or removing a field** is caught at compile time: every one of
//!   these messages is destructured exhaustively (no `..`), so a changed field
//!   set fails to compile here.
//! - **Changing a field's tag number or type** is caught by the byte-for-byte
//!   equality tests against prost (which populate all fields). Keep those tests
//!   exhaustive when adding fields.
//!
//! Leaf messages are delegated to prost, so changes to them need no update here.

use api::v1::{Mutation, Row, Rows, Value, WalEntry};
use prost::Message;
use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint, key_len};

// Field tags (must match greptime-proto definitions exactly).
const MUTATION_TAG: u32 = 1; // WalEntry.mutations
const BULK_ENTRY_TAG: u32 = 2; // WalEntry.bulk_entries
const OP_TYPE_TAG: u32 = 1; // Mutation.op_type
const SEQUENCE_TAG: u32 = 2; // Mutation.sequence
const ROWS_TAG: u32 = 3; // Mutation.rows
const WRITE_HINT_TAG: u32 = 4; // Mutation.write_hint
const SCHEMA_TAG: u32 = 1; // Rows.schema
const ROW_TAG: u32 = 2; // Rows.rows
const VALUE_TAG: u32 = 1; // Row.values

/// Length contribution of a length-delimited message field:
/// key + length varint + body.
#[inline]
fn msg_field_len(tag: u32, body_len: usize) -> usize {
    key_len(tag) + encoded_len_varint(body_len as u64) + body_len
}

/// A reusable encoder that caches message body sizes between its size pass and
/// its encode pass.
#[derive(Default)]
pub struct WalEntryEncoder {
    /// Cached body lengths of message nodes, in pre-order.
    sizes: Vec<usize>,
}

impl WalEntryEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Encodes `entry` to a new `Vec<u8>`, byte-for-byte identical to
    /// `entry.encode_to_vec()`.
    pub fn encode_to_vec(&mut self, entry: &WalEntry) -> Vec<u8> {
        self.sizes.clear();
        let body_len = self.size_entry(entry);
        let mut buf = Vec::with_capacity(body_len);
        let mut cursor = 0;
        self.encode_entry(entry, &mut buf, &mut cursor);
        // Invariants of the two-pass design. Kept as `debug_assert` to avoid any
        // overhead on the hot write path; correctness is covered by the
        // byte-for-byte equality tests against prost.
        debug_assert_eq!(
            cursor,
            self.sizes.len(),
            "cursor did not consume all cached sizes"
        );
        debug_assert_eq!(buf.len(), body_len, "encoded length mismatch");
        buf
    }

    /// Reserves a size slot in pre-order and returns its index.
    #[inline]
    fn reserve(&mut self) -> usize {
        let idx = self.sizes.len();
        self.sizes.push(0);
        idx
    }

    // ---- size pass ----------------------------------------------------------

    /// Returns the body length of the `WalEntry` (no length delimiter; it is
    /// the root). Pushes cached slots for all nested message nodes.
    fn size_entry(&mut self, entry: &WalEntry) -> usize {
        // Exhaustive destructure (no `..`): adding a field to `WalEntry` in
        // greptime-proto makes this fail to compile, forcing this encoder to be
        // updated rather than silently dropping the new field from the WAL.
        let WalEntry {
            mutations,
            bulk_entries,
        } = entry;
        let mut body = 0;
        for m in mutations {
            let mb = self.size_mutation(m);
            body += msg_field_len(MUTATION_TAG, mb);
        }
        for be in bulk_entries {
            // Leaf: delegate sizing to prost, cache the result.
            let bl = be.encoded_len();
            let slot = self.reserve();
            self.sizes[slot] = bl;
            body += msg_field_len(BULK_ENTRY_TAG, bl);
        }
        body
    }

    fn size_mutation(&mut self, m: &Mutation) -> usize {
        // Exhaustive destructure: see note in `size_entry`.
        let Mutation {
            op_type,
            sequence,
            rows,
            write_hint,
        } = m;
        let slot = self.reserve();
        let mut body = 0;
        // proto3 scalars are only encoded when non-default.
        if *op_type != 0 {
            body += key_len(OP_TYPE_TAG) + encoded_len_varint(*op_type as u64);
        }
        if *sequence != 0 {
            body += key_len(SEQUENCE_TAG) + encoded_len_varint(*sequence);
        }
        if let Some(rows) = rows {
            let rb = self.size_rows(rows);
            body += msg_field_len(ROWS_TAG, rb);
        }
        if let Some(hint) = write_hint {
            let hl = hint.encoded_len();
            let s = self.reserve();
            self.sizes[s] = hl;
            body += msg_field_len(WRITE_HINT_TAG, hl);
        }
        self.sizes[slot] = body;
        body
    }

    fn size_rows(&mut self, rows: &Rows) -> usize {
        // Exhaustive destructure: see note in `size_entry`.
        let Rows { schema, rows } = rows;
        let slot = self.reserve();
        let mut body = 0;
        for col in schema {
            let cl = col.encoded_len();
            let s = self.reserve();
            self.sizes[s] = cl;
            body += msg_field_len(SCHEMA_TAG, cl);
        }
        for row in rows {
            let rb = self.size_row(row);
            body += msg_field_len(ROW_TAG, rb);
        }
        self.sizes[slot] = body;
        body
    }

    fn size_row(&mut self, row: &Row) -> usize {
        // Exhaustive destructure: see note in `size_entry`.
        let Row { values } = row;
        let slot = self.reserve();
        let mut body = 0;
        for value in values {
            let vl = self.size_value(value);
            body += msg_field_len(VALUE_TAG, vl);
        }
        self.sizes[slot] = body;
        body
    }

    #[inline]
    fn size_value(&mut self, value: &Value) -> usize {
        // Value is a leaf message (a single oneof); its `encoded_len` is not
        // recursively redundant, but we cache it so the encode pass does not
        // recompute it.
        let vl = value.encoded_len();
        let slot = self.reserve();
        self.sizes[slot] = vl;
        vl
    }

    // ---- encode pass --------------------------------------------------------

    #[inline]
    fn next_size(&self, cursor: &mut usize) -> usize {
        let v = *self
            .sizes
            .get(*cursor)
            .expect("size pass and encode pass disagree: cursor out of bounds (encoder bug)");
        *cursor += 1;
        v
    }

    // Convention: the caller reads a child message's cached body length (via
    // `next_size`, consuming that child's slot in pre-order) and writes the
    // key + length delimiter; the callee then writes only the body.

    fn encode_entry(&self, entry: &WalEntry, buf: &mut Vec<u8>, cursor: &mut usize) {
        // Exhaustive destructure: see note in `size_entry`.
        let WalEntry {
            mutations,
            bulk_entries,
        } = entry;
        for m in mutations {
            let mb = self.next_size(cursor);
            encode_key(MUTATION_TAG, WireType::LengthDelimited, buf);
            encode_varint(mb as u64, buf);
            self.encode_mutation_body(m, buf, cursor);
        }
        for be in bulk_entries {
            let bl = self.next_size(cursor);
            encode_key(BULK_ENTRY_TAG, WireType::LengthDelimited, buf);
            encode_varint(bl as u64, buf);
            be.encode_raw(buf);
        }
    }

    fn encode_mutation_body(&self, m: &Mutation, buf: &mut Vec<u8>, cursor: &mut usize) {
        // Exhaustive destructure: see note in `size_entry`.
        let Mutation {
            op_type,
            sequence,
            rows,
            write_hint,
        } = m;
        if *op_type != 0 {
            encode_key(OP_TYPE_TAG, WireType::Varint, buf);
            encode_varint(*op_type as u64, buf);
        }
        if *sequence != 0 {
            encode_key(SEQUENCE_TAG, WireType::Varint, buf);
            encode_varint(*sequence, buf);
        }
        if let Some(rows) = rows {
            let rb = self.next_size(cursor);
            encode_key(ROWS_TAG, WireType::LengthDelimited, buf);
            encode_varint(rb as u64, buf);
            self.encode_rows_body(rows, buf, cursor);
        }
        if let Some(hint) = write_hint {
            let hl = self.next_size(cursor);
            encode_key(WRITE_HINT_TAG, WireType::LengthDelimited, buf);
            encode_varint(hl as u64, buf);
            hint.encode_raw(buf);
        }
    }

    fn encode_rows_body(&self, rows: &Rows, buf: &mut Vec<u8>, cursor: &mut usize) {
        // Exhaustive destructure: see note in `size_entry`.
        let Rows { schema, rows } = rows;
        for col in schema {
            let cl = self.next_size(cursor);
            encode_key(SCHEMA_TAG, WireType::LengthDelimited, buf);
            encode_varint(cl as u64, buf);
            col.encode_raw(buf);
        }
        for row in rows {
            let rb = self.next_size(cursor);
            encode_key(ROW_TAG, WireType::LengthDelimited, buf);
            encode_varint(rb as u64, buf);
            self.encode_row_body(row, buf, cursor);
        }
    }

    fn encode_row_body(&self, row: &Row, buf: &mut Vec<u8>, cursor: &mut usize) {
        // Exhaustive destructure: see note in `size_entry`.
        let Row { values } = row;
        for value in values {
            let vl = self.next_size(cursor);
            encode_key(VALUE_TAG, WireType::LengthDelimited, buf);
            encode_varint(vl as u64, buf);
            value.encode_raw(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{
        BulkWalEntry, ColumnDataType, ColumnSchema, Mutation, OpType, Row, Rows, SemanticType,
        Value, WalEntry, WriteHint,
    };
    use prost::Message;

    use super::*;

    fn col(name: &str, dt: ColumnDataType, st: SemanticType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            datatype: dt as i32,
            semantic_type: st as i32,
            datatype_extension: None,
            options: None,
        }
    }

    fn sample_rows(num_rows: usize, with_null: bool) -> Rows {
        let schema = vec![
            col("host", ColumnDataType::String, SemanticType::Tag),
            col(
                "ts",
                ColumnDataType::TimestampMillisecond,
                SemanticType::Timestamp,
            ),
            col("v", ColumnDataType::Float64, SemanticType::Field),
        ];
        let rows = (0..num_rows)
            .map(|i| Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::StringValue(format!("h{i}"))),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(i as i64)),
                    },
                    // Exercise the null (None oneof) path.
                    Value {
                        value_data: if with_null && i % 2 == 0 {
                            None
                        } else {
                            Some(ValueData::F64Value(i as f64))
                        },
                    },
                ],
            })
            .collect();
        Rows { schema, rows }
    }

    fn assert_byte_identical(entry: &WalEntry) {
        let expected = entry.encode_to_vec();
        let actual = WalEntryEncoder::new().encode_to_vec(entry);
        assert_eq!(
            expected,
            actual,
            "fast encoder output differs from prost (len {} vs {})",
            expected.len(),
            actual.len()
        );
    }

    #[test]
    fn test_matches_prost_basic() {
        let entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 42,
                rows: Some(sample_rows(5, false)),
                write_hint: None,
            }],
            bulk_entries: vec![],
        };
        assert_byte_identical(&entry);
    }

    #[test]
    fn test_matches_prost_multi_mutation_with_nulls_and_delete() {
        let entry = WalEntry {
            mutations: vec![
                Mutation {
                    op_type: OpType::Put as i32,
                    sequence: 1,
                    rows: Some(sample_rows(3, true)),
                    write_hint: Some(WriteHint {
                        primary_key_encoding: 1,
                    }),
                },
                // op_type Delete == 0: must be skipped as proto3 default.
                Mutation {
                    op_type: OpType::Delete as i32,
                    sequence: 4,
                    rows: Some(sample_rows(2, true)),
                    write_hint: None,
                },
            ],
            bulk_entries: vec![],
        };
        assert_byte_identical(&entry);
    }

    #[test]
    fn test_matches_prost_empty_and_edge() {
        // Empty entry.
        assert_byte_identical(&WalEntry::default());
        // Mutation with sequence 0 (default, skipped) and empty rows.
        let entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 0,
                rows: Some(Rows {
                    schema: vec![],
                    rows: vec![],
                }),
                write_hint: None,
            }],
            bulk_entries: vec![],
        };
        assert_byte_identical(&entry);
    }

    #[test]
    fn test_reused_encoder_matches() {
        let mut enc = WalEntryEncoder::new();
        for n in [1usize, 10, 100] {
            let entry = WalEntry {
                mutations: vec![Mutation {
                    op_type: OpType::Put as i32,
                    sequence: n as u64,
                    rows: Some(sample_rows(n, true)),
                    write_hint: None,
                }],
                bulk_entries: vec![],
            };
            assert_eq!(entry.encode_to_vec(), enc.encode_to_vec(&entry));
        }
    }

    /// Exercises the `bulk_entries` path (the other branch of `encode_entry`),
    /// which the other tests leave empty.
    #[test]
    fn test_matches_prost_with_bulk_entries() {
        let entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 7,
                rows: Some(sample_rows(2, false)),
                write_hint: None,
            }],
            bulk_entries: vec![
                BulkWalEntry {
                    sequence: 100,
                    max_ts: 200,
                    min_ts: 50,
                    timestamp_index: 3,
                    body: None,
                },
                BulkWalEntry {
                    sequence: 101,
                    max_ts: 0, // default, must be skipped
                    min_ts: 0, // default, must be skipped
                    timestamp_index: 0,
                    body: None,
                },
            ],
        };
        assert_byte_identical(&entry);
    }

    /// Populates every field of the hand-rolled messages (mutations + bulk
    /// entries, rows with schema and rows, write_hint set) so the byte-equality
    /// check guards against tag/type changes, not just field additions (which
    /// the exhaustive destructure already catches at compile time).
    #[test]
    fn test_matches_prost_all_fields_populated() {
        let entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 12345,
                rows: Some(sample_rows(4, true)),
                write_hint: Some(WriteHint {
                    primary_key_encoding: 1,
                }),
            }],
            bulk_entries: vec![BulkWalEntry {
                sequence: 999,
                max_ts: 1_700_000_000_000,
                min_ts: 1_699_000_000_000,
                timestamp_index: 2,
                body: None,
            }],
        };
        assert_byte_identical(&entry);
    }
}
