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

//! # SST Files with Inverted Index Format Specification
//!
//! ## File Structure
//!
//! Each SST file includes a series of inverted indices followed by a footer.
//!
//! `inverted_index₀ inverted_index₁ ... inverted_indexₙ footer`
//!
//! - Each `inverted_indexᵢ` represents an index entry corresponding to tag values and their locations within the file.
//! - `footer`: Contains metadata about the inverted indices, encoded as a protobuf message.
//!
//! ## Inverted Index Internals
//!
//! An inverted index comprises a collection of bitmaps, a null bitmap, and a finite state transducer (FST) indicating tag values' positions:
//!
//! `null_bitmap bitmap₀ bitmap₁ bitmap₂ ... bitmapₙ fst`
//!
//! - `null_bitmap`: Bitset tracking the presence of null values within the tag column.
//! - `bitmapᵢ`: Bitset indicating the presence of tag values within a row group.
//! - `fst`: Finite State Transducer providing an ordered map of bytes, representing the tag values.
//!
//! ## Footer Details
//!
//! The footer encapsulates the metadata for inversion mappings:
//!
//! `footer_payload footer_payload_size`
//!
//! - `footer_payload`: Protobuf-encoded [`InvertedIndexMetas`] describing the metadata of each inverted index.
//! - `footer_payload_size`: Size in bytes of the `footer_payload`, displayed as a `u32` integer.
//! - The footer aids in the interpretation of the inverted indices, providing necessary offset and count information.
//!
//! ## Reference
//!
//! More detailed information regarding the encoding of the inverted indices can be found in the [RFC].
//!
//! [`InvertedIndexMetas`]: https://github.com/GreptimeTeam/greptime-proto/blob/2aaee38de81047537dfa42af9df63bcfb866e06c/proto/greptime/v1/index/inverted_index.proto#L32-L64
//! [RFC]: https://github.com/GreptimeTeam/greptimedb/blob/main/docs/rfcs/2023-11-03-inverted-index.md

pub mod reader;
pub mod writer;

use greptime_proto::v1::index::InvertedIndexMeta;

use crate::bitmap::Bitmap;

/// FST block size used when writing split FSTs. A point lookup reads one block of about
/// this size instead of the whole FST.
pub const DEFAULT_FST_BLOCK_SIZE: usize = 16 * 1024;

/// Whether the tag's FST is split into blocks indexed by
/// [`InvertedIndexMeta::fst_block_index`].
pub fn is_chunked_fst(meta: &InvertedIndexMeta) -> bool {
    !meta.fst_block_index.is_empty()
}

const FOOTER_PAYLOAD_SIZE_SIZE: u64 = 4;
const MIN_BLOB_SIZE: u64 = FOOTER_PAYLOAD_SIZE_SIZE;

/// Location of a value's segment bitmap, as stored in the FST.
///
/// A legacy value packs `[offset: u32, size: u32]` where `size` is the length of a
/// non-empty serialized bitmap, so it is never 0 and never reaches 2^31. Inline postings use
/// the two encodings legacy values cannot produce:
///
/// - `size == 0`: one run, `start` in bits 0..20 and `len - 1` in bits 20..32. Keeping the
///   value below 2^32 keeps FST outputs short.
/// - bit 63 set: two runs, each `start` in 21 bits and `len - 1` in 10 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FstValue {
    /// A serialized bitmap at `offset..offset + size` relative to the index's base offset.
    Bitmap { offset: u32, size: u32 },
    /// Up to two runs of segment ids stored in the FST value itself.
    Inline(InlinePosting),
}

/// Segment runs `[start, start + len)` small enough to live in an FST value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlinePosting {
    runs: [(u32, u32); 2],
    num_runs: u8,
}

const TWO_RUNS_FLAG: u64 = 1 << 63;
const ONE_RUN_START_BITS: u32 = 20;
const ONE_RUN_LEN_BITS: u32 = 12;
const TWO_RUN_START_BITS: u32 = 21;
const TWO_RUN_LEN_BITS: u32 = 10;

fn run_fits((start, len): (u32, u32), start_bits: u32, len_bits: u32) -> bool {
    (start as u64) < (1 << start_bits) && ((len - 1) as u64) < (1 << len_bits)
}

impl InlinePosting {
    /// Returns the inline form of `segments` (ascending) when it is at most two runs that
    /// fit the encoding.
    pub fn try_from_segments(segments: impl Iterator<Item = u32>) -> Option<Self> {
        let mut runs = [(0u32, 0u32); 2];
        let mut num_runs = 0usize;
        for seg in segments {
            if num_runs > 0 {
                let (start, len) = &mut runs[num_runs - 1];
                if *start + *len == seg {
                    *len += 1;
                    continue;
                }
            }
            if num_runs == 2 {
                return None;
            }
            runs[num_runs] = (seg, 1);
            num_runs += 1;
        }
        let ok = match num_runs {
            1 => {
                run_fits(runs[0], ONE_RUN_START_BITS, ONE_RUN_LEN_BITS)
                    || run_fits(runs[0], TWO_RUN_START_BITS, TWO_RUN_LEN_BITS)
            }
            2 => runs
                .iter()
                .all(|r| run_fits(*r, TWO_RUN_START_BITS, TWO_RUN_LEN_BITS)),
            _ => false,
        };
        ok.then_some(Self {
            runs,
            num_runs: num_runs as u8,
        })
    }

    pub fn to_bitmap(self) -> Bitmap {
        let mut bitmap = Bitmap::new_roaring();
        for &(start, len) in &self.runs[..self.num_runs as usize] {
            bitmap.insert_range(start as usize..=(start + len - 1) as usize);
        }
        bitmap
    }
}

impl FstValue {
    pub fn encode(self) -> u64 {
        match self {
            FstValue::Bitmap { offset, size } => bytemuck::cast::<[u32; 2], u64>([offset, size]),
            FstValue::Inline(p) => {
                let [(s0, l0), (s1, l1)] = p.runs;
                if p.num_runs == 1 && run_fits(p.runs[0], ONE_RUN_START_BITS, ONE_RUN_LEN_BITS) {
                    return s0 as u64 | ((l0 - 1) as u64) << ONE_RUN_START_BITS;
                }
                let pack =
                    |start: u32, len: u32| start as u64 | ((len - 1) as u64) << TWO_RUN_START_BITS;
                // A single run that only fits the two-run layout repeats itself as the second
                // run, which decodes to the same set.
                let second = if p.num_runs == 1 {
                    pack(s0, l0)
                } else {
                    pack(s1, l1)
                };
                let b = TWO_RUN_START_BITS + TWO_RUN_LEN_BITS;
                TWO_RUNS_FLAG | pack(s0, l0) | second << b
            }
        }
    }

    pub fn decode(value: u64) -> Self {
        let field = |shift: u32, bits: u32| ((value >> shift) & ((1 << bits) - 1)) as u32;
        if value >> 32 == 0 {
            let run = (
                field(0, ONE_RUN_START_BITS),
                field(ONE_RUN_START_BITS, ONE_RUN_LEN_BITS) + 1,
            );
            return FstValue::Inline(InlinePosting {
                runs: [run, (0, 0)],
                num_runs: 1,
            });
        }
        if value & TWO_RUNS_FLAG == 0 {
            let [offset, size] = bytemuck::cast::<u64, [u32; 2]>(value);
            return FstValue::Bitmap { offset, size };
        }
        let b = TWO_RUN_START_BITS + TWO_RUN_LEN_BITS;
        FstValue::Inline(InlinePosting {
            runs: [
                (
                    field(0, TWO_RUN_START_BITS),
                    field(TWO_RUN_START_BITS, TWO_RUN_LEN_BITS) + 1,
                ),
                (
                    field(b, TWO_RUN_START_BITS),
                    field(b + TWO_RUN_START_BITS, TWO_RUN_LEN_BITS) + 1,
                ),
            ],
            num_runs: 2,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segments(bitmap: &Bitmap) -> Vec<usize> {
        bitmap.iter_ones().collect()
    }

    #[test]
    fn test_inline_posting_round_trip() {
        for segs in [
            vec![0u32],
            vec![7, 8, 9],
            vec![3, 10, 11],
            vec![(1 << 21) - 3, (1 << 21) - 1],
            // One run that only fits the two-run layout.
            vec![1 << 20],
            // Largest one-run value: start 2^20 - 1, length 4096.
            ((1 << 20) - 1..(1 << 20) - 1 + 4096).collect(),
            // Just past the one-run layout: start 2^20, longest two-run length 1024.
            ((1 << 20)..(1 << 20) + 1024).collect(),
            // Two runs, each at its layout's limit.
            (0..1024).chain((1 << 21) - 1024..1 << 21).collect(),
        ] {
            let posting = InlinePosting::try_from_segments(segs.iter().copied())
                .unwrap_or_else(|| panic!("{segs:?} should inline"));
            let FstValue::Inline(decoded) = FstValue::decode(FstValue::Inline(posting).encode())
            else {
                panic!("{segs:?} decoded as bitmap");
            };
            let expected = segs.iter().map(|s| *s as usize).collect::<Vec<_>>();
            assert_eq!(segments(&decoded.to_bitmap()), expected);
        }
    }

    #[test]
    fn test_inline_posting_rejects_what_does_not_fit() {
        // Three runs.
        assert!(InlinePosting::try_from_segments([1, 3, 5].into_iter()).is_none());
        // One run starting beyond 21 bits.
        assert!(InlinePosting::try_from_segments([u32::MAX - 1].into_iter()).is_none());
        // Two runs, second start beyond 21 bits.
        assert!(InlinePosting::try_from_segments([1, 1 << 21].into_iter()).is_none());
        // Two runs, first run longer than 1024.
        assert!(InlinePosting::try_from_segments((0..1025).chain([2000])).is_none());
    }

    #[test]
    fn test_legacy_value_decodes_as_bitmap() {
        // Legacy writers pack `[offset, size]` with a serialized bitmap size, which never
        // reaches 2^31.
        let value = bytemuck::cast::<[u32; 2], u64>([u32::MAX, (1 << 31) - 1]);
        assert_eq!(
            FstValue::decode(value),
            FstValue::Bitmap {
                offset: u32::MAX,
                size: (1 << 31) - 1
            }
        );
    }
}
