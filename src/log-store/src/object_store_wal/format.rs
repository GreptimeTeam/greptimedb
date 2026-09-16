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

//! Encoding and decoding of a single WAL object. The byte layout is described
//! in the [module documentation](super).

use std::collections::BTreeMap;
use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};
use snafu::{OptionExt, ensure};
use store_api::storage::RegionId;

use crate::error::{CorruptedWalObjectSnafu, Result};

const HEADER_MAGIC: &[u8; 8] = b"GTWALOBJ";
const TRAILER_MAGIC: &[u8; 8] = b"GTWALTRL";
const FORMAT_VERSION: u16 = 1;

/// Length of the object header: magic, version, object sequence, writer instance.
pub(super) const HEADER_LEN: usize = 8 + 2 + 8 + 16;
/// Length of the fixed trailer: footer offset, footer length, footer CRC32,
/// object CRC32 and magic.
pub(super) const TRAILER_LEN: usize = 8 + 8 + 4 + 4 + 8;
const SEGMENT_HEADER_LEN: usize = 8 + 4;
/// Length of one footer entry: region id, entry id range, entry count, segment
/// offset, segment length and segment CRC32.
pub(super) const FOOTER_ENTRY_LEN: usize = 8 + 8 + 8 + 4 + 8 + 8 + 4;
/// Length of the entry count the footer starts with.
const FOOTER_COUNT_LEN: usize = 4;

/// Header of a WAL object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Header {
    pub(super) object_seq: u64,
    pub(super) writer_instance: [u8; 16],
}

/// A single WAL entry inside an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Record {
    pub(super) region_id: RegionId,
    pub(super) entry_id: u64,
    pub(super) payload: Bytes,
}

/// Footer entry describing the segment of one region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FooterEntry {
    pub(super) region_id: RegionId,
    pub(super) min_entry_id: u64,
    pub(super) max_entry_id: u64,
    pub(super) entry_count: u32,
    pub(super) segment_offset: u64,
    pub(super) segment_len: u64,
    pub(super) segment_crc32: u32,
}

/// Fixed-size trailer locating the footer of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FixedTrailer {
    pub(super) footer_offset: u64,
    pub(super) footer_len: u64,
    pub(super) footer_crc32: u32,
    pub(super) object_crc32: u32,
}

/// An encoded object together with the metadata a writer indexes it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EncodedObject {
    pub(super) bytes: Bytes,
    pub(super) footer: Vec<FooterEntry>,
    pub(super) trailer: FixedTrailer,
}

/// A decoded object with its records ordered by region id and entry id.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DecodedObject {
    pub(super) header: Header,
    pub(super) records: Vec<Record>,
    pub(super) footer: Vec<FooterEntry>,
}

/// Encodes `records` into one object. Records are grouped into a segment per
/// region; entry ids within a region must be unique.
pub(super) fn encode_object(header: Header, records: &[Record]) -> Result<EncodedObject> {
    ensure!(
        !records.is_empty(),
        CorruptedWalObjectSnafu {
            reason: "object has no records",
        }
    );

    let mut grouped = BTreeMap::<RegionId, Vec<&Record>>::new();
    for record in records {
        grouped.entry(record.region_id).or_default().push(record);
    }
    for (region_id, records) in &mut grouped {
        records.sort_unstable_by_key(|record| record.entry_id);
        ensure!(
            !records
                .windows(2)
                .any(|pair| pair[0].entry_id >= pair[1].entry_id),
            CorruptedWalObjectSnafu {
                reason: format!("non-monotonic entry id for region {region_id}"),
            }
        );
    }

    let mut output = BytesMut::new();
    encode_header(&header, &mut output);

    let mut directory = Vec::with_capacity(grouped.len());
    for (region_id, records) in grouped {
        let segment_offset = to_u64(output.len(), "segment offset")?;
        let entry_count = to_u32(records.len(), "segment entry count")?;
        let segment_start = output.len();

        output.put_u64(region_id.as_u64());
        output.put_u32(entry_count);
        for record in &records {
            output.put_u64(record.entry_id);
            output.put_u32(to_u32(record.payload.len(), "record payload")?);
            output.put_slice(&record.payload);
        }

        let segment = &output[segment_start..];
        directory.push(FooterEntry {
            region_id,
            min_entry_id: records[0].entry_id,
            max_entry_id: records[records.len() - 1].entry_id,
            entry_count,
            segment_offset,
            segment_len: to_u64(segment.len(), "segment length")?,
            segment_crc32: crc32fast::hash(segment),
        });
    }

    let footer_offset = to_u64(output.len(), "footer offset")?;
    let footer_start = output.len();
    encode_footer(&directory, &mut output)?;
    let footer = &output[footer_start..];
    let trailer = FixedTrailer {
        footer_offset,
        footer_len: to_u64(footer.len(), "footer length")?,
        footer_crc32: crc32fast::hash(footer),
        object_crc32: 0,
    };
    let trailer = FixedTrailer {
        object_crc32: object_crc32(&output, trailer),
        ..trailer
    };
    encode_trailer(trailer, &mut output);

    Ok(EncodedObject {
        bytes: output.freeze(),
        footer: directory,
        trailer,
    })
}

/// Returns the length of the object whose footer is `footer`, derived from the
/// layout: the segments tile the body after the header, and the footer, which
/// holds an entry count and one entry per segment, and the trailer follow them.
pub(super) fn object_len(footer: &[FooterEntry]) -> u64 {
    let segments = footer.iter().map(|entry| entry.segment_len).sum::<u64>();
    let framing = HEADER_LEN + FOOTER_COUNT_LEN + footer.len() * FOOTER_ENTRY_LEN + TRAILER_LEN;
    segments + framing as u64
}

/// Decodes the header from the first [`HEADER_LEN`] bytes of an object.
pub(super) fn decode_header(bytes: &[u8]) -> Result<Header> {
    ensure!(
        bytes.len() >= HEADER_LEN,
        CorruptedWalObjectSnafu {
            reason: truncated("header", bytes.len(), HEADER_LEN),
        }
    );

    let mut reader = Reader::new(&bytes[..HEADER_LEN]);
    let magic = reader.take(HEADER_MAGIC.len(), "header")?;
    ensure!(
        magic == HEADER_MAGIC,
        CorruptedWalObjectSnafu {
            reason: invalid_magic("header", HEADER_MAGIC, magic),
        }
    );
    let version = reader.u16("header")?;
    ensure!(
        version == FORMAT_VERSION,
        CorruptedWalObjectSnafu {
            reason: format!(
                "unsupported format version {version}, expected version {FORMAT_VERSION}"
            ),
        }
    );
    let object_seq = reader.u64("header")?;
    let writer_instance = reader
        .take(16, "header")?
        .try_into()
        .expect("writer instance has a fixed length");
    Ok(Header {
        object_seq,
        writer_instance,
    })
}

/// Decodes the trailer from the last [`TRAILER_LEN`] bytes of an object.
pub(super) fn decode_trailer(bytes: &[u8]) -> Result<FixedTrailer> {
    ensure!(
        bytes.len() >= TRAILER_LEN,
        CorruptedWalObjectSnafu {
            reason: truncated("trailer", bytes.len(), TRAILER_LEN),
        }
    );
    ensure!(
        bytes.len() <= TRAILER_LEN,
        CorruptedWalObjectSnafu {
            reason: trailing_bytes("trailer", bytes.len(), TRAILER_LEN),
        }
    );

    let mut reader = Reader::new(bytes);
    let trailer = FixedTrailer {
        footer_offset: reader.u64("trailer")?,
        footer_len: reader.u64("trailer")?,
        footer_crc32: reader.u32("trailer")?,
        object_crc32: reader.u32("trailer")?,
    };
    let magic = reader.take(TRAILER_MAGIC.len(), "trailer")?;
    ensure!(
        magic == TRAILER_MAGIC,
        CorruptedWalObjectSnafu {
            reason: invalid_magic("trailer", TRAILER_MAGIC, magic),
        }
    );
    Ok(trailer)
}

/// Decodes the footer that `trailer` points at.
pub(super) fn decode_footer(bytes: &[u8], trailer: FixedTrailer) -> Result<Vec<FooterEntry>> {
    let footer_len = to_usize(trailer.footer_len, "footer length")?;
    ensure!(
        bytes.len() >= footer_len,
        CorruptedWalObjectSnafu {
            reason: truncated("footer", bytes.len(), footer_len),
        }
    );
    ensure!(
        bytes.len() <= footer_len,
        CorruptedWalObjectSnafu {
            reason: trailing_bytes("footer", bytes.len(), footer_len),
        }
    );
    let checksum = crc32fast::hash(bytes);
    ensure!(
        checksum == trailer.footer_crc32,
        CorruptedWalObjectSnafu {
            reason: checksum_mismatch("footer", trailer.footer_crc32, checksum),
        }
    );

    let mut reader = Reader::new(bytes);
    let count = reader.u32("footer")? as usize;
    let expected_len = count
        .checked_mul(FOOTER_ENTRY_LEN)
        .and_then(|len| len.checked_add(4))
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!("footer declares {count} entries, which overflows its length"),
        })?;
    ensure!(
        expected_len == bytes.len(),
        CorruptedWalObjectSnafu {
            reason: format!(
                "footer declares {count} entries, expected length {expected_len}, actual {}",
                bytes.len()
            ),
        }
    );

    let mut directory = Vec::with_capacity(count);
    let mut previous_region = None;
    for _ in 0..count {
        let region_id = RegionId::from_u64(reader.u64("footer")?);
        if let Some(previous) = previous_region {
            ensure!(
                previous < region_id,
                CorruptedWalObjectSnafu {
                    reason: if previous == region_id {
                        format!("duplicate footer entry for region {region_id}")
                    } else {
                        format!(
                            "footer entries are not ordered by region, {previous} precedes {region_id}"
                        )
                    },
                }
            );
        }
        previous_region = Some(region_id);

        let entry = FooterEntry {
            region_id,
            min_entry_id: reader.u64("footer")?,
            max_entry_id: reader.u64("footer")?,
            entry_count: reader.u32("footer")?,
            segment_offset: reader.u64("footer")?,
            segment_len: reader.u64("footer")?,
            segment_crc32: reader.u32("footer")?,
        };
        ensure!(
            entry.entry_count > 0
                && entry.min_entry_id <= entry.max_entry_id
                && entry.segment_len >= SEGMENT_HEADER_LEN as u64,
            CorruptedWalObjectSnafu {
                reason: format!(
                    "invalid footer entry for region {}, entry ids {}..={}, {} entries, segment length {}",
                    entry.region_id,
                    entry.min_entry_id,
                    entry.max_entry_id,
                    entry.entry_count,
                    entry.segment_len
                ),
            }
        );
        directory.push(entry);
    }
    Ok(directory)
}

/// Decodes the segment that `entry` describes.
pub(super) fn decode_segment(bytes: &[u8], entry: &FooterEntry) -> Result<Vec<Record>> {
    let segment_len = to_usize(entry.segment_len, "segment length")?;
    ensure!(
        bytes.len() >= segment_len,
        CorruptedWalObjectSnafu {
            reason: truncated("segment", bytes.len(), segment_len),
        }
    );
    ensure!(
        bytes.len() <= segment_len,
        CorruptedWalObjectSnafu {
            reason: trailing_bytes("segment", bytes.len(), segment_len),
        }
    );
    let checksum = crc32fast::hash(bytes);
    ensure!(
        checksum == entry.segment_crc32,
        CorruptedWalObjectSnafu {
            reason: checksum_mismatch(
                &format!("segment of region {}", entry.region_id),
                entry.segment_crc32,
                checksum
            ),
        }
    );

    let mut reader = Reader::new(bytes);
    let region_id = RegionId::from_u64(reader.u64("segment")?);
    let count = reader.u32("segment")?;
    ensure!(
        region_id == entry.region_id && count == entry.entry_count,
        CorruptedWalObjectSnafu {
            reason: format!(
                "segment holds region {region_id} with {count} entries, footer expects region {} with {} entries",
                entry.region_id, entry.entry_count
            ),
        }
    );
    // Every record needs at least an entry id and a payload length. Bound the
    // allocation by bytes that are actually present before trusting `count`.
    let max_count = reader.remaining_len() / (8 + 4);
    ensure!(
        count as usize <= max_count,
        CorruptedWalObjectSnafu {
            reason: format!(
                "segment declares {count} entries but only holds bytes for {max_count}"
            ),
        }
    );

    let mut records = Vec::with_capacity(count as usize);
    let mut previous_id = None;
    for _ in 0..count {
        let entry_id = reader.u64("segment")?;
        ensure!(
            previous_id.is_none_or(|previous| previous < entry_id),
            CorruptedWalObjectSnafu {
                reason: format!("non-monotonic entry id for region {region_id}"),
            }
        );
        previous_id = Some(entry_id);
        let payload_len = reader.u32("segment")? as usize;
        let payload = Bytes::copy_from_slice(reader.take(payload_len, "segment")?);
        records.push(Record {
            region_id,
            entry_id,
            payload,
        });
    }
    ensure!(
        reader.is_empty(),
        CorruptedWalObjectSnafu {
            reason: trailing_bytes("segment", bytes.len(), bytes.len() - reader.remaining_len()),
        }
    );
    ensure!(
        records.first().map(|record| record.entry_id) == Some(entry.min_entry_id)
            && records.last().map(|record| record.entry_id) == Some(entry.max_entry_id),
        CorruptedWalObjectSnafu {
            reason: format!(
                "segment of region {region_id} holds entry ids {:?}..={:?}, footer expects {}..={}",
                records.first().map(|record| record.entry_id),
                records.last().map(|record| record.entry_id),
                entry.min_entry_id,
                entry.max_entry_id
            ),
        }
    );
    Ok(records)
}

/// Smallest length of a well-formed object: header, a footer holding only its
/// entry count and the trailer.
pub(super) const MIN_OBJECT_LEN: usize = HEADER_LEN + 4 + TRAILER_LEN;

/// Locates the footer inside an object of `object_len` bytes from its trailer.
/// The footer must follow the header and end where the trailer starts.
pub(super) fn footer_range(trailer: FixedTrailer, object_len: usize) -> Result<Range<usize>> {
    ensure!(
        object_len >= MIN_OBJECT_LEN,
        CorruptedWalObjectSnafu {
            reason: truncated("object", object_len, MIN_OBJECT_LEN),
        }
    );
    let trailer_start = object_len - TRAILER_LEN;
    let footer_start = to_usize(trailer.footer_offset, "footer offset")?;
    let footer_len = to_usize(trailer.footer_len, "footer length")?;
    let footer_end =
        footer_start
            .checked_add(footer_len)
            .with_context(|| CorruptedWalObjectSnafu {
                reason: format!("footer range {footer_start}..{footer_len} overflows the object"),
            })?;
    ensure!(
        footer_start >= HEADER_LEN && footer_end == trailer_start,
        CorruptedWalObjectSnafu {
            reason: format!(
                "invalid footer range {footer_start}..{footer_end}, expected {HEADER_LEN}..{trailer_start}"
            ),
        }
    );
    Ok(footer_start..footer_end)
}

/// Checks that the segments `footer` describes tile the object body: the first
/// starts right after the header, each follows the previous one without a gap
/// or overlap, and the last ends where the footer at `footer_start` begins.
pub(super) fn verify_segment_ranges(footer: &[FooterEntry], footer_start: usize) -> Result<()> {
    let mut expected_offset = HEADER_LEN;
    for entry in footer {
        let start = to_usize(entry.segment_offset, "segment offset")?;
        let len = to_usize(entry.segment_len, "segment length")?;
        let end = start
            .checked_add(len)
            .with_context(|| CorruptedWalObjectSnafu {
                reason: format!(
                    "segment range {start}..{len} of region {} overflows the object",
                    entry.region_id
                ),
            })?;
        ensure!(
            start == expected_offset && end <= footer_start,
            CorruptedWalObjectSnafu {
                reason: format!(
                    "invalid segment range {start}..{end} of region {}, expected {expected_offset}..{footer_start}",
                    entry.region_id
                ),
            }
        );
        expected_offset = end;
    }
    ensure!(
        expected_offset == footer_start,
        CorruptedWalObjectSnafu {
            reason: format!(
                "invalid segment range, segments end at {expected_offset}, footer starts at {footer_start}"
            ),
        }
    );
    Ok(())
}

/// Decodes a whole object, verifying every checksum and byte range. Recovery
/// reads only the header, trailer and footer, so this is the reference
/// decoder that tests check the store against.
#[cfg(test)]
pub(super) fn decode_object(bytes: &[u8]) -> Result<DecodedObject> {
    ensure!(
        bytes.len() >= MIN_OBJECT_LEN,
        CorruptedWalObjectSnafu {
            reason: truncated("object", bytes.len(), MIN_OBJECT_LEN),
        }
    );

    let header = decode_header(bytes)?;
    let trailer_start = bytes.len() - TRAILER_LEN;
    let trailer = decode_trailer(&bytes[trailer_start..])?;
    let footer_range = footer_range(trailer, bytes.len())?;
    let footer_start = footer_range.start;

    let footer = decode_footer(&bytes[footer_range], trailer)?;
    ensure!(
        !footer.is_empty(),
        CorruptedWalObjectSnafu {
            reason: "object has no records",
        }
    );
    verify_segment_ranges(&footer, footer_start)?;

    let mut records = Vec::new();
    for entry in &footer {
        let start = entry.segment_offset as usize;
        let end = start + entry.segment_len as usize;
        records.extend(decode_segment(&bytes[start..end], entry)?);
    }
    let checksum = object_crc32(&bytes[..trailer_start], trailer);
    ensure!(
        checksum == trailer.object_crc32,
        CorruptedWalObjectSnafu {
            reason: checksum_mismatch("object", trailer.object_crc32, checksum),
        }
    );

    Ok(DecodedObject {
        header,
        records,
        footer,
    })
}

fn encode_header(header: &Header, output: &mut BytesMut) {
    output.put_slice(HEADER_MAGIC);
    output.put_u16(FORMAT_VERSION);
    output.put_u64(header.object_seq);
    output.put_slice(&header.writer_instance);
}

fn encode_footer(directory: &[FooterEntry], output: &mut BytesMut) -> Result<()> {
    output.put_u32(to_u32(directory.len(), "footer entry count")?);
    for entry in directory {
        output.put_u64(entry.region_id.as_u64());
        output.put_u64(entry.min_entry_id);
        output.put_u64(entry.max_entry_id);
        output.put_u32(entry.entry_count);
        output.put_u64(entry.segment_offset);
        output.put_u64(entry.segment_len);
        output.put_u32(entry.segment_crc32);
    }
    Ok(())
}

fn encode_trailer(trailer: FixedTrailer, output: &mut BytesMut) {
    output.put_u64(trailer.footer_offset);
    output.put_u64(trailer.footer_len);
    output.put_u32(trailer.footer_crc32);
    output.put_u32(trailer.object_crc32);
    output.put_slice(TRAILER_MAGIC);
}

/// Checksum of everything but the object checksum itself, so the trailer can
/// carry it.
fn object_crc32(bytes_before_trailer: &[u8], trailer: FixedTrailer) -> u32 {
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(bytes_before_trailer);
    checksum.update(&trailer.footer_offset.to_be_bytes());
    checksum.update(&trailer.footer_len.to_be_bytes());
    checksum.update(&trailer.footer_crc32.to_be_bytes());
    checksum.update(TRAILER_MAGIC);
    checksum.finalize()
}

fn truncated(part: &str, actual: usize, expected: usize) -> String {
    format!("truncated {part}, expected at least {expected} bytes, actual {actual}")
}

fn trailing_bytes(part: &str, actual: usize, expected: usize) -> String {
    format!("trailing bytes in {part}, expected {expected} bytes, actual {actual}")
}

fn invalid_magic(part: &str, expected: &[u8], actual: &[u8]) -> String {
    format!(
        "invalid {part} magic, expected {:?}, actual {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(actual)
    )
}

fn checksum_mismatch(part: &str, expected: u32, actual: u32) -> String {
    format!("{part} checksum mismatch, expected {expected:#010x}, actual {actual:#010x}")
}

fn to_u32(value: usize, name: &str) -> Result<u32> {
    u32::try_from(value)
        .ok()
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!("{name} {value} does not fit into u32"),
        })
}

fn to_u64(value: usize, name: &str) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!("{name} {value} does not fit into u64"),
        })
}

fn to_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value)
        .ok()
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!("{name} {value} does not fit into usize"),
        })
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize, part: &'static str) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .with_context(|| CorruptedWalObjectSnafu {
                reason: truncated(part, self.bytes.len(), len),
            })?;
        let value = self
            .bytes
            .get(self.offset..end)
            .with_context(|| CorruptedWalObjectSnafu {
                reason: truncated(part, self.bytes.len(), end),
            })?;
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self, part: &'static str) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2, part)?.try_into().expect("u16 has two bytes"),
        ))
    }

    fn u32(&mut self, part: &'static str) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4, part)?.try_into().expect("u32 has four bytes"),
        ))
    }

    fn u64(&mut self, part: &'static str) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8, part)?.try_into().expect("u64 has eight bytes"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn header() -> Header {
        Header {
            object_seq: 42,
            writer_instance: *b"writer-instance!",
        }
    }

    fn records() -> Vec<Record> {
        vec![
            Record {
                region_id: RegionId::new(2, 1),
                entry_id: 7,
                payload: Bytes::from_static(b"region-2"),
            },
            Record {
                region_id: RegionId::new(1, 1),
                entry_id: 2,
                payload: Bytes::from_static(b"second"),
            },
            Record {
                region_id: RegionId::new(1, 1),
                entry_id: 1,
                payload: Bytes::from_static(b"first"),
            },
        ]
    }

    #[test]
    fn test_format_round_trip_with_multiple_regions_and_entries() {
        let encoded = encode_object(header(), &records()).unwrap();
        let decoded = decode_object(&encoded.bytes).unwrap();

        assert_eq!(header(), decoded.header);
        assert_eq!(2, decoded.footer.len());
        assert_eq!(RegionId::new(1, 1), decoded.footer[0].region_id);
        assert_eq!((1, 2, 2), footer_ids(&decoded.footer[0]));
        assert_eq!(RegionId::new(2, 1), decoded.footer[1].region_id);
        assert_eq!((7, 7, 1), footer_ids(&decoded.footer[1]));
        assert_eq!(
            vec![
                (RegionId::new(1, 1), 1, Bytes::from_static(b"first")),
                (RegionId::new(1, 1), 2, Bytes::from_static(b"second")),
                (RegionId::new(2, 1), 7, Bytes::from_static(b"region-2")),
            ],
            decoded
                .records
                .into_iter()
                .map(|record| (record.region_id, record.entry_id, record.payload))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_format_decodes_footer_trailer_and_segment_ranges() {
        let encoded = encode_object(header(), &records()).unwrap();
        let trailer_start = encoded.bytes.len() - TRAILER_LEN;
        let trailer = decode_trailer(&encoded.bytes[trailer_start..]).unwrap();
        assert_eq!(encoded.trailer, trailer);

        let footer_start = trailer.footer_offset as usize;
        let footer_end = footer_start + trailer.footer_len as usize;
        let footer = decode_footer(&encoded.bytes[footer_start..footer_end], trailer).unwrap();
        assert_eq!(encoded.footer, footer);

        let entry = &footer[0];
        let start = entry.segment_offset as usize;
        let end = start + entry.segment_len as usize;
        let segment = decode_segment(&encoded.bytes[start..end], entry).unwrap();
        assert_eq!(
            vec![1, 2],
            segment
                .iter()
                .map(|entry| entry.entry_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_format_rejects_truncation() {
        let encoded = encode_object(header(), &records()).unwrap();
        assert_corrupted(
            decode_object(&encoded.bytes[..HEADER_LEN - 1]),
            "truncated object",
        );
        assert_corrupted(
            decode_trailer(&encoded.bytes[encoded.bytes.len() - TRAILER_LEN + 1..]),
            "truncated trailer",
        );

        let entry = &encoded.footer[0];
        let start = entry.segment_offset as usize;
        let end = start + entry.segment_len as usize - 1;
        assert_corrupted(
            decode_segment(&encoded.bytes[start..end], entry),
            "truncated segment",
        );
    }

    #[test]
    fn test_format_rejects_header_magic_and_version() {
        let encoded = encode_object(header(), &records()).unwrap();

        let mut bad_magic = encoded.bytes.to_vec();
        bad_magic[0] ^= 1;
        assert_corrupted(decode_object(&bad_magic), "invalid header magic");

        let mut bad_version = encoded.bytes.to_vec();
        bad_version[8..10].copy_from_slice(&(FORMAT_VERSION + 1).to_be_bytes());
        assert_corrupted(decode_object(&bad_version), "unsupported format version 2");

        let mut bad_object_seq = encoded.bytes.to_vec();
        bad_object_seq[10] ^= 1;
        assert_corrupted(decode_object(&bad_object_seq), "object checksum mismatch");

        let mut bad_writer = encoded.bytes.to_vec();
        bad_writer[18] ^= 1;
        assert_corrupted(decode_object(&bad_writer), "object checksum mismatch");
    }

    #[test]
    fn test_format_rejects_trailer_magic_and_invalid_footer_range() {
        let encoded = encode_object(header(), &records()).unwrap();

        let mut bad_magic = encoded.bytes.to_vec();
        let last = bad_magic.len() - 1;
        bad_magic[last] ^= 1;
        assert_corrupted(decode_object(&bad_magic), "invalid trailer magic");

        let mut bad_range = encoded.bytes.to_vec();
        let trailer_start = bad_range.len() - TRAILER_LEN;
        bad_range[trailer_start..trailer_start + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_corrupted(decode_object(&bad_range), "footer");
    }

    #[test]
    fn test_format_rejects_segment_and_footer_crc_errors() {
        let encoded = encode_object(header(), &records()).unwrap();

        let mut bad_segment = encoded.bytes.to_vec();
        let segment = &encoded.footer[0];
        bad_segment[segment.segment_offset as usize + SEGMENT_HEADER_LEN] ^= 1;
        assert_corrupted(
            decode_object(&bad_segment),
            &format!("segment of region {} checksum mismatch", segment.region_id),
        );

        let mut bad_footer = encoded.bytes.to_vec();
        bad_footer[encoded.trailer.footer_offset as usize] ^= 1;
        assert_corrupted(decode_object(&bad_footer), "footer checksum mismatch");
    }

    #[test]
    fn test_format_rejects_corrupted_trailer_checksums() {
        let encoded = encode_object(header(), &records()).unwrap();
        let trailer_start = encoded.bytes.len() - TRAILER_LEN;

        // The object checksum the trailer stores, at footer offset, footer length
        // and footer checksum.
        let object_crc32_start = trailer_start + 8 + 8 + 4;
        let mut bad_object_crc32 = encoded.bytes.to_vec();
        bad_object_crc32[object_crc32_start] ^= 1;
        assert_corrupted(decode_object(&bad_object_crc32), "object checksum mismatch");

        let footer_crc32_start = trailer_start + 8 + 8;
        let mut bad_footer_crc32 = encoded.bytes.to_vec();
        bad_footer_crc32[footer_crc32_start] ^= 1;
        assert_corrupted(decode_object(&bad_footer_crc32), "footer checksum mismatch");
    }

    #[test]
    fn test_format_rejects_empty_object() {
        assert_corrupted(encode_object(header(), &[]), "object has no records");
    }

    #[test]
    fn test_format_rejects_duplicate_entry_ids() {
        let region_id = RegionId::new(1, 1);
        let records = vec![
            Record {
                region_id,
                entry_id: 1,
                payload: Bytes::from_static(b"first"),
            },
            Record {
                region_id,
                entry_id: 1,
                payload: Bytes::from_static(b"duplicate"),
            },
        ];
        assert_corrupted(
            encode_object(header(), &records),
            &format!("non-monotonic entry id for region {region_id}"),
        );
    }

    #[test]
    fn test_format_rejects_segment_count_larger_than_available_bytes() {
        let region_id = RegionId::new(1, 1);
        let mut segment = BytesMut::new();
        segment.put_u64(region_id.as_u64());
        segment.put_u32(u32::MAX);
        let entry = FooterEntry {
            region_id,
            min_entry_id: 1,
            max_entry_id: 1,
            entry_count: u32::MAX,
            segment_offset: HEADER_LEN as u64,
            segment_len: segment.len() as u64,
            segment_crc32: crc32fast::hash(&segment),
        };

        assert_corrupted(
            decode_segment(&segment, &entry),
            "segment declares 4294967295 entries but only holds bytes for 0",
        );
    }

    fn footer_ids(entry: &FooterEntry) -> (u64, u64, u32) {
        (entry.min_entry_id, entry.max_entry_id, entry.entry_count)
    }

    fn assert_corrupted<T: std::fmt::Debug>(result: Result<T>, expected_reason: &str) {
        match result {
            Err(Error::CorruptedWalObject { reason, .. }) => assert!(
                reason.contains(expected_reason),
                "expected reason to contain {expected_reason:?}, actual {reason:?}"
            ),
            other => panic!("expected a corrupted object error, actual {other:?}"),
        }
    }
}
