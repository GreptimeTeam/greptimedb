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
/// Version of an object without release records.
const FORMAT_VERSION: u16 = 1;
/// Version of an object with release records: the version 1 layout whose
/// footer ends with the release records.
const RELEASE_FORMAT_VERSION: u16 = 2;
/// Predecessor sequence recorded for an object that starts a chain. Object
/// sequences stay below `OBJECT_SEQ_LIMIT`, so it never names an object.
const NO_PREDECESSOR: u64 = u64::MAX;

/// Length of the object header: magic, version, object sequence, epoch,
/// predecessor sequence, predecessor epoch, reclaim boundary and the CRC32 of
/// the header.
pub(crate) const HEADER_LEN: usize = 8 + 2 + 8 + 8 + 8 + 8 + 8 + 4;
/// Length of the fixed trailer: footer offset, footer length, footer CRC32,
/// object CRC32 and magic.
pub(crate) const TRAILER_LEN: usize = 8 + 8 + 4 + 4 + 8;
const SEGMENT_HEADER_LEN: usize = 8 + 4;
/// Length of one footer entry: region id, entry id range, entry count, segment
/// offset, segment length and segment CRC32.
pub(crate) const FOOTER_ENTRY_LEN: usize = 8 + 8 + 8 + 4 + 8 + 8 + 4;
/// Length of the entry count the footer starts with, and of the release
/// record count that follows the entries in a version 2 footer.
const FOOTER_COUNT_LEN: usize = 4;
/// Length of one release record: region id and the entry id the region is
/// released through.
const RELEASE_LEN: usize = 8 + 8;

/// Header of a WAL object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) object_seq: u64,
    /// Epoch of the store instance that wrote the object: one above the
    /// sequence of the start object the instance created, so the epoch also
    /// identifies the instance.
    pub(crate) epoch: u64,
    /// The object this object extends, `None` for the first object of a chain.
    pub(crate) prev: Option<ChainLink>,
    /// Every object below this sequence was fully flushed when this object
    /// was sealed, so the chain recovery replays need not reach below it. It
    /// is at or below the sequence of the object.
    pub(crate) reclaim_boundary: u64,
}

/// Names the object another object extends: its sequence and the epoch
/// recorded in its header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChainLink {
    pub(crate) object_seq: u64,
    pub(crate) epoch: u64,
}

/// A single WAL entry inside an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) region_id: RegionId,
    pub(crate) entry_id: u64,
    pub(crate) payload: Bytes,
}

/// Footer entry describing the segment of one region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FooterEntry {
    pub(crate) region_id: RegionId,
    pub(crate) min_entry_id: u64,
    pub(crate) max_entry_id: u64,
    pub(crate) entry_count: u32,
    pub(crate) segment_offset: u64,
    pub(crate) segment_len: u64,
    pub(crate) segment_crc32: u32,
}

/// A release record: no entry of the region with an id at or below
/// `entry_id` is needed any more, `u64::MAX` releasing the region in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Release {
    pub(crate) region_id: RegionId,
    pub(crate) entry_id: u64,
}

/// Fixed-size trailer locating the footer of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FixedTrailer {
    pub(crate) footer_offset: u64,
    pub(crate) footer_len: u64,
    pub(crate) footer_crc32: u32,
    pub(crate) object_crc32: u32,
}

/// An encoded object together with the metadata a writer indexes it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedObject {
    pub(crate) bytes: Bytes,
    pub(crate) footer: Vec<FooterEntry>,
    pub(crate) trailer: FixedTrailer,
}

/// A decoded object with its records ordered by region id and entry id.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedObject {
    pub(crate) header: Header,
    pub(crate) records: Vec<Record>,
    pub(crate) footer: Vec<FooterEntry>,
    pub(crate) releases: Vec<Release>,
}

/// Encodes `records` and `releases` into one object. Records are grouped into
/// a segment per region; entry ids within a region must be unique, and so must
/// the regions of the release records. An object without release records is
/// encoded as version 1, one with them as version 2. An object without
/// records or release records only extends the chain.
pub(crate) fn encode_object(
    header: Header,
    records: &[Record],
    releases: &[Release],
) -> Result<EncodedObject> {
    let mut releases = releases.to_vec();
    releases.sort_unstable_by_key(|release| release.region_id);
    ensure!(
        !releases
            .windows(2)
            .any(|pair| pair[0].region_id == pair[1].region_id),
        CorruptedWalObjectSnafu {
            reason: "duplicate release record of a region",
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
    let version = if releases.is_empty() {
        FORMAT_VERSION
    } else {
        RELEASE_FORMAT_VERSION
    };
    encode_header(&header, version, &mut output);

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
    encode_footer(&directory, &releases, &mut output)?;
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

/// Returns the length of the object whose footer is `footer` and that carries
/// `releases` release records, derived from the layout: the segments tile the
/// body after the header, and the footer, which holds an entry count, one
/// entry per segment and, in a version 2 object, the release record count and
/// the records, and the trailer follow them.
pub(crate) fn object_len(footer: &[FooterEntry], releases: usize) -> u64 {
    let segments = footer.iter().map(|entry| entry.segment_len).sum::<u64>();
    let release_records = if releases == 0 {
        0
    } else {
        FOOTER_COUNT_LEN + releases * RELEASE_LEN
    };
    let framing = HEADER_LEN
        + FOOTER_COUNT_LEN
        + footer.len() * FOOTER_ENTRY_LEN
        + release_records
        + TRAILER_LEN;
    segments + framing as u64
}

/// Decodes and verifies the header from the first [`HEADER_LEN`] bytes of an
/// object.
pub(crate) fn decode_header(bytes: &[u8]) -> Result<Header> {
    decode_versioned_header(bytes).map(|(header, _)| header)
}

/// Decodes the header as [`decode_header`] does, together with the format
/// version, which says how to decode the footer.
pub(crate) fn decode_versioned_header(bytes: &[u8]) -> Result<(Header, u16)> {
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
        version == FORMAT_VERSION || version == RELEASE_FORMAT_VERSION,
        CorruptedWalObjectSnafu {
            reason: format!(
                "unsupported format version {version}, expected version {FORMAT_VERSION} or {RELEASE_FORMAT_VERSION}"
            ),
        }
    );
    let object_seq = reader.u64("header")?;
    let epoch = reader.u64("header")?;
    let prev_object_seq = reader.u64("header")?;
    let prev_epoch = reader.u64("header")?;
    let reclaim_boundary = reader.u64("header")?;
    let header_crc32 = reader.u32("header")?;
    let checksum = crc32fast::hash(&bytes[..HEADER_LEN - 4]);
    ensure!(
        checksum == header_crc32,
        CorruptedWalObjectSnafu {
            reason: checksum_mismatch("header", header_crc32, checksum),
        }
    );
    ensure!(
        reclaim_boundary <= object_seq,
        CorruptedWalObjectSnafu {
            reason: format!(
                "reclaim boundary {reclaim_boundary} is above the object sequence {object_seq}"
            ),
        }
    );
    let prev = (prev_object_seq != NO_PREDECESSOR).then_some(ChainLink {
        object_seq: prev_object_seq,
        epoch: prev_epoch,
    });
    let header = Header {
        object_seq,
        epoch,
        prev,
        reclaim_boundary,
    };
    Ok((header, version))
}

/// Decodes the trailer from the last [`TRAILER_LEN`] bytes of an object.
pub(crate) fn decode_trailer(bytes: &[u8]) -> Result<FixedTrailer> {
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

/// Decodes the footer that `trailer` points at in an object of format
/// `version`: its segment entries and, in version 2, its release records.
pub(crate) fn decode_footer(
    bytes: &[u8],
    trailer: FixedTrailer,
    version: u16,
) -> Result<(Vec<FooterEntry>, Vec<Release>)> {
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
    let entries_len = count
        .checked_mul(FOOTER_ENTRY_LEN)
        .and_then(|len| len.checked_add(FOOTER_COUNT_LEN))
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!("footer declares {count} entries, which overflows its length"),
        })?;
    // A version 2 footer holds at least one release record after its entries.
    let release_count = if version == RELEASE_FORMAT_VERSION {
        let release_count = Reader::new(bytes.get(entries_len..).unwrap_or_default())
            .u32("footer release records")? as usize;
        ensure!(
            release_count > 0,
            CorruptedWalObjectSnafu {
                reason: "footer of a version 2 object holds no release record",
            }
        );
        release_count
    } else {
        0
    };
    let expected_len = if release_count == 0 {
        Some(entries_len)
    } else {
        release_count
            .checked_mul(RELEASE_LEN)
            .and_then(|len| len.checked_add(FOOTER_COUNT_LEN))
            .and_then(|len| len.checked_add(entries_len))
    }
    .with_context(|| CorruptedWalObjectSnafu {
        reason: format!(
            "footer declares {release_count} release records, which overflows its length"
        ),
    })?;
    ensure!(
        expected_len == bytes.len(),
        CorruptedWalObjectSnafu {
            reason: format!(
                "footer declares {count} entries and {release_count} release records, expected length {expected_len}, actual {}",
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

    let mut releases = Vec::with_capacity(release_count);
    if release_count > 0 {
        // The release record count, read above.
        reader.u32("footer")?;
    }
    for _ in 0..release_count {
        let region_id = RegionId::from_u64(reader.u64("footer")?);
        if let Some(previous) = releases.last().map(|release: &Release| release.region_id) {
            ensure!(
                previous < region_id,
                CorruptedWalObjectSnafu {
                    reason: if previous == region_id {
                        format!("duplicate release record for region {region_id}")
                    } else {
                        format!(
                            "release records are not ordered by region, {previous} precedes {region_id}"
                        )
                    },
                }
            );
        }
        releases.push(Release {
            region_id,
            entry_id: reader.u64("footer")?,
        });
    }
    Ok((directory, releases))
}

/// Decodes the segment that `entry` describes.
pub(crate) fn decode_segment(bytes: &[u8], entry: &FooterEntry) -> Result<Vec<Record>> {
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

/// Lower bound on the length of an object: header, a footer holding only its
/// entry count and the trailer.
pub(crate) const MIN_OBJECT_LEN: usize = HEADER_LEN + 4 + TRAILER_LEN;

/// Locates the footer inside an object of `object_len` bytes from its trailer.
/// The footer must follow the header and end where the trailer starts.
pub(crate) fn footer_range(trailer: FixedTrailer, object_len: usize) -> Result<Range<usize>> {
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
pub(crate) fn verify_segment_ranges(footer: &[FooterEntry], footer_start: usize) -> Result<()> {
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
pub(crate) fn decode_object(bytes: &[u8]) -> Result<DecodedObject> {
    ensure!(
        bytes.len() >= MIN_OBJECT_LEN,
        CorruptedWalObjectSnafu {
            reason: truncated("object", bytes.len(), MIN_OBJECT_LEN),
        }
    );

    let (header, version) = decode_versioned_header(bytes)?;
    let trailer_start = bytes.len() - TRAILER_LEN;
    let trailer = decode_trailer(&bytes[trailer_start..])?;
    let footer_range = footer_range(trailer, bytes.len())?;
    let footer_start = footer_range.start;

    let (footer, releases) = decode_footer(&bytes[footer_range], trailer, version)?;
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
        releases,
    })
}

fn encode_header(header: &Header, version: u16, output: &mut BytesMut) {
    let start = output.len();
    output.put_slice(HEADER_MAGIC);
    output.put_u16(version);
    output.put_u64(header.object_seq);
    output.put_u64(header.epoch);
    let prev = header.prev.unwrap_or(ChainLink {
        object_seq: NO_PREDECESSOR,
        epoch: 0,
    });
    output.put_u64(prev.object_seq);
    output.put_u64(prev.epoch);
    output.put_u64(header.reclaim_boundary);
    output.put_u32(crc32fast::hash(&output[start..]));
}

fn encode_footer(
    directory: &[FooterEntry],
    releases: &[Release],
    output: &mut BytesMut,
) -> Result<()> {
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
    if !releases.is_empty() {
        output.put_u32(to_u32(releases.len(), "release record count")?);
        for release in releases {
            output.put_u64(release.region_id.as_u64());
            output.put_u64(release.entry_id);
        }
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
            epoch: 5,
            prev: Some(ChainLink {
                object_seq: 40,
                epoch: 4,
            }),
            reclaim_boundary: 37,
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
        let encoded = encode_object(header(), &records(), &[]).unwrap();
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
        let encoded = encode_object(header(), &records(), &[]).unwrap();
        let trailer_start = encoded.bytes.len() - TRAILER_LEN;
        let trailer = decode_trailer(&encoded.bytes[trailer_start..]).unwrap();
        assert_eq!(encoded.trailer, trailer);

        let footer_start = trailer.footer_offset as usize;
        let footer_end = footer_start + trailer.footer_len as usize;
        let footer = decode_footer(
            &encoded.bytes[footer_start..footer_end],
            trailer,
            FORMAT_VERSION,
        )
        .unwrap()
        .0;
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
        let encoded = encode_object(header(), &records(), &[]).unwrap();
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
        let encoded = encode_object(header(), &records(), &[]).unwrap();

        let mut bad_magic = encoded.bytes.to_vec();
        bad_magic[0] ^= 1;
        assert_corrupted(decode_object(&bad_magic), "invalid header magic");

        let mut bad_version = encoded.bytes.to_vec();
        bad_version[8..10].copy_from_slice(&(RELEASE_FORMAT_VERSION + 1).to_be_bytes());
        assert_corrupted(decode_object(&bad_version), "unsupported format version 3");

        // Every field after the version is covered by the header checksum,
        // which recovery verifies without reading the whole object.
        for offset in [10, 18, 26, 34, 42, HEADER_LEN - 1] {
            let mut bad_field = encoded.bytes.to_vec();
            bad_field[offset] ^= 1;
            assert_corrupted(decode_header(&bad_field), "header checksum mismatch");
        }
    }

    #[test]
    fn test_format_round_trips_the_first_object_of_a_chain() {
        let first = Header {
            prev: None,
            ..header()
        };
        let encoded = encode_object(first.clone(), &records(), &[]).unwrap();
        assert_eq!(first, decode_object(&encoded.bytes).unwrap().header);
    }

    #[test]
    fn test_format_rejects_a_reclaim_boundary_above_the_object() {
        let above = Header {
            reclaim_boundary: header().object_seq + 1,
            ..header()
        };
        let encoded = encode_object(above, &records(), &[]).unwrap();
        assert_corrupted(
            decode_header(&encoded.bytes),
            "reclaim boundary 43 is above the object sequence 42",
        );
    }

    #[test]
    fn test_format_rejects_trailer_magic_and_invalid_footer_range() {
        let encoded = encode_object(header(), &records(), &[]).unwrap();

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
        let encoded = encode_object(header(), &records(), &[]).unwrap();

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
        let encoded = encode_object(header(), &records(), &[]).unwrap();
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
    fn test_format_round_trips_an_object_without_records() {
        let encoded = encode_object(header(), &[], &[]).unwrap();
        assert!(encoded.footer.is_empty());
        assert_eq!(
            MIN_OBJECT_LEN,
            encoded.bytes.len(),
            "an empty object is a header, an empty footer and the trailer"
        );
        assert_eq!(encoded.bytes.len() as u64, object_len(&encoded.footer, 0));
        let decoded = decode_object(&encoded.bytes).unwrap();
        assert_eq!(header(), decoded.header);
        assert!(decoded.records.is_empty());
        assert!(decoded.footer.is_empty());
    }

    fn release(region_id: RegionId, entry_id: u64) -> Release {
        Release {
            region_id,
            entry_id,
        }
    }

    #[test]
    fn test_format_round_trips_release_records() {
        let (one, two) = (RegionId::new(1, 1), RegionId::new(2, 1));
        let releases = [release(two, u64::MAX), release(one, 7)];

        // An object of release records alone is version 2 and holds no
        // segment; its release records are ordered by region.
        let encoded = encode_object(header(), &[], &releases).unwrap();
        assert_eq!(RELEASE_FORMAT_VERSION.to_be_bytes(), encoded.bytes[8..10]);
        assert!(encoded.footer.is_empty());
        assert_eq!(MIN_OBJECT_LEN + 4 + 2 * RELEASE_LEN, encoded.bytes.len());
        assert_eq!(encoded.bytes.len() as u64, object_len(&encoded.footer, 2));
        let decoded = decode_object(&encoded.bytes).unwrap();
        assert_eq!(header(), decoded.header);
        assert!(decoded.records.is_empty());
        assert_eq!(
            vec![release(one, 7), release(two, u64::MAX)],
            decoded.releases
        );

        // The segments of an object with release records are laid out as in
        // version 1, and a region may have both.
        let without = encode_object(header(), &records(), &[]).unwrap();
        assert_eq!(FORMAT_VERSION.to_be_bytes(), without.bytes[8..10]);
        let encoded = encode_object(header(), &records(), &releases[1..]).unwrap();
        assert_eq!(without.footer, encoded.footer);
        assert_eq!(encoded.bytes.len() as u64, object_len(&encoded.footer, 1));
        let decoded = decode_object(&encoded.bytes).unwrap();
        assert_eq!(without.footer, decoded.footer);
        assert_eq!(3, decoded.records.len());
        assert_eq!(releases[1..], decoded.releases);

        assert_corrupted(
            encode_object(header(), &[], &[release(one, 1), release(one, 2)]),
            "duplicate release record",
        );
    }

    /// A trailer that points at `footer` and carries its checksum.
    fn trailer_of(footer: &[u8]) -> FixedTrailer {
        FixedTrailer {
            footer_offset: HEADER_LEN as u64,
            footer_len: footer.len() as u64,
            footer_crc32: crc32fast::hash(footer),
            object_crc32: 0,
        }
    }

    #[test]
    fn test_format_rejects_invalid_release_records() {
        let (one, two) = (RegionId::new(1, 1), RegionId::new(2, 1));
        // A footer without segments that declares `count` release records
        // and holds `releases`.
        let footer = |releases: &[(RegionId, u64)], count: u32| {
            let mut footer = BytesMut::new();
            footer.put_u32(0);
            footer.put_u32(count);
            for (region_id, entry_id) in releases {
                footer.put_u64(region_id.as_u64());
                footer.put_u64(*entry_id);
            }
            footer.to_vec()
        };
        let decode =
            |footer: &[u8], version| decode_footer(footer, trailer_of(footer), version).map(|_| ());

        let valid = footer(&[(one, 7), (two, 3)], 2);
        assert_eq!(
            vec![release(one, 7), release(two, 3)],
            decode_footer(&valid, trailer_of(&valid), RELEASE_FORMAT_VERSION)
                .unwrap()
                .1
        );
        // A version 1 footer ends after its entries.
        assert_corrupted(
            decode(&valid, FORMAT_VERSION),
            "footer declares 0 entries and 0 release records, expected length 4, actual 40",
        );
        assert_corrupted(
            decode(&footer(&[], 0)[..4], RELEASE_FORMAT_VERSION),
            "truncated footer release records",
        );
        assert_corrupted(
            decode(&footer(&[], 0), RELEASE_FORMAT_VERSION),
            "footer of a version 2 object holds no release record",
        );
        assert_corrupted(
            decode(&footer(&[(one, 7)], 2), RELEASE_FORMAT_VERSION),
            "footer declares 0 entries and 2 release records, expected length 40, actual 24",
        );
        assert_corrupted(
            decode(&footer(&[(two, 3), (one, 7)], 2), RELEASE_FORMAT_VERSION),
            "release records are not ordered by region",
        );
        assert_corrupted(
            decode(&footer(&[(one, 3), (one, 7)], 2), RELEASE_FORMAT_VERSION),
            &format!("duplicate release record for region {one}"),
        );
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
            encode_object(header(), &records, &[]),
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

    /// A version 1 object holding entries 1, 2 and 3 of region 1 and entries
    /// 10 and 11 of region 2. The bytes, frozen with their layout annotated
    /// under `tests/compatibility/wal/object_store`, were derived from the
    /// layout described in the module documentation, independently of
    /// [`encode_object`], so a change to field order, endianness or checksum
    /// coverage fails this test even when the encoder and the decoder change
    /// together.
    const FIXTURE_V1: &[u8] =
        include_bytes!("../../../../tests/compatibility/wal/object_store/version_1.wal");

    fn fixture_records() -> Vec<Record> {
        [
            (RegionId::new(1, 1), 1, &b"a"[..]),
            (RegionId::new(1, 1), 2, b"bb"),
            (RegionId::new(1, 1), 3, b"ccc"),
            (RegionId::new(2, 1), 10, b"x"),
            (RegionId::new(2, 1), 11, b"yy"),
        ]
        .into_iter()
        .map(|(region_id, entry_id, payload)| Record {
            region_id,
            entry_id,
            payload: Bytes::copy_from_slice(payload),
        })
        .collect()
    }

    #[test]
    fn test_format_matches_version_1_fixture() {
        let fixture = FIXTURE_V1.to_vec();
        let header = Header {
            object_seq: 7,
            epoch: 3,
            prev: Some(ChainLink {
                object_seq: 6,
                epoch: 2,
            }),
            reclaim_boundary: 5,
        };
        let footer = vec![
            FooterEntry {
                region_id: RegionId::new(1, 1),
                min_entry_id: 1,
                max_entry_id: 3,
                entry_count: 3,
                segment_offset: 54,
                segment_len: 54,
                segment_crc32: 0x25ac0486,
            },
            FooterEntry {
                region_id: RegionId::new(2, 1),
                min_entry_id: 10,
                max_entry_id: 11,
                entry_count: 2,
                segment_offset: 108,
                segment_len: 39,
                segment_crc32: 0x65dc08ec,
            },
        ];
        assert_eq!(279, fixture.len());

        let decoded = decode_object(&fixture).unwrap();
        assert_eq!(header, decoded.header);
        assert_eq!(footer, decoded.footer);
        assert_eq!(fixture_records(), decoded.records);
        assert_eq!(
            FixedTrailer {
                footer_offset: 147,
                footer_len: 100,
                footer_crc32: 0xd80c9f07,
                object_crc32: 0x65c91839,
            },
            decode_trailer(&fixture[fixture.len() - TRAILER_LEN..]).unwrap()
        );

        // The encoding does not depend on the order records are passed in.
        let mut reversed = fixture_records();
        reversed.reverse();
        for records in [fixture_records(), reversed] {
            let encoded = encode_object(header.clone(), &records, &[]).unwrap();
            assert_eq!(fixture, encoded.bytes.as_ref());
            assert_eq!(footer, encoded.footer);
            assert_eq!(fixture.len() as u64, object_len(&encoded.footer, 0));
        }
    }

    /// A version 2 object holding entries 1 and 2 of region 1, and the
    /// release records of region 1 through entry 2 and of region 2 in full,
    /// frozen and derived like [`FIXTURE_V1`].
    const FIXTURE_V2: &[u8] =
        include_bytes!("../../../../tests/compatibility/wal/object_store/version_2.wal");

    #[test]
    fn test_format_matches_version_2_fixture() {
        let fixture = FIXTURE_V2.to_vec();
        let header = Header {
            object_seq: 9,
            epoch: 3,
            prev: Some(ChainLink {
                object_seq: 8,
                epoch: 3,
            }),
            reclaim_boundary: 6,
        };
        let records = fixture_records()[..2].to_vec();
        let footer = vec![FooterEntry {
            region_id: RegionId::new(1, 1),
            min_entry_id: 1,
            max_entry_id: 2,
            entry_count: 2,
            segment_offset: 54,
            segment_len: 39,
            segment_crc32: 0xb79527ef,
        }];
        let releases = vec![
            release(RegionId::new(1, 1), 2),
            release(RegionId::new(2, 1), u64::MAX),
        ];
        assert_eq!(213, fixture.len());

        let decoded = decode_object(&fixture).unwrap();
        assert_eq!(header, decoded.header);
        assert_eq!(footer, decoded.footer);
        assert_eq!(records, decoded.records);
        assert_eq!(releases, decoded.releases);
        assert_eq!(
            FixedTrailer {
                footer_offset: 93,
                footer_len: 88,
                footer_crc32: 0xfab0fe5b,
                object_crc32: 0x30f82508,
            },
            decode_trailer(&fixture[fixture.len() - TRAILER_LEN..]).unwrap()
        );

        // The encoding does not depend on the order release records are
        // passed in.
        let mut reversed = releases.clone();
        reversed.reverse();
        for releases in [releases, reversed] {
            let encoded = encode_object(header.clone(), &records, &releases).unwrap();
            assert_eq!(fixture, encoded.bytes.as_ref());
            assert_eq!(footer, encoded.footer);
            assert_eq!(fixture.len() as u64, object_len(&encoded.footer, 2));
        }
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
