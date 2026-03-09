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

//! Encoding and decoding of WAL entries as NATS messages.
//!
//! Each entry (or part of a split entry) becomes one NATS message:
//! - **payload** = raw entry data bytes.
//! - **headers** = lightweight key-value metadata (record type, entry id, region id).
//!
//! Using NATS headers avoids JSON parsing overhead on the hot path while
//! keeping metadata visible in the NATS monitoring UI.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_nats::HeaderMap;
use bytes::Bytes;
use serde::{Deserialize, Serialize}; // used by RecordType derive
use snafu::{OptionExt, ensure};
use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader, NaiveEntry};
use store_api::logstore::provider::{NatsProvider, Provider};
use store_api::storage::RegionId;

use crate::error::{DecodeNatsHeaderSnafu, IllegalSequenceSnafu, MissingKeySnafu, Result};
use crate::nats::EntryId;

// --------------------------------------------------------------------------
// Header keys
// --------------------------------------------------------------------------

/// The current version of the record format.
const VERSION: u8 = 0;

pub(crate) const HEADER_VERSION: &str = "X-GT-Version";
pub(crate) const HEADER_RECORD_TYPE: &str = "X-GT-Record-Type";
pub(crate) const HEADER_ENTRY_ID: &str = "X-GT-Entry-Id";
pub(crate) const HEADER_REGION_ID: &str = "X-GT-Region-Id";

/// Estimated overhead for NATS headers.  A record's data slice is kept to
/// `max_batch_bytes - ESTIMATED_META_SIZE` so the final NATS message stays
/// within the server's `max_payload` limit.
pub(crate) const ESTIMATED_META_SIZE: usize = 256;

// --------------------------------------------------------------------------
// RecordType
// --------------------------------------------------------------------------

/// The type of a WAL record.  Mirrors the Kafka WAL's `RecordType`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(crate) enum RecordType {
    /// The entry fits in a single NATS message.
    Full,
    /// First fragment of a split entry.
    First,
    /// Intermediate fragment; the inner value is the 1-based fragment index.
    Middle(usize),
    /// Last fragment of a split entry.
    Last,
}

impl RecordType {
    fn as_str(&self) -> String {
        match self {
            RecordType::Full => "Full".to_string(),
            RecordType::First => "First".to_string(),
            RecordType::Middle(n) => format!("Middle({})", n),
            RecordType::Last => "Last".to_string(),
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "Full" => Some(RecordType::Full),
            "First" => Some(RecordType::First),
            "Last" => Some(RecordType::Last),
            _ if s.starts_with("Middle(") && s.ends_with(')') => {
                let inner = &s[7..s.len() - 1];
                usize::from_str(inner).ok().map(RecordType::Middle)
            }
            _ => None,
        }
    }
}

// --------------------------------------------------------------------------
// NatsRecord
// --------------------------------------------------------------------------

/// A single NATS WAL record.
///
/// One `NatsRecord` corresponds to one NATS message sent to / received from a
/// JetStream subject.  Large entries are split into multiple `NatsRecord`s
/// using the same `First / Middle / Last` convention as the Kafka WAL.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NatsRecord {
    /// The type (Full / First / Middle / Last).
    pub(crate) tp: RecordType,
    /// The logical entry id set on write (0); overwritten on read with the NATS sequence number.
    pub(crate) entry_id: EntryId,
    /// The region this record belongs to.
    pub(crate) region_id: u64,
    /// Raw entry data bytes.
    pub(crate) data: Vec<u8>,
}

impl NatsRecord {
    /// Encode the record into a NATS `(HeaderMap, Bytes)` pair.
    pub(crate) fn to_nats_message(&self) -> Result<(HeaderMap, Bytes)> {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_VERSION, VERSION.to_string().as_str());
        headers.insert(HEADER_RECORD_TYPE, self.tp.as_str().as_str());
        headers.insert(HEADER_ENTRY_ID, self.entry_id.to_string().as_str());
        headers.insert(HEADER_REGION_ID, self.region_id.to_string().as_str());
        Ok((headers, Bytes::from(self.data.clone())))
    }

    /// Decode a NATS record from a raw message's headers and payload.
    ///
    /// `nats_sequence` is the stream sequence number; it overwrites `entry_id`.
    pub(crate) fn from_nats_message(
        headers: Option<&HeaderMap>,
        payload: Bytes,
        nats_sequence: u64,
    ) -> Result<Self> {
        let headers = headers.context(MissingKeySnafu)?;

        let tp_str = headers
            .get(HEADER_RECORD_TYPE)
            .context(MissingKeySnafu)?
            .to_string();

        let tp = RecordType::parse(&tp_str).context(MissingKeySnafu)?;

        let region_id: u64 = headers
            .get(HEADER_REGION_ID)
            .context(MissingKeySnafu)?
            .to_string()
            .parse()
            .map_err(|e: std::num::ParseIntError| crate::error::Error::DecodeNatsHeader {
                msg: e.to_string(),
                location: snafu::location!(),
            })?;

        Ok(Self {
            tp,
            // Use the NATS sequence number as the entry_id for ordering.
            entry_id: nats_sequence,
            region_id,
            data: payload.to_vec(),
        })
    }
}

// --------------------------------------------------------------------------
// Entry ↔ Vec<NatsRecord> conversion
// --------------------------------------------------------------------------

/// Convert an [`Entry`] into a list of [`NatsRecord`]s ready to publish.
pub(crate) fn convert_to_nats_records(entry: Entry) -> Result<Vec<NatsRecord>> {
    match entry {
        Entry::Naive(e) => Ok(vec![NatsRecord {
            tp: RecordType::Full,
            entry_id: 0,
            region_id: e.region_id.as_u64(),
            data: e.data,
        }]),
        Entry::MultiplePart(e) => {
            let mut records = Vec::with_capacity(e.parts.len());
            for (idx, part) in e.parts.into_iter().enumerate() {
                let tp = match e.headers[idx] {
                    MultiplePartHeader::First => RecordType::First,
                    MultiplePartHeader::Middle(i) => RecordType::Middle(i),
                    MultiplePartHeader::Last => RecordType::Last,
                };
                records.push(NatsRecord {
                    tp,
                    entry_id: 0,
                    region_id: e.region_id.as_u64(),
                    data: part,
                });
            }
            Ok(records)
        }
    }
}

fn convert_to_naive_entry(provider: Arc<NatsProvider>, record: NatsRecord) -> Entry {
    Entry::Naive(NaiveEntry {
        provider: Provider::Nats(provider),
        region_id: RegionId::from_u64(record.region_id),
        entry_id: record.entry_id,
        data: record.data,
    })
}

fn convert_to_multiple_entry(
    provider: Arc<NatsProvider>,
    region_id: RegionId,
    records: Vec<NatsRecord>,
) -> Entry {
    let mut headers = Vec::with_capacity(records.len());
    let mut parts = Vec::with_capacity(records.len());
    let entry_id = records.last().map(|r| r.entry_id).unwrap_or_default();

    for record in records {
        let header = match record.tp {
            RecordType::Full => unreachable!(),
            RecordType::First => MultiplePartHeader::First,
            RecordType::Middle(i) => MultiplePartHeader::Middle(i),
            RecordType::Last => MultiplePartHeader::Last,
        };
        headers.push(header);
        parts.push(record.data);
    }

    Entry::MultiplePart(MultiplePartEntry {
        provider: Provider::Nats(provider),
        region_id,
        entry_id,
        headers,
        parts,
    })
}

/// Drains `buffered_records` and returns any incomplete multi-part entries.
pub(crate) fn remaining_entries(
    provider: &Arc<NatsProvider>,
    buffered_records: &mut HashMap<RegionId, Vec<NatsRecord>>,
) -> Option<Vec<Entry>> {
    if buffered_records.is_empty() {
        None
    } else {
        let mut entries = Vec::with_capacity(buffered_records.len());
        for (region_id, records) in buffered_records.drain() {
            entries.push(convert_to_multiple_entry(
                provider.clone(),
                region_id,
                records,
            ));
        }
        Some(entries)
    }
}

/// Attempt to emit a complete (or incomplete) [`Entry`] from a single [`NatsRecord`].
///
/// This implements the same buffering logic as the Kafka WAL consumer.
pub(crate) fn maybe_emit_entry(
    provider: &Arc<NatsProvider>,
    record: NatsRecord,
    buffered_records: &mut HashMap<RegionId, Vec<NatsRecord>>,
) -> Result<Option<Entry>> {
    let mut entry = None;
    match record.tp {
        RecordType::Full => entry = Some(convert_to_naive_entry(provider.clone(), record)),
        RecordType::First => {
            let region_id = RegionId::from_u64(record.region_id);
            if let Some(records) = buffered_records.insert(region_id, vec![record]) {
                // Incomplete entry from prior window — emit it.
                entry = Some(convert_to_multiple_entry(
                    provider.clone(),
                    region_id,
                    records,
                ));
            }
        }
        RecordType::Middle(seq) => {
            let region_id = RegionId::from_u64(record.region_id);
            let records = buffered_records.entry(region_id).or_default();

            if !records.is_empty() {
                let last = records.last().unwrap();
                let legal = match last.tp {
                    RecordType::First => seq == 1,
                    RecordType::Middle(last_seq) => last_seq + 1 == seq,
                    _ => false,
                };
                ensure!(
                    legal,
                    IllegalSequenceSnafu {
                        error: format!(
                            "Illegal NATS WAL record sequence: last={:?} incoming=Middle({})",
                            last.tp, seq
                        )
                    }
                );
            }
            records.push(record);
        }
        RecordType::Last => {
            let region_id = RegionId::from_u64(record.region_id);
            if let Some(mut records) = buffered_records.remove(&region_id) {
                records.push(record);
                entry = Some(convert_to_multiple_entry(
                    provider.clone(),
                    region_id,
                    records,
                ));
            } else {
                entry = Some(convert_to_multiple_entry(
                    provider.clone(),
                    region_id,
                    vec![record],
                ));
            }
        }
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use super::*;
    use crate::error;

    fn make_record(tp: RecordType, region_id: u64, data: Vec<u8>) -> NatsRecord {
        NatsRecord {
            tp,
            entry_id: 1,
            region_id,
            data,
        }
    }

    #[test]
    fn test_record_type_roundtrip() {
        for tp in [
            RecordType::Full,
            RecordType::First,
            RecordType::Middle(1),
            RecordType::Middle(42),
            RecordType::Last,
        ] {
            assert_eq!(RecordType::parse(&tp.as_str()), Some(tp));
        }
    }

    #[test]
    fn test_nats_record_encode_decode() {
        let record = NatsRecord {
            tp: RecordType::Full,
            entry_id: 0,
            region_id: 12345,
            data: b"hello world".to_vec(),
        };
        let (headers, payload) = record.to_nats_message().unwrap();
        let decoded = NatsRecord::from_nats_message(Some(&headers), payload, 99).unwrap();

        assert_eq!(decoded.tp, RecordType::Full);
        assert_eq!(decoded.entry_id, 99); // overwritten with NATS sequence
        assert_eq!(decoded.region_id, 12345);
        assert_eq!(decoded.data, b"hello world");
    }

    #[test]
    fn test_maybe_emit_entry_full() {
        let provider = Arc::new(NatsProvider::new("greptimedb_wal_subject.0".to_string()));
        let region_id = RegionId::new(1, 1);
        let mut buf = HashMap::new();
        let record = make_record(RecordType::Full, region_id.as_u64(), vec![1; 100]);
        let entry = maybe_emit_entry(&provider, record, &mut buf)
            .unwrap()
            .unwrap();
        assert!(matches!(entry, Entry::Naive(_)));
    }

    #[test]
    fn test_maybe_emit_entry_illegal_seq() {
        let provider = Arc::new(NatsProvider::new("greptimedb_wal_subject.0".to_string()));
        let region_id = RegionId::new(1, 1);
        let mut buf = HashMap::new();
        let r1 = make_record(RecordType::First, region_id.as_u64(), vec![1; 100]);
        maybe_emit_entry(&provider, r1, &mut buf).unwrap();
        // Sequence jumps from expected Middle(1) to Middle(2) — illegal.
        let r2 = make_record(RecordType::Middle(2), region_id.as_u64(), vec![2; 100]);
        let err = maybe_emit_entry(&provider, r2, &mut buf).unwrap_err();
        assert_matches!(err, error::Error::IllegalSequence { .. });
    }
}
