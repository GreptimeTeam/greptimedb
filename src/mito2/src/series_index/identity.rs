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

//! The exact series identity that SSTs already store.
//!
//! `tsid` is a hash, so two distinct series can share one [`MetricSeriesId`]. The
//! sparse primary key stored by the SST is exact, so every place that decides
//! whether two rows belong to the same series decides it on [`ExactSeriesKey`].

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use mito_codec::row_converter::SparseOffsetsCache;
use mito_codec::row_converter::sparse::SparsePrimaryKeyView;
use snafu::ResultExt;
use store_api::storage::ColumnId;

use crate::error::{DecodeSnafu, Result};
use crate::series_index::MetricSeriesId;

/// The identity semantics an artifact was written with.
///
/// This is the one mandatory discriminator every artifact manifest carries. Artifacts
/// written under hash-only identity are structurally valid and perfectly ordered, so
/// nothing else in them distinguishes them from exact ones: an already collapsed series
/// index looks fine and its lost labels cannot be recovered. A manifest that does not
/// name this encoding is therefore not reusable, whatever its coverage says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum IdentityEncoding {
    /// Identity is [`ExactSeriesKey`]: the exact canonical sparse primary key, with
    /// `(table_id, tsid)` only as its coarse, hash-based prefix.
    ExactSparseKey,
}

impl IdentityEncoding {
    /// The encoding new artifacts are written with; readers require exactly this.
    pub(crate) const CURRENT: Self = Self::ExactSparseKey;
}

/// The exact identity of one series: its canonical sparse primary-key bytes.
///
/// The key is the SST's own encoding of
/// `(table_id, tsid, label column-id/value pairs sorted by name, non-null only)`,
/// as produced by [`mito_codec::row_converter::sparse::SparsePrimaryKeyCodec`]. No
/// second label encoding exists: callers hand in the bytes they read from the SST.
///
/// # Ordering
///
/// Identity and order are both defined on the encoded bytes only:
///
/// * `PartialEq`/`Hash` use the bytes, so two keys are the same series iff their
///   encodings are equal.
/// * `Ord` is byte-lexicographic. The reserved `table_id`/`tsid` prefix is fixed
///   width and order preserving, so this order groups every key that shares one
///   [`MetricSeriesId`] together and sorts those groups by `MetricSeriesId`.
///   Inside one group it orders by the encoded `(column_id, value)` label sequence.
///
/// This is also the order SSTs store rows in, so artifact writers receive input in
/// key order and can reject regressions instead of guessing.
///
/// `id` is always derived from `key` and never participates in comparison: every
/// comparison below reads `key` only.
///
/// # Assumptions
///
/// This is *storage* identity, not Prometheus label-set identity. It relies on the
/// existing sparse format's invariants, none of which it redefines:
///
/// * `tsid` is a stable hash for a label set within one logical table; a rebuild of
///   the same rows writes the same `tsid`.
/// * Column IDs are stable for a region, so the encoded `(column_id, value)` pairs
///   identify the same labels over time. Re-creating a region with different IDs
///   changes the key bytes.
/// * Labels are encoded in the codec's canonical order (sorted by column name), and
///   null labels are omitted while empty strings are encoded. This type never
///   normalizes either case.
///
/// Equal keys therefore mean "the same series as far as this database stores it".
/// They are not claimed to be equal to any Prometheus series identity.
#[derive(Clone)]
pub(crate) struct ExactSeriesKey {
    /// Order-preserving `(table_id, tsid)` prefix decoded from `key`.
    id: MetricSeriesId,
    /// Canonical sparse primary-key encoding of the whole series.
    key: Box<[u8]>,
}

impl ExactSeriesKey {
    /// Wraps one encoded sparse primary key after validating its framing.
    ///
    /// # Length
    ///
    /// This entry point deliberately has no length cap, and neither does any other:
    /// key length is bounded by the containers that store a key (one key per block,
    /// plus the descriptor, page, and column limits), and a cap enforced on only one
    /// entry point would let a writer publish a key another one refuses. Long labels
    /// are real data — a single 8 KiB label already encodes to a larger sparse key —
    /// so framing validation is the only requirement here.
    pub(crate) fn from_sparse_key(key: &[u8], offsets: &mut SparseOffsetsCache) -> Result<Self> {
        let mut view = SparsePrimaryKeyView::new(key, offsets).context(DecodeSnafu)?;
        // `new` validates the reserved prefix only; walk the labels so a truncated or
        // trailing-garbage key cannot become an identity that happens to share a prefix.
        view.visit_label_ids(|_| Ok(())).context(DecodeSnafu)?;
        Ok(Self::from_view(&view, key))
    }

    /// The reserved `(table_id, tsid)` prefix. It is a coarse bucket, not identity.
    pub(crate) fn id(&self) -> MetricSeriesId {
        self.id
    }

    /// The canonical bytes; the only thing that decides identity.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.key
    }

    /// Builds the key from a view already validated against `key`.
    fn from_view(view: &SparsePrimaryKeyView<'_, '_>, key: &[u8]) -> Self {
        Self {
            id: MetricSeriesId {
                table_id: view.table_id(),
                tsid: view.tsid(),
            },
            key: key.into(),
        }
    }
}

// Only `key` participates in identity. `id` is a prefix function of `key`, so a
// future field cannot accidentally join (or reorder) the identity.

impl PartialEq for ExactSeriesKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for ExactSeriesKey {}

impl PartialOrd for ExactSeriesKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactSeriesKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl fmt::Debug for ExactSeriesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExactSeriesKey")
            .field("id", &self.id)
            .field("key", &Hex(&self.key))
            .finish()
    }
}

/// Lets a map keyed by the exact key be probed with the raw canonical bytes it was
/// built from, so hot loops do not allocate a key per row. `Ord`/`Eq` are byte
/// comparisons, which is exactly `[u8]`'s order, so the borrow is consistent.
impl std::borrow::Borrow<[u8]> for ExactSeriesKey {
    fn borrow(&self) -> &[u8] {
        &self.key
    }
}

impl Hash for ExactSeriesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

/// Prints at most [`HEX_LIMIT`] bytes of a key, so logs stay bounded.
struct Hex<'a>(&'a [u8]);

const HEX_LIMIT: usize = 64;

impl fmt::Debug for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.iter().take(HEX_LIMIT) {
            write!(f, "{byte:02x}")?;
        }
        if self.0.len() > HEX_LIMIT {
            write!(f, "...({} bytes)", self.0.len())?;
        }
        Ok(())
    }
}

/// Decodes one sparse primary key into its exact identity and `tag_columns` labels.
///
/// This is the only decoder artifact writers use, so the identity they aggregate and
/// the labels they persist always come from the same key.
pub(crate) fn decode_series_identity(
    primary_key: &[u8],
    tag_columns: &[(ColumnId, String)],
    offsets: &mut SparseOffsetsCache,
    buf: &mut Vec<u8>,
) -> Result<(ExactSeriesKey, Vec<Option<String>>)> {
    let mut view = SparsePrimaryKeyView::new(primary_key, offsets).context(DecodeSnafu)?;
    // Validate every label once and cache its offset; the tag lookups below reuse it,
    // and a truncated key never reaches a persisted row.
    view.visit_label_ids(|_| Ok(())).context(DecodeSnafu)?;
    let key = ExactSeriesKey::from_view(&view, primary_key);
    let mut tags = Vec::with_capacity(tag_columns.len());
    for (column_id, _) in tag_columns {
        tags.push(
            view.label(*column_id, buf)
                .context(DecodeSnafu)?
                .map(str::to_owned),
        );
    }
    Ok((key, tags))
}

/// Decodes only the reserved `(table_id, tsid)` prefix, without allocating the key.
///
/// Callers that already compared the raw bytes must not use this to decide identity;
/// it only exists where the coarse bucket is the output.
pub(crate) fn decode_series_id(
    primary_key: &[u8],
    offsets: &mut SparseOffsetsCache,
) -> Result<MetricSeriesId> {
    let view = SparsePrimaryKeyView::new(primary_key, offsets).context(DecodeSnafu)?;
    Ok(MetricSeriesId {
        table_id: view.table_id(),
        tsid: view.tsid(),
    })
}

#[cfg(test)]
mod tests {
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn metadata() -> std::sync::Arc<store_api::metadata::RegionMetadata> {
        std::sync::Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ))
    }

    fn key(tags: &[&str], table_id: u32, tsid: u64) -> Vec<u8> {
        new_sparse_primary_key(tags, &metadata(), table_id, tsid)
    }

    #[test]
    fn identity_is_the_exact_key_not_the_tsid() {
        let mut offsets = SparseOffsetsCache::new();
        let first =
            ExactSeriesKey::from_sparse_key(&key(&["host-020078", "z"], 1, 7), &mut offsets)
                .unwrap();
        let partner =
            ExactSeriesKey::from_sparse_key(&key(&["host-029838", "z"], 1, 7), &mut offsets)
                .unwrap();
        assert_eq!(first.id(), partner.id());
        assert_ne!(first, partner);
        assert_ne!(first.bytes(), partner.bytes());
        let same = ExactSeriesKey::from_sparse_key(&key(&["host-020078", "z"], 1, 7), &mut offsets)
            .unwrap();
        assert_eq!(first, same);
    }

    #[test]
    fn order_groups_equal_identifiers_and_sorts_groups_by_identifier() {
        let mut offsets = SparseOffsetsCache::new();
        let mut keys = Vec::new();
        for (table_id, tsid, label) in [
            (1, 2, "b"),
            (1, 2, "a"),
            (1, 3, "a"),
            (2, 1, "a"),
            (1, 2, "c"),
        ] {
            keys.push(
                ExactSeriesKey::from_sparse_key(&key(&[label, "z"], table_id, tsid), &mut offsets)
                    .unwrap(),
            );
        }
        let mut sorted = keys.clone();
        sorted.sort();
        // Byte order sorts by (table_id, tsid) first, then by the encoded labels, and
        // every key sharing an identifier stays adjacent.
        let ids: Vec<_> = sorted.iter().map(|key| key.id()).collect();
        assert_eq!(
            ids,
            vec![
                MetricSeriesId {
                    table_id: 1,
                    tsid: 2
                },
                MetricSeriesId {
                    table_id: 1,
                    tsid: 2
                },
                MetricSeriesId {
                    table_id: 1,
                    tsid: 2
                },
                MetricSeriesId {
                    table_id: 1,
                    tsid: 3
                },
                MetricSeriesId {
                    table_id: 2,
                    tsid: 1
                },
            ]
        );
        // Ordering is decided on the encoded bytes alone.
        for pair in keys.windows(2) {
            assert_eq!(pair[0].cmp(&pair[1]), pair[0].bytes().cmp(pair[1].bytes()));
        }
        assert!(sorted.is_sorted());
    }

    #[test]
    fn long_keys_are_identity_not_an_error() {
        let mut offsets = SparseOffsetsCache::new();
        // One 8 KiB label already encodes to a sparse key past 8 KiB. Both entry points
        // must accept it: the containers that store a key bound its length, and a cap on
        // one entry point alone would publish keys another refuses.
        let label = "x".repeat(8 * 1024);
        let encoded = key(&[&label, "z"], 1, 7);
        assert!(encoded.len() > 8 * 1024, "{}", encoded.len());
        let exact = ExactSeriesKey::from_sparse_key(&encoded, &mut offsets).unwrap();
        assert_eq!(encoded, exact.bytes());
        let tag_columns = [(0, "tag_0".to_string()), (1, "tag_1".to_string())];
        let (decoded, tags) =
            decode_series_identity(&encoded, &tag_columns, &mut offsets, &mut Vec::new()).unwrap();
        assert_eq!(exact, decoded);
        assert_eq!(Some(label), tags[0]);
    }

    #[test]
    fn malformed_keys_are_rejected() {
        let mut offsets = SparseOffsetsCache::new();
        assert!(ExactSeriesKey::from_sparse_key(&[], &mut offsets).is_err());
        assert!(ExactSeriesKey::from_sparse_key(&[0; 13], &mut offsets).is_err());
        let good = key(&["a", "b"], 1, 1);
        assert!(ExactSeriesKey::from_sparse_key(&good[..good.len() - 1], &mut offsets).is_err());
        assert_eq!(
            decode_series_id(&good, &mut offsets).unwrap(),
            MetricSeriesId {
                table_id: 1,
                tsid: 1
            }
        );
    }
}
