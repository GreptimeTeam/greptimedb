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
use std::sync::OnceLock;

use mito_codec::row_converter::SparseOffsetsCache;
use mito_codec::row_converter::sparse::{SparsePrimaryKeyCodec, SparsePrimaryKeyView};
use snafu::ResultExt;
use store_api::storage::ColumnId;

use crate::error::{DecodeSnafu, Result};
use crate::series_index::MetricSeriesId;
use crate::series_index::columnar::invalid;

/// The identity semantics *and storage layout* an artifact was written with.
///
/// This is the one mandatory discriminator every artifact manifest carries. Artifacts
/// written under hash-only identity are structurally valid and perfectly ordered, so
/// nothing else in them distinguishes them from exact ones: an already collapsed series
/// index looks fine and its lost labels cannot be recovered. A manifest that does not
/// name this encoding is therefore not reusable, whatever its coverage says.
///
/// The serialized value names the layout as well as the identity, because two artifacts can
/// store the same exact key in incompatible places: the samples format kept one copy per block
/// inside its pages, then moved to one copy per series in a dedicated key area with a stored
/// checksum. An artifact of the older layout is structurally valid and ordered too, so without
/// the layout in this value maintenance would mark it reusable and only a part read would fail.
// Every variant starts with `ExactSparseKey` because that is the identity they all store; the
// suffix names the *layout* that stores it, and the name is also the serialized value every
// artifact manifest carries, so a shorter variant name would rename the wire contract.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum IdentityEncoding {
    /// Identity is [`ExactSeriesKey`], stored inside the artifact's own pages: the
    /// series-index layout, and the retired samples layout that repeated the key in every
    /// block descriptor.
    #[serde(rename = "exact-sparse-key")]
    ExactSparseKey,
    /// Identity is [`ExactSeriesKey`], stored once per series in a part-level key area after
    /// the column payloads, each whole key followed by its checksum.
    #[serde(rename = "exact-sparse-key-area")]
    ExactSparseKeyArea,
    /// Identity is [`ExactSeriesKey`], stored once per series in a part-level key area that
    /// keeps only the bytes after the key's fixed [`SERIES_KEY_PREFIX_LEN`]-byte
    /// `(table_id, tsid)` prefix, followed by the checksum of the whole reconstructed key.
    ///
    /// The prefix is not lost: every block descriptor carries its series' `(table_id, tsid)` as
    /// the block's identifier, and the sparse key codec rebuilds the exact prefix from it. An
    /// artifact of this layout is therefore indistinguishable, byte for byte, from one of
    /// [`Self::ExactSparseKeyArea`] once its keys are reconstructed, but it is much smaller: those
    /// 22 bytes are stored once per series in the descriptor instead of once per key copy.
    #[serde(rename = "exact-sparse-key-area-suffix")]
    ExactSparseKeyAreaSuffix,
}

impl IdentityEncoding {
    /// The encoding new samples artifacts are written with; their readers require exactly this.
    ///
    /// Distinct from the series-index encoding on purpose: it names the key-area layout, so an
    /// artifact written before the key area cannot be taken for a current one, and an artifact
    /// written before the suffix layout keeps naming the whole-key layout it really has.
    pub(crate) const CURRENT_SAMPLES: Self = Self::ExactSparseKeyAreaSuffix;

    /// The encoding new series-index artifacts are written with; their readers require this.
    pub(crate) const CURRENT_SERIES_INDEX: Self = Self::ExactSparseKey;
}

/// Length of the reserved `(table_id, tsid)` prefix every sparse primary key starts with.
///
/// It is the codec's encoding of `(table_id, tsid)` itself — two reserved column ids, two non-null
/// markers, a u32 table id, and a u64 tsid — and it is fixed width, which is why it can be left
/// out of the key area and rebuilt from a block descriptor's identifier.
pub(crate) const SERIES_KEY_PREFIX_LEN: usize = 22;

/// The exact prefix [`SparsePrimaryKeyCodec::encode_internal`] writes for `id`.
///
/// Taken from the codec rather than re-spelled here, so a key rebuilt from a stored suffix is
/// byte-identical to the key the SST wrote: identity, ordering, and checksums all compare these
/// bytes.
pub(crate) fn series_key_prefix(id: MetricSeriesId) -> Result<[u8; SERIES_KEY_PREFIX_LEN]> {
    // The reserved prefix does not depend on the codec's labels, so one instance serves every
    // identifier and a part write does not build one per series.
    static CODEC: OnceLock<SparsePrimaryKeyCodec> = OnceLock::new();
    let mut prefix = Vec::with_capacity(SERIES_KEY_PREFIX_LEN);
    CODEC
        .get_or_init(SparsePrimaryKeyCodec::schemaless)
        .encode_internal(id.table_id, id.tsid, &mut prefix)
        .context(DecodeSnafu)?;
    prefix
        .try_into()
        .map_err(|_| invalid("sparse primary key prefix length"))
}

/// Splits `key` into the bytes the samples key area stores for `id`.
///
/// Refuses a key that does not start with `id`'s prefix instead of stripping whatever its first
/// twenty-two bytes are: the stored suffix is recombined under the descriptor's identifier, so a
/// key that belongs to another identifier would silently be read back as a series the writer never
/// wrote.
pub(crate) fn series_key_suffix(key: &[u8], id: MetricSeriesId) -> Result<&[u8]> {
    let prefix = series_key_prefix(id)?;
    key.strip_prefix(prefix.as_slice())
        .ok_or_else(|| invalid("sample series key does not start with its identifier's prefix"))
}

/// Rebuilds one exact key from a block descriptor's identifier and the stored suffix.
///
/// The inverse of [`series_key_suffix`]; the suffix may be empty, which is the key of a series
/// whose labels are all null and whose identity is the reserved prefix alone.
pub(crate) fn series_key_bytes(id: MetricSeriesId, suffix: &[u8]) -> Result<Vec<u8>> {
    let prefix = series_key_prefix(id)?;
    let mut key = Vec::with_capacity(SERIES_KEY_PREFIX_LEN + suffix.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(suffix);
    Ok(key)
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
    fn prefix_is_the_codec_prefix_and_suffixes_rebuild_the_key() {
        let mut offsets = SparseOffsetsCache::new();
        let id = MetricSeriesId {
            table_id: 9,
            tsid: 0x0102_0304_0506_0708,
        };
        let mut expected = Vec::new();
        SparsePrimaryKeyCodec::schemaless()
            .encode_internal(id.table_id, id.tsid, &mut expected)
            .unwrap();
        assert_eq!(SERIES_KEY_PREFIX_LEN, expected.len());
        assert_eq!(
            expected.as_slice(),
            series_key_prefix(id).unwrap().as_slice()
        );
        // A key with no labels is exactly the prefix, so its stored suffix is empty and the
        // rebuilt key is still a valid identity.
        let bare = ExactSeriesKey::from_sparse_key(&expected, &mut offsets).unwrap();
        assert_eq!(id, bare.id());
        assert_eq!(b"".as_slice(), series_key_suffix(bare.bytes(), id).unwrap());
        assert_eq!(expected, series_key_bytes(id, b"").unwrap());
        // A labelled key splits into exactly its label bytes and rebuilds byte for byte.
        let encoded = key(&["host-020078", "z"], id.table_id, id.tsid);
        let labelled = ExactSeriesKey::from_sparse_key(&encoded, &mut offsets).unwrap();
        let suffix = series_key_suffix(&encoded, id).unwrap();
        assert_eq!(&encoded[SERIES_KEY_PREFIX_LEN..], suffix);
        assert_eq!(encoded, series_key_bytes(id, suffix).unwrap());
        assert_eq!(bare.id(), labelled.id());
        assert_ne!(bare, labelled);
        // The same suffix under another identifier is a different key, which is why the split
        // refuses a key whose prefix belongs to someone else.
        let other = MetricSeriesId {
            table_id: 9,
            tsid: 8,
        };
        assert!(series_key_suffix(&encoded, other).is_err());
        assert_ne!(
            encoded,
            series_key_bytes(other, suffix).unwrap(),
            "the suffix alone does not name a series"
        );
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
