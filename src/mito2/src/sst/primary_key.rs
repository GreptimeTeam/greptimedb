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

//! Schema-bound views of persisted primary key ranges.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use bytes::Bytes;
use common_telemetry::warn;
use datatypes::prelude::ConcreteDataType;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SortField};
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::storage::RegionId;

use crate::error::{DecodePrimaryKeyRangeSnafu, InvalidPrimaryKeyRangeSnafu, Result};
use crate::sst::file::{FileHandle, RegionFileId};

type CachedPrimaryKeyRanges = HashMap<RegionFileId, Option<(Bytes, Bytes)>>;

/// Comparable bounds for one scan or compaction task's pinned schema.
///
/// Keep the cache with the task rather than physical files: old snapshots may
/// need different defaults, and completed tasks must release their cached bounds.
#[derive(Debug)]
pub(crate) struct PrimaryKeyRanges {
    mapper: Arc<PrimaryKeyRangeMapper>,
    ranges: RwLock<CachedPrimaryKeyRanges>,
}

impl PrimaryKeyRanges {
    /// Shares schema constants while keeping file bounds local to this task.
    pub(crate) fn new(mapper: Arc<PrimaryKeyRangeMapper>) -> Self {
        Self {
            mapper,
            ranges: RwLock::new(HashMap::new()),
        }
    }

    /// Returns normalized bounds, or unknown when they cannot safely exclude data.
    /// Invalid statistics are diagnosed once per task without failing the operation.
    pub(crate) fn range(&self, file: &FileHandle) -> Option<(Bytes, Bytes)> {
        if let Some(range) = self.ranges.read().unwrap().get(&file.file_id()) {
            return range.clone();
        }
        // Legacy statistics can arrive after the first comparison. Retry missing bounds.
        let raw = file.raw_primary_key_range()?;
        self.ranges
            .write()
            .unwrap()
            .entry(file.file_id())
            .or_insert_with(|| match self.mapper.map(file.region_id(), raw) {
                Ok(range) => range,
                Err(err) => {
                    warn!(err; "Invalid SST primary key range; using unknown bounds, region: {}, file: {}, schema version: {}",
                        file.region_id(), file.file_id(), self.mapper.schema_version());
                    None
                }
            })
            .clone()
    }
}

/// Schema conversion and default encodings shared by a version's comparison contexts.
#[derive(Debug)]
pub(crate) struct PrimaryKeyRangeMapper {
    metadata: RegionMetadataRef,
    codec: DensePrimaryKeyCodec,
    /// Encoding a constant is independent of the file's missing-prefix length.
    encoded_defaults: Vec<OnceLock<Option<Bytes>>>,
    suffixes: Vec<OnceLock<Option<Bytes>>>,
}

impl PrimaryKeyRangeMapper {
    pub(crate) fn new(metadata: RegionMetadataRef) -> Self {
        let codec = DensePrimaryKeyCodec::new(&metadata);
        let encoded_defaults = (0..codec.num_fields()).map(|_| OnceLock::new()).collect();
        let suffixes = (0..codec.num_fields()).map(|_| OnceLock::new()).collect();
        Self {
            metadata,
            codec,
            encoded_defaults,
            suffixes,
        }
    }

    /// Reuses encoded constants without retaining a chain of old schema snapshots.
    pub(crate) fn with_metadata(&self, metadata: RegionMetadataRef) -> Self {
        let mut mapper = Self::new(metadata);
        for (index, (old, new)) in self
            .metadata
            .primary_key_columns()
            .zip(mapper.metadata.primary_key_columns())
            .enumerate()
        {
            if same_pk_column(old, new) {
                mapper.encoded_defaults[index] = self.encoded_defaults[index].clone();
            }
        }
        mapper
    }

    /// Returns the schema version used to interpret the bounds.
    pub(crate) fn schema_version(&self) -> u64 {
        self.metadata.schema_version
    }

    /// Returns unknown for foreign regions or defaults that cannot be safely encoded.
    /// Invalid bounds return an error so callers can diagnose them before falling back
    /// to unknown. Neither the error nor its source includes the encoded keys.
    pub(crate) fn map(
        &self,
        region_id: RegionId,
        (min, max): (Bytes, Bytes),
    ) -> Result<Option<(Bytes, Bytes)>> {
        ensure!(
            min <= max,
            InvalidPrimaryKeyRangeSnafu {
                reason: "min is greater than max",
            }
        );
        if region_id != self.metadata.region_id {
            return Ok(None);
        }
        // Sparse-to-sparse read compatibility preserves encoded keys verbatim.
        if self.metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
            return Ok(Some((min, max)));
        }
        // Ordinary same-region Dense ALTER only appends PK fields and preserves
        // prefix order/types. SyncColumns violating that invariant is out of scope.
        let prefix_len = self.range_prefix_len(&min, &max)?;
        if prefix_len == self.codec.num_fields() {
            return Ok(Some((min, max)));
        }
        let Some(suffix) = self.suffixes[prefix_len]
            .get_or_init(|| self.encode_suffix(prefix_len))
            .as_ref()
        else {
            return Ok(None);
        };
        // Fixed-layout Dense keys are prefix-free, so appending one constant
        // suffix is monotone. Transform each file before aggregating any ranges.
        Ok(Some((
            append_suffix(min, suffix),
            append_suffix(max, suffix),
        )))
    }

    fn range_prefix_len(&self, min: &[u8], max: &[u8]) -> Result<usize> {
        let min_len = self
            .codec
            .decode_prefix_len(min)
            .context(DecodePrimaryKeyRangeSnafu { endpoint: "min" })?;
        let max_len = self
            .codec
            .decode_prefix_len(max)
            .context(DecodePrimaryKeyRangeSnafu { endpoint: "max" })?;
        ensure!(
            min_len == max_len,
            InvalidPrimaryKeyRangeSnafu {
                reason: format!(
                    "endpoints have different field counts: min {min_len}, max {max_len}"
                ),
            }
        );
        Ok(min_len)
    }

    fn encode_suffix(&self, prefix_len: usize) -> Option<Bytes> {
        let mut suffix = Vec::with_capacity(self.codec.num_fields() - prefix_len);
        for (index, column) in self
            .metadata
            .primary_key_columns()
            .enumerate()
            .skip(prefix_len)
        {
            let encoded = self.encoded_defaults[index]
                .get_or_init(|| encode_default(column))
                .as_ref()?;
            suffix.extend_from_slice(encoded);
        }
        Some(suffix.into())
    }
}

/// Changes to fields, indexes or schema version alone do not invalidate PK views.
pub(crate) fn primary_key_metadata_eq(left: &RegionMetadata, right: &RegionMetadata) -> bool {
    left.region_id == right.region_id
        && left.primary_key_encoding == right.primary_key_encoding
        && left.primary_key.len() == right.primary_key.len()
        && left
            .primary_key_columns()
            .zip(right.primary_key_columns())
            .all(|(left, right)| same_pk_column(left, right))
}

fn same_pk_column(left: &ColumnMetadata, right: &ColumnMetadata) -> bool {
    left.column_id == right.column_id
        && left.column_schema.data_type == right.column_schema.data_type
        && left.column_schema.is_nullable() == right.column_schema.is_nullable()
        && left.column_schema.default_constraint() == right.column_schema.default_constraint()
}

/// Missing, impure or unencodable defaults cannot produce exact bounds.
fn encode_default(column: &ColumnMetadata) -> Option<Bytes> {
    let schema = &column.column_schema;
    if schema.is_default_impure() {
        return None;
    }
    let default = schema.create_default().ok()??;
    let field = SortField::new(schema.data_type.clone());
    if default.is_null() {
        // Dense uses one NULL marker per field, but unsupported types must stay unknown.
        return match field.encode_data_type() {
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Struct(_)
            | ConcreteDataType::Dictionary(_) => None,
            _ => Some(Bytes::from_static(&[0])),
        };
    }
    let mut encoded = Vec::new();
    DensePrimaryKeyCodec::with_fields(vec![(column.column_id, field)])
        .encode_values(&[(column.column_id, default)], &mut encoded)
        .ok()?;
    Some(encoded.into())
}

fn append_suffix(key: Bytes, suffix: &Bytes) -> Bytes {
    let mut completed = Vec::with_capacity(key.len() + suffix.len());
    completed.extend_from_slice(&key);
    completed.extend_from_slice(suffix);
    completed.into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_time::Timestamp;
    use datatypes::prelude::{ConcreteDataType, Value};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use rstest::rstest;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::run::files_overlap_inclusive;
    use crate::manifest::action::RegionEdit;
    use crate::memtable::time_partition::TimePartitions;
    use crate::memtable::time_series::TimeSeriesMemtableBuilder;
    use crate::region::version::{VersionBuilder, VersionRef};
    use crate::sst::file::{FileHandle, FileMeta};
    use crate::test_util::new_noop_file_purger;

    fn metadata(defaults: &[Value]) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        for (id, value) in defaults.iter().enumerate() {
            let data_type = if value.is_null() {
                ConcreteDataType::string_datatype()
            } else {
                value.data_type()
            };
            builder.push_column_metadata(ColumnMetadata {
                column_id: id as u32,
                semantic_type: SemanticType::Tag,
                column_schema: ColumnSchema::new(format!("tag_{id}"), data_type, true)
                    .with_default_constraint(Some(ColumnDefaultConstraint::Value(value.clone())))
                    .unwrap(),
            });
        }
        builder.push_column_metadata(ColumnMetadata {
            column_id: 100,
            semantic_type: SemanticType::Timestamp,
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        });
        builder.primary_key((0..defaults.len() as u32).collect());
        Arc::new(builder.build().unwrap())
    }

    fn encode(metadata: &RegionMetadataRef, values: &[Value]) -> Bytes {
        let mut bytes = Vec::new();
        let values: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(id, value)| (id as u32, value.clone()))
            .collect();
        DensePrimaryKeyCodec::new(metadata)
            .encode_values(&values, &mut bytes)
            .unwrap();
        bytes.into()
    }

    fn file_meta(metadata: &RegionMetadataRef, values: &[Value]) -> FileMeta {
        let key = encode(metadata, values);
        FileMeta {
            region_id: metadata.region_id,
            file_id: FileId::random(),
            time_range: (Timestamp::new_millisecond(0), Timestamp::new_millisecond(1)),
            primary_key_min: Some(key.clone()),
            primary_key_max: Some(key),
            ..Default::default()
        }
    }

    fn version_with_files(metadata: RegionMetadataRef, files: Vec<FileMeta>) -> VersionRef {
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            Arc::new(TimeSeriesMemtableBuilder::default()),
            0,
            None,
        ));
        Arc::new(
            VersionBuilder::new(metadata, mutable)
                .add_files(new_noop_file_purger(), files.into_iter())
                .build(),
        )
    }

    #[test]
    fn test_field_addition_preserves_sst_range_views() {
        let metadata = metadata(&[Value::from(""), Value::from("default")]);
        let raw = file_meta(&metadata, &[Value::from("a")]);
        let file_id = raw.file_id;
        let original = version_with_files(metadata.clone(), vec![raw]);
        let ranges = original.ssts.primary_key_ranges();
        let range = ranges
            .range(&original.ssts.levels()[0].files[&file_id])
            .unwrap();
        let mut builder = RegionMetadataBuilder::from_existing((*metadata).clone());
        builder
            .push_column_metadata(ColumnMetadata {
                column_id: 101,
                semantic_type: SemanticType::Field,
                column_schema: ColumnSchema::new("field", ConcreteDataType::int64_datatype(), true),
            })
            .bump_version();
        let changed = VersionBuilder::from_version(original.clone())
            .metadata(Arc::new(builder.build().unwrap()))
            .build();

        assert_eq!(1, changed.metadata.schema_version);
        assert!(changed.metadata.column_by_id(101).is_some());
        // Unrelated metadata changes share the SST snapshot and its encoded defaults.
        assert!(Arc::ptr_eq(&original.ssts, &changed.ssts));
        let changed_ranges = changed.ssts.primary_key_ranges();
        assert!(Arc::ptr_eq(&ranges.mapper, &changed_ranges.mapper));
        let changed_range = ranges
            .range(&changed.ssts.levels()[0].files[&file_id])
            .unwrap();
        assert_eq!(range, changed_range);
        assert_eq!(range.0.as_ptr(), changed_range.0.as_ptr());
        assert_eq!(range.1.as_ptr(), changed_range.1.as_ptr());
    }

    // SET/DROP DEFAULT may change padding even when the number of PK columns stays fixed.
    #[rstest]
    #[case::set(Value::from("x"), Some(Value::from("y")))]
    #[case::set_null(Value::from("x"), Some(Value::Null))]
    #[case::drop(Value::from("x"), None)]
    #[case::replace_null(Value::Null, Some(Value::from("y")))]
    fn test_tag_default_changes_refresh_only_missing_values(
        #[case] previous_default: Value,
        #[case] new_default: Option<Value>,
    ) {
        let metadata = metadata(&[Value::from(""), previous_default.clone()]);
        let missing = file_meta(&metadata, &[Value::from("a")]);
        let complete = file_meta(&metadata, &[Value::from("b"), Value::from("stored")]);
        let missing_id = missing.file_id;
        let complete_id = complete.file_id;
        let complete_range = complete.primary_key_range();
        let original = version_with_files(metadata.clone(), vec![missing, complete]);
        let old_ranges = original.ssts.primary_key_ranges();
        let old_range = old_ranges.range(&original.ssts.levels()[0].files[&missing_id]);

        let mut changed_metadata = (*metadata).clone();
        changed_metadata.column_metadatas[1].column_schema = changed_metadata.column_metadatas[1]
            .column_schema
            .clone()
            .with_default_constraint(new_default.clone().map(ColumnDefaultConstraint::Value))
            .unwrap();
        let mut builder = RegionMetadataBuilder::from_existing(changed_metadata);
        builder.bump_version();
        let changed_metadata = Arc::new(builder.build().unwrap());
        let changed = VersionBuilder::from_version(original.clone())
            .metadata(changed_metadata.clone())
            .build();

        let expected = encode(
            &changed_metadata,
            &[Value::from("a"), new_default.unwrap_or(Value::Null)],
        );
        let changed_ranges = changed.ssts.primary_key_ranges();
        assert_eq!(
            Some((expected.clone(), expected)),
            changed_ranges.range(&changed.ssts.levels()[0].files[&missing_id])
        );
        assert_eq!(
            complete_range,
            changed_ranges.range(&changed.ssts.levels()[0].files[&complete_id])
        );
        let expected_old = encode(&metadata, &[Value::from("a"), previous_default]);
        assert_eq!(Some((expected_old.clone(), expected_old)), old_range);
        assert_eq!(
            old_range,
            old_ranges.range(&original.ssts.levels()[0].files[&missing_id])
        );
    }

    #[rstest]
    #[case::mixed(vec![
            Value::from("default"),
            Value::Null,
            Value::Int64(42),
            Value::Binary(vec![0, 255].into()),
    ])]
    #[case::nulls(vec![Value::Null; 4])]
    #[case::empty(vec![])]
    fn test_dense_ranges_complete_every_historical_prefix(#[case] defaults: Vec<Value>) {
        let metadata = metadata(&defaults);
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        let expected = encode(&metadata, &defaults);
        for count in 0..=defaults.len() {
            let prefix = encode(&metadata, &defaults[..count]);
            assert_eq!(
                Some((expected.clone(), expected.clone())),
                mapper
                    .map(metadata.region_id, (prefix.clone(), prefix))
                    .unwrap()
            );
        }
    }

    #[rstest]
    #[case::cold(false)]
    #[case::warm(true)]
    fn test_successive_pk_appends_reuse_constants_and_complete_all_prefixes(#[case] warm: bool) {
        let defaults = [
            Value::from(""),
            Value::from("constant"),
            Value::Null,
            Value::Int64(42),
            Value::Null,
        ];
        let mut mapper = PrimaryKeyRangeMapper::new(metadata(&defaults[..2]));
        let mut values = defaults.clone();
        values[0] = Value::from("stored");
        let raw = encode(&mapper.metadata, &values[..1]);
        if warm {
            assert!(
                mapper
                    .map(mapper.metadata.region_id, (raw.clone(), raw))
                    .unwrap()
                    .is_some()
            );
        }

        for count in 3..=defaults.len() {
            let next_metadata = metadata(&defaults[..count]);
            let next_mapper = mapper.with_metadata(next_metadata.clone());
            if let Some(Some(encoded)) = mapper.encoded_defaults[1].get() {
                // An already encoded non-NULL constant must share its allocation across ALTER.
                let reused = next_mapper.encoded_defaults[1]
                    .get()
                    .unwrap()
                    .as_ref()
                    .unwrap();
                assert_eq!(encoded.as_ptr(), reused.as_ptr());
            }
            let expected = encode(&next_metadata, &values[..count]);
            for prefix_len in 1..=count {
                let prefix = encode(&next_metadata, &values[..prefix_len]);
                assert_eq!(
                    Some((expected.clone(), expected.clone())),
                    next_mapper
                        .map(next_metadata.region_id, (prefix.clone(), prefix))
                        .unwrap()
                );
            }
            let old_expected = encode(&mapper.metadata, &values[..count - 1]);
            let raw = encode(&mapper.metadata, &values[..1]);
            assert_eq!(
                Some((old_expected.clone(), old_expected)),
                mapper
                    .map(mapper.metadata.region_id, (raw.clone(), raw))
                    .unwrap()
            );
            mapper = next_mapper;
        }
    }

    #[rstest]
    #[case::string(ConcreteDataType::string_datatype(), true, true)]
    #[case::int(ConcreteDataType::int64_datatype(), true, true)]
    #[case::binary(ConcreteDataType::binary_datatype(), true, true)]
    #[case::timestamp(ConcreteDataType::timestamp_millisecond_datatype(), true, true)]
    #[case::required(ConcreteDataType::string_datatype(), false, false)]
    #[case::unsupported_list(
        ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::string_datatype())),
        true,
        false
    )]
    #[case::unsupported_null(ConcreteDataType::null_datatype(), true, false)]
    fn test_null_padding_requires_a_nullable_encodable_column(
        #[case] data_type: ConcreteDataType,
        #[case] nullable: bool,
        #[case] known: bool,
    ) {
        let mut metadata = (*metadata(&[Value::Null])).clone();
        metadata.column_metadatas[0].column_schema =
            ColumnSchema::new("tag_0", data_type, nullable);
        let metadata = Arc::new(
            RegionMetadataBuilder::from_existing(metadata)
                .build()
                .unwrap(),
        );
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        let expected = known.then(|| (Bytes::from_static(&[0]), Bytes::from_static(&[0])));
        assert_eq!(
            expected,
            mapper
                .map(metadata.region_id, (Bytes::new(), Bytes::new()))
                .unwrap()
        );
        if known {
            let encoded_null = encode(&metadata, &[Value::Null]);
            assert_eq!(Some((encoded_null.clone(), encoded_null)), expected);
        }
    }

    // Invalid order/layout is an error; foreign bounds and unavailable defaults
    // remain unknown. Exercise both endpoints without an inverted range masking decoding.
    #[rstest]
    #[case::dense(PrimaryKeyEncoding::Dense)]
    #[case::sparse(PrimaryKeyEncoding::Sparse)]
    fn test_reversed_pk_bounds_return_error(#[case] encoding: PrimaryKeyEncoding) {
        use common_error::ext::ErrorExt;
        use common_error::status_code::StatusCode;
        use mito_codec::row_converter::build_primary_key_codec;

        let mut metadata = (*metadata(&[Value::from("")])).clone();
        metadata.primary_key_encoding = encoding;
        let metadata = Arc::new(metadata);
        let codec = build_primary_key_codec(&metadata);
        let mut a = Vec::new();
        let mut b = Vec::new();
        codec
            .encode_values(&[(0, Value::from("a"))], &mut a)
            .unwrap();
        codec
            .encode_values(&[(0, Value::from("b"))], &mut b)
            .unwrap();
        let mapper = PrimaryKeyRangeMapper::new(metadata);
        let err = mapper
            .map(mapper.metadata.region_id, (b.into(), a.into()))
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::Error::InvalidPrimaryKeyRange { .. }
        ));
        assert_eq!(StatusCode::Internal, err.status_code());
    }

    #[test]
    fn test_different_pk_prefix_lengths_return_error() {
        let metadata = metadata(&[Value::from(""), Value::Int64(42)]);
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        let min = encode(&metadata, &[Value::from("a")]);
        let max = encode(&metadata, &[Value::from("b"), Value::Int64(42)]);
        assert!(matches!(
            mapper.map(metadata.region_id, (min, max)),
            Err(crate::error::Error::InvalidPrimaryKeyRange { .. })
        ));
    }

    #[rstest]
    #[case::min("min")]
    #[case::max("max")]
    fn test_truncated_pk_endpoint_preserves_decode_error(#[case] endpoint: &'static str) {
        use common_error::ext::ErrorExt;
        use common_error::status_code::StatusCode;

        let metadata = metadata(&[Value::from("")]);
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        let mut min = encode(&metadata, &[Value::from("a")]);
        let mut max = encode(&metadata, &[Value::from("b")]);
        if endpoint == "min" {
            min.truncate(min.len() - 1);
        } else {
            max.truncate(max.len() - 1);
        }
        let err = mapper.map(metadata.region_id, (min, max)).unwrap_err();
        assert_eq!(StatusCode::Internal, err.status_code());
        assert!(matches!(err, crate::error::Error::DecodePrimaryKeyRange {
            endpoint: actual,
            source: mito_codec::error::Error::InvalidDensePrimaryKey { .. },
            ..
        } if actual == endpoint));
    }

    #[test]
    fn test_foreign_pk_bounds_are_unknown_without_decoding() {
        let mapper = PrimaryKeyRangeMapper::new(metadata(&[Value::from("")]));
        // Foreign files may use a different layout, so don't decode them as local Dense keys.
        let key = Bytes::from_static(&[2]);
        assert_eq!(
            None,
            mapper.map(RegionId::new(2, 1), (key.clone(), key)).unwrap()
        );
    }

    #[rstest]
    #[case::local_first(false)]
    #[case::foreign_first(true)]
    fn test_comparison_cache_distinguishes_region_owners(#[case] foreign_first: bool) {
        let metadata = metadata(&[Value::from("")]);
        let raw = file_meta(&metadata, &[Value::from("a")]);
        let expected = raw.primary_key_range();
        let local = FileHandle::new(raw.clone(), new_noop_file_purger());
        let foreign = FileHandle::new(
            FileMeta {
                region_id: RegionId::new(2, 1),
                ..raw
            },
            new_noop_file_purger(),
        );
        let ranges = PrimaryKeyRanges::new(Arc::new(PrimaryKeyRangeMapper::new(metadata)));
        let mut cases = [(&local, expected), (&foreign, None)];
        if foreign_first {
            cases.reverse();
        }
        for _ in 0..2 {
            for (file, expected) in &cases {
                assert_eq!(*expected, ranges.range(file));
            }
        }
    }

    #[test]
    fn test_invalid_file_ranges_log_once_and_keep_possible_overlap() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use common_telemetry::tracing_subscriber::Layer;
        use common_telemetry::tracing_subscriber::layer::Context;
        use common_telemetry::tracing_subscriber::prelude::*;
        use tracing::{Event, Level, Subscriber};

        struct WarningCounter(Arc<AtomicUsize>);
        impl<S: Subscriber> Layer<S> for WarningCounter {
            fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
                if *event.metadata().level() == Level::WARN {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let warnings = Arc::new(AtomicUsize::new(0));
        let subscriber =
            common_telemetry::tracing_subscriber::registry().with(WarningCounter(warnings.clone()));
        let metadata = metadata(&[Value::from("")]);
        let mapper = Arc::new(PrimaryKeyRangeMapper::new(metadata.clone()));
        let ranges = PrimaryKeyRanges::new(mapper);
        let healthy_meta = file_meta(&metadata, &[Value::from("c")]);
        let healthy = FileHandle::new(healthy_meta, new_noop_file_purger());
        let a = encode(&metadata, &[Value::from("a")]);
        let b = encode(&metadata, &[Value::from("b")]);

        tracing::subscriber::with_default(subscriber, || {
            for (min, max) in [(b.clone(), a.clone()), (a.slice(..a.len() - 1), b)] {
                let mut meta = file_meta(&metadata, &[Value::from("a")]);
                meta.primary_key_min = Some(min);
                meta.primary_key_max = Some(max);
                let file = FileHandle::new(meta, new_noop_file_purger());
                assert_eq!(None, ranges.range(&file));
                assert_eq!(None, ranges.range(&file.clone()));
                // These raw ranges look disjoint from c; unknown must prevent pruning.
                assert!(files_overlap_inclusive(&file, &healthy, &ranges));
                assert!(files_overlap_inclusive(&healthy, &file, &ranges));
            }
        });
        assert_eq!(2, warnings.load(Ordering::Relaxed));
    }

    #[test]
    fn test_missing_impure_default_is_unknown_but_complete_key_is_usable() {
        let mut metadata = (*metadata(&[Value::Timestamp(Timestamp::new_millisecond(0))])).clone();
        metadata.column_metadatas[0].column_schema = metadata.column_metadatas[0]
            .column_schema
            .clone()
            .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                "current_timestamp()".into(),
            )))
            .unwrap();
        let metadata = Arc::new(metadata);
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        assert_eq!(
            None,
            mapper
                .map(metadata.region_id, (Bytes::new(), Bytes::new()))
                .unwrap()
        );
        let key = encode(
            &metadata,
            &[Value::Timestamp(Timestamp::new_millisecond(10))],
        );
        assert_eq!(
            Some((key.clone(), key.clone())),
            mapper.map(metadata.region_id, (key.clone(), key)).unwrap()
        );
    }

    #[test]
    fn test_sparse_ranges_keep_their_original_encoding() {
        use mito_codec::row_converter::SparsePrimaryKeyCodec;
        let mut metadata = (*metadata(&[Value::from(""), Value::Null])).clone();
        metadata.primary_key_encoding = PrimaryKeyEncoding::Sparse;
        let metadata = Arc::new(metadata);
        let mut bytes = Vec::new();
        SparsePrimaryKeyCodec::new(&metadata)
            .encode_values(&[(0, Value::from("a"))], &mut bytes)
            .unwrap();
        let key = Bytes::from(bytes);
        let region_id = metadata.region_id;
        let mapper = PrimaryKeyRangeMapper::new(metadata);
        assert_eq!(
            Some((key.clone(), key.clone())),
            mapper.map(region_id, (key.clone(), key)).unwrap()
        );
    }

    #[test]
    fn test_comparison_contexts_isolate_schemas_and_share_file_state() {
        let old_metadata = metadata(&[Value::from("")]);
        let new_metadata = metadata(&[Value::from(""), Value::Null, Value::Int64(42)]);
        let raw_meta = file_meta(&old_metadata, &[Value::from("b")]);
        let original = version_with_files(old_metadata.clone(), vec![raw_meta.clone()]);
        let old_file = original.ssts.levels()[0].files().next().unwrap();
        let old_ranges = original.ssts.primary_key_ranges();
        let changed = VersionBuilder::from_version(original.clone())
            .metadata(new_metadata.clone())
            .build();
        let new_file = changed.ssts.levels()[0].files().next().unwrap();
        let new_ranges = changed.ssts.primary_key_ranges();
        let expected = encode(
            &new_metadata,
            &[Value::from("b"), Value::Null, Value::Int64(42)],
        );
        assert_eq!(raw_meta.primary_key_range(), old_ranges.range(old_file));
        assert_eq!(
            Some((expected.clone(), expected)),
            new_ranges.range(new_file)
        );
        assert_eq!(raw_meta, *new_file.meta_ref());
        assert_eq!(
            raw_meta.primary_key_range(),
            new_file.raw_primary_key_range()
        );
        // Either context can interpret the same physical handle without rebinding it.
        assert_eq!(old_ranges.range(old_file), old_ranges.range(new_file));
        assert_eq!(new_ranges.range(new_file), new_ranges.range(old_file));
        old_file.set_compacting(true);
        assert!(new_file.compacting());
        new_file.mark_deleted();
        assert!(old_file.is_deleted());

        // Old-schema flushes can finish after ALTER; compare them with the same context.
        let added_meta = file_meta(&old_metadata, &[Value::from("a")]);
        let added_id = added_meta.file_id;
        let changed = VersionBuilder::from_version(Arc::new(changed))
            .apply_edit(
                RegionEdit {
                    files_to_add: vec![added_meta],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                },
                new_noop_file_purger(),
            )
            .build();
        let added = &changed.ssts.levels()[0].files[&added_id];
        let expected = encode(
            &new_metadata,
            &[Value::from("a"), Value::Null, Value::Int64(42)],
        );
        assert_eq!(Some((expected.clone(), expected)), new_ranges.range(added));
    }

    #[test]
    fn test_late_statistics_and_changed_defaults_use_each_pinned_schema() {
        let metadata_v1 = metadata(&[Value::from(""), Value::Int64(42)]);
        let metadata_v2 = metadata(&[Value::from(""), Value::Int64(100)]);
        let mut meta = file_meta(&metadata_v1, &[Value::from("a")]);
        let raw = meta.primary_key_range().unwrap();
        meta.primary_key_min = None;
        meta.primary_key_max = None;
        let file = FileHandle::new(meta, new_noop_file_purger());
        let v1 = PrimaryKeyRanges::new(Arc::new(PrimaryKeyRangeMapper::new(metadata_v1.clone())));
        let v2 = PrimaryKeyRanges::new(Arc::new(PrimaryKeyRangeMapper::new(metadata_v2.clone())));
        assert_eq!(None, v1.range(&file));
        assert_eq!(None, v2.range(&file));
        file.set_primary_key_range(raw.clone());
        for (ranges, metadata, default) in [(v1, metadata_v1, 42), (v2, metadata_v2, 100)] {
            let expected = encode(&metadata, &[Value::from("a"), Value::Int64(default)]);
            assert_eq!(Some((expected.clone(), expected)), ranges.range(&file));
        }
        assert_eq!(Some(raw), file.raw_primary_key_range());
    }

    #[test]
    fn test_deserialized_compaction_files_use_the_comparison_schema() {
        use crate::compaction::CompactionOutput;
        use crate::compaction::picker::{PickerOutput, SerializedPickerOutput};

        let metadata = metadata(&[Value::from(""), Value::Int64(42)]);
        let raw = file_meta(&metadata, &[Value::from("a")]);
        let picked = PickerOutput {
            outputs: vec![CompactionOutput {
                output_level: 1,
                inputs: vec![FileHandle::new(raw.clone(), new_noop_file_purger())],
                filter_deleted: false,
                output_time_range: None,
            }],
            ..Default::default()
        };
        let serialized = SerializedPickerOutput::from(&picked);
        let output = PickerOutput::from_serialized(serialized, new_noop_file_purger());
        let ranges = PrimaryKeyRanges::new(Arc::new(PrimaryKeyRangeMapper::new(metadata.clone())));
        let file = &output.outputs[0].inputs[0];
        let completed = encode(&metadata, &[Value::from("a"), Value::Int64(42)]);
        assert_eq!(Some((completed.clone(), completed)), ranges.range(file));
        assert_eq!(raw, *file.meta_ref());
    }

    #[test]
    fn test_compaction_overlap_uses_completed_endpoints() {
        let metadata = metadata(&[Value::from(""), Value::Null]);
        let mapper = Arc::new(PrimaryKeyRangeMapper::new(metadata.clone()));
        let ranges = PrimaryKeyRanges::new(mapper);
        let mut old = file_meta(&metadata, &[Value::from("a")]);
        old.primary_key_max = Some(encode(&metadata, &[Value::from("b")]));
        let mut new = file_meta(&metadata, &[Value::from("b"), Value::Null]);
        new.primary_key_max = Some(encode(&metadata, &[Value::from("c"), Value::Null]));
        assert!(old.primary_key_max < new.primary_key_min);
        let old = FileHandle::new(old, new_noop_file_purger());
        let new = FileHandle::new(new, new_noop_file_purger());
        assert!(files_overlap_inclusive(&old, &new, &ranges));
        assert!(files_overlap_inclusive(&new, &old, &ranges));
    }
}
