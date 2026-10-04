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

use std::sync::OnceLock;

use bytes::Bytes;
use datatypes::prelude::ConcreteDataType;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SortField};
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::storage::RegionId;

use crate::error::{DecodePrimaryKeyRangeSnafu, InvalidPrimaryKeyRangeSnafu, Result};

/// Schema conversion and default encodings shared by a version's comparison contexts.
#[derive(Debug)]
pub(crate) struct PrimaryKeyRangeMapper {
    /// Target metadata pinned by the owning version or scan, not the SST's write schema.
    /// ALTER creates a new mapper; existing snapshots keep their original target schema.
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

    /// Returns the region id of mapper.
    pub(crate) fn region_id(&self) -> RegionId {
        self.metadata.region_id
    }

    /// Maps bounds from the same table's encoding and a compatible schema prefix.
    /// Returns unknown for defaults that cannot be safely encoded.
    /// Invalid bounds return an error so callers can diagnose them before falling back
    /// to unknown. Neither the error nor its source includes the encoded keys.
    pub(crate) fn map(&self, (min, max): (Bytes, Bytes)) -> Result<Option<(Bytes, Bytes)>> {
        ensure!(
            min <= max,
            InvalidPrimaryKeyRangeSnafu {
                reason: "min is greater than max",
            }
        );
        // Sparse-to-sparse read compatibility preserves encoded keys verbatim.
        if self.metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
            return Ok(Some((min, max)));
        }
        // Ordinary same-table Dense ALTER only appends PK fields and preserves
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

    fn metadata_at_version(defaults: &[Value], version: u64) -> RegionMetadataRef {
        let mut metadata = (*metadata(defaults)).clone();
        metadata.schema_version = version;
        Arc::new(metadata)
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
    fn test_field_addition_updates_cache_version_and_shares_sst_list() {
        let metadata = metadata(&[Value::from(""), Value::from("default")]);
        let raw = file_meta(&metadata, &[Value::from("a")]);
        let file_id = raw.file_id;
        let original = version_with_files(metadata.clone(), vec![raw]);
        let mapper = original.ssts.primary_key_mapper();
        let range = original.ssts.levels()[0].files[&file_id]
            .primary_key_range(&mapper)
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
        assert_eq!(
            original.ssts.levels().as_ptr(),
            changed.ssts.levels().as_ptr()
        );
        let changed_mapper = changed.ssts.primary_key_mapper();
        assert_eq!(1, changed_mapper.schema_version());
        let changed_range = changed.ssts.levels()[0].files[&file_id]
            .primary_key_range(&changed_mapper)
            .unwrap();
        assert_eq!(range, changed_range);
        let cached = changed.ssts.levels()[0].files[&file_id]
            .primary_key_range(&changed_mapper)
            .unwrap();
        assert_eq!(cached.0.as_ptr(), changed_range.0.as_ptr());
        assert_eq!(cached.1.as_ptr(), changed_range.1.as_ptr());
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
        let old_mapper = original.ssts.primary_key_mapper();
        let old_range = original.ssts.levels()[0].files[&missing_id].primary_key_range(&old_mapper);

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
        let changed_mapper = changed.ssts.primary_key_mapper();
        assert_eq!(
            Some((expected.clone(), expected)),
            changed.ssts.levels()[0].files[&missing_id].primary_key_range(&changed_mapper)
        );
        assert_eq!(
            complete_range,
            changed.ssts.levels()[0].files[&complete_id].primary_key_range(&changed_mapper)
        );
        let expected_old = encode(&metadata, &[Value::from("a"), previous_default]);
        assert_eq!(Some((expected_old.clone(), expected_old)), old_range);
        assert_eq!(
            old_range,
            original.ssts.levels()[0].files[&missing_id].primary_key_range(&old_mapper)
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
                mapper.map((prefix.clone(), prefix)).unwrap()
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
            assert!(mapper.map((raw.clone(), raw)).unwrap().is_some());
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
                    next_mapper.map((prefix.clone(), prefix)).unwrap()
                );
            }
            let old_expected = encode(&mapper.metadata, &values[..count - 1]);
            let raw = encode(&mapper.metadata, &values[..1]);
            assert_eq!(
                Some((old_expected.clone(), old_expected)),
                mapper.map((raw.clone(), raw)).unwrap()
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
        assert_eq!(expected, mapper.map((Bytes::new(), Bytes::new())).unwrap());
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
        let err = mapper.map((b.into(), a.into())).unwrap_err();
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
            mapper.map((min, max)),
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
        let err = mapper.map((min, max)).unwrap_err();
        assert_eq!(StatusCode::Internal, err.status_code());
        assert!(matches!(err, crate::error::Error::DecodePrimaryKeyRange {
            endpoint: actual,
            source: mito_codec::error::Error::InvalidDensePrimaryKey { .. },
            ..
        } if actual == endpoint));
    }

    #[rstest]
    #[case::source_first(false)]
    #[case::destination_first(true)]
    fn test_aligned_cache_uses_table_schema_across_region_migration(
        #[case] destination_first: bool,
    ) {
        let source = metadata(&[Value::from("")]);
        let file = FileHandle::new(
            file_meta(&source, &[Value::from("a")]),
            new_noop_file_purger(),
        );
        let target = metadata_at_version(&[Value::from(""), Value::Int64(42)], 1);
        let expected = encode(&target, &[Value::from("a"), Value::Int64(42)]);
        let mut regions = [RegionId::new(1, 1), RegionId::new(1, 2)];
        if destination_first {
            regions.reverse();
        }
        let mut previous: Option<(Bytes, Bytes)> = None;
        for region_id in regions {
            let mut metadata = (*target).clone();
            metadata.region_id = region_id;
            let mapper = PrimaryKeyRangeMapper::new(Arc::new(metadata));
            let aligned = file.primary_key_range(&mapper).unwrap();
            assert_eq!((expected.clone(), expected.clone()), aligned);
            if let Some(previous) = previous {
                // Different metadata allocations/region ids still share one schema-version cache.
                assert_eq!(previous.0.as_ptr(), aligned.0.as_ptr());
                assert_eq!(previous.1.as_ptr(), aligned.1.as_ptr());
            }
            previous = Some(aligned);
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
                assert_eq!(None, file.primary_key_range(&mapper));
                assert_eq!(None, file.clone().primary_key_range(&mapper));
                // These raw ranges look disjoint from c; unknown must prevent pruning.
                assert!(files_overlap_inclusive(&file, &healthy, &mapper));
                assert!(files_overlap_inclusive(&healthy, &file, &mapper));
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
        assert_eq!(None, mapper.map((Bytes::new(), Bytes::new())).unwrap());
        let key = encode(
            &metadata,
            &[Value::Timestamp(Timestamp::new_millisecond(10))],
        );
        assert_eq!(
            Some((key.clone(), key.clone())),
            mapper.map((key.clone(), key)).unwrap()
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
        let mapper = PrimaryKeyRangeMapper::new(metadata);
        assert_eq!(
            Some((key.clone(), key.clone())),
            mapper.map((key.clone(), key)).unwrap()
        );
    }

    #[test]
    fn test_aligned_cache_isolates_schemas_and_shares_file_state() {
        let old_metadata = metadata(&[Value::from("")]);
        let new_metadata =
            metadata_at_version(&[Value::from(""), Value::Null, Value::Int64(42)], 1);
        let raw_meta = file_meta(&old_metadata, &[Value::from("b")]);
        let original = version_with_files(old_metadata.clone(), vec![raw_meta.clone()]);
        let old_file = original.ssts.levels()[0].files().next().unwrap();
        let old_mapper = original.ssts.primary_key_mapper();
        let changed = VersionBuilder::from_version(original.clone())
            .metadata(new_metadata.clone())
            .build();
        let new_file = changed.ssts.levels()[0].files().next().unwrap();
        let new_mapper = changed.ssts.primary_key_mapper();
        let expected = encode(
            &new_metadata,
            &[Value::from("b"), Value::Null, Value::Int64(42)],
        );
        assert_eq!(
            raw_meta.primary_key_range(),
            old_file.primary_key_range(&old_mapper)
        );
        assert_eq!(
            Some((expected.clone(), expected)),
            new_file.primary_key_range(&new_mapper)
        );
        assert_eq!(raw_meta, *new_file.meta_ref());
        assert_eq!(
            raw_meta.primary_key_range(),
            new_file.raw_primary_key_range()
        );
        assert_eq!(
            old_file.primary_key_range(&old_mapper),
            new_file.primary_key_range(&old_mapper)
        );
        assert_eq!(
            new_file.primary_key_range(&new_mapper),
            old_file.primary_key_range(&new_mapper)
        );
        old_file.set_compacting(true);
        assert!(new_file.compacting());
        new_file.mark_deleted();
        assert!(old_file.is_deleted());

        // Old-schema flushes can finish after ALTER; compare them with the target mapper.
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
        assert_eq!(
            Some((expected.clone(), expected)),
            added.primary_key_range(&new_mapper)
        );
    }

    #[test]
    fn test_late_statistics_and_changed_defaults_use_each_pinned_schema() {
        let metadata_v1 = metadata(&[Value::from(""), Value::Int64(42)]);
        let metadata_v2 = metadata_at_version(&[Value::from(""), Value::Int64(100)], 1);
        let mut meta = file_meta(&metadata_v1, &[Value::from("a")]);
        let raw = meta.primary_key_range().unwrap();
        meta.primary_key_min = None;
        meta.primary_key_max = None;
        let file = FileHandle::new(meta, new_noop_file_purger());
        let v1 = PrimaryKeyRangeMapper::new(metadata_v1.clone());
        let v2 = PrimaryKeyRangeMapper::new(metadata_v2.clone());
        assert_eq!(None, file.primary_key_range(&v1));
        assert_eq!(None, file.primary_key_range(&v2));
        file.set_primary_key_range(raw.clone());
        for (mapper, metadata, default) in [
            (&v2, &metadata_v2, 100),
            (&v1, &metadata_v1, 42),
            (&v2, &metadata_v2, 100),
        ] {
            let expected = encode(metadata, &[Value::from("a"), Value::Int64(default)]);
            assert_eq!(
                Some((expected.clone(), expected)),
                file.primary_key_range(mapper)
            );
        }
        assert_eq!(Some(raw), file.raw_primary_key_range());
    }

    #[test]
    fn test_concurrent_snapshots_do_not_return_each_others_aligned_bounds() {
        let old_metadata = metadata(&[Value::from(""), Value::Int64(42)]);
        let new_metadata = metadata_at_version(&[Value::from(""), Value::Int64(100)], 1);
        let raw = file_meta(&old_metadata, &[Value::from("a")]);
        let original_bounds = raw.primary_key_range();
        let file = FileHandle::new(raw, new_noop_file_purger());
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            for (metadata, default) in [(old_metadata, 42), (new_metadata, 100)] {
                let file = &file;
                let barrier = &barrier;
                scope.spawn(move || {
                    let expected = encode(&metadata, &[Value::from("a"), Value::Int64(default)]);
                    let mapper = PrimaryKeyRangeMapper::new(metadata);
                    let mut all_matched = true;
                    for _ in 0..64 {
                        barrier.wait();
                        all_matched &= file.primary_key_range(&mapper)
                            == Some((expected.clone(), expected.clone()));
                    }
                    // Assert after all barriers so a failure cannot strand the other snapshot.
                    assert!(
                        all_matched,
                        "wrong bounds for schema version {}",
                        mapper.schema_version()
                    );
                });
            }
        });
        assert_eq!(original_bounds, file.raw_primary_key_range());
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
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        let file = &output.outputs[0].inputs[0];
        let completed = encode(&metadata, &[Value::from("a"), Value::Int64(42)]);
        assert_eq!(
            Some((completed.clone(), completed)),
            file.primary_key_range(&mapper)
        );
        assert_eq!(raw, *file.meta_ref());
    }

    #[test]
    fn test_compaction_overlap_uses_completed_endpoints() {
        let metadata = metadata(&[Value::from(""), Value::Null]);
        let mapper = Arc::new(PrimaryKeyRangeMapper::new(metadata.clone()));
        let mut old = file_meta(&metadata, &[Value::from("a")]);
        old.primary_key_max = Some(encode(&metadata, &[Value::from("b")]));
        let mut new = file_meta(&metadata, &[Value::from("b"), Value::Null]);
        new.primary_key_max = Some(encode(&metadata, &[Value::from("c"), Value::Null]));
        assert!(old.primary_key_max < new.primary_key_min);
        let old = FileHandle::new(old, new_noop_file_purger());
        let new = FileHandle::new(new, new_noop_file_purger());
        assert!(files_overlap_inclusive(&old, &new, &mapper));
        assert!(files_overlap_inclusive(&new, &old, &mapper));
    }
}
