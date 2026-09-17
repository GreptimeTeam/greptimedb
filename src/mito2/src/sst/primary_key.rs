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
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::storage::RegionId;

/// Shared by files in a version. Raw SST bounds always remain in their original schema.
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

    /// Returns unknown unless the range can be mapped without changing its ordering.
    pub(crate) fn map(
        &self,
        region_id: RegionId,
        (min, max): (Bytes, Bytes),
    ) -> Option<(Bytes, Bytes)> {
        if min > max || region_id != self.metadata.region_id {
            return None;
        }
        // Sparse-to-sparse read compatibility preserves encoded keys verbatim.
        if self.metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
            return Some((min, max));
        }
        // Ordinary same-region Dense ALTER only appends PK fields and preserves
        // prefix order/types. SyncColumns violating that invariant is out of scope.
        let prefix_len = self.codec.decode_prefix_len(&min).ok()?;
        if self.codec.decode_prefix_len(&max).ok()? != prefix_len {
            return None;
        }
        if prefix_len == self.codec.num_fields() {
            return Some((min, max));
        }
        let suffix = self.suffixes[prefix_len]
            .get_or_init(|| self.encode_suffix(prefix_len))
            .as_ref()?;
        // Fixed-layout Dense keys are prefix-free, so appending one constant
        // suffix is monotone. Transform each file before aggregating any ranges.
        Some((append_suffix(min, suffix), append_suffix(max, suffix)))
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
    use crate::compaction::run::Ranged;
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
        let range = original.ssts.levels()[0].files[&file_id]
            .primary_key_range()
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
        // Sharing the SST snapshot avoids an O(files) copy and invalidating warm views.
        assert!(Arc::ptr_eq(&original.ssts, &changed.ssts));
        let changed_range = changed.ssts.levels()[0].files[&file_id]
            .primary_key_range()
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
        let old_range = original.ssts.levels()[0].files[&missing_id].primary_key_range();

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
        assert_eq!(
            Some((expected.clone(), expected)),
            changed.ssts.levels()[0].files[&missing_id].primary_key_range()
        );
        assert_eq!(
            complete_range,
            changed.ssts.levels()[0].files[&complete_id].primary_key_range()
        );
        let expected_old = encode(&metadata, &[Value::from("a"), previous_default]);
        assert_eq!(Some((expected_old.clone(), expected_old)), old_range);
        assert_eq!(
            old_range,
            original.ssts.levels()[0].files[&missing_id].primary_key_range()
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
                mapper.map(metadata.region_id, (prefix.clone(), prefix))
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
                    next_mapper.map(next_metadata.region_id, (prefix.clone(), prefix))
                );
            }
            let old_expected = encode(&mapper.metadata, &values[..count - 1]);
            let raw = encode(&mapper.metadata, &values[..1]);
            assert_eq!(
                Some((old_expected.clone(), old_expected)),
                mapper.map(mapper.metadata.region_id, (raw.clone(), raw))
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
            mapper.map(metadata.region_id, (Bytes::new(), Bytes::new()))
        );
        if known {
            let encoded_null = encode(&metadata, &[Value::Null]);
            assert_eq!(Some((encoded_null.clone(), encoded_null)), expected);
        }
    }

    #[test]
    fn test_dense_invalid_ranges_are_unknown() {
        let metadata = metadata(&[Value::from(""), Value::Int64(42)]);
        let mapper = PrimaryKeyRangeMapper::new(metadata.clone());
        let a = encode(&metadata, &[Value::from("a")]);
        let b = encode(&metadata, &[Value::from("b")]);
        let full = encode(&metadata, &[Value::from("b"), Value::Int64(42)]);
        for range in [
            (b.clone(), a.clone()),
            (a.clone(), full.clone()),
            (full.slice(..full.len() - 1), full.clone()),
            (Bytes::from_static(&[2]), b.clone()),
        ] {
            assert_eq!(None, mapper.map(metadata.region_id, range));
        }
        assert_eq!(None, mapper.map(RegionId::new(2, 1), (a, b)));
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
            mapper.map(metadata.region_id, (Bytes::new(), Bytes::new()))
        );
        let key = encode(
            &metadata,
            &[Value::Timestamp(Timestamp::new_millisecond(10))],
        );
        assert_eq!(
            Some((key.clone(), key.clone())),
            mapper.map(metadata.region_id, (key.clone(), key))
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
            mapper.map(region_id, (key.clone(), key))
        );
    }

    #[test]
    fn test_version_schema_changes_rebind_views_without_changing_file_state() {
        let old_metadata = metadata(&[Value::from("")]);
        let new_metadata = metadata(&[Value::from(""), Value::Null, Value::Int64(42)]);
        let raw_meta = file_meta(&old_metadata, &[Value::from("b")]);
        let original = version_with_files(old_metadata.clone(), vec![raw_meta.clone()]);
        let old_file = original.ssts.levels()[0].files().next().unwrap();
        let changed = VersionBuilder::from_version(original.clone())
            .metadata(new_metadata.clone())
            .build();
        let new_file = changed.ssts.levels()[0].files().next().unwrap();
        let expected = encode(
            &new_metadata,
            &[Value::from("b"), Value::Null, Value::Int64(42)],
        );
        assert_eq!(raw_meta.primary_key_range(), old_file.primary_key_range());
        assert_eq!(
            Some((expected.clone(), expected)),
            new_file.primary_key_range()
        );
        assert_eq!(raw_meta, *new_file.meta_ref());
        old_file.set_compacting(true);
        assert!(new_file.compacting());
        new_file.mark_deleted();
        assert!(old_file.is_deleted());

        // Old-schema flushes can finish after ALTER; newly admitted files must also bind.
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
            added.primary_key_range()
        );
    }

    #[test]
    fn test_late_statistics_and_changed_defaults_use_each_pinned_schema() {
        let metadata_v1 = metadata(&[Value::from(""), Value::Int64(42)]);
        let metadata_v2 = metadata(&[Value::from(""), Value::Int64(100)]);
        let mut meta = file_meta(&metadata_v1, &[Value::from("a")]);
        let raw = meta.primary_key_range().unwrap();
        meta.primary_key_min = None;
        meta.primary_key_max = None;
        let unbound = FileHandle::new(meta, new_noop_file_purger());
        let v1 = unbound
            .clone()
            .with_primary_key_mapper(Arc::new(PrimaryKeyRangeMapper::new(metadata_v1.clone())));
        let v2 = v1
            .clone()
            .with_primary_key_mapper(Arc::new(PrimaryKeyRangeMapper::new(metadata_v2.clone())));
        assert_eq!(None, v1.primary_key_range());
        unbound.set_primary_key_range(raw);
        assert_eq!(None, unbound.primary_key_range());
        for (file, metadata, default) in [(v1, metadata_v1, 42), (v2, metadata_v2, 100)] {
            let expected = encode(&metadata, &[Value::from("a"), Value::Int64(default)]);
            assert_eq!(Some((expected.clone(), expected)), file.primary_key_range());
        }
    }

    #[test]
    fn test_deserialized_compaction_files_bind_to_the_target_schema() {
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
        let output =
            PickerOutput::from_serialized(serialized, new_noop_file_purger(), metadata.clone());
        let file = &output.outputs[0].inputs[0];
        let completed = encode(&metadata, &[Value::from("a"), Value::Int64(42)]);
        assert_eq!(
            Some((completed.clone(), completed)),
            file.primary_key_range()
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
        let old =
            FileHandle::new(old, new_noop_file_purger()).with_primary_key_mapper(mapper.clone());
        let new = FileHandle::new(new, new_noop_file_purger()).with_primary_key_mapper(mapper);
        assert!(old.overlap_inclusive(&new));
        assert!(new.overlap_inclusive(&old));
    }
}
