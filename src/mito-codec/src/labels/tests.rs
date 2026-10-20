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

use std::sync::Arc;

use api::v1::SemanticType;
use common_time::timestamp::TimeUnit;
use common_time::{Date, Timestamp};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::schema::ColumnSchema;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::BoundTag;
use store_api::storage::RegionId;

use crate::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodec, SortField, SparsePrimaryKeyCodec,
};

use super::*;

fn metadata(
    tags: &[(u32, &str, ConcreteDataType)],
    encoding: PrimaryKeyEncoding,
) -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(7, 1));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 0,
    });
    let mut primary_key = Vec::new();
    for (id, name, data_type) in tags {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(*name, data_type.clone(), true),
            semantic_type: SemanticType::Tag,
            column_id: *id,
        });
        primary_key.push(*id);
    }
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new("field", ConcreteDataType::int64_datatype(), true),
        semantic_type: SemanticType::Field,
        column_id: 99,
    });
    builder.primary_key(primary_key);
    builder.primary_key_encoding(encoding);
    Arc::new(builder.build().unwrap())
}

fn source(
    metadata: Arc<RegionMetadata>,
    metric: &str,
    logical_tags: &[(u32, &str)],
) -> RawLabelSource {
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from(metric),
        physical_metadata: metadata,
        logical_tags: logical_tags
            .iter()
            .map(|(column_id, name)| BoundTag {
                column_id: *column_id,
                name: Arc::from(*name),
            })
            .collect::<Vec<_>>()
            .into(),
    }
}

fn encoded(encoding: PrimaryKeyEncoding, values: &[(u32, Value)]) -> Vec<u8> {
    let fields = values
        .iter()
        .map(|(id, value)| (*id, SortField::new(value.data_type())))
        .collect::<Vec<_>>();
    let mut bytes = Vec::new();
    match encoding {
        PrimaryKeyEncoding::Dense => DensePrimaryKeyCodec::with_fields(fields)
            .encode_values(values, &mut bytes)
            .unwrap(),
        PrimaryKeyEncoding::Sparse => {
            let codec = SparsePrimaryKeyCodec::with_fields(fields);
            codec.encode_internal(42, 9, &mut bytes).unwrap();
            codec.encode_values(values, &mut bytes).unwrap();
        }
    }
    bytes
}

fn string_metadata(encoding: PrimaryKeyEncoding) -> Arc<RegionMetadata> {
    metadata(
        &[
            (8, "zeta", ConcreteDataType::string_datatype()),
            (2, "private", ConcreteDataType::string_datatype()),
            (41, "alpha", ConcreteDataType::string_datatype()),
        ],
        encoding,
    )
}

fn values() -> Vec<(u32, Value)> {
    vec![
        (8, Value::String("last".into())),
        (2, Value::String("hidden".into())),
        (41, Value::String("first".into())),
    ]
}

#[test]
fn dense_and_sparse_decode_complete_or_projected_labels() {
    for encoding in [PrimaryKeyEncoding::Dense, PrimaryKeyEncoding::Sparse] {
        let source = source(
            string_metadata(encoding),
            "requests_total",
            &[(8, "zeta"), (41, "alpha")],
        );
        let key = encoded(encoding, &values());
        let mut all = PrimaryKeyLabelDecoder::new(&source, LabelProjection::All).unwrap();
        let complete = all.decode(&key).unwrap();
        let DecodedLabels::Complete(metric) = complete else {
            panic!("All must return a complete metric identity")
        };
        assert_eq!(metric.name(), Some("requests_total"));
        assert_eq!(metric.get("alpha"), Some("first"));
        assert_eq!(metric.get("zeta"), Some("last"));
        assert_eq!(metric.get("private"), None);

        let mut projected = PrimaryKeyLabelDecoder::new(
            &source,
            LabelProjection::Only(Arc::from([
                Arc::<str>::from("zeta"),
                Arc::from("alpha"),
                Arc::from("__name__"),
                Arc::from("missing"),
                Arc::from("alpha"),
            ])),
        )
        .unwrap();
        assert_eq!(
            projected.decode(&key).unwrap(),
            DecodedLabels::Projected(vec![
                Label {
                    name: Arc::from("__name__"),
                    value: Arc::from("requests_total")
                },
                Label {
                    name: Arc::from("alpha"),
                    value: Arc::from("first")
                },
                Label {
                    name: Arc::from("zeta"),
                    value: Arc::from("last")
                },
            ])
        );
        assert_eq!(
            projected.decode(&key).unwrap(),
            DecodedLabels::Projected(vec![
                Label {
                    name: Arc::from("__name__"),
                    value: Arc::from("requests_total")
                },
                Label {
                    name: Arc::from("alpha"),
                    value: Arc::from("first")
                },
                Label {
                    name: Arc::from("zeta"),
                    value: Arc::from("last")
                },
            ])
        );

        let mut without_name = PrimaryKeyLabelDecoder::new(
            &source,
            LabelProjection::Only(Arc::from([Arc::<str>::from("alpha")])),
        )
        .unwrap();
        assert_eq!(
            without_name.decode(&key).unwrap(),
            DecodedLabels::Projected(vec![Label {
                name: Arc::from("alpha"),
                value: Arc::from("first")
            }])
        );

        let mut empty_projection =
            PrimaryKeyLabelDecoder::new(&source, LabelProjection::Only(Arc::from([]))).unwrap();
        assert_eq!(
            empty_projection.decode(&key).unwrap(),
            DecodedLabels::Projected(vec![])
        );
    }
}

#[test]
fn projection_snapshots_its_layout_and_distinguishes_missing_and_empty() {
    let original = metadata(
        &[
            (21, "old", ConcreteDataType::string_datatype()),
            (4, "added_later", ConcreteDataType::string_datatype()),
        ],
        PrimaryKeyEncoding::Sparse,
    );
    let old_source = source(
        original.clone(),
        "old_metric",
        &[(21, "old"), (4, "added_later")],
    );
    let weak_metadata = Arc::downgrade(&original);
    let replacement = metadata(
        &[(21, "renamed", ConcreteDataType::string_datatype())],
        PrimaryKeyEncoding::Sparse,
    );
    let new_source = source(replacement, "new_metric", &[(21, "renamed")]);
    assert!(PrimaryKeyLabelDecoder::new(&new_source, LabelProjection::All).is_ok());

    let codec = SparsePrimaryKeyCodec::with_fields(vec![(
        21,
        SortField::new(ConcreteDataType::string_datatype()),
    )]);
    let mut key = Vec::new();
    codec.encode_internal(42, 9, &mut key).unwrap();
    codec
        .encode_values(&[(21, Value::String("".into()))], &mut key)
        .unwrap();
    let mut decoder = PrimaryKeyLabelDecoder::new(
        &old_source,
        LabelProjection::Only(Arc::from([
            Arc::<str>::from("old"),
            Arc::from("added_later"),
        ])),
    )
    .unwrap();
    let mut all = PrimaryKeyLabelDecoder::new(&old_source, LabelProjection::All).unwrap();
    drop(old_source);
    drop(original);
    assert!(
        weak_metadata.upgrade().is_none(),
        "the decoder keeps its own key layout (ids, positions, types), not the region metadata"
    );
    assert_eq!(
        decoder.decode(&key).unwrap(),
        DecodedLabels::Projected(vec![Label {
            name: Arc::from("old"),
            value: Arc::from("")
        },])
    );

    let mut absent = Vec::new();
    codec.encode_internal(42, 9, &mut absent).unwrap();
    assert_eq!(
        decoder.decode(&absent).unwrap(),
        DecodedLabels::Projected(vec![])
    );
    let DecodedLabels::Complete(absent_tags) = all.decode(&absent).unwrap() else {
        panic!("All must return complete identity")
    };
    assert_eq!(absent_tags.get("old"), None);
    let DecodedLabels::Complete(empty_tag) = all.decode(&key).unwrap() else {
        panic!("All must return complete identity")
    };
    assert_eq!(empty_tag.get("old"), Some(""));
    let mut complete = PrimaryKeyLabelDecoder::new(
        &source(
            metadata(
                &[(21, "old", ConcreteDataType::string_datatype())],
                PrimaryKeyEncoding::Sparse,
            ),
            "",
            &[(21, "old")],
        ),
        LabelProjection::All,
    )
    .unwrap();
    let DecodedLabels::Complete(empty_name) = complete.decode(&absent).unwrap() else {
        panic!("All must return complete identity")
    };
    assert_eq!(empty_name.name(), Some(""));
    let named_source = source(
        metadata(
            &[(21, "old", ConcreteDataType::string_datatype())],
            PrimaryKeyEncoding::Sparse,
        ),
        "old_metric",
        &[(21, "old")],
    );
    let mut complete = PrimaryKeyLabelDecoder::new(&named_source, LabelProjection::All).unwrap();
    let DecodedLabels::Complete(named) = complete.decode(&absent).unwrap() else {
        panic!("All must return complete identity")
    };
    assert_eq!(named.name(), Some("old_metric"));
    // A null value is represented by its sparse column ID and null marker.
    let mut null = Vec::new();
    codec.encode_internal(42, 9, &mut null).unwrap();
    null.extend_from_slice(&21_u32.to_be_bytes());
    null.push(0);
    assert_eq!(
        decoder.decode(&null).unwrap(),
        DecodedLabels::Projected(vec![])
    );
    drop(decoder);
    drop(all);
    assert!(weak_metadata.upgrade().is_none());
}

#[test]
fn projection_does_not_hide_bad_framing_or_materialize_unselected_values() {
    let meta = metadata(
        &[
            (11, "wanted", ConcreteDataType::string_datatype()),
            (35, "unwanted", ConcreteDataType::string_datatype()),
        ],
        PrimaryKeyEncoding::Sparse,
    );
    let bound = source(meta, "m", &[(11, "wanted"), (35, "unwanted")]);
    let codec = SparsePrimaryKeyCodec::with_fields(vec![
        (11, SortField::new(ConcreteDataType::string_datatype())),
        (35, SortField::new(ConcreteDataType::string_datatype())),
    ]);
    let mut key = Vec::new();
    codec.encode_internal(42, 9, &mut key).unwrap();
    codec
        .encode_raw_tag_value(
            [(11, b"ok".as_slice()), (35, &[0xff])].into_iter(),
            &mut key,
        )
        .unwrap();

    let mut only = PrimaryKeyLabelDecoder::new(
        &bound,
        LabelProjection::Only(Arc::from([Arc::<str>::from("wanted")])),
    )
    .unwrap();
    assert_eq!(
        only.decode(&key).unwrap(),
        DecodedLabels::Projected(vec![Label {
            name: Arc::from("wanted"),
            value: Arc::from("ok")
        },])
    );
    let mut all = PrimaryKeyLabelDecoder::new(&bound, LabelProjection::All).unwrap();
    assert!(all.decode(&key).is_err());
    let mut malformed = key.clone();
    malformed.pop();
    assert!(only.decode(&malformed).is_err());
    assert!(all.decode(&malformed).is_err());
    let mut empty_projection =
        PrimaryKeyLabelDecoder::new(&bound, LabelProjection::Only(Arc::from([]))).unwrap();
    assert!(empty_projection.decode(&malformed).is_err());
}

#[test]
fn dense_projection_still_rejects_short_and_extra_key_bytes() {
    let meta = metadata(
        &[(17, "label", ConcreteDataType::string_datatype())],
        PrimaryKeyEncoding::Dense,
    );
    let bound = source(meta, "m", &[(17, "label")]);
    let key = encoded(
        PrimaryKeyEncoding::Dense,
        &[(17, Value::String("ok".into()))],
    );
    let mut decoder =
        PrimaryKeyLabelDecoder::new(&bound, LabelProjection::Only(Arc::from([]))).unwrap();
    assert!(decoder.decode(&key[..key.len() - 1]).is_err());
    let mut extra = key;
    extra.push(0);
    assert!(decoder.decode(&extra).is_err());
}

#[test]
fn dense_unselected_invalid_utf8_is_not_materialized() {
    let meta = metadata(
        &[
            (17, "wanted", ConcreteDataType::string_datatype()),
            (18, "unwanted", ConcreteDataType::string_datatype()),
        ],
        PrimaryKeyEncoding::Dense,
    );
    let bound = source(meta, "m", &[(17, "wanted"), (18, "unwanted")]);
    let first_value = encoded(
        PrimaryKeyEncoding::Dense,
        &[(17, Value::String("ok".into()))],
    );
    let mut key = encoded(
        PrimaryKeyEncoding::Dense,
        &[
            (17, Value::String("ok".into())),
            (18, Value::String("x".into())),
        ],
    );
    key[first_value.len() + 2] = 0xff;
    let mut only = PrimaryKeyLabelDecoder::new(
        &bound,
        LabelProjection::Only(Arc::from([Arc::<str>::from("wanted")])),
    )
    .unwrap();
    assert_eq!(
        only.decode(&key).unwrap(),
        DecodedLabels::Projected(vec![Label {
            name: Arc::from("wanted"),
            value: Arc::from("ok")
        }])
    );
    let mut selected_invalid = PrimaryKeyLabelDecoder::new(
        &bound,
        LabelProjection::Only(Arc::from([Arc::<str>::from("unwanted")])),
    )
    .unwrap();
    assert!(selected_invalid.decode(&key).is_err());
    let mut all = PrimaryKeyLabelDecoder::new(&bound, LabelProjection::All).unwrap();
    assert!(all.decode(&key).is_err());
}

#[test]
fn unknown_sparse_ids_are_skipped_only_for_projection_and_full_keys_stay_distinct() {
    let meta = metadata(
        &[(11, "wanted", ConcreteDataType::string_datatype())],
        PrimaryKeyEncoding::Sparse,
    );
    let bound = source(meta, "m", &[(11, "wanted")]);
    let codec = SparsePrimaryKeyCodec::with_fields(vec![(
        11,
        SortField::new(ConcreteDataType::string_datatype()),
    )]);
    let mut first = Vec::new();
    codec.encode_internal(42, 9, &mut first).unwrap();
    codec
        .encode_raw_tag_value(
            [
                (10, b"before".as_slice()),
                (11, b"one".as_slice()),
                (12, b"between".as_slice()),
                (90, b"after".as_slice()),
            ]
            .into_iter(),
            &mut first,
        )
        .unwrap();
    let original_first = first.clone();
    let mut second = Vec::new();
    codec.encode_internal(42, 9, &mut second).unwrap();
    codec
        .encode_raw_tag_value(
            [
                (10, b"before".as_slice()),
                (11, b"two".as_slice()),
                (12, b"between".as_slice()),
                (90, b"after".as_slice()),
            ]
            .into_iter(),
            &mut second,
        )
        .unwrap();

    let mut only = PrimaryKeyLabelDecoder::new(
        &bound,
        LabelProjection::Only(Arc::from([Arc::<str>::from("wanted")])),
    )
    .unwrap();
    assert_eq!(
        only.decode(&first).unwrap(),
        DecodedLabels::Projected(vec![Label {
            name: Arc::from("wanted"),
            value: Arc::from("one")
        },])
    );
    assert_eq!(
        only.decode(&second).unwrap(),
        DecodedLabels::Projected(vec![Label {
            name: Arc::from("wanted"),
            value: Arc::from("two")
        },])
    );
    assert_ne!(first, second);
    assert_eq!(first, original_first);
    let original_second = second.clone();

    let mut all = PrimaryKeyLabelDecoder::new(&bound, LabelProjection::All).unwrap();
    assert!(all.decode(&first).is_err());
    assert_eq!(first, original_first);
    assert_eq!(second, original_second);
}

#[test]
fn unknown_sparse_ids_before_between_and_after_known_tags_are_projected() {
    let meta = metadata(
        &[
            (11, "wanted", ConcreteDataType::string_datatype()),
            (13, "other", ConcreteDataType::string_datatype()),
        ],
        PrimaryKeyEncoding::Sparse,
    );
    let bound = source(meta, "m", &[(11, "wanted"), (13, "other")]);
    let codec = SparsePrimaryKeyCodec::schemaless();
    let cases: [&[(u32, &[u8])]; 3] = [
        &[(10, b"before"), (11, b"one"), (12, b"two")],
        &[(11, b"one"), (12, b"between"), (13, b"two")],
        &[(11, b"one"), (13, b"two"), (90, b"after")],
    ];
    for tags in cases {
        let mut key = Vec::new();
        codec.encode_internal(42, 9, &mut key).unwrap();
        codec
            .encode_raw_tag_value(tags.iter().copied(), &mut key)
            .unwrap();
        let mut only = PrimaryKeyLabelDecoder::new(
            &bound,
            LabelProjection::Only(Arc::from([Arc::<str>::from("wanted")])),
        )
        .unwrap();
        assert_eq!(
            only.decode(&key).unwrap(),
            DecodedLabels::Projected(vec![Label {
                name: Arc::from("wanted"),
                value: Arc::from("one")
            }])
        );
        let mut all = PrimaryKeyLabelDecoder::new(&bound, LabelProjection::All).unwrap();
        assert!(all.decode(&key).is_err());
    }
}

#[test]
fn constructor_rejects_invalid_bindings_but_accepts_valid_control() {
    let meta = metadata(
        &[
            (11, "wanted", ConcreteDataType::string_datatype()),
            (35, "other", ConcreteDataType::string_datatype()),
        ],
        PrimaryKeyEncoding::Sparse,
    );
    for bindings in [
        vec![(11, "wrong")],
        vec![(11, "same"), (35, "same")],
        vec![(11, "wanted"), (11, "wanted")],
        vec![(0, "ts")],
        vec![(90, "not_a_tag")],
        vec![(99, "field")],
        vec![(11, "__name__")],
        vec![(u32::MAX, "internal")],
    ] {
        let invalid = source(meta.clone(), "m", &bindings);
        assert!(PrimaryKeyLabelDecoder::new(&invalid, LabelProjection::All).is_err());
    }
    let valid = source(meta, "m", &[(11, "wanted"), (35, "other")]);
    assert!(PrimaryKeyLabelDecoder::new(&valid, LabelProjection::All).is_ok());
    assert!(PrimaryKeyLabelDecoder::new(&RawLabelSource::Explicit, LabelProjection::All).is_err());
}

#[test]
fn dense_typed_values_keep_exact_integer_string_representation() {
    let cases = [
        (
            "count",
            ConcreteDataType::int64_datatype(),
            Value::Int64(9_007_199_254_740_993),
        ),
        (
            "binary",
            ConcreteDataType::binary_datatype(),
            Value::Binary(vec![0xab, 0xcd].into()),
        ),
        (
            "date",
            ConcreteDataType::date_datatype(),
            Value::Date(Date::new(0)),
        ),
        (
            "timestamp",
            ConcreteDataType::timestamp_millisecond_datatype(),
            Value::Timestamp(Timestamp::new(1234, TimeUnit::Millisecond)),
        ),
    ];
    for (name, data_type, value) in cases {
        let expected = value.to_string();
        let meta = metadata(&[(17, name, data_type)], PrimaryKeyEncoding::Dense);
        let bound = source(meta, "typed", &[(17, name)]);
        let key = encoded(PrimaryKeyEncoding::Dense, &[(17, value)]);
        let mut decoder = PrimaryKeyLabelDecoder::new(&bound, LabelProjection::All).unwrap();
        let DecodedLabels::Complete(metric) = decoder.decode(&key).unwrap() else {
            panic!("All must return a complete metric identity")
        };
        assert_eq!(metric.get(name), Some(expected.as_str()));
    }
    assert_eq!(
        Value::Int64(9_007_199_254_740_993).to_string(),
        "9007199254740993"
    );
}

/// One field is enough: the frame schema only carries ordered payload fields and metadata.
fn frame_series_schema() -> Arc<datatypes::series::samples::SeriesSchema> {
    Arc::new(datatypes::series::samples::SeriesSchema {
        fields: vec![datatypes::series::samples::FieldSpec {
            name: Arc::from("value"),
        }]
        .into(),
        eval_grid: None,
    })
}

/// A key-encoded frame's schema describes its own labels: recovering the description from the
/// schema and decoding with it matches decoding with the original bound source, for both primary
/// key encodings and both projections.
#[test]
fn frame_label_decode_description_decodes_like_the_bound_source() {
    for encoding in [PrimaryKeyEncoding::Dense, PrimaryKeyEncoding::Sparse] {
        let bound = source(
            string_metadata(encoding),
            "requests_total",
            &[(8, "zeta"), (41, "alpha")],
        );
        let description = common_query::series_frame::label_decode_spec(&bound)
            .expect("a primary-key binding describes its key layout");
        let frame_schema = datatypes::series::frame::raw_series_frame_schema_with_label_decode(
            &frame_series_schema(),
            datatypes::series::frame::RawLabelMode::Key,
            Some(&description),
        )
        .unwrap();
        let recovered = datatypes::series::frame::raw_series_frame_label_decode(&frame_schema)
            .unwrap()
            .expect("the frame declares its decode description");
        assert_eq!(recovered, description);

        let key = encoded(encoding, &values());
        for projection in [
            LabelProjection::All,
            LabelProjection::Only(Arc::from([Arc::<str>::from("alpha")])),
            LabelProjection::Only(Arc::from([])),
        ] {
            let mut direct = PrimaryKeyLabelDecoder::new(&bound, projection.clone()).unwrap();
            let mut from_frame =
                PrimaryKeyLabelDecoder::from_decode_spec(&recovered, projection).unwrap();
            assert_eq!(
                from_frame.decode(&key).unwrap(),
                direct.decode(&key).unwrap()
            );
        }
    }
}

/// A description this revision cannot honour is rejected when a decoder is bound, not decoded
/// best-effort.
#[test]
fn frame_label_decode_description_rejects_unsupported_version() {
    let bound = source(
        string_metadata(PrimaryKeyEncoding::Dense),
        "requests_total",
        &[(8, "zeta")],
    );
    let description = common_query::series_frame::label_decode_spec(&bound).unwrap();
    let mut future = description.clone();
    future.version += 1;
    assert!(PrimaryKeyLabelDecoder::from_decode_spec(&future, LabelProjection::All).is_err());

    // The description must list the column a logical tag reads.
    let mut unbound = description;
    unbound.primary_key.retain(|column| column.column_id != 8);
    assert!(PrimaryKeyLabelDecoder::from_decode_spec(&unbound, LabelProjection::All).is_err());
}
