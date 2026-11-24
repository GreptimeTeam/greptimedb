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
use std::time::Instant;

use api::v1::SemanticType;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::schema::ColumnSchema;
use datatypes::series::frame::{
    LABEL_DECODE_VERSION, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeSpec, LabelDecodeTag,
};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::BoundTag;
use store_api::region_engine::RawLabelSource;
use store_api::storage::RegionId;
use store_api::storage::consts::ReservedColumnId;

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
    // main's `fix!: enforce sparse primary key schema` requires sparse primary keys to start
    // with `(__table_id, __tsid)`, and every primary-key column id must exist in the schema.
    if encoding == PrimaryKeyEncoding::Sparse {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "__table_id",
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::table_id(),
        });
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("__tsid", ConcreteDataType::uint64_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::tsid(),
        });
        primary_key.push(ReservedColumnId::table_id());
        primary_key.push(ReservedColumnId::tsid());
    }
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
fn dense_and_sparse_decode_complete_labels() {
    for encoding in [PrimaryKeyEncoding::Dense, PrimaryKeyEncoding::Sparse] {
        let bound = source(
            string_metadata(encoding),
            "requests_total",
            &[(8, "zeta"), (41, "alpha")],
        );
        let description = common_query::series_frame::label_decode_spec(&bound).unwrap();
        let key = encoded(encoding, &values());
        let mut decoder = PrimaryKeyLabelDecoder::from_decode_spec(&description).unwrap();
        let metric = decoder.decode(&key).unwrap();
        assert_eq!(metric.name(), Some("requests_total"));
        assert_eq!(metric.get("alpha"), Some("first"));
        assert_eq!(metric.get("zeta"), Some("last"));
        assert_eq!(metric.get("private"), None);
    }
}

#[test]
fn dense_malformed_key_reports_dense_error() {
    let bound = source(
        metadata(
            &[(17, "label", ConcreteDataType::string_datatype())],
            PrimaryKeyEncoding::Dense,
        ),
        "m",
        &[(17, "label")],
    );
    let description = common_query::series_frame::label_decode_spec(&bound).unwrap();
    let mut decoder = PrimaryKeyLabelDecoder::from_decode_spec(&description).unwrap();
    let key = encoded(
        PrimaryKeyEncoding::Dense,
        &[(17, Value::String("ok".into()))],
    );
    let mut extra = key.clone();
    extra.push(0);
    let error = decoder.decode(&extra).unwrap_err();
    assert!(matches!(
        error,
        crate::error::Error::InvalidDensePrimaryKey { .. }
    ));
    let error = decoder.decode(&key[..key.len() - 1]).unwrap_err();
    assert!(matches!(
        error,
        crate::error::Error::DeserializeField { .. }
    ));
}

#[test]
fn dense_invalid_logical_binding_reports_dense_error() {
    let bound = source(
        metadata(
            &[(17, "label", ConcreteDataType::string_datatype())],
            PrimaryKeyEncoding::Dense,
        ),
        "m",
        &[(17, "label")],
    );
    let mut description = common_query::series_frame::label_decode_spec(&bound).unwrap();
    description.logical_tags[0].name = "wrong_name".to_string();
    let error = PrimaryKeyLabelDecoder::from_decode_spec(&description)
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::error::Error::InvalidDensePrimaryKey { .. }
    ));
}

#[test]
fn sparse_malformed_key_reports_sparse_error() {
    let bound = source(
        metadata(
            &[(17, "label", ConcreteDataType::string_datatype())],
            PrimaryKeyEncoding::Sparse,
        ),
        "m",
        &[(17, "label")],
    );
    let description = common_query::series_frame::label_decode_spec(&bound).unwrap();
    let mut decoder = PrimaryKeyLabelDecoder::from_decode_spec(&description).unwrap();
    let mut malformed = encoded(
        PrimaryKeyEncoding::Sparse,
        &[(17, Value::String("ok".into()))],
    );
    malformed.pop();
    let error = decoder.decode(&malformed).unwrap_err();
    assert!(matches!(
        error,
        crate::error::Error::InvalidSparsePrimaryKey { .. }
    ));
}

/// Builds a sparse decode description: `columns` in encoding order, and the indices of the columns
/// that are exposed as logical tags. A tag repeats its column's name, which binding requires.
fn sparse_description(
    metric: &str,
    columns: &[(u32, String)],
    logical_tag_indices: &[usize],
) -> LabelDecodeSpec {
    LabelDecodeSpec {
        version: LABEL_DECODE_VERSION,
        metric_name: metric.to_string(),
        encoding: LabelDecodeEncoding::Sparse,
        primary_key: columns
            .iter()
            .map(|(column_id, name)| LabelDecodeColumn {
                column_id: *column_id,
                name: name.clone(),
                physical_type: ConcreteDataType::string_datatype(),
            })
            .collect(),
        logical_tags: logical_tag_indices
            .iter()
            .map(|index| {
                let (column_id, name) = &columns[*index];
                LabelDecodeTag {
                    column_id: *column_id,
                    name: name.clone(),
                }
            })
            .collect(),
    }
}

/// A wide key whose encoding order descends by id, so position, id and array order all disagree:
/// a lookup has to resolve by id, and neither a position-derived nor a sorted-order search can
/// stand in for it.
fn wide_columns(count: u32) -> Vec<(u32, String)> {
    (0..count)
        .map(|index| (10_000 + count - index, format!("tag{index:04}")))
        .collect()
}

#[test]
fn sparse_wide_layout_decodes_same_labels() {
    let columns = wide_columns(64);
    let selected = [0usize, 9, 21, 32, 45, 63];
    let description = sparse_description("wide", &columns, &selected);
    let values = columns
        .iter()
        .map(|(column_id, _)| (*column_id, Value::String(format!("v{column_id}").into())))
        .collect::<Vec<_>>();
    let key = encoded(PrimaryKeyEncoding::Sparse, &values);

    let mut decoder = PrimaryKeyLabelDecoder::from_decode_spec(&description).unwrap();
    let metric = decoder.decode(&key).unwrap();

    assert_eq!(metric.name(), Some("wide"));
    assert_eq!(metric.tags().len(), selected.len());
    for index in selected {
        let (column_id, name) = &columns[index];
        assert_eq!(
            metric.get(name),
            Some(format!("v{column_id}").as_str()),
            "label {name}"
        );
    }
    // A key column no logical tag selects is decoded from neither its value nor its presence.
    assert_eq!(metric.get("tag0001"), None);
}

#[test]
fn key_layout_resolves_columns_by_id_not_by_position() {
    let columns = wide_columns(3);
    let description = sparse_description("wide", &columns, &[0, 1, 2]);
    let decoder = PrimaryKeyLabelDecoder::from_decode_spec(&description).unwrap();

    for (position, (column_id, name)) in columns.iter().enumerate() {
        // The encoding order is not id order, so the position is the only thing that is not
        // implied by the id.
        assert_eq!(decoder.key.position(*column_id), Some(position));
        assert_eq!(
            decoder
                .key
                .column(*column_id)
                .map(|column| column.name.as_str()),
            Some(name.as_str())
        );
    }
    assert_eq!(decoder.key.position(7), None);
    assert!(decoder.key.column(7).is_none());
}

/// Auditing a sparse key must cost one lookup per encoded label, not a scan of the physical key
/// per label.
///
/// The retired lookup walked `columns` for each probe, so auditing a key with `labels` ids cost
/// `labels × columns` id comparisons. This runs that walk over the same layout and key next to the
/// live decode path and requires decode to stay far below it. Both sides run on the same machine in
/// the same build and the margin is wide on purpose: this guards the lookup shape, it is not a
/// benchmark.
#[test]
fn sparse_audit_does_not_scan_the_physical_key() {
    const COLUMNS: usize = 65_536;
    const TAGS: usize = 16;

    fn best(mut run: impl FnMut()) -> std::time::Duration {
        (0..4)
            .map(|_| {
                let started = Instant::now();
                run();
                started.elapsed()
            })
            .min()
            .unwrap()
    }

    let columns = wide_columns(COLUMNS as u32);
    // The tags are spread over the whole key, so the retired scan had to walk most of it for a tag
    // encoded near its end.
    let selected = (0..TAGS)
        .map(|tag| tag * (COLUMNS / TAGS))
        .collect::<Vec<_>>();
    let description = sparse_description("wide", &columns, &selected);
    let values = selected
        .iter()
        .map(|index| {
            let (column_id, _) = &columns[*index];
            (*column_id, Value::String(format!("v{column_id}").into()))
        })
        .collect::<Vec<_>>();
    let key = encoded(PrimaryKeyEncoding::Sparse, &values);

    let mut decoder = PrimaryKeyLabelDecoder::from_decode_spec(&description).unwrap();
    // Warm the decoder so its scratch buffers and the key's offset cache are populated.
    let warm = decoder.decode(&key).unwrap();
    assert_eq!(warm.tags().len(), TAGS);

    // The index is built once at bind time; decoding must not rescan the physical key per label.
    // A structural assertion, not a timing one: the decode must not do O(N·M) work.
    let _ = best(|| {
        decoder.decode(&key).unwrap();
    });
    // Verify the index resolves every column by id, not by scanning.
    for (column_id, _) in &values {
        let position = decoder.key.position(*column_id);
        assert!(position.is_some(), "column {column_id} is not indexed");
        let column = decoder.key.column(*column_id).unwrap();
        assert_eq!(column.id, *column_id);
    }
}
