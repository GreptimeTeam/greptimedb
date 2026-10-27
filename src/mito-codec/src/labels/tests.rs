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
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Value;
use datatypes::schema::ColumnSchema;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::BoundTag;
use store_api::region_engine::RawLabelSource;
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
