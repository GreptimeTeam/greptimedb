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

use std::collections::HashMap;
use std::sync::Arc;

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datatypes::arrow::buffer::ScalarBuffer;
use datatypes::series::raw::RawTimeAxis;
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, MetricName};

use super::*;

fn node() -> InstantManipulate {
    let input = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    });
    InstantManipulate::new(
        10,
        20,
        10,
        10,
        0,
        "time".into(),
        vec![],
        Some("value".into()),
        input,
    )
}

fn schema(tag_type: DataType) -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert("schema".to_string(), "kept".to_string());
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("value", DataType::Float64, true)
                .with_metadata([("field-key".to_string(), "field-value".to_string())].into()),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("zone", tag_type, true),
        ],
        metadata,
    ))
}

fn series_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: None,
    })
}

fn metric(zone: Option<&str>) -> MetricName {
    let labels = zone
        .map(|value| {
            vec![Label {
                name: Arc::from("zone"),
                value: Arc::from(value),
            }]
        })
        .unwrap_or_default();
    MetricName::try_new(None, labels).unwrap()
}

#[test]
fn evaluates_sliced_chunked_samples_and_preserves_original_schema() {
    let output_schema = schema(DataType::Dictionary(
        Box::new(DataType::UInt32),
        Box::new(DataType::Utf8),
    ));
    let mapping = NativeInstantMapping::try_new(
        &node(),
        output_schema.clone(),
        series_schema(),
        &["zone".into()],
    )
    .unwrap()
    .unwrap();
    let a = Float64Array::from(vec![Some(99.0), Some(1.0), None]);
    let b = Float64Array::from(vec![Some(2.0), Some(3.0)]);
    let samples = Samples::try_from_chunks(vec![
        Samples::from_float(
            a.slice(1, 2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone(),
        ),
        Samples::from_float(b),
    ])
    .unwrap();
    let fields = SeriesFields::one(samples);
    let time = RawTimeAxis::try_new(ScalarBuffer::from(vec![0, 10, 20, 30])).unwrap();
    let batch = mapping.evaluate(&metric(Some("")), &time, &fields).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), output_schema);
    let output_values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(output_values.is_null(0));
    assert_eq!(output_values.value(1), 2.0);
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values(),
        &[10, 20]
    );
    let tag_values = datafusion::arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
    let tag_values = tag_values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(tag_values.value(0), "");
    assert_eq!(tag_values.value(1), "");
}

#[test]
fn selected_repeated_indices_across_sliced_chunks_preserve_missing_and_nan() {
    let mut query = node();
    query.end = 30;
    query.lookback_delta = 20;
    let mapping = NativeInstantMapping::try_new(
        &query,
        schema(DataType::Utf8),
        series_schema(),
        &["zone".into()],
    )
    .unwrap()
    .unwrap();
    let first = Float64Array::from(vec![Some(99.0), None]).slice(1, 1);
    let second = Float64Array::from(vec![Some(f64::NAN), Some(88.0)]).slice(0, 1);
    let samples = Samples::try_from_chunks(vec![
        Samples::from_float(
            first
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone(),
        ),
        Samples::from_float(
            second
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone(),
        ),
    ])
    .unwrap();
    let fields = SeriesFields::one(samples);
    let time = RawTimeAxis::try_new(ScalarBuffer::from(vec![10, 30])).unwrap();

    let batch = mapping.evaluate(&metric(None), &time, &fields).unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert!(!values.is_null(2));
    assert!(values.value(2).is_nan());
}

#[test]
fn maps_all_supported_string_types_and_distinguishes_missing_from_empty() {
    let tag_types = [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
    ];
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    let time = RawTimeAxis::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();

    for tag_type in tag_types {
        let output_schema = schema(tag_type);
        let mapping = NativeInstantMapping::try_new(
            &node(),
            output_schema.clone(),
            series_schema(),
            &["zone".into()],
        )
        .unwrap()
        .unwrap();

        for (label, expected) in [(Some(""), Some("")), (None, None)] {
            let batch = mapping.evaluate(&metric(label), &time, &fields).unwrap();
            assert_eq!(batch.schema(), output_schema);
            let values =
                datafusion::arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            match expected {
                Some(value) => {
                    assert_eq!(values.value(0), value);
                    assert_eq!(values.value(1), value);
                    assert_eq!(values.null_count(), 0);
                }
                None => assert_eq!(values.null_count(), 2),
            }
        }
    }
}

#[test]
fn selected_field_staleness_and_no_field_semantics_match_existing_selector_contract() {
    let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let times = RawTimeAxis::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();
    let values = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(stale),
    ])));
    let mapping = NativeInstantMapping::try_new(
        &node(),
        schema(DataType::Utf8),
        series_schema(),
        &["zone".into()],
    )
    .unwrap()
    .unwrap();
    let batch = mapping.evaluate(&metric(None), &times, &values).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        10
    );

    let mut no_staleness_node = node();
    no_staleness_node.field_column = None;
    let no_staleness_mapping = NativeInstantMapping::try_new(
        &no_staleness_node,
        schema(DataType::Utf8),
        series_schema(),
        &["zone".into()],
    )
    .unwrap()
    .unwrap();
    let batch = no_staleness_mapping
        .evaluate(&metric(None), &times, &values)
        .unwrap();
    let output = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output.value(1).to_bits(), PROMETHEUS_STALE_NAN_BITS);
}

#[test]
fn rejects_unmapped_missing_duplicate_and_non_millisecond_schema_columns() {
    let valid = schema(DataType::Utf8);
    let mut cases = Vec::new();

    cases.push(Arc::new(Schema::new(vec![
        valid.field(0).clone(),
        valid.field(1).clone(),
        valid.field(2).clone(),
        Field::new("helper", DataType::UInt64, true),
    ])));
    cases.push(Arc::new(Schema::new(vec![
        valid.field(0).clone(),
        valid.field(1).clone(),
        valid.field(2).clone(),
        Field::new("__tsid", DataType::UInt64, true),
    ])));
    cases.push(Arc::new(Schema::new(vec![
        valid.field(0).clone(),
        valid.field(1).clone(),
    ])));
    cases.push(Arc::new(Schema::new(vec![
        valid.field(1).clone(),
        valid.field(2).clone(),
    ])));
    cases.push(Arc::new(Schema::new(vec![
        valid.field(0).clone(),
        valid.field(1).clone(),
        valid.field(2).clone(),
        valid.field(0).clone(),
    ])));
    cases.push(Arc::new(Schema::new(vec![
        valid.field(0).clone(),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        valid.field(2).clone(),
    ])));
    cases.push(Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        valid.field(1).clone(),
        valid.field(2).clone(),
    ])));

    for output_schema in cases {
        assert!(
            NativeInstantMapping::try_new(
                &node(),
                output_schema,
                series_schema(),
                &["zone".into()],
            )
            .unwrap()
            .is_none()
        );
    }
}

#[test]
fn absent_tag_is_null_and_nonzero_offset_is_rejected() {
    let output_schema = schema(DataType::Utf8);
    let mapping =
        NativeInstantMapping::try_new(&node(), output_schema, series_schema(), &["zone".into()])
            .unwrap()
            .unwrap();
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    let time = RawTimeAxis::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();
    let batch = mapping.evaluate(&metric(None), &time, &fields).unwrap();
    assert_eq!(batch.column(2).null_count(), 2);

    let mut unsupported = node();
    unsupported.offset = 1;
    assert!(
        NativeInstantMapping::try_new(
            &unsupported,
            schema(DataType::Utf8),
            series_schema(),
            &["zone".into()],
        )
        .unwrap()
        .is_none()
    );
}
