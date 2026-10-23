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
use datatypes::series::raw::RawTimestamps;
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, SeriesLabels};

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

fn metric(zone: Option<&str>) -> SeriesLabels {
    let labels = zone
        .map(|value| {
            vec![Label {
                name: Arc::from("zone"),
                value: Arc::from(value),
            }]
        })
        .unwrap_or_default();
    SeriesLabels::try_new(None, labels).unwrap()
}

/// The ordered, deliberately non-lexical field names of the multifloat regression.
const MULTIFLOAT_FIELDS: [&str; 3] = ["value", "a_second", "m_third"];

/// The strict millisecond timestamps of every multifloat case.
const MULTIFLOAT_TIMESTAMPS: [i64; 5] = [1000, 2000, 3000, 4000, 5000];

/// The kept source rows of each configured staleness field: the stale row of the configured
/// field is suppressed, so the first field drops row 3 (4000) and the second field drops row 2
/// (3000) without ever retrying an older sample.
const MULTIFLOAT_KEPT: [[usize; 4]; 2] = [[0, 1, 2, 4], [0, 1, 3, 4]];

/// An ordinary, non-stale NaN payload that must survive the mapping as exact bits.
const ORDINARY_NAN_BITS: u64 = 0x7ff8_0000_0000_0001;

/// The output schema of the regression: the declared field order, then the time index and the
/// tag, with metadata that every case must retain.
///
/// Every declared field is nullable because every fixture lane keeps a NULL row in each of them:
/// the first row of the second and third fields and the second row of the first field.
fn multifloat_schema(field_names: &[&str], tag_type: DataType) -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert("schema".to_string(), "kept".to_string());
    let mut fields = field_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            Field::new(*name, DataType::Float64, true)
                .with_metadata([("field-key".to_string(), format!("field-value-{index}"))].into())
        })
        .collect::<Vec<_>>();
    fields.push(Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));
    fields.push(Field::new("zone", tag_type, true));
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

/// The series schema of the same declared field order.
fn multifloat_series_schema(field_names: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: field_names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

/// The five source rows of each multifloat field.
///
/// Row `i` is the strict millisecond timestamp `MULTIFLOAT_TIMESTAMPS[i]`. The first field is
/// stale at 4000 and carries an ordinary NaN payload at 5000; the unrelated second field is
/// stale at 3000; the fields carry NULLs and a signed zero as well.
fn multifloat_source(field_names: &[&str]) -> Vec<Float64Array> {
    let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let nan = f64::from_bits(ORDINARY_NAN_BITS);
    let rows: [&[Option<f64>]; 3] = [
        &[Some(1.0), None, Some(-0.0), Some(stale), Some(nan)],
        &[None, Some(-0.0), Some(stale), Some(2.0), Some(4.0)],
        &[None, Some(3.0), Some(nan), Some(3.0), Some(5.0)],
    ];
    field_names
        .iter()
        .enumerate()
        .map(|(index, _)| Float64Array::from(rows[index].to_vec()))
        .collect()
}

/// The chunked samples of the multifloat input: the first field arrives in three chunks, two of
/// which are slices with non-zero offsets, and every other field arrives as one dense array.
fn multifloat_fields(source: &[Float64Array]) -> SeriesFields {
    let fields = source
        .iter()
        .enumerate()
        .map(|(index, values)| {
            if index > 0 {
                return Samples::from_float(values.clone());
            }
            Samples::try_from_chunks(vec![
                Samples::from_float(values.slice(0, 1)),
                Samples::from_float(values.slice(1, 3)),
                Samples::from_float(values.slice(4, 1)),
            ])
            .unwrap()
        })
        .collect::<Vec<_>>();
    SeriesFields::try_from_vec(fields).unwrap()
}

/// The bounded instant declaration of the regression: one output point per input millisecond
/// with one lookback step, whose staleness field is exactly one declared field.
fn multifloat_node(field_column: &str) -> InstantManipulate {
    let mut query = node();
    query.start = MULTIFLOAT_TIMESTAMPS[0];
    query.end = MULTIFLOAT_TIMESTAMPS[4];
    query.lookback_delta = 1000;
    query.interval = 1000;
    query.field_column = Some(field_column.to_string());
    query
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
    let time = RawTimestamps::try_new(ScalarBuffer::from(vec![0, 10, 20, 30])).unwrap();
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
    let time = RawTimestamps::try_new(ScalarBuffer::from(vec![10, 30])).unwrap();

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
    let time = RawTimestamps::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();

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
    let times = RawTimestamps::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();
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
    let time = RawTimestamps::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();
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

#[test]
fn multifloat_native_mapping_keeps_ordered_fields_and_checks_only_the_configured_field() {
    let time = RawTimestamps::try_new(ScalarBuffer::from(MULTIFLOAT_TIMESTAMPS.to_vec())).unwrap();

    for field_names in [&MULTIFLOAT_FIELDS[..2], &MULTIFLOAT_FIELDS[..3]] {
        for (configured, kept) in MULTIFLOAT_KEPT.iter().enumerate() {
            let output_schema = multifloat_schema(field_names, DataType::Utf8);
            let source = multifloat_source(field_names);
            let fields = multifloat_fields(&source);
            let mapping = NativeInstantMapping::try_new(
                &multifloat_node(field_names[configured]),
                output_schema.clone(),
                multifloat_series_schema(field_names),
                &["zone".into()],
            )
            .unwrap()
            .expect("the complete ordered Float64 fields are mapped instead of declined");
            let batch = mapping
                .evaluate(&metric(Some("multifloat")), &time, &fields)
                .unwrap();

            // The complete output schema, including its metadata and nullability, is retained.
            assert_eq!(batch.schema(), output_schema);
            assert_eq!(batch.num_rows(), kept.len());
            let kept_timestamps = batch
                .column(field_names.len())
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values()
                .to_vec();
            assert_eq!(
                kept_timestamps,
                kept.iter()
                    .map(|row| MULTIFLOAT_TIMESTAMPS[*row])
                    .collect::<Vec<_>>()
            );
            let suppressed = if configured == 0 { 3 } else { 2 };
            let unrelated = if configured == 0 { 2 } else { 3 };
            assert!(
                !kept_timestamps.contains(&MULTIFLOAT_TIMESTAMPS[suppressed]),
                "the stale row of the configured field is suppressed instead of retried"
            );
            assert!(
                kept_timestamps.contains(&MULTIFLOAT_TIMESTAMPS[unrelated]),
                "the stale marker of another field does not suppress its row"
            );

            // Every output field keeps the nullness and the exact bits of its own source row.
            for (index, values) in source.iter().enumerate() {
                let output = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for (row, source_row) in kept.iter().enumerate() {
                    if values.is_null(*source_row) {
                        assert!(output.is_null(row));
                    } else {
                        assert!(!output.is_null(row));
                        assert_eq!(
                            output.value(row).to_bits(),
                            values.value(*source_row).to_bits(),
                            "field {index} row {row} must keep its exact source bits"
                        );
                    }
                }
            }

            // The unchecked stale marker travels through as exact bits, so the row of another
            // field is kept with the marker intact instead of being compared or re-encoded.
            let (unchecked_field, unchecked_row) = if configured == 0 { (1, 2) } else { (0, 3) };
            let unchecked_output_row = kept
                .iter()
                .position(|row| *row == unchecked_row)
                .expect("the unchecked stale row is kept");
            let marker = batch
                .column(unchecked_field)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(unchecked_output_row);
            assert_eq!(marker.to_bits(), PROMETHEUS_STALE_NAN_BITS);

            // The ordinary NaN payload and the signed zero of the first two fields survive as
            // exact bits as well.
            let nan_row = kept
                .iter()
                .position(|row| *row == 4)
                .expect("the ordinary NaN row is kept");
            let nan = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(nan_row);
            assert_eq!(nan.to_bits(), ORDINARY_NAN_BITS);
            let signed_zero_row = kept
                .iter()
                .position(|row| *row == 1)
                .expect("the signed zero row is kept");
            let signed_zero = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(signed_zero_row);
            assert_eq!(signed_zero.to_bits(), (-0.0_f64).to_bits());

            // The series key of every kept row is the requested label value.
            let tags = datafusion::arrow::compute::cast(
                batch.column(field_names.len() + 1),
                &DataType::Utf8,
            )
            .unwrap();
            let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(tags.null_count(), 0);
            assert!(tags.iter().all(|value| value == Some("multifloat")));
        }
    }
}
