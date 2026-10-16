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
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::memory::MemoryStream;
use datatypes::arrow::buffer::ScalarBuffer;
use datatypes::series::raw::RawTimeAxis;
use datatypes::series::samples::{Samples, SeriesFields};

use super::*;
use crate::extension_plan::range_manipulate::ScanNanoseconds;
use crate::extension_plan::test_util::TIME_INDEX_COLUMN;
use crate::instant_eval::select_instants;

fn stream(
    batch: &RecordBatch,
    start: i64,
    end: i64,
    step: i64,
    lookback: i64,
    offset: i64,
) -> InstantManipulateStream {
    let schema = batch.schema();
    InstantManipulateStream {
        offset,
        start,
        end,
        lookback_delta: lookback,
        interval: step,
        time_index: 0,
        time_unit: TimeUnit::Millisecond,
        field_indices: [Some(1), None],
        tsid_index: None,
        reuse_tsid_column: false,
        schema: schema.clone(),
        input: Box::pin(MemoryStream::try_new(vec![], schema, None).unwrap()),
        metric: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        num_series: Count::new(),
    }
}

fn input_batch(times: Vec<i64>, first: Vec<Option<f64>>, second: Vec<Option<f64>>) -> RecordBatch {
    let len = times.len();
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert("schema-key".to_string(), "schema-value".to_string());
    let mut field_metadata = HashMap::new();
    field_metadata.insert("field-key".to_string(), "field-value".to_string());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true).with_metadata(field_metadata),
            Field::new("other", DataType::Float64, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("exact", DataType::Int64, false),
        ],
        schema_metadata,
    ));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(times)),
            Arc::new(Float64Array::from(first)),
            Arc::new(Float64Array::from(second)),
            Arc::new(StringArray::from_iter_values((0..len).map(|_| "payload"))),
            Arc::new(Int64Array::from(
                (0..len)
                    .map(|i| 9_007_199_254_740_993_i64 + i as i64 * 2)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn eligible_arrow_stream_uses_typed_selection_and_preserves_payload() {
    let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let original = input_batch(
        vec![0, 10, 20, 30],
        vec![Some(1.0), Some(2.0), None, Some(4.0)],
        vec![Some(10.0), Some(20.0), Some(stale), Some(40.0)],
    );
    let view = original.slice(1, 3);
    drop(original);
    let source_schema = view.schema();
    let expected_schema = source_schema.clone();
    let source = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![view]], source_schema.clone(), None).unwrap(),
    )));
    let exec = Arc::new(InstantManipulateExec {
        offset: 0,
        start: 10,
        end: 30,
        lookback_delta: 10,
        interval: 10,
        time_index_column: TIME_INDEX_COLUMN.to_string(),
        field_column: Some("value".to_string()),
        reuse_tsid_column: false,
        output_schema: source_schema,
        properties: source.properties().clone(),
        input: source,
        metric: ExecutionPlanMetricsSet::new(),
    });
    let batches = datafusion::physical_plan::collect(exec, SessionContext::default().task_ctx())
        .await
        .unwrap();
    let result = &batches[0];
    let times = result
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(times.values(), &[10, 20, 30]);
    let values = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.values(), &[2.0, 0.0, 4.0]);
    assert!(!values.is_valid(1));
    let payload = result
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(payload.iter().collect::<Vec<_>>(), [Some("payload"); 3]);
    let exact = result
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        exact.values(),
        &[
            9_007_199_254_740_995,
            9_007_199_254_740_997,
            9_007_199_254_740_999
        ]
    );
    let secondary = result
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(secondary.value(1).to_bits(), PROMETHEUS_STALE_NAN_BITS);
    assert_eq!(result.schema(), expected_schema);
    assert_eq!(
        result.schema().metadata().get("schema-key").unwrap(),
        "schema-value"
    );
    assert_eq!(
        result
            .schema()
            .field(1)
            .metadata()
            .get("field-key")
            .unwrap(),
        "field-value"
    );
}

#[test]
fn typed_selection_matches_existing_wide_native_scan_oracle() {
    let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
    let timestamps = [0, 10, 20, 30, 40];
    let values = [Some(1.0), Some(f64::NAN), Some(3.0), Some(stale), Some(5.0)];
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec())));
    let axis = RawTimeAxis::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap();
    let typed = select_instants(&axis, &fields, 5..=45, 10, 20, 0).unwrap();
    let batch = input_batch(timestamps.to_vec(), values.to_vec(), vec![Some(0.0); 5]);
    let scanner = stream(&batch, 5, 45, 10, 20, 0);
    let old = scanner.scan_instants::<i128>(
        &timestamps,
        5,
        45,
        &ScanNanoseconds::<i128>::wide(TimeUnit::Millisecond, 0, 20),
        5,
        |row| row == 3,
    );
    assert_eq!(typed, old);
    // Evaluations 5 and 15 select row 0/1; 25 selects 20, stale row 3 suppresses 35,
    // and 45 selects row 4. The lower lookback boundary is exclusive.
    assert_eq!(typed, (vec![0, 1, 2, 4], vec![5, 15, 25, 45]));

    for (range, lookback, expected) in [
        (10..=20, 0, (vec![1, 2], vec![10, 20])),
        (20..=20, 10, (vec![2], vec![20])),
    ] {
        let selected = select_instants(&axis, &fields, range, 10, lookback, 0).unwrap();
        assert_eq!(selected, expected);
    }
    let offset = select_instants(&axis, &fields, 15..=35, 10, 20, 10).unwrap();
    assert_eq!(offset, (vec![0, 1, 2], vec![15, 25, 35]));

    let lower_axis = RawTimeAxis::try_new(ScalarBuffer::from(vec![0, 10])).unwrap();
    let lower_fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    assert_eq!(
        select_instants(&lower_axis, &lower_fields, 20..=20, 1, 10, 0).unwrap(),
        (vec![], vec![])
    );
    let missing_fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        None,
    ])));
    assert_eq!(
        select_instants(&lower_axis, &missing_fields, 10..=10, 1, 10, 0).unwrap(),
        (vec![1], vec![10])
    );

    let edge_axis = RawTimeAxis::try_new(ScalarBuffer::from(vec![i64::MIN, i64::MAX])).unwrap();
    let edge_fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    assert_eq!(
        select_instants(
            &edge_axis,
            &edge_fields,
            i64::MIN..=i64::MIN,
            1,
            0,
            i64::MAX
        )
        .unwrap(),
        (vec![], vec![])
    );
}

#[test]
fn duplicate_arrow_timestamps_fall_back_and_invalid_typed_arguments_propagate() {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        Some(2.0),
    ])));
    let duplicate = RawTimeAxis::Explicit(ScalarBuffer::from(vec![10, 10]));
    assert!(select_instants(&duplicate, &fields, 10..=11, 1, 2, 0).is_err());
    let batch = input_batch(vec![10, 10], vec![Some(1.0), Some(2.0)], vec![Some(0.0); 2]);
    let result = stream(&batch, 10, 11, 1, 2, 0).manipulate(batch).unwrap();
    let times = result
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let values = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(times.values(), &[10, 11]);
    assert_eq!(values.values(), &[1.0, 2.0]);

    let batch = input_batch(vec![10, 20], vec![Some(1.0), Some(2.0)], vec![Some(0.0); 2]);
    let axis = RawTimeAxis::try_new(ScalarBuffer::from(vec![10, 20])).unwrap();
    let invalid_fields =
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![Some(1.0); 2])));
    let typed_error = select_instants(&axis, &invalid_fields, 10..=20, 10, -1, 0)
        .unwrap_err()
        .to_string();
    let error = stream(&batch, 10, 20, 10, -1, 0)
        .manipulate(batch)
        .unwrap_err()
        .to_string();
    assert_eq!(error, typed_error);
    assert!(error.contains("lookback must be non-negative"));
}
