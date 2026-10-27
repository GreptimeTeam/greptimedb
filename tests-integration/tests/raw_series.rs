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

use api::v1::region::{QueryRequest, RegionRequestHeader};
use common_query::request::{RawSeriesRead, RawSeriesReadColumn};
use common_query::series_frame::unpack_raw_series;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::arrow::array::{
    Array, DictionaryArray, Float64Array, StringArray, StringViewArray, TimestampMillisecondArray,
};
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::SampleRef;
use futures::StreamExt;
use promql::source::materialize_raw_labels;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use session::context::QueryContext;
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::execute_sql;

#[tokio::test(flavor = "multi_thread")]
async fn raw_series_read_scans_real_mito_region() {
    raw_series_read_case(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_series_read_scans_real_mito_region_without_primary_key() {
    raw_series_read_case(true).await;
}

async fn raw_series_read_case(tagless: bool) {
    let instance = GreptimeDbStandaloneBuilder::new(if tagless {
        "raw_series_mito_tagless_scan"
    } else {
        "raw_series_mito_scan"
    })
    .with_auto_create_table(false)
    .build()
    .await;
    let fe = instance.fe_instance();
    if tagless {
        execute_sql(
            fe,
            "CREATE TABLE raw_scan (ts TIMESTAMP TIME INDEX, \"value\" DOUBLE)",
        )
        .await;
        execute_sql(
            fe,
            "INSERT INTO raw_scan (\"value\", ts) VALUES (1.0, 1000), (2.0, 2000)",
        )
        .await;
    } else {
        execute_sql(fe, "CREATE TABLE raw_scan (ts TIMESTAMP TIME INDEX, \"value\" DOUBLE, host STRING, PRIMARY KEY(host))").await;
        execute_sql(fe, "INSERT INTO raw_scan(host, \"value\", ts) VALUES ('a', 1.0, 1000), ('a', 2.0, 2000), ('b', 3.0, 1000), ('b', 4.0, 2000)").await;
    }

    let region =
        instance
            .mito_engine
            .regions()
            .into_iter()
            .find(|region| {
                region.metadata().column_metadatas.iter().any(|column| {
                    column.column_schema.name == if tagless { "value" } else { "host" }
                })
            })
            .expect("created Mito table region");
    let metadata = region.metadata();
    if tagless {
        assert!(
            metadata.primary_key.is_empty(),
            "tagless fixture has no primary key"
        );
    }
    let col = |name: &str| {
        let column = metadata
            .column_metadatas
            .iter()
            .find(|column| column.column_schema.name == name)
            .unwrap();
        RawSeriesReadColumn {
            column_id: column.column_id,
            name: name.to_string(),
            data_type: column.column_schema.data_type.clone(),
            nullable: column.column_schema.is_nullable(),
        }
    };
    let read = RawSeriesRead {
        version: 1,
        region_id: metadata.region_id,
        metric_name: "raw_scan".to_string(),
        primary_key: metadata
            .primary_key
            .iter()
            .map(|id| {
                let column = metadata.column_by_id(*id).unwrap();
                RawSeriesReadColumn {
                    column_id: *id,
                    name: column.column_schema.name.clone(),
                    data_type: column.column_schema.data_type.clone(),
                    nullable: column.column_schema.is_nullable(),
                }
            })
            .collect(),
        time_index: col("ts"),
        tags: if tagless { vec![] } else { vec![col("host")] },
        fields: vec![col("value")],
    };
    let ctx = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql("SELECT * FROM raw_scan", &ctx).unwrap();
    let planned = fe
        .query_engine()
        .planner()
        .plan(&statement, ctx.clone())
        .await
        .unwrap();
    let datafusion::logical_expr::LogicalPlan::Projection(projection) = planned else {
        panic!("raw scan fixture must plan through a projection: {planned:?}");
    };
    let datafusion::logical_expr::LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
        panic!("raw scan fixture projection must read directly from a table scan: {projection:?}");
    };
    let scan = scan.clone();
    assert!(
        scan.filters.is_empty(),
        "raw scan fixture has no pushed filters"
    );
    let plan = datafusion::logical_expr::LogicalPlan::TableScan(
        datafusion::logical_expr::logical_plan::TableScan {
            projection: Some((0..scan.projected_schema.fields().len()).collect()),
            ..scan
        },
    );
    assert!(matches!(
        plan,
        datafusion::logical_expr::LogicalPlan::TableScan(_)
    ));
    let plan = DFLogicalSubstraitConvertor
        .encode(&plan, DefaultSerializer)
        .unwrap();
    let plan_bytes = plan.to_vec();
    let mut raw_ctx = ctx.as_ref().clone();
    raw_ctx.set_extension(RawSeriesRead::EXTENSION_KEY, read.encode().unwrap());
    let server = instance.region_server();
    let mut stream = server
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some((&raw_ctx).into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: plan_bytes.clone(),
            },
            std::sync::Arc::new(raw_ctx.clone()),
        )
        .await
        .unwrap();
    let mut frames = Vec::new();
    while let Some(batch) = stream.next().await {
        frames.push(batch.unwrap());
    }
    let frame_schema = frames
        .first()
        .expect("raw-series frames")
        .df_record_batch()
        .schema();
    assert_eq!(
        common_query::series_frame::series_frame_ordering(frame_schema.as_ref()).unwrap(),
        SeriesOrdering::KeyThenTime,
        "the raw frame must declare its key-then-time guarantee"
    );
    let frame_schema = frames.first().expect("raw-series frames").schema.clone();
    let frames = RecordBatches::try_new(frame_schema, frames).unwrap();
    let raw = unpack_raw_series(
        Box::pin(DfRecordBatchStreamAdapter::new(frames.as_stream())),
        None,
    )
    .unwrap();
    let mut decoded = materialize_raw_labels(raw).unwrap();
    let mut observations = Vec::new();
    while let Some(chunk) = decoded.next().await {
        let chunk = chunk.unwrap();
        let labels = chunk
            .metric
            .expect("materialization decodes primary-key labels");
        assert_eq!(labels.name(), Some("raw_scan"));
        let host = labels.get("host").map(ToString::to_string);
        if tagless {
            assert!(host.is_none(), "tagless series has no host label");
        } else {
            assert!(host.is_some(), "tagged series has a host label");
        }
        let values = chunk.fields.get(0).unwrap();
        for row in 0..chunk.time.len() {
            let value = match values.sample(row) {
                SampleRef::Float(value) => value,
                _ => panic!("unexpected non-float raw sample"),
            };
            observations.push((chunk.time.timestamp(row).unwrap(), value));
        }
    }
    assert_eq!(
        observations,
        if tagless {
            vec![(1000, 1.0), (2000, 2.0)]
        } else {
            vec![(1000, 1.0), (2000, 2.0), (1000, 3.0), (2000, 4.0)]
        }
    );

    let ordinary_ctx = QueryContext::arc();
    let ordinary_sql = if tagless {
        "SELECT ts, \"value\" FROM raw_scan"
    } else {
        "SELECT host, ts, \"value\" FROM raw_scan"
    };
    let statement = QueryLanguageParser::parse_sql(ordinary_sql, &ordinary_ctx).unwrap();
    let plan = fe
        .query_engine()
        .planner()
        .plan(&statement, ordinary_ctx.clone())
        .await
        .unwrap();
    let plan = DFLogicalSubstraitConvertor
        .encode(&plan, DefaultSerializer)
        .unwrap();
    let ordinary_plan_bytes = plan.to_vec();
    let mut ordinary_stream = server
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some(ordinary_ctx.as_ref().into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: ordinary_plan_bytes.clone(),
            },
            ordinary_ctx.clone(),
        )
        .await
        .unwrap();
    let mut ordinary_rows = Vec::new();
    while let Some(batch) = ordinary_stream.next().await {
        ordinary_rows.extend(ordinary_rows_from_batch(&batch.unwrap()));
    }
    ordinary_rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    assert_eq!(
        ordinary_rows,
        if tagless {
            vec![(None, 1000, 1.0), (None, 2000, 2.0)]
        } else {
            vec![
                (Some("a".to_string()), 1000, 1.0),
                (Some("a".to_string()), 2000, 2.0),
                (Some("b".to_string()), 1000, 3.0),
                (Some("b".to_string()), 2000, 4.0),
            ]
        },
        "ordinary reads remain available without raw-series control"
    );

    let mut wrong = read;
    wrong.region_id = RegionId::new(
        metadata.region_id.table_id(),
        metadata.region_id.region_number() + 10,
    );
    let mut bad_ctx = ctx.as_ref().clone();
    bad_ctx.set_extension(RawSeriesRead::EXTENSION_KEY, wrong.encode().unwrap());
    let error = server
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some((&bad_ctx).into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: plan_bytes.clone(),
            },
            std::sync::Arc::new(bad_ctx.clone()),
        )
        .await
        .err()
        .expect("wrong raw-series identity must be rejected");
    assert!(
        format!("{error:?}").contains("raw-series control region mismatch"),
        "{error:?}"
    );
}

fn ordinary_rows_from_batch(batch: &RecordBatch) -> Vec<(Option<String>, i64, f64)> {
    let schema = batch.df_record_batch().schema();
    let host_index = schema.index_of("host").ok();
    let ts_index = schema.index_of("ts").unwrap();
    let value_index = schema.index_of("value").unwrap();
    let hosts = host_index.map(|index| batch.column(index));
    let timestamps = batch
        .column(ts_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let values = batch
        .column(value_index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            let host = match hosts {
                None => None,
                Some(hosts) => Some(match hosts.data_type() {
                    DataType::Utf8 => hosts
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                    DataType::Utf8View => hosts
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                    DataType::Dictionary(key_type, value_type)
                        if key_type.as_ref() == &DataType::UInt32
                            && value_type.as_ref() == &DataType::Utf8 =>
                    {
                        let dictionary = hosts
                            .as_any()
                            .downcast_ref::<DictionaryArray<UInt32Type>>()
                            .unwrap();
                        let values = dictionary
                            .values()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        values.value(dictionary.key(row).unwrap()).to_string()
                    }
                    other => panic!("unexpected host type {other}"),
                }),
            };
            (host, timestamps.value(row), values.value(row))
        })
        .collect()
}
