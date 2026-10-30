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

//! A serialized native PromQL prefix executed on a datanode over its own region scan.

use std::sync::Arc;

use api::v1::region::{QueryRequest, RegionRequestHeader};
use common_recordbatch::RecordBatches;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::SampleRef;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use futures::{StreamExt, TryStreamExt};
use promql::extension_plan::{FusedOutputFormat, FusedSeries, FusedSeriesBinding};
use promql::native::plan::{
    BoundInput, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use query::options::QueryOptions;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use session::context::QueryContext;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::execute_sql;

/// The logical frame schema a `FusedOutputFormat::SeriesFrame` node declares: the shared evaluated
/// packer's layout for the tree's evaluated root, which `try_new_with_format` compares against.
fn evaluated_frame_schema(tree: &NativeTree) -> datafusion::common::DFSchemaRef {
    let NativeType::Samples(samples) = &tree.root().output else {
        panic!("the prefix root evaluates to samples");
    };
    let stream = common_query::series_frame::pack_evaluated_series(
        Box::pin(futures::stream::empty()),
        Arc::clone(samples),
    )
    .expect("an evaluated root declares a frame");
    Arc::new(
        datafusion::common::DFSchema::try_from(stream.schema())
            .expect("the frame schema converts to a logical schema"),
    )
}

// This deliberately uses a one-position grid: every host has one data chunk plus one empty close
// chunk. It checks decoded content but does NOT exercise interleaving of multi-chunk chains.
#[tokio::test(flavor = "multi_thread")]
async fn native_prefix_multi_partition_single_position_chains() {
    let instance = GreptimeDbStandaloneBuilder::new("native_prefix_partitioned_chains")
        .with_query_options(QueryOptions {
            parallelism: 16,
            ..Default::default()
        })
        .with_auto_create_table(false)
        .build()
        .await;
    let fe = instance.fe_instance();
    execute_sql(fe, "CREATE TABLE native_prefix_partitioned (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING, PRIMARY KEY(host))").await;
    let mut values = String::new();
    for host in 0..16 {
        for sample in 0..20_000 {
            let timestamp = sample as u64 * 1_000;
            let value = sample as f64;
            values.push_str(&format!("('host{host}', {value}, {timestamp}),"));
        }
    }
    values.pop();
    execute_sql(
        fe,
        &format!("INSERT INTO native_prefix_partitioned(host, val, ts) VALUES {values}"),
    )
    .await;
    execute_sql(fe, "ADMIN FLUSH_TABLE('native_prefix_partitioned')").await;

    let ctx = QueryContext::arc();
    let statement =
        QueryLanguageParser::parse_sql("SELECT * FROM native_prefix_partitioned", &ctx).unwrap();
    let planned = fe
        .query_engine()
        .planner()
        .plan(&statement, ctx.clone())
        .await
        .unwrap();
    let LogicalPlan::Projection(projection) = planned else {
        panic!("expected projection");
    };
    let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
        panic!("expected table scan");
    };
    let scan = scan.clone();
    let child = LogicalPlan::TableScan(datafusion::logical_expr::logical_plan::TableScan {
        projection: Some((0..scan.projected_schema.fields().len()).collect()),
        ..scan
    });
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(20_000, 1_000, 1).unwrap()).unwrap());
    let raw = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("val"),
        }]),
        eval_grid: None,
    });
    let evaluated = Arc::new(SeriesSchema {
        fields: Arc::clone(&raw.fields),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let raw_type = NativeType::Samples(raw);
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: raw_type.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw_type,
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 10_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: grid.clone(),
            },
        },
        output: NativeType::Samples(evaluated),
    })
    .unwrap();
    let binding = FusedSeriesBinding {
        grid,
        metric_name: Some(Arc::from("native_prefix_partitioned")),
        time_index: "ts".into(),
        tags: vec!["host".into()],
    };
    let frame_schema = evaluated_frame_schema(&tree);
    let fused = FusedSeries::try_new_with_format(
        Arc::from("rate(native_prefix_partitioned[10s])"),
        tree,
        binding,
        vec![child],
        frame_schema,
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(fused),
    });
    let bytes = DFLogicalSubstraitConvertor
        .encode(&plan, DefaultSerializer)
        .unwrap()
        .to_vec();
    let region = instance
        .mito_engine
        .regions()
        .into_iter()
        .find(|region| {
            region
                .metadata()
                .column_metadatas
                .iter()
                .any(|column| column.column_schema.name == "host")
        })
        .unwrap();
    let metadata = region.metadata();
    let mut stream = instance
        .datanode
        .region_server()
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some((&*ctx).into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: bytes,
            },
            ctx,
        )
        .await
        .unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    let schema = batches.first().expect("evaluated frames").schema.clone();
    let batches = RecordBatches::try_new(schema, batches).unwrap();
    let series = common_query::series_frame::unpack_evaluated_series(
        Box::pin(DfRecordBatchStreamAdapter::new(batches.as_stream())),
        None,
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(
        series.len(),
        32,
        "one data chunk and one close chunk per host"
    );
    for host in 0..16 {
        let expected_label = format!("host{host}");
        let matching = series
            .iter()
            .filter(|(series, _)| {
                series
                    .metric
                    .get("host")
                    .map(ToString::to_string)
                    .as_deref()
                    == Some(expected_label.as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 2, "two chunks for {expected_label}");
        let (data, data_closed) = matching
            .iter()
            .find(|(series, _)| !series.time.is_empty())
            .unwrap();
        assert!(!data_closed, "data chunk must not close {expected_label}");
        assert_eq!(data.time.len(), 1, "one sample for {expected_label}");
        let timestamps = data.time.timestamps().unwrap();
        assert_eq!(timestamps.as_ref(), &[20_000]);
        assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
        let SampleRef::Float(value) = data.fields.get(0).unwrap().sample(0) else {
            panic!("the rate field is float for {expected_label}");
        };
        assert!(
            (value - 1.0).abs() < 1e-12,
            "unexpected value for {expected_label}: {value}"
        );
        let (close, is_closed) = matching
            .iter()
            .find(|(series, _)| series.time.is_empty())
            .unwrap();
        assert!(*is_closed, "close chunk must close {expected_label}");
        assert_eq!(close.time.len(), 0);
    }
}

// Unlike the one-position case, this 8,192-position grid should produce multiple evaluated data
// chunks per host. It tests whether multi-chunk chains survive the 16-partition region exit.
#[tokio::test(flavor = "multi_thread")]
async fn native_prefix_multi_partition_multi_chunk_chains() {
    let instance = GreptimeDbStandaloneBuilder::new("native_prefix_partitioned_multi_chunk_chains")
        .with_query_options(QueryOptions {
            parallelism: 16,
            ..Default::default()
        })
        .with_auto_create_table(false)
        .build()
        .await;
    let fe = instance.fe_instance();
    execute_sql(fe, "CREATE TABLE native_prefix_partitioned (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING, PRIMARY KEY(host))").await;
    let mut values = String::new();
    for host in 0..16 {
        for sample in 0..20_000 {
            let timestamp = sample as u64 * 1_000;
            let value = sample as f64;
            values.push_str(&format!("('host{host}', {value}, {timestamp}),"));
        }
    }
    values.pop();
    execute_sql(
        fe,
        &format!("INSERT INTO native_prefix_partitioned(host, val, ts) VALUES {values}"),
    )
    .await;
    execute_sql(fe, "ADMIN FLUSH_TABLE('native_prefix_partitioned')").await;

    let ctx = QueryContext::arc();
    let statement =
        QueryLanguageParser::parse_sql("SELECT * FROM native_prefix_partitioned", &ctx).unwrap();
    let planned = fe
        .query_engine()
        .planner()
        .plan(&statement, ctx.clone())
        .await
        .unwrap();
    let LogicalPlan::Projection(projection) = planned else {
        panic!("expected projection");
    };
    let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
        panic!("expected table scan");
    };
    let scan = scan.clone();
    let child = LogicalPlan::TableScan(datafusion::logical_expr::logical_plan::TableScan {
        projection: Some((0..scan.projected_schema.fields().len()).collect()),
        ..scan
    });
    let grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(10_000, 1_000, 8_192).unwrap()).unwrap());
    let raw = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("val"),
        }]),
        eval_grid: None,
    });
    let evaluated = Arc::new(SeriesSchema {
        fields: Arc::clone(&raw.fields),
        eval_grid: Some(Arc::clone(&grid)),
    });
    let raw_type = NativeType::Samples(raw);
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: raw_type.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw_type,
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 10_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: grid.clone(),
            },
        },
        output: NativeType::Samples(evaluated),
    })
    .unwrap();
    let binding = FusedSeriesBinding {
        grid,
        metric_name: Some(Arc::from("native_prefix_partitioned")),
        time_index: "ts".into(),
        tags: vec!["host".into()],
    };
    let frame_schema = evaluated_frame_schema(&tree);
    let fused = FusedSeries::try_new_with_format(
        Arc::from("rate(native_prefix_partitioned[10s])"),
        tree,
        binding,
        vec![child],
        frame_schema,
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(fused),
    });
    let bytes = DFLogicalSubstraitConvertor
        .encode(&plan, DefaultSerializer)
        .unwrap()
        .to_vec();
    let region = instance
        .mito_engine
        .regions()
        .into_iter()
        .find(|region| {
            region
                .metadata()
                .column_metadatas
                .iter()
                .any(|column| column.column_schema.name == "host")
        })
        .unwrap();
    let metadata = region.metadata();
    let mut stream = instance
        .datanode
        .region_server()
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some((&*ctx).into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: bytes,
            },
            ctx,
        )
        .await
        .unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    let schema = batches.first().expect("evaluated frames").schema.clone();
    let batches = RecordBatches::try_new(schema, batches).unwrap();
    let series = common_query::series_frame::unpack_evaluated_series(
        Box::pin(DfRecordBatchStreamAdapter::new(batches.as_stream())),
        None,
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    for host in 0..16 {
        let expected_label = format!("host{host}");
        let matching = series
            .iter()
            .filter(|(series, _)| {
                series
                    .metric
                    .get("host")
                    .map(ToString::to_string)
                    .as_deref()
                    == Some(expected_label.as_str())
            })
            .collect::<Vec<_>>();
        let data_chunks = matching
            .iter()
            .filter(|(series, _)| !series.time.is_empty())
            .collect::<Vec<_>>();
        assert!(
            data_chunks.len() >= 2,
            "multiple data chunks for {expected_label}"
        );
        let mut timestamps = Vec::new();
        let mut samples = 0;
        for (data, is_closed) in &data_chunks {
            assert!(!is_closed, "data chunk must not close {expected_label}");
            let chunk_timestamps = data.time.timestamps().unwrap();
            assert!(chunk_timestamps.windows(2).all(|pair| pair[0] < pair[1]));
            timestamps.extend_from_slice(chunk_timestamps.as_ref());
            samples += data.time.len();
            for index in 0..data.time.len() {
                let SampleRef::Float(value) = data.fields.get(0).unwrap().sample(index) else {
                    panic!("the rate field is float for {expected_label}");
                };
                assert!(
                    (value - 1.0).abs() < 1e-12,
                    "unexpected value for {expected_label}: {value}"
                );
            }
        }
        assert_eq!(samples, 8_192, "sample count for {expected_label}");
        let expected_timestamps = (0..8_192).map(|i| 10_000 + i * 1_000).collect::<Vec<_>>();
        assert_eq!(
            timestamps, expected_timestamps,
            "contiguous grid for {expected_label}"
        );
        let close_chunks = matching
            .iter()
            .filter(|(series, _)| series.time.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(
            close_chunks.len(),
            1,
            "one close chunk for {expected_label}"
        );
        assert!(close_chunks[0].1, "close chunk must close {expected_label}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_prefix_executes_over_real_mito_region() {
    let instance = GreptimeDbStandaloneBuilder::new("native_prefix_region_scan")
        .with_auto_create_table(false)
        .build()
        .await;
    let fe = instance.fe_instance();
    execute_sql(fe, "CREATE TABLE native_prefix (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING, PRIMARY KEY(host))").await;
    execute_sql(fe, "INSERT INTO native_prefix(host, val, ts) VALUES ('a', 0.0, 1000), ('a', 10.0, 2000), ('a', 20.0, 3000), ('a', 30.0, 4000)").await;
    execute_sql(fe, "ADMIN FLUSH_TABLE('native_prefix')").await;

    let ctx = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql("SELECT * FROM native_prefix", &ctx).unwrap();
    let planned = fe
        .query_engine()
        .planner()
        .plan(&statement, ctx.clone())
        .await
        .unwrap();
    let datafusion::logical_expr::LogicalPlan::Projection(projection) = planned else {
        panic!("expected projection: {planned:?}");
    };
    let datafusion::logical_expr::LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
        panic!("expected table scan: {projection:?}");
    };
    let scan = scan.clone();
    let child = LogicalPlan::TableScan(datafusion::logical_expr::logical_plan::TableScan {
        projection: Some((0..scan.projected_schema.fields().len()).collect()),
        ..scan
    });
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(3000, 1000, 2).unwrap()).unwrap());
    let raw = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("val"),
        }]),
        eval_grid: None,
    });
    let evaluated = Arc::new(SeriesSchema {
        fields: Arc::clone(&raw.fields),
        eval_grid: Some(Arc::clone(&grid)),
    });
    // The input node's output must be the same binding as its `BoundInput.output`: the tree
    // compares those two by allocation identity, so an equal-but-separate schema is refused.
    let raw_type = NativeType::Samples(raw);
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: raw_type.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw_type,
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 3000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: Arc::clone(&grid),
            },
        },
        output: NativeType::Samples(evaluated),
    })
    .unwrap();
    let binding = FusedSeriesBinding {
        grid,
        metric_name: Some(Arc::from("native_prefix")),
        time_index: "ts".into(),
        tags: vec!["host".into()],
    };
    // A frame-output node declares the frame layout of its evaluated root, not the projection of
    // the scan below it: `try_new_with_format` compares the two and refuses a mismatch.
    let frame_schema = evaluated_frame_schema(&tree);
    let fused = FusedSeries::try_new_with_format(
        Arc::from("rate(native_prefix[3s])"),
        tree,
        binding,
        vec![child],
        frame_schema,
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap();
    let fused_payload = fused.serialize().unwrap();
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(fused),
    });
    let bytes = DFLogicalSubstraitConvertor
        .encode(&plan, DefaultSerializer)
        .unwrap()
        .to_vec();
    let region = instance
        .mito_engine
        .regions()
        .into_iter()
        .find(|r| {
            r.metadata()
                .column_metadatas
                .iter()
                .any(|c| c.column_schema.name == "host")
        })
        .unwrap();
    let metadata = region.metadata();
    let server = instance.datanode.region_server();
    let mut stream = server
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some((&*ctx).into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: bytes.clone(),
            },
            ctx.clone(),
        )
        .await
        .unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    let schema = batches.first().expect("evaluated frames").schema.clone();
    let batches = RecordBatches::try_new(schema, batches).unwrap();
    let series = common_query::series_frame::unpack_evaluated_series(
        Box::pin(DfRecordBatchStreamAdapter::new(batches.as_stream())),
        None,
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(series.len(), 1);
    let (series, _) = &series[0];
    // `rate` drops `__name__`, as Prometheus does for every range function except
    // `last_over_time` and `first_over_time`; the series' own tags survive.
    assert_eq!(series.metric.name(), None);
    assert_eq!(
        series
            .metric
            .get("host")
            .map(ToString::to_string)
            .as_deref(),
        Some("a")
    );
    assert_eq!(series.time.timestamps().unwrap().as_ref(), &[3000, 4000]);
    let values = series.fields.get(0).unwrap();
    // At 3000 ms the window holds 0, 10, 20: an increase of 20 over 2 observed
    // seconds. Prometheus would keep the 1 s leading gap (below the 1.1x average
    // threshold), but a counter whose first sample is 0 cannot be extrapolated
    // backwards at all: `duration_to_zero = sampled * first/value = 0` clamps it,
    // so the rate is 20 * (2 + 0) / 2 / 3 = 20/3.
    //
    // At 4000 ms the window holds 10, 20, 30: also 20 over 2 s, but now
    // `duration_to_zero = 2 * (10/20) = 1 s` does not shorten the 1 s leading gap,
    // so the rate is 20 * (2 + 1 + 0) / 2 / 3 = 10.
    let expected = [20.0 / 3.0, 10.0];
    for (index, expected) in expected.into_iter().enumerate() {
        let SampleRef::Float(value) = values.sample(index) else {
            panic!("the rate field is float at position {index}");
        };
        assert!(
            (value - expected).abs() < 1e-12,
            "value at position {index} is {value}, not {expected}"
        );
    }
    let mut tampered = bytes;
    // The tree envelope begins with a fixed magic marker and framed tree-length field. Mutate the
    // native plan version after that envelope prefix rather than the Substrait framing.
    let embedded = tampered
        .windows(fused_payload.len())
        .position(|window| window == fused_payload.as_slice())
        .expect("FusedSeries payload in Substrait plan");
    let prefix_version = embedded + 12;
    assert_eq!(
        &tampered[prefix_version..prefix_version + 4],
        &2u32.to_le_bytes()
    );
    tampered[prefix_version] = 99;
    let error = server
        .handle_remote_read(
            QueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some((&*ctx).into()),
                    ..Default::default()
                }),
                region_id: metadata.region_id.as_u64(),
                plan: tampered,
            },
            ctx,
        )
        .await
        .err()
        .expect("invalid prefix rejected");
    assert!(
        format!("{error:?}").contains("unknown native tree version"),
        "{error:?}"
    );
}
