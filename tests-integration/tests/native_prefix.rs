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

use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use api::v1::helper::row;
use api::v1::region::{QueryRequest, RegionRequestHeader, build_index_request};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Rows, SemanticType, WriteHint};
use common_error::ext::BoxedError;
use common_query::series_frame::unpack_raw_series;
use common_recordbatch::RecordBatches;
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr};
use datatypes::data_type::ConcreteDataType;
use datatypes::series::SeriesLabels;
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use futures::{Stream, StreamExt, TryStreamExt};
use mito2::config::{MitoConfig, SeriesIndexFormat};
use mito2::engine::MitoEngine;
use mito2::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use mito2::test_util::{CreateRequestBuilder, TestEnv, flush_region};
use promql::extension_plan::{
    FusedOutputFormat, FusedSeries, FusedSeriesBinding, SeriesRepartitionExec, SeriesRouting,
};
use promql::native::plan::{
    BoundInput, BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use promql::source::materialize_raw_labels;
use query::options::QueryOptions;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use session::context::QueryContext;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::region_engine::{
    BoundTag, InitializedRawSeriesScan, PrepareRequest, QueryScanContext, RawLabelSource,
    RawSeriesKeyRange, RawSeriesStream, RegionEngine, RegionScanner, RegionScannerRef,
    ScannerProperties, SendableRawSeriesStream,
};
use store_api::region_request::{
    RegionBuildIndexRequest, RegionCreateRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::consts::{ReservedColumnId, is_internal_column};
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::scan::RegionScanExec;
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

// Sends `rate[10s]` on a one-point evaluation grid for 16 hosts over 16 region-scan
// partitions to the datanode's remote-read entry point, then checks that the evaluated frames
// decode and each host reports the rate its samples produce. The former decode failure no longer
// reproduces because #10023 makes region package completion close its pending final raw chunk,
// so a chain's value and close are no longer latched apart in separate batches by the
// batch-interleaving `CoalescePartitionsExec`. That coalescing is still not chain-atomic: a chain
// spanning batches can still be cut. This one-point grid gives each host one evaluated value plus
// a possible empty closing chunk, so it does not exercise a chain spanning many batches. The
// parallelism-one path is exercised by `native_prefix_executes_over_real_mito_region`.
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
    // Package completion closes each region's pending final raw chunk, so the evaluated frames
    // decode; ordinary partition coalescing still does not keep chains atomic. Check that every
    // host's chain closes exactly once and carries the rate its own samples produce.
    let decoded = common_query::series_frame::unpack_evaluated_series(
        Box::pin(DfRecordBatchStreamAdapter::new(batches.as_stream())),
        None,
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .expect("a chain-atomic merge decodes every chain");

    let mut closed = std::collections::BTreeMap::<String, (usize, Vec<f64>)>::new();
    for (series, is_last) in decoded {
        let host = series
            .metric
            .get("host")
            .unwrap_or_else(|| panic!("a decoded series keeps its host label: {:?}", series.metric))
            .to_string();
        let entry = closed.entry(host).or_default();
        if is_last {
            entry.0 += 1;
        }
        for field in series.fields.iter() {
            entry
                .1
                .extend(field.iter().filter_map(|sample| match sample {
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Histogram(_) | SampleRef::Missing => None,
                }));
        }
    }
    assert_eq!(16, closed.len(), "one chain per host: {closed:?}");
    for (host, (closes, values)) in &closed {
        assert_eq!(1, *closes, "`{host}` closes its chain exactly once");
        assert_eq!(
            vec![1.0],
            *values,
            "`{host}` reports the rate of its own samples, which a cut chain would lose"
        );
    }
}

// The one-position case above cannot exercise a chain that spans chunks. This 8,192-position grid
// across 16 region-scan partitions does: whatever chunk boundaries the merge produces, each host's
// chain must arrive complete, in time order, and close exactly once.
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
    let decoded = common_query::series_frame::unpack_evaluated_series(
        Box::pin(DfRecordBatchStreamAdapter::new(batches.as_stream())),
        None,
    )
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let mut per_host = std::collections::BTreeMap::<String, (usize, Vec<i64>, Vec<f64>)>::new();
    for (series, is_last) in decoded {
        let host = series
            .metric
            .get("host")
            .unwrap_or_else(|| panic!("a decoded series keeps its host label: {:?}", series.metric))
            .to_string();
        let entry = per_host.entry(host).or_default();
        if is_last {
            entry.0 += 1;
        }
        if !series.time.is_empty() {
            entry
                .1
                .extend_from_slice(series.time.timestamps().unwrap().as_ref());
        }
        for field in series.fields.iter() {
            entry
                .2
                .extend(field.iter().filter_map(|sample| match sample {
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Histogram(_) | SampleRef::Missing => None,
                }));
        }
    }
    assert_eq!(16, per_host.len(), "one chain per host: {per_host:?}");
    let expected_timestamps = (0..8_192).map(|i| 10_000 + i * 1_000).collect::<Vec<_>>();
    for (host, (closes, timestamps, values)) in &per_host {
        assert_eq!(1, *closes, "`{host}` closes its chain exactly once");
        assert_eq!(
            8_192,
            timestamps.len(),
            "`{host}` keeps every grid position across chunk boundaries"
        );
        assert_eq!(
            &expected_timestamps, timestamps,
            "`{host}` arrives in order over its whole contiguous grid"
        );
        assert_eq!(8_192, values.len(), "`{host}` keeps every value");
        for value in values {
            assert!(
                (value - 1.0).abs() < 1e-12,
                "`{host}` reports the rate of its own samples, which a cut chain would lose: {value}"
            );
        }
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
    // Locate the complete FusedSeries envelope in the Substrait bytes. Its version is the first
    // word and its payload length is the second; no payload-field offsets are needed here.
    let envelope_at = tampered
        .windows(fused_payload.len())
        .position(|window| window == fused_payload.as_slice())
        .expect("FusedSeries envelope in Substrait plan");
    let version_bytes: [u8; 4] = tampered[envelope_at..envelope_at + 4]
        .try_into()
        .unwrap_or_else(|_| panic!("missing version bytes at {envelope_at}"));
    let version = u32::from_le_bytes(version_bytes);
    assert_eq!(version, 1, "observed version bytes: {version_bytes:?}");
    let length_bytes: [u8; 4] = tampered[envelope_at + 4..envelope_at + 8]
        .try_into()
        .unwrap_or_else(|_| panic!("missing length bytes at {}", envelope_at + 4));
    let payload_length = u32::from_le_bytes(length_bytes);
    assert_eq!(
        payload_length as usize,
        fused_payload.len() - 8,
        "observed length bytes: {length_bytes:?}"
    );
    tampered[envelope_at] = 99;
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
        format!("{error:?}").contains("unsupported FusedSeries envelope version"),
        "{error:?}"
    );
}

// ---- Real artifacts under the real router: a partition's candidate range crosses the plan ----
//
// The static-range cases in `promql::extension_plan::series_repartition` use a scanner double. These
// two use the region's own columnar series and sample artifacts: a sparse-encoded default mito
// region whose series index is built, read as a prepared two-partition raw scan, wrapped in the same
// `RegionScanExec`/`SeriesScanExec` leaf a planned query builds, and consumed by the real
// `SeriesRepartitionExec`. Only the delegate around the scanner and the wrapper around one
// partition's stream are local: they hold a payload and record what each partition did, so a test
// can state what the route published while the higher partition had handed over nothing.

/// The one Float64 field the fixture's artifacts carry.
fn artifact_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("field_0"),
        }]),
        eval_grid: None,
    })
}

/// The samples the fixture writes as `(tsid, timestamp, value)`. Two series, so the prepared source
/// splits its two ordinals between the scan's two partitions.
const ARTIFACT_SAMPLES: [(u64, i64, f64); 5] = [
    (0, 1_000, 1.0),
    (0, 2_000, 2.0),
    (0, 3_000, 3.0),
    (1, 1_000, 4.0),
    (1, 2_000, 5.0),
];

/// A per-series region scan: the distribution a raw series route is planned under.
fn raw_scan_request() -> ScanRequest {
    ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    }
}

/// The create request a sparse region with this metadata takes.
///
/// The options are the ones the prepared columnar fixture uses: a bulk memtable and flat SSTs, so a
/// flushed SST really carries the series and sample artifacts a prepared read needs, and compaction
/// windows wide enough that the artifacts under a running route are not rewritten.
fn create_request(metadata: &RegionMetadata) -> RegionCreateRequest {
    let mut request = CreateRequestBuilder::new().build();
    request.column_metadatas = metadata.column_metadatas.clone();
    request.primary_key = metadata.primary_key.clone();
    request
        .options
        .insert("primary_key_encoding".to_string(), "sparse".to_string());
    request
        .options
        .insert("memtable.type".to_string(), "bulk".to_string());
    request
        .options
        .insert("sst_format".to_string(), "flat".to_string());
    request
        .options
        .insert("compaction.type".to_string(), "twcs".to_string());
    request.options.insert(
        "compaction.twcs.time_window".to_string(),
        "100s".to_string(),
    );
    for window in ["active_window", "inactive_window"] {
        request.options.insert(
            format!("compaction.twcs.{window}.trigger_file_num"),
            "100".to_string(),
        );
    }
    request
}

/// Writes one sample of `tsid` through the sparse primary key the region stores.
async fn put_sample(
    engine: &MitoEngine,
    region_id: RegionId,
    metadata: &RegionMetadataRef,
    tsid: u64,
    timestamp: i64,
    value: f64,
) {
    let rows = Rows {
        schema: vec![
            ColumnSchema {
                column_name: "__primary_key".into(),
                datatype: ColumnDataType::Binary as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "ts".into(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "field_0".into(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ],
        rows: vec![row(vec![
            ValueData::BinaryValue(new_sparse_primary_key(&["a", "x"], metadata, 10, tsid)),
            ValueData::TimestampMillisecondValue(timestamp),
            ValueData::F64Value(value),
        ])],
    };
    let response = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows,
                skip_wal: false,
                hint: Some(WriteHint {
                    primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
                }),
                partition_expr_version: None,
            }),
        )
        .await
        .expect("the sample is written");
    assert_eq!(1, response.affected_rows);
}

/// A real sparse-encoded region with two series, flushed to SSTs whose columnar series index is
/// built: the smallest genuine artifact set a prepared raw scan reads.
async fn artifact_region() -> (TestEnv, MitoEngine, RegionId) {
    let mut env = TestEnv::with_prefix("native-prefix-artifact-range").await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_enable_series_index: true,
            experimental_series_index_format: SeriesIndexFormat::Columnar,
            experimental_series_index_maintenance_interval: Duration::from_secs(3600),
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    metadata
        .column_metadatas
        .iter_mut()
        .find(|column| column.column_id == 2)
        .expect("the fixture metadata carries one field")
        .column_schema
        .data_type = ConcreteDataType::float64_datatype();
    engine
        .handle_request(region_id, RegionRequest::Create(create_request(&metadata)))
        .await
        .expect("the sparse region is created");
    // The region's own metadata, not the template above: the sparse key codec encodes through the
    // layout the region actually stores.
    let stored = engine
        .get_metadata(region_id)
        .await
        .expect("the created region reports its metadata");
    for (tsid, timestamp, value) in ARTIFACT_SAMPLES {
        put_sample(&engine, region_id, &stored, tsid, timestamp, value).await;
    }
    flush_region(&engine, region_id, None).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::BuildIndex(RegionBuildIndexRequest {
                options: Some(build_index_request::Options::SeriesIndex(Default::default())),
            }),
        )
        .await
        .expect("the flushed SSTs take a columnar series index");
    (env, engine, region_id)
}

/// The scan-bound identity a planned raw series read declares for this region: its own metadata
/// snapshot - the pointer the scanner prepared under - and its user tag columns in primary key
/// order, with the reserved key columns left out.
fn artifact_binding(metadata: &RegionMetadataRef) -> RawLabelSource {
    RawLabelSource::PrimaryKey {
        metric_name: Arc::from("native_prefix_artifact"),
        physical_metadata: Arc::clone(metadata),
        logical_tags: metadata
            .column_metadatas
            .iter()
            .filter(|column| {
                column.semantic_type == SemanticType::Tag
                    && metadata.primary_key.contains(&column.column_id)
                    && !ReservedColumnId::is_reserved(column.column_id)
                    && !is_internal_column(&column.column_schema.name)
            })
            .map(|column| BoundTag {
                column_id: column.column_id,
                name: Arc::from(column.column_schema.name.as_str()),
            })
            .collect::<Vec<_>>()
            .into(),
    }
}

/// Every sample of one chunk: its timestamp and its single Float64 field value.
fn chunk_rows(chunk: &RawSeriesChunk) -> Vec<(i64, f64)> {
    let field = chunk
        .fields
        .get(0)
        .expect("the artifact schema has one field");
    (0..chunk.time.len())
        .map(|index| {
            let timestamp = chunk
                .time
                .timestamp(index)
                .expect("a raw chunk carries its timestamps");
            let SampleRef::Float(value) = field.sample(index) else {
                panic!("the fixture's field is Float64");
            };
            (timestamp, value)
        })
        .collect()
}

/// One series as a route publishes it: its complete key, its explicit labels, and every sample in
/// order.
///
/// How many frame batches the packer split one identity into is not part of this: the batch limits,
/// not the route, decide that. What is part of it is how often the identity was closed - one chain,
/// closed at its end - so a comparison covers the chunks and their finality, not only the rows.
#[derive(Clone, Debug, PartialEq)]
struct SeriesView {
    key: Vec<u8>,
    labels: Option<SeriesLabels>,
    rows: Vec<(i64, f64)>,
    closes: usize,
}

/// Groups a raw stream into per-series views, checking that each series arrives as one contiguous
/// run whose last chunk closes it exactly once.
fn chain_series(chunks: &[RawSeriesChunk]) -> Vec<SeriesView> {
    let mut views: Vec<SeriesView> = Vec::new();
    for chunk in chunks {
        if views
            .last()
            .is_none_or(|view| view.key != chunk.primary_key.as_slice())
        {
            if let Some(previous) = views.last() {
                assert_eq!(1, previous.closes, "a series is closed exactly once");
            }
            views.push(SeriesView {
                key: chunk.primary_key.to_vec(),
                labels: chunk.metric.clone(),
                rows: Vec::new(),
                closes: 0,
            });
        }
        let view = views.last_mut().expect("a view was just pushed");
        assert_eq!(
            view.labels, chunk.metric,
            "the pieces of one identity agree on their labels"
        );
        assert_eq!(0, view.closes, "a closed series does not resume");
        view.closes += usize::from(chunk.is_last);
        view.rows.extend(chunk_rows(chunk));
    }
    if let Some(previous) = views.last() {
        assert_eq!(1, previous.closes, "a series is closed exactly once");
    }
    views
}

/// The artifacts' own content: every series the prepared reader returns, in key order and with its
/// labels materialized, read through one partition that owns every candidate.
async fn artifact_series(engine: &MitoEngine, region_id: RegionId) -> Vec<RawSeriesChunk> {
    let mut scanner = engine
        .handle_query(region_id, raw_scan_request())
        .await
        .expect("the region answers a per-series scan");
    let binding = artifact_binding(&scanner.metadata());
    let ranges = scanner
        .properties()
        .partitions
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    assert!(
        !ranges.is_empty(),
        "the built artifacts assign candidate ranges to the scan"
    );
    scanner
        .prepare(
            PrepareRequest::default()
                .with_ranges(vec![ranges])
                .with_target_partitions(1),
        )
        .expect("the pinned ranges are accepted");
    let initialization = scanner
        .initialize_series_partition(
            &QueryScanContext::default(),
            &ExecutionPlanMetricsSet::new(),
            0,
            &binding,
        )
        .expect("the built artifacts give the region a prepared reader");
    let initialized = initialization
        .await
        .expect("the prepared initialization resolves");
    let stream =
        materialize_raw_labels(initialized.stream).expect("key chunks materialize their labels");
    stream
        .try_collect::<Vec<_>>()
        .await
        .expect("the prepared reader reads every candidate")
}

/// A switch a test opens to let gated streams produce.
///
/// Opening it wakes the streams already parked, and a stream polled after the opening sees it
/// straight away: registering before the second read is what makes an opening that lands between the
/// two reads impossible to lose.
#[derive(Default)]
struct Switch {
    open: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl Switch {
    fn open(&self) {
        self.open.store(true, AtomicOrdering::Release);
        let wakers = std::mem::take(
            &mut *self
                .wakers
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );
        for waker in wakers {
            waker.wake();
        }
    }

    fn is_open(&self) -> bool {
        self.open.load(AtomicOrdering::Acquire)
    }

    /// Parks `cx` until the switch is opened, unless it already is.
    fn wait(&self, cx: &mut std::task::Context<'_>) {
        if self.is_open() {
            return;
        }
        self.wakers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(cx.waker().clone());
        if self.is_open() {
            // The opening landed between the two reads, so its wake did not find this waker.
            cx.waker().wake_by_ref();
        }
    }
}

/// How one partition's payload is held back, so a test shows that a read happened when it did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hold {
    /// Hand the payload over as soon as it is polled.
    None,
    /// Park on the first poll without polling the source: no chunk and no EOF.
    BeforePayload,
    /// Hand over the chain including its closing chunk, then park every later poll.
    AfterFinal,
}

/// What the test can see of the partitions behind the delegate, and what it holds.
///
/// Payload polling is the observable a negative control has to wait for: a partition's stream is
/// built and polled only after its initialization resolved and any range it states was published, so
/// a parked payload says the input has done everything it can before the assertion.
struct Gate {
    holds: Vec<Hold>,
    switches: Vec<Arc<Switch>>,
    polls: Vec<AtomicUsize>,
    chunks: Vec<AtomicUsize>,
    post_final: Vec<AtomicUsize>,
    /// The genuine candidate range each partition's initialization reported, recorded before this
    /// wrapper decides whether to keep it.
    ranges: Mutex<Vec<Option<RawSeriesKeyRange>>>,
    /// Whether each partition keeps the range its own initialization reported.
    keep_range: bool,
}

impl Gate {
    fn new(holds: Vec<Hold>, keep_range: bool) -> Self {
        let partitions = holds.len();
        Self {
            holds,
            switches: (0..partitions)
                .map(|_| Arc::new(Switch::default()))
                .collect(),
            polls: (0..partitions).map(|_| AtomicUsize::new(0)).collect(),
            chunks: (0..partitions).map(|_| AtomicUsize::new(0)).collect(),
            post_final: (0..partitions).map(|_| AtomicUsize::new(0)).collect(),
            ranges: Mutex::new(vec![None; partitions]),
            keep_range,
        }
    }

    fn switch(&self, partition: usize) -> Arc<Switch> {
        Arc::clone(&self.switches[partition])
    }

    fn record_poll(&self, partition: usize) {
        self.polls[partition].fetch_add(1, AtomicOrdering::SeqCst);
    }

    fn record_post_final(&self, partition: usize) {
        self.post_final[partition].fetch_add(1, AtomicOrdering::SeqCst);
    }

    fn record_chunk(&self, partition: usize) {
        self.chunks[partition].fetch_add(1, AtomicOrdering::SeqCst);
    }

    /// Whether this partition's payload has been polled at all.
    fn polled(&self, partition: usize) -> bool {
        self.polls[partition].load(AtomicOrdering::SeqCst) > 0
    }

    /// Whether this partition handed over any chunk.
    fn handed_over(&self, partition: usize) -> usize {
        self.chunks[partition].load(AtomicOrdering::SeqCst)
    }

    /// How many polls this partition's gate answered after its closing chunk.
    fn post_final_polls(&self, partition: usize) -> usize {
        self.post_final[partition].load(AtomicOrdering::SeqCst)
    }

    fn record_range(&self, partition: usize, range: &Option<RawSeriesKeyRange>) {
        self.ranges
            .lock()
            .unwrap_or_else(|error| error.into_inner())[partition] = range.clone();
    }

    fn range(&self, partition: usize) -> Option<RawSeriesKeyRange> {
        self.ranges
            .lock()
            .unwrap_or_else(|error| error.into_inner())[partition]
            .clone()
    }

    /// Wraps `stream` when this partition's payload is held.
    fn hold_stream(
        self: &Arc<Self>,
        partition: usize,
        stream: SendableRawSeriesStream,
    ) -> SendableRawSeriesStream {
        match self.holds[partition] {
            Hold::None => stream,
            hold => Box::pin(GatedStream {
                inner: stream,
                partition,
                hold,
                switch: self.switch(partition),
                gate: Arc::clone(self),
                closed: false,
            }),
        }
    }
}

/// One partition's stream with its payload held as the test asks, recording what it saw.
struct GatedStream {
    inner: SendableRawSeriesStream,
    partition: usize,
    hold: Hold,
    switch: Arc<Switch>,
    gate: Arc<Gate>,
    /// Whether this stream already handed over the chunk that closes its series.
    closed: bool,
}

impl Stream for GatedStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.gate.record_poll(this.partition);
        match this.hold {
            Hold::None => {}
            Hold::BeforePayload => {
                if !this.switch.is_open() {
                    this.switch.wait(cx);
                    return Poll::Pending;
                }
            }
            Hold::AfterFinal => {
                if this.closed && !this.switch.is_open() {
                    this.gate.record_post_final(this.partition);
                    this.switch.wait(cx);
                    return Poll::Pending;
                }
            }
        }
        let polled = this.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(chunk))) = &polled {
            this.gate.record_chunk(this.partition);
            this.closed |= chunk.is_last;
        }
        polled
    }
}

impl RawSeriesStream for GatedStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        self.inner.schema()
    }

    fn label_source(&self) -> &RawLabelSource {
        self.inner.label_source()
    }

    fn label_decode_spec(&self) -> Option<&datatypes::series::frame::LabelDecodeSpec> {
        self.inner.label_decode_spec()
    }

    fn ordering(&self) -> SeriesOrdering {
        self.inner.ordering()
    }
}

/// The region's own scanner with two changes: its initialized raw partition is recorded - and, for
/// the negative control, stripped of the range it selected - and the stream it hands out may be
/// held. Every other appointment is the region's.
struct GatedScanner {
    inner: RegionScannerRef,
    gate: Arc<Gate>,
}

impl fmt::Debug for GatedScanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatedScanner").finish_non_exhaustive()
    }
}

impl DisplayAs for GatedScanner {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl RegionScanner for GatedScanner {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &ScannerProperties {
        self.inner.properties()
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.inner.schema()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.inner.metadata()
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.inner.prepare(request)
    }

    fn scan_partition(
        &self,
        ctx: &QueryScanContext,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        self.inner.scan_partition(ctx, metrics, partition)
    }

    fn raw_series_ordering(&self) -> Result<SeriesOrdering, BoxedError> {
        self.inner.raw_series_ordering()
    }

    fn raw_series_schema(
        &self,
        label_source: &RawLabelSource,
    ) -> Result<Option<Arc<SeriesSchema>>, BoxedError> {
        self.inner.raw_series_schema(label_source)
    }

    fn raw_series_has_explicit_labels(
        &self,
        label_source: &RawLabelSource,
    ) -> Result<bool, BoxedError> {
        self.inner.raw_series_has_explicit_labels(label_source)
    }

    fn scan_series_partition(
        &self,
        ctx: &QueryScanContext,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        label_source: &RawLabelSource,
    ) -> Result<Option<SendableRawSeriesStream>, BoxedError> {
        self.inner
            .scan_series_partition(ctx, metrics, partition, label_source)
    }

    /// Initializes through the region's own prepared reader, records the range that reader selected,
    /// and only then decides what this route hands out.
    fn initialize_series_partition(
        &self,
        ctx: &QueryScanContext,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        label_source: &RawLabelSource,
    ) -> Option<futures::future::BoxFuture<'static, Result<InitializedRawSeriesScan, BoxedError>>>
    {
        let initialization =
            self.inner
                .initialize_series_partition(ctx, metrics, partition, label_source)?;
        let gate = Arc::clone(&self.gate);
        Some(Box::pin(async move {
            let initialized = initialization.await?;
            gate.record_range(partition, &initialized.key_range);
            let stream = gate.hold_stream(partition, initialized.stream);
            let key_range = if gate.keep_range {
                initialized.key_range
            } else {
                None
            };
            Ok(InitializedRawSeriesScan { stream, key_range })
        }))
    }

    fn prepare_series(
        &self,
        ctx: &QueryScanContext,
        metrics: &ExecutionPlanMetricsSet,
        label_source: &RawLabelSource,
    ) -> futures::future::BoxFuture<
        'static,
        Result<Option<Arc<dyn store_api::region_engine::PreparedSeriesSource>>, BoxedError>,
    > {
        self.inner.prepare_series(ctx, metrics, label_source)
    }

    fn has_predicate_without_region(&self) -> bool {
        self.inner.has_predicate_without_region()
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        self.inner.add_dyn_filter_to_predicate(filter_exprs)
    }

    fn reset_state(&mut self) {
        self.inner.reset_state()
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.inner.set_logical_region(logical_region)
    }

    fn set_query_load_region_id(&mut self, region_id: RegionId) {
        self.inner.set_query_load_region_id(region_id)
    }

    fn snapshot_sequence(&self) -> Option<store_api::storage::SequenceNumber> {
        self.inner.snapshot_sequence()
    }
}

/// One planned route over the region's real artifacts.
struct Route {
    plan: Arc<SeriesRepartitionExec>,
    gate: Arc<Gate>,
}

/// Builds the route a planned raw series read builds: the region's own scanner behind the delegate,
/// selected as the native raw leaf with two prepared partitions, and read under one repartition
/// output.
///
/// Both partitions are assigned the same candidate ranges, so the prepared source splits its
/// ordinals between them: the lower half - the smaller complete keys, because the source sorts its
/// candidates by key - to partition 0 and the upper half to partition 1.
async fn artifact_route(
    engine: &MitoEngine,
    region_id: RegionId,
    holds: Vec<Hold>,
    keep_range: bool,
) -> Route {
    let gate = Arc::new(Gate::new(holds, keep_range));
    let scanner = engine
        .handle_query(region_id, raw_scan_request())
        .await
        .expect("the region answers a per-series scan");
    let binding = artifact_binding(&scanner.metadata());
    let scan = RegionScanExec::new(
        Box::new(GatedScanner {
            inner: scanner,
            gate: Arc::clone(&gate),
        }),
        raw_scan_request(),
        None,
    )
    .expect("the raw region scan builds");
    let ranges = scan.get_partition_ranges();
    let scan = scan
        .with_new_partitions(vec![ranges.clone(), ranges], 2)
        .expect("the prepared partition ranges are accepted");
    let leaf = Arc::new(scan)
        .try_to_series_scan(binding)
        .expect("the raw leaf is selected")
        .expect("the built artifacts support a prepared raw scan");
    let plan = Arc::new(
        SeriesRepartitionExec::try_new_with_routing(leaf, 1, SeriesRouting::RawPrimaryKey)
            .expect("the router is built over the raw leaf"),
    );
    Route { plan, gate }
}

/// A task context whose memory pool is `pool`, so what the read charges can be read afterwards.
fn task_context(pool: &Arc<GreedyMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// Starts one route's single output read.
fn start_route(
    plan: &Arc<SeriesRepartitionExec>,
) -> (SendableRawSeriesStream, Arc<GreedyMemoryPool>) {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let frames = plan
        .execute(0, task_context(&pool))
        .expect("the one output executes");
    let output = unpack_raw_series(frames, Some(artifact_schema()))
        .expect("the routed frames decode as raw series");
    (output, pool)
}

/// The complete output of one route that holds nothing back.
async fn complete_route(
    engine: &MitoEngine,
    region_id: RegionId,
    keep_range: bool,
) -> Vec<RawSeriesChunk> {
    let Route { plan, .. } =
        artifact_route(engine, region_id, vec![Hold::None, Hold::None], keep_range).await;
    let (output, _pool) = start_route(&plan);
    tokio::time::timeout(Duration::from_secs(10), output.try_collect::<Vec<_>>())
        .await
        .expect("the route completes")
        .expect("the route reads the artifacts")
}

/// Waits until `done` holds, so a test states a condition rather than a duration.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut spins = 0usize;
        while !done() {
            spins += 1;
            if spins.is_multiple_of(64) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// Whether `output` has nothing to hand over right now.
async fn poll_pending(output: &mut SendableRawSeriesStream) -> bool {
    futures::future::poll_fn(|cx| match output.as_mut().poll_next(cx) {
        Poll::Pending => Poll::Ready(true),
        Poll::Ready(_) => Poll::Ready(false),
    })
    .await
}

/// The genuine candidate range of a prepared partition makes the identity below it publishable while
/// the higher partition has produced neither a chunk nor an EOF.
#[tokio::test(flavor = "multi_thread")]
async fn real_artifact_range_publishes_the_lower_identity_before_the_held_payload() {
    let (_env, engine, region_id) = artifact_region().await;
    let expected = chain_series(&artifact_series(&engine, region_id).await);
    assert_eq!(
        2,
        expected.len(),
        "the fixture's two series are the artifacts' candidates"
    );

    let Route { plan, gate } = artifact_route(
        &engine,
        region_id,
        vec![Hold::None, Hold::BeforePayload],
        true,
    )
    .await;
    let (mut output, _pool) = start_route(&plan);
    // The higher partition resolves its own initialization - so the range its slice selected is
    // already published - and parks on its first poll.
    wait_until("the held partition parks on its first poll", || {
        gate.polled(1)
    })
    .await;
    assert!(
        !gate.switch(1).is_open(),
        "the held partition is released only by this test"
    );
    assert_eq!(
        0,
        gate.handed_over(1),
        "the held partition has handed over no chunk, and its EOF cannot have happened either"
    );

    let first = tokio::time::timeout(Duration::from_secs(10), output.next())
        .await
        .expect("the lower identity is published while the higher partition is silent")
        .expect("one chunk is published")
        .expect("the published chunk succeeds");

    let low = gate
        .range(0)
        .expect("the lower partition's prepared slice covers a candidate");
    let high = gate
        .range(1)
        .expect("the higher partition's prepared slice covers a candidate");
    assert!(
        low.max.as_slice() < high.min.as_slice(),
        "the lower slice's largest key is below the higher slice's smallest key"
    );
    assert_eq!(
        0,
        gate.handed_over(1),
        "the published identity cannot be the held partition's"
    );
    let expected_low = expected
        .iter()
        .find(|view| view.key.as_slice() == low.max.as_slice())
        .expect("the lower slice owns one candidate");
    assert_eq!(
        vec![expected_low.clone()],
        chain_series(std::slice::from_ref(&first)),
        "the early publication is that identity's own key, labels, samples and close"
    );

    gate.switch(1).open();
    let rest = tokio::time::timeout(Duration::from_secs(10), output.try_collect::<Vec<_>>())
        .await
        .expect("the released partition finishes the read")
        .expect("the rest of the read succeeds");
    let mut published = vec![first];
    published.extend(rest);
    assert_eq!(
        expected,
        chain_series(&published),
        "the route publishes the artifacts' own content in key order, each identity closed once"
    );

    // The same artifacts under a fresh route that states no range: the evidence decides only when an
    // identity may be published, never what it contains.
    let control = chain_series(&complete_route(&engine, region_id, false).await);
    assert_eq!(
        expected, control,
        "dropping every range changes nothing about the published content"
    );
    engine.stop().await.expect("the engine stops");
}

/// Without the range the same artifacts, route and ordering wait for the silent partition: the lower
/// identity is complete and charged to the read, and it is still not published.
#[tokio::test(flavor = "multi_thread")]
async fn real_artifact_route_without_a_range_waits_for_the_held_payload() {
    let (_env, engine, region_id) = artifact_region().await;
    let Route { plan, gate } = artifact_route(
        &engine,
        region_id,
        vec![Hold::AfterFinal, Hold::BeforePayload],
        false,
    )
    .await;
    let (mut output, pool) = start_route(&plan);
    // The lower chain is complete and charged into the read, and its input asked for the next one; the
    // higher partition resolved its initialization - it states no range - and parked. Only then does
    // waiting mean the barrier held, rather than that nothing had happened yet.
    wait_until("the lower chain is queued and asked for again", || {
        gate.post_final_polls(0) >= 2 && gate.handed_over(0) >= 1 && gate.polled(1)
    })
    .await;
    assert!(
        pool.reserved() > 0,
        "the queued chain is charged to the read"
    );
    assert_eq!(
        0,
        gate.handed_over(1),
        "the held partition has handed over no chunk, and its EOF cannot have happened either"
    );
    assert!(
        !gate.switch(1).is_open(),
        "the held partition is released only by this test"
    );
    assert!(
        poll_pending(&mut output).await,
        "an input that states no range keeps the original barrier"
    );

    gate.switch(0).open();
    gate.switch(1).open();
    let released = tokio::time::timeout(Duration::from_secs(10), output.try_collect::<Vec<_>>())
        .await
        .expect("the released inputs finish the read")
        .expect("the read succeeds");
    let positive = chain_series(&complete_route(&engine, region_id, true).await);
    assert_eq!(
        positive,
        chain_series(&released),
        "the released route publishes what the ranged route publishes, in the same order"
    );
    engine.stop().await.expect("the engine stops");
}
