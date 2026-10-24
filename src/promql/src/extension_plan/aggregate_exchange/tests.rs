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

use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
use datatypes::series::samples::{FieldSpec, SampleRef, Samples, Series, SeriesFields};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

use super::*;
use crate::aggregate::FloatMerger;

/// One field's samples, as a grid position and the value at it.
type FieldPoints = Vec<(usize, f64)>;

fn series_schema(fields: &[&str], points: u64) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: (*name).into(),
            })
            .collect(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(-1000, 250, points).unwrap()).unwrap(),
        )),
    })
}

fn metric(name: Option<&str>, labels: &[(&str, &str)]) -> MetricName {
    MetricName::try_new(
        name.map(Arc::from),
        labels
            .iter()
            .map(|(name, value)| Label {
                name: (*name).into(),
                value: (*value).into(),
            })
            .collect(),
    )
    .unwrap()
}

/// One closed chunk of `metric` over `fields`, from grid position zero.
fn chunk(
    schema: &Arc<SeriesSchema>,
    metric: MetricName,
    fields: &[Vec<Option<f64>>],
) -> EvaluatedChunk {
    let rows = fields[0].len();
    let fields = fields
        .iter()
        .map(|values| Samples::from_float(Float64Array::from(values.clone())))
        .collect();
    let series = Series::try_new(
        schema,
        metric,
        TimeAxis::Grid(GridRange::try_new(schema.eval_grid.clone().unwrap(), 0, rows).unwrap()),
        SeriesFields::try_from_vec(fields).unwrap(),
    )
    .unwrap();
    EvaluatedChunk::try_new(series, true).unwrap()
}

fn bind(kind: AggregateKind, group: &[&str]) -> BoundAggregate {
    BoundAggregate {
        kind,
        group: BoundGrouping::By(group.iter().map(|label| Arc::from(*label)).collect()),
        stage: AggregateStage::Complete,
    }
}

fn tags(names: &[&str]) -> Arc<[String]> {
    names
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>()
        .into()
}

fn unbounded() -> Arc<dyn MemoryPool> {
    Arc::new(UnboundedMemoryPool::default())
}

fn fold(
    op: &BoundAggregate,
    schema: &Arc<SeriesSchema>,
    tags: &Arc<[String]>,
    pool: &Arc<dyn MemoryPool>,
) -> DfResult<PartialFold> {
    PartialFold::try_new(
        op.clone(),
        Arc::clone(schema),
        Arc::clone(tags),
        Arc::new(Mutex::new(None)),
        pool,
    )
}

/// The fold's own state, from the kernel's serial fold of the same input.
fn expected(
    op: AggregateOp,
    by: &[&str],
    schema: &Arc<SeriesSchema>,
    chunks: &[EvaluatedChunk],
) -> PartialAggregate {
    let mut aggregator = FloatAggregator::try_new(schema, op, Grouping::By(by)).unwrap();
    for chunk in chunks {
        aggregator.update(&chunk.series).unwrap();
    }
    aggregator.finish_partial()
}

/// Every state of `partial` as one row, sorted so that two sides compare whatever their order.
fn rows(partial: &PartialAggregate) -> Vec<(String, usize, usize, AccumulatorState)> {
    let mut rows: Vec<_> = partial
        .groups
        .iter()
        .flat_map(|group| {
            group.points.iter().flat_map(|(at, states)| {
                states.iter().enumerate().filter_map(|(field, state)| {
                    state.map(|state| (format!("{:?}", group.metric), *at, field, state))
                })
            })
        })
        .collect();
    rows.sort_unstable_by(|a, b| (&a.0, a.1, a.2).cmp(&(&b.0, b.1, b.2)));
    rows
}

fn decode(
    batch: &RecordBatch,
    schema: &Arc<SeriesSchema>,
    op: AggregateOp,
    tags: &[String],
) -> DfResult<PartialAggregate> {
    decode_state(batch, schema, op, tags, |_| Ok(()))
}

/// Each decoded batch's rows, for a roundtrip that spans several of them.
fn decoded_rows(
    batches: &[RecordBatch],
    schema: &Arc<SeriesSchema>,
    op: AggregateOp,
    tags: &[String],
) -> Vec<(String, usize, usize, AccumulatorState)> {
    batches
        .iter()
        .flat_map(|batch| rows(&decode(batch, schema, op, tags).unwrap()))
        .collect()
}

/// What the merge of `partials` answers, as its values per field per series.
fn merged(
    op: AggregateOp,
    schema: &Arc<SeriesSchema>,
    partials: Vec<PartialAggregate>,
) -> Vec<(String, Vec<FieldPoints>)> {
    let mut merger = FloatMerger::try_new(schema, op).unwrap();
    for partial in partials {
        merger.merge(partial).unwrap();
    }
    merger
        .finish()
        .unwrap()
        .into_iter()
        .map(|series| {
            let positions: Vec<usize> = series.time.positions().map(|(at, _)| at).collect();
            let fields = series
                .fields
                .iter()
                .map(|field| {
                    field
                        .iter()
                        .zip(&positions)
                        .filter_map(|(sample, at)| match sample {
                            SampleRef::Float(value) => Some((*at, value)),
                            _ => None,
                        })
                        .collect()
                })
                .collect();
            (format!("{:?}", series.metric), fields)
        })
        .collect()
}

fn state(value: f64) -> AccumulatorState {
    AccumulatorState {
        value,
        mean: 0.0,
        compensation: 0.0,
        count: 1.0,
        incremental_mean: false,
    }
}

/// One hand-written state row, for the batches a fold would not produce.
#[derive(Clone, Copy)]
struct Row {
    name: Option<&'static str>,
    /// One per ordinary tag, `None` for an absent label.
    labels: &'static [Option<&'static str>],
    point: u64,
    field: u32,
    /// Every state column is non-nullable, so a row of [`state_batch`] carries one: writing a null
    /// here would be a batch Arrow refuses, which the null-state test builds itself.
    state: Option<AccumulatorState>,
}

fn state_batch(tags: &[String], rows: &[Row]) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
        rows.iter().map(|row| row.name).collect::<Vec<_>>(),
    ))];
    for (position, _) in ordinary_tags(tags).enumerate() {
        columns.push(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.labels[position])
                .collect::<Vec<_>>(),
        )));
    }
    columns.push(Arc::new(UInt64Array::from(
        rows.iter().map(|row| row.point).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt32Array::from(
        rows.iter().map(|row| row.field).collect::<Vec<_>>(),
    )));
    let components: [Vec<Option<f64>>; 4] = std::array::from_fn(|index| {
        rows.iter()
            .map(|row| {
                row.state
                    .map(|state| [state.value, state.mean, state.compensation, state.count][index])
            })
            .collect()
    });
    columns.extend(
        components
            .into_iter()
            .map(|values| Arc::new(Float64Array::from(values)) as ArrayRef),
    );
    columns.push(Arc::new(BooleanArray::from(
        rows.iter()
            .map(|row| row.state.map(|state| state.incremental_mean))
            .collect::<Vec<_>>(),
    )));
    RecordBatch::try_new(state_schema(tags), columns).unwrap()
}

#[test]
fn state_schema_is_the_private_layout() {
    let schema = state_schema(&["job".to_string(), "__name__".into(), "instance".into()]);
    let fields: Vec<(&str, DataType, bool)> = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().as_str(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect();
    assert_eq!(
        fields,
        vec![
            ("metric_name", DataType::Utf8, true),
            ("tag_0", DataType::Utf8, true),
            ("tag_1", DataType::Utf8, true),
            ("point", DataType::UInt64, false),
            ("field", DataType::UInt32, false),
            ("value", DataType::Float64, false),
            ("mean", DataType::Float64, false),
            ("compensation", DataType::Float64, false),
            ("count", DataType::Float64, false),
            ("incremental_mean", DataType::Boolean, false),
        ]
    );
}

#[test]
fn aggregate_op_folds_complete_kinds_only() {
    for (kind, op) in [
        (AggregateKind::Sum, AggregateOp::Sum),
        (AggregateKind::Count, AggregateOp::Count),
        (AggregateKind::Avg, AggregateOp::Avg),
        (AggregateKind::Min, AggregateOp::Min),
        (AggregateKind::Max, AggregateOp::Max),
        (AggregateKind::Group, AggregateOp::Group),
        (AggregateKind::Stddev, AggregateOp::Stddev),
        (AggregateKind::Stdvar, AggregateOp::Stdvar),
    ] {
        assert_eq!(aggregate_op(&bind(kind, &["job"])).unwrap(), op);
    }
    for stage in [AggregateStage::Partial, AggregateStage::Final] {
        let op = BoundAggregate {
            kind: AggregateKind::Sum,
            group: bind(AggregateKind::Sum, &["job"]).group,
            stage,
        };
        assert!(matches!(
            aggregate_op(&op),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
    let quantile = bind(AggregateKind::Quantile { quantile: 0.5 }, &["job"]);
    assert!(matches!(
        aggregate_op(&quantile),
        Err(DataFusionError::NotImplemented(_))
    ));
}

#[test]
fn roundtrip_carries_every_component() {
    let schema = series_schema(&["a", "b"], 4);
    let tags = tags(&["job", "instance"]);
    for (kind, op) in [
        (AggregateKind::Sum, AggregateOp::Sum),
        (AggregateKind::Count, AggregateOp::Count),
        (AggregateKind::Avg, AggregateOp::Avg),
        (AggregateKind::Stddev, AggregateOp::Stddev),
        (AggregateKind::Stdvar, AggregateOp::Stdvar),
    ] {
        let chunks = vec![
            chunk(
                &schema,
                metric(None, &[("job", "x"), ("instance", "one")]),
                &[
                    vec![Some(1.0), None, Some(3.0), Some(4.0)],
                    vec![Some(10.0), Some(20.0), None, None],
                ],
            ),
            chunk(
                &schema,
                metric(None, &[("job", "x"), ("instance", "two")]),
                &[
                    vec![Some(5.0), None, None, Some(7.0)],
                    vec![None, None, Some(30.0), Some(40.0)],
                ],
            ),
        ];
        let pool = unbounded();
        let mut fold = fold(&bind(kind, &["job"]), &schema, &tags, &pool).unwrap();
        for chunk in &chunks {
            fold.update(chunk.clone()).unwrap();
        }
        let batches = fold.finish().unwrap();
        assert_eq!(
            decoded_rows(&batches, &schema, op, &tags),
            rows(&expected(op, &["job"], &schema, &chunks))
        );
    }
}

#[test]
fn roundtrip_keeps_the_name_and_absent_labels() {
    let schema = series_schema(&["a"], 2);
    // `__name__` is in the tag list and must be filtered out; `instance` never reaches a group.
    let tags = tags(&["instance", "__name__", "job"]);
    let chunks = [chunk(
        &schema,
        metric(Some("up"), &[("job", "x"), ("instance", "one")]),
        &[vec![Some(1.0), Some(2.0)]],
    )];
    let pool = unbounded();
    let mut fold = fold(
        &bind(AggregateKind::Sum, &["job", "__name__"]),
        &schema,
        &tags,
        &pool,
    )
    .unwrap();
    fold.update(chunks[0].clone()).unwrap();
    let batches = fold.finish().unwrap();
    assert_eq!(batches[0].num_columns(), 10);
    let decoded = decode(&batches[0], &schema, AggregateOp::Sum, &tags).unwrap();
    // One group per point, so both points are checked over every group rather than one of them.
    let mut points: Vec<usize> = decoded
        .groups
        .iter()
        .flat_map(|group| group.points.iter().map(|(at, _)| *at))
        .collect();
    points.sort_unstable();
    assert_eq!(points, vec![0, 1]);
    assert!(decoded.groups.iter().all(|group| {
        group.metric.name() == Some("up")
            && group.metric.get("job") == Some("x")
            && group.metric.get("instance").is_none()
    }));
}

#[test]
fn decoded_rows_keep_an_empty_label_beside_an_absent_one() {
    let schema = series_schema(&["a"], 2);
    let tags = tags(&["job", "instance"]);
    let batch = state_batch(
        &tags,
        &[Row {
            name: None,
            labels: &[Some(""), None],
            point: 1,
            field: 0,
            state: Some(state(4.0)),
        }],
    );
    let decoded = decode(&batch, &schema, AggregateOp::Sum, &tags).unwrap();
    let group = &decoded.groups[0];
    assert_eq!(group.metric.name(), None);
    assert_eq!(group.metric.get("job"), Some(""));
    assert_eq!(group.metric.get("instance"), None);
    assert_eq!(group.points, vec![(1, vec![Some(state(4.0))])]);
}

#[test]
fn group_state_roundtrips_where_a_value_exists() {
    let schema = series_schema(&["a"], 3);
    let tags = tags(&["job"]);
    let chunks = vec![
        chunk(
            &schema,
            metric(None, &[("job", "x")]),
            &[vec![Some(1.0), None, Some(1.0)]],
        ),
        chunk(
            &schema,
            metric(None, &[("job", "x")]),
            &[vec![None, Some(1.0), Some(1.0)]],
        ),
    ];
    let pool = unbounded();
    let mut fold = fold(&bind(AggregateKind::Group, &["job"]), &schema, &tags, &pool).unwrap();
    for chunk in &chunks {
        fold.update(chunk.clone()).unwrap();
    }
    let batches = fold.finish().unwrap();
    let decoded = decode(&batches[0], &schema, AggregateOp::Group, &tags).unwrap();
    assert_eq!(
        rows(&decoded),
        rows(&expected(AggregateOp::Group, &["job"], &schema, &chunks))
    );
    assert_eq!(rows(&decoded).len(), 3);
}

#[test]
fn a_sparse_field_leaves_the_other_field_unset() {
    let schema = series_schema(&["a", "b"], 3);
    let tags = tags(&["job"]);
    let chunks = [chunk(
        &schema,
        metric(None, &[("job", "x")]),
        &[
            vec![Some(1.0), None, Some(2.0)],
            vec![None, Some(5.0), None],
        ],
    )];
    let pool = unbounded();
    let mut fold = fold(&bind(AggregateKind::Sum, &["job"]), &schema, &tags, &pool).unwrap();
    fold.update(chunks[0].clone()).unwrap();
    let batches = fold.finish().unwrap();
    let decoded = decode(&batches[0], &schema, AggregateOp::Sum, &tags).unwrap();
    let points: Vec<(usize, usize)> = rows(&decoded)
        .into_iter()
        .map(|(_, at, field, _)| (at, field))
        .collect();
    assert_eq!(points, vec![(0, 0), (1, 1), (2, 0)]);
}

#[test]
fn a_point_is_assembled_from_its_fields() {
    let names: Vec<String> = (0..64).map(|index| format!("f{index}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let schema = series_schema(&names, 64);
    let tags = tags(&["job"]);
    let values: Vec<Option<f64>> = (0..64).map(|at| Some(at as f64)).collect();
    let chunks = vec![chunk(
        &schema,
        metric(None, &[("job", "x")]),
        &vec![values; 64],
    )];
    let pool = unbounded();
    let mut fold = fold(&bind(AggregateKind::Sum, &["job"]), &schema, &tags, &pool).unwrap();
    fold.update(chunks[0].clone()).unwrap();
    let batches = fold.finish().unwrap();
    let decoded = decode(&batches[0], &schema, AggregateOp::Sum, &tags).unwrap();
    // One group per point, each holding one field-wide state vector, rather than one per row.
    assert_eq!(decoded.groups.len(), 64);
    assert!(
        decoded
            .groups
            .iter()
            .all(|group| group.points.len() == 1 && group.points[0].1.len() == 64)
    );
    assert_eq!(rows(&decoded).len(), 64 * 64);
    assert_eq!(
        rows(&decoded),
        rows(&expected(AggregateOp::Sum, &["job"], &schema, &chunks))
    );
}

#[test]
fn one_group_spans_several_batches() {
    let points = BATCH_ROWS as u64 / 2 + 1;
    let schema = series_schema(&["a", "b"], points);
    let tags = tags(&["job"]);
    let values: Vec<Option<f64>> = (0..points).map(|at| Some(at as f64)).collect();
    let chunks = vec![chunk(
        &schema,
        metric(None, &[("job", "x")]),
        &[values.clone(), values],
    )];
    let pool = unbounded();
    let mut fold = fold(&bind(AggregateKind::Sum, &["job"]), &schema, &tags, &pool).unwrap();
    fold.update(chunks[0].clone()).unwrap();
    let batches = fold.finish().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), BATCH_ROWS);
    assert_eq!(batches[1].num_rows(), 2);
    assert_eq!(
        decoded_rows(&batches, &schema, AggregateOp::Sum, &tags),
        rows(&expected(AggregateOp::Sum, &["job"], &schema, &chunks))
    );
}

#[test]
fn a_point_split_across_batches_merges_back_together() {
    let schema = series_schema(&["a", "b"], 1);
    let tags = tags(&["job"]);
    let chunks = [chunk(
        &schema,
        metric(None, &[("job", "x")]),
        &[vec![Some(3.0)], vec![Some(4.0)]],
    )];
    let pool = unbounded();
    let mut fold = fold(&bind(AggregateKind::Sum, &["job"]), &schema, &tags, &pool).unwrap();
    fold.update(chunks[0].clone()).unwrap();
    let batch = fold.finish().unwrap().remove(0);
    assert_eq!(batch.num_rows(), 2);
    let partials = [batch.slice(0, 1), batch.slice(1, 1)]
        .iter()
        .map(|batch| decode(batch, &schema, AggregateOp::Sum, &tags).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(partials[0].groups.len(), 1);
    assert_eq!(partials[1].groups.len(), 1);
    // One group at one point again, carrying both fields the split batches held apart.
    let merged = merged(AggregateOp::Sum, &schema, partials);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].1, vec![vec![(0, 3.0)], vec![(0, 4.0)]]);
}

#[test]
fn finish_charges_the_export_and_releases_the_fold() {
    let schema = series_schema(&["a"], 2);
    let tags = tags(&["job"]);
    let budget = Arc::new(Mutex::new(None));
    let pool = unbounded();
    let mut fold = PartialFold::try_new(
        bind(AggregateKind::Sum, &["job"]),
        Arc::clone(&schema),
        Arc::clone(&tags),
        Arc::clone(&budget),
        &pool,
    )
    .unwrap();
    fold.update(chunk(
        &schema,
        metric(None, &[("job", "x")]),
        &[vec![Some(1.0), Some(2.0)]],
    ))
    .unwrap();
    assert!(budget.lock().unwrap().is_none());
    assert_eq!(fold.finish().unwrap().len(), 1);
    // The states are given back; what this fold still holds is its own tags and schema.
    assert_eq!(fold.reservation.size(), fold.held);
    assert!(budget.lock().unwrap().as_ref().unwrap().size() > 0);
    // A finished partition folds and finishes nothing more.
    assert!(matches!(fold.finish(), Err(DataFusionError::Execution(_))));
    assert!(matches!(
        fold.update(chunk(&schema, metric(None, &[]), &[vec![Some(1.0)]])),
        Err(DataFusionError::Execution(_))
    ));
}

#[test]
fn absent_labels_still_need_export_budget() {
    let names: Vec<String> = (0..8).map(|index| format!("t{index}")).collect();
    let schema = series_schema(&["a"], 2);
    let tags: Arc<[String]> = names.into();
    let tiny = Arc::new(GreedyMemoryPool::new(16)) as Arc<dyn MemoryPool>;
    let claimed = Arc::new(Mutex::new(Some(
        MemoryConsumer::new("test.export").register(&tiny),
    )));
    let mut fold = PartialFold::try_new(
        bind(AggregateKind::Sum, &["job"]),
        Arc::clone(&schema),
        Arc::clone(&tags),
        claimed,
        &unbounded(),
    )
    .unwrap();
    // No label is present, so only the rows themselves are charged -- and refused, by the pool
    // itself: the refusal is a resource error, not one of this module's own.
    fold.update(chunk(
        &schema,
        metric(None, &[("job", "x")]),
        &[vec![Some(1.0), Some(2.0)]],
    ))
    .unwrap();
    let error = fold
        .finish()
        .expect_err("16 bytes cannot hold the rows the fold exports");
    assert!(
        matches!(
            error,
            DataFusionError::ResourcesExhausted(ref reason)
                if reason.contains("Failed to allocate additional")
        ),
        "refused with another error: {error}"
    );
}

#[test]
fn a_tiny_pool_refuses_the_fold() {
    let schema = series_schema(&["a"], 2);
    let tags = tags(&["job"]);
    let pool = Arc::new(GreedyMemoryPool::new(1)) as Arc<dyn MemoryPool>;
    let result =
        fold(&bind(AggregateKind::Sum, &["job"]), &schema, &tags, &pool).and_then(|mut fold| {
            fold.update(chunk(
                &schema,
                metric(None, &[("job", "x")]),
                &[vec![Some(1.0), Some(2.0)]],
            ))
        });
    assert!(result.is_err());
}

#[test]
fn decode_refuses_a_foreign_layout() {
    let schema = series_schema(&["a"], 2);
    let tags = tags(&["job"]);
    let batch = state_batch(
        &tags,
        &[Row {
            name: None,
            labels: &[Some("x")],
            point: 0,
            field: 0,
            state: Some(state(1.0)),
        }],
    );
    for (from, to) in [("point", "grid_point"), ("value", "sum")] {
        let fields: Vec<Arc<Field>> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                if field.name() == from {
                    Arc::new(Field::new(
                        to,
                        field.data_type().clone(),
                        field.is_nullable(),
                    ))
                } else {
                    Arc::clone(field)
                }
            })
            .collect();
        let foreign =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();
        assert!(matches!(
            decode(&foreign, &schema, AggregateOp::Sum, &tags),
            Err(DataFusionError::Execution(_))
        ));
    }
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from(vec![0])));
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("extra", DataType::UInt64, false)));
    let extra = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(matches!(
        decode(&extra, &schema, AggregateOp::Sum, &tags),
        Err(DataFusionError::Execution(_))
    ));
}

/// A null state cannot reach the decoder: the layout's state columns are non-nullable, so Arrow
/// refuses such a batch, and a valid batch against a grid-less schema is what the decoder refuses.
#[test]
fn a_null_state_is_rejected_at_the_batch_boundary_and_ungridded_decode_is_refused() {
    let tags = tags(&["job"]);
    let batch = state_batch(
        &tags,
        &[Row {
            name: None,
            labels: &[Some("x")],
            point: 0,
            field: 0,
            state: Some(state(1.0)),
        }],
    );
    // The decoder's own null check is unreachable through a batch: a non-nullable state column
    // holding nulls cannot be built at all, so the boundary refuses it before decode is called.
    let mut columns = batch.columns().to_vec();
    let value = batch
        .schema()
        .index_of("value")
        .expect("the layout has a value column");
    columns[value] = Arc::new(Float64Array::from(vec![None]));
    let error = RecordBatch::try_new(Arc::clone(batch.schema_ref()), columns)
        .expect_err("a null in a non-nullable state column is not a batch");
    assert!(
        error.to_string().contains("non-nullable"),
        "refused for another reason: {error}"
    );
    let plain = Arc::new(SeriesSchema {
        fields: vec![FieldSpec { name: "a".into() }].into(),
        eval_grid: None,
    });
    let error = decode(&batch, &plain, AggregateOp::Sum, &tags)
        .expect_err("state is indexed on a grid the schema does not have");
    assert!(
        error.to_string().contains("grid"),
        "refused for another reason: {error}"
    );
}

#[test]
fn decode_refuses_a_point_a_field_or_a_field_twice() {
    let schema = series_schema(&["a"], 2);
    let tags = tags(&["job"]);
    let row = |point, field| Row {
        name: None,
        labels: &[Some("x")],
        point,
        field,
        state: Some(state(1.0)),
    };
    for row in [row(2, 0), row(0, 1)] {
        assert!(matches!(
            decode(
                &state_batch(&tags, &[row]),
                &schema,
                AggregateOp::Sum,
                &tags
            ),
            Err(DataFusionError::Execution(_))
        ));
    }
    // Two rows of one point naming one field: the producer contract's one defect.
    let repeated = decode(
        &state_batch(&tags, &[row(0, 0), row(0, 0)]),
        &schema,
        AggregateOp::Sum,
        &tags,
    );
    assert!(
        matches!(repeated, Err(DataFusionError::Execution(ref reason)) if reason.contains("repeats field"))
    );
}

/// Decode admits a figure that covers every container its answer keeps.
///
/// The decoded answer retains the group vector, each group's point vector and state vectors, and the
/// labels of each group. What was admitted has to reach that; a budget one byte below it must refuse
/// rather than answer with containers nothing paid for.
#[test]
fn decode_admits_the_groups_it_keeps() {
    let schema = series_schema(&["a", "b"], 2);
    let tags = tags(&["job", "instance"]);
    let batch = state_batch(
        &tags,
        &[
            Row {
                name: Some("up"),
                labels: &[Some("x"), Some("one")],
                point: 0,
                field: 0,
                state: Some(state(1.0)),
            },
            Row {
                name: Some("up"),
                labels: &[Some("x"), Some("one")],
                point: 1,
                field: 0,
                state: Some(state(2.0)),
            },
            Row {
                name: None,
                labels: &[None, Some("two")],
                point: 0,
                field: 1,
                state: Some(state(3.0)),
            },
        ],
    );
    let mut totals = Vec::new();
    let decoded = decode_state(&batch, &schema, AggregateOp::Sum, &tags, |total| {
        totals.push(total);
        Ok(())
    })
    .unwrap();
    // Every container the answer keeps, at the capacity it actually has.
    let retained = decoded.groups.capacity() * size_of::<PartialGroup>()
        + decoded
            .groups
            .iter()
            .map(|group| {
                group.points.capacity() * size_of::<(usize, Vec<Option<AccumulatorState>>)>()
                    + group
                        .points
                        .iter()
                        .map(|(_, states)| {
                            states.capacity() * size_of::<Option<AccumulatorState>>()
                        })
                        .sum::<usize>()
                    + group.metric.name().map_or(0, str::len)
                    + group
                        .metric
                        .tags()
                        .iter()
                        .map(|tag| {
                            size_of::<Label>()
                                + 2 * size_of::<usize>()
                                + tag.name.len()
                                + tag.value.len()
                        })
                        .sum::<usize>()
            })
            .sum::<usize>();
    let admitted = *totals.iter().max().expect("decode charged its workspace");
    assert!(
        admitted >= retained,
        "decode admitted {admitted} and kept {retained}"
    );
    let refused = decode_state(&batch, &schema, AggregateOp::Sum, &tags, |total| {
        if total >= retained {
            Err(DataFusionError::Execution("no budget".into()))
        } else {
            Ok(())
        }
    });
    assert!(
        refused.is_err(),
        "a budget below the {retained} bytes decode keeps answered anyway"
    );
}

#[test]
fn decode_admits_before_it_allocates() {
    let schema = series_schema(&["a", "b"], 2);
    let tags = tags(&["job"]);
    let batch = state_batch(
        &tags,
        &[Row {
            name: None,
            labels: &[Some("x")],
            point: 0,
            field: 1,
            state: Some(state(1.0)),
        }],
    );
    let mut totals = Vec::new();
    let decoded = decode_state(&batch, &schema, AggregateOp::Sum, &tags, |total| {
        totals.push(total);
        Ok(())
    })
    .unwrap();
    // The workspace, the group's state vector and its labels, and the figure never shrinks.
    assert!(totals.len() >= 3);
    assert!(totals.windows(2).all(|pair| pair[1] >= pair[0]));
    assert_eq!(decoded.groups.len(), 1);
    assert!(matches!(
        decode_state(&batch, &schema, AggregateOp::Sum, &tags, |_| Err(
            DataFusionError::Execution("no budget".into())
        )),
        Err(DataFusionError::Execution(_))
    ));
}
