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

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::IsNotNullExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::FutureExt;

use super::*;

fn label_at(array: &dyn Array, row: usize) -> Result<Option<&str>> {
    if let Some(values) = array.as_any().downcast_ref::<DictionaryArray<UInt16Type>>() {
        return values
            .key(row)
            .map(|key| string_at(values.values().as_ref(), key))
            .unwrap_or(Ok(None));
    }
    if let Some(values) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
        return values
            .key(row)
            .map(|key| string_at(values.values().as_ref(), key))
            .unwrap_or(Ok(None));
    }
    string_at(array, row)
}

fn context(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(64),
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .unwrap(),
    )
    .task_ctx()
}

fn plan(label_type: DataType, label_count: usize, null_values: bool) -> Arc<dyn ExecutionPlan> {
    // Keep the timestamp first in the schema but last in the output ordering.
    let mut fields = vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )];
    fields.extend(
        (0..label_count).map(|label| Field::new(format!("label{label}"), label_type.clone(), true)),
    );
    fields.push(Field::new("value", DataType::Float64, true));
    let schema = Arc::new(Schema::new(fields));
    let mut partitions = vec![Vec::new(); 4];
    for series in 0..48 {
        let mut columns: Vec<ArrayRef> = vec![Arc::new(
            TimestampMillisecondArray::from_iter_values((0..37).map(|slot| -1000 + slot * 1000)),
        )];
        for label in 0..label_count {
            let value = format!("scope-{label}-host-{}", series % 5);
            let labels =
                StringViewArray::from(vec![(series % 5 != 0).then_some(value.as_str()); 37]);
            columns.push(cast(&labels, &label_type).unwrap());
        }
        columns.push(Arc::new(Float64Array::from_iter((0..37).map(|slot| {
            if null_values || slot % 13 == 0 {
                None
            } else {
                Some(match slot % 13 {
                    1 => f64::NAN,
                    2 => f64::INFINITY,
                    3 => f64::NEG_INFINITY,
                    4 => -0.0,
                    _ => series as f64 * 0.125 + slot as f64 * 0.0625,
                })
            }
        }))));
        partitions[series % 4].push(
            RecordBatch::try_new(schema.clone(), columns)
                .unwrap()
                .slice(1, 36),
        );
    }
    let input = MemorySourceConfig::try_new_exec(&partitions, schema.clone(), None).unwrap();
    let filter = Arc::new(
        FilterExec::try_new(
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new(
                "value",
                label_count + 1,
            )))),
            input,
        )
        .unwrap(),
    );
    let groups = PhysicalGroupBy::new_single(
        (0..=label_count)
            .map(|index| {
                let name = schema.field(index).name();
                (
                    Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect(),
    );
    let aggregates = [avg_udaf(), sum_udaf()]
        .into_iter()
        .map(|udaf| {
            Arc::new(
                AggregateExprBuilder::new(
                    udaf.clone(),
                    vec![Arc::new(Column::new("value", label_count + 1))],
                )
                .schema(schema.clone())
                .alias(udaf.name())
                .build()
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let partial = AggregateExec::try_new(
        AggregateMode::Partial,
        groups.clone(),
        aggregates.clone(),
        vec![None, None],
        filter,
        schema.clone(),
    )
    .unwrap();
    let partial: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesAggregateExec::try_new(&partial, -1000, 35000, 1000)
            .unwrap()
            .unwrap(),
    );
    let repartition = Arc::new(
        RepartitionExec::try_new(partial, Partitioning::Hash(groups.input_exprs(), 4)).unwrap(),
    );
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            groups,
            aggregates,
            vec![None, None],
            repartition,
            schema.clone(),
        )
        .unwrap(),
    );
    let order = LexOrdering::new((1..=label_count).chain(std::iter::once(0)).map(|index| {
        PhysicalSortExpr {
            expr: Arc::new(Column::new(schema.field(index).name(), index)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }
    }))
    .unwrap();
    let sort = Arc::new(SortExec::new(order.clone(), aggregate).with_preserve_partitioning(true));
    Arc::new(SortPreservingMergeExec::new(order, sort))
}

/// (labels, timestamp, aggregate value bit patterns), in output order.
type OutputRow = (Vec<Option<String>>, i64, Vec<Option<u64>>);

fn rows(batches: &[RecordBatch], labels: usize) -> Vec<OutputRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                (1..=labels)
                    .map(|column| {
                        label_at(batch.column(column).as_ref(), row)
                            .unwrap()
                            .map(str::to_owned)
                    })
                    .collect(),
                times.value(row),
                batch.columns()[labels + 1..]
                    .iter()
                    .map(|array| {
                        let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        (!values.is_null(row)).then(|| {
                            if values.value(row).is_nan() {
                                f64::NAN.to_bits()
                            } else {
                                values.value(row).to_bits()
                            }
                        })
                    })
                    .collect(),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn complete_sorted_outputs_match_existing_final_chain() {
    for label_type in [
        DataType::Utf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View)),
    ] {
        for labels in [0, 1, 3, 4] {
            for all_null in [false, true] {
                let original = plan(label_type.clone(), labels, all_null);
                let candidate = Arc::new(
                    SeriesFinalAggregateExec::try_new(&original)
                        .unwrap()
                        .unwrap(),
                );
                SanityCheckPlan {}
                    .optimize(candidate.clone(), &Default::default())
                    .unwrap();
                let pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
                let context = context(pool.clone());
                let expected = collect(original.clone(), context.clone()).await.unwrap();
                let actual = collect(candidate.clone(), context.clone()).await.unwrap();
                assert_eq!(candidate.schema(), original.schema());
                assert_eq!(rows(&expected, labels), rows(&actual, labels));
                assert_eq!(pool.reserved(), 0);
                let rebuilt = candidate
                    .clone()
                    .with_new_children(vec![candidate.input.clone()])
                    .unwrap();
                assert!(rebuilt.as_any().is::<SeriesFinalAggregateExec>());
                assert_eq!(
                    rows(&actual, labels),
                    rows(&collect(rebuilt, context).await.unwrap(), labels)
                );
            }
        }
    }
}

#[tokio::test]
async fn unordered_final_preserves_all_labeled_samples() {
    for label_type in [
        DataType::Utf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View)),
    ] {
        for labels in [0, 1, 4] {
            let original = plan(label_type.clone(), labels, false);
            let aggregate = original.children()[0].children()[0].clone();
            let candidate = Arc::new(
                SeriesFinalAggregateExec::try_new_unordered(&aggregate)
                    .unwrap()
                    .unwrap(),
            );
            assert_eq!(candidate.schema(), aggregate.schema());
            assert_eq!(candidate.properties().partitioning.partition_count(), 1);
            let pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
            let context = context(pool.clone());
            let expected = collect(original, context.clone()).await.unwrap();
            let actual = collect(candidate.clone(), context.clone()).await.unwrap();
            assert_eq!(rows(&actual, labels), rows(&expected, labels));
            let rebuilt = candidate
                .clone()
                .with_new_children(vec![candidate.input.clone()])
                .unwrap();
            assert_eq!(
                rows(&collect(rebuilt, context).await.unwrap(), labels),
                rows(&expected, labels)
            );
            assert_eq!(pool.reserved(), 0);
        }
    }
}

#[derive(Debug)]
struct RefuseGrowth {
    pool: GreedyMemoryPool,
    calls: AtomicUsize,
    fail_on: usize,
    /// Refuse every growth from `fail_on` onwards, not just that one.
    refuse_rest: bool,
    consumer: &'static str,
}

impl MemoryPool for RefuseGrowth {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.pool.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.pool.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if reservation.consumer().name().starts_with(self.consumer)
            && match self.calls.fetch_add(1, AtomicOrdering::SeqCst) + 1 {
                call if self.refuse_rest => call >= self.fail_on,
                call => call == self.fail_on,
            }
        {
            return Err(datafusion::error::DataFusionError::ResourcesExhausted(
                "injected final aggregate back pressure".to_string(),
            ));
        }
        self.pool.try_grow(reservation, additional)
    }
    fn reserved(&self) -> usize {
        self.pool.reserved()
    }
}

#[tokio::test]
async fn back_pressure_preserves_states_and_unread_input() {
    let original = plan(DataType::Utf8View, 3, false);
    let candidate = Arc::new(
        SeriesFinalAggregateExec::try_new(&original)
            .unwrap()
            .unwrap(),
    );
    let expected = collect(
        original,
        context(Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024))),
    )
    .await
    .unwrap();
    for (consumer, fail_on) in [
        ("PromSeriesFinalAggregate", 1),
        ("PromSeriesFinalAggregate", 2),
        ("PromSeriesAggregate[", 1),
        ("PromSeriesAggregate[", 2),
    ] {
        let pool = Arc::new(RefuseGrowth {
            pool: GreedyMemoryPool::new(64 * 1024 * 1024),
            calls: AtomicUsize::new(0),
            fail_on,
            refuse_rest: false,
            consumer,
        });
        let actual = collect(candidate.clone(), context(pool.clone()))
            .await
            .unwrap();
        // Handing the state to the fallback reserves it, so the refused growth is not
        // the last one this consumer asks for.
        assert!(pool.calls.load(AtomicOrdering::SeqCst) > fail_on);
        assert_eq!(rows(&expected, 3), rows(&actual, 3));
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn fallback_refuses_to_carry_state_it_cannot_reserve() {
    let original = plan(DataType::Utf8View, 3, false);
    let candidate = SeriesFinalAggregateExec::try_new(&original)
        .unwrap()
        .unwrap();
    // Nothing on the final side can grow, so the state cannot be accounted for before
    // it moves into the fallback. Exceeding the budget silently is not an option.
    let pool = Arc::new(RefuseGrowth {
        pool: GreedyMemoryPool::new(64 * 1024 * 1024),
        calls: AtomicUsize::new(0),
        fail_on: 1,
        refuse_rest: true,
        consumer: "PromSeriesFinalAggregate",
    });
    let error = collect(Arc::new(candidate), context(pool.clone()))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    for _ in 0..100 {
        if pool.reserved() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn partial_task_failure_is_not_successful_eof() {
    let original = plan(DataType::Utf8View, 3, false);
    let candidate = Arc::new(
        SeriesFinalAggregateExec::try_new(&original)
            .unwrap()
            .unwrap(),
    );
    let pool = Arc::new(GreedyMemoryPool::new(1));
    let error = collect(candidate, context(pool.clone())).await.unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    for _ in 0..100 {
        if pool.reserved() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn detached_partial_state_keeps_its_memory_reserved() {
    let original = plan(DataType::Utf8View, 3, false);
    let candidate = SeriesFinalAggregateExec::try_new(&original)
        .unwrap()
        .unwrap();
    let pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
    let mut input = candidate.states(context(pool.clone())).unwrap();
    let state = input.next().await.unwrap().unwrap();
    let bytes = state._reservation.size();
    assert!(bytes > 0);
    assert!(pool.reserved() >= bytes);
    drop(input);
    for _ in 0..100 {
        if pool.reserved() == bytes {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.reserved(), bytes);
    drop(state);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn cancellation_releases_final_and_input_reservations() {
    let original = plan(DataType::Utf8View, 3, false);
    let candidate = SeriesFinalAggregateExec::try_new(&original)
        .unwrap()
        .unwrap();
    let pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
    for after_output in [false, true] {
        let mut stream = candidate.execute(0, context(pool.clone())).unwrap();
        if after_output {
            assert!(stream.next().await.unwrap().unwrap().num_rows() > 0);
            assert!(pool.reserved() > 0);
        } else {
            let _ = stream.next().now_or_never();
        }
        drop(stream);
        for _ in 0..100 {
            if pool.reserved() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn limited_sort_is_not_replaced() {
    let original = plan(DataType::Utf8View, 3, false);
    let merge = original
        .as_any()
        .downcast_ref::<SortPreservingMergeExec>()
        .unwrap();
    let limited: Arc<dyn ExecutionPlan> = Arc::new(
        SortPreservingMergeExec::new(merge.expr().clone(), merge.input().clone())
            .with_fetch(Some(1)),
    );
    assert!(
        SeriesFinalAggregateExec::try_new(&limited)
            .unwrap()
            .is_none()
    );
}
