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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt, stream};

use crate::research_series_grid::evaluate::{SelectorKey, VectorResult};
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::online_input::SeriesInput;

pub(crate) struct CollectedSeries {
    pub inputs: BTreeMap<SelectorKey, VectorResult>,
    // Blocking workers retain this reservation even if the consumer is cancelled.
    pub reservation: Arc<MemoryReservation>,
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn checked_bytes(parts: impl IntoIterator<Item = usize>) -> Result<usize> {
    parts.into_iter().try_fold(0usize, |sum, part| {
        sum.checked_add(part)
            .ok_or_else(|| invalid("Online series memory size overflow"))
    })
}

/// Inputs must emit complete, disjoint series per batch, as guaranteed by SeriesDivide.
pub(crate) async fn collect_series(
    specs: Arc<Vec<SeriesInput>>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    grid: Arc<TimeGrid>,
    lookback: i64,
    workers: usize,
    context: Arc<TaskContext>,
    cancelled: Arc<AtomicBool>,
) -> Result<CollectedSeries> {
    if specs.len() != children.len() || workers == 0 || workers > 32 {
        return Err(invalid("Invalid online series inputs or worker count"));
    }
    if cancelled.load(Ordering::Relaxed) {
        return Err(invalid("Online series query cancelled"));
    }
    let reservation = Arc::new(
        MemoryConsumer::new("ResearchOnlineSeries").register(&context.runtime_env().memory_pool),
    );
    let mut collected = CollectedSeries {
        inputs: BTreeMap::new(),
        reservation: reservation.clone(),
    };
    for spec in specs.iter() {
        if collected
            .inputs
            .insert(
                spec.key.clone(),
                VectorResult {
                    series: Vec::new(),
                    tag_order: spec.tags.clone(),
                    reservation: None,
                },
            )
            .is_some()
        {
            return Err(invalid("Duplicate online selector input"));
        }
    }
    // Poll every partition: limiting active partitions can deadlock bounded repartition queues.
    // Only the CPU evaluation jobs are bounded by `workers`.
    let mut streams = Vec::new();
    for (index, child) in children.iter().enumerate() {
        for partition in 0..child.output_partitioning().partition_count() {
            streams.push(
                child
                    .execute(partition, context.clone())?
                    .map_ok(move |batch| (index, batch))
                    .boxed(),
            );
        }
    }
    let tasks = stream::select_all(streams)
        .map_ok(|(index, batch)| {
            let reservation = reservation.clone();
            let spec = specs[index].clone();
            let grid = grid.clone();
            let cancelled = cancelled.clone();
            async move {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(invalid("Online series query cancelled"));
                }
                let temporary = reservation.new_empty();
                temporary.try_grow(checked_bytes([
                    batch.get_array_memory_size(),
                    batch.num_rows().saturating_mul(64),
                    grid.instants.len().saturating_mul(64),
                    spec.tags.len().saturating_mul(256),
                ])?)?;
                common_runtime::spawn_blocking_query(move || {
                    if cancelled.load(Ordering::Relaxed) {
                        return Err(invalid("Online series query cancelled"));
                    }
                    let series = spec.evaluate(&batch, &grid, lookback)?;
                    if let Some(series) = &series {
                        reservation.try_grow(series.memory_size())?;
                    }
                    drop(temporary);
                    Ok::<_, DataFusionError>((index, series))
                })
                .await
                .map_err(|error| invalid(error.to_string()))?
            }
        })
        .try_buffer_unordered(workers);
    futures::pin_mut!(tasks);
    while let Some((index, series)) = tasks.try_next().await? {
        let input = collected
            .inputs
            .get_mut(&specs[index].key)
            .ok_or_else(|| invalid("Online selector input disappeared"))?;
        if let Some(series) = series {
            input.series.push(series);
        }
    }
    let cancelled = cancelled.clone();
    common_runtime::spawn_blocking_query(move || {
        // Partition completion must not choose the floating-point reduction order.
        for input in collected.inputs.values_mut() {
            if cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Online series query cancelled"));
            }
            input
                .series
                .sort_unstable_by(|left, right| left.labels.cmp(&right.labels));
            if input
                .series
                .windows(2)
                .any(|pair| pair[0].labels == pair[1].labels)
            {
                return Err(invalid(
                    "Online input split a series across batches or partitions",
                ));
            }
        }
        Ok(collected)
    })
    .await
    .map_err(|error| invalid(error.to_string()))?
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use promql_parser::parser::{Expr, parse};

    use super::*;

    async fn collect(
        labels: &[&str],
        limit: usize,
        cancelled: bool,
    ) -> (Result<CollectedSeries>, Arc<GreedyMemoryPool>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("host", DataType::Utf8, false),
        ]));
        let partitions: Vec<_> = labels
            .iter()
            .map(|label| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(TimestampMillisecondArray::from(vec![0, 10])),
                            Arc::new(Float64Array::from(vec![2.0, 3.0])),
                            Arc::new(StringArray::from(vec![*label; 2])),
                        ],
                    )
                    .unwrap(),
                ]
            })
            .collect();
        let child = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, None).unwrap(),
        )));
        let pool = Arc::new(GreedyMemoryPool::new(limit));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .unwrap();
        let context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
        let Expr::VectorSelector(selector) = parse("x").unwrap() else {
            panic!("selector")
        };
        let spec = SeriesInput {
            key: SelectorKey::new(&selector, None),
            metric: "x".into(),
            time_column: "ts".into(),
            value_column: "value".into(),
            tags: vec!["host".into()],
            offset: 0,
        };
        let result = collect_series(
            Arc::new(vec![spec]),
            vec![child],
            Arc::new(TimeGrid::new(0, 10, 10).unwrap()),
            30,
            2,
            context,
            Arc::new(AtomicBool::new(cancelled)),
        )
        .await;
        (result, pool)
    }

    #[tokio::test]
    async fn consumes_all_partitions_and_keeps_reservations_until_drop() {
        let (result, pool) = collect(&["d", "b", "c", "a"], 1 << 20, false).await;
        let result = result.unwrap();
        let series = &result.inputs.values().next().unwrap().series;
        assert_eq!(
            series
                .iter()
                .map(|s| s.labels["host"].as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c", "d"]
        );
        assert!(series.iter().all(|s| s.values == [Some(2.0), Some(3.0)]));
        assert!(pool.reserved() > 0);
        assert_eq!(result.reservation.size(), pool.reserved());
        drop(result);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn rejects_split_series_memory_exhaustion_and_cancellation() {
        let (duplicate, pool) = collect(&["a", "a"], 1 << 20, false).await;
        assert!(
            duplicate
                .err()
                .unwrap()
                .to_string()
                .contains("split a series")
        );
        assert_eq!(pool.reserved(), 0);
        let (exhausted, pool) = collect(&["a"], 1, false).await;
        assert!(matches!(
            exhausted,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(pool.reserved(), 0);
        let (cancelled, pool) = collect(&["a"], 1 << 20, true).await;
        assert!(cancelled.err().unwrap().to_string().contains("cancelled"));
        assert_eq!(pool.reserved(), 0);
    }
}
