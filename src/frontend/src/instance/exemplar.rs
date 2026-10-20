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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use auth::{PermissionTableTarget, PermissionTableTargets};
use common_base::cancellation::CancellableFuture;
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_query::prelude::{
    EXEMPLAR_LABELS_COLUMN, EXEMPLAR_METRIC_COLUMN, METRICS_EXEMPLARS_TABLE, greptime_timestamp,
    greptime_value,
};
use common_query::prometheus::format_prometheus_float;
use common_recordbatch::RecordBatch;
use common_time::{Timestamp, timestamp::TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{BinaryExpr, Expr, Operator, lit};
use datatypes::arrow::datatypes::DataType;
use datatypes::value::Value;
use datatypes::vectors::json::array::JsonArray;
use datatypes::vectors::{Helper, VectorRef};
use futures::StreamExt;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use servers::error::{self, Result};
use servers::exemplar::query::{ExactTimestamp, Observation, Query, Selector, Series};
use servers::prometheus_handler::PrometheusHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::metadata::TableInfo;

use super::Instance;

type Labels = BTreeMap<String, String>;
// Retain NaN payloads and signed zero as distinct observations, using their stored bits.
type Observations = BTreeMap<Labels, BTreeMap<(i64, u64, Labels), Observation>>;

impl Instance {
    pub(super) async fn query_exemplars_inner(
        &self,
        query: Query,
        ctx: QueryContextRef,
    ) -> Result<Vec<Series>> {
        self.check_prom_query_privilege(&ctx)?;
        let mut groups = BTreeMap::<String, Vec<Selector>>::new();
        for selector in query.selectors {
            groups
                .entry(selector.schema.clone())
                .or_default()
                .push(selector);
        }
        // Authorize every branch before catalog lookup, including absent exemplar destinations.
        for (schema, selectors) in &groups {
            let local = ctx.fork();
            local.set_current_schema(schema);
            let local = Arc::new(local);
            let mut targets = vec![PermissionTableTarget::new(
                ctx.current_catalog(),
                schema,
                METRICS_EXEMPLARS_TABLE,
            )];
            for selector in selectors {
                let sources = selector
                    .matchers
                    .iter()
                    .filter(|matcher| {
                        matcher.name == METRIC_NAME
                            && matches!(matcher.op, MatchOp::Equal)
                            && !matcher.value.is_empty()
                    })
                    .map(|matcher| &matcher.value)
                    .collect::<BTreeSet<_>>();
                if sources.is_empty() {
                    self.check_query_target_permission(PermissionTableTargets::Unresolved, &local)
                        .await?;
                }
                targets.extend(sources.into_iter().map(|source| {
                    PermissionTableTarget::new(ctx.current_catalog(), schema, source)
                }));
            }
            self.check_query_target_permission(PermissionTableTargets::resolved(targets), &local)
                .await?;
        }
        let Some(bounds) = query.bounds else {
            return Ok(Vec::new());
        };
        let ticket = self.process_manager.register_query(
            ctx.current_catalog().to_string(),
            groups.keys().cloned().collect(),
            format!("query_exemplars({})", query.expression),
            ctx.conn_info().to_string(),
            Some(ctx.process_id()),
            None,
        );
        let cancellation = ticket.cancellation_handle.clone();
        let future = async {
            let pool = self
                .query_engine
                .engine_state()
                .session_state()
                .runtime_env()
                .memory_pool
                .clone();
            let mut reservation =
                MemoryConsumer::new("Prometheus exemplar response").register(&pool);
            let mut observations = Observations::new();
            for (schema, selectors) in groups {
                let local = ctx.fork();
                local.set_current_schema(&schema);
                let local = Arc::new(local);
                let Some(table) = self
                    .catalog_manager
                    .table(
                        ctx.current_catalog(),
                        &schema,
                        METRICS_EXEMPLARS_TABLE,
                        Some(&local),
                    )
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?
                else {
                    continue;
                };
                let info = table.table_info();
                operator::insert::validate_exemplar_table(&info)
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?;
                let filter = filter(&info, &selectors, bounds)?;
                let frame = self
                    .query_engine
                    .read_table(table)
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?;
                let plan = frame
                    .filter(filter)
                    .context(error::DataFusionSnafu)?
                    .into_unoptimized_plan();
                let output = self
                    .statement_executor
                    .exec_plan(plan, local)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?;
                match output.data {
                    OutputData::Stream(mut stream) => {
                        while let Some(batch) = stream.next().await {
                            collect(
                                batch.context(error::CollectRecordbatchSnafu)?,
                                &info,
                                &mut observations,
                                &mut reservation,
                            )?;
                            tokio::task::yield_now().await;
                        }
                    }
                    OutputData::RecordBatches(batches) => {
                        for batch in batches.take() {
                            collect(batch, &info, &mut observations, &mut reservation)?;
                            tokio::task::yield_now().await;
                        }
                    }
                    _ => {
                        return error::UnexpectedResultSnafu {
                            reason: "exemplar scan did not return rows",
                        }
                        .fail();
                    }
                }
            }
            Ok(observations
                .into_iter()
                .map(|(series_labels, observations)| Series {
                    series_labels,
                    exemplars: observations.into_values().collect(),
                })
                .collect())
        };
        let result = CancellableFuture::new(future, cancellation)
            .await
            .map_err(|_| error::CancelledSnafu.build())?;
        drop(ticket);
        result
    }
}

fn column(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}

fn filter(info: &TableInfo, selectors: &[Selector], bounds: (i64, i64)) -> Result<Expr> {
    let schema = &info.meta.schema;
    let tags = info
        .meta
        .primary_key_indices
        .iter()
        .map(|index| schema.column_schemas()[*index].name.as_str())
        .collect::<BTreeSet<_>>();
    let labels = selectors
        .iter()
        .map(|selector| {
            selector
                .matchers
                .iter()
                .map(|matcher| matcher_filter(matcher, &tags))
                .fold(lit(true), Expr::and)
        })
        .fold(lit(false), Expr::or);
    let unit = schema
        .timestamp_column()
        .and_then(|column| column.data_type.as_timestamp())
        .map(|t| t.unit())
        .context(error::UnexpectedResultSnafu {
            reason: "exemplar time index is missing",
        })?;
    let start = Timestamp::new_nanosecond(bounds.0)
        .convert_to_ceil(unit)
        .context(error::InvalidQuerySnafu {
            reason: "exemplar start exceeds timestamp range",
        })?;
    let end = Timestamp::new_nanosecond(bounds.1)
        .convert_to(unit)
        .context(error::InvalidQuerySnafu {
            reason: "exemplar end exceeds timestamp range",
        })?;
    let literal = |timestamp: Timestamp| {
        Expr::Literal(
            match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp.value()), None),
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(timestamp.value()), None)
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(timestamp.value()), None)
                }
                TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(timestamp.value()), None)
                }
            },
            None,
        )
    };
    Ok(labels
        .and(column(greptime_timestamp()).gt_eq(literal(start)))
        .and(column(greptime_timestamp()).lt_eq(literal(end))))
}

fn matcher_filter(matcher: &Matcher, tags: &BTreeSet<&str>) -> Expr {
    let name = if matcher.name == METRIC_NAME {
        EXEMPLAR_METRIC_COLUMN
    } else {
        matcher.name.as_str()
    };
    // Storage fields and the physical metric column are not Prometheus series labels.
    if (matcher.name == EXEMPLAR_METRIC_COLUMN || !tags.contains(name))
        && matcher.name != METRIC_NAME
    {
        return lit(matcher.is_match(""));
    }
    let col = column(name);
    let condition = match &matcher.op {
        MatchOp::Equal => col.clone().eq(lit(matcher.value.clone())),
        MatchOp::NotEqual => col.clone().not_eq(lit(matcher.value.clone())),
        MatchOp::Re(regex) | MatchOp::NotRe(regex) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col.clone()),
            op: if matches!(matcher.op, MatchOp::Re(_)) {
                Operator::RegexMatch
            } else {
                Operator::RegexNotMatch
            },
            right: Box::new(lit(regex.as_str().to_string())),
        }),
    };
    if matcher.is_match("") {
        col.is_null().or(condition)
    } else {
        condition
    }
}

fn collect(
    batch: RecordBatch,
    info: &TableInfo,
    observations: &mut Observations,
    reservation: &mut MemoryReservation,
) -> Result<()> {
    let memory = batch.buffer_memory_size();
    reservation
        .try_grow(memory)
        .context(error::DataFusionSnafu)?;
    let schema = batch.df_record_batch().schema();
    let index = |name: &str| schema.index_of(name).context(error::ArrowSnafu);
    let metric = Helper::try_into_vector(batch.column(index(EXEMPLAR_METRIC_COLUMN)?))
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let time = Helper::try_into_vector(batch.column(index(greptime_timestamp())?))
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let value = Helper::try_into_vector(batch.column(index(greptime_value())?))
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let label_index = index(EXEMPLAR_LABELS_COLUMN)?;
    let labels = JsonArray::from(batch.column(label_index))
        .project_to_v2(schema.field(label_index), &DataType::Binary)
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let mut tags = Vec::<(&str, VectorRef)>::new();
    for tag in &info.meta.primary_key_indices {
        let name = info.meta.schema.column_schemas()[*tag].name.as_str();
        if name != EXEMPLAR_METRIC_COLUMN {
            tags.push((
                name,
                Helper::try_into_vector(batch.column(index(name)?))
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?,
            ));
        }
    }
    for row in 0..batch.num_rows() {
        let Value::String(metric) = metric.get(row) else {
            return error::UnexpectedResultSnafu {
                reason: "null or non-string exemplar metric",
            }
            .fail();
        };
        let Value::Timestamp(timestamp) = time.get(row) else {
            return error::UnexpectedResultSnafu {
                reason: "null exemplar timestamp",
            }
            .fail();
        };
        let timestamp = timestamp
            .convert_to(TimeUnit::Nanosecond)
            .context(error::UnexpectedResultSnafu {
                reason: "stored exemplar timestamp overflow",
            })?
            .value();
        let Value::Float64(value) = value.get(row) else {
            return error::UnexpectedResultSnafu {
                reason: "null exemplar value",
            }
            .fail();
        };
        let mut series = BTreeMap::from([(METRIC_NAME.to_string(), metric.into_string())]);
        for (name, values) in &tags {
            if let Value::String(value) = values.get(row) {
                series.insert((*name).to_string(), value.into_string());
            }
        }
        let labels = JsonArray::from(&labels)
            .try_get_value(row)
            .map_err(BoxedError::new)
            .context(error::ExecuteQuerySnafu)?;
        let serde_json::Value::Object(labels) = labels else {
            return error::UnexpectedResultSnafu {
                reason: "exemplar_labels must be an object",
            }
            .fail();
        };
        let labels = labels
            .into_iter()
            .map(|(name, value)| {
                let serde_json::Value::String(value) = value else {
                    return error::UnexpectedResultSnafu {
                        reason: "exemplar label value must be a string",
                    }
                    .fail();
                };
                Ok((name, value))
            })
            .collect::<Result<Labels>>()?;
        let key = (timestamp, value.0.to_bits(), labels.clone());
        if observations
            .get(&series)
            .is_some_and(|items| items.contains_key(&key))
        {
            continue;
        }
        let bytes = 256
            + series
                .iter()
                .chain(&labels)
                .map(|(name, value)| name.len() + value.len() + 128)
                .sum::<usize>()
                * 2;
        reservation
            .try_grow(bytes)
            .context(error::DataFusionSnafu)?;
        observations.entry(series).or_default().insert(
            key,
            Observation {
                labels,
                value: format_prometheus_float(value.0),
                timestamp: ExactTimestamp(timestamp),
            },
        );
    }
    reservation.shrink(memory);
    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Float64Vector;
    use table::metadata::{TableInfoBuilder, TableMeta};

    use super::*;

    #[test]
    fn test_exemplar_scan_batch_respects_query_memory_budget() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            greptime_value(),
            ConcreteDataType::float64_datatype(),
            false,
        )]));
        let batch = RecordBatch::new(
            schema,
            vec![Arc::new(Float64Vector::from_vec(vec![1.0; 128])) as VectorRef],
        )
        .unwrap();
        let info = TableInfoBuilder::new(METRICS_EXEMPLARS_TABLE, TableMeta::empty())
            .build()
            .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let mut reservation = MemoryConsumer::new("exemplar test").register(&pool);
        let mut observations = Observations::new();
        let error = collect(batch, &info, &mut observations, &mut reservation).unwrap_err();
        assert!(matches!(
            error,
            error::Error::DataFusion {
                error: datafusion::error::DataFusionError::ResourcesExhausted(_),
                ..
            }
        ));
        assert!(observations.is_empty());
        assert_eq!(pool.reserved(), 0);
    }
}
