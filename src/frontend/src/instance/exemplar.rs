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
use catalog::process_manager::{QueryStatement as CatalogQueryStatement, SlowQueryTimer};
use common_base::cancellation::CancellableFuture;
use common_catalog::consts::METRICS_EXEMPLARS_TABLE;
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_query::prelude::{
    EXEMPLAR_METRIC_COLUMN, greptime_exemplar_labels, greptime_timestamp, greptime_value,
};
use common_query::prometheus::format_prometheus_float;
use common_recordbatch::RecordBatch;
use common_time::{Timestamp, timestamp::TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{BinaryExpr, Expr, Operator, lit};
use datatypes::arrow::datatypes::DataType;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use datatypes::vectors::json::array::JsonArray;
use datatypes::vectors::{Helper, VectorRef};
use futures::StreamExt;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use servers::error::{self, Result};
use servers::exemplar::query::{Observation, Query, Selector, Series};
use servers::interceptor::{PromQueryInterceptor, PromQueryInterceptorRef};
use servers::prometheus_handler::PrometheusHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
    METRIC_ENGINE_NAME,
};
use table::metadata::TableInfo;

use crate::instance::Instance;

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
        let interceptor = self.plugins.get::<PromQueryInterceptorRef<error::Error>>();
        let mut groups = BTreeMap::<String, Vec<&Selector>>::new();
        for selector in &query.selectors {
            groups
                .entry(selector.schema.clone())
                .or_default()
                .push(selector);
        }
        // Authorizes every branch before any exemplar scan, including absent destinations.
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
        let raw_query = format!(
            "query_exemplars({}, start_ns={}, end_ns={})",
            query.expression, query.bounds.0, query.bounds.1
        );
        let slow_query_timer = self.slow_query_options.enable.then(|| {
            SlowQueryTimer::new(
                CatalogQueryStatement::Plan(raw_query.clone()),
                ctx.current_schema(),
                self.slow_query_options.threshold,
                self.slow_query_options.sample_ratio,
                self.slow_query_options.record_type,
                self.slow_query_recorder.clone(),
            )
        });
        let ticket = self.process_manager.register_query(
            ctx.current_catalog().to_string(),
            groups.keys().cloned().collect(),
            raw_query,
            ctx.conn_info().to_string(),
            Some(ctx.process_id()),
            slow_query_timer,
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
            let mut skipped = 0;
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
                let filter = filter(&info, &selectors, query.bounds)?;
                let source_tags = self.exemplar_source_tags(&selectors, &local).await?;
                let (projection, tags) = exemplar_projection(&info, source_tags.as_ref());
                let frame = self
                    .query_engine
                    .read_table(table)
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?;
                let plan = frame
                    .filter(filter)
                    .context(error::DataFusionSnafu)?
                    .select(projection)
                    .context(error::DataFusionSnafu)?
                    .into_unoptimized_plan();
                interceptor.pre_execute_exemplars(&query, info.table_ref(), local.clone())?;
                let output = self
                    .statement_executor
                    .exec_plan(plan, local)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteQuerySnafu)?;
                match output.data {
                    OutputData::Stream(mut stream) => {
                        while let Some(batch) = stream.next().await {
                            skipped += collect(
                                batch.context(error::CollectRecordbatchSnafu)?,
                                &tags,
                                &mut observations,
                                &mut reservation,
                            )?;
                            tokio::task::yield_now().await;
                        }
                    }
                    OutputData::RecordBatches(batches) => {
                        for batch in batches.take() {
                            skipped += collect(batch, &tags, &mut observations, &mut reservation)?;
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
            if skipped > 0 {
                let warning = format!("Skipped {skipped} malformed stored exemplars");
                ctx.set_warning(match ctx.warning() {
                    Some(existing) => format!("{existing}; {warning}"),
                    None => warning,
                });
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

    async fn exemplar_source_tags(
        &self,
        selectors: &[&Selector],
        ctx: &QueryContextRef,
    ) -> Result<Option<BTreeSet<String>>> {
        let Some(metrics) = selectors
            .iter()
            .map(|selector| {
                selector.matchers.iter().find_map(|matcher| {
                    (matcher.name == METRIC_NAME
                        && matches!(matcher.op, MatchOp::Equal)
                        && !matcher.value.is_empty())
                    .then_some(matcher.value.as_str())
                })
            })
            .collect::<Option<BTreeSet<_>>>()
        else {
            return Ok(None);
        };
        let mut tags = BTreeSet::new();
        for metric in metrics {
            let Some(table) = self
                .catalog_manager
                .table(
                    ctx.current_catalog(),
                    &ctx.current_schema(),
                    metric,
                    Some(ctx),
                )
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteQuerySnafu)?
            else {
                return Ok(None);
            };
            let info = table.table_info();
            if info.meta.engine != METRIC_ENGINE_NAME
                || !info
                    .meta
                    .options
                    .extra_options
                    .contains_key(LOGICAL_TABLE_METADATA_KEY)
            {
                return Ok(None);
            }
            tags.extend(
                info.meta
                    .primary_key_indices
                    .iter()
                    .map(|&index| info.meta.schema.column_schemas()[index].name.clone()),
            );
        }
        Ok(Some(tags))
    }
}

fn column(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}

fn filter(info: &TableInfo, selectors: &[&Selector], bounds: (i64, i64)) -> Result<Expr> {
    let schema = &info.meta.schema;
    let tags = info
        .meta
        .primary_key_indices
        .iter()
        .map(|index| schema.column_schemas()[*index].name.as_str())
        .filter(|name| {
            ![
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                DATA_SCHEMA_TSID_COLUMN_NAME,
            ]
            .contains(name)
        })
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

fn exemplar_projection<'a>(
    info: &'a TableInfo,
    source_tags: Option<&BTreeSet<String>>,
) -> (Vec<Expr>, Vec<&'a str>) {
    // Exact metric queries use current logical schemas, including for historical rows.
    let tags = info
        .meta
        .primary_key_indices
        .iter()
        .map(|&index| info.meta.schema.column_schemas()[index].name.as_str())
        .filter(|name| {
            ![
                EXEMPLAR_METRIC_COLUMN,
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                DATA_SCHEMA_TSID_COLUMN_NAME,
            ]
            .contains(name)
                && source_tags.is_none_or(|tags| tags.contains(*name))
        })
        .collect::<Vec<_>>();
    let projection = [
        EXEMPLAR_METRIC_COLUMN,
        greptime_timestamp(),
        greptime_value(),
        greptime_exemplar_labels(),
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
        DATA_SCHEMA_TSID_COLUMN_NAME,
    ]
    .into_iter()
    .chain(tags.iter().copied())
    .map(column)
    .collect();
    (projection, tags)
}

fn collect(
    batch: RecordBatch,
    tag_names: &[&str],
    observations: &mut Observations,
    reservation: &mut MemoryReservation,
) -> Result<usize> {
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
    let table_id = Helper::try_into_vector(batch.column(index(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)?))
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let tsid = Helper::try_into_vector(batch.column(index(DATA_SCHEMA_TSID_COLUMN_NAME)?))
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let value_type = |vector: &VectorRef| {
        let mut data_type = vector.data_type();
        // Storage may dictionary-encode tags; Vector::get returns their decoded values.
        while let ConcreteDataType::Dictionary(dictionary) = data_type {
            data_type = dictionary.value_type().clone();
        }
        data_type
    };
    ensure!(
        value_type(&metric).is_string()
            && value_type(&time).is_timestamp()
            && value_type(&value) == ConcreteDataType::float64_datatype()
            && value_type(&table_id) == ConcreteDataType::uint32_datatype()
            && value_type(&tsid) == ConcreteDataType::uint64_datatype(),
        error::UnexpectedResultSnafu {
            reason: "exemplar scan returned incompatible column types",
        }
    );
    let label_index = index(greptime_exemplar_labels())?;
    let labels = JsonArray::from(batch.column(label_index))
        .project_to_v2(schema.field(label_index), &DataType::Binary)
        .map_err(BoxedError::new)
        .context(error::ExecuteQuerySnafu)?;
    let mut tags = Vec::<(&str, VectorRef)>::new();
    for &name in tag_names {
        let values = Helper::try_into_vector(batch.column(index(name)?))
            .map_err(BoxedError::new)
            .context(error::ExecuteQuerySnafu)?;
        ensure!(
            value_type(&values).is_string(),
            error::UnexpectedResultSnafu {
                reason: "exemplar scan returned a non-string tag",
            }
        );
        tags.push((name, values));
    }
    let mut skipped = 0;
    'rows: for row in 0..batch.num_rows() {
        let Value::String(metric) = metric.get(row) else {
            skipped += 1;
            continue;
        };
        let Value::Timestamp(timestamp) = time.get(row) else {
            skipped += 1;
            continue;
        };
        let Some(timestamp) = timestamp.convert_to(TimeUnit::Nanosecond) else {
            skipped += 1;
            continue;
        };
        let timestamp = timestamp.value();
        let Value::Float64(value) = value.get(row) else {
            skipped += 1;
            continue;
        };
        let (Value::UInt32(table_id), Value::UInt64(tsid)) = (table_id.get(row), tsid.get(row))
        else {
            skipped += 1;
            continue;
        };
        let mut series = BTreeMap::from([
            (METRIC_NAME.to_string(), metric.into_string()),
            (
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                table_id.to_string(),
            ),
            (DATA_SCHEMA_TSID_COLUMN_NAME.to_string(), tsid.to_string()),
        ]);
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
            skipped += 1;
            continue;
        };
        let mut string_labels = Labels::new();
        for (name, value) in labels {
            let serde_json::Value::String(value) = value else {
                skipped += 1;
                continue 'rows;
            };
            string_labels.insert(name, value);
        }
        let labels = string_labels;
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
                timestamp: timestamp.div_euclid(1_000_000) as f64 / 1000.0,
            },
        );
    }
    reservation.shrink(memory);
    Ok(skipped)
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datatypes::arrow::array::{
        ArrayRef, Float64Array, StringArray, StructArray, TimestampNanosecondArray, UInt32Array,
        UInt64Array,
    };
    use datatypes::arrow::datatypes::Field;
    use datatypes::extension::json::Json2ExtensionType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Float64Vector;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use super::*;

    #[test]
    fn test_exemplar_projection_prunes_tags_and_preserves_source_ids() {
        let labels = StructArray::from(vec![(
            Arc::new(Field::new("trace_id", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["trace"; 4])) as ArrayRef,
        )]);
        let columns = vec![
            (
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                Arc::new(UInt32Array::from(vec![1, 1, 2, 2])) as ArrayRef,
            ),
            (
                DATA_SCHEMA_TSID_COLUMN_NAME,
                Arc::new(UInt64Array::from(vec![u64::MAX, u64::MAX, u64::MAX, 2])) as ArrayRef,
            ),
            (
                EXEMPLAR_METRIC_COLUMN,
                Arc::new(StringArray::from(vec!["metric"; 4])) as ArrayRef,
            ),
            (
                "job",
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some(""), None])) as ArrayRef,
            ),
            (
                greptime_timestamp(),
                Arc::new(TimestampNanosecondArray::from(vec![1; 4])) as ArrayRef,
            ),
            (
                greptime_value(),
                Arc::new(Float64Array::from(vec![1.0; 4])) as ArrayRef,
            ),
            (greptime_exemplar_labels(), Arc::new(labels) as ArrayRef),
            (
                "unrelated_tag",
                Arc::new(StringArray::from(vec!["other"; 4])) as ArrayRef,
            ),
        ];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, values)| {
                    let mut column = ColumnSchema::new(
                        *name,
                        ConcreteDataType::from_arrow_type(values.data_type()),
                        true,
                    );
                    if *name == greptime_exemplar_labels() {
                        column.with_extension_type(&Json2ExtensionType::default());
                    }
                    column
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::new(
            schema.clone(),
            columns
                .into_iter()
                .map(|(_, array)| Helper::try_into_vector(&array).unwrap()),
        )
        .unwrap();
        let info = TableInfoBuilder::new(
            METRICS_EXEMPLARS_TABLE,
            TableMetaBuilder::empty()
                .schema(schema)
                .primary_key_indices(vec![0, 1, 2, 3, 7])
                .next_column_id(8)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
        let source_tags = BTreeSet::from(["job".into(), "sample_only_tag".into()]);
        let (projection, tags) = exemplar_projection(&info, Some(&source_tags));
        assert_eq!(tags, ["job"]);
        let (full_projection, full_tags) = exemplar_projection(&info, None);
        assert_eq!(full_projection.len(), 8);
        assert_eq!(full_tags, ["job", "unrelated_tag"]);
        let (no_tag_projection, no_tags) = exemplar_projection(&info, Some(&BTreeSet::new()));
        assert_eq!(no_tag_projection.len(), 6);
        assert!(no_tags.is_empty());
        let indices = projection
            .into_iter()
            .map(|expr| {
                let Expr::Column(column) = expr else {
                    panic!("expected projected column")
                };
                batch
                    .df_record_batch()
                    .schema()
                    .index_of(&column.name)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(indices, [2, 4, 5, 6, 0, 1, 3]);
        let projected = batch.try_project(&indices).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut reservation = MemoryConsumer::new("projection test").register(&pool);
        let mut full = Observations::new();
        let mut narrow = Observations::new();
        assert_eq!(
            collect(batch, &tags, &mut full, &mut reservation).unwrap(),
            0
        );
        assert_eq!(
            collect(projected, &tags, &mut narrow, &mut reservation).unwrap(),
            0
        );
        assert_eq!(full.len(), 3);
        assert_eq!(full.values().map(BTreeMap::len).sum::<usize>(), 3);
        assert_eq!(full, narrow);
        for table_id in ["1", "2"] {
            assert!(narrow.keys().any(|labels| {
                labels[DATA_SCHEMA_TABLE_ID_COLUMN_NAME] == table_id
                    && labels[DATA_SCHEMA_TSID_COLUMN_NAME] == u64::MAX.to_string()
            }));
        }
        assert!(
            narrow
                .keys()
                .all(|labels| !labels.contains_key("unrelated_tag"))
        );
    }

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
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let mut reservation = MemoryConsumer::new("exemplar test").register(&pool);
        let mut observations = Observations::new();
        let error = collect(batch, &[], &mut observations, &mut reservation).unwrap_err();
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
