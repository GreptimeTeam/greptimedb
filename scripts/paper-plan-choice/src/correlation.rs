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

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::hint::black_box;
use std::time::Instant;

use datafusion_common::config::ConfigOptions;

use crate::Context;
use datafusion_common::ScalarValue;
use datafusion_common::stats::{ColumnStatistics, Precision};
use datafusion_common::{JoinType, NullEquality, Result, Statistics};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::join_selection::JoinSelection;
use datafusion_physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
use datafusion_physical_plan::operator_statistics::{
    ExtendedStatistics, StatisticsProvider, StatisticsRegistry, StatisticsResult,
};
use datafusion_physical_plan::{ExecutionPlan, collect};

#[derive(Debug, Clone, Copy)]
enum BoundMode {
    Ordinary,
    Head(usize),
}

#[derive(Debug)]
struct FixtureStats {
    mode: BoundMode,
    anti: bool,
}

impl StatisticsProvider for FixtureStats {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        _children: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        if let Some(source) = plan.downcast_ref::<DataSourceExec>() {
            if source
                .data_source()
                .downcast_ref::<MemorySourceConfig>()
                .is_none()
            {
                return Ok(StatisticsResult::Delegate);
            }
            let fields = plan.schema();
            let Some(key_index) = fields
                .index_of("r_key")
                .ok()
                .or_else(|| fields.index_of("s_key").ok())
                .or_else(|| fields.index_of("t_key").ok())
            else {
                return Ok(StatisticsResult::Delegate);
            };
            let key_name = fields.field(key_index).name();
            let (rows, distinct) = if key_name == "t_key" {
                (2049, 2048)
            } else {
                let frequencies = frequencies_for_case(key_name, self.anti);
                (frequencies.values().sum::<usize>() + 1, frequencies.len())
            };
            let mut stats = Statistics::new_unknown(&fields).with_num_rows(Precision::Exact(rows));
            stats.total_byte_size = Precision::Absent;
            let columns = stats
                .column_statistics
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    if i == key_index {
                        ColumnStatistics::new_unknown()
                            .with_null_count(Precision::Exact(1))
                            .with_distinct_count(Precision::Exact(distinct))
                            .with_min_value(Precision::Exact(ScalarValue::Int64(Some(0))))
                            .with_max_value(Precision::Exact(ScalarValue::Int64(Some(
                                if key_name == "t_key" { 2047 } else { 15 },
                            ))))
                    } else {
                        ColumnStatistics::new_unknown()
                    }
                })
                .collect();
            stats.column_statistics = columns;
            return Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)));
        }
        if let Some(join) = plan.downcast_ref::<HashJoinExec>()
            && let BoundMode::Head(m) = self.mode
            && rs_keys(join)
        {
            let bound = head_tail_bound(
                &frequencies_for_case("r_key", self.anti),
                &frequencies_for_case("s_key", self.anti),
                m,
            );
            let mut stats =
                Statistics::new_unknown(&plan.schema()).with_num_rows(Precision::Inexact(bound));
            stats.total_byte_size = Precision::Absent;
            return Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)));
        }
        Ok(StatisticsResult::Delegate)
    }
}

fn frequencies_for_case(name: &str, anti: bool) -> BTreeMap<i64, usize> {
    let mut f = BTreeMap::new();
    for k in 0..16 {
        f.insert(
            k,
            if (name == "r_key" && k == 0)
                || (name == "s_key" && if anti { k == 1 } else { k == 0 })
            {
                68
            } else {
                4
            },
        );
    }
    f
}
fn rs_keys(join: &HashJoinExec) -> bool {
    let left = join.left().schema();
    let right = join.right().schema();
    let schemas = [&left, &right];
    schemas
        .iter()
        .any(|s| s.fields().iter().any(|f| f.name() == "r_key"))
        && schemas
            .iter()
            .any(|s| s.fields().iter().any(|f| f.name() == "s_key"))
        && !schemas
            .iter()
            .any(|s| s.fields().iter().any(|f| f.name() == "t_key"))
}
fn isqrt_ceil(n: u128) -> u128 {
    if n == 0 {
        return 0;
    }
    let mut lo = 1u128;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if mid >= n.div_ceil(mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}
fn head_tail_bound(r: &BTreeMap<i64, usize>, s: &BTreeMap<i64, usize>, m: usize) -> usize {
    let mut keys: Vec<_> = r.keys().copied().collect();
    keys.sort_by_key(|k| {
        (
            std::cmp::Reverse((r[k] as u128).pow(2) + (s[k] as u128).pow(2)),
            *k,
        )
    });
    let head: u128 = keys
        .iter()
        .take(m)
        .map(|k| (r[k] as u128) * (s[k] as u128))
        .sum();
    let rr: u128 = keys.iter().skip(m).map(|k| (r[k] as u128).pow(2)).sum();
    let ss: u128 = keys.iter().skip(m).map(|k| (s[k] as u128).pow(2)).sum();
    usize::try_from(head + isqrt_ceil(rr * ss)).unwrap()
}

fn schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(format!("{prefix}_key"), DataType::Int64, true),
        Field::new(format!("{prefix}_val"), DataType::Int64, true),
    ]))
}
fn source(prefix: &str, anti: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    if prefix == "r" || prefix == "s" {
        for key in 0..16 {
            let count = if (key == 0 && prefix == "r")
                || (key == if anti && prefix == "s" { 1 } else { 0 })
            {
                68
            } else {
                4
            };
            for i in 0..count {
                keys.push(Some(key));
                vals.push(if i % 5 == 0 {
                    None
                } else {
                    Some((i % 9) as i64)
                });
            }
        }
        keys.push(None);
        vals.push(None);
    } else {
        for key in 0..2048 {
            keys.push(Some(key));
            vals.push(if key % 17 == 0 { None } else { Some(key % 11) });
        }
        keys.push(None);
        vals.push(None);
    }
    let arrays: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(vals)),
    ];
    let schema = schema(prefix);
    Ok(MemorySourceConfig::try_new_exec(
        &[vec![RecordBatch::try_new(Arc::clone(&schema), arrays)?]],
        schema,
        None,
    )?)
}
fn join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    lk: &str,
    rk: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    let on: JoinOn = vec![(
        Arc::new(Column::new(lk, left.schema().index_of(lk)?)),
        Arc::new(Column::new(rk, right.schema().index_of(rk)?)),
    )];
    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Auto,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}
fn outer_metrics_build_mem(plan: &dyn ExecutionPlan) -> Option<usize> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        let has_t =
            |p: &Arc<dyn ExecutionPlan>| p.schema().fields().iter().any(|f| f.name() == "t_key");
        if has_t(join.left()) || has_t(join.right()) {
            return join
                .metrics()?
                .sum_by_name("build_mem_used")
                .map(|value| value.as_usize());
        }
    }
    plan.children()
        .into_iter()
        .find_map(|c| outer_metrics_build_mem(c.as_ref()))
}
fn find_joins(p: &dyn ExecutionPlan, out: &mut Vec<(bool, bool, bool, PartitionMode)>) {
    if let Some(j) = p.downcast_ref::<HashJoinExec>() {
        let has_t = |plan: &Arc<dyn ExecutionPlan>| {
            plan.schema().fields().iter().any(|f| f.name() == "t_key")
        };
        let left_t = has_t(j.left());
        let any_t = left_t || has_t(j.right());
        let rs = rs_keys(j);
        out.push((left_t, any_t, rs, j.mode));
    }
    for c in p.children() {
        find_joins(c.as_ref(), out);
    }
}
type Row = (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);
async fn rows(plan: Arc<dyn ExecutionPlan>, schema: &SchemaRef) -> Result<Vec<Row>> {
    let mut out = Vec::new();
    for b in collect(plan, Arc::new(TaskContext::default())).await? {
        assert_eq!(b.schema().as_ref(), schema.as_ref());
        for i in 0..b.num_rows() {
            let v = |n| {
                let a = b.column(n).as_any().downcast_ref::<Int64Array>().unwrap();
                (!a.is_null(i)).then(|| a.value(i))
            };
            out.push((v(0), v(1), v(2), v(3), v(4), v(5)));
        }
    }
    out.sort_unstable();
    Ok(out)
}
fn oracle(anti: bool) -> Vec<Row> {
    let mut result = Vec::new();
    let raw = |prefix: &str| {
        let mut rows = Vec::new();
        if prefix == "r" || prefix == "s" {
            for key in 0..16i64 {
                let count = if (prefix == "r" && key == 0)
                    || (prefix == "s" && key == if anti { 1 } else { 0 })
                {
                    68
                } else {
                    4
                };
                for i in 0..count {
                    rows.push((
                        Some(key),
                        if i % 5 == 0 {
                            None
                        } else {
                            Some((i % 9) as i64)
                        },
                    ));
                }
            }
        } else {
            for key in 0..2048i64 {
                rows.push((Some(key), if key % 17 == 0 { None } else { Some(key % 11) }));
            }
        }
        rows.push((None, None));
        rows
    };
    let r = raw("r");
    let s = raw("s");
    let t = raw("t");
    for (rk, rv) in &r {
        for (sk, sv) in &s {
            if rk.is_some() && rk == sk {
                for (tk, tv) in &t {
                    if rk == tk {
                        result.push((*rk, *rv, *sk, *sv, *tk, *tv));
                    }
                }
            }
        }
    }
    result.sort_unstable();
    result
}

fn plan_case(
    anti: bool,
    mode: BoundMode,
    validate: bool,
) -> Result<(Arc<dyn ExecutionPlan>, SchemaRef, String)> {
    let r = source("r", anti)?;
    let s = source("s", anti)?;
    let t = source("t", anti)?;
    let rs = join(Arc::clone(&r), Arc::clone(&s), "r_key", "s_key")?;
    let fixed = join(Arc::clone(&rs), Arc::clone(&t), "r_key", "t_key")?;
    let schema = fixed.schema();
    let mut config = ConfigOptions::default();
    config.optimizer.use_statistics_registry = true;
    config.optimizer.join_reordering = true;
    config.optimizer.hash_join_single_partition_threshold = 0;
    config.optimizer.hash_join_single_partition_threshold_rows = 0;
    let mut registry = StatisticsRegistry::default_with_builtin_providers();
    registry.register(Arc::new(FixtureStats { mode, anti }));
    let context = Context { config, registry };
    if validate {
        for (source_plan, expected_rows, ndv, key_name) in [
            (&r, 129, 16, "r_key"),
            (&s, 129, 16, "s_key"),
            (&t, 2049, 2048, "t_key"),
        ] {
            let stats = context.registry.compute(source_plan.as_ref())?;
            assert_eq!(stats.base().num_rows, Precision::Exact(expected_rows));
            assert_eq!(stats.base().total_byte_size, Precision::Absent);
            let key = source_plan.schema().index_of(key_name)?;
            assert_eq!(
                stats.base().column_statistics[key].null_count,
                Precision::Exact(1)
            );
            assert_eq!(
                stats.base().column_statistics[key].distinct_count,
                Precision::Exact(ndv)
            );
        }
    }
    if validate {
        let rsstat = context.registry.compute(rs.as_ref())?;
        let actual = rsstat.base().num_rows;
        let expected_stat = match mode {
            BoundMode::Ordinary => Precision::Inexact(1040),
            BoundMode::Head(0) => Precision::Inexact(4864),
            BoundMode::Head(1) => Precision::Inexact(if anti { 1351 } else { 4864 }),
            BoundMode::Head(2) => Precision::Inexact(if anti { 768 } else { 4864 }),
            BoundMode::Head(_) => unreachable!(),
        };
        assert_eq!(actual, expected_stat);
        assert_eq!(rsstat.base().total_byte_size, Precision::Absent);
        println!("correlation_stats anti={anti} mode={mode:?} rs_estimate={actual:?}");
    }
    let plan = JoinSelection::new().optimize_with_context(fixed, &context)?;
    let build = if validate {
        let mut joins = Vec::new();
        find_joins(plan.as_ref(), &mut joins);
        assert_eq!(joins.len(), 2);
        assert!(
            joins
                .iter()
                .all(|(_, _, _, mode)| *mode == PartitionMode::Partitioned)
        );
        let outer: Vec<_> = joins.iter().filter(|(_, has_t, _, _)| *has_t).collect();
        let inner: Vec<_> = joins
            .iter()
            .filter(|(_, has_t, rs, _)| !has_t && *rs)
            .collect();
        assert_eq!(outer.len(), 1);
        assert_eq!(inner.len(), 1);
        if outer[0].0 { "T" } else { "RS" }.to_string()
    } else {
        String::new()
    };
    Ok((plan, schema, build))
}

async fn run_one(anti: bool, mode: BoundMode) -> Result<()> {
    let (plan, schema, build) = plan_case(anti, mode, true)?;
    let output = rows(Arc::clone(&plan), &schema).await?;
    let expected = oracle(anti);
    assert_eq!(output, expected);
    let expected_build = match mode {
        BoundMode::Ordinary => "RS",
        BoundMode::Head(0) => "T",
        BoundMode::Head(1) | BoundMode::Head(2) => {
            if anti {
                "RS"
            } else {
                "T"
            }
        }
        BoundMode::Head(_) => unreachable!(),
    };
    assert_eq!(build, expected_build);
    let mem = outer_metrics_build_mem(plan.as_ref());
    println!(
        "correlation anti={anti} mode={mode:?} base_rows=129 ndv=16 true_rs_rows={} outer_build={build} output_rows={} outer_partition_build_mem_sum_bytes={mem:?}",
        expected.len(),
        output.len()
    );
    Ok(())
}

pub async fn run_cases() -> Result<()> {
    for anti in [false, true] {
        for mode in [
            BoundMode::Ordinary,
            BoundMode::Head(1),
            BoundMode::Head(0),
            BoundMode::Head(2),
        ] {
            run_one(anti, mode).await?;
        }
    }
    Ok(())
}

pub async fn run_bench() -> Result<()> {
    let cases = [
        (false, BoundMode::Ordinary),
        (false, BoundMode::Head(1)),
        (false, BoundMode::Head(0)),
        (false, BoundMode::Head(2)),
        (true, BoundMode::Ordinary),
        (true, BoundMode::Head(1)),
        (true, BoundMode::Head(0)),
        (true, BoundMode::Head(2)),
    ];
    let mut planning = vec![Vec::new(); cases.len()];
    let mut execution = vec![Vec::new(); cases.len()];
    let mut total = vec![Vec::new(); cases.len()];
    for _ in 0..2 {
        for &(anti, mode) in &cases {
            let (plan, schema, _) = plan_case(anti, mode, false)?;
            let batches = collect(plan, Arc::new(TaskContext::default())).await?;
            black_box(batches);
            black_box(schema);
        }
    }
    for round in 0..11 {
        for step in 0..cases.len() {
            let i = (step + round) % cases.len();
            let (anti, mode) = cases[i];
            let total_start = Instant::now();
            let start = Instant::now();
            let (plan, schema, _) = plan_case(anti, mode, false)?;
            planning[i].push(start.elapsed().as_nanos());
            let start = Instant::now();
            let batches = collect(plan, Arc::new(TaskContext::default())).await?;
            execution[i].push(start.elapsed().as_nanos());
            total[i].push(total_start.elapsed().as_nanos());
            black_box(batches);
            black_box(schema);
        }
    }
    for (i, &(anti, mode)) in cases.iter().enumerate() {
        planning[i].sort_unstable();
        execution[i].sort_unstable();
        total[i].sort_unstable();
        println!(
            "bench anti={anti} mode={mode:?} median_planning_ns={} median_execute_ns={} median_total_ns={} boundary=source_construction+statistics+JoinSelection+collect;no_oracle_or_output_sort",
            planning[i][5], execution[i][5], total[i][5]
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_are_head_tail_and_conservative() {
        let r = frequencies_for_case("r_key", false);
        let s = r.clone();
        assert_eq!(head_tail_bound(&r, &s, 1), 4864);
        let anti = frequencies_for_case("s_key", true);
        let mut aligned_degrees: Vec<_> = s.values().copied().collect();
        let mut r_degrees: Vec<_> = r.values().copied().collect();
        let mut s_degrees: Vec<_> = anti.values().copied().collect();
        aligned_degrees.sort_unstable();
        r_degrees.sort_unstable();
        s_degrees.sort_unstable();
        assert_eq!(r_degrees, aligned_degrees);
        assert_eq!(r_degrees, s_degrees);
        assert_eq!(
            (r.keys().min(), r.keys().max()),
            (anti.keys().min(), anti.keys().max())
        );
        assert_eq!(
            (r.keys().min(), r.keys().max()),
            (s.keys().min(), s.keys().max())
        );
        assert_eq!(head_tail_bound(&r, &anti, 1), 1351);
        assert_eq!(head_tail_bound(&r, &anti, 0), 4864);
    }
    #[tokio::test]
    async fn real_join_selection_and_results() -> Result<()> {
        run_cases().await
    }
}
