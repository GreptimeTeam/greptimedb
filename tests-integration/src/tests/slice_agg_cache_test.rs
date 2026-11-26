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

//! Differential test of the slice aggregate cache: random writes, deletes, flushes,
//! compactions, schema changes and truncations interleaved with aggregations, each run
//! with and without the cache.

use std::sync::Arc;
use std::time::Duration;

use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::standalone::GreptimeDbStandaloneBuilder;
use crate::test_util::execute_sql;

const TABLE: &str = "slice_cache";
const HOUR_MS: i64 = 3_600_000;
const MINUTE_MS: i64 = 60_000;
/// 2023-11-14T22:00:00Z, aligned to the hour.
const BASE_MS: i64 = 1_700_000_000_000 - 1_700_000_000_000 % HOUR_MS;
const SPAN_HOURS: i64 = 12;
const HOSTS: u64 = 4;

/// Deterministic xorshift, so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

async fn query(instance: &Arc<Instance>, sql: &str, cache: bool) -> String {
    let mut ctx = QueryContext::with_db_name(None);
    ctx.set_extension("query.experimental_slice_agg_cache", cache.to_string());
    let output = instance
        .do_query(sql, Arc::new(ctx))
        .await
        .remove(0)
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e:?}"));
    output.data.pretty_print().await
}

/// How the cache plans a query.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Plan {
    /// Not rewritten.
    Original,
    /// Slices taken from a time bucket the query groups by.
    Bucket,
    /// Slices from an added group key.
    Added,
}

/// The aggregations under test over `[start, end)`, and how the cache plans them.
/// Values are integers stored as doubles, so sums and averages do not depend on the order
/// states are merged in.
fn queries(start: i64, end: i64) -> Vec<(String, Plan)> {
    let range = format!("ts >= {start}::TIMESTAMP(3) AND ts < {end}::TIMESTAMP(3)");
    vec![
        (
            format!(
                "SELECT date_bin(INTERVAL '10 minutes', ts) AS b, host, count(*), sum(v), avg(v), max(v) \
                 FROM {TABLE} WHERE {range} GROUP BY b, host ORDER BY b, host"
            ),
            Plan::Bucket,
        ),
        // The time index is not projected, so the scan has to carry it for slicing.
        (
            format!("SELECT sum(v), count(v) FROM {TABLE} WHERE {range} AND host != 'h0'"),
            Plan::Added,
        ),
        // Predicates on the time index that the slice bounds do not imply stay in the key,
        // so these two do not share entries.
        (
            format!(
                "SELECT date_part('minute', ts) < 30 AS early, count(*) FROM {TABLE} WHERE {range} \
                 AND date_part('minute', ts) >= 10 GROUP BY early ORDER BY early"
            ),
            Plan::Added,
        ),
        (
            format!(
                "SELECT date_part('minute', ts) < 30 AS early, count(*) FROM {TABLE} WHERE {range} \
                 AND date_part('minute', ts) >= 40 GROUP BY early ORDER BY early"
            ),
            Plan::Added,
        ),
        (
            format!(
                "SELECT v > 50 AS high, count(DISTINCT host) FROM {TABLE} WHERE {range} \
                 GROUP BY high ORDER BY high"
            ),
            Plan::Added,
        ),
        (
            format!(
                "SELECT date_bin(INTERVAL '30 minutes', ts) AS b, approx_distinct(CAST(v AS BIGINT)) FROM {TABLE} \
                 WHERE {range} GROUP BY b ORDER BY b"
            ),
            Plan::Bucket,
        ),
        // A string interval makes `date_bin` cast the time index to nanoseconds.
        (
            format!(
                "SELECT date_bin('20 minutes', ts) AS b, count(*), sum(v) FROM {TABLE} \
                 WHERE {range} GROUP BY b ORDER BY b"
            ),
            Plan::Bucket,
        ),
        // Buckets that cross slice boundaries.
        (
            format!(
                "SELECT date_bin(INTERVAL '25 minutes', ts) AS b, count(*), sum(v) FROM {TABLE} \
                 WHERE {range} GROUP BY b ORDER BY b"
            ),
            Plan::Added,
        ),
        // A function that is not immutable can change its value between queries.
        (
            format!(
                "SELECT geohash(v - 50.0, v - 50.0, 3) AS g, count(*) FROM {TABLE} WHERE {range} \
                 GROUP BY g ORDER BY g"
            ),
            Plan::Original,
        ),
        // Grouping by the whole primary key aggregates each series in one partition, with
        // no partial stage to rewrite.
        (
            format!(
                "SELECT host, count(*), sum(v), min(v) FROM {TABLE} WHERE {range} \
                 GROUP BY host ORDER BY host"
            ),
            Plan::Original,
        ),
    ]
}

async fn insert(instance: &Arc<Instance>, rng: &mut Rng, rows: usize) {
    let values: Vec<String> = (0..rows)
        .map(|_| {
            let ts =
                BASE_MS + rng.below((SPAN_HOURS * HOUR_MS) as u64) as i64 / MINUTE_MS * MINUTE_MS;
            let host = rng.below(HOSTS);
            let v = rng.below(100);
            format!("({ts}, 'h{host}', {v})")
        })
        .collect();
    execute_sql(
        instance,
        &format!(
            "INSERT INTO {TABLE} (ts, host, v) VALUES {}",
            values.join(",")
        ),
    )
    .await;
}

async fn compare_window(instance: &Arc<Instance>, start: i64, end: i64, label: &str) {
    for (sql, _) in queries(start, end) {
        let expected = query(instance, &sql, false).await;
        let actual = query(instance, &sql, true).await;
        assert_eq!(expected, actual, "{label}: `{sql}`");
    }
}

async fn explain(instance: &Arc<Instance>, sql: &str) -> String {
    query(instance, &format!("EXPLAIN ANALYZE {sql}"), true).await
}

/// Runs `EXPLAIN ANALYZE` until a rewritten plan hits the cache, as entries are written
/// after a query returns.
async fn explain_cached(instance: &Arc<Instance>, sql: &str) -> String {
    let mut explain = String::new();
    for _ in 0..50 {
        explain = self::explain(instance, sql).await;
        if slice_hits(&explain) != Some(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    explain
}

/// Returns the sum of `slice_hits` over the plans of an `EXPLAIN ANALYZE`, or `None` if
/// no plan was rewritten.
fn slice_hits(explain: &str) -> Option<usize> {
    if !explain.contains("SliceAggExec") {
        return None;
    }
    Some(
        explain
            .match_indices("slice_hits: ")
            .map(|(i, m)| {
                explain[i + m.len()..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse::<usize>()
                    .unwrap()
            })
            .sum(),
    )
}

async fn create_table(instance: &Arc<Instance>, rng: &mut Rng) {
    execute_sql(
        instance,
        &format!(
            "CREATE TABLE {TABLE} (ts TIMESTAMP(3) TIME INDEX, host STRING, v DOUBLE, \
             PRIMARY KEY (host)) WITH ('compaction.type' = 'twcs', \
             'compaction.twcs.time_window' = '1h')"
        ),
    )
    .await;
    for _ in 0..3 {
        insert(instance, rng, 400).await;
        execute_sql(instance, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slice_agg_cache_matches_uncached_results() {
    let standalone = GreptimeDbStandaloneBuilder::new("slice_agg_cache")
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    let mut rng = Rng(0x5eed_cafe_f00d_d00d);
    create_table(&instance, &mut rng).await;

    // The rewritten shapes must hit the cache when repeated, otherwise the comparisons
    // below prove nothing. A plan with an added key is rewritten from its second run.
    // Neither bound falls on a slice boundary of the widths these plans get (15 minutes
    // to 1 hour), so each window has two partial slices.
    let (start, end) = (
        BASE_MS + 37 * MINUTE_MS,
        BASE_MS + 7 * HOUR_MS + 5 * MINUTE_MS,
    );
    for (sql, plan) in queries(start, end) {
        let first = explain(&instance, &sql).await;
        let rewritten_first = first.contains("SliceAggExec");
        assert_eq!(plan == Plan::Bucket, rewritten_first, "`{sql}`: {first}");
        query(&instance, &sql, true).await;
        let explain = explain_cached(&instance, &sql).await;
        let hits = slice_hits(&explain);
        if plan == Plan::Original {
            assert_eq!(None, hits, "`{sql}` should not be rewritten");
        } else {
            assert!(
                hits.is_some_and(|h| h > 0),
                "expected cache hits for `{sql}`, got {hits:?}"
            );
            assert_eq!(
                plan == Plan::Added,
                explain.contains("slice_start("),
                "`{sql}`"
            );
            // The window cuts the first and last slices short; they are never cached.
            assert!(
                explain.contains("slice_uncacheable: 2"),
                "`{sql}`: {explain}"
            );
        }
    }

    let range = format!("ts >= {start}::TIMESTAMP(3) AND ts < {end}::TIMESTAMP(3)");
    // Each pair differs only where the debug form of a plan could lose information, so a
    // DataFusion upgrade that drops it from the debug output fails here. The first query
    // of a pair is cached; the second must not be served from its entries.
    let pairs = [
        // Debug prints both lists as `List([h0, h1])`. `array_has` would be simplified into
        // an `IN` list of separate strings, so `array_position` keeps the list literal.
        (
            "count(*), sum(v)",
            "count(*), sum(v)",
            "array_position(['h0, h1'], host) IS NOT NULL",
            "array_position(['h0', 'h1'], host) IS NOT NULL",
        ),
        ("count(v)", "count(DISTINCT v)", "true", "true"),
        (
            "first_value(v ORDER BY v)",
            "first_value(v ORDER BY v DESC)",
            "true",
            "true",
        ),
        ("sum(v) FILTER (WHERE v > 50)", "sum(v)", "true", "true"),
    ];
    for (cached_aggs, other_aggs, cached_filter, other_filter) in pairs {
        let cached = format!("SELECT {cached_aggs} FROM {TABLE} WHERE {range} AND {cached_filter}");
        let other = format!("SELECT {other_aggs} FROM {TABLE} WHERE {range} AND {other_filter}");
        query(&instance, &cached, true).await;
        query(&instance, &cached, true).await;
        assert!(
            slice_hits(&explain_cached(&instance, &cached).await).is_some_and(|h| h > 0),
            "`{cached}`"
        );
        let expected = query(&instance, &other, false).await;
        assert_ne!(
            query(&instance, &cached, false).await,
            expected,
            "`{other}`"
        );
        assert_eq!(expected, query(&instance, &other, true).await, "`{other}`");
    }

    for sql in [
        // A partial aggregation with a soft limit stops once it has enough groups, so
        // adding the slice key would let it stop before it sees every distinct value.
        format!("SELECT DISTINCT v FROM {TABLE} WHERE {range} LIMIT 2"),
        // `first_value` orders rows inside the aggregate, so no sort below it blocks the
        // rewrite.
        format!(
            "SELECT v > 50 AS high, first_value(host ORDER BY random()) FROM {TABLE} \
             WHERE {range} GROUP BY high"
        ),
    ] {
        query(&instance, &sql, true).await;
        assert_eq!(None, slice_hits(&explain(&instance, &sql).await), "`{sql}`");
    }

    // Rounding the earliest representable timestamp down to the hour underflows.
    let earliest = i64::MIN + 808;
    let earliest = format!(
        "SELECT count(*) FROM {TABLE} WHERE ts >= CAST({earliest} AS TIMESTAMP(3)) \
         AND ts < CAST({} AS TIMESTAMP(3))",
        earliest + 2 * HOUR_MS
    );
    let expected = query(&instance, &earliest, false).await;
    for _ in 0..2 {
        assert_eq!(expected, query(&instance, &earliest, true).await);
    }

    // `date_bin` returns null for millisecond timestamps past 2262, whose nanoseconds
    // overflow i64.
    execute_sql(
        &instance,
        "CREATE TABLE slice_far (ts TIMESTAMP(3) TIME INDEX, v DOUBLE)",
    )
    .await;
    let year_2300 = 10_413_792_000_000i64;
    execute_sql(
        &instance,
        &format!(
            "INSERT INTO slice_far VALUES ({year_2300}, 1), ({}, 2)",
            year_2300 + HOUR_MS
        ),
    )
    .await;
    execute_sql(&instance, "ADMIN FLUSH_TABLE('slice_far')").await;
    let far = format!(
        "SELECT date_bin(INTERVAL '5 minutes', ts) AS b, count(*) FROM slice_far \
         WHERE ts >= {year_2300}::TIMESTAMP(3) AND ts < {}::TIMESTAMP(3) GROUP BY b",
        year_2300 + 2 * HOUR_MS
    );
    let expected = query(&instance, &far, false).await;
    for _ in 0..2 {
        assert_eq!(expected, query(&instance, &far, true).await);
    }

    // Metric engine tables are never rewritten.
    execute_sql(
        &instance,
        "CREATE TABLE slice_phy (ts TIMESTAMP(3) TIME INDEX, val DOUBLE) ENGINE = metric \
         WITH ('physical_metric_table' = '')",
    )
    .await;
    execute_sql(
        &instance,
        "CREATE TABLE slice_metric (ts TIMESTAMP(3) TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) \
         ENGINE = metric WITH ('on_physical_table' = 'slice_phy')",
    )
    .await;
    execute_sql(
        &instance,
        &format!(
            "INSERT INTO slice_metric (ts, val, host) VALUES ({BASE_MS}, 1, 'a'), ({}, 2, 'b')",
            BASE_MS + 5 * HOUR_MS
        ),
    )
    .await;
    execute_sql(&instance, "ADMIN FLUSH_TABLE('slice_phy')").await;
    let metric_sql = format!(
        "SELECT date_bin(INTERVAL '1 hour', ts) AS b, sum(val) FROM slice_metric \
         WHERE ts >= {start}::TIMESTAMP(3) AND ts < {end}::TIMESTAMP(3) GROUP BY b ORDER BY b"
    );
    query(&instance, &metric_sql, true).await;
    assert_eq!(None, slice_hits(&explain(&instance, &metric_sql).await));

    let mut added_columns = 0;
    let mut window = (BASE_MS, BASE_MS + 3 * HOUR_MS);
    for round in 0..40 {
        let op = rng.below(10);
        match op {
            0..=2 => {
                let rows = 1 + rng.below(50) as usize;
                insert(&instance, &mut rng, rows).await;
            }
            3 => {
                let ts = BASE_MS
                    + rng.below((SPAN_HOURS * HOUR_MS) as u64) as i64 / MINUTE_MS * MINUTE_MS;
                execute_sql(
                    &instance,
                    &format!(
                        "DELETE FROM {TABLE} WHERE host = 'h{}' AND ts = {ts}::TIMESTAMP(3)",
                        rng.below(HOSTS)
                    ),
                )
                .await;
            }
            4..=6 => {
                execute_sql(&instance, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
            }
            7 => {
                execute_sql(&instance, &format!("ADMIN COMPACT_TABLE('{TABLE}')")).await;
            }
            8 => {
                added_columns += 1;
                execute_sql(
                    &instance,
                    &format!("ALTER TABLE {TABLE} ADD COLUMN extra{added_columns} DOUBLE"),
                )
                .await;
            }
            _ => {
                if rng.below(4) == 0 {
                    execute_sql(&instance, &format!("TRUNCATE TABLE {TABLE}")).await;
                }
                insert(&instance, &mut rng, 200).await;
                execute_sql(&instance, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
            }
        }

        // A window that slides by five minutes shares its interior slices with the
        // previous one; a random window covers other slice combinations.
        window = (window.0 + 5 * MINUTE_MS, window.1 + 5 * MINUTE_MS);
        compare_window(
            &instance,
            window.0,
            window.1,
            &format!("round {round} op {op}"),
        )
        .await;
        let start = BASE_MS + rng.below(((SPAN_HOURS - 1) * HOUR_MS) as u64) as i64;
        let end = start + MINUTE_MS + rng.below((6 * HOUR_MS) as u64) as i64;
        compare_window(&instance, start, end, &format!("round {round} op {op}")).await;
    }
}

/// Runs cached queries over `[start, end)` from several tasks at once, dropping some of
/// them midway.
async fn query_concurrently(instance: &Arc<Instance>, rng: &mut Rng, start: i64, end: i64) {
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let instance = instance.clone();
            let mut rng = Rng(rng.next());
            tokio::spawn(async move {
                let queries = queries(start, end);
                for _ in 0..3 {
                    let (sql, _) = &queries[rng.below(queries.len() as u64) as usize];
                    let budget = Duration::from_micros(rng.below(20_000));
                    let _ = tokio::time::timeout(budget, query(&instance, sql, true)).await;
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

/// Queries of the same plans that run concurrently with each other and with writes,
/// flushes and compactions, some dropped midway, must leave only entries that agree with
/// an uncached run. The comparisons hold for every interleaving.
#[tokio::test(flavor = "multi_thread")]
async fn test_slice_agg_cache_under_concurrent_queries() {
    let standalone = GreptimeDbStandaloneBuilder::new("slice_agg_cache_concurrent")
        .build()
        .await;
    let instance = Arc::clone(standalone.fe_instance());
    let mut rng = Rng(0xc0c0_4e55_1ce5_0002);
    create_table(&instance, &mut rng).await;

    let (start, end) = (BASE_MS + 30 * MINUTE_MS, BASE_MS + 7 * HOUR_MS);
    for round in 0..10 {
        let writer = {
            let instance = instance.clone();
            let mut rng = Rng(rng.next());
            tokio::spawn(async move {
                for _ in 0..3 {
                    insert(&instance, &mut rng, 20).await;
                    let admin = ["FLUSH_TABLE", "COMPACT_TABLE"][rng.below(2) as usize];
                    execute_sql(&instance, &format!("ADMIN {admin}('{TABLE}')")).await;
                }
            })
        };
        query_concurrently(&instance, &mut rng, start, end).await;
        writer.await.unwrap();
        execute_sql(&instance, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
        // With the data unchanged, entries of dropped queries would match the
        // fingerprints of the comparisons below.
        for _ in 0..4 {
            query_concurrently(&instance, &mut rng, start, end).await;
        }
        compare_window(&instance, start, end, &format!("round {round}")).await;
    }
    let (sql, _) = &queries(start, end)[0];
    assert!(slice_hits(&explain_cached(&instance, sql).await).is_some_and(|h| h > 0));
}

/// In distributed mode the partial aggregation runs on the datanodes, which must see the
/// per-query option and cache each region's slices.
#[tokio::test(flavor = "multi_thread")]
async fn test_slice_agg_cache_in_distributed_mode() {
    let cluster = GreptimeDbClusterBuilder::new("slice_agg_cache_distributed")
        .await
        .build(false)
        .await;
    let instance = Arc::clone(cluster.fe_instance());
    execute_sql(
        &instance,
        &format!(
            "CREATE TABLE {TABLE} (ts TIMESTAMP(3) TIME INDEX, host STRING, v DOUBLE, \
             PRIMARY KEY (host)) PARTITION ON COLUMNS (host) (host < 'h2', host >= 'h2')"
        ),
    )
    .await;
    let mut rng = Rng(0x0d15_7a1b_5eed_0001);
    for _ in 0..2 {
        insert(&instance, &mut rng, 400).await;
        execute_sql(&instance, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
    }

    let (start, end) = (BASE_MS + 30 * MINUTE_MS, BASE_MS + 7 * HOUR_MS);
    let range = format!("ts >= {start}::TIMESTAMP(3) AND ts < {end}::TIMESTAMP(3)");
    for sql in [
        format!(
            "SELECT date_bin('10 minutes', ts) AS b, count(*), sum(v) FROM {TABLE} \
             WHERE {range} GROUP BY b ORDER BY b"
        ),
        format!(
            "SELECT v > 50 AS high, count(*), max(v) FROM {TABLE} WHERE {range} \
             GROUP BY high ORDER BY high"
        ),
    ] {
        let expected = query(&instance, &sql, false).await;
        for _ in 0..2 {
            assert_eq!(expected, query(&instance, &sql, true).await, "`{sql}`");
        }
        let explain = explain_cached(&instance, &sql).await;
        assert!(
            slice_hits(&explain).is_some_and(|h| h > 0),
            "expected cache hits for `{sql}`: {explain}"
        );
        assert_eq!(expected, query(&instance, &sql, true).await, "`{sql}`");
    }
}
