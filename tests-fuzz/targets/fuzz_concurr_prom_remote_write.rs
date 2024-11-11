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

//! Fuzz testing for auto create table and potentially concurrent alter table caused by auto create table.
//!
//! Example Command to run this fuzz test:
//! ```bash
//! GT_MYSQL_ADDR=localhost:4002 GT_HTTP_ADDR=localhost:4000 cargo fuzz run fuzz_concurr_prom_remote_write --fuzz-dir tests-fuzz -D -s none
//! ```
#![no_main]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::Hasher;

use common_error::ext::BoxedError;
use common_telemetry::info;
use greptime_proto::prometheus::remote::{self, Label, Sample, TimeSeries};
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use prost::Message;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use servers::prom_store::snappy_compress;
use snafu::ResultExt;
use sqlx::{MySql, Pool};
use tests_fuzz::error::{self, ExternalSnafu, Result, SendPromRemoteRequestSnafu};
use tests_fuzz::fake::{
    random_capitalize_map, uppercase_and_keyword_backtick_map, MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::Random;
use tests_fuzz::ir::Ident;
use tests_fuzz::utils::{init_greptime_connections_via_env, Connections};
use tokio::task::JoinSet;
use tokio::time::Instant;

struct FuzzContext {
    greptime: Pool<MySql>,
}

impl FuzzContext {
    async fn close(self) {
        self.greptime.close().await;
    }
}

#[derive(Copy, Clone, Debug)]
struct FuzzInput {
    seed: u64,
    concurr_writes: usize,
    tables: usize,
    labels: usize,
    always_choose_old_table: bool,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        Ok(FuzzInput {
            concurr_writes: rng.gen_range(2..256),
            // one table is not enough for this concurrent test
            tables: 2,
            labels: rng.gen_range(0..4),
            seed,
            always_choose_old_table: true,
        })
    }
}

/// Generate prometheus remote write requests
fn generate_prom_metrics_write_reqs<R: Rng + 'static>(
    input: FuzzInput,
    rng: &mut R,
    timestamp: &mut i64,
    table_used_col_names: &mut BTreeMap<String, BTreeSet<u64>>,
) -> Result<remote::WriteRequest> {
    let name_generator = Box::new(MappedGenerator::new(
        WordGenerator,
        random_capitalize_map::<R>,
    ));

    // advance timestamp by a random number
    // *timestamp += rng.gen_range(0..256);

    let mut timeseries = Vec::new();

    for _ in 0..input.tables {
        let len = table_used_col_names.len();
        // more tables means more chance to select an existing table
        let table_name = {
            let prob = 1.0 - (-(len as f64)).exp();
            let choose_old = rng.gen_bool(prob) || input.always_choose_old_table;
            let use_old = choose_old && table_used_col_names.len() >= input.tables;

            if use_old {
                // randomly select a table name from the table_used_col_names
                let idx = rng.gen_range(0..table_used_col_names.len());
                let table_name = table_used_col_names.keys().nth(idx).unwrap().clone();

                uppercase_and_keyword_backtick_map(rng, Ident::new(table_name))
            } else {
                let name = name_generator.gen(rng);
                uppercase_and_keyword_backtick_map(rng, name)
            }
        };
        // TODO: support more metric types&labels
        /*let mf_type = match rng.gen_range(0..3) {
            0 => MetricType::COUNTER,
            1 => MetricType::GAUGE,
            2 => MetricType::HISTOGRAM,
            _ => MetricType::SUMMARY,
        };*/

        let mut random_labels: Vec<Label> = (0..input.labels)
            .map(|_| {
                let label_name = name_generator.gen(rng);
                let label_value = name_generator.gen(rng);
                Label {
                    name: label_name.value,
                    value: label_value.value,
                }
            })
            .chain(Some(Label {
                name: "__name__".to_string(),
                value: table_name.value.clone(),
            }))
            .collect();

        // have label names sorted in lexicographical order.
        random_labels.sort_by(|a, b| a.name.cmp(&b.name));

        let hashed = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            random_labels
                .iter()
                .map(|l| {
                    hasher.write(l.name.as_bytes());
                    hasher.write(l.value.as_bytes());
                })
                .count();
            hasher.finish()
        };
        let all_col_ids = table_used_col_names
            .entry(table_name.value.clone())
            .or_default();
        // if this table already have this column, skip
        if all_col_ids.contains(&hashed) {
            continue;
        } else {
            all_col_ids.insert(hashed);
        }

        timeseries.push(TimeSeries {
            labels: random_labels,
            samples: vec![Sample {
                value: rng.gen_range(0.0..100.0),
                timestamp: *timestamp,
            }],
            exemplars: vec![],
        })
    }

    Ok(remote::WriteRequest {
        timeseries,
        metadata: vec![],
    })
}

async fn execute_insert(ctx: FuzzContext, input: FuzzInput, http: String) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    let sql = "CREATE DATABASE IF NOT EXISTS greptime_metrics;";
    let result = sqlx::query(sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Create database: greptime_metrics\n\nResult: {result:?}\n\n");

    let client = reqwest::Client::new();
    let endpoint = format!("http://{http}/v1/prometheus/write?db=greptime_metrics");

    let mut table_used_col_labels = Default::default();
    let mut timestamp = 42;
    let mut concurrent_writes = Vec::new();
    for _ in 0..input.concurr_writes {
        let write_req = generate_prom_metrics_write_reqs(
            input,
            &mut rng,
            &mut timestamp,
            &mut table_used_col_labels,
        )?;
        concurrent_writes.push(write_req);
    }

    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    for write_req in concurrent_writes {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let _task = join_set.spawn(async move {
            let body = snappy_compress(&write_req.encode_to_vec())
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            client
                .post(endpoint.as_str())
                .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                .header("Content-Type", "application/x-protobuf")
                .body(body)
                .send()
                .await
                .context(SendPromRemoteRequestSnafu)?;
            Ok(())
        });
    }
    join_and_err(join_set).await?;
    info!("All write requests are sent, wait for alter procdure to complete...\n\n");
    let check_done_sql = "SELECT * FROM INFORMATION_SCHEMA.PROCEDURE_INFO WHERE status!='Done';";

    loop {
        let res = sqlx::query(check_done_sql)
            .fetch_all(&ctx.greptime)
            .await
            .unwrap();
        // all alter procedures are done
        if res.is_empty() {
            break;
        } else {
            info!("Waiting for alter procedures to complete...\n\n");
            info!("Not done procedures: {res:?}\n\n");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    info!("All alter procedures are done, check if all tables are oked...\n\n");

    struct Req {
        table_name: String,
        last_time: Instant,
        retry: usize,
    }

    let mut req_pool = VecDeque::<Req>::from_iter(table_used_col_labels.keys().map(|k| Req {
        table_name: k.to_string(),
        last_time: Instant::now(),
        retry: 0,
    }));

    while let Some(Req {
        table_name,
        last_time,
        retry,
    }) = req_pool.pop_front()
    {
        if last_time.elapsed().as_millis() < 1000 {
            req_pool.push_back(Req {
                table_name,
                last_time,
                retry,
            });
            continue;
        }

        let select_sql = format!(
            "SELECT * FROM greptime_metrics.{} LIMIT 1",
            Ident::with_quote('`', &table_name)
        );
        let conn = ctx.greptime.clone();
        if conn.is_closed() {
            common_telemetry::error!("Connection is closed");
            return Err(error::ConnAbortedSnafu.build());
        }
        match sqlx::query(&select_sql).fetch_all(&conn).await {
            Ok(_) => {}
            Err(err) => {
                // the alter table is not yet finished, accept as normal
                if err.to_string().contains("No field named greptime_metrics") {
                    if retry >= 5 {
                        common_telemetry::error!("Retry limit reached for table: {}", table_name);
                        // TODO(discord9): make this hard error later
                        // too many retry, remove this table from the pool
                        continue;
                    }
                    req_pool.push_back(Req {
                        table_name: table_name.clone(),
                        last_time: Instant::now(),
                        retry: retry + 1,
                    });
                    continue;
                } else {
                    Err(err).context(error::ExecuteQuerySnafu { sql: &select_sql })?
                }
            }
        }
    }

    // Cleans up
    let sql = "DROP DATABASE greptime_metrics".to_string();
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Drop database: greptime_metrics\n\nResult: {result:?}\n\n");
    ctx.close().await;

    Ok(())
}

/// Join all tasks and return first error if exist
async fn join_and_err(pool: JoinSet<Result<()>>) -> Result<()> {
    let output = pool.join_all().await;
    for (idx, err) in output.iter().enumerate().filter_map(|(i, u)| match u {
        Ok(_) => None,
        Err(e) => Some((i, e)),
    }) {
        // found first error and return
        // but also print all errors for debugging
        common_telemetry::error!("Error at parallel task {}: {err:?}", idx);
    }

    // return first error if exist
    if let Some(err) = output.into_iter().filter_map(|u| u.err()).next() {
        if let error::Error::ConnAborted { .. } = err {
            Err(err)
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let Connections { mysql, http } = init_greptime_connections_via_env().await;
        if let Some(mysql) = &mysql {
            if mysql.is_closed() {
                mysql.begin().await.unwrap();
            }
        }
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        let http = http.expect("http address must be set");
        execute_insert(ctx, input, http)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
