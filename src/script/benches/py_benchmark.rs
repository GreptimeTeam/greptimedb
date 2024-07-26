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

use std::collections::HashMap;
use std::sync::Arc;

use catalog::memory::MemoryCatalogManager;
use common_catalog::consts::NUMBERS_TABLE_ID;
use common_query::OutputData;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::Future;
use once_cell::sync::{Lazy, OnceCell};
use query::QueryEngineFactory;
use rayon::ThreadPool;
use script::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use script::python::{PyEngine, PyScript};
use table::table::numbers::NumbersTable;
use tokio::runtime::Runtime;

static SCRIPT_ENGINE: Lazy<PyEngine> = Lazy::new(sample_script_engine);
static LOCAL_RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
fn get_local_runtime() -> std::thread::Result<&'static Runtime> {
    let rt = LOCAL_RUNTIME
        .get_or_try_init(|| tokio::runtime::Runtime::new().map_err(|e| Box::new(e) as _))?;
    Ok(rt)
}
/// a terrible hack to call async from sync by:
/// TODO(discord9): find a better way
/// 1. spawn a new thread
/// 2. create a new runtime in new thread and call `block_on` on it
pub fn block_on_async<T, F>(f: F) -> std::thread::Result<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = get_local_runtime()?;

    std::thread::spawn(move || rt.block_on(f)).join()
}

pub(crate) fn sample_script_engine() -> PyEngine {
    let catalog_manager =
        MemoryCatalogManager::new_with_table(NumbersTable::table(NUMBERS_TABLE_ID));
    let query_engine =
        QueryEngineFactory::new(catalog_manager, None, None, None, None, false).query_engine();

    PyEngine::new(query_engine.clone())
}

async fn compile_script(script: &str) -> PyScript {
    SCRIPT_ENGINE
        .compile(script, CompileContext::default())
        .await
        .unwrap()
}
async fn run_compiled(script: &PyScript) {
    let output = script
        .execute(HashMap::default(), EvalContext::default())
        .await
        .unwrap();
    let _res = match output.data {
        OutputData::Stream(s) => common_recordbatch::util::collect_batches(s).await.unwrap(),
        OutputData::RecordBatches(rbs) => rbs,
        _ => unreachable!(),
    };
}

async fn fibonacci(n: u64, backend: &str) {
    let source = format!(
        r#"
@copr(returns=["value"], backend="{backend}")
def entry() -> vector[i64]:
    def fibonacci(n):
        if n <2:
            return 1
        else:
            return fibonacci(n-1) + fibonacci(n-2)
    return fibonacci({n})
"#
    );
    let compiled = compile_script(&source).await;
    for _ in 0..10 {
        run_compiled(&compiled).await;
    }
}

/// TODO(discord9): use a better way to benchmark in parallel
async fn parallel_fibonacci(n: u64, backend: &str, pool: &ThreadPool) {
    let source = format!(
        r#"
@copr(returns=["value"], backend="{backend}")
def entry() -> vector[i64]:
    def fibonacci(n):
        if n <2:
            return 1
        else:
            return fibonacci(n-1) + fibonacci(n-2)
    return fibonacci({n})
"#
    );
    let source = Arc::new(source);
    // execute the script in parallel for every thread in the pool
    let _ = pool.broadcast(|_| {
        let source = source.clone();
        let rt = get_local_runtime().unwrap();
        rt.block_on(async move {
            let compiled = compile_script(&source).await;
            for _ in 0..10 {
                run_compiled(&compiled).await;
            }
        });
    });
}

async fn loop_1_million(backend: &str) {
    let source = format!(
        r#"
@copr(returns=["value"], backend="{backend}")
def entry() -> vector[i64]:
    for i in range(1000000):
        pass
    return 1
"#
    );
    let compiled = compile_script(&source).await;
    for _ in 0..10 {
        run_compiled(&compiled).await;
    }
}

async fn api_heavy(backend: &str) {
    let source = format!(
        r#"
from greptime import vector
@copr(args=["number"], sql="select number from numbers", returns=["value"], backend="{backend}")
def entry(number) -> vector[i64]:
    for i in range(1000):
        n2 = number + number
        n_mul = n2 * n2
        n_mask = n_mul[n_mul>2]
    return 1
"#
    );
    let compiled = compile_script(&source).await;
    for _ in 0..10 {
        run_compiled(&compiled).await;
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    // TODO(discord9): Prime Number,
    // and database-local computation/remote download python script comparison
    // which require a local mock library
    // TODO(discord9): revisit once mock library is ready

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(16)
        .build()
        .unwrap();

    let _ = c
        .bench_function("fib 20 rspy", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| fibonacci(black_box(20), "rspy"))
        })
        .bench_function("fib 20 pyo3", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| fibonacci(black_box(20), "pyo3"))
        })
        .bench_function("par fib 20 rspy", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| parallel_fibonacci(black_box(20), "rspy", &pool))
        })
        .bench_function("par fib 20 pyo3", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| parallel_fibonacci(black_box(20), "pyo3", &pool))
        })
        .bench_function("loop 1M rspy", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| loop_1_million(black_box("rspy")))
        })
        .bench_function("loop 1M pyo3", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| loop_1_million(black_box("pyo3")))
        })
        .bench_function("api heavy rspy", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| api_heavy(black_box("rspy")))
        })
        .bench_function("api heavy pyo3", |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| api_heavy(black_box("pyo3")))
        });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
