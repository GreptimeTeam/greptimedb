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
use std::thread;

use common_query::Output;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::Future;
use once_cell::sync::{Lazy, OnceCell};
use script::engine::{CompileContext, EvalContext, Script, ScriptEngine};
static SCRIPT_ENGINE: Lazy<PyEngine> = Lazy::new(sample_script_engine);

use catalog::local::{MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{CatalogList, CatalogProvider, SchemaProvider};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use query::QueryEngineFactory;
use script::python::PyEngine;
use table::table::numbers::NumbersTable;
use tokio::runtime::Runtime;

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
    let catalog_list = catalog::local::new_memory_catalog_list().unwrap();

    let default_schema = Arc::new(MemorySchemaProvider::new());
    default_schema
        .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
        .unwrap();
    let default_catalog = Arc::new(MemoryCatalogProvider::new());
    default_catalog
        .register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)
        .unwrap();
    catalog_list
        .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog)
        .unwrap();

    let factory = QueryEngineFactory::new(catalog_list);
    let query_engine = factory.query_engine();

    PyEngine::new(query_engine.clone())
}
async fn run_script(script: &str) {
    let script = script;
    let script = SCRIPT_ENGINE
        .compile(script, CompileContext::default())
        .await
        .unwrap();
    let output = script
        .execute(HashMap::default(), EvalContext::default())
        .await
        .unwrap();
    let _res = match output {
        Output::Stream(s) => common_recordbatch::util::collect_batches(s).await.unwrap(),
        Output::RecordBatches(rbs) => rbs,
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
    run_script(&source).await;
}

async fn parallel_fibonacci(n: u64, backend: &str, par: usize) {
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
    let mut results = Vec::with_capacity(par);
    for _ in 0..par {
        let source = source.clone();
        // spawn new thread for parallel(To test GIL for CPython)
        results.push(thread::spawn(move || {
            block_on_async(async move {
                run_script(&source).await;
            })
        }));
    }
    for i in results {
        i.join().unwrap().unwrap();
    }
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
    run_script(&source).await;
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
    run_script(&source).await;
}

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: Prime Number, api heavy computation
    // and database-local computation/remote download python script comparison

    c.bench_function("fib 20 rspy", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| fibonacci(black_box(20), "rspy"))
    });
    c.bench_function("fib 20 pyo3", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| fibonacci(black_box(20), "pyo3"))
    });

    c.bench_function("par fib 20 rspy", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| parallel_fibonacci(black_box(20), "rspy", 8))
    });
    c.bench_function("par fib 20 pyo3", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| parallel_fibonacci(black_box(20), "pyo3", 8))
    });

    c.bench_function("loop 1M rspy", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| loop_1_million(black_box("rspy")))
    });
    c.bench_function("loop 1M pyo3", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| loop_1_million(black_box("pyo3")))
    });
    c.bench_function("api heavy rspy", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| api_heavy(black_box("rspy")))
    });
    c.bench_function("api heavy pyo3", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| api_heavy(black_box("pyo3")))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
