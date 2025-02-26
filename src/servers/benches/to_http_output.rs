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

use std::sync::Arc;
use std::time::Instant;

use arrow::array::StringArray;
use arrow_schema::{DataType, Field, Schema};
use common_recordbatch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datatypes::schema::SchemaRef;
use datatypes::vectors::StringVector;
use servers::http::HttpRecordsOutput;

fn mock_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(10);
    for i in 0..10 {
        fields.push(Field::new(format!("field{}", i), DataType::Utf8, true));
    }
    let arrow_schema = Arc::new(Schema::new(fields));
    Arc::new(arrow_schema.try_into().unwrap())
}

fn mock_input_record_batch(batch_size: usize, num_batches: usize) -> Vec<RecordBatch> {
    let mut result = Vec::with_capacity(num_batches);
    for _ in 0..num_batches {
        let mut vectors = Vec::with_capacity(10);
        for _ in 0..10 {
            let vector: StringVector = StringArray::from(
                (0..batch_size)
                    .map(|_| String::from("Copyright 2024 Greptime Team"))
                    .collect::<Vec<_>>(),
            )
            .into();
            vectors.push(Arc::new(vector) as _);
        }

        let schema = mock_schema();
        let record_batch = RecordBatch::new(schema, vectors).unwrap();
        result.push(record_batch);
    }

    result
}

fn bench_convert_record_batch_to_http_output(c: &mut Criterion) {
    let record_batches = mock_input_record_batch(4096, 100);
    c.bench_function("convert_record_batch_to_http_output", |b| {
        b.iter_custom(|iters| {
            let mut elapsed_sum = std::time::Duration::new(0, 0);
            for _ in 0..iters {
                let record_batches = record_batches.clone();
                let start = Instant::now();
                let _result = HttpRecordsOutput::try_new(mock_schema(), record_batches);
                elapsed_sum += start.elapsed();
            }
            elapsed_sum
        });
    });
}

#[cfg(not(windows))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(pprof::criterion::PProfProfiler::new(101, pprof::criterion::Output::Flamegraph(None)));
    targets = bench_convert_record_batch_to_http_output
}
#[cfg(windows)]
criterion_group!(benches, bench_convert_record_batch_to_http_output);
criterion_main!(benches);
