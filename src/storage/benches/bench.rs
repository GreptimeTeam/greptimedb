use std::{
    sync::{atomic::*, Arc},
    thread,
    thread::JoinHandle,
};

use criterion::*;
use datatypes::prelude::*;
use datatypes::vectors::{Int64VectorBuilder, UInt64VectorBuilder};
use rand::prelude::*;
use storage::memtable::IterContext;
use storage::test_util::descriptor_util::RegionDescBuilder;
use storage::{
    memtable::{
        BatchIterator, DefaultMemtableBuilder, KeyValues, Memtable, MemtableBuilderRef,
        MemtableRef, MemtableSchema,
    },
    metadata::RegionMetadata,
};
use store_api::storage::{SequenceNumber, ValueType};

struct TestContext {
    schema: MemtableSchema,
    memtable: MemtableRef,
}

fn schema_for_test() -> MemtableSchema {
    // Just build a region desc and use its columns_row_key metadata.
    let desc = RegionDescBuilder::new("bench")
        .push_value_column(("v1", LogicalTypeId::UInt64, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();
    MemtableSchema::new(metadata.columns_row_key)
}

struct MemtableTester {
    schema: MemtableSchema,
    builders: Vec<MemtableBuilderRef>,
}

impl Default for MemtableTester {
    fn default() -> MemtableTester {
        MemtableTester::new()
    }
}

impl MemtableTester {
    fn schema(&self) -> MemtableSchema {
        self.schema.clone()
    }
    fn new() -> MemtableTester {
        let schema = schema_for_test();
        let builders = vec![Arc::new(DefaultMemtableBuilder {}) as _];
        MemtableTester { schema, builders }
    }

    fn new_memtables(&self) -> Vec<MemtableRef> {
        self.builders
            .iter()
            .map(|b| b.build(self.schema.clone()))
            .collect()
    }

    fn prepare_data(&self) -> Vec<Arc<dyn Memtable>> {
        let mut vec_contest = Vec::<MemtableRef>::new();
        for memtable in self.new_memtables() {
            vec_contest.push(memtable);
        }
        vec_contest
    }

    fn run_testcase<F>(&self, testcase: F)
    where
        F: Fn(TestContext),
    {
        for memtable in self.new_memtables() {
            let test_ctx = TestContext {
                schema: self.schema.clone(),
                memtable,
            };
            testcase(test_ctx);
        }
    }
}

static PACKETS_SERVED: AtomicU64 = AtomicU64::new(0);
fn get_sequence() -> SequenceNumber {
    PACKETS_SERVED.fetch_add(1, Ordering::SeqCst);
    PACKETS_SERVED.load(Ordering::SeqCst)
}

fn random_kv(rng: &mut ThreadRng) -> (i64, u64, Option<u64>) {
    let key0: i64 = rng.gen_range(0..10000);
    let key1 = rng.gen::<u64>();
    let value = Some(rng.gen::<u64>());
    return (key0, key1, value);
}

fn random_kvs(len: usize) -> (Vec<(i64, u64)>, Vec<Option<u64>>) {
    let mut keys: Vec<(i64, u64)> = Vec::with_capacity(len);
    let mut values: Vec<Option<u64>> = Vec::with_capacity(len);
    for i in 0..len {
        let mut rng = rand::thread_rng();
        let (key0, key1, value) = random_kv(&mut rng);
        keys.push((key0, key1));
        values.push(value);
    }
    (keys, values)
}

fn kvs_with_index(
    sequence: SequenceNumber,
    value_type: ValueType,
    start_index_in_batch: usize,
    keys: &[(i64, u64)],
    values: &[Option<u64>],
) -> KeyValues {
    let mut key_builders = (
        Int64VectorBuilder::with_capacity(keys.len()),
        UInt64VectorBuilder::with_capacity(keys.len()),
    );
    for key in keys {
        key_builders.0.push(Some(key.0));
        key_builders.1.push(Some(key.1));
    }
    let row_keys = vec![
        Arc::new(key_builders.0.finish()) as _,
        Arc::new(key_builders.1.finish()) as _,
    ];
    let mut value_builder = UInt64VectorBuilder::with_capacity(values.len());
    for value in values {
        value_builder.push(*value);
    }
    let row_values = vec![Arc::new(value_builder.finish()) as _];
    let kvs = KeyValues {
        sequence,
        value_type,
        start_index_in_batch,
        keys: row_keys,
        values: row_values,
    };
    kvs
}

fn generator_kvs(
    sequence: SequenceNumber,
    value_type: ValueType,
    keys: &[(i64, u64)],
    values: &[Option<u64>],
) -> KeyValues {
    kvs_with_index(sequence, value_type, 0, keys, values)
}

fn write_kvs(
    memtable: &dyn Memtable,
    sequence: SequenceNumber,
    value_type: ValueType,
    keys: &[(i64, u64)],
    values: &[Option<u64>],
) {
    let kvs = generator_kvs(sequence, value_type, &keys, &values);
    memtable.write(&kvs).unwrap()
}

fn write_iter_memtable(memtable: &dyn Memtable, kv_size: usize) {
    let (keys, values) = random_kvs(kv_size);
    write_kvs(memtable, get_sequence(), ValueType::Put, &keys, &values);
}

// write
fn write_memtable(ctx: &TestContext, kv_size: usize) {
    write_iter_memtable(&*ctx.memtable, kv_size);
}

fn read_iter_memtable_case(iter: &mut dyn BatchIterator) {
    let mut _index = 0;
    while let Some(batch) = iter.next().unwrap() {
        let row_num = batch.keys[0].len();
        for i in 0..row_num {
            let (_k0, _k1) = (batch.keys[0].get(i), batch.keys[1].get(i));
            let _sequence = batch.sequences.get_data(i).unwrap();
            let _value_type = batch.value_types.get_data(i).unwrap();
            let _v = batch.values[0].get(i);
            _index += 1;
        }
    }
}
// read
fn read_memtable(mem: &MemtableRef, batch_size: usize) {
    let iter_ctx = IterContext::default();
    let mut iter = mem.iter(iter_ctx).unwrap();
    read_iter_memtable_case(&mut *iter);
}

fn read_ctx_memtable(ctx: &TestContext, batch_size: usize) {
    let iter_ctx = IterContext::default();
    let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
    read_iter_memtable_case(&mut *iter);
}

fn memtable_round(ctx: &TestContext, case: &(bool, usize, usize)) {
    if case.0 {
        read_ctx_memtable(&ctx, case.1);
    } else {
        write_memtable(&ctx, case.2);
    }
}

fn bench_read_write_ctx_frac(b: &mut Bencher<'_>, frac: &usize) {
    let frac = *frac;
    let mem_tester = MemtableTester::default();
    let mut vec_ctx = Vec::<Arc<TestContext>>::new();
    for memtable in mem_tester.new_memtables() {
        let ctx = TestContext {
            schema: mem_tester.schema.clone(),
            memtable,
        };
        vec_ctx.push(Arc::new(ctx));
    }
    let arc_vec_ctx = Arc::new(vec_ctx);
    let child1 = arc_vec_ctx.clone();
    let child2 = arc_vec_ctx.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();

    let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        while !thread_stop.load(Ordering::SeqCst) {
            let f = rng.gen_range(1..=11);
            let case = (f < frac, black_box(256), black_box(100000));
            for ctx in child1.iter() {
                memtable_round(&ctx, &case);
            }
        }
    });

    let mut rng = rand::thread_rng();
    b.iter_batched_ref(
        || {
            let f = rng.gen_range(1..=11);
            (f < frac, black_box(256), black_box(100000))
        },
        |case| {
            for ctx in child2.iter() {
                memtable_round(&ctx, &case);
            }
        },
        BatchSize::SmallInput,
    );

    stop.store(true, Ordering::SeqCst);
    handle.join().unwrap();
}
// Control the read/write ratio
fn bench_read_write_memtable_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_read_write_ratio");
    for i in 1..=10 {
        group.bench_with_input(
            BenchmarkId::from_parameter(i),
            &i,
            bench_read_write_ctx_frac,
        );
    }
    group.finish();
}

fn bench_write(c: &mut Criterion) {
    c.bench_function("write", |b| {
        b.iter(|| {
            let mut handles = Vec::<JoinHandle<()>>::new();
            for _i in 0..10 {
                let handle = thread::spawn(move || {
                    let memtable_tester = MemtableTester::default();
                    memtable_tester.run_testcase(|ctx| write_memtable(&ctx, black_box(100000)));
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

fn bench_read(c: &mut Criterion) {
    let memtable_tester = MemtableTester::default();

    let vec_ctx = memtable_tester.prepare_data();
    let arc_vec_ctx = Arc::new(vec_ctx);

    c.bench_function("read", |b| {
        b.iter(|| {
            let mut handles = Vec::<JoinHandle<()>>::new();

            for i in 0..10 {
                let child = arc_vec_ctx.clone();
                let handle = thread::spawn(move || {
                    for mem in child.iter() {
                        read_memtable(&mem, black_box(256));
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

// fn bench_read_write_memtable(c: &mut Criterion) {
//     c.bench_function("read_write_memtable", |b| {
//         b.iter(|| {
//             let memtable_tester = MemtableTester::default();
//             let mut vec_memtable=Vec::<MemtableRef>::new();

//             for memtable in memtable_tester.new_memtables(){
//                 let child_memtable=memtable.clone();
//                 let test_ctx = TestContext {
//                     schema: memtable_tester.schema(),
//                     memtable,
//                 };
//                 write_memtable(&test_ctx,black_box(100000));
//                 vec_memtable.push(child_memtable);
//             }

//             let arc_vec_memtable=Arc::new(vec_memtable);

//             let mut handles = Vec::<JoinHandle<()>>::new();
//             for _i in 0..10 {
//                 let child = arc_vec_memtable.clone();
//                 let handle = thread::spawn(move || {
//                     for mem in child.iter() {
//                         read_memtable(&mem, black_box(256));
//                     }
//                 });
//                 handles.push(handle);
//             }

//             for handle in handles {
//                 handle.join().unwrap();
//             }
//         });
//     });
// }

criterion_group!(
    benches,
    bench_write,
    bench_read,
    // bench_read_write_memtable,
    bench_read_write_memtable_ratio,
);
criterion_main!(benches);
