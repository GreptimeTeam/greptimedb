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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datatypes::vectors::json::array::JsonArray;

const ROWS: usize = 8192;
const MISSING_FIELDS: usize = 32;
const ITEMS_PER_ROW: usize = 4;

fn sorted_struct_type(mut fields: Vec<Field>) -> DataType {
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    DataType::Struct(Fields::from(fields))
}

fn make_struct_input(rows: usize) -> ArrayRef {
    Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("a_id", DataType::Int64, true)),
        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
    )]))
}

fn make_expect_with_same_type_missing(missing_fields: usize) -> DataType {
    let mut fields = vec![Field::new("a_id", DataType::Int64, true)];
    fields.extend(
        (0..missing_fields).map(|i| Field::new(format!("m_{i:03}"), DataType::Int64, true)),
    );
    sorted_struct_type(fields)
}

fn make_expect_with_mixed_missing(missing_fields: usize) -> DataType {
    let mut fields = vec![Field::new("a_id", DataType::Int64, true)];
    fields.extend((0..missing_fields).map(|i| {
        let data_type = match i % 4 {
            0 => DataType::Int64,
            1 => DataType::Utf8,
            2 => DataType::Boolean,
            _ => DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::Utf8, true),
            ])),
        };
        Field::new(format!("m_{i:03}"), data_type, true)
    }));
    sorted_struct_type(fields)
}

fn make_list_struct_input(rows: usize, items_per_row: usize) -> ArrayRef {
    let values_len = rows * items_per_row;
    let item_fields = Fields::from(vec![Field::new("a_id", DataType::Int64, true)]);
    let item_type = DataType::Struct(item_fields.clone());
    let values = Arc::new(StructArray::new(
        item_fields,
        vec![Arc::new(Int64Array::from_iter_values(0..values_len as i64)) as ArrayRef],
        None,
    ));
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(items_per_row, rows));
    let list = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new_list_field(item_type.clone(), true)),
            offsets,
            values,
            None,
        )
        .unwrap(),
    ) as ArrayRef;

    Arc::new(StructArray::from(vec![(
        Arc::new(Field::new_list(
            "items",
            Field::new_list_field(item_type, true),
            true,
        )),
        list,
    )]))
}

fn make_list_struct_expect(missing_fields: usize) -> DataType {
    let mut item_fields = vec![Field::new("a_id", DataType::Int64, true)];
    item_fields.extend(
        (0..missing_fields).map(|i| Field::new(format!("m_{i:03}"), DataType::Int64, true)),
    );
    item_fields.sort_by(|a, b| a.name().cmp(b.name()));

    DataType::Struct(Fields::from(vec![Field::new_list(
        "items",
        Field::new_list_field(DataType::Struct(Fields::from(item_fields)), true),
        true,
    )]))
}

fn assert_struct_dataset(input: &ArrayRef, expect: &DataType, rows: usize, fields: usize) {
    assert_eq!(input.len(), rows);
    let DataType::Struct(expect_fields) = expect else {
        panic!("expected struct type");
    };
    assert_eq!(expect_fields.len(), fields);
}

fn assert_list_struct_dataset(
    input: &ArrayRef,
    expect: &DataType,
    rows: usize,
    items_per_row: usize,
    missing_fields: usize,
) {
    assert_eq!(input.len(), rows);
    let input_struct = input.as_struct();
    let list = input_struct.column(0).as_list::<i32>();
    assert_eq!(list.len(), rows);
    assert_eq!(list.values().len(), rows * items_per_row);

    let DataType::Struct(expect_fields) = expect else {
        panic!("expected struct type");
    };
    assert_eq!(expect_fields.len(), 1);
    let DataType::List(item) = expect_fields[0].data_type() else {
        panic!("expected list field");
    };
    let DataType::Struct(item_fields) = item.data_type() else {
        panic!("expected struct list item");
    };
    assert_eq!(item_fields.len(), missing_fields + 1);
}

fn bench_json_array_align(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_array_align");

    let input = make_struct_input(ROWS);
    let expect = make_expect_with_same_type_missing(MISSING_FIELDS);
    assert_struct_dataset(&input, &expect, ROWS, MISSING_FIELDS + 1);
    group.bench_with_input(
        BenchmarkId::new(
            "missing_same_type",
            format!("rows_{ROWS}_missing_{MISSING_FIELDS}"),
        ),
        &(&input, &expect),
        |b, (input, expect)| {
            b.iter(|| {
                black_box(
                    JsonArray::from(black_box(*input))
                        .try_align(black_box(*expect))
                        .unwrap(),
                )
            })
        },
    );

    let input = make_struct_input(ROWS);
    let expect = make_expect_with_mixed_missing(MISSING_FIELDS);
    assert_struct_dataset(&input, &expect, ROWS, MISSING_FIELDS + 1);
    group.bench_with_input(
        BenchmarkId::new(
            "missing_mixed_type",
            format!("rows_{ROWS}_missing_{MISSING_FIELDS}"),
        ),
        &(&input, &expect),
        |b, (input, expect)| {
            b.iter(|| {
                black_box(
                    JsonArray::from(black_box(*input))
                        .try_align(black_box(*expect))
                        .unwrap(),
                )
            })
        },
    );

    let input = make_list_struct_input(ROWS, ITEMS_PER_ROW);
    let expect = make_list_struct_expect(MISSING_FIELDS);
    assert_list_struct_dataset(&input, &expect, ROWS, ITEMS_PER_ROW, MISSING_FIELDS);
    group.bench_with_input(
        BenchmarkId::new(
            "list_struct_missing",
            format!("rows_{ROWS}_items_{ITEMS_PER_ROW}_missing_{MISSING_FIELDS}"),
        ),
        &(&input, &expect),
        |b, (input, expect)| {
            b.iter(|| {
                black_box(
                    JsonArray::from(black_box(*input))
                        .try_align(black_box(*expect))
                        .unwrap(),
                )
            })
        },
    );

    let input = make_struct_input(ROWS);
    let expect = input.data_type().clone();
    assert_struct_dataset(&input, &expect, ROWS, 1);
    group.bench_with_input(
        BenchmarkId::new("already_aligned", format!("rows_{ROWS}")),
        &(&input, &expect),
        |b, (input, expect)| {
            b.iter(|| {
                black_box(
                    JsonArray::from(black_box(*input))
                        .try_align(black_box(*expect))
                        .unwrap(),
                )
            })
        },
    );

    group.finish();
}

criterion_group!(benches, bench_json_array_align);
criterion_main!(benches);
