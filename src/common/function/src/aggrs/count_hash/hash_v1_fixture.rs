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

//! Frozen DataFusion 53 hash-v1 fixture verification.

use std::fs;
use std::sync::Arc;

use ahash::RandomState;
use datafusion_common::ScalarValue;
use datafusion_common::error::Result;
use datafusion_common::hash_utils::{
    RandomState as FoldHashState, create_hashes as foldhash_create_hashes,
};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_expr::{Accumulator, EmitTo, GroupsAccumulator};
use datatypes::arrow::array::*;
use datatypes::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datatypes::arrow::datatypes::{i256, *};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    CountHashAccumulator, CountHashGroupAccumulator, RANDOM_SEED_0, RANDOM_SEED_1, RANDOM_SEED_2,
    RANDOM_SEED_3, hash_v1,
};

const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/count_hash_hash_v1.json"
);
const FLAT_CASE_IDS: &[&str] = &[
    "null",
    "boolean",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16-bits",
    "float32-bits",
    "float64-bits",
    "decimal32",
    "decimal64",
    "decimal128",
    "decimal256",
    "date32",
    "date64",
    "time32-second",
    "time32-millisecond",
    "time64-microsecond",
    "time64-nanosecond",
    "timestamp-second",
    "timestamp-second-utc",
    "timestamp-millisecond",
    "timestamp-millisecond-utc",
    "timestamp-microsecond",
    "timestamp-microsecond-utc",
    "timestamp-nanosecond",
    "timestamp-nanosecond-utc",
    "duration-second",
    "duration-millisecond",
    "duration-microsecond",
    "duration-nanosecond",
    "interval-year-month",
    "interval-day-time",
    "interval-month-day-nano",
    "utf8",
    "large-utf8",
    "utf8-view",
    "binary",
    "large-binary",
    "binary-view",
    "fixed-size-binary",
    "dictionary-int8",
    "dictionary-int16",
    "dictionary-int32",
    "dictionary-int64",
    "dictionary-uint8",
    "dictionary-uint16",
    "dictionary-uint32",
    "dictionary-uint64",
];
const NESTED_CASE_IDS: &[&str] = &[
    "struct",
    "struct-sliced",
    "list",
    "list-sliced",
    "large-list",
    "nested-list",
    "list-view",
    "large-list-view",
    "fixed-size-list",
    "fixed-size-list-empty",
    "fixed-size-list-sliced",
    "map",
    "map-sliced",
    "union-dense",
    "union-dense-sliced",
    "union-sparse",
    "union-sparse-sliced",
    "ree-int16",
    "ree-int16-sliced",
    "ree-int32",
    "ree-int32-sliced",
    "ree-int64",
    "ree-int64-sliced",
];

#[derive(Debug, Deserialize, PartialEq)]
struct Fixture {
    format: String,
    metadata: Metadata,
    int64: Int64Case,
    flat_cases: Vec<FlatCase>,
    nested_cases: Vec<FlatCase>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Metadata {
    greptime_commit: String,
    datafusion_commit: String,
    hash_utils_blob: String,
    ahash_version: String,
    arrow_version: String,
    seeds_hex: Vec<String>,
    target_endianness: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Int64Case {
    logical_inputs: Vec<Option<i64>>,
    slot_hashes: Vec<String>,
    null_handling: NullHandling,
    scalar_partial_states: Vec<ScalarPartialState>,
    grouped: GroupedCase,
}

#[derive(Debug, Deserialize, PartialEq)]
struct NullHandling {
    create_hashes: String,
    scalar_accumulator: String,
    grouped_accumulator: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct ScalarPartialState {
    logical_inputs: Vec<i64>,
    sorted_hashes: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct GroupedCase {
    group_indices: Vec<usize>,
    filter: Vec<Option<bool>>,
    total_num_groups: usize,
    sorted_group_states: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct FlatCase {
    id: String,
    arrow_type: String,
    logical_inputs: Value,
    slot_hashes: Vec<String>,
}

fn random_state() -> RandomState {
    RandomState::with_seeds(RANDOM_SEED_0, RANDOM_SEED_1, RANDOM_SEED_2, RANDOM_SEED_3)
}

fn int64_array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn hashes(values: ArrayRef) -> Result<Vec<String>> {
    let mut output = vec![0; values.len()];
    hash_v1::create_hashes(&[values], &random_state(), &mut output)?;
    Ok(output.into_iter().map(|hash| hash.to_string()).collect())
}

fn flat_case(
    id: &str,
    arrow_type: &str,
    logical_inputs: Value,
    array: ArrayRef,
) -> Result<FlatCase> {
    Ok(FlatCase {
        id: id.to_string(),
        arrow_type: arrow_type.to_string(),
        logical_inputs,
        slot_hashes: hashes(array)?,
    })
}

fn sorted_scalar_state(accumulator: &mut CountHashAccumulator) -> Result<Vec<String>> {
    let state = accumulator.state()?;
    let array = state[0].to_array()?;
    let list = array.as_any().downcast_ref::<ListArray>().unwrap();
    let hashes = list.value(0);
    let hashes = hashes.as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut result = hashes.values().to_vec();
    result.sort_unstable();
    Ok(result.into_iter().map(|hash| hash.to_string()).collect())
}

fn scalar_partial_state(values: Vec<i64>) -> Result<ScalarPartialState> {
    let mut accumulator = CountHashAccumulator {
        values: Default::default(),
        random_state: random_state(),
        batch_hashes: vec![],
    };
    accumulator.update_batch(&[int64_array(values.iter().copied().map(Some).collect())])?;

    Ok(ScalarPartialState {
        logical_inputs: values,
        sorted_hashes: sorted_scalar_state(&mut accumulator)?,
    })
}

fn sorted_group_states(accumulator: &mut CountHashGroupAccumulator) -> Result<Vec<Vec<String>>> {
    let state = accumulator.state(EmitTo::All)?;
    let list = state[0].as_any().downcast_ref::<ListArray>().unwrap();
    let mut result = Vec::with_capacity(list.len());

    for index in 0..list.len() {
        let hashes = list.value(index);
        let hashes = hashes.as_any().downcast_ref::<UInt64Array>().unwrap();
        let mut group = hashes.values().to_vec();
        group.sort_unstable();
        result.push(group.into_iter().map(|hash| hash.to_string()).collect());
    }

    Ok(result)
}

fn build_flat_cases() -> Result<Vec<FlatCase>> {
    let mut cases = Vec::new();
    macro_rules! push_case {
        ($id:expr, $ty:expr, $logical:expr, $array:expr) => {
            cases.push(flat_case($id, $ty, $logical, Arc::new($array))?);
        };
    }

    push_case!("null", "Null", json!([null, null, null]), NullArray::new(3));
    push_case!(
        "boolean",
        "Boolean",
        json!([true, false, null, true]),
        BooleanArray::from(vec![Some(true), Some(false), None, Some(true)])
    );

    push_case!(
        "int8",
        "Int8",
        json!(["-128", "0", "127", null]),
        Int8Array::from(vec![Some(-128), Some(0), Some(127), None])
    );
    push_case!(
        "int16",
        "Int16",
        json!(["-32768", "0", "32767", null]),
        Int16Array::from(vec![Some(-32768), Some(0), Some(32767), None])
    );
    push_case!(
        "int32",
        "Int32",
        json!(["-2147483648", "0", "2147483647", null]),
        Int32Array::from(vec![Some(i32::MIN), Some(0), Some(i32::MAX), None])
    );
    push_case!(
        "int64",
        "Int64",
        json!(["-9223372036854775808", "0", "9223372036854775807", null]),
        Int64Array::from(vec![Some(i64::MIN), Some(0), Some(i64::MAX), None])
    );
    push_case!(
        "uint8",
        "UInt8",
        json!(["0", "1", "255", null]),
        UInt8Array::from(vec![Some(0), Some(1), Some(u8::MAX), None])
    );
    push_case!(
        "uint16",
        "UInt16",
        json!(["0", "1", "65535", null]),
        UInt16Array::from(vec![Some(0), Some(1), Some(u16::MAX), None])
    );
    push_case!(
        "uint32",
        "UInt32",
        json!(["0", "1", "4294967295", null]),
        UInt32Array::from(vec![Some(0), Some(1), Some(u32::MAX), None])
    );
    push_case!(
        "uint64",
        "UInt64",
        json!(["0", "1", "18446744073709551615", null]),
        UInt64Array::from(vec![Some(0), Some(1), Some(u64::MAX), None])
    );

    let f16 = |bits| <Float16Type as ArrowPrimitiveType>::Native::from_bits(bits);
    push_case!(
        "float16-bits",
        "Float16",
        json!([
            "0x0000", "0x8000", "0x7c00", "0xfc00", "0x7e01", "0x7e02", "0x3e00", null
        ]),
        Float16Array::from(vec![
            Some(f16(0)),
            Some(f16(0x8000)),
            Some(f16(0x7c00)),
            Some(f16(0xfc00)),
            Some(f16(0x7e01)),
            Some(f16(0x7e02)),
            Some(f16(0x3e00)),
            None
        ])
    );
    push_case!(
        "float32-bits",
        "Float32",
        json!([
            "0x00000000",
            "0x80000000",
            "0x7f800000",
            "0xff800000",
            "0x7fc00001",
            "0x7fc00002",
            "0x3fc00000",
            null
        ]),
        Float32Array::from(vec![
            Some(f32::from_bits(0)),
            Some(f32::from_bits(0x80000000)),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0x7fc00001)),
            Some(f32::from_bits(0x7fc00002)),
            Some(1.5),
            None
        ])
    );
    push_case!(
        "float64-bits",
        "Float64",
        json!([
            "0x0000000000000000",
            "0x8000000000000000",
            "0x7ff0000000000000",
            "0xfff0000000000000",
            "0x7ff8000000000001",
            "0x7ff8000000000002",
            "0x3ff8000000000000",
            null
        ]),
        Float64Array::from(vec![
            Some(f64::from_bits(0)),
            Some(f64::from_bits(0x8000000000000000)),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0x7ff8000000000001)),
            Some(f64::from_bits(0x7ff8000000000002)),
            Some(1.5),
            None
        ])
    );

    push_case!(
        "decimal32",
        "Decimal32(9, 2)",
        json!({"precision":9,"scale":2,"values":["-12345","0","67890",null]}),
        Decimal32Array::from(vec![Some(-12345), Some(0), Some(67890), None])
            .with_precision_and_scale(9, 2)
            .unwrap()
    );
    push_case!(
        "decimal64",
        "Decimal64(18, 4)",
        json!({"precision":18,"scale":4,"values":["-12345678901234","0","67890123456789",null]}),
        Decimal64Array::from(vec![
            Some(-12345678901234),
            Some(0),
            Some(67890123456789),
            None
        ])
        .with_precision_and_scale(18, 4)
        .unwrap()
    );
    push_case!(
        "decimal128",
        "Decimal128(30, 6)",
        json!({"precision":30,"scale":6,"values":["-123456789012345678901234","0","678901234567890123456789",null]}),
        Decimal128Array::from(vec![
            Some(-123456789012345678901234i128),
            Some(0),
            Some(678901234567890123456789i128),
            None
        ])
        .with_precision_and_scale(30, 6)
        .unwrap()
    );
    push_case!(
        "decimal256",
        "Decimal256(50, 8)",
        json!({"precision":50,"scale":8,"values":["-1234567890123456789012345678901234567890","0","6789012345678901234567890123456789012345",null]}),
        Decimal256Array::from(vec![
            Some(i256::from_string("-1234567890123456789012345678901234567890").unwrap()),
            Some(i256::from(0)),
            Some(i256::from_string("6789012345678901234567890123456789012345").unwrap()),
            None
        ])
        .with_precision_and_scale(50, 8)
        .unwrap()
    );

    macro_rules! temporal_case { ($id:literal, $ty:literal, $unit:literal, $array:expr) => { push_case!($id, $ty, json!({"unit":$unit,"raw_values":["-1","0","123456",null]}), $array); }; }
    temporal_case!(
        "date32",
        "Date32",
        "days",
        Date32Array::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "date64",
        "Date64",
        "milliseconds",
        Date64Array::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "time32-second",
        "Time32(Second)",
        "second",
        Time32SecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "time32-millisecond",
        "Time32(Millisecond)",
        "millisecond",
        Time32MillisecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "time64-microsecond",
        "Time64(Microsecond)",
        "microsecond",
        Time64MicrosecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "time64-nanosecond",
        "Time64(Nanosecond)",
        "nanosecond",
        Time64NanosecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    macro_rules! timestamp_cases { ($id:literal, $ty:literal, $unit:literal, $array:expr) => { push_case!($id, $ty, json!({"unit":$unit,"timezone":null,"raw_values":["-1","0","123456",null]}), $array.clone()); push_case!(concat!($id, "-utc"), concat!($ty, " timezone=UTC"), json!({"unit":$unit,"timezone":"UTC","raw_values":["-1","0","123456",null]}), $array.with_timezone("UTC")); }; }
    timestamp_cases!(
        "timestamp-second",
        "Timestamp(Second)",
        "second",
        TimestampSecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    timestamp_cases!(
        "timestamp-millisecond",
        "Timestamp(Millisecond)",
        "millisecond",
        TimestampMillisecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    timestamp_cases!(
        "timestamp-microsecond",
        "Timestamp(Microsecond)",
        "microsecond",
        TimestampMicrosecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    timestamp_cases!(
        "timestamp-nanosecond",
        "Timestamp(Nanosecond)",
        "nanosecond",
        TimestampNanosecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "duration-second",
        "Duration(Second)",
        "second",
        DurationSecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "duration-millisecond",
        "Duration(Millisecond)",
        "millisecond",
        DurationMillisecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "duration-microsecond",
        "Duration(Microsecond)",
        "microsecond",
        DurationMicrosecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    temporal_case!(
        "duration-nanosecond",
        "Duration(Nanosecond)",
        "nanosecond",
        DurationNanosecondArray::from(vec![Some(-1), Some(0), Some(123456), None])
    );
    push_case!(
        "interval-year-month",
        "Interval(YearMonth)",
        json!({"unit":"year_month","raw_values":["-1","0","25",null]}),
        IntervalYearMonthArray::from(vec![Some(-1), Some(0), Some(25), None])
    );
    push_case!(
        "interval-day-time",
        "Interval(DayTime)",
        json!({"unit":"day_time","raw_values":[{"days":-1,"milliseconds":2},{"days":0,"milliseconds":0},{"days":3,"milliseconds":4},null]}),
        IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(-1, 2)),
            Some(IntervalDayTime::new(0, 0)),
            Some(IntervalDayTime::new(3, 4)),
            None
        ])
    );
    push_case!(
        "interval-month-day-nano",
        "Interval(MonthDayNano)",
        json!({"unit":"month_day_nano","raw_values":[{"months":-1,"days":2,"nanoseconds":3},{"months":0,"days":0,"nanoseconds":0},{"months":4,"days":5,"nanoseconds":6},null]}),
        IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(-1, 2, 3)),
            Some(IntervalMonthDayNano::new(0, 0, 0)),
            Some(IntervalMonthDayNano::new(4, 5, 6)),
            None
        ])
    );

    let long_text = "0123456789abcdef0123456789abcdef";
    let text_logical = json!(["", "short", long_text, "雪", "short", null]);
    push_case!(
        "utf8",
        "Utf8",
        text_logical.clone(),
        StringArray::from(vec![
            Some(""),
            Some("short"),
            Some(long_text),
            Some("雪"),
            Some("short"),
            None
        ])
    );
    push_case!(
        "large-utf8",
        "LargeUtf8",
        text_logical.clone(),
        LargeStringArray::from(vec![
            Some(""),
            Some("short"),
            Some(long_text),
            Some("雪"),
            Some("short"),
            None
        ])
    );
    push_case!(
        "utf8-view",
        "Utf8View",
        text_logical,
        StringViewArray::from_iter(vec![
            Some("".to_string()),
            Some("short".to_string()),
            Some(long_text.to_string()),
            Some("雪".to_string()),
            Some("short".to_string()),
            None
        ])
    );
    let long_bytes = vec![0xabu8; 32];
    let bytes_logical = json!([
        "hex:",
        "hex:0102",
        "hex:abababababababababababababababababababababababababababababababab",
        "hex:e29883",
        "hex:0102",
        null
    ]);
    push_case!(
        "binary",
        "Binary",
        bytes_logical.clone(),
        BinaryArray::from(vec![
            Some(&b""[..]),
            Some(&b"\x01\x02"[..]),
            Some(long_bytes.as_slice()),
            Some("☃".as_bytes()),
            Some(&b"\x01\x02"[..]),
            None
        ])
    );
    push_case!(
        "large-binary",
        "LargeBinary",
        bytes_logical.clone(),
        LargeBinaryArray::from(vec![
            Some(&b""[..]),
            Some(&b"\x01\x02"[..]),
            Some(long_bytes.as_slice()),
            Some("☃".as_bytes()),
            Some(&b"\x01\x02"[..]),
            None
        ])
    );
    push_case!(
        "binary-view",
        "BinaryView",
        bytes_logical,
        BinaryViewArray::from_iter(vec![
            Some(Vec::<u8>::new()),
            Some(vec![1, 2]),
            Some(long_bytes.clone()),
            Some("☃".as_bytes().to_vec()),
            Some(vec![1, 2]),
            None
        ])
    );
    push_case!(
        "fixed-size-binary",
        "FixedSizeBinary(4)",
        json!([
            "hex:00000000",
            "hex:01020304",
            "hex:ffffffff",
            "hex:01020304",
            null
        ]),
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0, 0, 0, 0]),
                Some(vec![1, 2, 3, 4]),
                Some(vec![255, 255, 255, 255]),
                Some(vec![1, 2, 3, 4]),
                None
            ]
            .into_iter(),
            4
        )
        .unwrap()
    );

    macro_rules! dict_case { ($id:literal, $key_type:literal, $key_ty:ty, $keys:expr) => { push_case!($id, concat!("Dictionary(", $key_type, ", Utf8)"), json!({"key_type":$key_type,"keys":["0","1","2",null,"1"],"dictionary_values":["alpha","alpha",null],"logical_values":["alpha","alpha",null,null,"alpha"]}), DictionaryArray::<$key_ty>::new($keys, Arc::new(StringArray::from(vec![Some("alpha"), Some("alpha"), None])))); }; }
    dict_case!(
        "dictionary-int8",
        "Int8",
        Int8Type,
        Int8Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-int16",
        "Int16",
        Int16Type,
        Int16Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-int32",
        "Int32",
        Int32Type,
        Int32Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-int64",
        "Int64",
        Int64Type,
        Int64Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-uint8",
        "UInt8",
        UInt8Type,
        UInt8Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-uint16",
        "UInt16",
        UInt16Type,
        UInt16Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-uint32",
        "UInt32",
        UInt32Type,
        UInt32Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    dict_case!(
        "dictionary-uint64",
        "UInt64",
        UInt64Type,
        UInt64Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])
    );
    Ok(cases)
}

fn nullable_int_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::Int32, true))
}

fn build_nested_cases() -> Result<Vec<FlatCase>> {
    let mut cases = Vec::new();
    macro_rules! push_case {
        ($id:expr, $ty:expr, $logical:expr, $array:expr) => {
            cases.push(flat_case($id, $ty, $logical, Arc::new($array))?);
        };
    }

    let struct_fields = vec![
        Arc::new(Field::new("i", DataType::Int32, true)),
        Arc::new(Field::new("s", DataType::Utf8, true)),
    ]
    .into();
    let struct_array = StructArray::new(
        struct_fields,
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(1),
                None,
                Some(1),
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("a"),
                Some("z"),
                None,
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, true, false, true])),
    );
    push_case!(
        "struct",
        "Struct<i:Int32,s:Utf8>",
        json!({"rows":[{"i":"1","s":"a"},{"i":"2","s":"b"},{"i":"1","s":"a"},null,{"i":"1","s":null}]}),
        struct_array.clone()
    );
    push_case!(
        "struct-sliced",
        "Struct<i:Int32,s:Utf8>; slice(1,3)",
        json!({"physical":"slice(1,3)","rows":[{"i":"2","s":"b"},{"i":"1","s":"a"},null]}),
        struct_array.slice(1, 3)
    );

    let list_values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        None,
        Some(2),
        Some(1),
        None,
        Some(2),
    ]));
    let list = ListArray::new(
        nullable_int_field("item"),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3, 5, 6])),
        list_values.clone(),
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    );
    push_case!(
        "list",
        "List<Int32>",
        json!({"offsets":[0,2,2,3,5,6],"rows":[["1",null],[],null,["1",null],["2"]]}),
        list.clone()
    );
    push_case!(
        "list-sliced",
        "List<Int32>; slice(1,3)",
        json!({"physical":"slice(1,3)","rows":[[],null,["1",null]]}),
        list.slice(1, 3)
    );
    let large_list = LargeListArray::new(
        nullable_int_field("item"),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 2, 2, 3, 5, 6])),
        list_values.clone(),
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    );
    push_case!(
        "large-list",
        "LargeList<Int32>",
        json!({"offsets":["0","2","2","3","5","6"],"rows":[["1",null],[],null,["1",null],["2"]]}),
        large_list
    );
    let inner = ListArray::new(
        nullable_int_field("item"),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3, 4])),
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(1)])),
        None,
    );
    let nested = ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        Arc::new(inner),
        Some(NullBuffer::from(vec![true, true, false])),
    );
    push_case!(
        "nested-list",
        "List<List<Int32>>",
        json!({"rows":[[["1"],["2","3"]],[],null]}),
        nested
    );

    let view_values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(10),
        Some(20),
        Some(30),
        Some(40),
        Some(50),
    ]));
    let list_view = ListViewArray::new(
        nullable_int_field("item"),
        ScalarBuffer::from(vec![2, 0, 1, 4, 0]),
        ScalarBuffer::from(vec![2, 2, 2, 0, 1]),
        view_values.clone(),
        Some(NullBuffer::from(vec![true, true, true, true, false])),
    );
    push_case!(
        "list-view",
        "ListView<Int32>",
        json!({"offsets":[2,0,1,4,0],"sizes":[2,2,2,0,1],"rows":[["30","40"],["10","20"],["20","30"],[],null],"representation_sensitive":true}),
        list_view
    );
    let large_list_view = LargeListViewArray::new(
        nullable_int_field("item"),
        ScalarBuffer::from(vec![2i64, 0, 1, 4, 0]),
        ScalarBuffer::from(vec![2i64, 2, 2, 0, 1]),
        view_values,
        Some(NullBuffer::from(vec![true, true, true, true, false])),
    );
    push_case!(
        "large-list-view",
        "LargeListView<Int32>",
        json!({"offsets":["2","0","1","4","0"],"sizes":["2","2","2","0","1"],"rows":[["30","40"],["10","20"],["20","30"],[],null],"representation_sensitive":true}),
        large_list_view
    );

    let fixed_values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(1),
        None,
        Some(1),
        Some(2),
        Some(9),
        Some(9),
    ]));
    let fixed = FixedSizeListArray::new(
        nullable_int_field("item"),
        2,
        fixed_values,
        Some(NullBuffer::from(vec![true, true, true, false])),
    );
    push_case!(
        "fixed-size-list",
        "FixedSizeList<Int32;2>",
        json!({"rows":[["1","2"],["1",null],["1","2"],null]}),
        fixed.clone()
    );
    push_case!(
        "fixed-size-list-empty",
        "FixedSizeList<Int32;2>; empty",
        json!({"rows":[]}),
        FixedSizeListArray::new(
            nullable_int_field("item"),
            2,
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            None
        )
    );
    push_case!(
        "fixed-size-list-sliced",
        "FixedSizeList<Int32;2>; slice(1,2)",
        json!({"physical":"slice(1,2)","rows":[["1",null],["1","2"]]}),
        fixed.slice(1, 2)
    );

    let map_fields: Fields = vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Int32, true)),
    ]
    .into();
    let entries = StructArray::new(
        map_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "a"])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(1), Some(1)])),
        ],
        None,
    );
    let map_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(
        map_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3, 4])),
        entries,
        Some(NullBuffer::from(vec![true, true, false, true])),
        false,
    );
    push_case!(
        "map",
        "Map<Utf8,Int32>",
        json!({"offsets":[0,2,2,3,4],"rows":[[["a","1"],["b",null]],[],null,[["a","1"]]]}),
        map.clone()
    );
    push_case!(
        "map-sliced",
        "Map<Utf8,Int32>; slice(1,3)",
        json!({"physical":"slice(1,3)","rows":[[],null,[["a","1"]]]}),
        map.slice(1, 3)
    );

    let union_fields = UnionFields::new(
        vec![0, 1],
        vec![
            Arc::new(Field::new("i", DataType::Int32, true)),
            Arc::new(Field::new("s", DataType::Utf8, true)),
        ],
    );
    let dense = UnionArray::try_new(
        union_fields.clone(),
        ScalarBuffer::from(vec![0i8, 1, 0, 1, 0]),
        Some(ScalarBuffer::from(vec![0, 0, 1, 1, 2])),
        vec![
            Arc::new(Int32Array::from(vec![Some(7), Some(7), None])),
            Arc::new(StringArray::from(vec![Some("x"), Some("x")])),
        ],
    )
    .unwrap();
    push_case!(
        "union-dense",
        "Union(Dense; 0:Int32,1:Utf8)",
        json!({"type_ids":[0,1,0,1,0],"offsets":[0,0,1,1,2],"rows":["7","x","7","x",null]}),
        dense.clone()
    );
    push_case!(
        "union-dense-sliced",
        "Union(Dense); slice(1,3)",
        json!({"physical":"slice(1,3)","rows":["x","7","x"]}),
        dense.slice(1, 3)
    );
    let sparse = UnionArray::try_new(
        union_fields,
        ScalarBuffer::from(vec![0i8, 1, 0, 1, 0]),
        None,
        vec![
            Arc::new(Int32Array::from(vec![
                Some(7),
                Some(0),
                Some(7),
                Some(0),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("x"),
                Some(""),
                Some("x"),
                Some(""),
            ])),
        ],
    )
    .unwrap();
    push_case!(
        "union-sparse",
        "Union(Sparse; 0:Int32,1:Utf8)",
        json!({"type_ids":[0,1,0,1,0],"rows":["7","x","7","x",null]}),
        sparse.clone()
    );
    push_case!(
        "union-sparse-sliced",
        "Union(Sparse); slice(1,3)",
        json!({"physical":"slice(1,3)","rows":["x","7","x"]}),
        sparse.slice(1, 3)
    );

    macro_rules! ree_cases { ($id:literal, $run_ty:ty, $array_ty:ty) => {{ let run_ends: $array_ty = <$array_ty>::from(vec![2, 5, 7]); let values = Int32Array::from(vec![Some(1), Some(2), None]); let array = RunArray::<$run_ty>::try_new(&run_ends, &values).unwrap(); push_case!($id, concat!("RunEndEncoded(", stringify!($run_ty), ")"), json!({"run_ends":["2","5","7"],"values":["1","2",null],"logical":["1","1","2","2","2",null,null]}), array.clone()); push_case!(concat!($id, "-sliced"), concat!("RunEndEncoded(", stringify!($run_ty), "); slice(1,5)"), json!({"physical":"slice(1,5)","logical":["1","2","2","2",null]}), array.slice(1, 5)); }}; }
    ree_cases!("ree-int16", Int16Type, Int16Array);
    ree_cases!("ree-int32", Int32Type, Int32Array);
    ree_cases!("ree-int64", Int64Type, Int64Array);
    Ok(cases)
}

fn reconstruct_fixture() -> Result<Fixture> {
    let logical_inputs = vec![Some(1), Some(2), Some(3), Some(4), None];
    let grouped = GroupedCase {
        group_indices: vec![0, 0, 1, 1, 1],
        filter: vec![Some(true), Some(false), Some(true), Some(true), Some(true)],
        total_num_groups: 2,
        sorted_group_states: {
            let mut accumulator = CountHashGroupAccumulator::new();
            let filter = BooleanArray::from(vec![true, false, true, true, true]);
            accumulator.update_batch(
                &[int64_array(logical_inputs.clone())],
                &[0, 0, 1, 1, 1],
                Some(&filter),
                2,
            )?;
            sorted_group_states(&mut accumulator)?
        },
    };

    Ok(Fixture {
        format: "greptime-count-hash-v1".to_string(),
        metadata: Metadata {
            greptime_commit: "67683cef2e445f555edf0506c1c302ac61b5548f".to_string(),
            datafusion_commit: "7a84c45104d35408f00bbf880268c47193b73b28".to_string(),
            hash_utils_blob: "3be6118c55ff2045d9ebdc7ef1a1f6899c50732d".to_string(),
            ahash_version: "0.8.12".to_string(),
            arrow_version: "58.3.0".to_string(),
            seeds_hex: vec![
                format!("0x{RANDOM_SEED_0:016x}"),
                format!("0x{RANDOM_SEED_1:016x}"),
                format!("0x{RANDOM_SEED_2:016x}"),
                format!("0x{RANDOM_SEED_3:016x}"),
            ],
            target_endianness: if cfg!(target_endian = "little") {
                "little".to_string()
            } else {
                "big".to_string()
            },
        },
        int64: Int64Case {
            slot_hashes: hashes(int64_array(logical_inputs.clone()))?,
            logical_inputs,
            null_handling: NullHandling {
                create_hashes:
                    "create_hashes returns one u64 slot for every input row, including null"
                        .to_string(),
                scalar_accumulator:
                    "CountHashAccumulator inserts every create_hashes slot, including null"
                        .to_string(),
                grouped_accumulator:
                    "CountHashGroupAccumulator skips null logical values after hashing".to_string(),
            },
            scalar_partial_states: vec![
                scalar_partial_state(vec![1, 2, 3])?,
                scalar_partial_state(vec![2, 3, 4])?,
            ],
            grouped,
        },
        flat_cases: build_flat_cases()?,
        nested_cases: build_nested_cases()?,
    })
}

fn frozen_fixture() -> Fixture {
    serde_json::from_str(
        &fs::read_to_string(FIXTURE_PATH).expect("failed to read frozen CountHash hash-v1 fixture"),
    )
    .expect("invalid frozen CountHash hash-v1 fixture")
}

fn state_array(hashes: Vec<u64>) -> Result<ArrayRef> {
    SingleRowListArrayBuilder::new(Arc::new(UInt64Array::from(hashes)))
        .build_list_scalar()
        .to_array()
}

fn grouped_state_array(groups: Vec<Vec<u64>>) -> ArrayRef {
    let mut offsets = Vec::with_capacity(groups.len() + 1);
    let mut values = Vec::new();
    offsets.push(0);
    for group in groups {
        values.extend(group);
        offsets.push(values.len() as i32);
    }

    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::UInt64, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(UInt64Array::from(values)),
        None,
    ))
}

#[test]
fn verify_count_hash_hash_v1_fixture() -> Result<()> {
    let fixture = frozen_fixture();

    assert_eq!(fixture.format, "greptime-count-hash-v1");
    assert_eq!(hash_v1::HASH_VERSION, "count_hash-ahash-v1");
    for (cases, required_ids, category) in [
        (&fixture.flat_cases, FLAT_CASE_IDS, "flat"),
        (&fixture.nested_cases, NESTED_CASE_IDS, "nested"),
    ] {
        assert_eq!(cases.len(), required_ids.len());
        let fixture_ids = cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let expected_ids = required_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            fixture_ids.len(),
            cases.len(),
            "{category} case IDs must be unique"
        );
        assert_eq!(
            fixture_ids, expected_ids,
            "{category} case coverage changed"
        );
        assert!(
            cases
                .iter()
                .all(|case| !case.slot_hashes.is_empty() || case.id == "fixed-size-list-empty")
        );
    }

    assert_eq!(
        fixture.metadata.greptime_commit,
        "67683cef2e445f555edf0506c1c302ac61b5548f"
    );
    assert_eq!(
        fixture.metadata.datafusion_commit,
        "7a84c45104d35408f00bbf880268c47193b73b28"
    );
    assert_eq!(
        fixture.metadata.hash_utils_blob,
        "3be6118c55ff2045d9ebdc7ef1a1f6899c50732d"
    );
    assert_eq!(fixture.metadata.ahash_version, "0.8.12");
    assert_eq!(fixture.metadata.arrow_version, "58.3.0");
    assert_eq!(
        fixture.metadata.seeds_hex,
        [
            format!("0x{RANDOM_SEED_0:016x}"),
            format!("0x{RANDOM_SEED_1:016x}"),
            format!("0x{RANDOM_SEED_2:016x}"),
            format!("0x{RANDOM_SEED_3:016x}"),
        ]
    );
    assert_eq!(
        fixture.metadata.target_endianness,
        if cfg!(target_endian = "little") {
            "little"
        } else {
            "big"
        }
    );

    let recomputed = reconstruct_fixture()?;
    assert_eq!(recomputed.flat_cases.len(), FLAT_CASE_IDS.len());
    assert_eq!(recomputed.nested_cases.len(), NESTED_CASE_IDS.len());
    let struct_case = fixture
        .nested_cases
        .iter()
        .find(|case| case.id == "struct")
        .unwrap();
    assert_eq!(struct_case.slot_hashes[0], struct_case.slot_hashes[2]);
    assert_eq!(fixture, recomputed);
    Ok(())
}

#[test]
fn count_hash_v1_merges_frozen_states_but_foldhash_does_not() -> Result<()> {
    let fixture = frozen_fixture();
    let frozen_first_state = fixture.int64.scalar_partial_states[0]
        .sorted_hashes
        .iter()
        .map(|hash| hash.parse().unwrap())
        .collect();
    let expected_overlaps: Vec<u64> = fixture.int64.slot_hashes[1..3]
        .iter()
        .map(|hash| hash.parse().unwrap())
        .collect();

    let overlap_values = int64_array(vec![Some(2), Some(3)]);
    let mut v1_overlap_hashes = vec![0; overlap_values.len()];
    hash_v1::create_hashes(&[overlap_values], &random_state(), &mut v1_overlap_hashes)?;
    assert_eq!(v1_overlap_hashes, expected_overlaps);

    let foldhash_values = int64_array(vec![Some(2), Some(3), Some(4)]);
    let mut foldhashes = vec![0; foldhash_values.len()];
    foldhash_create_hashes(
        &[foldhash_values],
        &FoldHashState::default(),
        &mut foldhashes,
    )?;
    let mut foldhash_merged = CountHashAccumulator {
        values: Default::default(),
        random_state: random_state(),
        batch_hashes: vec![],
    };
    foldhash_merged.merge_batch(&[state_array(frozen_first_state)?])?;
    foldhash_merged.merge_batch(&[state_array(foldhashes)?])?;
    let foldhash_result = foldhash_merged.evaluate()?;
    assert_ne!(foldhash_result, ScalarValue::Int64(Some(4)));
    assert_eq!(foldhash_result, ScalarValue::Int64(Some(6)));

    let frozen_state = state_array(
        fixture.int64.scalar_partial_states[0]
            .sorted_hashes
            .iter()
            .map(|hash| hash.parse().unwrap())
            .collect(),
    )?;
    let mut current = CountHashAccumulator {
        values: Default::default(),
        random_state: random_state(),
        batch_hashes: vec![],
    };
    current.update_batch(&[int64_array(vec![Some(2), Some(3), Some(4)])])?;
    let current_state = current.state()?[0].to_array()?;
    for (direction, states) in [
        (
            "frozen DF53 -> current hash-v1",
            [frozen_state.clone(), current_state.clone()],
        ),
        (
            "current hash-v1 -> frozen DF53",
            [current_state, frozen_state],
        ),
    ] {
        let mut merged = CountHashAccumulator {
            values: Default::default(),
            random_state: random_state(),
            batch_hashes: vec![],
        };
        for state in states {
            merged.merge_batch(&[state])?;
        }
        assert_eq!(
            merged.evaluate()?,
            ScalarValue::Int64(Some(4)),
            "{direction}"
        );
    }
    Ok(())
}

#[test]
fn count_hash_v1_grouped_reciprocal_merge_preserves_null_filter_asymmetry() -> Result<()> {
    let mut scalar = CountHashAccumulator {
        values: Default::default(),
        random_state: random_state(),
        batch_hashes: vec![],
    };
    scalar.update_batch(&[int64_array(vec![None])])?;
    assert_eq!(scalar.evaluate()?, ScalarValue::Int64(Some(1)));

    let fixture = frozen_fixture();
    let frozen_state = grouped_state_array(
        fixture
            .int64
            .grouped
            .sorted_group_states
            .iter()
            .map(|group| group.iter().map(|hash| hash.parse().unwrap()).collect())
            .collect(),
    );

    let filter = BooleanArray::from(vec![true, true, true, false]);
    let mut current = CountHashGroupAccumulator::new();
    current.update_batch(
        &[int64_array(vec![Some(2), Some(3), None, Some(4)])],
        &[0, 0, 0, 0],
        Some(&filter),
        1,
    )?;
    let current_state = current.state(EmitTo::All)?[0].clone();

    for (direction, first, first_groups, second, second_groups) in [
        (
            "frozen DF53 -> current hash-v1",
            frozen_state.clone(),
            vec![0, 0],
            current_state.clone(),
            vec![0],
        ),
        (
            "current hash-v1 -> frozen DF53",
            current_state,
            vec![0],
            frozen_state,
            vec![0, 0],
        ),
    ] {
        let mut merged = CountHashGroupAccumulator::new();
        merged.merge_batch(&[first], &first_groups, None, 1)?;
        merged.merge_batch(&[second], &second_groups, None, 1)?;
        let counts = merged.evaluate(EmitTo::All)?;
        let counts = counts.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.values(), &[4], "{direction}");
    }
    Ok(())
}
