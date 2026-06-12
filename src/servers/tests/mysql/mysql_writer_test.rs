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

use common_time::timestamp::TimeUnit;
use common_time::{Timestamp, Timezone};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::{
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use opensrv_mysql::{ColumnFlags, ColumnType};
use servers::mysql::writer::{create_mysql_column, create_mysql_column_def};

use crate::mysql::{TestingData, all_datatype_testing_data};

// ── Schema-to-column-def tests ────────────────────────────────────────────────

/// Round-trip: every column schema in `all_datatype_testing_data()` must produce
/// a column definition whose type and name match the expected values.
#[test]
fn test_create_mysql_column_def() {
    let TestingData {
        column_schemas,
        mysql_columns_def,
        ..
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let columns_def = create_mysql_column_def(&schema).unwrap();
    assert_eq!(
        column_schemas.len(),
        columns_def.len(),
        "column count must match"
    );

    for (i, column_def) in columns_def.iter().enumerate() {
        assert_eq!(
            column_schemas[i].name, column_def.column,
            "column name mismatch at index {i}"
        );
        assert_eq!(
            mysql_columns_def[i], column_def.coltype,
            "column type mismatch at index {i} ({})",
            column_schemas[i].name
        );
    }
}

// ── Individual create_mysql_column type-mapping tests ────────────────────────

/// Verify every concrete data type maps to the correct MySQL column type.
#[test]
fn test_create_mysql_column_type_mapping() {
    let cases: &[(&str, ConcreteDataType, ColumnType)] = &[
        ("null_col", ConcreteDataType::null_datatype(), ColumnType::MYSQL_TYPE_NULL),
        ("bool_col", ConcreteDataType::boolean_datatype(), ColumnType::MYSQL_TYPE_TINY),
        ("int8_col", ConcreteDataType::int8_datatype(), ColumnType::MYSQL_TYPE_TINY),
        ("int16_col", ConcreteDataType::int16_datatype(), ColumnType::MYSQL_TYPE_SHORT),
        ("int32_col", ConcreteDataType::int32_datatype(), ColumnType::MYSQL_TYPE_LONG),
        ("int64_col", ConcreteDataType::int64_datatype(), ColumnType::MYSQL_TYPE_LONGLONG),
        ("uint8_col", ConcreteDataType::uint8_datatype(), ColumnType::MYSQL_TYPE_TINY),
        ("uint16_col", ConcreteDataType::uint16_datatype(), ColumnType::MYSQL_TYPE_SHORT),
        ("uint32_col", ConcreteDataType::uint32_datatype(), ColumnType::MYSQL_TYPE_LONG),
        ("uint64_col", ConcreteDataType::uint64_datatype(), ColumnType::MYSQL_TYPE_LONGLONG),
        ("float32_col", ConcreteDataType::float32_datatype(), ColumnType::MYSQL_TYPE_FLOAT),
        ("float64_col", ConcreteDataType::float64_datatype(), ColumnType::MYSQL_TYPE_DOUBLE),
        ("string_col", ConcreteDataType::string_datatype(), ColumnType::MYSQL_TYPE_VARCHAR),
        ("binary_col", ConcreteDataType::binary_datatype(), ColumnType::MYSQL_TYPE_VARCHAR),
        (
            "ts_s_col",
            ConcreteDataType::timestamp_second_datatype(),
            ColumnType::MYSQL_TYPE_TIMESTAMP,
        ),
        (
            "ts_ms_col",
            ConcreteDataType::timestamp_millisecond_datatype(),
            ColumnType::MYSQL_TYPE_TIMESTAMP,
        ),
        (
            "ts_us_col",
            ConcreteDataType::timestamp_microsecond_datatype(),
            ColumnType::MYSQL_TYPE_TIMESTAMP,
        ),
        (
            "ts_ns_col",
            ConcreteDataType::timestamp_nanosecond_datatype(),
            ColumnType::MYSQL_TYPE_TIMESTAMP,
        ),
        ("date_col", ConcreteDataType::date_datatype(), ColumnType::MYSQL_TYPE_DATE),
        (
            "time_s_col",
            ConcreteDataType::time_second_datatype(),
            ColumnType::MYSQL_TYPE_TIME,
        ),
        (
            "time_ms_col",
            ConcreteDataType::time_millisecond_datatype(),
            ColumnType::MYSQL_TYPE_TIME,
        ),
        (
            "time_us_col",
            ConcreteDataType::time_microsecond_datatype(),
            ColumnType::MYSQL_TYPE_TIME,
        ),
        (
            "time_ns_col",
            ConcreteDataType::time_nanosecond_datatype(),
            ColumnType::MYSQL_TYPE_TIME,
        ),
        (
            "interval_ym",
            ConcreteDataType::interval_year_month_datatype(),
            ColumnType::MYSQL_TYPE_VARCHAR,
        ),
        (
            "interval_dt",
            ConcreteDataType::interval_day_time_datatype(),
            ColumnType::MYSQL_TYPE_VARCHAR,
        ),
        (
            "interval_mdn",
            ConcreteDataType::interval_month_day_nano_datatype(),
            ColumnType::MYSQL_TYPE_VARCHAR,
        ),
        (
            "decimal_col",
            ConcreteDataType::decimal128_datatype(10, 2),
            ColumnType::MYSQL_TYPE_DECIMAL,
        ),
    ];

    for (col_name, data_type, expected_type) in cases {
        let col = create_mysql_column(data_type, col_name)
            .unwrap_or_else(|_| panic!("create_mysql_column failed for {col_name}"));
        assert_eq!(
            *expected_type, col.coltype,
            "type mismatch for column '{col_name}'"
        );
        assert_eq!(*col_name, col.column, "name mismatch for '{col_name}'");
        // collen == 0 means "use MySQL default length"
        assert_eq!(0, col.collen, "collen should be 0 for '{col_name}'");
        // table should be empty (not populated yet)
        assert!(col.table.is_empty(), "table should be empty for '{col_name}'");
    }
}

// ── Column flags tests ────────────────────────────────────────────────────────

/// Unsigned integer types must carry `UNSIGNED_FLAG`; all other types must not.
#[test]
fn test_create_mysql_column_unsigned_flags() {
    // Types that must have UNSIGNED_FLAG
    let unsigned_types: &[(&str, ConcreteDataType)] = &[
        ("u8", ConcreteDataType::uint8_datatype()),
        ("u16", ConcreteDataType::uint16_datatype()),
        ("u32", ConcreteDataType::uint32_datatype()),
        ("u64", ConcreteDataType::uint64_datatype()),
    ];
    for (name, dt) in unsigned_types {
        let col = create_mysql_column(dt, name).unwrap();
        assert!(
            col.colflags.contains(ColumnFlags::UNSIGNED_FLAG),
            "UNSIGNED_FLAG must be set for {name}"
        );
    }

    // Types that must NOT have UNSIGNED_FLAG
    let signed_types: &[(&str, ConcreteDataType)] = &[
        ("i8", ConcreteDataType::int8_datatype()),
        ("i16", ConcreteDataType::int16_datatype()),
        ("i32", ConcreteDataType::int32_datatype()),
        ("i64", ConcreteDataType::int64_datatype()),
        ("f32", ConcreteDataType::float32_datatype()),
        ("f64", ConcreteDataType::float64_datatype()),
        ("str", ConcreteDataType::string_datatype()),
        ("ts", ConcreteDataType::timestamp_millisecond_datatype()),
        ("bool", ConcreteDataType::boolean_datatype()),
        ("date", ConcreteDataType::date_datatype()),
    ];
    for (name, dt) in signed_types {
        let col = create_mysql_column(dt, name).unwrap();
        assert!(
            !col.colflags.contains(ColumnFlags::UNSIGNED_FLAG),
            "UNSIGNED_FLAG must NOT be set for {name}"
        );
    }
}

// ── Timestamp precision tests (issue #8227) ───────────────────────────────────

/// Timestamps displayed through the MySQL writer must respect their storage unit.
///
/// Before the fix, `to_chrono_datetime_with_timezone` → `NaiveDateTime::Display`
/// always emitted 6 fractional-second digits, so:
///   - `TIMESTAMP(3)` showed `.195000` instead of `.195`
///   - `TIMESTAMP(9)` showed `.195123` instead of `.195123456`
///
#[test]
fn test_timestamp_display_precision_utc() {
    // Use UTC timezone for deterministic, timezone-independent output.
    let tz = Timezone::from_tz_string("UTC").unwrap();

    // Base instant: 2026-06-02 03:50:00 UTC  (unix epoch seconds = 1748836200)
    // Sub-second component: 195_123_456 ns = 195.123456 ms = 0.195123456 s
    let base_s: i64 = 1_748_836_200;

    // TIMESTAMP(0) — second precision: no fractional part
    let ts = Timestamp::new(base_s, TimeUnit::Second);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00",
        "TIMESTAMP(0) must have no fractional seconds"
    );

    // TIMESTAMP(3) — millisecond precision: trailing zeros must be stripped
    let ts = Timestamp::new(base_s * 1_000 + 195, TimeUnit::Millisecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.195",
        "TIMESTAMP(3) must show exactly 3 fractional digits (no trailing zeros)"
    );

    // TIMESTAMP(6) — microsecond precision
    let ts = Timestamp::new(base_s * 1_000_000 + 195_123, TimeUnit::Microsecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.195123",
        "TIMESTAMP(6) must show exactly 6 fractional digits"
    );

    // TIMESTAMP(9) — nanosecond precision: full 9-digit fidelity
    let ts = Timestamp::new(base_s * 1_000_000_000 + 195_123_456, TimeUnit::Nanosecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.195123456",
        "TIMESTAMP(9) must show all 9 fractional digits"
    );
}

/// When the subsecond part is exactly zero, no fractional portion is emitted
/// for any precision level (the `%.f` specifier omits the dot entirely).
#[test]
fn test_timestamp_display_precision_no_subseconds() {
    let tz = Timezone::from_tz_string("UTC").unwrap();
    let base_s: i64 = 1_748_836_200; // 2026-06-02 03:50:00 UTC

    for (unit, value) in [
        (TimeUnit::Second, base_s),
        (TimeUnit::Millisecond, base_s * 1_000),
        (TimeUnit::Microsecond, base_s * 1_000_000),
        (TimeUnit::Nanosecond, base_s * 1_000_000_000),
    ] {
        let ts = Timestamp::new(value, unit);
        assert_eq!(
            ts.to_timezone_aware_string(Some(&tz)),
            "2026-06-02 03:50:00",
            "zero subseconds must produce no fractional part (unit = {unit:?})"
        );
    }
}

/// Trailing zeros within the subsecond part are stripped correctly.
/// e.g. 100 ms → `.1`, not `.100` or `.100000`.
#[test]
fn test_timestamp_display_precision_trailing_zeros_stripped() {
    let tz = Timezone::from_tz_string("UTC").unwrap();
    let base_s: i64 = 1_748_836_200;

    // 100 ms → ".1"
    let ts = Timestamp::new(base_s * 1_000 + 100, TimeUnit::Millisecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.1",
        "100 ms must display as '.1'"
    );

    // 100_000 µs → ".1"
    let ts = Timestamp::new(base_s * 1_000_000 + 100_000, TimeUnit::Microsecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.1",
        "100_000 µs must display as '.1'"
    );

    // 100_000_000 ns → ".1"
    let ts = Timestamp::new(base_s * 1_000_000_000 + 100_000_000, TimeUnit::Nanosecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.1",
        "100_000_000 ns must display as '.1'"
    );

    // 10 ms → ".01"
    let ts = Timestamp::new(base_s * 1_000 + 10, TimeUnit::Millisecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.01",
        "10 ms must display as '.01'"
    );

    // 1 ms → ".001"
    let ts = Timestamp::new(base_s * 1_000 + 1, TimeUnit::Millisecond);
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz)),
        "2026-06-02 03:50:00.001",
        "1 ms must display as '.001'"
    );
}

/// Verify the Unix epoch (timestamp == 0) formats correctly in UTC.
#[test]
fn test_timestamp_display_unix_epoch() {
    let tz = Timezone::from_tz_string("UTC").unwrap();

    let ts_s = Timestamp::new(0, TimeUnit::Second);
    assert_eq!(ts_s.to_timezone_aware_string(Some(&tz)), "1970-01-01 00:00:00");

    let ts_ms = Timestamp::new(0, TimeUnit::Millisecond);
    assert_eq!(ts_ms.to_timezone_aware_string(Some(&tz)), "1970-01-01 00:00:00");

    let ts_us = Timestamp::new(0, TimeUnit::Microsecond);
    assert_eq!(ts_us.to_timezone_aware_string(Some(&tz)), "1970-01-01 00:00:00");

    let ts_ns = Timestamp::new(0, TimeUnit::Nanosecond);
    assert_eq!(ts_ns.to_timezone_aware_string(Some(&tz)), "1970-01-01 00:00:00");
}

/// Negative timestamps (before the Unix epoch) must format correctly.
#[test]
fn test_timestamp_display_negative_value() {
    let tz = Timezone::from_tz_string("UTC").unwrap();

    // -1 second = 1969-12-31 23:59:59
    let ts = Timestamp::new(-1, TimeUnit::Second);
    assert_eq!(ts.to_timezone_aware_string(Some(&tz)), "1969-12-31 23:59:59");

    // -1 millisecond = 1969-12-31 23:59:59.999
    let ts = Timestamp::new(-1, TimeUnit::Millisecond);
    assert_eq!(ts.to_timezone_aware_string(Some(&tz)), "1969-12-31 23:59:59.999");
}

/// Verify that timezone offset shifts the display time correctly.
#[test]
fn test_timestamp_display_timezone_offset() {
    // 2026-06-02 03:50:00 UTC = 2026-06-02 09:20:00 in +05:30 (India)
    let base_s: i64 = 1_748_836_200;

    let tz_utc = Timezone::from_tz_string("UTC").unwrap();
    let tz_ist = Timezone::from_tz_string("+05:30").unwrap();
    let tz_neg = Timezone::from_tz_string("-08:00").unwrap();

    let ts = Timestamp::new(base_s * 1_000 + 195, TimeUnit::Millisecond);

    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz_utc)),
        "2026-06-02 03:50:00.195"
    );
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz_ist)),
        "2026-06-02 09:20:00.195"
    );
    assert_eq!(
        ts.to_timezone_aware_string(Some(&tz_neg)),
        "2026-06-01 19:50:00.195"
    );
}

// ── Column schema convenience constructors ────────────────────────────────────

/// All four timestamp types should map to `MYSQL_TYPE_TIMESTAMP`.
#[test]
fn test_create_mysql_column_all_timestamp_precisions() {
    let timestamp_types: &[(&str, ConcreteDataType)] = &[
        (
            "ts_s",
            ConcreteDataType::Timestamp(datatypes::types::TimestampType::Second(
                TimestampSecondType,
            )),
        ),
        (
            "ts_ms",
            ConcreteDataType::Timestamp(datatypes::types::TimestampType::Millisecond(
                TimestampMillisecondType,
            )),
        ),
        (
            "ts_us",
            ConcreteDataType::Timestamp(datatypes::types::TimestampType::Microsecond(
                TimestampMicrosecondType,
            )),
        ),
        (
            "ts_ns",
            ConcreteDataType::Timestamp(datatypes::types::TimestampType::Nanosecond(
                TimestampNanosecondType,
            )),
        ),
    ];

    for (name, dt) in timestamp_types {
        let col = create_mysql_column(dt, name).unwrap();
        assert_eq!(
            ColumnType::MYSQL_TYPE_TIMESTAMP,
            col.coltype,
            "timestamp type '{name}' must map to MYSQL_TYPE_TIMESTAMP"
        );
        assert!(
            !col.colflags.contains(ColumnFlags::UNSIGNED_FLAG),
            "timestamps must not be unsigned"
        );
    }
}

/// All three interval types should map to `MYSQL_TYPE_VARCHAR`.
#[test]
fn test_create_mysql_column_all_interval_types() {
    let interval_types: &[(&str, ConcreteDataType)] = &[
        (
            "iv_ym",
            ConcreteDataType::Interval(datatypes::types::IntervalType::YearMonth(
                IntervalYearMonthType,
            )),
        ),
        (
            "iv_dt",
            ConcreteDataType::Interval(datatypes::types::IntervalType::DayTime(
                IntervalDayTimeType,
            )),
        ),
        (
            "iv_mdn",
            ConcreteDataType::Interval(datatypes::types::IntervalType::MonthDayNano(
                IntervalMonthDayNanoType,
            )),
        ),
    ];

    for (name, dt) in interval_types {
        let col = create_mysql_column(dt, name).unwrap();
        assert_eq!(
            ColumnType::MYSQL_TYPE_VARCHAR,
            col.coltype,
            "interval type '{name}' must map to MYSQL_TYPE_VARCHAR"
        );
    }
}

/// create_mysql_column_def must preserve the ordering and names of all columns.
#[test]
fn test_create_mysql_column_def_preserves_order() {
    let schemas: Vec<ColumnSchema> = vec![
        ColumnSchema::new("alpha", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new("beta", ConcreteDataType::string_datatype(), true),
        ColumnSchema::new("gamma", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("delta", ConcreteDataType::timestamp_millisecond_datatype(), false),
    ];
    let schema = Arc::new(Schema::new(schemas.clone()));
    let cols = create_mysql_column_def(&schema).unwrap();

    assert_eq!(4, cols.len());
    assert_eq!("alpha", cols[0].column);
    assert_eq!(ColumnType::MYSQL_TYPE_LONG, cols[0].coltype);
    assert_eq!("beta", cols[1].column);
    assert_eq!(ColumnType::MYSQL_TYPE_VARCHAR, cols[1].coltype);
    assert_eq!("gamma", cols[2].column);
    assert_eq!(ColumnType::MYSQL_TYPE_DOUBLE, cols[2].coltype);
    assert_eq!("delta", cols[3].column);
    assert_eq!(ColumnType::MYSQL_TYPE_TIMESTAMP, cols[3].coltype);
}

/// An empty schema must produce an empty column-def list (no panic).
#[test]
fn test_create_mysql_column_def_empty_schema() {
    let schema = Arc::new(Schema::new(vec![]));
    let cols = create_mysql_column_def(&schema).unwrap();
    assert!(cols.is_empty(), "empty schema must produce no columns");
}

/// Decimal128 must map to MYSQL_TYPE_DECIMAL regardless of precision/scale.
#[test]
fn test_create_mysql_column_decimal_variants() {
    for (p, s) in [(10u8, 2i8), (38, 10), (5, 0)] {
        let dt = ConcreteDataType::decimal128_datatype(p, s);
        let col = create_mysql_column(&dt, "dec").unwrap();
        assert_eq!(ColumnType::MYSQL_TYPE_DECIMAL, col.coltype);
        assert!(!col.colflags.contains(ColumnFlags::UNSIGNED_FLAG));
    }
}
