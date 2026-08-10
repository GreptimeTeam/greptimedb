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

// gtdb-log-loader — high-throughput log ingestion for the o11ybench regression
// benchmark.
//
// Reads a JSONL file and writes it into GreptimeDB over gRPC (port 4001)
// using the official `client` crate, with parallel in-flight insert requests.
// The table must already exist (created by the SQL DDL from the benchmark
// setup) so schema + indexes match the benchmark contract exactly.
//
// Usage:
//   gtdb-log-loader --endpoint 127.0.0.1:4001 --data app_logs.jsonl \
//       --table app_logs --batch-size 10000 --concurrency 8
//   gtdb-log-loader --endpoint 127.0.0.1:4001 --data agent_observations.jsonl \
//       --table agent_observations --database public \
//       --columns-file columns_agent_observations.json
//
// Without `--columns-file` the built-in logbench-lite app_logs schema is used
// (ts TIMESTAMP(3) NOT NULL (TIME INDEX), then 9 STRING/JSON fields) for
// backward compatibility. With `--columns-file` the schema comes from a JSON
// array of {"name": "...", "kind": "ts"|"string"|"int64"|"date"|"decimal"|"json"}
// entries, and each JSONL line must be an object whose keys match the column
// names (no nested aliases). The timestamp column accepts either an integer
// epoch-millis value or an RFC3339 string with an optional timezone offset.
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, Decimal128, DecimalTypeExtension, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value,
};
use client::Client;
use serde_json::Value as Json;
use tokio::task::JoinSet;

const COLUMNS: [(&str, ColumnDataType, SemanticType); 10] = [
    (
        "ts",
        ColumnDataType::TimestampMillisecond,
        SemanticType::Timestamp,
    ),
    ("event_id", ColumnDataType::String, SemanticType::Field),
    ("service", ColumnDataType::String, SemanticType::Field),
    ("host", ColumnDataType::String, SemanticType::Field),
    ("region", ColumnDataType::String, SemanticType::Field),
    ("level", ColumnDataType::String, SemanticType::Field),
    ("trace_id", ColumnDataType::String, SemanticType::Field),
    ("span_id", ColumnDataType::String, SemanticType::Field),
    ("message", ColumnDataType::String, SemanticType::Field),
    ("attrs", ColumnDataType::Json, SemanticType::Field),
];

/// How a JSONL field is interpreted for a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    /// Time index: RFC3339 string or integer epoch-millis.
    Ts,
    String,
    Int64,
    /// "YYYY-MM-DD" string -> days since epoch (Date32).
    Date,
    /// JSON number or decimal string -> Decimal128 (scale from the columns
    /// file, matching the target column's DECIMAL(precision, scale)).
    Decimal,
    /// Raw JSON value, serialized to a string; the server converts to JSONB.
    Json,
}

#[derive(Clone, Debug)]
struct ColumnSpec {
    name: String,
    kind: ColumnKind,
    /// Decimal precision/scale; only used for ColumnKind::Decimal.
    /// Defaults mirror the server's DECIMAL128 defaults (38/10) when absent.
    precision: i32,
    scale: i32,
}

/// Built-in schema, kept for backward compatibility (logbench-lite app_logs).
fn default_specs() -> Vec<ColumnSpec> {
    COLUMNS
        .iter()
        .map(|(name, dt, _st)| ColumnSpec {
            name: name.to_string(),
            kind: match dt {
                ColumnDataType::TimestampMillisecond => ColumnKind::Ts,
                ColumnDataType::String => ColumnKind::String,
                ColumnDataType::Json => ColumnKind::Json,
                other => panic!("unsupported built-in column datatype {other:?}"),
            },
            precision: 38,
            scale: 10,
        })
        .collect()
}

/// Parse a `--columns-file` (JSON array of {"name","kind"}) by hand-traversing
/// serde_json::Value so no serde-derive dependency is needed.
fn load_specs(path: &PathBuf) -> anyhow::Result<Vec<ColumnSpec>> {
    let text = std::fs::read_to_string(path)?;
    let arr: Json = serde_json::from_str(&text)?;
    let arr = arr
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("columns file must be a JSON array: {path:?}"))?;
    arr.iter()
        .map(|item| {
            let name = item
                .get("name")
                .and_then(Json::as_str)
                .ok_or_else(|| {
                    anyhow::anyhow!("columns file entry missing string \"name\": {item}")
                })?
                .to_string();
            let kind = item
                .get("kind")
                .and_then(Json::as_str)
                .ok_or_else(|| anyhow::anyhow!("column {name}: missing string \"kind\""))?;
            let kind = match kind {
                "ts" => ColumnKind::Ts,
                "string" => ColumnKind::String,
                "int64" => ColumnKind::Int64,
                "date" => ColumnKind::Date,
                "decimal" => ColumnKind::Decimal,
                "json" => ColumnKind::Json,
                other => {
                    anyhow::bail!("column {name}: unknown kind {other:?} (expected ts|string|int64|date|decimal|json)")
                }
            };
            let precision = item
                .get("precision")
                .and_then(Json::as_i64)
                .unwrap_or(38) as i32;
            let scale = item.get("scale").and_then(Json::as_i64).unwrap_or(10) as i32;
            Ok(ColumnSpec { name, kind, precision, scale })
        })
        .collect()
}

fn datatype_of(kind: ColumnKind) -> ColumnDataType {
    match kind {
        ColumnKind::Ts => ColumnDataType::TimestampMillisecond,
        ColumnKind::String => ColumnDataType::String,
        ColumnKind::Int64 => ColumnDataType::Int64,
        ColumnKind::Date => ColumnDataType::Date,
        ColumnKind::Decimal => ColumnDataType::Decimal128,
        ColumnKind::Json => ColumnDataType::Json,
    }
}

fn semantic_of(kind: ColumnKind) -> SemanticType {
    match kind {
        ColumnKind::Ts => SemanticType::Timestamp,
        _ => SemanticType::Field,
    }
}

fn make_schema(specs: &[ColumnSpec]) -> Vec<ColumnSchema> {
    specs
        .iter()
        .map(|c| {
            let datatype_extension = match c.kind {
                ColumnKind::Decimal => Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                        precision: c.precision,
                        scale: c.scale,
                    })),
                }),
                _ => None,
            };
            ColumnSchema {
                column_name: c.name.clone(),
                datatype: datatype_of(c.kind) as i32,
                semantic_type: semantic_of(c.kind) as i32,
                datatype_extension,
                ..Default::default()
            }
        })
        .collect()
}

fn str_val(s: &str) -> Value {
    Value {
        value_data: Some(ValueData::StringValue(s.to_string())),
    }
}

fn int64_val(v: i64) -> Value {
    Value {
        value_data: Some(ValueData::I64Value(v)),
    }
}

/// String coercion for STRING/JSON columns: null/missing -> "".
fn string_of(raw: &Json) -> String {
    match raw {
        Json::Null => String::new(),
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Int coercion for INT64 columns: null/missing -> 0, floats truncated.
fn int64_of(raw: &Json) -> i64 {
    match raw {
        Json::Null => 0,
        Json::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or(0),
        Json::String(s) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

/// Date coercion for DATE columns: "YYYY-MM-DD" -> days since epoch.
fn date_of(raw: &Json) -> i32 {
    match raw {
        Json::Null => 0,
        Json::String(s) => parse_date_days(s).unwrap_or(0),
        Json::Number(n) => n.as_i64().unwrap_or(0) as i32,
        _ => 0,
    }
}

/// Decimal coercion for DECIMAL128 columns: the JSON number or decimal string
/// is parsed with pure integer/fixed-point arithmetic (never f64) and scaled
/// by 10^scale (scale from the columns file, matching the target column's
/// DECIMAL(precision, scale)), so all 38 digits of precision survive. The
/// row-insert API carries precision/scale via the column's datatype_extension
/// (see `make_schema`); the server decodes the raw i128 using that column
/// scale, so the encoding must use the same scale. Values with more fractional
/// digits than `scale` are rounded half away from zero.
fn decimal_of(raw: &Json, scale: i32) -> anyhow::Result<Decimal128> {
    let text = match raw {
        Json::Null => return Ok(Decimal128 { hi: 0, lo: 0 }),
        Json::Number(n) => n.to_string(),
        Json::String(s) => s.clone(),
        other => anyhow::bail!("decimal value must be a number or string, got {other}"),
    };
    let scaled = parse_fixed_point(&text, scale)?;
    Ok(Decimal128 {
        hi: (scaled >> 64) as i64,
        lo: scaled as u64 as i64,
    })
}

/// Parse a decimal literal ("123", "-0.0184", "1.5e3", ...) into a scaled
/// i128 (value * 10^scale) using integer arithmetic only, so DECIMAL(38, s)
/// values are not truncated by f64 precision. Rounding (half away from zero)
/// applies only when the literal has more fractional digits than `scale`.
fn parse_fixed_point(text: &str, scale: i32) -> anyhow::Result<i128> {
    let b = text.as_bytes();
    let (neg, b) = match b.first() {
        Some(b'-') => (true, &b[1..]),
        Some(b'+') => (false, &b[1..]),
        _ => (false, b),
    };
    if b.is_empty() {
        anyhow::bail!("empty decimal literal: {text}");
    }
    // Split off an optional exponent ("e"/"E", e.g. "1.5e3").
    let (mantissa, exp) = match b.iter().position(|&c| c == b'e' || c == b'E') {
        Some(i) => (&b[..i], std::str::from_utf8(&b[i + 1..])?.parse::<i64>()?),
        None => (b, 0),
    };
    let mut int_digits: &[u8] = mantissa;
    let mut frac_digits: &[u8] = &[];
    if let Some(dot) = mantissa.iter().position(|&c| c == b'.') {
        int_digits = &mantissa[..dot];
        frac_digits = &mantissa[dot + 1..];
    }
    let mut digits: Vec<u8> = Vec::with_capacity(int_digits.len() + frac_digits.len());
    digits.extend_from_slice(int_digits);
    digits.extend_from_slice(frac_digits);
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        anyhow::bail!("invalid decimal literal: {text}");
    }
    let digits: Vec<u8> = digits.into_iter().skip_while(|&d| d == b'0').collect();
    let digits = if digits.is_empty() {
        vec![b'0']
    } else {
        digits
    };

    // value * 10^scale == digits * 10^(exp + scale - fractional_digits).
    let mut value: i128 = 0;
    for &d in &digits {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(d - b'0')))
            .ok_or_else(|| anyhow::anyhow!("decimal overflow: {text}"))?;
    }
    let shift = i128::from(exp) + i128::from(scale) - frac_digits.len() as i128;
    if shift != 0 {
        let magnitude = shift.unsigned_abs();
        if magnitude > u128::from(u32::MAX) {
            anyhow::bail!("decimal overflow: {text}");
        }
        let power = 10i128
            .checked_pow(magnitude as u32)
            .ok_or_else(|| anyhow::anyhow!("decimal overflow: {text}"))?;
        if shift > 0 {
            value = value
                .checked_mul(power)
                .ok_or_else(|| anyhow::anyhow!("decimal overflow: {text}"))?;
        } else {
            let (q, r) = (value / power, value % power);
            value = if r >= power - r { q + 1 } else { q };
        }
    }
    Ok(if neg { -value } else { value })
}

/// Convert one field value according to the column kind.
fn value_for(c: &ColumnSpec, raw: &Json) -> anyhow::Result<Value> {
    match c.kind {
        ColumnKind::Ts => {
            let ms = match raw {
                Json::Null => anyhow::bail!("column {}: missing value", c.name),
                Json::Number(n) => n.as_i64().ok_or_else(|| {
                    anyhow::anyhow!("column {}: ts integer out of i64 range: {n}", c.name)
                })?,
                Json::String(s) => parse_ts_ms(s)?,
                other => anyhow::bail!(
                    "column {}: ts must be an integer epoch-ms or RFC3339 string, got {}",
                    c.name,
                    other
                ),
            };
            Ok(Value {
                value_data: Some(ValueData::TimestampMillisecondValue(ms)),
            })
        }
        ColumnKind::String => {
            // JSON null -> SQL NULL (unset value_data -> ValueRef::Null on the
            // server). This matters for nullable STRING columns whose stored
            // value must round-trip as NULL, not "" (correctness oracle).
            if raw.is_null() {
                Ok(Value { value_data: None })
            } else {
                Ok(str_val(&string_of(raw)))
            }
        }
        ColumnKind::Int64 => {
            if raw.is_null() {
                Ok(Value { value_data: None })
            } else {
                Ok(int64_val(int64_of(raw)))
            }
        }
        ColumnKind::Date => {
            if raw.is_null() {
                Ok(Value { value_data: None })
            } else {
                Ok(Value {
                    value_data: Some(ValueData::DateValue(date_of(raw))),
                })
            }
        }
        ColumnKind::Decimal => {
            if raw.is_null() {
                Ok(Value { value_data: None })
            } else {
                let dec = decimal_of(raw, c.scale)
                    .map_err(|e| anyhow::anyhow!("column {}: {e:#}", c.name))?;
                Ok(Value {
                    value_data: Some(ValueData::Decimal128Value(dec)),
                })
            }
        }
        // Same trick as the built-in `attrs` column: send the JSON as a string
        // and let the server convert it to JSONB.
        ColumnKind::Json => Ok(str_val(&string_of(raw))),
    }
}

/// Parse one JSONL line into a Row (values in spec order).
fn line_to_row(line: &str, specs: &[ColumnSpec]) -> anyhow::Result<Row> {
    let v: Json = serde_json::from_str(line)?;
    let values = specs
        .iter()
        .map(|c| value_for(c, &v[c.name.as_str()]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Row { values })
}

/// Days since epoch for a civil date (valid 1900-2100).
fn civil_to_days(y: i64, m: i64, d: i64) -> i64 {
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parse "YYYY-MM-DD" into days since epoch (Date32 value).
fn parse_date_days(s: &str) -> anyhow::Result<i32> {
    let digits = |b: &[u8]| -> anyhow::Result<i64> {
        let s = std::str::from_utf8(b)?;
        s.parse::<i64>().map_err(Into::into)
    };
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        anyhow::bail!("bad date format (expected YYYY-MM-DD): {s}");
    }
    let y = digits(&b[0..4])?;
    let m = digits(&b[5..7])?;
    let d = digits(&b[8..10])?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        anyhow::bail!("bad date value: {s}");
    }
    Ok(civil_to_days(y, m, d) as i32)
}

/// Parse RFC3339 with millisecond precision and timezone offset:
/// "YYYY-MM-DDTHH:MM:SS[.mmm][Z|±HH:MM|±HHMM]". `Z` and explicit offsets are
/// converted to UTC epoch-millis. Plain integer epoch-millis values are
/// handled by the caller, not here.
fn parse_ts_ms(s: &str) -> anyhow::Result<i64> {
    let digits = |b: &[u8]| -> anyhow::Result<i64> {
        let s = std::str::from_utf8(b)?;
        s.parse::<i64>().map_err(Into::into)
    };
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        anyhow::bail!("bad ts format: {s}");
    }
    let year = digits(&b[0..4])?;
    let month = digits(&b[5..7])?;
    let day = digits(&b[8..10])?;
    let hour = digits(&b[11..13])?;
    let min = digits(&b[14..16])?;
    let sec = digits(&b[17..19])?;
    let mut ms = 0i64;
    let mut end = 19;
    if b.len() > 20 && b[19] == b'.' {
        let frac_end = b[20..]
            .iter()
            .position(|&c| c == b'Z' || c == b'+' || c == b'-')
            .unwrap_or(b.len() - 20)
            + 20;
        let frac = std::str::from_utf8(&b[20..frac_end])?;
        let frac_padded = format!("{frac:0<3}");
        ms = frac_padded[..3].parse::<i64>()?;
        end = frac_end;
    }
    // Parse the trailing timezone: "Z" (UTC) or a ±HH:MM / ±HHMM offset, and
    // normalize the local time to UTC (epoch_ms = local - offset).
    let mut offset_min = 0i64;
    if end < b.len() {
        match b[end] {
            b'Z' => {
                if end + 1 != b.len() {
                    anyhow::bail!("bad ts suffix: {s}");
                }
            }
            b'+' | b'-' => {
                let sign = if b[end] == b'-' { -1 } else { 1 };
                let off = std::str::from_utf8(&b[end + 1..])?;
                let (hh, mm) = match off.len() {
                    5 if off.as_bytes()[2] == b':' => (
                        digits(&off.as_bytes()[0..2])?,
                        digits(&off.as_bytes()[3..5])?,
                    ),
                    4 => (
                        digits(&off.as_bytes()[0..2])?,
                        digits(&off.as_bytes()[2..4])?,
                    ),
                    _ => anyhow::bail!("bad ts offset (expected ±HH:MM or ±HHMM): {s}"),
                };
                if hh > 23 || mm > 59 {
                    anyhow::bail!("bad ts offset value: {s}");
                }
                offset_min = sign * (hh * 60 + mm);
            }
            _ => anyhow::bail!("bad ts suffix: {s}"),
        }
    } else {
        anyhow::bail!("bad ts suffix: {s}");
    }
    let days = civil_to_days(year, month, day);
    let epoch_ms =
        days * 86400 * 1000 + (hour * 3600 + min * 60 + sec) * 1000 + ms - offset_min * 60_000;
    Ok(epoch_ms)
}

#[derive(clap::Parser)]
struct Args {
    #[clap(long)]
    endpoint: String,
    #[clap(long)]
    data: PathBuf,
    #[clap(long, default_value = "app_logs")]
    table: String,
    /// Database to write into (must already exist).
    #[clap(long, default_value = "public")]
    database: String,
    #[clap(long, default_value = "10000")]
    batch_size: usize,
    #[clap(long, default_value = "8")]
    concurrency: usize,
    /// JSON array of {"name": "...", "kind": "ts"|"string"|"int64"|"date"|"decimal"|"json"}
    /// describing the target table columns. Omit to use the built-in app_logs schema.
    #[clap(long)]
    columns_file: Option<PathBuf>,
    /// Skip JSONL lines that fail to parse (printed as warnings) instead of
    /// aborting the whole load.
    #[clap(long)]
    skip_bad_lines: bool,
}

/// Read a JSONL file into row batches, forwarding each full batch to `tx`.
/// Returns `(parsed_rows, skipped_lines)`. With `skip_bad_lines` set, lines
/// that fail to parse are warned about and counted instead of failing the
/// whole load.
fn read_lines(
    path: &PathBuf,
    specs: &[ColumnSpec],
    batch_size: usize,
    skip_bad_lines: bool,
    tx: &mpsc::SyncSender<Vec<Row>>,
) -> anyhow::Result<(u64, u64)> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::with_capacity(1 << 20, file);
    use std::io::BufRead;
    let mut batch: Vec<Row> = Vec::with_capacity(batch_size);
    let mut total: u64 = 0;
    let mut skipped: u64 = 0;
    let mut line_no: u64 = 0;
    for line in reader.lines() {
        let line = line?;
        line_no += 1;
        if line.trim().is_empty() {
            continue;
        }
        match line_to_row(&line, specs) {
            Ok(row) => {
                batch.push(row);
                total += 1;
            }
            Err(e) if skip_bad_lines => {
                eprintln!("gtdb-log-loader: skipping bad line {line_no}: {e:#}");
                skipped += 1;
            }
            Err(e) => return Err(e),
        }
        if batch.len() >= batch_size {
            tx.send(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        tx.send(batch)?;
    }
    Ok((total, skipped))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = <Args as clap::Parser>::parse();
    let specs = match &args.columns_file {
        Some(p) => load_specs(p)?,
        None => default_specs(),
    };
    let client = Client::with_urls([args.endpoint.clone()]);
    let db = client::Database::new_with_dbname(args.database.clone(), client);

    let (tx, rx) = mpsc::sync_channel::<Vec<Row>>(args.concurrency * 2);
    let rx = Arc::new(Mutex::new(rx));
    let specs_for_reader = specs.clone();
    let reader = std::thread::spawn(move || -> anyhow::Result<(u64, u64)> {
        read_lines(
            &args.data,
            &specs_for_reader,
            args.batch_size,
            args.skip_bad_lines,
            &tx,
        )
    });

    let schema = make_schema(&specs);
    let table = args.table.clone();
    // Engine-option hints forwarded to the server. The keys/values correspond
    // to the constants in `src/store-api/src/mito_engine_options.rs`
    // (APPEND_MODE_KEY "append_mode", COMPACTION_TYPE "compaction.type", and
    // TWCS_TIME_WINDOW "compaction.twcs.time_window"); the server derives the
    // remaining compaction configuration from `compaction.type`.
    let hints: Vec<(&str, &str)> = vec![
        ("append_mode", "true"),
        ("compaction.type", "twcs"),
        ("compaction.twcs.time_window", "2h"),
    ];

    let start = Instant::now();
    let mut rows_written: u64 = 0;
    let mut batches: u64 = 0;
    let mut set = JoinSet::new();
    for _ in 0..args.concurrency {
        let rx = Arc::clone(&rx);
        let db = db.clone();
        let schema = schema.clone();
        let table = table.clone();
        let hints = hints.clone();
        set.spawn(async move {
            let mut written = 0u64;
            let mut sent = 0u64;
            loop {
                let batch = {
                    let guard = rx.lock().unwrap();
                    match guard.recv() {
                        Ok(b) => b,
                        Err(_) => break,
                    }
                };
                let req = RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: table.clone(),
                        rows: Some(Rows {
                            schema: schema.clone(),
                            rows: batch,
                        }),
                    }],
                };
                // `row_inserts_with_hints` returns the number of rows the
                // server reports as affected; accumulate that (not the local
                // batch length) so the final check compares against the
                // server's accounting.
                written += db.row_inserts_with_hints(req, &hints).await? as u64;
                sent += 1;
            }
            Ok::<(u64, u64), anyhow::Error>((written, sent))
        });
    }
    while let Some(res) = set.join_next().await {
        let (written, sent) = res??;
        rows_written += written;
        batches += sent;
    }
    let (total, skipped) = reader.join().unwrap()?;
    let elapsed = start.elapsed();
    let skipped_note = if skipped > 0 {
        format!(", {skipped} bad lines skipped")
    } else {
        String::new()
    };
    println!(
        "gtdb-log-loader: wrote {rows_written} rows (source {total}{skipped_note}) in {:.1}s ({:.0} rows/s)",
        elapsed.as_secs_f64(),
        rows_written as f64 / elapsed.as_secs_f64()
    );
    if rows_written != total {
        anyhow::bail!(
            "rows written ({rows_written}) != source line count ({total}) after {batches} batches: \
             the server reported fewer affected rows than sent; duplicate \
             (timestamp, tag) rows are deduplicated unless the table uses \
             append_mode"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, kind: ColumnKind) -> ColumnSpec {
        ColumnSpec {
            name: name.to_string(),
            kind,
            precision: 38,
            scale: if kind == ColumnKind::Decimal { 6 } else { 10 },
        }
    }

    /// 23-column agent_observations schema (logbench-agent-observability).
    fn agent_specs() -> Vec<ColumnSpec> {
        vec![
            spec("event_time", ColumnKind::Ts),
            spec("biz_date", ColumnKind::Date),
            spec("trace_id", ColumnKind::String),
            spec("session_id", ColumnKind::String),
            spec("observation_id", ColumnKind::String),
            spec("parent_observation_id", ColumnKind::String),
            spec("type", ColumnKind::String),
            spec("status", ColumnKind::String),
            spec("tenant", ColumnKind::String),
            spec("app", ColumnKind::String),
            spec("environment", ColumnKind::String),
            spec("task_category", ColumnKind::String),
            spec("trace_archetype", ColumnKind::String),
            spec("model", ColumnKind::String),
            spec("tool_name", ColumnKind::String),
            spec("input", ColumnKind::String),
            spec("output", ColumnKind::String),
            spec("seq_no", ColumnKind::Int64),
            spec("input_tokens", ColumnKind::Int64),
            spec("output_tokens", ColumnKind::Int64),
            spec("latency_ms", ColumnKind::Int64),
            spec("total_cost", ColumnKind::Decimal),
            spec("payload", ColumnKind::Json),
        ]
    }

    #[test]
    fn test_agent_observations_line_to_row() {
        let specs = agent_specs();
        let schema = make_schema(&specs);
        assert_eq!(schema.len(), 23);
        // First column is the time index; total_cost is DECIMAL128; payload JSON.
        assert_eq!(
            schema[0].datatype,
            ColumnDataType::TimestampMillisecond as i32
        );
        assert_eq!(schema[0].semantic_type, SemanticType::Timestamp as i32);
        assert_eq!(schema[1].datatype, ColumnDataType::Date as i32);
        assert_eq!(schema[17].datatype, ColumnDataType::Int64 as i32);
        assert_eq!(schema[21].datatype, ColumnDataType::Decimal128 as i32);
        assert_eq!(schema[22].datatype, ColumnDataType::Json as i32);

        let line = r#"{"event_time": 1712743200123, "biz_date": "2024-04-10", "trace_id": "trace_1", "session_id": null, "observation_id": "obs_1", "parent_observation_id": null, "type": "GENERATION", "status": "ok", "tenant": "t1", "app": "a1", "environment": "prod", "task_category": "coding_devops", "trace_archetype": null, "model": "coder-large", "tool_name": null, "input": "hi there", "output": "hello", "seq_no": 7, "input_tokens": 100, "output_tokens": 200, "latency_ms": 1240, "total_cost": 0.0184, "payload": {"provider": {"stop_reason": "tool_use", "cache_hit": false}}}"#;
        let row = line_to_row(line, &specs).unwrap();
        assert_eq!(row.values.len(), 23);

        // event_time: integer epoch ms.
        match &row.values[0].value_data {
            Some(ValueData::TimestampMillisecondValue(ms)) => assert_eq!(*ms, 1712743200123),
            other => panic!("expected TimestampMillisecondValue, got {other:?}"),
        }
        // biz_date: "2024-04-10" -> 19823 days since epoch.
        match &row.values[1].value_data {
            Some(ValueData::DateValue(d)) => assert_eq!(*d, 19823),
            other => panic!("expected DateValue, got {other:?}"),
        }
        // Strings, including null -> "".
        match &row.values[2].value_data {
            Some(ValueData::StringValue(s)) => assert_eq!(s, "trace_1"),
            other => panic!("expected StringValue, got {other:?}"),
        }
        match &row.values[3].value_data {
            None => {}
            other => panic!("expected NULL for null session_id, got {other:?}"),
        }
        match &row.values[12].value_data {
            None => {}
            other => panic!("expected NULL for null trace_archetype, got {other:?}"),
        }
        // Int64 columns.
        match &row.values[17].value_data {
            Some(ValueData::I64Value(v)) => assert_eq!(*v, 7),
            other => panic!("expected I64Value, got {other:?}"),
        }
        match &row.values[20].value_data {
            Some(ValueData::I64Value(v)) => assert_eq!(*v, 1240),
            other => panic!("expected I64Value, got {other:?}"),
        }
        // Decimal: 0.0184 * 10^6 = 18_400 (column scale 6).
        match &row.values[21].value_data {
            Some(ValueData::Decimal128Value(d)) => {
                assert_eq!((d.hi, d.lo), (0, 18_400));
            }
            other => panic!("expected Decimal128Value, got {other:?}"),
        }
        // Json payload serialized to a string (server converts to JSONB).
        match &row.values[22].value_data {
            Some(ValueData::StringValue(s)) => {
                assert!(s.contains("stop_reason"), "payload serialized: {s}");
            }
            other => panic!("expected StringValue, got {other:?}"),
        }
    }

    #[test]
    fn test_agent_observations_nulls_and_int_decimal() {
        let specs = agent_specs();
        // Nullable fields as JSON null; int decimal (3 -> 3 * 10^6).
        let line = r#"{"event_time": 1712743200124, "biz_date": null, "trace_id": null, "session_id": "s", "observation_id": null, "parent_observation_id": null, "type": null, "status": null, "tenant": null, "app": null, "environment": null, "task_category": null, "trace_archetype": null, "model": null, "tool_name": null, "input": null, "output": null, "seq_no": null, "input_tokens": null, "output_tokens": null, "latency_ms": null, "total_cost": 3, "payload": {"a": [1, 2, 3]}}"#;
        let row = line_to_row(line, &specs).unwrap();
        // null int -> SQL NULL (unset value_data).
        assert!(row.values[17].value_data.is_none(), "seq_no should be NULL");
        // null date -> SQL NULL.
        assert!(
            row.values[1].value_data.is_none(),
            "biz_date should be NULL"
        );
        // null string -> SQL NULL.
        assert!(
            row.values[2].value_data.is_none(),
            "trace_id should be NULL"
        );
        // non-null string still present.
        match &row.values[3].value_data {
            Some(ValueData::StringValue(s)) => assert_eq!(s, "s"),
            other => panic!("expected StringValue(s), got {other:?}"),
        }
        // int decimal: 3 * 10^6 = 3_000_000 (column scale 6).
        match &row.values[21].value_data {
            Some(ValueData::Decimal128Value(d)) => {
                assert_eq!((d.hi, d.lo), (0, 3_000_000));
            }
            other => panic!("expected Decimal128Value, got {other:?}"),
        }
    }

    #[test]
    fn test_ts_accepts_rfc3339_and_integer() {
        let specs = vec![spec("ts", ColumnKind::Ts), spec("msg", ColumnKind::String)];

        // Integer epoch ms.
        let row = line_to_row(r#"{"ts": 1712743200123, "msg": "a"}"#, &specs).unwrap();
        match &row.values[0].value_data {
            Some(ValueData::TimestampMillisecondValue(ms)) => assert_eq!(*ms, 1712743200123),
            other => panic!("expected TimestampMillisecondValue, got {other:?}"),
        }

        // RFC3339 string with fraction.
        let row = line_to_row(r#"{"ts": "2024-04-10T10:00:00.123Z", "msg": "b"}"#, &specs).unwrap();
        match &row.values[0].value_data {
            Some(ValueData::TimestampMillisecondValue(ms)) => assert_eq!(*ms, 1712743200123),
            other => panic!("expected TimestampMillisecondValue, got {other:?}"),
        }

        // Missing ts must fail.
        assert!(line_to_row(r#"{"msg": "c"}"#, &specs).is_err());
    }

    #[test]
    fn test_default_app_logs_backward_compat() {
        let specs = default_specs();
        assert_eq!(specs.len(), 10);
        let schema = make_schema(&specs);
        assert_eq!(schema[0].column_name, "ts");
        assert_eq!(schema[9].column_name, "attrs");
        assert_eq!(schema[9].datatype, ColumnDataType::Json as i32);

        let line = r#"{"ts": "2025-03-21T09:00:00.123Z", "event_id": "e1", "service": "svc", "host": "h1", "region": "r1", "level": "info", "trace_id": "t1", "span_id": "s1", "message": "hello", "attrs": {"a": 1}}"#;
        let row = line_to_row(line, &specs).unwrap();
        assert_eq!(row.values.len(), 10);
        match &row.values[0].value_data {
            Some(ValueData::TimestampMillisecondValue(ms)) => {
                assert_eq!(*ms, 1742547600123);
            }
            other => panic!("expected TimestampMillisecondValue, got {other:?}"),
        }
        match &row.values[9].value_data {
            Some(ValueData::StringValue(s)) => {
                assert!(s.contains("\"a\":1") || s.contains("\"a\": 1"))
            }
            other => panic!("expected StringValue, got {other:?}"),
        }
    }

    /// Write `contents` to a unique temp file (no tempfile dev-dependency).
    fn temp_jsonl(contents: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "gtdb-log-loader-test-{}-{n}.jsonl",
            std::process::id()
        ));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn test_skip_bad_lines() {
        let specs = vec![spec("ts", ColumnKind::Ts), spec("msg", ColumnKind::String)];
        let path = temp_jsonl(
            "{\"ts\": 1, \"msg\": \"ok\"}\nnot json\n{\"ts\": 2, \"msg\": \"ok too\"}\n\n",
        );
        let (tx, rx) = mpsc::sync_channel::<Vec<Row>>(4);
        let (total, skipped) = read_lines(&path, &specs, 10, true, &tx).unwrap();
        drop(tx);
        let rows: usize = rx.iter().map(|b| b.len()).sum();
        std::fs::remove_file(&path).ok();
        assert_eq!(total, 2, "bad lines must not count towards total");
        assert_eq!(skipped, 1);
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_skip_bad_lines_strict_by_default() {
        let specs = vec![spec("ts", ColumnKind::Ts)];
        let path = temp_jsonl("{\"ts\": 1}\nnot json\n");
        let (tx, _rx) = mpsc::sync_channel::<Vec<Row>>(4);
        // Without `--skip-bad-lines` the parse error must abort the load
        // instead of being skipped.
        assert!(read_lines(&path, &specs, 10, false, &tx).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_ts_rfc3339_with_timezone_offset() {
        // Local time 2024-04-10T10:00:00+08:00 == 02:00:00Z.
        assert_eq!(
            parse_ts_ms("2024-04-10T10:00:00+08:00").unwrap(),
            1712714400000
        );
        // Local time 2024-04-10T10:00:00-05:00 == 15:00:00Z.
        assert_eq!(
            parse_ts_ms("2024-04-10T10:00:00-05:00").unwrap(),
            1712761200000
        );
        // Offset combined with fractional seconds.
        assert_eq!(
            parse_ts_ms("2024-04-10T10:00:00.123+08:00").unwrap(),
            1712714400123
        );
        assert_eq!(
            parse_ts_ms("2024-04-10T10:00:00.123-05:00").unwrap(),
            1712761200123
        );
        // Compact +HHMM offset.
        assert_eq!(
            parse_ts_ms("2024-04-10T10:00:00+0800").unwrap(),
            1712714400000
        );
        // Z behavior unchanged.
        assert_eq!(parse_ts_ms("2024-04-10T10:00:00Z").unwrap(), 1712743200000);
        // Missing or malformed offsets are rejected.
        assert!(parse_ts_ms("2024-04-10T10:00:00").is_err());
        assert!(parse_ts_ms("2024-04-10T10:00:00+8:00").is_err());
        assert!(parse_ts_ms("2024-04-10T10:00:00+08:60").is_err());
    }

    #[test]
    fn test_decimal_negative_and_long_precision() {
        let specs = vec![spec("c", ColumnKind::Decimal)];
        let i128_of = |d: &Decimal128| ((d.hi as i128) << 64) | (d.lo as u64 as i128);

        // Negative fixed-point value, as JSON number and as JSON string.
        let row = line_to_row(r#"{"c": -0.0184}"#, &specs).unwrap();
        match &row.values[0].value_data {
            Some(ValueData::Decimal128Value(d)) => assert_eq!(i128_of(d), -18_400),
            other => panic!("expected Decimal128Value, got {other:?}"),
        }
        let row = line_to_row(r#"{"c": "-0.0184"}"#, &specs).unwrap();
        match &row.values[0].value_data {
            Some(ValueData::Decimal128Value(d)) => assert_eq!(i128_of(d), -18_400),
            other => panic!("expected Decimal128Value, got {other:?}"),
        }
        // Negative integer with scale 6.
        let row = line_to_row(r#"{"c": -3}"#, &specs).unwrap();
        match &row.values[0].value_data {
            Some(ValueData::Decimal128Value(d)) => assert_eq!(i128_of(d), -3_000_000),
            other => panic!("expected Decimal128Value, got {other:?}"),
        }

        // 38 significant digits (32 integer + 6 fractional) survive intact:
        // the encoding path never round-trips through f64.
        let row = line_to_row(
            r#"{"c": "12345678901234567890123456789012.345678"}"#,
            &specs,
        )
        .unwrap();
        let expected: i128 = "12345678901234567890123456789012345678".parse().unwrap();
        match &row.values[0].value_data {
            Some(ValueData::Decimal128Value(d)) => assert_eq!(i128_of(d), expected),
            other => panic!("expected Decimal128Value, got {other:?}"),
        }

        // More fractional digits than the column scale rounds half away from
        // zero: 0.0184567 * 10^6 -> 18457.
        let row = line_to_row(r#"{"c": "0.0184567"}"#, &specs).unwrap();
        match &row.values[0].value_data {
            Some(ValueData::Decimal128Value(d)) => assert_eq!(i128_of(d), 18_457),
            other => panic!("expected Decimal128Value, got {other:?}"),
        }
    }
}
