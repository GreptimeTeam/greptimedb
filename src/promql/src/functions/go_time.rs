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

//! PromQL date functions (`minute`, `hour`, `month`, `year`, `day_of_month`,
//! `day_of_week`, `day_of_year`, `days_in_month`) applied to sample values.
//!
//! Prometheus interprets a sample value as a Unix timestamp in seconds and
//! derives calendar fields from it with Go's `time` package. DataFusion's
//! `date_part` goes through Chrono, whose calendar stops at year 262143: a
//! finite value around 8.2e12 seconds or larger makes `date_part` return NULL,
//! and the empty-value filter then drops the sample. Prometheus instead keeps
//! the sample (e.g. `1e13` gives year 318857 and minute 46).
//!
//! This UDF ports Go's absolute-date arithmetic (golang/go `src/time/time.go`)
//! so the fields are computed from the `i64` seconds directly, with no Chrono
//! date range limit. A float-to-int cast follows Rust/Prometheus saturating
//! semantics (NaN -> 0, infinities clamp to the int64 bounds), which together
//! with Go's wrapping arithmetic reproduces Prometheus on both arm64 and amd64.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, Float64Array, Int32Array};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

// Constants from Go's time package (src/time/time.go).
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
const SECONDS_PER_HOUR: u64 = 60 * 60;
const SECONDS_PER_MINUTE: u64 = 60;
const MARCH_THRU_DECEMBER: u64 = 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31; // 306
const ABSOLUTE_YEARS: u64 = 292_277_022_400;
/// `unixToInternal + internalToAbsolute`, the offset added to Unix seconds to
/// obtain Go's absolute seconds. Computed from the Go constants:
/// `(1969*365 + 1969/4 - 1969/100 + 1969/400)*86400
///  + (292277022400*365.2425 + 306)*86400`.
const UNIX_TO_ABSOLUTE: u64 = 9_223_372_028_741_760_000;
const WEDNESDAY: u64 = 3;

/// Calendar fields of a Unix-seconds sample value, as Prometheus reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateFields {
    year: i64,
    month: i64,
    day: i64,
    day_of_year: i64,
    day_of_week: i64,
    hour: i64,
    minute: i64,
    days_in_month: i64,
}

/// Compute all calendar fields for a single sample value.
///
/// `value as i64` is Rust's saturating float-to-int cast: NaN -> 0, +Inf ->
/// i64::MAX, -Inf -> i64::MIN, and out-of-range values clamp. This matches the
/// arm64 behavior Prometheus observes. The rest is a faithful port of Go's
/// `absSeconds`/`absDays` arithmetic using wrapping `u64` math.
fn date_fields(value: f64) -> DateFields {
    let sec = value as i64;
    // Go: absSeconds(sec + (unixToInternal + internalToAbsolute)). The addition
    // wraps in uint64 for saturated inputs.
    let abs = (sec as u64).wrapping_add(UNIX_TO_ABSOLUTE);

    let days = abs / SECONDS_PER_DAY;
    let day_sec = abs % SECONDS_PER_DAY;
    let hour = day_sec / SECONDS_PER_HOUR;
    let minute = (day_sec % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;

    // absDays.split(): century, cyear (year within century), ayday (absolute
    // day of year, March-based).
    let d = days.wrapping_mul(4).wrapping_add(3);
    let century = d / 146_097;
    let cd = (d % 146_097) as u32 | 3;
    let prod = 2_939_745u64 * cd as u64;
    let cyear = prod >> 32;
    let ayday = (prod & 0xFFFF_FFFF) / 2_939_745 / 4;

    let jan_feb = u64::from(ayday >= MARCH_THRU_DECEMBER);

    // absYday.split(): absolute month (March=1 .. February=12) and 1-based day.
    let dd = 2_141 * ayday + 197_913;
    let amonth = dd >> 16;
    let day = 1 + (dd & 0xFFFF) / 2_141;

    let year = (century * 100) as i64 - ABSOLUTE_YEARS as i64 + cyear as i64 + jan_feb as i64;
    let month = amonth as i64 - jan_feb as i64 * 12;

    // Proleptic Gregorian leap year, matching Go: every year divisible by 4,
    // except centuries not divisible by 400.
    let y4ok = u64::from(cyear % 4 == 0);
    let y100ok = u64::from(cyear != 0);
    let y400ok = u64::from(century % 4 == 0);
    let leap = y4ok & (y100ok | y400ok);

    // absYday.yday(): 1-based day of year.
    let day_of_year =
        ayday as i64 + (1 + 31 + 28) + (leap as i64 & !(jan_feb as i64)) - 365 * jan_feb as i64;

    // absDays.weekday(): Sunday=0 .. Saturday=6. March 1 of absolute year was
    // a Wednesday.
    let day_of_week = ((days + WEDNESDAY) % 7) as i64;

    let days_in_month = days_in_month(year, month);

    DateFields {
        year,
        month,
        day: day as i64,
        day_of_year,
        day_of_week,
        hour: hour as i64,
        minute: minute as i64,
        days_in_month,
    }
}

/// Number of days in a (possibly huge) proleptic Gregorian month, matching Go's
/// `32 - time.Date(year, month, 32, ...).Day()`.
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => unreachable!("month out of range: {month}"),
    }
}

/// Which calendar field a `GoDatePart` instance extracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DateUnit {
    Minute,
    Hour,
    Month,
    Year,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
}

impl DateUnit {
    fn udf_name(self) -> &'static str {
        match self {
            DateUnit::Minute => "prom_go_date_minute",
            DateUnit::Hour => "prom_go_date_hour",
            DateUnit::Month => "prom_go_date_month",
            DateUnit::Year => "prom_go_date_year",
            DateUnit::DayOfMonth => "prom_go_date_day_of_month",
            DateUnit::DayOfWeek => "prom_go_date_day_of_week",
            DateUnit::DayOfYear => "prom_go_date_day_of_year",
            DateUnit::DaysInMonth => "prom_go_date_days_in_month",
        }
    }

    /// `year` can reach 292277026596 and does not fit Int32; Prometheus
    /// returns it as a float. All other fields fit Int32.
    fn returns_float(self) -> bool {
        matches!(self, DateUnit::Year)
    }
}

/// Extracts one calendar field from a sample value using Go/Prometheus date
/// arithmetic. One instance per [`DateUnit`] so the return type is fixed.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GoDatePart {
    unit: DateUnit,
    signature: Signature,
}

impl GoDatePart {
    pub fn scalar_udf(unit: DateUnit) -> ScalarUDF {
        ScalarUDF::new_from_impl(Self {
            unit,
            signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
        })
    }

    /// All date-part UDFs, for registering them where plans are decoded.
    pub fn all() -> Vec<ScalarUDF> {
        [
            DateUnit::Minute,
            DateUnit::Hour,
            DateUnit::Month,
            DateUnit::Year,
            DateUnit::DayOfMonth,
            DateUnit::DayOfWeek,
            DateUnit::DayOfYear,
            DateUnit::DaysInMonth,
        ]
        .into_iter()
        .map(Self::scalar_udf)
        .collect()
    }

    fn field(&self, f: &DateFields) -> i64 {
        match self.unit {
            DateUnit::Minute => f.minute,
            DateUnit::Hour => f.hour,
            DateUnit::Month => f.month,
            DateUnit::Year => f.year,
            DateUnit::DayOfMonth => f.day,
            DateUnit::DayOfWeek => f.day_of_week,
            DateUnit::DayOfYear => f.day_of_year,
            DateUnit::DaysInMonth => f.days_in_month,
        }
    }
}

impl ScalarUDFImpl for GoDatePart {
    fn name(&self) -> &str {
        self.unit.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(if self.unit.returns_float() {
            DataType::Float64
        } else {
            DataType::Int32
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0].as_primitive::<Float64Type>();

        if self.unit.returns_float() {
            let result: Float64Array = input
                .iter()
                .map(|v| v.map(|v| self.field(&date_fields(v)) as f64))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            let result: Int32Array = input
                .iter()
                .map(|v| v.map(|v| self.field(&date_fields(v)) as i32))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(v: f64) -> DateFields {
        date_fields(v)
    }

    #[test]
    fn epoch_and_small_values() {
        let f = fields(0.0);
        assert_eq!(
            (
                f.year,
                f.month,
                f.day,
                f.hour,
                f.minute,
                f.day_of_week,
                f.day_of_year
            ),
            (1970, 1, 1, 0, 0, 4, 1)
        );
        assert_eq!(f.days_in_month, 31);

        let f = fields(120.0);
        assert_eq!((f.hour, f.minute), (0, 2));
        let f = fields(1234567890.0);
        assert_eq!(
            (
                f.year,
                f.month,
                f.day,
                f.hour,
                f.minute,
                f.day_of_week,
                f.day_of_year
            ),
            (2009, 2, 13, 23, 31, 5, 44)
        );
    }

    #[test]
    fn nan_maps_to_epoch() {
        let f = fields(f64::NAN);
        assert_eq!(
            (f.year, f.month, f.day, f.hour, f.minute),
            (1970, 1, 1, 0, 0)
        );
    }

    #[test]
    fn one_e13_is_not_dropped() {
        // The core of the maintainer's 11-04 review: Chrono returns NULL here.
        let f = fields(1e13);
        assert_eq!((f.year, f.minute, f.days_in_month), (318857, 46, 31));
    }

    #[test]
    fn saturated_values_match_prometheus() {
        for v in [f64::INFINITY, f64::NEG_INFINITY, 1e19] {
            let f = fields(v);
            assert_eq!(
                (f.year, f.hour, f.minute, f.day_of_week, f.day_of_year),
                (292277026596, 15, 30, 0, 339),
                "value {v}"
            );
        }
    }

    #[test]
    fn leap_year_days_in_month() {
        assert_eq!(fields(1_676_555_423.0).days_in_month, 28); // 2023-02
        assert_eq!(fields(1_706_769_823.0).days_in_month, 29); // 2024-02
        assert_eq!(fields(32_503_680_000.0).year, 3000);
    }
}
