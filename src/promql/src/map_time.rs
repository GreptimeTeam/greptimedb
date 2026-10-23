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

//! `time()` and the date functions over evaluated series, following Prometheus `funcTime` and
//! `dateWrapper`.

use datafusion::arrow::array::Float64Array;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::samples::{Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::grid_series::external;

#[cfg(test)]
mod tests;

/// A PromQL date function: `minute(v)`, `year()` and so on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateFunction {
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
    Month,
    Year,
}

/// `start()`, `end()`, `range()` and `step()`: constants of a query, which Prometheus folds into
/// number literals before evaluating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryContextFunction {
    Start,
    End,
    Range,
    Step,
}

impl QueryContextFunction {
    /// The value for a query from `start_ms` to `end_ms` every `step_ms`. `step()` is 0 when the
    /// start and end are equal, as for an instant query.
    pub fn value(self, start_ms: i64, end_ms: i64, step_ms: i64) -> f64 {
        match self {
            Self::Start => start_ms as f64 / 1000.0,
            Self::End => end_ms as f64 / 1000.0,
            Self::Range => duration_seconds(end_ms.saturating_sub(start_ms)),
            Self::Step if start_ms == end_ms => 0.0,
            Self::Step => duration_seconds(step_ms),
        }
    }
}

/// Go's `time.Duration.Seconds` of `ms` milliseconds: whole seconds plus the remainder over 1e9,
/// two roundings rather than one division. The nanosecond count saturates as `time.Time.Sub` does.
fn duration_seconds(ms: i64) -> f64 {
    let nanos = ms.saturating_mul(1_000_000);
    (nanos / 1_000_000_000) as f64 + (nanos % 1_000_000_000) as f64 / 1e9
}

/// `time()`: the evaluation time in seconds at every point of the schema's grid, unlabelled.
pub fn time(schema: &SeriesSchema) -> Result<Series> {
    eval_time_series(schema, |ms| ms as f64 / 1000.0)
}

/// A date function without argument, which reads the evaluation time: one unlabelled series
/// over the whole grid.
pub fn date_of_eval_time(function: DateFunction, schema: &SeriesSchema) -> Result<Series> {
    // Prometheus truncates the timestamp to whole seconds towards zero.
    eval_time_series(schema, |ms| function.apply(ms / 1000))
}

fn eval_time_series(schema: &SeriesSchema, value: impl Fn(i64) -> f64) -> Result<Series> {
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("time functions require an evaluation grid".into())
    })?;
    let values: Float64Array = grid
        .timestamps()
        .map_err(external)?
        .iter()
        .map(|&ms| value(ms))
        .collect();
    let range = GridRange::try_new(grid.clone(), 0, values.len()).map_err(external)?;
    let fields = vec![Samples::from_float(values); schema.fields.len()];
    Series::try_new(
        schema,
        SeriesLabels::try_new(None, Vec::new()).map_err(external)?,
        TimeAxis::Grid(range),
        SeriesFields::try_from_vec(fields).map_err(external)?,
    )
    .map_err(external)
}

/// Go's `int64(f)` on amd64: NaN and values out of range become `i64::MIN`.
fn go_int64(value: f64) -> i64 {
    if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value) {
        value as i64
    } else {
        i64::MIN
    }
}

const SECONDS_PER_DAY: u64 = 86_400;
const MARCH_THRU_DECEMBER: u32 = 306;
const ABSOLUTE_YEARS: u64 = 292_277_022_400;
/// Go's `unixToInternal + internalToAbsolute`.
const UNIX_TO_ABSOLUTE: i64 = (1969 * 365 + 1969 / 4 - 1969 / 100 + 1969 / 400) * 86_400
    + (ABSOLUTE_YEARS as i64 / 400 * 146_097 + MARCH_THRU_DECEMBER as i64) * 86_400;

impl DateFunction {
    /// The PromQL name of the function.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::DayOfMonth => "day_of_month",
            Self::DayOfWeek => "day_of_week",
            Self::DayOfYear => "day_of_year",
            Self::DaysInMonth => "days_in_month",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Month => "month",
            Self::Year => "year",
        }
    }

    /// The component of `seconds` read as a Unix time, as Prometheus truncates it with `int64`.
    pub(crate) fn of_seconds(self, seconds: f64) -> f64 {
        self.apply(go_int64(seconds))
    }

    /// The component of Unix second `seconds` in UTC, computed the way Go's `time` package does,
    /// including its wrapping arithmetic for instants out of its range.
    fn apply(self, seconds: i64) -> f64 {
        let abs = seconds.wrapping_add(UNIX_TO_ABSOLUTE) as u64;
        let days = abs / SECONDS_PER_DAY;
        let value = match self {
            Self::Hour => (abs % SECONDS_PER_DAY / 3600) as i64,
            Self::Minute => (abs % 3600 / 60) as i64,
            Self::DayOfWeek => ((days + 3) % 7) as i64,
            Self::Year => civil(days).year,
            Self::Month => civil(days).month as i64,
            Self::DayOfMonth => civil(days).day as i64,
            Self::DayOfYear => civil(days).year_day,
            Self::DaysInMonth => {
                let Civil { year, month, .. } = civil(days);
                let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
                match month {
                    2 if leap => 29,
                    2 => 28,
                    4 | 6 | 9 | 11 => 30,
                    _ => 31,
                }
            }
        };
        value as f64
    }
}

struct Civil {
    year: i64,
    month: u32,
    day: u32,
    year_day: i64,
}

/// Go's `absDays.split`, `date` and `yearYday`: days since March 1 of year `-ABSOLUTE_YEARS`.
fn civil(days: u64) -> Civil {
    let d = days.wrapping_mul(4).wrapping_add(3);
    let century = d / 146_097;
    let cd = (d % 146_097) as u32 | 3;
    let product = 2_939_745u64 * cd as u64;
    let cyear = (product >> 32) as u32;
    let ayday = product as u32 / 2_939_745 / 4;

    let m = 2141 * ayday + 197_913;
    let jan_feb = u32::from(ayday >= MARCH_THRU_DECEMBER);
    let leap = cyear.is_multiple_of(4) && (cyear != 0 || century.is_multiple_of(4));
    Civil {
        year: century.wrapping_mul(100).wrapping_sub(ABSOLUTE_YEARS) as i64
            + cyear as i64
            + jan_feb as i64,
        month: (m >> 16) - jan_feb * 12,
        day: 1 + (m & 0xFFFF) / 2141,
        year_day: ayday as i64 + (1 + 31 + 28) + i64::from(leap && jan_feb == 0)
            - 365 * jan_feb as i64,
    }
}
