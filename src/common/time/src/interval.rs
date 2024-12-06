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

use std::hash::Hash;

use arrow::datatypes::IntervalUnit as ArrowIntervalUnit;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum IntervalUnit {
    /// Indicates the number of elapsed whole months, stored as 4-byte integers.
    YearMonth,
    /// Indicates the number of elapsed days and milliseconds,
    /// stored as 2 contiguous 32-bit integers (days, milliseconds) (8-bytes in total).
    DayTime,
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// The values are stored contiguously in 16 byte blocks. Months and
    /// days are encoded as 32 bit integers and nanoseconds is encoded as a
    /// 64 bit integer. All integers are signed. Each field is independent
    /// (e.g. there is no constraint that nanoseconds have the same sign
    /// as days or that the quantity of nanoseconds represents less
    /// than a day's worth of time).
    #[default]
    MonthDayNano,
}

impl From<&ArrowIntervalUnit> for IntervalUnit {
    fn from(unit: &ArrowIntervalUnit) -> Self {
        match unit {
            ArrowIntervalUnit::YearMonth => IntervalUnit::YearMonth,
            ArrowIntervalUnit::DayTime => IntervalUnit::DayTime,
            ArrowIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}

impl From<ArrowIntervalUnit> for IntervalUnit {
    fn from(unit: ArrowIntervalUnit) -> Self {
        (&unit).into()
    }
}

// The `Value` type requires Serialize, Deserialize.
#[derive(
    Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[repr(C)]
pub struct IntervalYearMonth {
    /// Number of months
    pub months: i32,
}

impl IntervalYearMonth {
    pub fn new(months: i32) -> Self {
        Self { months }
    }

    pub fn to_i32(&self) -> i32 {
        self.months
    }

    pub fn from_i32(months: i32) -> Self {
        Self { months }
    }

    pub fn negative(&self) -> Self {
        Self::new(-self.months)
    }

    pub fn to_iso8601_string(&self) -> String {
        IntervalFormat::from(*self).to_iso8601_string()
    }
}

impl From<IntervalYearMonth> for IntervalFormat {
    fn from(interval: IntervalYearMonth) -> Self {
        IntervalFormat {
            years: interval.months / 12,
            months: interval.months % 12,
            ..Default::default()
        }
    }
}

impl From<i32> for IntervalYearMonth {
    fn from(v: i32) -> Self {
        Self::from_i32(v)
    }
}

impl From<IntervalYearMonth> for i32 {
    fn from(v: IntervalYearMonth) -> Self {
        v.to_i32()
    }
}

impl From<IntervalYearMonth> for serde_json::Value {
    fn from(v: IntervalYearMonth) -> Self {
        serde_json::Value::from(v.to_i32())
    }
}

#[derive(
    Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[repr(C)]
pub struct IntervalDayTime {
    /// Number of days
    pub days: i32,
    /// Number of milliseconds
    pub milliseconds: i32,
}

impl IntervalDayTime {
    /// The additive identity i.e. `0`.
    pub const ZERO: Self = Self::new(0, 0);

    /// The multiplicative inverse, i.e. `-1`.
    pub const MINUS_ONE: Self = Self::new(-1, -1);

    /// The maximum value that can be represented
    pub const MAX: Self = Self::new(i32::MAX, i32::MAX);

    /// The minimum value that can be represented
    pub const MIN: Self = Self::new(i32::MIN, i32::MIN);

    pub const fn new(days: i32, milliseconds: i32) -> Self {
        Self { days, milliseconds }
    }

    pub fn to_i64(&self) -> i64 {
        let d = (self.days as u64 & u32::MAX as u64) << 32;
        let m = self.milliseconds as u64 & u32::MAX as u64;
        (d | m) as i64
    }

    pub fn from_i64(value: i64) -> Self {
        let days = (value >> 32) as i32;
        let milliseconds = value as i32;
        Self { days, milliseconds }
    }

    pub fn negative(&self) -> Self {
        Self::new(-self.days, -self.milliseconds)
    }

    pub fn to_iso8601_string(&self) -> String {
        IntervalFormat::from(*self).to_iso8601_string()
    }

    pub fn as_millis(&self) -> i64 {
        self.days as i64 * MS_PER_DAY + self.milliseconds as i64
    }
}

impl From<i64> for IntervalDayTime {
    fn from(v: i64) -> Self {
        Self::from_i64(v)
    }
}

impl From<IntervalDayTime> for i64 {
    fn from(v: IntervalDayTime) -> Self {
        v.to_i64()
    }
}

impl From<IntervalDayTime> for serde_json::Value {
    fn from(v: IntervalDayTime) -> Self {
        serde_json::Value::from(v.to_i64())
    }
}

impl From<arrow::datatypes::IntervalDayTime> for IntervalDayTime {
    fn from(value: arrow::datatypes::IntervalDayTime) -> Self {
        Self {
            days: value.days,
            milliseconds: value.milliseconds,
        }
    }
}

impl From<IntervalDayTime> for arrow::datatypes::IntervalDayTime {
    fn from(value: IntervalDayTime) -> Self {
        Self {
            days: value.days,
            milliseconds: value.milliseconds,
        }
    }
}

// Millisecond convert to other time unit
pub const MS_PER_SEC: i64 = 1_000;
pub const MS_PER_MINUTE: i64 = 60 * MS_PER_SEC;
pub const MS_PER_HOUR: i64 = 60 * MS_PER_MINUTE;
pub const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;
pub const NANOS_PER_MILLI: i64 = 1_000_000;

impl From<IntervalDayTime> for IntervalFormat {
    fn from(interval: IntervalDayTime) -> Self {
        IntervalFormat {
            days: interval.days,
            hours: interval.milliseconds as i64 / MS_PER_HOUR,
            minutes: (interval.milliseconds as i64 % MS_PER_HOUR) / MS_PER_MINUTE,
            seconds: (interval.milliseconds as i64 % MS_PER_MINUTE) / MS_PER_SEC,
            microseconds: (interval.milliseconds as i64 % MS_PER_SEC) * MS_PER_SEC,
            ..Default::default()
        }
    }
}

#[derive(
    Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[repr(C)]
pub struct IntervalMonthDayNano {
    /// Number of months
    pub months: i32,
    /// Number of days
    pub days: i32,
    /// Number of nanoseconds
    pub nanoseconds: i64,
}

impl IntervalMonthDayNano {
    /// The additive identity i.e. `0`.
    pub const ZERO: Self = Self::new(0, 0, 0);

    /// The multiplicative inverse, i.e. `-1`.
    pub const MINUS_ONE: Self = Self::new(-1, -1, -1);

    /// The maximum value that can be represented
    pub const MAX: Self = Self::new(i32::MAX, i32::MAX, i64::MAX);

    /// The minimum value that can be represented
    pub const MIN: Self = Self::new(i32::MIN, i32::MIN, i64::MIN);

    pub const fn new(months: i32, days: i32, nanoseconds: i64) -> Self {
        Self {
            months,
            days,
            nanoseconds,
        }
    }

    pub fn to_i128(&self) -> i128 {
        let m = (self.months as u128 & u32::MAX as u128) << 96;
        let d = (self.days as u128 & u32::MAX as u128) << 64;
        let n = self.nanoseconds as u128 & u64::MAX as u128;
        (m | d | n) as i128
    }

    pub fn from_i128(value: i128) -> Self {
        let months = (value >> 96) as i32;
        let days = (value >> 64) as i32;
        let nanoseconds = value as i64;
        Self {
            months,
            days,
            nanoseconds,
        }
    }

    pub fn negative(&self) -> Self {
        Self::new(-self.months, -self.days, -self.nanoseconds)
    }

    pub fn to_iso8601_string(&self) -> String {
        IntervalFormat::from(*self).to_iso8601_string()
    }
}

impl From<i128> for IntervalMonthDayNano {
    fn from(v: i128) -> Self {
        Self::from_i128(v)
    }
}

impl From<IntervalMonthDayNano> for i128 {
    fn from(v: IntervalMonthDayNano) -> Self {
        v.to_i128()
    }
}

impl From<IntervalMonthDayNano> for serde_json::Value {
    fn from(v: IntervalMonthDayNano) -> Self {
        serde_json::Value::from(v.to_i128().to_string())
    }
}

impl From<arrow::datatypes::IntervalMonthDayNano> for IntervalMonthDayNano {
    fn from(value: arrow::datatypes::IntervalMonthDayNano) -> Self {
        Self {
            months: value.months,
            days: value.days,
            nanoseconds: value.nanoseconds,
        }
    }
}

impl From<IntervalMonthDayNano> for arrow::datatypes::IntervalMonthDayNano {
    fn from(value: IntervalMonthDayNano) -> Self {
        Self {
            months: value.months,
            days: value.days,
            nanoseconds: value.nanoseconds,
        }
    }
}

// Nanosecond convert to other time unit
pub const NS_PER_SEC: i64 = 1_000_000_000;
pub const NS_PER_MINUTE: i64 = 60 * NS_PER_SEC;
pub const NS_PER_HOUR: i64 = 60 * NS_PER_MINUTE;
pub const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;

impl From<IntervalMonthDayNano> for IntervalFormat {
    fn from(interval: IntervalMonthDayNano) -> Self {
        IntervalFormat {
            years: interval.months / 12,
            months: interval.months % 12,
            days: interval.days,
            hours: interval.nanoseconds / NS_PER_HOUR,
            minutes: (interval.nanoseconds % NS_PER_HOUR) / NS_PER_MINUTE,
            seconds: (interval.nanoseconds % NS_PER_MINUTE) / NS_PER_SEC,
            microseconds: (interval.nanoseconds % NS_PER_SEC) / 1_000,
        }
    }
}

pub fn interval_year_month_to_month_day_nano(interval: IntervalYearMonth) -> IntervalMonthDayNano {
    IntervalMonthDayNano {
        months: interval.months,
        days: 0,
        nanoseconds: 0,
    }
}

pub fn interval_day_time_to_month_day_nano(interval: IntervalDayTime) -> IntervalMonthDayNano {
    IntervalMonthDayNano {
        months: 0,
        days: interval.days,
        nanoseconds: interval.milliseconds as i64 * NANOS_PER_MILLI,
    }
}

/// <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-OUTPUT>
/// support postgres format, iso8601 format and sql standard format
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct IntervalFormat {
    pub years: i32,
    pub months: i32,
    pub days: i32,
    pub hours: i64,
    pub minutes: i64,
    pub seconds: i64,
    pub microseconds: i64,
}

impl IntervalFormat {
    /// All the field in the interval is 0
    pub fn is_zero(&self) -> bool {
        self.years == 0
            && self.months == 0
            && self.days == 0
            && self.hours == 0
            && self.minutes == 0
            && self.seconds == 0
            && self.microseconds == 0
    }

    /// Determine if year or month exist
    pub fn has_year_month(&self) -> bool {
        self.years != 0 || self.months != 0
    }

    /// Determine if day exists
    pub fn has_day(&self) -> bool {
        self.days != 0
    }

    /// Determine time part(includes hours, minutes, seconds, microseconds) is positive
    pub fn has_time_part_positive(&self) -> bool {
        self.hours > 0 || self.minutes > 0 || self.seconds > 0 || self.microseconds > 0
    }

    // time part means hours, minutes, seconds, microseconds
    pub fn has_time_part(&self) -> bool {
        self.hours != 0 || self.minutes != 0 || self.seconds != 0 || self.microseconds != 0
    }

    /// Convert IntervalFormat to iso8601 format string
    /// ISO pattern - PnYnMnDTnHnMnS
    /// for example: P1Y2M3DT4H5M6.789S
    pub fn to_iso8601_string(&self) -> String {
        if self.is_zero() {
            return "PT0S".to_string();
        }
        let fract_str = match self.microseconds {
            0 => String::default(),
            _ => format!(".{:06}", self.microseconds)
                .trim_end_matches('0')
                .to_string(),
        };
        format!(
            "P{}Y{}M{}DT{}H{}M{}{}S",
            self.years, self.months, self.days, self.hours, self.minutes, self.seconds, fract_str
        )
    }

    /// Convert IntervalFormat to sql standard format string
    /// SQL standard pattern `- [years - months] [days] [hours:minutes:seconds[.fractional seconds]]`
    /// for example: 1-2 3:4:5.678
    pub fn to_sql_standard_string(self) -> String {
        if self.is_zero() {
            "0".to_string()
        } else if !self.has_time_part() && !self.has_day() {
            get_year_month(self.months, self.years, true)
        } else if !self.has_time_part() && !self.has_year_month() {
            format!("{} 0:00:00", self.days)
        } else if !self.has_year_month() && !self.has_day() {
            get_time_part(
                self.hours,
                self.minutes,
                self.seconds,
                self.microseconds,
                self.has_time_part_positive(),
                true,
            )
        } else {
            let year_month = get_year_month(self.months, self.years, false);
            let time_interval = get_time_part(
                self.hours,
                self.minutes,
                self.seconds,
                self.microseconds,
                self.has_time_part_positive(),
                false,
            );
            format!("{} {:+} {}", year_month, self.days, time_interval)
        }
    }

    /// Convert IntervalFormat to postgres format string
    /// postgres pattern `- [years - months] [days] [hours[:minutes[:seconds[.fractional seconds]]]]`
    /// for example: -1 year -2 mons +3 days -04:05:06
    pub fn to_postgres_string(&self) -> String {
        if self.is_zero() {
            return "00:00:00".to_string();
        }
        let mut result = String::default();
        if self.has_year_month() {
            if self.years != 0 {
                result.push_str(&format!("{} year ", self.years));
            }
            if self.months != 0 {
                result.push_str(&format!("{} mons ", self.months));
            }
        }
        if self.has_day() {
            result.push_str(&format!("{} days ", self.days));
        }
        result.push_str(&self.get_postgres_time_part());
        result.trim().to_string()
    }

    /// get postgres time part(include hours, minutes, seconds, microseconds)
    fn get_postgres_time_part(&self) -> String {
        let mut time_part = String::default();
        if self.has_time_part() {
            let sign = if !self.has_time_part_positive() {
                "-"
            } else {
                ""
            };
            let hours = Self::padding_i64(self.hours);
            time_part.push_str(&format!(
                "{}{}:{}:{}",
                sign,
                hours,
                Self::padding_i64(self.minutes),
                Self::padding_i64(self.seconds),
            ));
            if self.microseconds != 0 {
                time_part.push_str(&format!(".{:06}", self.microseconds.unsigned_abs()))
            }
        }
        time_part
    }

    /// padding i64 to string with 2 digits
    fn padding_i64(val: i64) -> String {
        let num = if val < 0 {
            val.unsigned_abs()
        } else {
            val as u64
        };
        format!("{:02}", num)
    }
}

/// get year month string
fn get_year_month(mons: i32, years: i32, is_only_year_month: bool) -> String {
    let months = mons.unsigned_abs();
    if years == 0 || is_only_year_month {
        format!("{}-{}", years, months)
    } else {
        format!("{:+}-{}", years, months)
    }
}

/// get time part string
fn get_time_part(
    hours: i64,
    mins: i64,
    secs: i64,
    micros: i64,
    is_time_part_positive: bool,
    is_only_time: bool,
) -> String {
    let mut interval = String::default();
    if is_time_part_positive && is_only_time {
        interval.push_str(&format!("{}:{:02}:{:02}", hours, mins, secs));
    } else {
        let minutes = mins.unsigned_abs();
        let seconds = secs.unsigned_abs();
        interval.push_str(&format!("{:+}:{:02}:{:02}", hours, minutes, seconds));
    }
    if micros != 0 {
        let microseconds = format!(".{:06}", micros.unsigned_abs());
        interval.push_str(&microseconds);
    }
    interval
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_year_month() {
        let interval = IntervalYearMonth::new(1);
        assert_eq!(interval.months, 1);
    }

    #[test]
    fn test_from_date_time() {
        let interval = IntervalDayTime::new(1, 2);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.milliseconds, 2);
    }

    #[test]
    fn test_from_month_day_nano() {
        let interval = IntervalMonthDayNano::new(1, 2, 3);
        assert_eq!(interval.months, 1);
        assert_eq!(interval.days, 2);
        assert_eq!(interval.nanoseconds, 3);
    }

    #[test]
    fn test_interval_i128_convert() {
        let test_interval_eq = |month, day, nano| {
            let interval = IntervalMonthDayNano::new(month, day, nano);
            let interval_i128 = interval.to_i128();
            let interval2 = IntervalMonthDayNano::from_i128(interval_i128);
            assert_eq!(interval, interval2);
        };

        test_interval_eq(1, 2, 3);
        test_interval_eq(1, -2, 3);
        test_interval_eq(1, -2, -3);
        test_interval_eq(-1, -2, -3);
        test_interval_eq(i32::MAX, i32::MAX, i64::MAX);
        test_interval_eq(i32::MIN, i32::MAX, i64::MAX);
        test_interval_eq(i32::MAX, i32::MIN, i64::MAX);
        test_interval_eq(i32::MAX, i32::MAX, i64::MIN);
        test_interval_eq(i32::MIN, i32::MIN, i64::MAX);
        test_interval_eq(i32::MAX, i32::MIN, i64::MIN);
        test_interval_eq(i32::MIN, i32::MAX, i64::MIN);
        test_interval_eq(i32::MIN, i32::MIN, i64::MIN);

        let interval = IntervalMonthDayNano::from_i128(1);
        assert_eq!(interval, IntervalMonthDayNano::new(0, 0, 1));
        assert_eq!(1, IntervalMonthDayNano::new(0, 0, 1).to_i128());
    }

    #[test]
    fn test_interval_i64_convert() {
        let interval = IntervalDayTime::from_i64(1);
        assert_eq!(interval, IntervalDayTime::new(0, 1));
        assert_eq!(1, IntervalDayTime::new(0, 1).to_i64());
    }

    #[test]
    fn test_convert_interval_format() {
        let interval = IntervalMonthDayNano {
            months: 14,
            days: 160,
            nanoseconds: 1000000,
        };
        let interval_format = IntervalFormat::from(interval);
        assert_eq!(interval_format.years, 1);
        assert_eq!(interval_format.months, 2);
        assert_eq!(interval_format.days, 160);
        assert_eq!(interval_format.hours, 0);
        assert_eq!(interval_format.minutes, 0);
        assert_eq!(interval_format.seconds, 0);
        assert_eq!(interval_format.microseconds, 1000);
    }

    #[test]
    fn test_to_iso8601_string() {
        // Test interval zero
        let interval = IntervalMonthDayNano::new(0, 0, 0);
        assert_eq!(interval.to_iso8601_string(), "PT0S");

        let interval = IntervalMonthDayNano::new(1, 1, 1);
        assert_eq!(interval.to_iso8601_string(), "P0Y1M1DT0H0M0S");

        let interval = IntervalMonthDayNano::new(14, 31, 10000000000);
        assert_eq!(interval.to_iso8601_string(), "P1Y2M31DT0H0M10S");

        let interval = IntervalMonthDayNano::new(14, 31, 23210200000000);
        assert_eq!(interval.to_iso8601_string(), "P1Y2M31DT6H26M50.2S");
    }

    #[test]
    fn test_to_postgres_string() {
        // Test interval zero
        let interval = IntervalMonthDayNano::new(0, 0, 0);
        assert_eq!(
            IntervalFormat::from(interval).to_postgres_string(),
            "00:00:00"
        );

        let interval = IntervalMonthDayNano::new(23, 100, 23210200000000);
        assert_eq!(
            IntervalFormat::from(interval).to_postgres_string(),
            "1 year 11 mons 100 days 06:26:50.200000"
        );
    }

    #[test]
    fn test_to_sql_standard_string() {
        // Test zero interval
        let interval = IntervalMonthDayNano::new(0, 0, 0);
        assert_eq!(IntervalFormat::from(interval).to_sql_standard_string(), "0");

        let interval = IntervalMonthDayNano::new(23, 100, 23210200000000);
        assert_eq!(
            IntervalFormat::from(interval).to_sql_standard_string(),
            "+1-11 +100 +6:26:50.200000"
        );

        // Test interval without year, month, day
        let interval = IntervalMonthDayNano::new(0, 0, 23210200000000);
        assert_eq!(
            IntervalFormat::from(interval).to_sql_standard_string(),
            "6:26:50.200000"
        );
    }

    #[test]
    fn test_from_arrow_interval_unit() {
        let unit = ArrowIntervalUnit::YearMonth;
        assert_eq!(IntervalUnit::from(unit), IntervalUnit::YearMonth);

        let unit = ArrowIntervalUnit::DayTime;
        assert_eq!(IntervalUnit::from(unit), IntervalUnit::DayTime);

        let unit = ArrowIntervalUnit::MonthDayNano;
        assert_eq!(IntervalUnit::from(unit), IntervalUnit::MonthDayNano);
    }
}
