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

use std::cmp::Ordering;
use std::default::Default;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};

use arrow::datatypes::IntervalUnit as ArrowIntervalUnit;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ParseIntervalSnafu;

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

/// Interval data format
/// | months | days   | micros     |
/// | 4bytes | 4bytes | 8bytes     |  
/// All interval types are stored in this format.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Interval {
    months: i32,
    days: i32,
    micros: i64,
    unit: IntervalUnit,
}

// The number of microseconds in a second, day, month etc.
pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const MICROS_PER_DAY: i64 = 86400 * MICROS_PER_SEC;
pub const MICROS_PER_MONTH: i64 = 30 * MICROS_PER_DAY;
pub const DAYS_PER_MONTH: i64 = 30;

// The number of seconds in a minute, hour etc.
pub const SECS_PER_MINUTE: i32 = 60;
pub const MINS_PER_HOUR: i32 = 60;
pub const HOURS_PER_DAY: i32 = 24;
pub const DAYS_PER_WEEK: i32 = 7;
pub const SECS_PER_HOUR: i32 = SECS_PER_MINUTE * MINS_PER_HOUR;
pub const SECS_PER_DAY: i32 = SECS_PER_HOUR * HOURS_PER_DAY;
pub const SECS_PER_WEEK: i32 = SECS_PER_DAY * DAYS_PER_WEEK;

impl Interval {
    /// Creates a new default interval.
    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Interval {
            months,
            days,
            micros,
            unit: IntervalUnit::MonthDayNano,
        }
    }

    /// Creates a new interval from months, days and nanoseconds.
    /// If nanoseconds is between [-999,999], the data will be lost, the
    /// precision is microsecond.
    pub fn from_month_day_nano(months: i32, days: i32, nsecs: i64) -> Self {
        Interval {
            months,
            days,
            micros: nsecs / 1_000,
            unit: IntervalUnit::MonthDayNano,
        }
    }

    /// Creates a new interval from months.
    pub fn from_year_month(months: i32) -> Self {
        Interval {
            months,
            days: 0,
            micros: 0,
            unit: IntervalUnit::YearMonth,
        }
    }

    /// Creates a new interval from days and milliseconds.
    pub fn from_day_time(days: i32, millis: i32) -> Self {
        Interval {
            months: 0,
            days,
            micros: (millis as i64) * 1_000,
            unit: IntervalUnit::DayTime,
        }
    }

    pub fn to_micros(&self) -> i128 {
        let days = (self.days as i64) + 30i64 * (self.months as i64);
        (self.micros as i128) + (MICROS_PER_DAY as i128) * (days as i128)
    }

    /// Smallest interval value.
    pub const MIN: Self = Self {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
        unit: IntervalUnit::MonthDayNano,
    };

    /// Largest interval value.
    pub const MAX: Self = Self {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
        unit: IntervalUnit::MonthDayNano,
    };

    /// Returns the justified interval.
    /// allows you to adjust the interval of 30-day as one month and the interval of 24-hour as one day
    pub fn justified_interval(&self) -> Self {
        let mut result = *self;
        let extra_months_d = self.days as i64 / DAYS_PER_MONTH;
        let extra_months_micros = self.micros / MICROS_PER_MONTH;
        result.days -= (extra_months_d * DAYS_PER_MONTH) as i32;
        result.micros -= extra_months_micros * MICROS_PER_MONTH;

        let extra_days_micros = self.micros / MICROS_PER_DAY;
        result.micros -= extra_days_micros * MICROS_PER_DAY;

        result.months += extra_months_d as i32 + extra_months_micros as i32;
        result.days += extra_days_micros as i32;

        result
    }

    /// check if [IntervalMonthDayNano] is positive
    pub fn is_positive(&self) -> bool {
        self.months >= 0 && self.days >= 0 && self.micros >= 0
    }

    /// is_zero
    pub fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.micros == 0
    }

    /// get unit
    pub fn unit(&self) -> IntervalUnit {
        self.unit
    }

    /// Multiple [`Interval`] by an integer with overflow check.
    /// Returns `None` if overflow occurred.
    pub fn checked_mul_int<I>(&self, rhs: I) -> Option<Self>
    where
        I: TryInto<i32>,
    {
        let rhs = rhs.try_into().ok()?;
        let months = self.months.checked_mul(rhs)?;
        let days = self.days.checked_mul(rhs)?;
        let nsecs = self.micros.checked_mul(rhs as i64)?;

        Some(Self {
            months,
            days,
            micros: nsecs,
            unit: self.unit,
        })
    }

    /// Convert Interval to ISO 8601 string
    pub fn to_iso8601_string(&self) -> String {
        IntervalFormat::from(self).to_iso8601_string()
    }

    /// Convert Interval to postgres verbose string
    pub fn to_postgres_string(&self) -> String {
        IntervalFormat::from(self).to_postgres_string()
    }

    /// Convert Interval to sql_standard string
    pub fn to_sql_standard_string(&self) -> String {
        IntervalFormat::from(self).to_sql_standard_string()
    }

    /// `Interval` Type and i128[MonthDayNano] Convert
    pub fn from_i128(v: i128) -> Self {
        Interval {
            micros: (v as i64) / 1_000,
            days: (v >> 64) as i32,
            months: (v >> 96) as i32,
            unit: IntervalUnit::MonthDayNano,
        }
    }

    /// `Interval` Type and i64[DayTime] Convert
    pub fn from_i64(v: i64) -> Self {
        Interval {
            micros: ((v as i32) as i64) * 1_000,
            days: (v >> 32) as i32,
            months: 0,
            unit: IntervalUnit::DayTime,
        }
    }

    /// `Interval` Type and i32[YearMonth] Convert
    pub fn from_i32(v: i32) -> Self {
        Interval {
            micros: 0,
            days: 0,
            months: v,
            unit: IntervalUnit::YearMonth,
        }
    }

    pub fn to_i128(&self) -> i128 {
        let mut result = 0;
        result |= self.months as i128;
        result <<= 32;
        result |= self.days as i128;
        result <<= 64;
        result |= (self.micros * 1_000) as i128;
        result
    }

    pub fn to_i64(&self) -> i64 {
        let mut result = 0;
        result |= self.days as i64;
        result <<= 32;
        result |= self.micros / 1_000;
        result
    }

    pub fn to_i32(&self) -> i32 {
        self.months
    }
}

impl From<i128> for Interval {
    fn from(v: i128) -> Self {
        Self::from_i128(v)
    }
}

impl From<Interval> for i128 {
    fn from(v: Interval) -> Self {
        v.to_i128()
    }
}

impl From<Interval> for serde_json::Value {
    fn from(v: Interval) -> Self {
        Value::String(v.to_string())
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        if self.months != 0 {
            s.push_str(&format!("{} months ", self.months));
        }
        if self.days != 0 {
            s.push_str(&format!("{} days ", self.days));
        }
        if self.micros != 0 {
            s.push_str(&format!("{} micros", self.micros));
        }
        write!(f, "{}", s.trim())
    }
}

/// https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-OUTPUT
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

impl<'a> From<&'a Interval> for IntervalFormat {
    fn from(val: &Interval) -> IntervalFormat {
        let months = val.months;
        let days = val.days;
        let microseconds = val.micros;
        let years = (months - (months % 12)) / 12;
        let months = months - years * 12;
        let hours = (microseconds - (microseconds % 3_600_000_000)) / 3_600_000_000;
        let microseconds = microseconds - hours * 3_600_000_000;
        let minutes = (microseconds - (microseconds % 60_000_000)) / 60_000_000;
        let microseconds = microseconds - minutes * 60_000_000;
        let seconds = (microseconds - (microseconds % 1_000_000)) / 1_000_000;
        let microseconds = microseconds - seconds * 1_000_000;
        IntervalFormat {
            years,
            months,
            days,
            hours,
            minutes,
            seconds,
            microseconds,
        }
    }
}

impl IntervalFormat {
    pub fn try_into_interval(self) -> crate::error::Result<Interval> {
        let months = self
            .years
            .checked_mul(12)
            .and_then(|years| self.months.checked_add(years));
        let microseconds = self
            .hours
            .checked_mul(60)
            .and_then(|minutes| self.minutes.checked_add(minutes))
            .and_then(|minutes| minutes.checked_mul(60))
            .and_then(|seconds| self.seconds.checked_add(seconds))
            .and_then(|seconds| seconds.checked_mul(1_000_000))
            .and_then(|microseconds| self.microseconds.checked_add(microseconds));
        // overflow handle
        let months = match months {
            Some(mon) => mon,
            None => {
                return ParseIntervalSnafu {
                    raw: "interval months overflow",
                }
                .fail()
            }
        };
        let microseconds = match microseconds {
            Some(micro) => micro,
            None => {
                return ParseIntervalSnafu {
                    raw: "interval microseconds overflow",
                }
                .fail()
            }
        };

        Ok(Interval {
            months,
            days: self.days,
            micros: microseconds,
            unit: Default::default(),
        })
    }

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

    pub fn has_year_month(&self) -> bool {
        self.years != 0 || self.months != 0
    }

    pub fn has_day(&self) -> bool {
        self.days != 0
    }

    pub fn has_time_part_positive(&self) -> bool {
        self.hours > 0 || self.minutes > 0 || self.seconds > 0 || self.microseconds > 0
    }

    // time part means hours, minutes, seconds, microseconds
    pub fn has_time_part(&self) -> bool {
        self.hours != 0 || self.minutes != 0 || self.seconds != 0 || self.microseconds != 0
    }

    pub fn to_iso8601_string(&self) -> String {
        if self.is_zero() {
            return "PT0S".to_owned();
        }
        let mut result = "P".to_owned();
        let mut day = "".to_owned();
        let mut time_part;
        if self.has_time_part() {
            time_part = "T".to_owned();
            if self.hours != 0 {
                time_part.push_str(&format!("{}H", self.hours));
            }
            if self.minutes != 0 {
                time_part.push_str(&format!("{}M", self.minutes));
            }
            if self.seconds != 0 {
                time_part.push_str(&format!("{}S", self.seconds));
            }
            if self.microseconds != 0 {
                let ms = self.microseconds.unsigned_abs();
                time_part.push_str(&format!(".{:06}", ms));
            }
        } else {
            time_part = "".to_owned();
        }
        if self.years != 0 {
            result.push_str(&format!("{}Y", self.years));
        }
        if self.months != 0 {
            result.push_str(&format!("{}M", self.months));
        }
        if self.days != 0 {
            day.push_str(&format!("{}D", self.days));
        }
        result.push_str(&day);
        result.push_str(&time_part);
        result
    }

    pub fn to_sql_standard_string(self) -> String {
        if self.is_zero() {
            "0".to_owned()
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

    pub fn to_postgres_string(&self) -> String {
        if self.is_zero() {
            return "00:00:00".to_owned();
        }
        let mut result = "".to_owned();
        let mut year_month = "".to_owned();
        let mut day = "".to_owned();
        let time_part = self.get_postgres_time_part();
        if self.has_day() {
            day = format!("{} days ", self.days)
        }
        if self.has_year_month() {
            if self.years != 0 {
                year_month.push_str(&format!("{} year ", self.years))
            }
            if self.months != 0 {
                year_month.push_str(&format!("{} mons ", self.months));
            }
        }
        result.push_str(&year_month);
        result.push_str(&day);
        result.push_str(&time_part);
        result.trim().to_owned()
    }

    fn get_postgres_time_part(&self) -> String {
        let mut time_part = "".to_owned();
        if self.has_time_part() {
            let sign = if !self.has_time_part_positive() {
                "-".to_owned()
            } else {
                "".to_owned()
            };
            let hours = Self::padding_i64(self.hours);
            time_part.push_str(
                &(sign
                    + &hours
                    + ":"
                    + &Self::padding_i64(self.minutes)
                    + ":"
                    + &Self::padding_i64(self.seconds)),
            );
            if self.microseconds != 0 {
                time_part.push_str(&format!(".{:06}", self.microseconds.unsigned_abs()))
            }
        }
        time_part
    }

    fn padding_i64(val: i64) -> String {
        let num = if val < 0 {
            val.unsigned_abs()
        } else {
            val as u64
        };
        format!("{:02}", num)
    }
}

fn get_year_month(mons: i32, years: i32, is_only_year_month: bool) -> String {
    let months = mons.unsigned_abs();
    if years == 0 || is_only_year_month {
        format!("{}-{}", years, months)
    } else {
        format!("{:+}-{}", years, months)
    }
}

fn get_time_part(
    hours: i64,
    mins: i64,
    secs: i64,
    micros: i64,
    is_time_interval_pos: bool,
    is_only_time: bool,
) -> String {
    let mut interval = "".to_owned();
    if is_time_interval_pos && is_only_time {
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

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
struct IntervalCompare(i128);

impl From<Interval> for IntervalCompare {
    fn from(interval: Interval) -> Self {
        Self(interval.to_micros())
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        IntervalCompare::from(*self).cmp(&IntervalCompare::from(*other))
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Interval {}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Hash for Interval {
    fn hash<H: Hasher>(&self, state: &mut H) {
        IntervalCompare::from(*self).hash(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_mul_int() {
        let interval = Interval::new(1, 1, 1);
        let interval2 = interval.checked_mul_int(2).unwrap();
        assert_eq!(interval2.months, 2);
        assert_eq!(interval2.days, 2);
        assert_eq!(interval2.micros, 2);
    }

    #[test]
    fn test_display() {
        let interval = Interval::from_month_day_nano(1, 1, 1);
        assert_eq!(interval.to_string(), "1 months 1 days");

        let interval = Interval::from_month_day_nano(14, 31, 10000000000);
        assert_eq!(interval.to_string(), "14 months 31 days 10000000 micros");
    }

    #[test]
    fn test_interval_justified() {
        let interval = Interval::from_month_day_nano(1, 131, 1).justified_interval();
        let interval2 = Interval::from_month_day_nano(5, 11, 1);
        assert_eq!(interval, interval2);

        let interval =
            Interval::from_month_day_nano(1, 1, 1000 * MICROS_PER_DAY * 2).justified_interval();
        let interval2 = Interval::from_month_day_nano(1, 3, 0);
        assert_eq!(interval, interval2);
    }

    #[test]
    fn test_serde_json() {
        let interval = Interval::new(1, 1, 1);
        let json = serde_json::to_string(&interval).unwrap();
        assert_eq!(
            json,
            "{\"months\":1,\"days\":1,\"micros\":1,\"unit\":\"MonthDayNano\"}"
        );
        let interval2: Interval = serde_json::from_str(&json).unwrap();
        assert_eq!(interval, interval2);
    }

    #[test]
    fn test_to_iso8601_string() {
        let interval = Interval::from_month_day_nano(1, 1, 1);
        assert_eq!(interval.to_iso8601_string(), "P1M1D");

        let interval = Interval::from_month_day_nano(14, 31, 10000000000);
        assert_eq!(interval.to_iso8601_string(), "P1Y2M31DT10S");
    }

    #[test]
    fn test_to_postgres_string() {
        let interval = Interval::from_month_day_nano(23, 100, 23210200000000);
        assert_eq!(
            interval.to_postgres_string(),
            "1 year 11 mons 100 days 06:26:50.200000"
        );
    }

    #[test]
    fn test_to_sql_standard_string() {
        let interval = Interval::from_month_day_nano(23, 100, 23210200000000);
        assert_eq!(
            interval.to_sql_standard_string(),
            "+1-11 +100 +6:26:50.200000"
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
