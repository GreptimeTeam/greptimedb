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

// /// Interval Type represents a period of time.
// /// It is composed of months, days and nanoseconds.
// /// 3 kinds of interval are supported: year-month, day-time and
// /// month-day-nano, which will be stored in the following format.
// /// Interval data format:
// /// | months | days   | nsecs      |
// /// | 4bytes | 4bytes | 8bytes     |
// #[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
// pub struct Interval {
//     months: i32,
//     days: i32,
//     nsecs: i64,
//     unit: IntervalUnit,
// }

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

// impl Interval {
//     /// Creates a new interval from months, days and nanoseconds.
//     /// Precision is nanosecond.
//     pub fn from_month_day_nano(months: i32, days: i32, nsecs: i64) -> Self {
//         Interval {
//             months,
//             days,
//             nsecs,
//             unit: IntervalUnit::MonthDayNano,
//         }
//     }

//     /// Creates a new interval from months.
//     pub fn from_year_month(months: i32) -> Self {
//         Interval {
//             months,
//             days: 0,
//             nsecs: 0,
//             unit: IntervalUnit::YearMonth,
//         }
//     }

//     /// Creates a new interval from days and milliseconds.
//     pub fn from_day_time(days: i32, millis: i32) -> Self {
//         Interval {
//             months: 0,
//             days,
//             nsecs: (millis as i64) * NANOS_PER_MILLI,
//             unit: IntervalUnit::DayTime,
//         }
//     }

//     pub fn to_duration(&self) -> Result<Duration> {
//         Ok(Duration::new_nanosecond(
//             self.to_nanosecond()
//                 .try_into()
//                 .context(TimestampOverflowSnafu)?,
//         ))
//     }

//     /// Return a tuple(months, days, nanoseconds) from the interval.
//     pub fn to_month_day_nano(&self) -> (i32, i32, i64) {
//         (self.months, self.days, self.nsecs)
//     }

//     /// Converts the interval to nanoseconds.
//     pub fn to_nanosecond(&self) -> i128 {
//         let days = (self.days as i64) + DAYS_PER_MONTH * (self.months as i64);
//         (self.nsecs as i128) + (NANOS_PER_DAY as i128) * (days as i128)
//     }

//     /// Smallest interval value.
//     pub const MIN: Self = Self {
//         months: i32::MIN,
//         days: i32::MIN,
//         nsecs: i64::MIN,
//         unit: IntervalUnit::MonthDayNano,
//     };

//     /// Largest interval value.
//     pub const MAX: Self = Self {
//         months: i32::MAX,
//         days: i32::MAX,
//         nsecs: i64::MAX,
//         unit: IntervalUnit::MonthDayNano,
//     };

//     /// Returns the justified interval.
//     /// allows you to adjust the interval of 30-day as one month and the interval of 24-hour as one day
//     pub fn justified_interval(&self) -> Self {
//         let mut result = *self;
//         let extra_months_d = self.days as i64 / DAYS_PER_MONTH;
//         let extra_months_nsecs = self.nsecs / NANOS_PER_MONTH;
//         result.days -= (extra_months_d * DAYS_PER_MONTH) as i32;
//         result.nsecs -= extra_months_nsecs * NANOS_PER_MONTH;

//         let extra_days = self.nsecs / NANOS_PER_DAY;
//         result.nsecs -= extra_days * NANOS_PER_DAY;

//         result.months += extra_months_d as i32 + extra_months_nsecs as i32;
//         result.days += extra_days as i32;

//         result
//     }

//     /// is_zero
//     pub fn is_zero(&self) -> bool {
//         self.months == 0 && self.days == 0 && self.nsecs == 0
//     }

//     /// get unit
//     pub fn unit(&self) -> IntervalUnit {
//         self.unit
//     }

//     /// Multiple Interval by an integer with overflow check.
//     /// Returns justified Interval, or `None` if overflow occurred.
//     pub fn checked_mul_int<I>(&self, rhs: I) -> Option<Self>
//     where
//         I: TryInto<i32>,
//     {
//         let rhs = rhs.try_into().ok()?;
//         let months = self.months.checked_mul(rhs)?;
//         let days = self.days.checked_mul(rhs)?;
//         let nsecs = self.nsecs.checked_mul(rhs as i64)?;

//         Some(
//             Self {
//                 months,
//                 days,
//                 nsecs,
//                 unit: self.unit,
//             }
//             .justified_interval(),
//         )
//     }

//     /// Convert Interval to ISO 8601 string
//     pub fn to_iso8601_string(self) -> String {
//         IntervalFormat::from(self).to_iso8601_string()
//     }

//     /// Convert Interval to postgres verbose string
//     pub fn to_postgres_string(self) -> String {
//         IntervalFormat::from(self).to_postgres_string()
//     }

//     /// Convert Interval to sql_standard string
//     pub fn to_sql_standard_string(self) -> String {
//         IntervalFormat::from(self).to_sql_standard_string()
//     }

//     /// Interval Type and i128 [IntervalUnit::MonthDayNano] Convert
//     /// v consists of months(i32) | days(i32) | nsecs(i64)
//     pub fn from_i128(v: i128) -> Self {
//         Interval {
//             nsecs: v as i64,
//             days: (v >> 64) as i32,
//             months: (v >> 96) as i32,
//             unit: IntervalUnit::MonthDayNano,
//         }
//     }

//     /// `Interval` Type and i64 [IntervalUnit::DayTime] Convert
//     /// v consists of days(i32) | milliseconds(i32)
//     pub fn from_i64(v: i64) -> Self {
//         Interval {
//             nsecs: ((v as i32) as i64) * NANOS_PER_MILLI,
//             days: (v >> 32) as i32,
//             months: 0,
//             unit: IntervalUnit::DayTime,
//         }
//     }

//     /// `Interval` Type and i32 [IntervalUnit::YearMonth] Convert
//     /// v consists of months(i32)
//     pub fn from_i32(v: i32) -> Self {
//         Interval {
//             nsecs: 0,
//             days: 0,
//             months: v,
//             unit: IntervalUnit::YearMonth,
//         }
//     }

//     pub fn to_i128(&self) -> i128 {
//         // 128            96              64                               0
//         // +-------+-------+-------+-------+-------+-------+-------+-------+
//         // |     months    |      days     |          nanoseconds          |
//         // +-------+-------+-------+-------+-------+-------+-------+-------+
//         let months = (self.months as u128 & u32::MAX as u128) << 96;
//         let days = (self.days as u128 & u32::MAX as u128) << 64;
//         let nsecs = self.nsecs as u128 & u64::MAX as u128;
//         (months | days | nsecs) as i128
//     }

//     pub fn to_i64(&self) -> i64 {
//         // 64                              32                              0
//         // +-------+-------+-------+-------+-------+-------+-------+-------+
//         // |             days              |         milliseconds          |
//         // +-------+-------+-------+-------+-------+-------+-------+-------+
//         let days = (self.days as u64 & u32::MAX as u64) << 32;
//         let milliseconds = (self.nsecs / NANOS_PER_MILLI) as u64 & u32::MAX as u64;
//         (days | milliseconds) as i64
//     }

//     pub fn to_i32(&self) -> i32 {
//         self.months
//     }

//     pub fn negative(&self) -> Self {
//         Self {
//             months: -self.months,
//             days: -self.days,
//             nsecs: -self.nsecs,
//             unit: self.unit,
//         }
//     }
// }

// impl From<i128> for Interval {
//     fn from(v: i128) -> Self {
//         Self::from_i128(v)
//     }
// }

// impl From<Interval> for i128 {
//     fn from(v: Interval) -> Self {
//         v.to_i128()
//     }
// }

// impl From<Interval> for serde_json::Value {
//     fn from(v: Interval) -> Self {
//         Value::String(v.to_string())
//     }
// }

// impl Display for Interval {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         let mut s = String::new();
//         if self.months != 0 {
//             write!(s, "{} months ", self.months)?;
//         }
//         if self.days != 0 {
//             write!(s, "{} days ", self.days)?;
//         }
//         if self.nsecs != 0 {
//             write!(s, "{} nsecs", self.nsecs)?;
//         }
//         write!(f, "{}", s.trim())
//     }
// }

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

// impl From<Interval> for IntervalFormat {
//     fn from(val: Interval) -> IntervalFormat {
//         let months = val.months;
//         let days = val.days;
//         let microseconds = val.nsecs / NANOS_PER_MICRO;
//         let years = (months - (months % 12)) / 12;
//         let months = months - years * 12;
//         let hours = (microseconds - (microseconds % 3_600_000_000)) / 3_600_000_000;
//         let microseconds = microseconds - hours * 3_600_000_000;
//         let minutes = (microseconds - (microseconds % 60_000_000)) / 60_000_000;
//         let microseconds = microseconds - minutes * 60_000_000;
//         let seconds = (microseconds - (microseconds % 1_000_000)) / 1_000_000;
//         let microseconds = microseconds - seconds * 1_000_000;
//         IntervalFormat {
//             years,
//             months,
//             days,
//             hours,
//             minutes,
//             seconds,
//             microseconds,
//         }
//     }
// }

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

// /// IntervalCompare is used to compare two intervals
// /// It makes interval into nanoseconds style.
// #[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
// struct IntervalCompare(i128);

// impl From<Interval> for IntervalCompare {
//     fn from(interval: Interval) -> Self {
//         Self(interval.to_nanosecond())
//     }
// }

// impl Ord for Interval {
//     fn cmp(&self, other: &Self) -> Ordering {
//         IntervalCompare::from(*self).cmp(&IntervalCompare::from(*other))
//     }
// }

// impl PartialOrd for Interval {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Eq for Interval {}

// impl PartialEq for Interval {
//     fn eq(&self, other: &Self) -> bool {
//         self.cmp(other).is_eq()
//     }
// }

// impl Hash for Interval {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         IntervalCompare::from(*self).hash(state)
//     }
// }

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

    // #[test]
    // fn test_to_duration() {
    //     let interval = Interval::from_day_time(1, 2);

    //     let duration = interval.to_duration().unwrap();
    //     assert_eq!(86400002000000, duration.value());
    //     assert_eq!(TimeUnit::Nanosecond, duration.unit());

    //     let interval = Interval::from_year_month(12);

    //     let duration = interval.to_duration().unwrap();
    //     assert_eq!(31104000000000000, duration.value());
    //     assert_eq!(TimeUnit::Nanosecond, duration.unit());
    // }

    // #[test]
    // fn test_to_nanosecond() {
    //     let interval = Interval::from_year_month(1);
    //     assert_eq!(interval.to_nanosecond(), 2592000000000000);
    //     let interval = Interval::from_day_time(1, 2);
    //     assert_eq!(interval.to_nanosecond(), 86400002000000);

    //     let max_interval = Interval::from_month_day_nano(i32::MAX, i32::MAX, i64::MAX);
    //     assert_eq!(max_interval.to_nanosecond(), 5751829423496836854775807);

    //     let min_interval = Interval::from_month_day_nano(i32::MIN, i32::MIN, i64::MIN);
    //     assert_eq!(min_interval.to_nanosecond(), -5751829426175236854775808);
    // }

    // #[test]
    // fn test_interval_is_zero() {
    //     let interval = Interval::from_month_day_nano(1, 1, 1);
    //     assert!(!interval.is_zero());
    //     let interval = Interval::from_month_day_nano(0, 0, 0);
    //     assert!(interval.is_zero());
    // }

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

    // #[test]
    // fn test_interval_hash() {
    //     let interval = Interval::from_month_day_nano(1, 31, 1);
    //     let interval2 = Interval::from_month_day_nano(2, 1, 1);
    //     let mut map = HashMap::new();
    //     map.insert(interval, 1);
    //     assert_eq!(map.get(&interval2), Some(&1));
    // }

    // #[test]
    // fn test_interval_mul_int() {
    //     let interval = Interval::from_month_day_nano(1, 1, 1);
    //     let interval2 = interval.checked_mul_int(2).unwrap();
    //     assert_eq!(interval2.months, 2);
    //     assert_eq!(interval2.days, 2);
    //     assert_eq!(interval2.nsecs, 2);

    //     // test justified interval
    //     let interval = Interval::from_month_day_nano(1, 31, 1);
    //     let interval2 = interval.checked_mul_int(2).unwrap();
    //     assert_eq!(interval2.months, 4);
    //     assert_eq!(interval2.days, 2);
    //     assert_eq!(interval2.nsecs, 2);

    //     // test overflow situation
    //     let interval = Interval::from_month_day_nano(i32::MAX, 1, 1);
    //     let interval2 = interval.checked_mul_int(2);
    //     assert!(interval2.is_none());
    // }

    // #[test]
    // fn test_display() {
    //     let interval = Interval::from_month_day_nano(1, 1, 1);
    //     assert_eq!(interval.to_string(), "1 months 1 days 1 nsecs");

    //     let interval = Interval::from_month_day_nano(14, 31, 10000000000);
    //     assert_eq!(interval.to_string(), "14 months 31 days 10000000000 nsecs");
    // }

    // #[test]
    // fn test_interval_justified() {
    //     let interval = Interval::from_month_day_nano(1, 131, 1).justified_interval();
    //     let interval2 = Interval::from_month_day_nano(5, 11, 1);
    //     assert_eq!(interval, interval2);

    //     let interval = Interval::from_month_day_nano(1, 1, NANOS_PER_MONTH + 2 * NANOS_PER_DAY)
    //         .justified_interval();
    //     let interval2 = Interval::from_month_day_nano(2, 3, 0);
    //     assert_eq!(interval, interval2);
    // }

    // #[test]
    // fn test_serde_json() {
    //     let interval = Interval::from_month_day_nano(1, 1, 1);
    //     let json = serde_json::to_string(&interval).unwrap();
    //     assert_eq!(
    //         json,
    //         "{\"months\":1,\"days\":1,\"nsecs\":1,\"unit\":\"MonthDayNano\"}"
    //     );
    //     let interval2: Interval = serde_json::from_str(&json).unwrap();
    //     assert_eq!(interval, interval2);
    // }

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
