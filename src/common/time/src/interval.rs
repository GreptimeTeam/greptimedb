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
    // Creates a new default interval.
    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Interval {
            months,
            days,
            micros,
            unit: IntervalUnit::MonthDayNano,
        }
    }

    // Creates a new interval from months, days and nanoseconds.
    // If nanoseconds is between [-999,999], the data will be lost, the
    // precision is microsecond.
    pub fn from_month_day_nano(months: i32, days: i32, nsecs: i64) -> Self {
        Interval {
            months,
            days,
            micros: nsecs / 1_000,
            unit: IntervalUnit::MonthDayNano,
        }
    }

    pub fn from_year_month(months: i32) -> Self {
        Interval {
            months,
            days: 0,
            micros: 0,
            unit: IntervalUnit::YearMonth,
        }
    }

    pub fn from_day_time(days: i32, millis: i32) -> Self {
        Interval {
            months: 0,
            days,
            micros: (millis as i64) * 1_000,
            unit: IntervalUnit::DayTime,
        }
    }

    /// Smallest interval value.
    pub const MIN: Self = Self {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
        unit: IntervalUnit::MonthDayNano,
    };

    // Largest interval value.
    pub const MAX: Self = Self {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
        unit: IntervalUnit::MonthDayNano,
    };

    /// Returns the normalized interval.
    pub fn normalize_interval(&self) -> Self {
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

    // is_zero
    pub fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.micros == 0
    }

    // get unit
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

    pub fn to_iso_8601(&self) -> String {
        // ISO pattern - PnYnMnDTnHnMnS
        let years = self.months / 12;
        let months = self.months % 12;
        let days = self.days;
        let secs_fract = (self.micros % MICROS_PER_SEC).abs();
        let total_secs = (self.micros / MICROS_PER_SEC).abs();
        let hours = total_secs / 3600;
        let minutes = (total_secs / 60) % 60;
        let seconds = total_secs % 60;

        let fract_str = if secs_fract != 0 {
            format!(".{:06}", secs_fract)
                .trim_end_matches('0')
                .to_owned()
        } else {
            "".to_owned()
        };
        format!("P{years}Y{months}M{days}DT{hours}H{minutes}M{seconds}{fract_str}S")
    }

    // `Interval` Type and i128(MonthDayNano) Convert
    pub fn from_i128(v: i128) -> Self {
        Interval {
            micros: (v as i64) / 1_000,
            days: (v >> 64) as i32,
            months: (v >> 96) as i32,
            unit: IntervalUnit::MonthDayNano,
        }
    }

    // `Interval` Type and i64(DayTime) Convert
    pub fn from_i64(v: i64) -> Self {
        Interval {
            micros: ((v as i32) as i64) * 1_000,
            days: (v >> 32) as i32,
            months: 0,
            unit: IntervalUnit::DayTime,
        }
    }

    // `Interval` Type and i32(YearMonth) Convert
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

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.to_i128().partial_cmp(&other.to_i128())
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_i128().cmp(&other.to_i128())
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.to_i128() == other.to_i128()
    }
}

impl Eq for Interval {}

impl Hash for Interval {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_i128().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_from_i128() {
        let interval = Interval::from_month_day_nano(1, 1, 1);
        let v: i128 = interval.into();
        let interval2 = Interval::from(v);
        assert_eq!(interval, interval2);
    }

    #[test]
    fn test_interval_eq() {
        let interval = Interval::from_month_day_nano(1, 1, 1);
        let interval2 = Interval::from_month_day_nano(1, 1, 1);
        assert_eq!(interval, interval2);
        assert_eq!(interval2, interval);
    }

    #[test]
    fn test_interval_cmp() {
        let interval = Interval::from_month_day_nano(1, 1, 1);
        let interval2 = Interval::from_month_day_nano(1, 1, 1);
        assert_eq!(interval, interval2);
        assert_eq!(interval2, interval);
        assert_eq!(interval.cmp(&interval2), Ordering::Equal);
        assert_eq!(interval2.cmp(&interval), Ordering::Equal);
    }

    #[test]
    fn test_interval_cmp2() {
        let interval = Interval::from_month_day_nano(1, 1, 1);
        let interval2 = Interval::from_month_day_nano(1, 2, 2);
        assert!(interval < interval2);
        assert!(interval2 > interval);
    }

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
    fn test_interval_normalization() {
        let interval = Interval::from_month_day_nano(1, 131, 1).normalize_interval();
        let interval2 = Interval::from_month_day_nano(5, 11, 1);
        assert_eq!(interval, interval2);

        let interval =
            Interval::from_month_day_nano(1, 1, 1000 * MICROS_PER_DAY * 2).normalize_interval();
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
        assert_eq!(interval.to_iso_8601(), "P0Y1M1DT0H0M0S");

        let interval = Interval::from_month_day_nano(14, 31, 10000000000);
        assert_eq!(interval.to_iso_8601(), "P1Y2M31DT0H0M10S");
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
