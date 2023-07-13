use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add, Neg, Sub};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Error;
use crate::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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
    MonthDayNano,
}

/// Interval data format
/// | months | days   | micros     |
/// | 4bytes | 4bytes | 8bytes     |  
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Interval {
    months: i32,
    days: i32,
    micros: i64,
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
        }
    }

    // Creates a new interval from months, days and nanoseconds.
    // If nanoseconds is between [-999,999], the data will be losted, the
    // precision is microsecond.
    pub fn from_month_day_nano(months: i32, days: i32, nsecs: i64) -> Self {
        Interval {
            months,
            days,
            micros: nsecs / 1_000,
        }
    }

    /// Smallest interval value.
    pub const MIN: Self = Self {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
    };

    // Largest interval value.
    pub const MAX: Self = Self {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
    };

    pub fn years(&self) -> i32 {
        self.months / 12
    }

    pub fn months(&self) -> i32 {
        self.months % 12
    }

    pub fn days(&self) -> i32 {
        self.days
    }

    pub fn hours(&self) -> i32 {
        (self.micros / MICROS_PER_SEC / SECS_PER_HOUR as i64) as i32
    }

    pub fn minutes(&self) -> i32 {
        (self.micros / MICROS_PER_SEC / SECS_PER_MINUTE as i64) as i32
    }

    pub fn seconds(&self) -> i32 {
        (self.micros / MICROS_PER_SEC) as i32
    }

    pub fn microseconds(&self) -> i64 {
        self.micros
    }

    pub fn get_diff(_t1: Timestamp, _t2: Timestamp) -> Self {
        todo!("get interval type from 2 timestamps")
    }

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

    // `Interval` Type and i128 Convert
    pub fn from_i128(v: i128) -> Self {
        Interval {
            micros: (v as i64) / 1000,
            days: (v >> 64) as i32,
            months: (v >> 96) as i32,
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
}

impl FromStr for Interval {
    type Err = Error;

    // E.g: INTERVAL '1 years 2 months 3 days 4 hours 5 minutes 6 seconds'
    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        // parse str
        todo!("Interval::from_str")
    }
}

// impl Add trait for Interval
// TODO: overflow check
impl Add for Interval {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let months = self.months + rhs.months;
        let days = self.days + rhs.days;
        let micros = self.micros + rhs.micros;
        Self {
            months,
            days,
            micros,
        }
    }
}

// impl Neg for Interval
// TODO: overflow check, if months | days | micros is i32 or i64 MIN, then overflow
impl Neg for Interval {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self {
            months: -self.months,
            days: -self.days,
            micros: -self.micros,
        }
    }
}

// impl Sub trait for Interval
// TODO: overflow check
impl Sub for Interval {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let months = self.months - rhs.months;
        let days = self.days - rhs.days;
        let micros = self.micros - rhs.micros;
        Self {
            months,
            days,
            micros,
        }
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
    fn test_interval_add() {
        let interval = Interval::new(1, 1, 1);
        let interval2 = Interval::new(1, 1, 1);
        let interval3 = interval + interval2;
        assert_eq!(interval3.months, 2);
        assert_eq!(interval3.days, 2);
        assert_eq!(interval3.micros, 2);
    }

    #[test]
    fn test_interval_sub() {
        let interval = Interval::new(1, 1, 1);
        let interval2 = Interval::new(1, 1, 1);
        let interval3 = interval - interval2;
        assert_eq!(interval3.months, 0);
        assert_eq!(interval3.days, 0);
        assert_eq!(interval3.micros, 0);
    }

    #[test]
    fn test_interval_neg() {
        let interval = Interval::new(1, 1, 1);
        let interval2 = -interval;
        assert_eq!(interval2.months, -1);
        assert_eq!(interval2.days, -1);
        assert_eq!(interval2.micros, -1);
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
        assert_eq!(json, "{\"months\":1,\"days\":1,\"micros\":1}");
        let interval2: Interval = serde_json::from_str(&json).unwrap();
        assert_eq!(interval, interval2);
    }
}
