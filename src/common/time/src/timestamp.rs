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

use core::default::Default;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::Duration;

use arrow::datatypes::TimeUnit as ArrowTimeUnit;
use chrono::{
    DateTime, Days, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeZone as ChronoTimeZone, Utc,
};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{ArithmeticOverflowSnafu, Error, ParseTimestampSnafu, TimestampOverflowSnafu};
use crate::timezone::{get_timezone, Timezone};
use crate::util::div_ceil;
use crate::{error, Interval};

/// Timestamp represents the value of units(seconds/milliseconds/microseconds/nanoseconds) elapsed
/// since UNIX epoch. The valid value range of [Timestamp] depends on it's unit (all in UTC timezone):
/// - for [TimeUnit::Second]: [-262144-01-01 00:00:00, +262143-12-31 23:59:59]
/// - for [TimeUnit::Millisecond]: [-262144-01-01 00:00:00.000, +262143-12-31 23:59:59.999]
/// - for [TimeUnit::Microsecond]: [-262144-01-01 00:00:00.000000, +262143-12-31 23:59:59.999999]
/// - for [TimeUnit::Nanosecond]: [1677-09-21 00:12:43.145225, 2262-04-11 23:47:16.854775807]
///
/// # Note:
/// For values out of range, you can still store these timestamps, but while performing arithmetic
/// or formatting operations, it will return an error or just overflow.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    value: i64,
    unit: TimeUnit,
}

impl Timestamp {
    /// Creates current timestamp in millisecond.
    pub fn current_millis() -> Self {
        Self {
            value: crate::util::current_time_millis(),
            unit: TimeUnit::Millisecond,
        }
    }

    /// Creates current timestamp in specific time `unit`.
    pub fn current_time(unit: TimeUnit) -> Timestamp {
        let now = chrono::Utc::now();
        let value = match unit {
            TimeUnit::Second => now.timestamp(),
            TimeUnit::Millisecond => now.timestamp_millis(),
            TimeUnit::Microsecond => now.timestamp_micros(),
            TimeUnit::Nanosecond => now.timestamp_nanos_opt().unwrap_or_default(),
        };
        Timestamp { value, unit }
    }

    /// Subtracts a duration from timestamp.
    /// # Note
    /// The result time unit remains unchanged even if `duration` has a different unit with `self`.
    /// For example, a timestamp with value 1 and time unit second, subtracted by 1 millisecond
    /// and the result is still 1 second.
    pub fn sub_duration(&self, duration: Duration) -> error::Result<Self> {
        let duration: i64 = match self.unit {
            TimeUnit::Second => {
                i64::try_from(duration.as_secs()).context(TimestampOverflowSnafu)?
            }
            TimeUnit::Millisecond => {
                i64::try_from(duration.as_millis()).context(TimestampOverflowSnafu)?
            }
            TimeUnit::Microsecond => {
                i64::try_from(duration.as_micros()).context(TimestampOverflowSnafu)?
            }
            TimeUnit::Nanosecond => {
                i64::try_from(duration.as_nanos()).context(TimestampOverflowSnafu)?
            }
        };

        let value = self
            .value
            .checked_sub(duration)
            .with_context(|| ArithmeticOverflowSnafu {
                msg: format!(
                    "Try to subtract timestamp: {:?} with duration: {:?}",
                    self, duration
                ),
            })?;
        Ok(Timestamp {
            value,
            unit: self.unit,
        })
    }

    /// Adds a duration to timestamp.
    /// # Note
    /// The result time unit remains unchanged even if `duration` has a different unit with `self`.
    /// For example, a timestamp with value 1 and time unit second, subtracted by 1 millisecond
    /// and the result is still 1 second.
    pub fn add_duration(&self, duration: Duration) -> error::Result<Self> {
        let duration: i64 = match self.unit {
            TimeUnit::Second => {
                i64::try_from(duration.as_secs()).context(TimestampOverflowSnafu)?
            }
            TimeUnit::Millisecond => {
                i64::try_from(duration.as_millis()).context(TimestampOverflowSnafu)?
            }
            TimeUnit::Microsecond => {
                i64::try_from(duration.as_micros()).context(TimestampOverflowSnafu)?
            }
            TimeUnit::Nanosecond => {
                i64::try_from(duration.as_nanos()).context(TimestampOverflowSnafu)?
            }
        };

        let value = self
            .value
            .checked_add(duration)
            .with_context(|| ArithmeticOverflowSnafu {
                msg: format!(
                    "Try to add timestamp: {:?} with duration: {:?}",
                    self, duration
                ),
            })?;
        Ok(Timestamp {
            value,
            unit: self.unit,
        })
    }

    /// Adds given Interval to the current timestamp.
    /// Returns None if the resulting timestamp would be out of range.
    pub fn add_interval(&self, interval: Interval) -> Option<Timestamp> {
        let naive_datetime = self.to_chrono_datetime()?;
        let (months, days, nsecs) = interval.to_month_day_nano();

        let naive_datetime = naive_datetime
            .checked_add_months(Months::new(months as u32))?
            .checked_add_days(Days::new(days as u64))?
            + Duration::from_nanos(nsecs as u64);

        match Timestamp::from_chrono_datetime(naive_datetime) {
            // Have to convert the new timestamp by the current unit.
            Some(ts) => ts.convert_to(self.unit),
            None => None,
        }
    }

    /// Subtracts given Interval to the current timestamp.
    /// Returns None if the resulting timestamp would be out of range.
    pub fn sub_interval(&self, interval: Interval) -> Option<Timestamp> {
        let naive_datetime = self.to_chrono_datetime()?;
        let (months, days, nsecs) = interval.to_month_day_nano();

        let naive_datetime = naive_datetime
            .checked_sub_months(Months::new(months as u32))?
            .checked_sub_days(Days::new(days as u64))?
            - Duration::from_nanos(nsecs as u64);

        match Timestamp::from_chrono_datetime(naive_datetime) {
            // Have to convert the new timestamp by the current unit.
            Some(ts) => ts.convert_to(self.unit),
            None => None,
        }
    }

    /// Subtracts current timestamp with another timestamp, yielding a duration.
    pub fn sub(&self, rhs: &Self) -> Option<chrono::Duration> {
        let lhs = self.to_chrono_datetime()?;
        let rhs = rhs.to_chrono_datetime()?;
        Some(lhs - rhs)
    }

    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { unit, value }
    }

    pub fn new_second(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Second,
        }
    }

    pub fn new_millisecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    pub fn new_microsecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Microsecond,
        }
    }

    pub fn new_nanosecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Nanosecond,
        }
    }

    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    /// Convert a timestamp to given time unit.
    /// Conversion from a timestamp with smaller unit to a larger unit may cause rounding error.
    /// Return `None` if conversion causes overflow.
    pub fn convert_to(&self, unit: TimeUnit) -> Option<Timestamp> {
        if self.unit().factor() >= unit.factor() {
            let mul = self.unit().factor() / unit.factor();
            let value = self.value.checked_mul(mul as i64)?;
            Some(Timestamp::new(value, unit))
        } else {
            let mul = unit.factor() / self.unit().factor();
            Some(Timestamp::new(self.value.div_euclid(mul as i64), unit))
        }
    }

    /// Convert a timestamp to given time unit.
    /// Conversion from a timestamp with smaller unit to a larger unit will round the value
    /// to ceil (positive infinity).
    /// Return `None` if conversion causes overflow.
    pub fn convert_to_ceil(&self, unit: TimeUnit) -> Option<Timestamp> {
        if self.unit().factor() >= unit.factor() {
            let mul = self.unit().factor() / unit.factor();
            let value = self.value.checked_mul(mul as i64)?;
            Some(Timestamp::new(value, unit))
        } else {
            let mul = unit.factor() / self.unit().factor();
            Some(Timestamp::new(div_ceil(self.value, mul as i64), unit))
        }
    }

    /// Split a [Timestamp] into seconds part and nanoseconds part.
    /// Notice the seconds part of split result is always rounded down to floor.
    pub fn split(&self) -> (i64, u32) {
        let sec_mul = (TimeUnit::Second.factor() / self.unit.factor()) as i64;
        let nsec_mul = (self.unit.factor() / TimeUnit::Nanosecond.factor()) as i64;

        let sec_div = self.value.div_euclid(sec_mul);
        let sec_mod = self.value.rem_euclid(sec_mul);
        // safety:  the max possible value of `sec_mod` is 999,999,999
        let nsec = u32::try_from(sec_mod * nsec_mul).unwrap();
        (sec_div, nsec)
    }

    /// Creates a new Timestamp instance from seconds and nanoseconds parts.
    /// Returns None if overflow.
    fn from_splits(sec: i64, nsec: u32) -> Option<Self> {
        if nsec == 0 {
            Some(Timestamp::new_second(sec))
        } else if nsec % 1_000_000 == 0 {
            let millis = nsec / 1_000_000;
            sec.checked_mul(1000)
                .and_then(|v| v.checked_add(millis as i64))
                .map(Timestamp::new_millisecond)
        } else if nsec % 1000 == 0 {
            let micros = nsec / 1000;
            sec.checked_mul(1_000_000)
                .and_then(|v| v.checked_add(micros as i64))
                .map(Timestamp::new_microsecond)
        } else {
            sec.checked_mul(1_000_000_000)
                .and_then(|v| v.checked_add(nsec as i64))
                .map(Timestamp::new_nanosecond)
        }
    }

    /// Format timestamp to ISO8601 string. If the timestamp exceeds what chrono timestamp can
    /// represent, this function simply print the timestamp unit and value in plain string.
    pub fn to_iso8601_string(&self) -> String {
        self.as_formatted_string("%Y-%m-%d %H:%M:%S%.f%z", None)
    }

    /// Format timestamp use **system timezone**.
    pub fn to_local_string(&self) -> String {
        self.as_formatted_string("%Y-%m-%d %H:%M:%S%.f", None)
    }

    /// Format timestamp for given timezone.
    /// If `tz==None`, the server default timezone will used.
    pub fn to_timezone_aware_string(&self, tz: Option<Timezone>) -> String {
        self.as_formatted_string("%Y-%m-%d %H:%M:%S%.f", tz)
    }

    fn as_formatted_string(self, pattern: &str, timezone: Option<Timezone>) -> String {
        if let Some(v) = self.to_chrono_datetime() {
            match get_timezone(timezone) {
                Timezone::Offset(offset) => {
                    format!("{}", offset.from_utc_datetime(&v).format(pattern))
                }
                Timezone::Named(tz) => {
                    format!("{}", tz.from_utc_datetime(&v).format(pattern))
                }
            }
        } else {
            format!("[Timestamp{}: {}]", self.unit, self.value)
        }
    }

    pub fn to_chrono_datetime(&self) -> Option<NaiveDateTime> {
        let (sec, nsec) = self.split();
        NaiveDateTime::from_timestamp_opt(sec, nsec)
    }

    pub fn to_chrono_datetime_with_timezone(&self, tz: Option<Timezone>) -> Option<NaiveDateTime> {
        let datetime = self.to_chrono_datetime();
        datetime.map(|v| match tz {
            Some(Timezone::Offset(offset)) => offset.from_utc_datetime(&v).naive_local(),
            Some(Timezone::Named(tz)) => tz.from_utc_datetime(&v).naive_local(),
            None => Utc.from_utc_datetime(&v).naive_local(),
        })
    }

    /// Convert timestamp to chrono date.
    pub fn to_chrono_date(&self) -> Option<NaiveDate> {
        self.to_chrono_datetime().map(|ndt| ndt.date())
    }

    /// Convert timestamp to chrono time.
    pub fn to_chrono_time(&self) -> Option<NaiveTime> {
        self.to_chrono_datetime().map(|ndt| ndt.time())
    }

    pub fn from_chrono_datetime(ndt: NaiveDateTime) -> Option<Self> {
        let sec = ndt.timestamp();
        let nsec = ndt.timestamp_subsec_nanos();
        Timestamp::from_splits(sec, nsec)
    }

    pub fn from_chrono_date(date: NaiveDate) -> Option<Self> {
        Timestamp::from_chrono_datetime(date.and_time(NaiveTime::default()))
    }
}

impl FromStr for Timestamp {
    type Err = Error;

    /// Accepts a string in RFC3339 / ISO8601 standard format and some variants and converts it to a nanosecond precision timestamp.
    /// This code is copied from [arrow-datafusion](https://github.com/apache/arrow-datafusion/blob/arrow2/datafusion-physical-expr/src/arrow_temporal_util.rs#L71)
    /// with some bugfixes.
    /// Supported format:
    /// - `2022-09-20T14:16:43.012345Z` (Zulu timezone)
    /// - `2022-09-20T14:16:43.012345+08:00` (Explicit offset)
    /// - `2022-09-20T14:16:43.012345` (Zulu timezone, with T)
    /// - `2022-09-20T14:16:43` (Zulu timezone, no fractional seconds, with T)
    /// - `2022-09-20 14:16:43.012345Z` (Zulu timezone, without T)
    /// - `2022-09-20 14:16:43` (Zulu timezone, without T)
    /// - `2022-09-20 14:16:43.012345` (Zulu timezone, without T)
    #[allow(deprecated)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // RFC3339 timestamp (with a T)
        let s = s.trim();
        if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
            return Timestamp::from_chrono_datetime(ts.naive_utc())
                .context(ParseTimestampSnafu { raw: s });
        }
        if let Ok(ts) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
            return Timestamp::from_chrono_datetime(ts.naive_utc())
                .context(ParseTimestampSnafu { raw: s });
        }
        if let Ok(ts) = chrono::Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
            return Timestamp::from_chrono_datetime(ts.naive_utc())
                .context(ParseTimestampSnafu { raw: s });
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return naive_datetime_to_timestamp(s, ts);
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            return naive_datetime_to_timestamp(s, ts);
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return naive_datetime_to_timestamp(s, ts);
        }

        if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
            return naive_datetime_to_timestamp(s, ts);
        }

        ParseTimestampSnafu { raw: s }.fail()
    }
}

/// Converts the naive datetime (which has no specific timezone) to a
/// nanosecond epoch timestamp in UTC.
fn naive_datetime_to_timestamp(
    s: &str,
    datetime: NaiveDateTime,
) -> crate::error::Result<Timestamp> {
    Timestamp::from_chrono_datetime(Utc.from_utc_datetime(&datetime).naive_utc())
        .context(ParseTimestampSnafu { raw: s })
}

impl From<i64> for Timestamp {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

impl From<Timestamp> for i64 {
    fn from(t: Timestamp) -> Self {
        t.value
    }
}

impl From<Timestamp> for serde_json::Value {
    fn from(d: Timestamp) -> Self {
        serde_json::Value::String(d.to_iso8601_string())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    #[default]
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(unit: &ArrowTimeUnit) -> Self {
        match unit {
            ArrowTimeUnit::Second => Self::Second,
            ArrowTimeUnit::Millisecond => Self::Millisecond,
            ArrowTimeUnit::Microsecond => Self::Microsecond,
            ArrowTimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

impl From<ArrowTimeUnit> for TimeUnit {
    fn from(unit: ArrowTimeUnit) -> Self {
        (&unit).into()
    }
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Second => {
                write!(f, "Second")
            }
            TimeUnit::Millisecond => {
                write!(f, "Millisecond")
            }
            TimeUnit::Microsecond => {
                write!(f, "Microsecond")
            }
            TimeUnit::Nanosecond => {
                write!(f, "Nanosecond")
            }
        }
    }
}

impl TimeUnit {
    pub fn factor(&self) -> u32 {
        match self {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        }
    }

    pub(crate) fn short_name(&self) -> &'static str {
        match self {
            TimeUnit::Second => "s",
            TimeUnit::Millisecond => "ms",
            TimeUnit::Microsecond => "us",
            TimeUnit::Nanosecond => "ns",
        }
    }

    pub fn as_arrow_time_unit(&self) -> ArrowTimeUnit {
        match self {
            Self::Second => ArrowTimeUnit::Second,
            Self::Millisecond => ArrowTimeUnit::Millisecond,
            Self::Microsecond => ArrowTimeUnit::Microsecond,
            Self::Nanosecond => ArrowTimeUnit::Nanosecond,
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A proper implementation of total order requires antisymmetry, reflexivity, transitivity and totality.
/// In this comparison implementation, we map a timestamp uniquely to a `(i64, i64)` tuple which is respectively
/// total order.
impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        // fast path: most comparisons use the same unit.
        if self.unit == other.unit {
            return self.value.cmp(&other.value);
        }

        let (s_sec, s_nsec) = self.split();
        let (o_sec, o_nsec) = other.split();
        match s_sec.cmp(&o_sec) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => s_nsec.cmp(&o_nsec),
        }
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Timestamp {}

impl Hash for Timestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (sec, nsec) = self.split();
        state.write_i64(sec);
        state.write_u32(nsec);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use rand::Rng;
    use serde_json::Value;

    use super::*;
    use crate::timezone::set_default_timezone;

    #[test]
    pub fn test_time_unit() {
        assert_eq!(
            TimeUnit::Millisecond.factor() * 1000,
            TimeUnit::Second.factor()
        );
        assert_eq!(
            TimeUnit::Microsecond.factor() * 1000000,
            TimeUnit::Second.factor()
        );
        assert_eq!(
            TimeUnit::Nanosecond.factor() * 1000000000,
            TimeUnit::Second.factor()
        );
    }

    #[test]
    pub fn test_timestamp() {
        let t = Timestamp::new(1, TimeUnit::Millisecond);
        assert_eq!(TimeUnit::Millisecond, t.unit());
        assert_eq!(1, t.value());
        assert_eq!(Timestamp::new(1000, TimeUnit::Microsecond), t);
        assert!(t > Timestamp::new(999, TimeUnit::Microsecond));
    }

    #[test]
    fn test_timestamp_antisymmetry() {
        let t1 = Timestamp::new(1, TimeUnit::Second);
        let t2 = Timestamp::new(1000, TimeUnit::Millisecond);
        assert!(t1 >= t2);
        assert!(t2 >= t1);
        assert_eq!(Ordering::Equal, t1.cmp(&t2));
    }

    fn gen_random_ts() -> Timestamp {
        let units = [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];
        let mut rng = rand::thread_rng();
        let unit_idx: usize = rng.gen_range(0..4);
        let unit = units[unit_idx];
        let value: i64 = rng.gen();
        Timestamp::new(value, unit)
    }

    #[test]
    fn test_add_sub_interval() {
        let ts = Timestamp::new(1000, TimeUnit::Millisecond);

        let interval = Interval::from_day_time(1, 200);

        let new_ts = ts.add_interval(interval).unwrap();
        assert_eq!(new_ts.unit(), TimeUnit::Millisecond);
        assert_eq!(new_ts.value(), 1000 + 3600 * 24 * 1000 + 200);

        assert_eq!(ts, new_ts.sub_interval(interval).unwrap());
    }

    #[test]
    fn test_timestamp_reflexivity() {
        for _ in 0..1000 {
            let ts = gen_random_ts();
            assert!(ts >= ts, "ts: {ts:?}");
        }
    }

    /// Generate timestamp less than or equal to `threshold`
    fn gen_ts_le(threshold: &Timestamp) -> Timestamp {
        let mut rng = rand::thread_rng();
        let timestamp = rng.gen_range(i64::MIN..=threshold.value);
        Timestamp::new(timestamp, threshold.unit)
    }

    #[test]
    fn test_timestamp_transitivity() {
        let t0 = Timestamp::new_millisecond(100);
        let t1 = gen_ts_le(&t0);
        let t2 = gen_ts_le(&t1);
        assert!(t0 >= t1, "t0: {t0:?}, t1: {t1:?}");
        assert!(t1 >= t2, "t1: {t1:?}, t2: {t2:?}");
        assert!(t0 >= t2, "t0: {t0:?}, t2: {t2:?}");

        let t0 = Timestamp::new_millisecond(-100);
        let t1 = gen_ts_le(&t0); // t0 >= t1
        let t2 = gen_ts_le(&t1); // t1 >= t2
        assert!(t0 >= t1, "t0: {t0:?}, t1: {t1:?}");
        assert!(t1 >= t2, "t1: {t1:?}, t2: {t2:?}");
        assert!(t0 >= t2, "t0: {t0:?}, t2: {t2:?}"); // check if t0 >= t2
    }

    #[test]
    fn test_antisymmetry() {
        let t0 = Timestamp::new(1, TimeUnit::Second);
        let t1 = Timestamp::new(1000, TimeUnit::Millisecond);
        assert!(t0 >= t1, "t0: {t0:?}, t1: {t1:?}");
        assert!(t1 >= t0, "t0: {t0:?}, t1: {t1:?}");
        assert_eq!(t1, t0, "t0: {t0:?}, t1: {t1:?}");
    }

    #[test]
    fn test_strong_connectivity() {
        let mut values = Vec::with_capacity(1000);
        for _ in 0..1000 {
            values.push(gen_random_ts());
        }

        for l in &values {
            for r in &values {
                assert!(l >= r || l <= r, "l: {l:?}, r: {r:?}");
            }
        }
    }

    #[test]
    fn test_cmp_timestamp() {
        let t1 = Timestamp::new(0, TimeUnit::Millisecond);
        let t2 = Timestamp::new(0, TimeUnit::Second);
        assert_eq!(t2, t1);

        let t1 = Timestamp::new(1, TimeUnit::Millisecond);
        let t2 = Timestamp::new(-1, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Timestamp::new(i64::MAX / 1000 * 1000, TimeUnit::Millisecond);
        let t2 = Timestamp::new(i64::MAX / 1000, TimeUnit::Second);
        assert_eq!(t2, t1);

        let t1 = Timestamp::new(i64::MAX, TimeUnit::Millisecond);
        let t2 = Timestamp::new(i64::MAX / 1000 + 1, TimeUnit::Second);
        assert!(t2 > t1);

        let t1 = Timestamp::new(i64::MAX, TimeUnit::Millisecond);
        let t2 = Timestamp::new(i64::MAX / 1000, TimeUnit::Second);
        assert!(t2 < t1);

        let t1 = Timestamp::new(10_010_001, TimeUnit::Millisecond);
        let t2 = Timestamp::new(100, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Timestamp::new(-100 * 10_001, TimeUnit::Millisecond);
        let t2 = Timestamp::new(-100, TimeUnit::Second);
        assert!(t2 > t1);

        let t1 = Timestamp::new(i64::MIN, TimeUnit::Millisecond);
        let t2 = Timestamp::new(i64::MIN / 1000 - 1, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Timestamp::new(i64::MIN, TimeUnit::Millisecond);
        let t2 = Timestamp::new(i64::MIN + 1, TimeUnit::Millisecond);
        assert!(t2 > t1);

        let t1 = Timestamp::new(i64::MAX, TimeUnit::Millisecond);
        let t2 = Timestamp::new(i64::MIN, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Timestamp::new(0, TimeUnit::Nanosecond);
        let t2 = Timestamp::new(i64::MIN, TimeUnit::Second);
        assert!(t1 > t2);
    }

    fn check_hash_eq(t1: Timestamp, t2: Timestamp) {
        let mut hasher = DefaultHasher::new();
        t1.hash(&mut hasher);
        let t1_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        t2.hash(&mut hasher);
        let t2_hash = hasher.finish();
        assert_eq!(t2_hash, t1_hash);
    }

    #[test]
    fn test_hash() {
        check_hash_eq(
            Timestamp::new(0, TimeUnit::Millisecond),
            Timestamp::new(0, TimeUnit::Second),
        );
        check_hash_eq(
            Timestamp::new(1000, TimeUnit::Millisecond),
            Timestamp::new(1, TimeUnit::Second),
        );
        check_hash_eq(
            Timestamp::new(1_000_000, TimeUnit::Microsecond),
            Timestamp::new(1, TimeUnit::Second),
        );
        check_hash_eq(
            Timestamp::new(1_000_000_000, TimeUnit::Nanosecond),
            Timestamp::new(1, TimeUnit::Second),
        );
    }

    #[test]
    pub fn test_from_i64() {
        let t: Timestamp = 42.into();
        assert_eq!(42, t.value());
        assert_eq!(TimeUnit::Millisecond, t.unit());
    }

    // Input timestamp string is regarded as local timezone if no timezone is specified,
    // but expected timestamp is in UTC timezone
    fn check_from_str(s: &str, expect: &str) {
        let ts = Timestamp::from_str(s).unwrap();
        let time = ts.to_chrono_datetime().unwrap();
        assert_eq!(expect, time.to_string());
    }

    #[test]
    fn test_from_str() {
        // Explicit Z means timestamp in UTC
        check_from_str("2020-09-08 13:42:29Z", "2020-09-08 13:42:29");
        check_from_str("2020-09-08T13:42:29+08:00", "2020-09-08 05:42:29");

        check_from_str("2020-09-08 13:42:29", "2020-09-08 13:42:29");

        check_from_str("2020-09-08 13:42:29.042Z", "2020-09-08 13:42:29.042");
        check_from_str("2020-09-08 13:42:29.042+08:00", "2020-09-08 05:42:29.042");

        check_from_str(
            "2020-09-08T13:42:29.0042+08:00",
            "2020-09-08 05:42:29.004200",
        );
    }

    #[test]
    fn test_to_iso8601_string() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let datetime_str = "2020-09-08 13:42:29.042+0000";
        let ts = Timestamp::from_str(datetime_str).unwrap();
        assert_eq!("2020-09-08 21:42:29.042+0800", ts.to_iso8601_string());

        let ts_millis = 1668070237000;
        let ts = Timestamp::new_millisecond(ts_millis);
        assert_eq!("2022-11-10 16:50:37+0800", ts.to_iso8601_string());

        let ts_millis = -1000;
        let ts = Timestamp::new_millisecond(ts_millis);
        assert_eq!("1970-01-01 07:59:59+0800", ts.to_iso8601_string());

        let ts_millis = -1;
        let ts = Timestamp::new_millisecond(ts_millis);
        assert_eq!("1970-01-01 07:59:59.999+0800", ts.to_iso8601_string());

        let ts_millis = -1001;
        let ts = Timestamp::new_millisecond(ts_millis);
        assert_eq!("1970-01-01 07:59:58.999+0800", ts.to_iso8601_string());
    }

    #[test]
    fn test_serialize_to_json_value() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        assert_eq!(
            "1970-01-01 08:00:01+0800",
            match serde_json::Value::from(Timestamp::new(1, TimeUnit::Second)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );

        assert_eq!(
            "1970-01-01 08:00:00.001+0800",
            match serde_json::Value::from(Timestamp::new(1, TimeUnit::Millisecond)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );

        assert_eq!(
            "1970-01-01 08:00:00.000001+0800",
            match serde_json::Value::from(Timestamp::new(1, TimeUnit::Microsecond)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );

        assert_eq!(
            "1970-01-01 08:00:00.000000001+0800",
            match serde_json::Value::from(Timestamp::new(1, TimeUnit::Nanosecond)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );
    }

    #[test]
    fn test_convert_timestamp() {
        let ts = Timestamp::new(1, TimeUnit::Second);
        assert_eq!(
            Timestamp::new(1000, TimeUnit::Millisecond),
            ts.convert_to(TimeUnit::Millisecond).unwrap()
        );
        assert_eq!(
            Timestamp::new(1_000_000, TimeUnit::Microsecond),
            ts.convert_to(TimeUnit::Microsecond).unwrap()
        );
        assert_eq!(
            Timestamp::new(1_000_000_000, TimeUnit::Nanosecond),
            ts.convert_to(TimeUnit::Nanosecond).unwrap()
        );

        let ts = Timestamp::new(1_000_100_100, TimeUnit::Nanosecond);
        assert_eq!(
            Timestamp::new(1_000_100, TimeUnit::Microsecond),
            ts.convert_to(TimeUnit::Microsecond).unwrap()
        );
        assert_eq!(
            Timestamp::new(1000, TimeUnit::Millisecond),
            ts.convert_to(TimeUnit::Millisecond).unwrap()
        );
        assert_eq!(
            Timestamp::new(1, TimeUnit::Second),
            ts.convert_to(TimeUnit::Second).unwrap()
        );

        let ts = Timestamp::new(1_000_100_100, TimeUnit::Nanosecond);
        assert_eq!(ts, ts.convert_to(TimeUnit::Nanosecond).unwrap());
        let ts = Timestamp::new(1_000_100_100, TimeUnit::Microsecond);
        assert_eq!(ts, ts.convert_to(TimeUnit::Microsecond).unwrap());
        let ts = Timestamp::new(1_000_100_100, TimeUnit::Millisecond);
        assert_eq!(ts, ts.convert_to(TimeUnit::Millisecond).unwrap());
        let ts = Timestamp::new(1_000_100_100, TimeUnit::Second);
        assert_eq!(ts, ts.convert_to(TimeUnit::Second).unwrap());

        // -9223372036854775808 in milliseconds should be rounded up to -9223372036854776 in seconds
        assert_eq!(
            Timestamp::new(-9223372036854776, TimeUnit::Second),
            Timestamp::new(i64::MIN, TimeUnit::Millisecond)
                .convert_to(TimeUnit::Second)
                .unwrap()
        );

        assert!(Timestamp::new(i64::MAX, TimeUnit::Second)
            .convert_to(TimeUnit::Millisecond)
            .is_none());
    }

    #[test]
    fn test_split() {
        assert_eq!((0, 0), Timestamp::new(0, TimeUnit::Second).split());
        assert_eq!((1, 0), Timestamp::new(1, TimeUnit::Second).split());
        assert_eq!(
            (0, 1_000_000),
            Timestamp::new(1, TimeUnit::Millisecond).split()
        );

        assert_eq!((0, 1_000), Timestamp::new(1, TimeUnit::Microsecond).split());
        assert_eq!((0, 1), Timestamp::new(1, TimeUnit::Nanosecond).split());

        assert_eq!(
            (1, 1_000_000),
            Timestamp::new(1001, TimeUnit::Millisecond).split()
        );

        assert_eq!(
            (-2, 999_000_000),
            Timestamp::new(-1001, TimeUnit::Millisecond).split()
        );

        // check min value of nanos
        let (sec, nsec) = Timestamp::new(i64::MIN, TimeUnit::Nanosecond).split();
        assert_eq!(
            i64::MIN as i128,
            sec as i128 * (TimeUnit::Second.factor() / TimeUnit::Nanosecond.factor()) as i128
                + nsec as i128
        );

        assert_eq!(
            (i64::MAX, 0),
            Timestamp::new(i64::MAX, TimeUnit::Second).split()
        );
    }

    #[test]
    fn test_convert_to_ceil() {
        assert_eq!(
            Timestamp::new(1, TimeUnit::Second),
            Timestamp::new(1000, TimeUnit::Millisecond)
                .convert_to_ceil(TimeUnit::Second)
                .unwrap()
        );

        // These two cases shows how `Timestamp::convert_to_ceil` behaves differently
        // from `Timestamp::convert_to` when converting larger unit to smaller unit.
        assert_eq!(
            Timestamp::new(1, TimeUnit::Second),
            Timestamp::new(1001, TimeUnit::Millisecond)
                .convert_to(TimeUnit::Second)
                .unwrap()
        );
        assert_eq!(
            Timestamp::new(2, TimeUnit::Second),
            Timestamp::new(1001, TimeUnit::Millisecond)
                .convert_to_ceil(TimeUnit::Second)
                .unwrap()
        );

        assert_eq!(
            Timestamp::new(-1, TimeUnit::Second),
            Timestamp::new(-1, TimeUnit::Millisecond)
                .convert_to(TimeUnit::Second)
                .unwrap()
        );
        assert_eq!(
            Timestamp::new(0, TimeUnit::Second),
            Timestamp::new(-1, TimeUnit::Millisecond)
                .convert_to_ceil(TimeUnit::Second)
                .unwrap()
        );

        // When converting large unit to smaller unit, there will be no rounding error,
        // so `Timestamp::convert_to_ceil` behaves just like `Timestamp::convert_to`
        assert_eq!(
            Timestamp::new(-1, TimeUnit::Second).convert_to(TimeUnit::Millisecond),
            Timestamp::new(-1, TimeUnit::Second).convert_to_ceil(TimeUnit::Millisecond)
        );
        assert_eq!(
            Timestamp::new(1000, TimeUnit::Second).convert_to(TimeUnit::Millisecond),
            Timestamp::new(1000, TimeUnit::Second).convert_to_ceil(TimeUnit::Millisecond)
        );
        assert_eq!(
            Timestamp::new(1, TimeUnit::Second).convert_to(TimeUnit::Millisecond),
            Timestamp::new(1, TimeUnit::Second).convert_to_ceil(TimeUnit::Millisecond)
        );
    }

    #[test]
    fn test_split_overflow() {
        let _ = Timestamp::new(i64::MAX, TimeUnit::Second).split();
        let _ = Timestamp::new(i64::MIN, TimeUnit::Second).split();
        let _ = Timestamp::new(i64::MAX, TimeUnit::Millisecond).split();
        let _ = Timestamp::new(i64::MIN, TimeUnit::Millisecond).split();
        let _ = Timestamp::new(i64::MAX, TimeUnit::Microsecond).split();
        let _ = Timestamp::new(i64::MIN, TimeUnit::Microsecond).split();
        let _ = Timestamp::new(i64::MAX, TimeUnit::Nanosecond).split();
        let _ = Timestamp::new(i64::MIN, TimeUnit::Nanosecond).split();
        let (sec, nsec) = Timestamp::new(i64::MIN, TimeUnit::Nanosecond).split();
        let time = NaiveDateTime::from_timestamp_opt(sec, nsec).unwrap();
        assert_eq!(sec, time.timestamp());
        assert_eq!(nsec, time.timestamp_subsec_nanos());
    }

    #[test]
    fn test_timestamp_sub() {
        let res = Timestamp::new(1, TimeUnit::Second)
            .sub_duration(Duration::from_secs(1))
            .unwrap();
        assert_eq!(0, res.value);
        assert_eq!(TimeUnit::Second, res.unit);

        let res = Timestamp::new(0, TimeUnit::Second)
            .sub_duration(Duration::from_secs(1))
            .unwrap();
        assert_eq!(-1, res.value);
        assert_eq!(TimeUnit::Second, res.unit);

        let res = Timestamp::new(1, TimeUnit::Second)
            .sub_duration(Duration::from_millis(1))
            .unwrap();
        assert_eq!(1, res.value);
        assert_eq!(TimeUnit::Second, res.unit);
    }

    #[test]
    fn test_timestamp_add() {
        let res = Timestamp::new(1, TimeUnit::Second)
            .add_duration(Duration::from_secs(1))
            .unwrap();
        assert_eq!(2, res.value);
        assert_eq!(TimeUnit::Second, res.unit);

        let res = Timestamp::new(0, TimeUnit::Second)
            .add_duration(Duration::from_secs(1))
            .unwrap();
        assert_eq!(1, res.value);
        assert_eq!(TimeUnit::Second, res.unit);

        let res = Timestamp::new(1, TimeUnit::Second)
            .add_duration(Duration::from_millis(1))
            .unwrap();
        assert_eq!(1, res.value);
        assert_eq!(TimeUnit::Second, res.unit);

        let res = Timestamp::new(100, TimeUnit::Second)
            .add_duration(Duration::from_millis(1000))
            .unwrap();
        assert_eq!(101, res.value);
        assert_eq!(TimeUnit::Second, res.unit);
    }

    // $TZ doesn't take effort.
    #[test]
    fn test_parse_in_timezone() {
        std::env::set_var("TZ", "Asia/Shanghai");
        assert_eq!(
            Timestamp::new(28800, TimeUnit::Second),
            Timestamp::from_str("1970-01-01 08:00:00.000").unwrap()
        );

        assert_eq!(
            Timestamp::new(28800, TimeUnit::Second),
            Timestamp::from_str("1970-01-01 08:00:00").unwrap()
        );

        assert_eq!(
            Timestamp::new(28800, TimeUnit::Second),
            Timestamp::from_str("      1970-01-01        08:00:00    ").unwrap()
        );
    }

    #[test]
    fn test_to_local_string() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();

        assert_eq!(
            "1970-01-01 08:00:00.000000001",
            Timestamp::new(1, TimeUnit::Nanosecond).to_local_string()
        );

        assert_eq!(
            "1970-01-01 08:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond).to_local_string()
        );

        assert_eq!(
            "1970-01-01 08:00:01",
            Timestamp::new(1, TimeUnit::Second).to_local_string()
        );
    }

    #[test]
    fn test_subtract_timestamp() {
        assert_eq!(
            Some(chrono::Duration::milliseconds(42)),
            Timestamp::new_millisecond(100).sub(&Timestamp::new_millisecond(58))
        );

        assert_eq!(
            Some(chrono::Duration::milliseconds(-42)),
            Timestamp::new_millisecond(58).sub(&Timestamp::new_millisecond(100))
        );
    }

    #[test]
    fn test_to_timezone_aware_string() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        std::env::set_var("TZ", "Asia/Shanghai");
        assert_eq!(
            "1970-01-01 08:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("SYSTEM").unwrap()))
        );
        assert_eq!(
            "1970-01-01 08:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("SYSTEM").unwrap()))
        );
        assert_eq!(
            "1970-01-01 08:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("+08:00").unwrap()))
        );
        assert_eq!(
            "1970-01-01 07:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("+07:00").unwrap()))
        );
        assert_eq!(
            "1969-12-31 23:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("-01:00").unwrap()))
        );
        assert_eq!(
            "1970-01-01 08:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("Asia/Shanghai").unwrap()))
        );
        assert_eq!(
            "1970-01-01 00:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("UTC").unwrap()))
        );
        assert_eq!(
            "1970-01-01 01:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("Europe/Berlin").unwrap()))
        );
        assert_eq!(
            "1970-01-01 03:00:00.001",
            Timestamp::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("Europe/Moscow").unwrap()))
        );
    }

    #[test]
    fn test_from_arrow_time_unit() {
        assert_eq!(TimeUnit::Second, TimeUnit::from(ArrowTimeUnit::Second));
        assert_eq!(
            TimeUnit::Millisecond,
            TimeUnit::from(ArrowTimeUnit::Millisecond)
        );
        assert_eq!(
            TimeUnit::Microsecond,
            TimeUnit::from(ArrowTimeUnit::Microsecond)
        );
        assert_eq!(
            TimeUnit::Nanosecond,
            TimeUnit::from(ArrowTimeUnit::Nanosecond)
        );
    }

    fn check_conversion(ts: Timestamp, valid: bool) {
        let Some(t2) = ts.to_chrono_datetime() else {
            if valid {
                panic!("Cannot convert {:?} to Chrono NaiveDateTime", ts);
            }
            return;
        };
        let Some(t3) = Timestamp::from_chrono_datetime(t2) else {
            if valid {
                panic!("Cannot convert Chrono NaiveDateTime {:?} to Timestamp", t2);
            }
            return;
        };

        assert_eq!(t3, ts);
    }

    #[test]
    fn test_from_naive_date_time() {
        let min_sec = Timestamp::new_second(-8334632851200);
        let max_sec = Timestamp::new_second(8210298412799);
        check_conversion(min_sec, true);
        check_conversion(Timestamp::new_second(min_sec.value - 1), false);
        check_conversion(max_sec, true);
        check_conversion(Timestamp::new_second(max_sec.value + 1), false);

        let min_millis = Timestamp::new_millisecond(-8334632851200000);
        let max_millis = Timestamp::new_millisecond(8210298412799999);
        check_conversion(min_millis, true);
        check_conversion(Timestamp::new_millisecond(min_millis.value - 1), false);
        check_conversion(max_millis, true);
        check_conversion(Timestamp::new_millisecond(max_millis.value + 1), false);

        let min_micros = Timestamp::new_microsecond(-8334632851200000000);
        let max_micros = Timestamp::new_microsecond(8210298412799999999);
        check_conversion(min_micros, true);
        check_conversion(Timestamp::new_microsecond(min_micros.value - 1), false);
        check_conversion(max_micros, true);
        check_conversion(Timestamp::new_microsecond(max_micros.value + 1), false);

        let min_nanos = Timestamp::new_nanosecond(-9223372036854775000);
        let max_nanos = Timestamp::new_nanosecond(i64::MAX);
        check_conversion(min_nanos, true);
        check_conversion(Timestamp::new_nanosecond(min_nanos.value - 1), false);
        check_conversion(max_nanos, true);
    }

    #[test]
    fn test_parse_timestamp_range() {
        let valid_strings = vec![
            "-262144-01-01 00:00:00Z",
            "+262143-12-31 23:59:59Z",
            "-262144-01-01 00:00:00Z",
            "+262143-12-31 23:59:59.999Z",
            "-262144-01-01 00:00:00Z",
            "+262143-12-31 23:59:59.999999Z",
            "1677-09-21 00:12:43.145225Z",
            "2262-04-11 23:47:16.854775807Z",
            "+100000-01-01 00:00:01.5Z",
        ];

        for s in valid_strings {
            Timestamp::from_str(s).unwrap();
        }
    }
}
