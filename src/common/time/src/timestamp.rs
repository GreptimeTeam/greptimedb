use core::default::Default;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use chrono::{offset::Local, DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, ParseTimestampSnafu};

#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    value: i64,
    unit: TimeUnit,
}

impl Timestamp {
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { unit, value }
    }

    pub fn from_millis(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn convert_to(&self, unit: TimeUnit) -> i64 {
        // TODO(hl): May result into overflow
        self.value * self.unit.factor() / unit.factor()
    }

    pub fn to_iso8601_string(&self) -> String {
        let nano_factor = TimeUnit::Second.factor() / TimeUnit::Nanosecond.factor();

        let mut secs = self.convert_to(TimeUnit::Second);
        let mut nsecs = self.convert_to(TimeUnit::Nanosecond) % nano_factor;

        if nsecs < 0 {
            secs -= 1;
            nsecs += nano_factor;
        }

        let datetime = Utc.timestamp(secs, nsecs as u32);
        format!("{}", datetime.format("%Y-%m-%d %H:%M:%S%.f%z"))
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
    /// - `2022-09-20T14:16:43.012345` (local timezone, with T)
    /// - `2022-09-20T14:16:43` (local timezone, no fractional seconds, with T)
    /// - `2022-09-20 14:16:43.012345Z` (Zulu timezone, without T)
    /// - `2022-09-20 14:16:43` (local timezone, without T)
    /// - `2022-09-20 14:16:43.012345` (local timezone, without T)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // RFC3339 timestamp (with a T)
        if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
            return Ok(Timestamp::new(ts.timestamp_nanos(), TimeUnit::Nanosecond));
        }
        if let Ok(ts) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
            return Ok(Timestamp::new(ts.timestamp_nanos(), TimeUnit::Nanosecond));
        }
        if let Ok(ts) = Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
            return Ok(Timestamp::new(ts.timestamp_nanos(), TimeUnit::Nanosecond));
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
/// nanosecond epoch timestamp relative to UTC.
/// This code is copied from [arrow-datafusion](https://github.com/apache/arrow-datafusion/blob/arrow2/datafusion-physical-expr/src/arrow_temporal_util.rs#L137).
fn naive_datetime_to_timestamp(
    s: &str,
    datetime: NaiveDateTime,
) -> crate::error::Result<Timestamp> {
    let l = Local {};

    match l.from_local_datetime(&datetime) {
        LocalResult::None => ParseTimestampSnafu { raw: s }.fail(),
        LocalResult::Single(local_datetime) => Ok(Timestamp::new(
            local_datetime.with_timezone(&Utc).timestamp_nanos(),
            TimeUnit::Nanosecond,
        )),
        LocalResult::Ambiguous(local_datetime, _) => Ok(Timestamp::new(
            local_datetime.with_timezone(&Utc).timestamp_nanos(),
            TimeUnit::Nanosecond,
        )),
    }
}

impl From<i64> for Timestamp {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
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

impl TimeUnit {
    pub fn factor(&self) -> i64 {
        match self {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.value * self.unit.factor()).partial_cmp(&(other.value * other.unit.factor()))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.value * self.unit.factor()).cmp(&(other.value * other.unit.factor()))
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.convert_to(TimeUnit::Nanosecond) == other.convert_to(TimeUnit::Nanosecond)
    }
}

impl Eq for Timestamp {}

impl Hash for Timestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.convert_to(TimeUnit::Nanosecond));
        state.finish();
    }
}

#[cfg(test)]
mod tests {
    use chrono::Offset;

    use super::*;

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
    pub fn test_from_i64() {
        let t: Timestamp = 42.into();
        assert_eq!(42, t.value());
        assert_eq!(TimeUnit::Millisecond, t.unit());
    }

    // Input timestamp string is regarded as local timezone if no timezone is specified,
    // but expected timestamp is in UTC timezone
    fn check_from_str(s: &str, expect: &str) {
        let ts = Timestamp::from_str(s).unwrap();
        let time = NaiveDateTime::from_timestamp(
            ts.value / 1_000_000_000,
            (ts.value % 1_000_000_000) as u32,
        );
        assert_eq!(expect, time.to_string());
    }

    #[test]
    fn test_from_str() {
        // Explicit Z means timestamp in UTC
        check_from_str("2020-09-08 13:42:29Z", "2020-09-08 13:42:29");
        check_from_str("2020-09-08T13:42:29+08:00", "2020-09-08 05:42:29");

        check_from_str(
            "2020-09-08 13:42:29",
            &NaiveDateTime::from_timestamp_opt(
                1599572549 - Local.timestamp(0, 0).offset().fix().local_minus_utc() as i64,
                0,
            )
            .unwrap()
            .to_string(),
        );

        check_from_str(
            "2020-09-08T13:42:29",
            &NaiveDateTime::from_timestamp_opt(
                1599572549 - Local.timestamp(0, 0).offset().fix().local_minus_utc() as i64,
                0,
            )
            .unwrap()
            .to_string(),
        );

        check_from_str(
            "2020-09-08 13:42:29.042",
            &NaiveDateTime::from_timestamp_opt(
                1599572549 - Local.timestamp(0, 0).offset().fix().local_minus_utc() as i64,
                42000000,
            )
            .unwrap()
            .to_string(),
        );
        check_from_str("2020-09-08 13:42:29.042Z", "2020-09-08 13:42:29.042");
        check_from_str("2020-09-08 13:42:29.042+08:00", "2020-09-08 05:42:29.042");
        check_from_str(
            "2020-09-08T13:42:29.042",
            &NaiveDateTime::from_timestamp_opt(
                1599572549 - Local.timestamp(0, 0).offset().fix().local_minus_utc() as i64,
                42000000,
            )
            .unwrap()
            .to_string(),
        );
        check_from_str("2020-09-08T13:42:29+08:00", "2020-09-08 05:42:29");
        check_from_str(
            "2020-09-08T13:42:29.0042+08:00",
            "2020-09-08 05:42:29.004200",
        );
    }

    #[test]
    fn test_to_iso8601_string() {
        let datetime_str = "2020-09-08 13:42:29.042+0000";
        let ts = Timestamp::from_str(datetime_str).unwrap();
        assert_eq!(datetime_str, ts.to_iso8601_string());

        let ts_millis = 1668070237000;
        let ts = Timestamp::from_millis(ts_millis);
        assert_eq!("2022-11-10 08:50:37+0000", ts.to_iso8601_string());

        let ts_millis = -1000;
        let ts = Timestamp::from_millis(ts_millis);
        assert_eq!("1969-12-31 23:59:59+0000", ts.to_iso8601_string());

        let ts_millis = -1;
        let ts = Timestamp::from_millis(ts_millis);
        assert_eq!("1969-12-31 23:59:59.999+0000", ts.to_iso8601_string());

        let ts_millis = -1001;
        let ts = Timestamp::from_millis(ts_millis);
        assert_eq!("1969-12-31 23:59:58.999+0000", ts.to_iso8601_string());
    }
}
