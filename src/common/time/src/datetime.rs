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

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use chrono::{Days, LocalResult, Months, NaiveDateTime, TimeZone as ChronoTimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, InvalidDateStrSnafu, Result};
use crate::timezone::Timezone;
use crate::util::{format_utc_datetime, local_datetime_to_utc};
use crate::{Date, Interval};

const DATETIME_FORMAT: &str = "%F %T";
const DATETIME_FORMAT_WITH_TZ: &str = "%F %T%z";

/// [DateTime] represents the **milliseconds elapsed since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct DateTime(i64);

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(abs_time) = NaiveDateTime::from_timestamp_millis(self.0) {
            write!(
                f,
                "{}",
                format_utc_datetime(&abs_time, DATETIME_FORMAT_WITH_TZ)
            )
        } else {
            write!(f, "DateTime({})", self.0)
        }
    }
}

impl From<DateTime> for serde_json::Value {
    fn from(d: DateTime) -> Self {
        serde_json::Value::String(d.to_string())
    }
}

impl From<NaiveDateTime> for DateTime {
    fn from(value: NaiveDateTime) -> Self {
        DateTime::from(value.timestamp_millis())
    }
}

impl FromStr for DateTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let timestamp_millis = if let Ok(d) = NaiveDateTime::parse_from_str(s, DATETIME_FORMAT) {
            match local_datetime_to_utc(&d) {
                LocalResult::None => {
                    return InvalidDateStrSnafu { raw: s }.fail();
                }
                LocalResult::Single(d) | LocalResult::Ambiguous(d, _) => d.timestamp_millis(),
            }
        } else if let Ok(v) = chrono::DateTime::parse_from_str(s, DATETIME_FORMAT_WITH_TZ) {
            v.timestamp_millis()
        } else {
            return InvalidDateStrSnafu { raw: s }.fail();
        };

        Ok(Self(timestamp_millis))
    }
}

impl From<i64> for DateTime {
    fn from(v: i64) -> Self {
        Self(v)
    }
}

impl From<Date> for DateTime {
    fn from(value: Date) -> Self {
        // It's safe, i32 * 86400000 won't be overflow
        Self(value.to_secs() * 1000)
    }
}

impl DateTime {
    /// Create a new [DateTime] from milliseconds elapsed since "1970-01-01 00:00:00 UTC" (UNIX Epoch).
    pub fn new(millis: i64) -> Self {
        Self(millis)
    }

    /// Get the milliseconds elapsed since "1970-01-01 00:00:00 UTC" (UNIX Epoch).
    pub fn val(&self) -> i64 {
        self.0
    }

    /// Convert to [NaiveDateTime].
    pub fn to_chrono_datetime(&self) -> Option<NaiveDateTime> {
        NaiveDateTime::from_timestamp_millis(self.0)
    }

    pub fn to_chrono_datetime_with_timezone(&self, tz: Option<Timezone>) -> Option<NaiveDateTime> {
        let datetime = self.to_chrono_datetime();
        datetime.map(|v| match tz {
            Some(Timezone::Offset(offset)) => offset.from_utc_datetime(&v).naive_local(),
            Some(Timezone::Named(tz)) => tz.from_utc_datetime(&v).naive_local(),
            None => Utc.from_utc_datetime(&v).naive_local(),
        })
    }
    /// Adds given Interval to the current datetime.
    /// Returns None if the resulting datetime would be out of range.
    pub fn add_interval(&self, interval: Interval) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;
        let (months, days, nsecs) = interval.to_month_day_nano();

        let naive_datetime = naive_datetime
            .checked_add_months(Months::new(months as u32))?
            .checked_add_days(Days::new(days as u64))?
            + Duration::from_nanos(nsecs as u64);

        Some(naive_datetime.into())
    }

    /// Subtracts given Interval to the current datetime.
    /// Returns None if the resulting datetime would be out of range.
    pub fn sub_interval(&self, interval: Interval) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;
        let (months, days, nsecs) = interval.to_month_day_nano();

        let naive_datetime = naive_datetime
            .checked_sub_months(Months::new(months as u32))?
            .checked_sub_days(Days::new(days as u64))?
            - Duration::from_nanos(nsecs as u64);

        Some(naive_datetime.into())
    }

    /// Convert to [common_time::date].
    pub fn to_date(&self) -> Option<Date> {
        self.to_chrono_datetime().map(|d| Date::from(d.date()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timezone::set_default_timezone;

    #[test]
    pub fn test_new_date_time() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        assert_eq!("1970-01-01 08:00:00+0800", DateTime::new(0).to_string());
        assert_eq!("1970-01-01 08:00:01+0800", DateTime::new(1000).to_string());
        assert_eq!("1970-01-01 07:59:59+0800", DateTime::new(-1000).to_string());
    }

    #[test]
    pub fn test_parse_from_string() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let time = "1970-01-01 00:00:00+0800";
        let dt = DateTime::from_str(time).unwrap();
        assert_eq!(time, &dt.to_string());
        let dt = DateTime::from_str("      1970-01-01       00:00:00+0800       ").unwrap();
        assert_eq!(time, &dt.to_string());
    }

    #[test]
    pub fn test_from() {
        let d: DateTime = 42.into();
        assert_eq!(42, d.val());
    }

    #[test]
    fn test_add_sub_interval() {
        let datetime = DateTime::new(1000);

        let interval = Interval::from_day_time(1, 200);

        let new_datetime = datetime.add_interval(interval).unwrap();
        assert_eq!(new_datetime.val(), 1000 + 3600 * 24 * 1000 + 200);

        assert_eq!(datetime, new_datetime.sub_interval(interval).unwrap());
    }

    #[test]
    fn test_parse_local_date_time() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        assert_eq!(
            -28800000,
            DateTime::from_str("1970-01-01 00:00:00").unwrap().val()
        );
        assert_eq!(0, DateTime::from_str("1970-01-01 08:00:00").unwrap().val());
    }

    #[test]
    fn test_parse_local_date_time_with_tz() {
        let ts = DateTime::from_str("1970-01-01 08:00:00+0000")
            .unwrap()
            .val();
        assert_eq!(28800000, ts);
    }

    #[test]
    fn test_from_max_date() {
        let date = Date::new(i32::MAX);
        let datetime = DateTime::from(date);
        assert_eq!(datetime.val(), 185542587100800000);
    }

    #[test]
    fn test_conversion_between_datetime_and_chrono_datetime() {
        let cases = [1, 10, 100, 1000, 100000];
        for case in cases {
            let dt = DateTime::new(case);
            let ndt = dt.to_chrono_datetime().unwrap();
            let dt2 = DateTime::from(ndt);
            assert_eq!(dt, dt2);
        }
    }
}
