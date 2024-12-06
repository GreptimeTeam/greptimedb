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

use std::fmt::{Display, Formatter, Write};

use chrono::{
    Days, LocalResult, Months, NaiveDateTime, TimeDelta, TimeZone as ChronoTimeZone, Utc,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{InvalidDateStrSnafu, Result};
use crate::interval::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use crate::timezone::{get_timezone, Timezone};
use crate::util::{datetime_to_utc, format_utc_datetime};
use crate::Date;

const DATETIME_FORMAT: &str = "%F %H:%M:%S%.f";
const DATETIME_FORMAT_WITH_TZ: &str = "%F %H:%M:%S%.f%z";

/// [DateTime] represents the **milliseconds elapsed since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct DateTime(i64);

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(abs_time) = chrono::DateTime::from_timestamp_millis(self.0) {
            write!(
                f,
                "{}",
                format_utc_datetime(&abs_time.naive_utc(), DATETIME_FORMAT_WITH_TZ)
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
        DateTime::from(value.and_utc().timestamp_millis())
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
    /// Try parsing a string into [`DateTime`] with the system timezone.
    /// See `DateTime::from_str`.
    pub fn from_str_system(s: &str) -> Result<Self> {
        Self::from_str(s, None)
    }

    /// Try parsing a string into [`DateTime`] with the given timezone.
    /// Supported format:
    /// - RFC3339 in the naive UTC timezone.
    /// - `%F %T`  with the given timezone
    /// - `%F %T%z`  with the timezone in string
    pub fn from_str(s: &str, timezone: Option<&Timezone>) -> Result<Self> {
        let s = s.trim();
        let timestamp_millis = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            dt.naive_utc().and_utc().timestamp_millis()
        } else if let Ok(d) = NaiveDateTime::parse_from_str(s, DATETIME_FORMAT) {
            match datetime_to_utc(&d, get_timezone(timezone)) {
                LocalResult::None => {
                    return InvalidDateStrSnafu { raw: s }.fail();
                }
                LocalResult::Single(t) | LocalResult::Ambiguous(t, _) => {
                    t.and_utc().timestamp_millis()
                }
            }
        } else if let Ok(v) = chrono::DateTime::parse_from_str(s, DATETIME_FORMAT_WITH_TZ) {
            v.timestamp_millis()
        } else {
            return InvalidDateStrSnafu { raw: s }.fail();
        };

        Ok(Self(timestamp_millis))
    }

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
        chrono::DateTime::from_timestamp_millis(self.0).map(|x| x.naive_utc())
    }

    /// Format DateTime for given format and timezone.
    /// If `tz==None`, the server default timezone will used.
    pub fn as_formatted_string(
        self,
        pattern: &str,
        timezone: Option<&Timezone>,
    ) -> Result<Option<String>> {
        if let Some(v) = self.to_chrono_datetime() {
            let mut formatted = String::new();

            match get_timezone(timezone) {
                Timezone::Offset(offset) => {
                    write!(
                        formatted,
                        "{}",
                        offset.from_utc_datetime(&v).format(pattern)
                    )
                    .context(crate::error::FormatSnafu { pattern })?;
                }
                Timezone::Named(tz) => {
                    write!(formatted, "{}", tz.from_utc_datetime(&v).format(pattern))
                        .context(crate::error::FormatSnafu { pattern })?;
                }
            }

            return Ok(Some(formatted));
        }

        Ok(None)
    }

    pub fn to_chrono_datetime_with_timezone(&self, tz: Option<&Timezone>) -> Option<NaiveDateTime> {
        let datetime = self.to_chrono_datetime();
        datetime.map(|v| match tz {
            Some(Timezone::Offset(offset)) => offset.from_utc_datetime(&v).naive_local(),
            Some(Timezone::Named(tz)) => tz.from_utc_datetime(&v).naive_local(),
            None => Utc.from_utc_datetime(&v).naive_local(),
        })
    }

    // FIXME(yingwen): remove add/sub intervals later
    /// Adds given [IntervalYearMonth] to the current datetime.
    pub fn add_year_month(&self, interval: IntervalYearMonth) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;

        naive_datetime
            .checked_add_months(Months::new(interval.months as u32))
            .map(Into::into)
    }

    /// Adds given [IntervalDayTime] to the current datetime.
    pub fn add_day_time(&self, interval: IntervalDayTime) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;

        naive_datetime
            .checked_add_days(Days::new(interval.days as u64))?
            .checked_add_signed(TimeDelta::milliseconds(interval.milliseconds as i64))
            .map(Into::into)
    }

    /// Adds given [IntervalMonthDayNano] to the current datetime.
    pub fn add_month_day_nano(&self, interval: IntervalMonthDayNano) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;

        naive_datetime
            .checked_add_months(Months::new(interval.months as u32))?
            .checked_add_days(Days::new(interval.days as u64))?
            .checked_add_signed(TimeDelta::nanoseconds(interval.nanoseconds))
            .map(Into::into)
    }

    /// Subtracts given [IntervalYearMonth] to the current datetime.
    pub fn sub_year_month(&self, interval: IntervalYearMonth) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;

        naive_datetime
            .checked_sub_months(Months::new(interval.months as u32))
            .map(Into::into)
    }

    /// Subtracts given [IntervalDayTime] to the current datetime.
    pub fn sub_day_time(&self, interval: IntervalDayTime) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;

        naive_datetime
            .checked_sub_days(Days::new(interval.days as u64))?
            .checked_sub_signed(TimeDelta::milliseconds(interval.milliseconds as i64))
            .map(Into::into)
    }

    /// Subtracts given [IntervalMonthDayNano] to the current datetime.
    pub fn sub_month_day_nano(&self, interval: IntervalMonthDayNano) -> Option<Self> {
        let naive_datetime = self.to_chrono_datetime()?;

        naive_datetime
            .checked_sub_months(Months::new(interval.months as u32))?
            .checked_sub_days(Days::new(interval.days as u64))?
            .checked_sub_signed(TimeDelta::nanoseconds(interval.nanoseconds))
            .map(Into::into)
    }

    /// Convert to [common_time::date].
    pub fn to_date(&self) -> Option<Date> {
        self.to_chrono_datetime().map(|d| Date::from(d.date()))
    }

    pub fn negative(&self) -> Self {
        Self(-self.0)
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
        let dt = DateTime::from_str(time, None).unwrap();
        assert_eq!(time, &dt.to_string());
        let dt = DateTime::from_str("      1970-01-01       00:00:00+0800       ", None).unwrap();
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

        let interval = IntervalDayTime::new(1, 200);

        let new_datetime = datetime.add_day_time(interval).unwrap();
        assert_eq!(new_datetime.val(), 1000 + 3600 * 24 * 1000 + 200);

        assert_eq!(datetime, new_datetime.sub_day_time(interval).unwrap());
    }

    #[test]
    fn test_parse_local_date_time() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        assert_eq!(
            -28800000,
            DateTime::from_str("1970-01-01 00:00:00", None)
                .unwrap()
                .val()
        );
        assert_eq!(
            0,
            DateTime::from_str("1970-01-01 08:00:00", None)
                .unwrap()
                .val()
        );
        assert_eq!(
            42,
            DateTime::from_str("1970-01-01 08:00:00.042", None)
                .unwrap()
                .val()
        );
        assert_eq!(
            42,
            DateTime::from_str("1970-01-01 08:00:00.042424", None)
                .unwrap()
                .val()
        );

        assert_eq!(
            0,
            DateTime::from_str(
                "1970-01-01 08:00:00",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap()
            .val()
        );

        assert_eq!(
            -28800000,
            DateTime::from_str(
                "1970-01-01 00:00:00",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap()
            .val()
        );

        assert_eq!(
            28800000,
            DateTime::from_str(
                "1970-01-01 00:00:00",
                Some(&Timezone::from_tz_string("-8:00").unwrap())
            )
            .unwrap()
            .val()
        );
    }

    #[test]
    fn test_parse_local_date_time_with_tz() {
        let ts = DateTime::from_str("1970-01-01 08:00:00+0000", None)
            .unwrap()
            .val();
        assert_eq!(28800000, ts);
        let ts = DateTime::from_str("1970-01-01 00:00:00.042+0000", None)
            .unwrap()
            .val();
        assert_eq!(42, ts);

        // the string has the time zone info, the argument doesn't change the result
        let ts = DateTime::from_str(
            "1970-01-01 08:00:00+0000",
            Some(&Timezone::from_tz_string("-8:00").unwrap()),
        )
        .unwrap()
        .val();
        assert_eq!(28800000, ts);
    }

    #[test]
    fn test_as_formatted_string() {
        let d: DateTime = DateTime::new(1000);

        assert_eq!(
            "1970-01-01",
            d.as_formatted_string("%Y-%m-%d", None).unwrap().unwrap()
        );
        assert_eq!(
            "1970-01-01 00:00:01",
            d.as_formatted_string("%Y-%m-%d %H:%M:%S", None)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            "1970-01-01T00:00:01:000",
            d.as_formatted_string("%Y-%m-%dT%H:%M:%S:%3f", None)
                .unwrap()
                .unwrap()
        );

        assert_eq!(
            "1970-01-01T08:00:01:000",
            d.as_formatted_string(
                "%Y-%m-%dT%H:%M:%S:%3f",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap()
            .unwrap()
        );
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
