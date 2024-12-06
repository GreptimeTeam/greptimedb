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

use chrono::{Datelike, Days, LocalResult, Months, NaiveDate, NaiveTime, TimeDelta, TimeZone};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{InvalidDateStrSnafu, ParseDateStrSnafu, Result};
use crate::interval::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use crate::timezone::get_timezone;
use crate::util::datetime_to_utc;
use crate::Timezone;

const UNIX_EPOCH_FROM_CE: i32 = 719_163;

/// ISO 8601 [Date] values. The inner representation is a signed 32 bit integer that represents the
/// **days since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct Date(i32);

impl From<Date> for Value {
    fn from(d: Date) -> Self {
        Value::String(d.to_string())
    }
}

impl From<i32> for Date {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<NaiveDate> for Date {
    fn from(date: NaiveDate) -> Self {
        Self(date.num_days_from_ce() - UNIX_EPOCH_FROM_CE)
    }
}

impl Display for Date {
    /// [Date] is formatted according to ISO-8601 standard.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(abs_date) = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_FROM_CE + self.0) {
            write!(f, "{}", abs_date.format("%F"))
        } else {
            write!(f, "Date({})", self.0)
        }
    }
}

impl Date {
    /// Try parsing a string into [`Date`] with UTC timezone.
    pub fn from_str_utc(s: &str) -> Result<Self> {
        Self::from_str(s, None)
    }

    /// Try parsing a string into [`Date`] with given timezone.
    pub fn from_str(s: &str, timezone: Option<&Timezone>) -> Result<Self> {
        let s = s.trim();
        let date = NaiveDate::parse_from_str(s, "%F").context(ParseDateStrSnafu { raw: s })?;
        let Some(timezone) = timezone else {
            return Ok(Self(date.num_days_from_ce() - UNIX_EPOCH_FROM_CE));
        };

        let datetime = date.and_time(NaiveTime::default());
        match datetime_to_utc(&datetime, timezone) {
            LocalResult::None => InvalidDateStrSnafu { raw: s }.fail(),
            LocalResult::Single(utc) | LocalResult::Ambiguous(utc, _) => Ok(Date::from(utc.date())),
        }
    }

    pub fn new(val: i32) -> Self {
        Self(val)
    }

    pub fn val(&self) -> i32 {
        self.0
    }

    pub fn to_chrono_date(&self) -> Option<NaiveDate> {
        NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_FROM_CE + self.0)
    }

    /// Format Date for given format and timezone.
    /// If `tz==None`, the server default timezone will used.
    pub fn as_formatted_string(
        self,
        pattern: &str,
        timezone: Option<&Timezone>,
    ) -> Result<Option<String>> {
        if let Some(v) = self.to_chrono_date() {
            // Safety: always success
            let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 0).unwrap();
            let v = v.and_time(time);
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

    pub fn to_secs(&self) -> i64 {
        (self.0 as i64) * 24 * 3600
    }

    // FIXME(yingwen): remove add/sub intervals later
    /// Adds given [IntervalYearMonth] to the current date.
    pub fn add_year_month(&self, interval: IntervalYearMonth) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        naive_date
            .checked_add_months(Months::new(interval as u32))
            .map(Into::into)
    }

    /// Adds given [IntervalDayTime] to the current date.
    pub fn add_day_time(&self, interval: IntervalDayTime) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        naive_date
            .checked_add_days(Days::new(interval.days as u64))?
            .checked_add_signed(TimeDelta::milliseconds(interval.milliseconds as i64))
            .map(Into::into)
    }

    /// Adds given [IntervalMonthDayNano] to the current date.
    pub fn add_month_day_nano(&self, interval: IntervalMonthDayNano) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        naive_date
            .checked_add_months(Months::new(interval.months as u32))?
            .checked_add_days(Days::new(interval.days as u64))?
            .checked_add_signed(TimeDelta::nanoseconds(interval.nanoseconds))
            .map(Into::into)
    }

    /// Subtracts given [IntervalYearMonth] to the current date.
    pub fn sub_year_month(&self, interval: IntervalYearMonth) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        naive_date
            .checked_sub_months(Months::new(interval as u32))
            .map(Into::into)
    }

    /// Subtracts given [IntervalDayTime] to the current date.
    pub fn sub_day_time(&self, interval: IntervalDayTime) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        naive_date
            .checked_sub_days(Days::new(interval.days as u64))?
            .checked_sub_signed(TimeDelta::milliseconds(interval.milliseconds as i64))
            .map(Into::into)
    }

    /// Subtracts given [IntervalMonthDayNano] to the current date.
    pub fn sub_month_day_nano(&self, interval: IntervalMonthDayNano) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        naive_date
            .checked_sub_months(Months::new(interval.months as u32))?
            .checked_sub_days(Days::new(interval.days as u64))?
            .checked_sub_signed(TimeDelta::nanoseconds(interval.nanoseconds))
            .map(Into::into)
    }

    pub fn negative(&self) -> Self {
        Self(-self.0)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    pub fn test_print_date2() {
        assert_eq!("1969-12-31", Date::new(-1).to_string());
        assert_eq!("1970-01-01", Date::new(0).to_string());
        assert_eq!("1970-02-12", Date::new(42).to_string());
    }

    #[test]
    pub fn test_date_parse() {
        assert_eq!(
            "1970-01-01",
            Date::from_str("1970-01-01", None).unwrap().to_string()
        );

        assert_eq!(
            "1969-01-01",
            Date::from_str("1969-01-01", None).unwrap().to_string()
        );

        assert_eq!(
            "1969-01-01",
            Date::from_str("     1969-01-01       ", None)
                .unwrap()
                .to_string()
        );

        let now = Utc::now().date_naive().format("%F").to_string();
        assert_eq!(now, Date::from_str(&now, None).unwrap().to_string());

        // with timezone
        assert_eq!(
            "1969-12-31",
            Date::from_str(
                "1970-01-01",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap()
            .to_string()
        );

        assert_eq!(
            "1969-12-31",
            Date::from_str(
                "1970-01-01",
                Some(&Timezone::from_tz_string("+16:00").unwrap())
            )
            .unwrap()
            .to_string()
        );

        assert_eq!(
            "1970-01-01",
            Date::from_str(
                "1970-01-01",
                Some(&Timezone::from_tz_string("-8:00").unwrap())
            )
            .unwrap()
            .to_string()
        );

        assert_eq!(
            "1970-01-01",
            Date::from_str(
                "1970-01-01",
                Some(&Timezone::from_tz_string("-16:00").unwrap())
            )
            .unwrap()
            .to_string()
        );
    }

    #[test]
    fn test_add_sub_interval() {
        let date = Date::new(1000);

        let interval = 3;

        let new_date = date.add_year_month(interval).unwrap();
        assert_eq!(new_date.val(), 1091);

        assert_eq!(date, new_date.sub_year_month(interval).unwrap());
    }

    #[test]
    pub fn test_min_max() {
        let mut date = Date::from_str("9999-12-31", None).unwrap();
        date.0 += 1000;
        assert_eq!(date, Date::from_str(&date.to_string(), None).unwrap());
    }

    #[test]
    fn test_as_formatted_string() {
        let d: Date = 42.into();

        assert_eq!(
            "1970-02-12",
            d.as_formatted_string("%Y-%m-%d", None).unwrap().unwrap()
        );
        assert_eq!(
            "1970-02-12 00:00:00",
            d.as_formatted_string("%Y-%m-%d %H:%M:%S", None)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            "1970-02-12T00:00:00:000",
            d.as_formatted_string("%Y-%m-%dT%H:%M:%S:%3f", None)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            "1970-02-12T08:00:00:000",
            d.as_formatted_string(
                "%Y-%m-%dT%H:%M:%S:%3f",
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap()
            .unwrap()
        );
    }

    #[test]
    pub fn test_from() {
        let d: Date = 42.into();
        assert_eq!(42, d.val());
    }

    #[test]
    fn test_to_secs() {
        let d = Date::from_str("1970-01-01", None).unwrap();
        assert_eq!(d.to_secs(), 0);
        let d = Date::from_str("1970-01-02", None).unwrap();
        assert_eq!(d.to_secs(), 24 * 3600);
        let d = Date::from_str("1970-01-03", None).unwrap();
        assert_eq!(d.to_secs(), 2 * 24 * 3600);
    }
}
