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

use std::fmt::Display;
use std::str::FromStr;

use chrono::{FixedOffset, Local, Offset};
use chrono_tz::Tz;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    InvalidTimeZoneOffsetSnafu, ParseOffsetStrSnafu, ParseTimeZoneNameSnafu, Result,
};
use crate::util::find_tz_from_env;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeZone {
    Offset(FixedOffset),
    Named(Tz),
}

impl TimeZone {
    /// Compute timezone from given offset hours and minutes
    /// Return `None` if given offset exceeds scope
    pub fn hours_mins_opt(offset_hours: i32, offset_mins: u32) -> Result<Self> {
        let offset_secs = if offset_hours > 0 {
            offset_hours * 3600 + offset_mins as i32 * 60
        } else {
            offset_hours * 3600 - offset_mins as i32 * 60
        };

        FixedOffset::east_opt(offset_secs)
            .map(Self::Offset)
            .context(InvalidTimeZoneOffsetSnafu {
                hours: offset_hours,
                minutes: offset_mins,
            })
    }

    /// Parse timezone offset string and return None if given offset exceeds
    /// scope.
    ///
    /// String examples are available as described in
    /// <https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html>
    ///
    /// - `SYSTEM`
    /// - Offset to UTC: `+08:00` , `-11:30`
    /// - Named zones: `Asia/Shanghai`, `Europe/Berlin`
    pub fn from_tz_string(tz_string: &str) -> Result<Option<Self>> {
        // Use system timezone
        if tz_string.eq_ignore_ascii_case("SYSTEM") {
            Ok(None)
        } else if let Some((hrs, mins)) = tz_string.split_once(':') {
            let hrs = hrs
                .parse::<i32>()
                .context(ParseOffsetStrSnafu { raw: tz_string })?;
            let mins = mins
                .parse::<u32>()
                .context(ParseOffsetStrSnafu { raw: tz_string })?;
            Self::hours_mins_opt(hrs, mins).map(Some)
        } else if let Ok(tz) = Tz::from_str(tz_string) {
            Ok(Some(Self::Named(tz)))
        } else {
            ParseTimeZoneNameSnafu { raw: tz_string }.fail()
        }
    }
}

impl Display for TimeZone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(tz) => write!(f, "{}", tz.name()),
            Self::Offset(offset) => write!(f, "{}", offset),
        }
    }
}

#[inline]
pub fn system_time_zone_name() -> String {
    if let Some(tz) = find_tz_from_env() {
        Local::now().with_timezone(&tz).offset().fix().to_string()
    } else {
        Local::now().offset().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_tz_string() {
        assert_eq!(None, TimeZone::from_tz_string("SYSTEM").unwrap());

        let utc_plus_8 = Some(TimeZone::Offset(FixedOffset::east_opt(3600 * 8).unwrap()));
        assert_eq!(utc_plus_8, TimeZone::from_tz_string("+8:00").unwrap());
        assert_eq!(utc_plus_8, TimeZone::from_tz_string("+08:00").unwrap());
        assert_eq!(utc_plus_8, TimeZone::from_tz_string("08:00").unwrap());

        let utc_minus_8 = Some(TimeZone::Offset(FixedOffset::west_opt(3600 * 8).unwrap()));
        assert_eq!(utc_minus_8, TimeZone::from_tz_string("-08:00").unwrap());
        assert_eq!(utc_minus_8, TimeZone::from_tz_string("-8:00").unwrap());

        let utc_minus_8_5 = Some(TimeZone::Offset(
            FixedOffset::west_opt(3600 * 8 + 60 * 30).unwrap(),
        ));
        assert_eq!(utc_minus_8_5, TimeZone::from_tz_string("-8:30").unwrap());

        let utc_plus_max = Some(TimeZone::Offset(FixedOffset::east_opt(3600 * 14).unwrap()));
        assert_eq!(utc_plus_max, TimeZone::from_tz_string("14:00").unwrap());

        let utc_minus_max = Some(TimeZone::Offset(
            FixedOffset::west_opt(3600 * 13 + 60 * 59).unwrap(),
        ));
        assert_eq!(utc_minus_max, TimeZone::from_tz_string("-13:59").unwrap());

        assert_eq!(
            Some(TimeZone::Named(Tz::Asia__Shanghai)),
            TimeZone::from_tz_string("Asia/Shanghai").unwrap()
        );
        assert_eq!(
            Some(TimeZone::Named(Tz::UTC)),
            TimeZone::from_tz_string("UTC").unwrap()
        );

        assert!(TimeZone::from_tz_string("WORLD_PEACE").is_err());
        assert!(TimeZone::from_tz_string("A0:01").is_err());
        assert!(TimeZone::from_tz_string("20:0A").is_err());
        assert!(TimeZone::from_tz_string(":::::").is_err());
        assert!(TimeZone::from_tz_string("Asia/London").is_err());
        assert!(TimeZone::from_tz_string("Unknown").is_err());
    }

    #[test]
    fn test_timezone_to_string() {
        assert_eq!("UTC", TimeZone::Named(Tz::UTC).to_string());
        assert_eq!(
            "+01:00",
            TimeZone::from_tz_string("01:00")
                .unwrap()
                .unwrap()
                .to_string()
        );
        assert_eq!(
            "Asia/Shanghai",
            TimeZone::from_tz_string("Asia/Shanghai")
                .unwrap()
                .unwrap()
                .to_string()
        );
    }
}
