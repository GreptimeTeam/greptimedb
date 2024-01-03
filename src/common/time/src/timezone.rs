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

use chrono::FixedOffset;
use chrono_tz::Tz;
use once_cell::sync::OnceCell;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    InvalidTimezoneOffsetSnafu, ParseOffsetStrSnafu, ParseTimezoneNameSnafu, Result,
};
use crate::util::find_tz_from_env;

/// System timezone in `frontend`/`standalone`,
/// config by option `default_timezone` in toml,
/// default value is `UTC` when `default_timezone` is not set.
static DEFAULT_TIMEZONE: OnceCell<Timezone> = OnceCell::new();

// Set the System timezone by `tz_str`
pub fn set_default_timezone(tz_str: Option<&str>) -> Result<()> {
    let tz = match tz_str {
        None | Some("") => Timezone::Named(Tz::UTC),
        Some(tz) => Timezone::from_tz_string(tz)?,
    };
    DEFAULT_TIMEZONE.get_or_init(|| tz);
    Ok(())
}

#[inline(always)]
/// If the `tz=Some(timezone)`, return `timezone` directly,
/// or return current system timezone.
pub fn get_timezone(tz: Option<Timezone>) -> Timezone {
    tz.unwrap_or_else(|| {
        DEFAULT_TIMEZONE
            .get()
            .cloned()
            .unwrap_or(Timezone::Named(Tz::UTC))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Timezone {
    Offset(FixedOffset),
    Named(Tz),
}

impl Timezone {
    /// Compute timezone from given offset hours and minutes
    /// Return `Err` if given offset exceeds scope
    pub fn hours_mins_opt(offset_hours: i32, offset_mins: u32) -> Result<Self> {
        let offset_secs = if offset_hours > 0 {
            offset_hours * 3600 + offset_mins as i32 * 60
        } else {
            offset_hours * 3600 - offset_mins as i32 * 60
        };

        FixedOffset::east_opt(offset_secs)
            .map(Self::Offset)
            .context(InvalidTimezoneOffsetSnafu {
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
    pub fn from_tz_string(tz_string: &str) -> Result<Self> {
        // Use system timezone
        if tz_string.eq_ignore_ascii_case("SYSTEM") {
            Ok(Timezone::Named(find_tz_from_env().unwrap_or(Tz::UTC)))
        } else if let Some((hrs, mins)) = tz_string.split_once(':') {
            let hrs = hrs
                .parse::<i32>()
                .context(ParseOffsetStrSnafu { raw: tz_string })?;
            let mins = mins
                .parse::<u32>()
                .context(ParseOffsetStrSnafu { raw: tz_string })?;
            Self::hours_mins_opt(hrs, mins)
        } else if let Ok(tz) = Tz::from_str(tz_string) {
            Ok(Self::Named(tz))
        } else {
            ParseTimezoneNameSnafu { raw: tz_string }.fail()
        }
    }
}

impl Display for Timezone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(tz) => write!(f, "{}", tz.name()),
            Self::Offset(offset) => write!(f, "{}", offset),
        }
    }
}

#[inline]
/// Return current system config timezone, default config is UTC
pub fn system_timezone_name() -> String {
    format!("{}", get_timezone(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_tz_string() {
        assert_eq!(
            Timezone::Named(Tz::UTC),
            Timezone::from_tz_string("SYSTEM").unwrap()
        );

        let utc_plus_8 = Timezone::Offset(FixedOffset::east_opt(3600 * 8).unwrap());
        assert_eq!(utc_plus_8, Timezone::from_tz_string("+8:00").unwrap());
        assert_eq!(utc_plus_8, Timezone::from_tz_string("+08:00").unwrap());
        assert_eq!(utc_plus_8, Timezone::from_tz_string("08:00").unwrap());

        let utc_minus_8 = Timezone::Offset(FixedOffset::west_opt(3600 * 8).unwrap());
        assert_eq!(utc_minus_8, Timezone::from_tz_string("-08:00").unwrap());
        assert_eq!(utc_minus_8, Timezone::from_tz_string("-8:00").unwrap());

        let utc_minus_8_5 = Timezone::Offset(FixedOffset::west_opt(3600 * 8 + 60 * 30).unwrap());
        assert_eq!(utc_minus_8_5, Timezone::from_tz_string("-8:30").unwrap());

        let utc_plus_max = Timezone::Offset(FixedOffset::east_opt(3600 * 14).unwrap());
        assert_eq!(utc_plus_max, Timezone::from_tz_string("14:00").unwrap());

        let utc_minus_max = Timezone::Offset(FixedOffset::west_opt(3600 * 13 + 60 * 59).unwrap());
        assert_eq!(utc_minus_max, Timezone::from_tz_string("-13:59").unwrap());

        assert_eq!(
            Timezone::Named(Tz::Asia__Shanghai),
            Timezone::from_tz_string("Asia/Shanghai").unwrap()
        );
        assert_eq!(
            Timezone::Named(Tz::UTC),
            Timezone::from_tz_string("UTC").unwrap()
        );

        assert!(Timezone::from_tz_string("WORLD_PEACE").is_err());
        assert!(Timezone::from_tz_string("A0:01").is_err());
        assert!(Timezone::from_tz_string("20:0A").is_err());
        assert!(Timezone::from_tz_string(":::::").is_err());
        assert!(Timezone::from_tz_string("Asia/London").is_err());
        assert!(Timezone::from_tz_string("Unknown").is_err());
    }

    #[test]
    fn test_timezone_to_string() {
        assert_eq!("UTC", Timezone::Named(Tz::UTC).to_string());
        assert_eq!(
            "+01:00",
            Timezone::from_tz_string("01:00").unwrap().to_string()
        );
        assert_eq!(
            "Asia/Shanghai",
            Timezone::from_tz_string("Asia/Shanghai")
                .unwrap()
                .to_string()
        );
    }
}
