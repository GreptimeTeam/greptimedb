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

use bytes::{Buf, BufMut};
use common_time::interval::IntervalFormat;
use common_time::timestamp::TimeUnit;
use common_time::{Duration, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use pgwire::types::format::FormatOptions;
use pgwire::types::{FromSqlText, ToSqlText};
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

use crate::error;

/// On average one month has 30.44 day, which is a common approximation.
const SECONDS_PER_MONTH: i64 = 24 * 6 * 6 * 3044;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
const MILLISECONDS_PER_MONTH: i64 = SECONDS_PER_MONTH * 1000;
const MILLISECONDS_PER_DAY: i64 = SECONDS_PER_DAY * 1000;

#[derive(Debug, Clone, Copy, Default)]
pub struct PgInterval {
    pub(crate) months: i32,
    pub(crate) days: i32,
    pub(crate) microseconds: i64,
}

impl From<IntervalYearMonth> for PgInterval {
    fn from(interval: IntervalYearMonth) -> Self {
        Self {
            months: interval.months,
            days: 0,
            microseconds: 0,
        }
    }
}

impl From<IntervalDayTime> for PgInterval {
    fn from(interval: IntervalDayTime) -> Self {
        Self {
            months: 0,
            days: interval.days,
            microseconds: interval.milliseconds as i64 * 1000,
        }
    }
}

impl From<IntervalMonthDayNano> for PgInterval {
    fn from(interval: IntervalMonthDayNano) -> Self {
        Self {
            months: interval.months,
            days: interval.days,
            microseconds: interval.nanoseconds / 1000,
        }
    }
}

impl TryFrom<Duration> for PgInterval {
    type Error = error::Error;

    fn try_from(duration: Duration) -> error::Result<Self> {
        let value = duration.value();
        let unit = duration.unit();

        // Convert the duration to microseconds
        match unit {
            TimeUnit::Second => {
                let months = i32::try_from(value / SECONDS_PER_MONTH)
                    .map_err(|_| error::DurationOverflowSnafu { val: duration }.build())?;
                let days =
                    i32::try_from((value - (months as i64) * SECONDS_PER_MONTH) / SECONDS_PER_DAY)
                        .map_err(|_| error::DurationOverflowSnafu { val: duration }.build())?;
                let microseconds =
                    (value - (months as i64) * SECONDS_PER_MONTH - (days as i64) * SECONDS_PER_DAY)
                        .checked_mul(1_000_000)
                        .ok_or(error::DurationOverflowSnafu { val: duration }.build())?;

                Ok(Self {
                    months,
                    days,
                    microseconds,
                })
            }
            TimeUnit::Millisecond => {
                let months = i32::try_from(value / MILLISECONDS_PER_MONTH)
                    .map_err(|_| error::DurationOverflowSnafu { val: duration }.build())?;
                let days = i32::try_from(
                    (value - (months as i64) * MILLISECONDS_PER_MONTH) / MILLISECONDS_PER_DAY,
                )
                .map_err(|_| error::DurationOverflowSnafu { val: duration }.build())?;
                let microseconds = ((value - (months as i64) * MILLISECONDS_PER_MONTH)
                    - (days as i64) * MILLISECONDS_PER_DAY)
                    * 1_000;
                Ok(Self {
                    months,
                    days,
                    microseconds,
                })
            }
            TimeUnit::Microsecond => Ok(Self {
                months: 0,
                days: 0,
                microseconds: value,
            }),
            TimeUnit::Nanosecond => Ok(Self {
                months: 0,
                days: 0,
                microseconds: value / 1000,
            }),
        }
    }
}

impl From<PgInterval> for IntervalMonthDayNano {
    fn from(interval: PgInterval) -> Self {
        IntervalMonthDayNano::new(
            interval.months,
            interval.days,
            // Maybe overflow, but most scenarios ok.
            interval.microseconds.checked_mul(1000).unwrap_or_else(|| {
                if interval.microseconds.is_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }
            }),
        )
    }
}

impl Display for PgInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            IntervalFormat::from(IntervalMonthDayNano::from(*self)).to_postgres_string()
        )
    }
}

impl ToSql for PgInterval {
    to_sql_checked!();

    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<postgres_types::IsNull, Box<dyn snafu::Error + Sync + Send>>
    where
        Self: Sized,
    {
        // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c#L989-L991
        out.put_i64(self.microseconds);
        out.put_i32(self.days);
        out.put_i32(self.months);
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        matches!(ty, &Type::INTERVAL)
    }
}

impl<'a> FromSql<'a> for PgInterval {
    fn from_sql(
        _: &Type,
        mut raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn snafu::Error + Sync + Send>> {
        // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c#L1007-L1010
        let microseconds = raw.get_i64();
        let days = raw.get_i32();
        let months = raw.get_i32();
        Ok(PgInterval {
            months,
            days,
            microseconds,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::INTERVAL)
    }
}

impl ToSqlText for PgInterval {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &FormatOptions,
    ) -> std::result::Result<postgres_types::IsNull, Box<dyn snafu::Error + Sync + Send>>
    where
        Self: Sized,
    {
        let fmt = match ty {
            &Type::INTERVAL => self.to_string(),
            _ => return Err("unsupported type".into()),
        };

        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl<'a> FromSqlText<'a> for PgInterval {
    fn from_sql_text(
        _ty: &Type,
        input: &[u8],
        _format_options: &FormatOptions,
    ) -> std::result::Result<Self, Box<dyn snafu::Error + Sync + Send>>
    where
        Self: Sized,
    {
        // only support parsing interval from postgres format
        if let Ok(interval) = pg_interval::Interval::from_postgres(str::from_utf8(input)?) {
            Ok(PgInterval {
                months: interval.months,
                days: interval.days,
                microseconds: interval.microseconds,
            })
        } else {
            Err("invalid interval format".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use common_time::Duration;
    use common_time::timestamp::TimeUnit;

    use super::*;

    #[test]
    fn test_duration_to_pg_interval() {
        // Test with seconds
        let duration = Duration::new(86400, TimeUnit::Second); // 1 day
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 0);

        // Test with milliseconds
        let duration = Duration::new(86400000, TimeUnit::Millisecond); // 1 day
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 0);

        // Test with microseconds
        let duration = Duration::new(86400000000, TimeUnit::Microsecond); // 1 day
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 86400000000);

        // Test with nanoseconds
        let duration = Duration::new(86400000000000, TimeUnit::Nanosecond); // 1 day
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 86400000000);

        // Test with partial day
        let duration = Duration::new(43200, TimeUnit::Second); // 12 hours
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 43_200_000_000); // 12 hours in microseconds

        // Test with negative duration
        let duration = Duration::new(-86400, TimeUnit::Second); // -1 day
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, -1);
        assert_eq!(interval.microseconds, 0);

        // Test with multiple days
        let duration = Duration::new(259200, TimeUnit::Second); // 3 days
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 3);
        assert_eq!(interval.microseconds, 0);

        // Test with small duration (less than a day)
        let duration = Duration::new(3600, TimeUnit::Second); // 1 hour
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 3600000000); // 1 hour in microseconds

        // Test with very small duration
        let duration = Duration::new(1, TimeUnit::Microsecond); // 1 microsecond
        let interval = PgInterval::try_from(duration).unwrap();
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 1);

        let duration = Duration::new(i64::MAX, TimeUnit::Second);
        assert!(PgInterval::try_from(duration).is_err());

        let duration = Duration::new(i64::MAX, TimeUnit::Millisecond);
        assert!(PgInterval::try_from(duration).is_err());
    }
}
