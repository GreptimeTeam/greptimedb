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
use pgwire::types::ToSqlText;
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

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

impl From<Duration> for PgInterval {
    fn from(duration: Duration) -> Self {
        let value = duration.value();
        let unit = duration.unit();

        // Convert the duration to microseconds
        let microseconds =
            match unit {
                TimeUnit::Second => value.checked_mul(1000000).unwrap_or_else(|| {
                    if value > 0 {
                        i64::MAX
                    } else {
                        i64::MIN
                    }
                }),
                TimeUnit::Millisecond => value.checked_mul(1000).unwrap_or_else(|| {
                    if value > 0 {
                        i64::MAX
                    } else {
                        i64::MIN
                    }
                }),
                TimeUnit::Microsecond => value,
                TimeUnit::Nanosecond => value / 1000,
            };

        let days = (microseconds / (24 * 60 * 60 * 1000000)) as i32;
        let remaining_microseconds = microseconds % (24 * 60 * 60 * 1000000);

        Self {
            months: 0,
            days,
            microseconds: remaining_microseconds,
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

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit;
    use common_time::Duration;

    use super::*;

    #[test]
    fn test_duration_to_pg_interval() {
        // Test with seconds
        let duration = Duration::new(86400, TimeUnit::Second); // 1 day
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 0);

        // Test with milliseconds
        let duration = Duration::new(86400000, TimeUnit::Millisecond); // 1 day
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 0);

        // Test with microseconds
        let duration = Duration::new(86400000000, TimeUnit::Microsecond); // 1 day
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 0);

        // Test with nanoseconds
        let duration = Duration::new(86400000000000, TimeUnit::Nanosecond); // 1 day
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.microseconds, 0);

        // Test with partial day
        let duration = Duration::new(43200, TimeUnit::Second); // 12 hours
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 43200000000); // 12 hours in microseconds

        // Test with negative duration
        let duration = Duration::new(-86400, TimeUnit::Second); // -1 day
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, -1);
        assert_eq!(interval.microseconds, 0);

        // Test with multiple days
        let duration = Duration::new(259200, TimeUnit::Second); // 3 days
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 3);
        assert_eq!(interval.microseconds, 0);

        // Test with small duration (less than a day)
        let duration = Duration::new(3600, TimeUnit::Second); // 1 hour
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 3600000000); // 1 hour in microseconds

        // Test with very small duration
        let duration = Duration::new(1, TimeUnit::Microsecond); // 1 microsecond
        let interval = PgInterval::from(duration);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.microseconds, 1);
    }
}
