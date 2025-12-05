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

use bytes::BufMut;
use chrono::{NaiveDate, NaiveDateTime};
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, ToSql, Type};
use session::session_config::{PGDateOrder, PGDateTimeStyle};

#[derive(Debug)]
pub struct StylingDate(pub NaiveDate, pub PGDateTimeStyle, pub PGDateOrder);

#[derive(Debug)]
pub struct StylingDateTime(pub NaiveDateTime, pub PGDateTimeStyle, pub PGDateOrder);

fn date_format_string(style: PGDateTimeStyle, order: PGDateOrder) -> &'static str {
    match style {
        PGDateTimeStyle::ISO => "%Y-%m-%d",
        PGDateTimeStyle::German => "%d.%m.%Y",
        PGDateTimeStyle::Postgres => match order {
            PGDateOrder::MDY | PGDateOrder::YMD => "%m-%d-%Y",
            PGDateOrder::DMY => "%d-%m-%Y",
        },
        PGDateTimeStyle::SQL => match order {
            PGDateOrder::MDY | PGDateOrder::YMD => "%m/%d/%Y",
            PGDateOrder::DMY => "%d/%m/%Y",
        },
    }
}

fn datetime_format_string(style: PGDateTimeStyle, order: PGDateOrder) -> &'static str {
    match style {
        PGDateTimeStyle::ISO => "%Y-%m-%d %H:%M:%S%.6f",
        PGDateTimeStyle::German => "%d.%m.%Y %H:%M:%S%.6f",
        PGDateTimeStyle::Postgres => match order {
            PGDateOrder::MDY | PGDateOrder::YMD => "%a %b %d %H:%M:%S%.6f %Y",
            PGDateOrder::DMY => "%a %d %b %H:%M:%S%.6f %Y",
        },
        PGDateTimeStyle::SQL => match order {
            PGDateOrder::MDY | PGDateOrder::YMD => "%m/%d/%Y %H:%M:%S%.6f",
            PGDateOrder::DMY => "%d/%m/%Y %H:%M:%S%.6f",
        },
    }
}
impl ToSqlText for StylingDate {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
        format_options: &FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match *ty {
            Type::DATE => {
                let fmt = self
                    .0
                    .format(date_format_string(self.1, self.2))
                    .to_string();
                out.put_slice(fmt.as_bytes());
            }
            _ => {
                self.0.to_sql_text(ty, out, format_options)?;
            }
        }
        Ok(IsNull::No)
    }
}

impl ToSqlText for StylingDateTime {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match *ty {
            Type::TIMESTAMP => {
                let fmt = self
                    .0
                    .format(datetime_format_string(self.1, self.2))
                    .to_string();
                out.put_slice(fmt.as_bytes());
            }
            Type::DATE => {
                let fmt = self
                    .0
                    .format(date_format_string(self.1, self.2))
                    .to_string();
                out.put_slice(fmt.as_bytes());
            }
            _ => {
                self.0.to_sql_text(ty, out, format_options)?;
            }
        }
        Ok(IsNull::No)
    }
}

macro_rules! delegate_to_sql {
    ($delegator:ident, $delegatee:ident) => {
        impl ToSql for $delegator {
            fn to_sql(
                &self,
                ty: &Type,
                out: &mut bytes::BytesMut,
            ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.0.to_sql(ty, out)
            }

            fn accepts(ty: &Type) -> bool {
                <$delegatee as ToSql>::accepts(ty)
            }

            fn to_sql_checked(
                &self,
                ty: &Type,
                out: &mut bytes::BytesMut,
            ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.0.to_sql_checked(ty, out)
            }
        }
    };
}

delegate_to_sql!(StylingDate, NaiveDate);
delegate_to_sql!(StylingDateTime, NaiveDateTime);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_styling_date() {
        let naive_date = NaiveDate::from_ymd_opt(1997, 12, 17).unwrap();

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::ISO, PGDateOrder::MDY);
            let expected = "1997-12-17";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::ISO, PGDateOrder::YMD);
            let expected = "1997-12-17";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::ISO, PGDateOrder::DMY);
            let expected = "1997-12-17";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::German, PGDateOrder::MDY);
            let expected = "17.12.1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::German, PGDateOrder::YMD);
            let expected = "17.12.1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::German, PGDateOrder::DMY);
            let expected = "17.12.1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::Postgres, PGDateOrder::MDY);
            let expected = "12-17-1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::Postgres, PGDateOrder::YMD);
            let expected = "12-17-1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::Postgres, PGDateOrder::DMY);
            let expected = "17-12-1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::SQL, PGDateOrder::MDY);
            let expected = "12/17/1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::SQL, PGDateOrder::YMD);
            let expected = "12/17/1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_date = StylingDate(naive_date, PGDateTimeStyle::SQL, PGDateOrder::DMY);
            let expected = "17/12/1997";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_date
                .to_sql_text(&Type::DATE, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }
    }

    #[test]
    fn test_styling_datetime() {
        let input =
            NaiveDateTime::parse_from_str("2021-09-01 12:34:56.789012", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap();

        {
            let styling_datetime = StylingDateTime(input, PGDateTimeStyle::ISO, PGDateOrder::MDY);
            let expected = "2021-09-01 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime = StylingDateTime(input, PGDateTimeStyle::ISO, PGDateOrder::YMD);
            let expected = "2021-09-01 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime = StylingDateTime(input, PGDateTimeStyle::ISO, PGDateOrder::DMY);
            let expected = "2021-09-01 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime =
                StylingDateTime(input, PGDateTimeStyle::German, PGDateOrder::MDY);
            let expected = "01.09.2021 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime =
                StylingDateTime(input, PGDateTimeStyle::German, PGDateOrder::YMD);
            let expected = "01.09.2021 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime =
                StylingDateTime(input, PGDateTimeStyle::German, PGDateOrder::DMY);
            let expected = "01.09.2021 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime =
                StylingDateTime(input, PGDateTimeStyle::Postgres, PGDateOrder::MDY);
            let expected = "Wed Sep 01 12:34:56.789012 2021";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime =
                StylingDateTime(input, PGDateTimeStyle::Postgres, PGDateOrder::YMD);
            let expected = "Wed Sep 01 12:34:56.789012 2021";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime =
                StylingDateTime(input, PGDateTimeStyle::Postgres, PGDateOrder::DMY);
            let expected = "Wed 01 Sep 12:34:56.789012 2021";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime = StylingDateTime(input, PGDateTimeStyle::SQL, PGDateOrder::MDY);
            let expected = "09/01/2021 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime = StylingDateTime(input, PGDateTimeStyle::SQL, PGDateOrder::YMD);
            let expected = "09/01/2021 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }

        {
            let styling_datetime = StylingDateTime(input, PGDateTimeStyle::SQL, PGDateOrder::DMY);
            let expected = "01/09/2021 12:34:56.789012";
            let mut out = bytes::BytesMut::new();
            let is_null = styling_datetime
                .to_sql_text(&Type::TIMESTAMP, &mut out, &FormatOptions::default())
                .unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(out, expected.as_bytes());
        }
    }
}
