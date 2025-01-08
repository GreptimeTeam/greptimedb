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

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use table::table_name::TableName;

use crate::error::{
    EndBeforeStartSnafu, InvalidDateFormatSnafu, InvalidSpanFormatSnafu, InvalidTimeFilterSnafu,
    Result,
};

/// GreptimeDB's log query request.
#[derive(Debug, Serialize, Deserialize)]
pub struct LogQuery {
    // Global query parameters
    /// A fully qualified table name to query logs from.
    pub table: TableName,
    /// Specifies the time range for the log query. See [`TimeFilter`] for more details.
    pub time_filter: TimeFilter,
    /// Controls row skipping and fetch on the result set.
    pub limit: Limit,
    /// Columns to return in the result set.
    ///
    /// The columns can be either from the original log or derived from processing exprs.
    /// Default (empty) means all columns.
    ///
    /// TODO: Do we need negative select?
    pub columns: Vec<String>,

    // Filters
    /// Conjunction of filters to apply for the raw logs.
    ///
    /// Filters here can only refer to the columns from the original log.
    pub filters: Vec<ColumnFilters>,
    /// Adjacent lines to return. Applies to all filters above.
    ///
    /// TODO: Do we need per-filter context?
    pub context: Context,

    // Processors
    /// Expressions to calculate after filter.
    pub exprs: Vec<LogQueryExpr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LogQueryExpr {
    NamedIdent(String),
    PositionalIdent(usize),
    Literal(String),
    ScalarFunc {
        name: String,
        args: Vec<LogQueryExpr>,
    },
    AggrFunc {
        name: String,
        args: Vec<LogQueryExpr>,
        /// Optional range function parameter. Stands for the time range for both step and align.
        range: Option<String>,
        by: Vec<LogQueryExpr>,
    },
    Decompose {
        expr: Box<LogQueryExpr>,
        /// JSON, CSV, etc.
        schema: String,
        /// Fields with type name to extract from the decomposed value.
        fields: Vec<(String, String)>,
    },
    BinaryOp {
        left: Box<LogQueryExpr>,
        op: String,
        right: Box<LogQueryExpr>,
    },
    Alias {
        expr: Box<LogQueryExpr>,
        alias: String,
    },
    Filter {
        expr: Box<LogQueryExpr>,
        filter: ContentFilter,
    },
}

impl Default for LogQuery {
    fn default() -> Self {
        Self {
            table: TableName::new("", "", ""),
            time_filter: Default::default(),
            filters: vec![],
            limit: Limit::default(),
            context: Default::default(),
            columns: vec![],
            exprs: vec![],
        }
    }
}

/// Represents a time range for log query.
///
/// This struct allows various formats to express a time range from the user side
/// for best flexibility:
/// - Only `start` is provided: the `start` string can be any valid "date" or vaguer
///     content. For example: "2024-12-01", "2024-12", "2024", etc. It will be treated
///     as an time range corresponding to the provided date. E.g., "2024-12-01" refers
///     to the entire 24 hours in that day. In this case, the `start` field cannot be a
///     timestamp (like "2024-12-01T12:00:00Z").
/// - Both `start` and `end` are provided: the `start` and `end` strings can be either
///     a date or a timestamp. The `end` field is exclusive (`[start, end)`). When
///     `start` is a date it implies the start of the day, and when `end` is a date it
///     implies the end of the day.
/// - `span` with `start` OR `end`: the `span` string can be any valid "interval"
///     For example: "1024s", "1 week", "1 month", etc. The `span` field is applied to
///     the `start` or `end` field to calculate the other one correspondingly. If `start`
///     is provided, `end` is calculated as `start + span` and vice versa.
/// - Only `span` is provided: the `span` string can be any valid "interval" as mentioned
///     above. In this case, the current time (on the server side) is considered as the `end`.
/// - All fields are provided: in this case, the `start` and `end` fields are considered
///     with higher priority, and the `span` field is ignored.
///
/// This struct doesn't require a timezone to be presented. When the timezone is not
/// provided, it will fill the default timezone with the same rules akin to other queries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeFilter {
    pub start: Option<String>,
    pub end: Option<String>,
    pub span: Option<String>,
}

impl TimeFilter {
    /// Validate and canonicalize the time filter.
    ///
    /// This function will try to fill the missing fields and convert all dates to timestamps
    #[allow(unused_assignments)] // false positive
    pub fn canonicalize(&mut self) -> Result<()> {
        let mut start_dt = None;
        let mut end_dt = None;

        if self.start.is_some() && self.end.is_none() && self.span.is_none() {
            // Only 'start' is provided
            let s = self.start.as_ref().unwrap();
            let (start, end_opt) = Self::parse_datetime(s)?;
            if end_opt.is_none() {
                return Err(InvalidTimeFilterSnafu {
                    filter: self.clone(),
                }
                .build());
            }
            start_dt = Some(start);
            end_dt = end_opt;
        } else if self.start.is_some() && self.end.is_some() {
            // Both 'start' and 'end' are provided
            let (start, _) = Self::parse_datetime(self.start.as_ref().unwrap())?;
            let (end, _) = Self::parse_datetime(self.end.as_ref().unwrap())?;
            start_dt = Some(start);
            end_dt = Some(end);
        } else if self.span.is_some() && (self.start.is_some() || self.end.is_some()) {
            // 'span' with 'start' or 'end'
            let span = Self::parse_span(self.span.as_ref().unwrap())?;
            if self.start.is_some() {
                let (start, _) = Self::parse_datetime(self.start.as_ref().unwrap())?;
                let end = start + span;
                start_dt = Some(start);
                end_dt = Some(end);
            } else {
                let (end, _) = Self::parse_datetime(self.end.as_ref().unwrap())?;
                let start = end - span;
                start_dt = Some(start);
                end_dt = Some(end);
            }
        } else if self.span.is_some() && self.start.is_none() && self.end.is_none() {
            // Only 'span' is provided
            let span = Self::parse_span(self.span.as_ref().unwrap())?;
            let end = Utc::now();
            let start = end - span;
            start_dt = Some(start);
            end_dt = Some(end);
        } else if self.start.is_some() && self.span.is_some() && self.end.is_some() {
            // All fields are provided; 'start' and 'end' take priority
            let (start, _) = Self::parse_datetime(self.start.as_ref().unwrap())?;
            let (end, _) = Self::parse_datetime(self.end.as_ref().unwrap())?;
            start_dt = Some(start);
            end_dt = Some(end);
        } else {
            // Exception
            return Err(InvalidTimeFilterSnafu {
                filter: self.clone(),
            }
            .build());
        }

        // Validate that end is after start
        if let (Some(start), Some(end)) = (&start_dt, &end_dt) {
            if end <= start {
                return Err(EndBeforeStartSnafu {
                    start: start.to_rfc3339(),
                    end: end.to_rfc3339(),
                }
                .build());
            }
        }

        // Update the fields with canonicalized timestamps
        if let Some(start) = start_dt {
            self.start = Some(start.to_rfc3339());
        }

        if let Some(end) = end_dt {
            self.end = Some(end.to_rfc3339());
        }

        Ok(())
    }

    /// Util function returns a start and optional end DateTime
    fn parse_datetime(s: &str) -> Result<(DateTime<Utc>, Option<DateTime<Utc>>)> {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            Ok((dt.with_timezone(&Utc), None))
        } else {
            let formats = ["%Y-%m-%d", "%Y-%m", "%Y"];
            for format in &formats {
                if let Ok(naive_date) = NaiveDate::parse_from_str(s, format) {
                    let start = Utc.from_utc_datetime(
                        &naive_date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
                    );
                    let end = match *format {
                        "%Y-%m-%d" => start + Duration::days(1),
                        "%Y-%m" => {
                            let next_month = if naive_date.month() == 12 {
                                NaiveDate::from_ymd_opt(naive_date.year() + 1, 1, 1).unwrap()
                            } else {
                                NaiveDate::from_ymd_opt(
                                    naive_date.year(),
                                    naive_date.month() + 1,
                                    1,
                                )
                                .unwrap()
                            };
                            Utc.from_utc_datetime(&next_month.and_hms_opt(0, 0, 0).unwrap())
                        }
                        "%Y" => {
                            let next_year =
                                NaiveDate::from_ymd_opt(naive_date.year() + 1, 1, 1).unwrap();
                            Utc.from_utc_datetime(&next_year.and_hms_opt(0, 0, 0).unwrap())
                        }
                        _ => unreachable!(),
                    };
                    return Ok((start, Some(end)));
                }
            }
            Err(InvalidDateFormatSnafu {
                input: s.to_string(),
            }
            .build())
        }
    }

    /// Util function handles durations like "1 week", "1 month", etc (unimplemented).
    fn parse_span(s: &str) -> Result<Duration> {
        // Simplified parsing logic
        if let Ok(seconds) = s.parse::<i64>() {
            Ok(Duration::seconds(seconds))
        } else {
            Err(InvalidSpanFormatSnafu {
                input: s.to_string(),
            }
            .build())
        }
    }
}

/// Represents a column with filters to query.
#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnFilters {
    /// Case-sensitive column name to query.
    pub column_name: String,
    /// Filters to apply to the column. Can be empty.
    pub filters: Vec<ContentFilter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ContentFilter {
    // Search-based filters
    /// Only match the exact content.
    ///
    /// For example, if the content is "pale blue dot", the filter "pale" or "pale blue" will match.
    Exact(String),
    /// Match the content with a prefix.
    ///
    /// For example, if the content is "error message", the filter "err" or "error mess" will match.
    Prefix(String),
    /// Match the content with a postfix. Similar to `Prefix`.
    Postfix(String),
    /// Match the content with a substring.
    Contains(String),
    /// Match the content with a regex pattern. The pattern should be a valid Rust regex.
    Regex(String),

    // Value-based filters
    /// Content exists, a.k.a. not null.
    Exist,
    Between(String, String),
    // TODO: arithmetic operations

    // Compound filters
    Compound(Vec<ContentFilter>, BinaryOperator),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BinaryOperator {
    And,
    Or,
}

/// Controls how many adjacent lines to return.
#[derive(Debug, Default, Serialize, Deserialize)]
pub enum Context {
    #[default]
    None,
    /// Specify the number of lines before and after the matched line separately.
    Lines(usize, usize),
    /// Specify the number of seconds before and after the matched line occurred.
    Seconds(usize, usize),
}

/// Represents limit and offset parameters for query pagination.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Limit {
    /// Optional number of items to skip before starting to return results
    pub skip: Option<usize>,
    /// Optional number of items to return after skipping
    pub fetch: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn test_canonicalize() {
        // with 'start' only
        let mut tf = TimeFilter {
            start: Some("2023-10-01".to_string()),
            end: None,
            span: None,
        };
        tf.canonicalize().unwrap();
        assert!(tf.end.is_some());

        // with 'start' and 'span'
        let mut tf = TimeFilter {
            start: Some("2023-10-01T00:00:00Z".to_string()),
            end: None,
            span: Some("86400".to_string()), // 1 day in seconds
        };
        tf.canonicalize().unwrap();
        assert_eq!(tf.end.as_ref().unwrap(), "2023-10-02T00:00:00+00:00");

        // with 'end' and 'span'
        let mut tf = TimeFilter {
            start: None,
            end: Some("2023-10-02T00:00:00Z".to_string()),
            span: Some("86400".to_string()), // 1 day in seconds
        };
        tf.canonicalize().unwrap();
        assert_eq!(tf.start.as_ref().unwrap(), "2023-10-01T00:00:00+00:00");

        // with both 'start' and 'end'
        let mut tf = TimeFilter {
            start: Some("2023-10-01T00:00:00Z".to_string()),
            end: Some("2023-10-02T00:00:00Z".to_string()),
            span: None,
        };
        tf.canonicalize().unwrap();
        assert_eq!(tf.start.as_ref().unwrap(), "2023-10-01T00:00:00+00:00");
        assert_eq!(tf.end.as_ref().unwrap(), "2023-10-02T00:00:00+00:00");

        // with invalid date format
        let mut tf = TimeFilter {
            start: Some("invalid-date".to_string()),
            end: None,
            span: None,
        };
        let result = tf.canonicalize();
        assert!(matches!(result, Err(Error::InvalidDateFormat { .. })));

        // with missing 'start' and 'end'
        let mut tf = TimeFilter {
            start: None,
            end: None,
            span: None,
        };
        let result = tf.canonicalize();
        assert!(matches!(result, Err(Error::InvalidTimeFilter { .. })));

        // 'end' is before 'start'
        let mut tf = TimeFilter {
            start: Some("2023-10-02T00:00:00Z".to_string()),
            end: Some("2023-10-01T00:00:00Z".to_string()),
            span: None,
        };
        let result = tf.canonicalize();
        assert!(matches!(result, Err(Error::EndBeforeStart { .. })));
    }
}
