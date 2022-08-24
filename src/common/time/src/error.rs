use chrono::ParseError;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}, source: {}", raw, source))]
    ParseDateStr { raw: String, source: ParseError },

    #[snafu(display("Failed to parse i32 value to Date: {}", value))]
    DateOverflow { value: i32, backtrace: Backtrace },

    #[snafu(display("Failed to parse i64 value to DateTime: {}", value))]
    DateTimeOverflow { value: i64, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;
