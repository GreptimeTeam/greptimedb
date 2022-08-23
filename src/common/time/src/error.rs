use chrono::ParseError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}, source: {}", raw, source))]
    ParseDateStr { raw: String, source: ParseError },

    #[snafu(display("Failed to parse i32 value to date: {}", value))]
    DateOverflow { value: i32 },
}

pub type Result<T> = std::result::Result<T, Error>;
