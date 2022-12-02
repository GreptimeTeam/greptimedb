// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::ParseError;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}, source: {}", raw, source))]
    ParseDateStr { raw: String, source: ParseError },
    #[snafu(display("Failed to parse a string into Timestamp, raw string: {}", raw))]
    ParseTimestamp { raw: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use snafu::ResultExt;

    use super::*;

    #[test]
    fn test_errors() {
        let raw = "2020-09-08T13:42:29.190855Z";
        let result = NaiveDateTime::parse_from_str(raw, "%F").context(ParseDateStrSnafu { raw });
        assert!(matches!(result.err().unwrap(), Error::ParseDateStr { .. }));

        assert_eq!(
            "Failed to parse a string into Timestamp, raw string: 2020-09-08T13:42:29.190855Z",
            ParseTimestampSnafu { raw }.build().to_string()
        );
    }
}
