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

//! Format-neutral parsing of the COPY `WITH (...)` option list.
//!
//! Every option recognized by any format is collected into a
//! [`CopyOptionSet`] here, with format-independent validation (single-byte
//! character options, redundant options). Which options a format actually
//! accepts and their defaults are decided by the format's builder.

use datafusion::sql::sqlparser::ast::CopyOption;
use pgwire::error::PgWireResult;

use super::{bad_copy_data, unsupported};

/// The options of a `COPY ... FROM STDIN [WITH (...)]` statement, shared
/// by all format builders.
#[derive(Default)]
pub(super) struct CopyOptionSet {
    /// `FORMAT name`; `None` means the format default applies.
    pub(super) format: Option<String>,
    pub(super) delimiter: Option<u8>,
    pub(super) quote: Option<u8>,
    pub(super) escape: Option<u8>,
    pub(super) null: Option<Vec<u8>>,
    pub(super) header: bool,
}

/// Parses the option list of a COPY FROM STDIN statement.
pub(super) fn parse_copy_options(options: &[CopyOption]) -> PgWireResult<CopyOptionSet> {
    let mut set = CopyOptionSet::default();
    for option in options {
        match option {
            CopyOption::Format(name) => {
                if set.format.replace(name.value.to_lowercase()).is_some() {
                    return Err(bad_copy_data("conflicting or redundant FORMAT option"));
                }
            }
            CopyOption::Delimiter(c) => {
                let d = single_byte(*c, "DELIMITER")?;
                if set.delimiter.replace(d).is_some() {
                    return Err(bad_copy_data("conflicting or redundant DELIMITER option"));
                }
            }
            CopyOption::Quote(c) => set.quote = Some(single_byte(*c, "QUOTE")?),
            CopyOption::Escape(c) => set.escape = Some(single_byte(*c, "ESCAPE")?),
            CopyOption::Null(s) => set.null = Some(s.clone().into_bytes()),
            CopyOption::Header(h) => set.header = *h,
            // Format-specific options unknown to every registered format.
            CopyOption::Freeze(_)
            | CopyOption::Encoding(_)
            | CopyOption::ForceQuote(_)
            | CopyOption::ForceNotNull(_)
            | CopyOption::ForceNull(_) => {
                return Err(unsupported(format!(
                    "COPY option {} is not supported",
                    option
                )));
            }
        }
    }
    Ok(set)
}

/// COPY character options must be a single printable one-byte character.
fn single_byte(c: char, option: &str) -> PgWireResult<u8> {
    if c.is_ascii() && !c.is_ascii_control() {
        Ok(c as u8)
    } else {
        Err(bad_copy_data(format!(
            "{} must be a single one-byte non-newline character",
            option
        )))
    }
}
