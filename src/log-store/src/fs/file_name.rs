use std::fmt::{Display, Formatter};
use std::path::Path;

use snafu::OptionExt;
use store_api::logstore::entry::Id;

use crate::error::{Error, FileNameIllegalSnafu, SuffixIllegalSnafu};
use crate::fs::file_name::FileName::Log;

/// FileName represents the file name with padded leading zeros.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FileName {
    // File name with .log as suffix.
    Log(Id),
}

impl TryFrom<&str> for FileName {
    type Error = Error;

    fn try_from(p: &str) -> Result<Self, Self::Error> {
        let path = Path::new(p);

        let extension =
            path.extension()
                .and_then(|s| s.to_str())
                .with_context(|| FileNameIllegalSnafu {
                    file_name: path.to_string_lossy(),
                })?;

        let id: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .with_context(|| FileNameIllegalSnafu {
                file_name: p.to_string(),
            })?;

        Self::new_with_suffix(id, extension)
    }
}

impl From<u64> for FileName {
    fn from(entry_id: u64) -> Self {
        Self::log(entry_id)
    }
}

impl FileName {
    pub fn log(entry_id: Id) -> Self {
        Log(entry_id)
    }

    pub fn new_with_suffix(entry_id: Id, suffix: &str) -> Result<Self, Error> {
        match suffix {
            "log" => Ok(Log(entry_id)),
            _ => SuffixIllegalSnafu { suffix }.fail(),
        }
    }

    pub fn entry_id(&self) -> Id {
        match self {
            Log(id) => *id,
        }
    }

    fn suffix(&self) -> &str {
        match self {
            Log(_) => ".log",
        }
    }
}

impl Display for FileName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:020}{}", self.entry_id(), self.suffix())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_padding_file_name() {
        let id = u64::MIN;
        assert_eq!("00000000000000000000", format!("{:020}", id));
        let id = 123u64;
        assert_eq!("00000000000000000123", format!("{:020}", id));
        let id = 123123123123u64;
        assert_eq!("00000000123123123123", format!("{:020}", id));
        let id = u64::MAX;
        assert_eq!(u64::MAX.to_string(), format!("{:020}", id));
    }

    #[test]
    pub fn test_file_name_to_string() {
        assert_eq!("00000000000000000000.log", FileName::log(0).to_string());
        assert_eq!(
            u64::MAX.to_string() + ".log",
            FileName::log(u64::MAX).to_string()
        );
    }

    #[test]
    pub fn test_parse_file_name() {
        let path = "/path/to/any/01010010000.log";
        let parsed = FileName::try_from(path).expect("Failed to parse file name");
        assert_eq!(1010010000u64, parsed.entry_id());
        assert_eq!(".log", parsed.suffix());
    }
}
