use std::path::Path;

use snafu::OptionExt;
use store_api::logstore::entry::Id;

use crate::error::{Error, FileNameIllegalSnafu};
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
                .context(FileNameIllegalSnafu {
                    file_name: path.to_string_lossy(),
                })?;

        let id: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(Error::FileNameIllegal {
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
            _ => Err(Error::FileNameIllegal {
                file_name: "Suffix ".to_string() + suffix + " is not valid.",
            }),
        }
    }

    pub fn entry_id(&self) -> Id {
        match self {
            Log(id) => *id,
        }
    }

    fn pad_file_name(id: u64) -> String {
        // file name padded to `u64::MAX.to_string().len()` with prefixed zeros
        format!("{:020}", id)
    }

    fn suffix(&self) -> &str {
        match self {
            Log(_) => ".log",
        }
    }
}

impl ToString for FileName {
    fn to_string(&self) -> String {
        FileName::pad_file_name(self.entry_id()) + self.suffix()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_padding_file_name() {
        assert_eq!("00000000000000000000", FileName::pad_file_name(u64::MIN));
        assert_eq!("00000000000000000123", FileName::pad_file_name(123));
        assert_eq!(
            "00000000123123123123",
            FileName::pad_file_name(123123123123)
        );
        assert_eq!(u64::MAX.to_string(), FileName::pad_file_name(u64::MAX));
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
