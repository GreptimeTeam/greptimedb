use std::path::Path;

use store_api::logstore::entry::Id;

use crate::error::Error;

/// FileName represents the file name with padded leading zeros.
#[derive(Debug, PartialEq, Eq)]
pub struct FileName {
    start_entry_id: Id,
    suffix: String,
}

impl TryFrom<&str> for FileName {
    type Error = Error;

    fn try_from(p: &str) -> Result<Self, Self::Error> {
        let path = Path::new(p);

        let extension = path
            .extension()
            .and_then(|s| s.to_str())
            .ok_or(Error::FileNameIllegal {
                file_name: p.to_string(),
            })?
            .to_string();

        let id: u64 = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(Error::FileNameIllegal {
                file_name: p.to_string(),
            })?;

        Ok(Self::new_with_suffix(id, extension))
    }
}

impl From<u64> for FileName {
    fn from(entry_id: u64) -> Self {
        Self::new(entry_id)
    }
}

impl FileName {
    pub fn new(entry_id: Id) -> Self {
        Self {
            start_entry_id: entry_id,
            suffix: ".log".to_string(),
        }
    }

    pub fn new_with_suffix(entry_id: Id, suffix: String) -> Self {
        Self {
            start_entry_id: entry_id,
            suffix,
        }
    }

    pub fn entry_id(&self) -> Id {
        self.start_entry_id
    }
}

impl ToString for FileName {
    fn to_string(&self) -> String {
        FileName::pad_file_name(self.start_entry_id, FileName::file_name_width(), '0')
            + self.suffix.as_str()
    }
}

impl FileName {
    fn file_name_width() -> usize {
        u64::MAX.to_string().len()
    }

    fn pad_file_name(id: u64, width: usize, padding: char) -> String {
        let mut res = String::new();
        let id_string = id.to_string();
        let padding_size = width - id_string.len();
        for _ in 0..padding_size {
            res.push(padding);
        }
        res.push_str(id_string.as_str());
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_file_name_width() {
        assert_eq!(20, FileName::file_name_width());
    }

    #[test]
    pub fn test_padding_file_name() {
        assert_eq!(
            "00000000000000000000",
            FileName::pad_file_name(u64::MIN, FileName::file_name_width(), '0')
        );
        assert_eq!(
            "00000000000000000123",
            FileName::pad_file_name(123, FileName::file_name_width(), '0')
        );
        assert_eq!(
            "00000000123123123123",
            FileName::pad_file_name(123123123123, FileName::file_name_width(), '0')
        );
        assert_eq!(
            u64::MAX.to_string(),
            FileName::pad_file_name(u64::MAX, FileName::file_name_width(), '0')
        );
    }

    #[test]
    pub fn test_file_name_to_string() {
        assert_eq!("00000000000000000000.log", FileName::new(0).to_string());
        assert_eq!(
            u64::MAX.to_string() + ".log",
            FileName::new(u64::MAX).to_string()
        );
    }

    #[test]
    pub fn test_parse_file_name() {
        let path = "/path/to/any/01010010000.log";
        let parsed = FileName::try_from(path).expect("Failed to parse file name");
        assert_eq!(1010010000u64, parsed.start_entry_id);
        assert_eq!("log", parsed.suffix.as_str());
    }
}
