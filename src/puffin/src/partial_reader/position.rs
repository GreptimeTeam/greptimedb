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

use std::io;

/// Calculates the new position after seeking. It checks if the new position
/// is valid (within the portion bounds) before returning it.
pub fn position_after_seek(
    seek_from: io::SeekFrom,
    position_in_portion: u64,
    size_of_portion: u64,
) -> io::Result<u64> {
    let new_position = match seek_from {
        io::SeekFrom::Start(offset) => offset,
        io::SeekFrom::Current(offset) => {
            let next = (position_in_portion as i64) + offset;
            if next < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ));
            }
            next as u64
        }
        io::SeekFrom::End(offset) => {
            let end = size_of_portion as i64;
            (end + offset) as u64
        }
    };

    if new_position > size_of_portion {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid seek to a position beyond the end of the portion",
        ));
    }

    Ok(new_position)
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    #[test]
    fn test_position_after_seek_from_start() {
        let result = position_after_seek(io::SeekFrom::Start(10), 0, 20).unwrap();
        assert_eq!(result, 10);
    }

    #[test]
    fn test_position_after_seek_from_start_out_of_bounds() {
        let result = position_after_seek(io::SeekFrom::Start(30), 0, 20);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn test_position_after_seek_from_current() {
        let result = position_after_seek(io::SeekFrom::Current(10), 10, 30).unwrap();
        assert_eq!(result, 20);
    }

    #[test]
    fn test_position_after_seek_from_current_negative_position_within_bounds() {
        let result = position_after_seek(io::SeekFrom::Current(-10), 15, 20).unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn test_position_after_seek_from_current_negative_position() {
        let result = position_after_seek(io::SeekFrom::Current(-10), 5, 20);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn test_position_after_seek_from_end() {
        let result = position_after_seek(io::SeekFrom::End(-10), 0, 30).unwrap();
        assert_eq!(result, 20);
    }

    #[test]
    fn test_position_after_seek_from_end_out_of_bounds() {
        let result = position_after_seek(io::SeekFrom::End(10), 0, 20);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }
}
