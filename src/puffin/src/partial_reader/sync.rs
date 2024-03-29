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

use crate::partial_reader::position::position_after_seek;
use crate::partial_reader::PartialReader;

impl<R: io::Read + io::Seek> io::Read for PartialReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // past end of portion
        if self.position() > self.size() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid read past the end of the portion",
            ));
        }

        // end of portion
        if self.is_eof() {
            return Ok(0);
        }

        // haven't read from the portion yet, need to seek to the start of it.
        if self.position_in_portion.is_none() {
            self.source.seek(io::SeekFrom::Start(self.offset))?;
            self.position_in_portion = Some(0);
        }

        // prevent reading over the end
        let max_len = (self.size() - self.position_in_portion.unwrap()) as usize;
        let actual_len = max_len.min(buf.len());

        // create a limited reader
        let target_buf = &mut buf[..actual_len];

        // perform the actual read from the source and update the position.
        let read_bytes = self.source.read(target_buf)?;
        self.position_in_portion = Some(self.position_in_portion.unwrap() + read_bytes as u64);

        Ok(read_bytes)
    }
}

impl<R: io::Read + io::Seek> io::Seek for PartialReader<R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let new_position = position_after_seek(pos, self.position(), self.size())?;
        let pos = io::SeekFrom::Start(self.offset + new_position);
        self.source.seek(pos)?;

        self.position_in_portion = Some(new_position);
        Ok(new_position)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Seek, SeekFrom};

    use super::*;

    #[test]
    fn read_all_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data.clone()), 0, 100);
        let mut buf = vec![0; 100];
        assert_eq!(reader.read(&mut buf).unwrap(), 100);
        assert_eq!(buf, data);
    }

    #[test]
    fn read_part_of_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        let mut buf = vec![0; 30];
        assert_eq!(reader.read(&mut buf).unwrap(), 30);
        assert_eq!(buf, (10..40).collect::<Vec<u8>>());
    }

    #[test]
    fn seek_and_read_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert_eq!(reader.seek(SeekFrom::Start(10)).unwrap(), 10);
        let mut buf = vec![0; 10];
        assert_eq!(reader.read(&mut buf).unwrap(), 10);
        assert_eq!(buf, (20..30).collect::<Vec<u8>>());
    }

    #[test]
    fn read_past_end_of_portion_is_eof() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        let mut buf = vec![0; 50];
        assert_eq!(reader.read(&mut buf).unwrap(), 30);
        assert_eq!(reader.read(&mut buf).unwrap(), 0); // hit EOF
    }

    #[test]
    fn seek_past_end_of_portion_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        // seeking past the portion returns an error
        assert!(reader.seek(SeekFrom::Start(31)).is_err());
    }

    #[test]
    fn seek_to_negative_position_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert_eq!(reader.seek(SeekFrom::Start(10)).unwrap(), 10);
        // seeking back to the start of the portion
        assert_eq!(reader.seek(SeekFrom::Current(-10)).unwrap(), 0);
        // seeking to a negative position returns an error
        assert!(reader.seek(SeekFrom::Current(-1)).is_err());
    }

    #[test]
    fn seek_from_end_of_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        let mut buf = vec![0; 10];
        // seek to 10 bytes before the end of the portion
        assert_eq!(reader.seek(SeekFrom::End(-10)).unwrap(), 20);
        assert_eq!(reader.read(&mut buf).unwrap(), 10);
        // the final 10 bytes of the portion
        assert_eq!(buf, (30..40).collect::<Vec<u8>>());
        assert!(reader.is_eof());
    }

    #[test]
    fn seek_from_end_to_negative_position_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data.clone()), 10, 30);
        // seeking to a negative position returns an error
        assert!(reader.seek(SeekFrom::End(-31)).is_err());
    }

    #[test]
    fn zero_length_portion_returns_zero_on_read() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 0);
        let mut buf = vec![0; 10];
        // reading a portion with zero length returns 0 bytes
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn is_eof_returns_true_at_end_of_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        // we are not at the end of the portion
        assert!(!reader.is_eof());
        let mut buf = vec![0; 30];
        assert_eq!(reader.read(&mut buf).unwrap(), 30);
        // we are at the end of the portion
        assert!(reader.is_eof());
    }

    #[test]
    fn position_resets_after_seek_to_start() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert_eq!(reader.seek(SeekFrom::Start(10)).unwrap(), 10);
        assert_eq!(reader.position(), 10);
        assert_eq!(reader.seek(SeekFrom::Start(0)).unwrap(), 0);
        assert_eq!(reader.position(), 0);
    }
}
