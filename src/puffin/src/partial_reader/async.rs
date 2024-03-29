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
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, AsyncRead, AsyncSeek};

use crate::partial_reader::position::position_after_seek;
use crate::partial_reader::PartialReader;

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncRead for PartialReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // past end of portion
        if self.position() > self.size() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid read past the end of the portion",
            )));
        }

        // end of portion
        if self.is_eof() {
            return Poll::Ready(Ok(0));
        }

        // first read, seek to the correct offset
        if self.position_in_portion.is_none() {
            // seek operation
            let seek_from = io::SeekFrom::Start(self.offset);
            ready!(self.as_mut().project().source.poll_seek(cx, seek_from))?;

            self.position_in_portion = Some(0);
        }

        // prevent reading over the end
        let max_len = (self.size() - self.position_in_portion.unwrap()) as usize;
        let actual_len = max_len.min(buf.len());

        // create a limited reader
        let target_buf = &mut buf[..actual_len];

        // read operation
        let read_bytes = ready!(self.as_mut().project().source.poll_read(cx, target_buf))?;
        self.position_in_portion = Some(self.position() + read_bytes as u64);

        Poll::Ready(Ok(read_bytes))
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncSeek for PartialReader<R> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let new_position = position_after_seek(pos, self.position(), self.size())?;
        let pos = io::SeekFrom::Start(self.offset + new_position);
        ready!(self.as_mut().project().source.poll_seek(cx, pos))?;

        self.position_in_portion = Some(new_position);
        Poll::Ready(Ok(new_position))
    }
}

#[cfg(test)]
mod tests {
    use futures::io::Cursor;
    use futures::{AsyncReadExt as _, AsyncSeekExt as _};

    use super::*;

    #[tokio::test]
    async fn read_all_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data.clone()), 0, 100);
        let mut buf = vec![0; 100];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 100);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn read_part_of_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        let mut buf = vec![0; 30];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 30);
        assert_eq!(buf, (10..40).collect::<Vec<u8>>());
    }

    #[tokio::test]
    async fn seek_and_read_data_in_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert_eq!(reader.seek(io::SeekFrom::Start(10)).await.unwrap(), 10);
        let mut buf = vec![0; 10];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 10);
        assert_eq!(buf, (20..30).collect::<Vec<u8>>());
    }

    #[tokio::test]
    async fn read_past_end_of_portion_is_eof() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        let mut buf = vec![0; 50];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 30);
        assert_eq!(reader.read(&mut buf).await.unwrap(), 0); // hit EOF
    }

    #[tokio::test]
    async fn seek_past_end_of_portion_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        // seeking past the portion returns an error
        assert!(reader.seek(io::SeekFrom::Start(31)).await.is_err());
    }

    #[tokio::test]
    async fn seek_to_negative_position_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert_eq!(reader.seek(io::SeekFrom::Start(10)).await.unwrap(), 10);
        // seeking back to the start of the portion
        assert_eq!(reader.seek(io::SeekFrom::Current(-10)).await.unwrap(), 0);
        // seeking to a negative position returns an error
        assert!(reader.seek(io::SeekFrom::Current(-1)).await.is_err());
    }

    #[tokio::test]
    async fn seek_from_end_of_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        let mut buf = vec![0; 10];
        // seek to 10 bytes before the end of the portion
        assert_eq!(reader.seek(io::SeekFrom::End(-10)).await.unwrap(), 20);
        assert_eq!(reader.read(&mut buf).await.unwrap(), 10);
        // the final 10 bytes of the portion
        assert_eq!(buf, (30..40).collect::<Vec<u8>>());
        assert!(reader.is_eof());
    }

    #[tokio::test]
    async fn seek_from_end_to_negative_position_returns_error() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data.clone()), 10, 30);
        // seeking to a negative position returns an error
        assert!(reader.seek(io::SeekFrom::End(-31)).await.is_err());
    }

    #[tokio::test]
    async fn zero_length_portion_returns_zero_on_read() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 0);
        let mut buf = vec![0; 10];
        // reading a portion with zero length returns 0 bytes
        assert_eq!(reader.read(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn is_eof_returns_true_at_end_of_portion() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        // we are not at the end of the portion
        assert!(!reader.is_eof());
        let mut buf = vec![0; 30];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 30);
        // we are at the end of the portion
        assert!(reader.is_eof());
    }

    #[tokio::test]
    async fn position_resets_after_seek_to_start() {
        let data: Vec<u8> = (0..100).collect();
        let mut reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert_eq!(reader.seek(io::SeekFrom::Start(10)).await.unwrap(), 10);
        assert_eq!(reader.position(), 10);
        assert_eq!(reader.seek(io::SeekFrom::Start(0)).await.unwrap(), 0);
        assert_eq!(reader.position(), 0);
    }
}
