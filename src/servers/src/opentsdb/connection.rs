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

//! Modified from Tokio's mini-redis example.

use bytes::{Buf, BytesMut};
use snafu::ResultExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use crate::error::{self, Result};

type Line = String;

#[derive(Debug)]
pub struct Connection<S: AsyncWrite + AsyncRead + Unpin> {
    stream: BufWriter<S>,
    buffer: BytesMut,
}

impl<S: AsyncWrite + AsyncRead + Unpin> Connection<S> {
    pub fn new(stream: S) -> Connection<S> {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read one line from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a line (terminated by \r\n).
    /// Any data remaining in the read buffer after the line has been parsed is kept there for the
    /// next call to `read_line`.
    ///
    /// # Returns
    ///
    /// On success, the received line is returned. If the stream is closed in a way that
    /// doesn't break a line in half, it returns `None`. Otherwise, an error is returned.
    pub async fn read_line(&mut self) -> Result<Option<Line>> {
        loop {
            // Attempt to parse a line from the buffered data. If enough data
            // has been buffered, the line is returned.
            if let Some(line) = self.parse_line()? {
                return Ok(Some(line));
            }

            // There is not enough buffered data as a line. Attempt to read more from the socket.
            // On success, the number of bytes is returned. `0` indicates "end of stream".
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // The remote closed the connection. For this to be a clean shutdown, there should
                // be no data in the read buffer. If there is, this means that the peer closed the
                // socket while sending a line.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return error::ConnResetByPeerSnafu {}.fail();
                }
            }
        }
    }

    /// Tries to parse a line from the buffer.
    ///
    /// If the buffer contains enough data, the line is returned and the buffered data is removed.
    /// If not enough data has been buffered yet, `Ok(None)` is returned.
    /// If the buffered data does not represent a valid UTF8 line, `Err` is returned.
    fn parse_line(&mut self) -> Result<Option<Line>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let buf = &self.buffer[..];
        if let Some(pos) = buf.windows(2).position(|w| w == [b'\r', b'\n']) {
            let line = buf[0..pos].to_vec();

            self.buffer.advance(pos + 2);

            Ok(Some(
                String::from_utf8(line).context(error::InvalidOpentsdbLineSnafu)?,
            ))
        } else {
            // There is not enough data present in the read buffer to parse a single line. We must
            // wait for more data to be received from the socket.
            Ok(None)
        }
    }

    pub async fn write_line(&mut self, line: String) -> Result<()> {
        self.stream
            .write_all(line.as_bytes())
            .await
            .context(error::InternalIoSnafu)?;
        let _ = self
            .stream
            .write(b"\r\n")
            .await
            .context(error::InternalIoSnafu)?;
        self.stream.flush().await.context(error::InternalIoSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::io::Write;

    use bytes::BufMut;
    use common_error::ext::ErrorExt;
    use tokio_test::io::Builder;

    use super::*;

    #[tokio::test]
    async fn test_read_line() {
        let mock = Builder::new()
            .read(b"This is")
            .read(b" a line.\r\n")
            .build();
        let mut conn = Connection::new(mock);
        let line = conn.read_line().await.unwrap();
        assert_eq!(line, Some("This is a line.".to_string()));

        let line = conn.read_line().await.unwrap();
        assert_eq!(line, None);

        let buffer = &mut conn.buffer;
        buffer
            .writer()
            .write_all(b"simulating buffer has remaining data")
            .unwrap();
        let result = conn.read_line().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Connection reset by peer"));
    }

    #[test]
    fn test_parse_line() {
        let mock = Builder::new().build();
        let mut conn = Connection::new(mock);

        // initially, no data in the buffer, so return None
        let line = conn.parse_line();
        assert_matches!(line, Ok(None));

        // still has no line, but we have data in the buffer
        {
            let buffer = &mut conn.buffer;
            buffer.writer().write_all(b"This is a ").unwrap();
            let line = conn.parse_line();
            assert_matches!(line, Ok(None));
        }
        let buffer = &conn.buffer[..];
        assert_eq!(String::from_utf8(buffer.to_vec()).unwrap(), "This is a ");

        // finally gets a line, and the buffer has the remaining data
        {
            let buffer = &mut conn.buffer;
            buffer
                .writer()
                .write_all(b"line.\r\n another line's remaining data")
                .unwrap();
            let line = conn.parse_line().unwrap();
            assert_eq!(line, Some("This is a line.".to_string()));
        }
        let buffer = &conn.buffer[..];
        assert_eq!(
            String::from_utf8(buffer.to_vec()).unwrap(),
            " another line's remaining data"
        );

        // expected failed on not valid utf-8 line
        let buffer = &mut conn.buffer;
        buffer.writer().write_all(b"Hello Wor\xffld.\r\n").unwrap();
        let result = conn.parse_line();
        assert!(result.is_err());
        let err = result.unwrap_err().output_msg();
        assert!(err.contains("invalid utf-8 sequence"));
    }

    #[tokio::test]
    async fn test_write_err() {
        let mock = Builder::new()
            .write(b"An OpenTSDB error.")
            .write(b"\r\n")
            .build();
        let mut conn = Connection::new(mock);
        conn.write_line("An OpenTSDB error.".to_string())
            .await
            .unwrap();
    }
}
