//! Modified from Tokio's mini-redis example.

use std::io::Cursor;

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

        let mut buf = Cursor::new(&self.buffer[..]);

        let mut line: Option<&[u8]> = None;

        let start = buf.position() as usize;
        let end = buf.get_ref().len() - 1;
        for i in start..end {
            if buf.get_ref()[i] == b'\r' && buf.get_ref()[i + 1] == b'\n' {
                // We found a line, update the position to be *after* the \n
                buf.set_position((i + 2) as u64);

                line = Some(&buf.get_ref()[start..i]);
                break;
            }
        }

        if let Some(line) = line {
            let line = line.to_vec();

            self.buffer.advance(buf.position() as usize);

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
        self.stream
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
            .contains("connection reset by peer"));
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid Opentsdb line, source: invalid utf-8 sequence"));
    }

    #[tokio::test]
    async fn test_write_err() {
        let mock = Builder::new()
            .write(b"An Opentsdb error.")
            .write(b"\r\n")
            .build();
        let mut conn = Connection::new(mock);
        let result = conn.write_line("An Opentsdb error.".to_string()).await;
        assert!(result.is_ok());
    }
}
