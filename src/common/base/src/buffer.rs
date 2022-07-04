use std::any::Any;
use std::io::{Read, Write};

use bytes::{Buf, BufMut, BytesMut};
use common_error::prelude::ErrorExt;
use paste::paste;
use snafu::{ensure, Backtrace, ErrorCompat, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Destination buffer overflow, src_len: {}, dst_len: {}",
        src_len,
        dst_len
    ))]
    Overflow {
        src_len: usize,
        dst_len: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("IO operation reach EOF, source: {}", source))]
    Eof {
        source: std::io::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

macro_rules! impl_read_le {
    ( $($num_ty: ty), *) => {
        $(
            paste!{
                fn [<read_ $num_ty _le>](&mut self) -> Result<$num_ty> {
                    let mut buf = [0u8; std::mem::size_of::<$num_ty>()];
                    self.read_to_slice(&mut buf)?;
                    Ok($num_ty::from_le_bytes(buf))
                }
            }
        )*
    }
}

macro_rules! impl_write_le {
    ( $($num_ty: ty), *) => {
        $(
            paste!{
                fn [<write_ $num_ty _le>](&mut self, n: $num_ty) -> Result<()> {
                    self.write_from_slice(&n.to_le_bytes())?;
                    Ok(())
                }
            }
        )*
    }
}

pub trait Buffer: AsRef<[u8]> {
    fn remaining_slice(&self) -> &[u8];

    fn remaining_size(&self) -> usize {
        self.remaining_slice().len()
    }

    fn is_empty(&self) -> bool {
        self.remaining_size() == 0
    }

    /// # Panics
    /// This method **may** panic if buffer does not have enough data to be copied to dst.
    fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()>;

    /// # Panics
    /// This method **may** panic if the offset after advancing exceeds the length of underlying buffer.
    fn advance_by(&mut self, by: usize);

    impl_read_le![u8, i8, u16, i16, u32, i32, u64, i64, f32, f64];
}

macro_rules! impl_buffer_for_bytes {
    ( $($buf_ty:ty), *) => {
        $(
        impl Buffer for $buf_ty {
            #[inline]
            fn remaining_slice(&self) -> &[u8] {
                &self
            }

            fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
                ensure!(self.remaining() >= dst.len(), OverflowSnafu {
                        src_len: self.remaining_size(),
                        dst_len: dst.len(),
                    }
                );
                self.copy_to_slice(dst);
                Ok(())
            }

            #[inline]
            fn advance_by(&mut self, by: usize) {
                self.advance(by);
            }
        }
        )*
    };
}

impl_buffer_for_bytes![bytes::Bytes, bytes::BytesMut];

impl Buffer for &[u8] {
    fn remaining_slice(&self) -> &[u8] {
        self
    }

    fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
        ensure!(
            self.len() >= dst.len(),
            OverflowSnafu {
                src_len: self.remaining_size(),
                dst_len: dst.len(),
            }
        );
        self.read_exact(dst).context(EofSnafu)
    }

    fn advance_by(&mut self, by: usize) {
        *self = &self[by..];
    }
}

/// Mutable buffer.
pub trait BufferMut {
    fn as_slice(&self) -> &[u8];

    fn write_from_slice(&mut self, src: &[u8]) -> Result<()>;

    impl_write_le![i8, u8, i16, u16, i32, u32, i64, u64, f32, f64];
}

impl BufferMut for BytesMut {
    fn as_slice(&self) -> &[u8] {
        self
    }

    fn write_from_slice(&mut self, src: &[u8]) -> Result<()> {
        self.put_slice(src);
        Ok(())
    }
}

impl BufferMut for &mut [u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }

    fn write_from_slice(&mut self, src: &[u8]) -> Result<()> {
        // see std::io::Write::write_all
        // https://doc.rust-lang.org/src/std/io/impls.rs.html#363
        self.write_all(src).map_err(|_| {
            OverflowSnafu {
                src_len: src.len(),
                dst_len: self.as_slice().len(),
            }
            .build()
        })
    }
}

impl BufferMut for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self
    }

    fn write_from_slice(&mut self, src: &[u8]) -> Result<()> {
        self.extend_from_slice(src);
        Ok(())
    }
}
