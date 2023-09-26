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

use std::any::Any;
use std::io::{Read, Write};

use bytes::{Buf, BufMut, BytesMut};
use common_error::ext::ErrorExt;
use common_macro::stack_trace_debug;
use paste::paste;
use snafu::{ensure, Location, ResultExt, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display(
        "Destination buffer overflow, src_len: {}, dst_len: {}",
        src_len,
        dst_len
    ))]
    Overflow {
        src_len: usize,
        dst_len: usize,
        location: Location,
    },

    #[snafu(display("Buffer underflow"))]
    Underflow { location: Location },

    #[snafu(display("IO operation reach EOF"))]
    Eof {
        #[snafu(source)]
        error: std::io::Error,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn location_opt(&self) -> Option<common_error::snafu::Location> {
        match self {
            Error::Overflow { location, .. } => Some(*location),
            Error::Underflow { location, .. } => Some(*location),
            Error::Eof { location, .. } => Some(*location),
        }
    }
}

macro_rules! impl_read_le {
    ( $($num_ty: ty), *) => {
        $(
            paste!{
                // TODO(hl): default implementation requires allocating a
                // temp buffer. maybe use more efficient impls in concrete buffers.
                // see https://github.com/GrepTimeTeam/greptimedb/pull/97#discussion_r930798941
                fn [<read_ $num_ty _le>](&mut self) -> Result<$num_ty> {
                    let mut buf = [0u8; std::mem::size_of::<$num_ty>()];
                    self.read_to_slice(&mut buf)?;
                    Ok($num_ty::from_le_bytes(buf))
                }

                fn [<peek_ $num_ty _le>](&mut self) -> Result<$num_ty> {
                    let mut buf = [0u8; std::mem::size_of::<$num_ty>()];
                    self.peek_to_slice(&mut buf)?;
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

pub trait Buffer {
    /// Returns remaining data size for read.
    fn remaining_size(&self) -> usize;

    /// Returns true if buffer has no data for read.
    fn is_empty(&self) -> bool {
        self.remaining_size() == 0
    }

    /// Peeks data into dst. This method should not change internal cursor,
    /// invoke `advance_by` if needed.
    /// # Panics
    /// This method **may** panic if buffer does not have enough data to be copied to dst.
    fn peek_to_slice(&self, dst: &mut [u8]) -> Result<()>;

    /// Reads data into dst. This method will change internal cursor.
    /// # Panics
    /// This method **may** panic if buffer does not have enough data to be copied to dst.
    fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
        self.peek_to_slice(dst)?;
        self.advance_by(dst.len());
        Ok(())
    }

    /// Advances internal cursor for next read.
    /// # Panics
    /// This method **may** panic if the offset after advancing exceeds the length of underlying buffer.
    fn advance_by(&mut self, by: usize);

    impl_read_le![u8, i8, u16, i16, u32, i32, u64, i64, f32, f64];
}

macro_rules! impl_buffer_for_bytes {
    ( $($buf_ty:ty), *) => {
        $(
        impl Buffer for $buf_ty {
            fn remaining_size(&self) -> usize{
                self.len()
            }

            fn peek_to_slice(&self, dst: &mut [u8]) -> Result<()> {
                let dst_len = dst.len();
                ensure!(self.remaining() >= dst.len(), OverflowSnafu {
                        src_len: self.remaining_size(),
                        dst_len,
                    }
                );
                dst.copy_from_slice(&self[0..dst_len]);
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
    fn remaining_size(&self) -> usize {
        self.len()
    }

    fn peek_to_slice(&self, dst: &mut [u8]) -> Result<()> {
        let dst_len = dst.len();
        ensure!(
            self.len() >= dst.len(),
            OverflowSnafu {
                src_len: self.remaining_size(),
                dst_len,
            }
        );
        dst.copy_from_slice(&self[0..dst_len]);
        Ok(())
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
