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

#![feature(assert_matches)]

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use bytes::{Buf, Bytes, BytesMut};
    use common_base::buffer::Error::Overflow;
    use common_base::buffer::{Buffer, BufferMut};
    use paste::paste;

    #[test]
    pub fn test_buffer_read_write() {
        let mut buf = BytesMut::with_capacity(16);
        buf.write_u64_le(1234u64).unwrap();
        let result = buf.peek_u64_le().unwrap();
        assert_eq!(1234u64, result);
        buf.advance_by(8);

        buf.write_from_slice("hello, world".as_bytes()).unwrap();
        let mut content = vec![0u8; 5];
        buf.peek_to_slice(&mut content).unwrap();
        let read = String::from_utf8_lossy(&content);
        assert_eq!("hello", read);
        buf.advance_by(5);
        // after read, buffer should still have 7 bytes to read.
        assert_eq!(7, buf.remaining());

        let mut content = vec![0u8; 6];
        buf.read_to_slice(&mut content).unwrap();
        let read = String::from_utf8_lossy(&content);
        assert_eq!(", worl", read);
        // after read, buffer should still have 1 byte to read.
        assert_eq!(1, buf.remaining());
    }

    #[test]
    pub fn test_buffer_read() {
        let mut bytes = Bytes::from_static("hello".as_bytes());
        assert_eq!(5, bytes.remaining_size());
        assert_eq!(b'h', bytes.peek_u8_le().unwrap());
        bytes.advance_by(1);
        assert_eq!(4, bytes.remaining_size());
    }

    macro_rules! test_primitive_read_write {
        ( $($num_ty: ty), *) => {
            $(
                paste!{
                    #[test]
                    fn [<test_read_write_ $num_ty>]() {
                        assert_eq!($num_ty::MAX,(&mut $num_ty::MAX.to_le_bytes() as &[u8]).[<read_ $num_ty _le>]().unwrap());
                        assert_eq!($num_ty::MIN,(&mut $num_ty::MIN.to_le_bytes() as &[u8]).[<read_ $num_ty _le>]().unwrap());
                    }
                }
            )*
        }
    }

    test_primitive_read_write![u8, u16, u32, u64, i8, i16, i32, i64, f32, f64];

    #[test]
    pub fn test_read_write_from_slice_buffer() {
        let mut buf = "hello".as_bytes();
        assert_eq!(104, buf.peek_u8_le().unwrap());
        buf.advance_by(1);
        assert_eq!(101, buf.peek_u8_le().unwrap());
        buf.advance_by(1);
        assert_eq!(108, buf.peek_u8_le().unwrap());
        buf.advance_by(1);
        assert_eq!(108, buf.peek_u8_le().unwrap());
        buf.advance_by(1);
        assert_eq!(111, buf.peek_u8_le().unwrap());
        buf.advance_by(1);
        assert_matches!(buf.peek_u8_le(), Err(Overflow { .. }));
    }

    #[test]
    pub fn test_read_u8_from_slice_buffer() {
        let mut buf = "hello".as_bytes();
        assert_eq!(104, buf.read_u8_le().unwrap());
        assert_eq!(101, buf.read_u8_le().unwrap());
        assert_eq!(108, buf.read_u8_le().unwrap());
        assert_eq!(108, buf.read_u8_le().unwrap());
        assert_eq!(111, buf.read_u8_le().unwrap());
        assert_matches!(buf.read_u8_le(), Err(Overflow { .. }));
    }

    #[test]
    pub fn test_read_write_numbers() {
        let mut buf: Vec<u8> = vec![];
        buf.write_u64_le(1234).unwrap();
        assert_eq!(1234, (&buf[..]).read_u64_le().unwrap());

        buf.write_u32_le(4242).unwrap();
        let mut p = &buf[..];
        assert_eq!(1234, p.read_u64_le().unwrap());
        assert_eq!(4242, p.read_u32_le().unwrap());
    }

    macro_rules! test_primitive_vec_read_write {
        ( $($num_ty: ty), *) => {
            $(
                paste!{
                    #[test]
                    fn [<test_read_write_ $num_ty _from_vec_buffer>]() {
                        let mut buf = vec![];
                        let _ = buf.[<write_ $num_ty _le>]($num_ty::MAX).unwrap();
                        assert_eq!($num_ty::MAX, buf.as_slice().[<read_ $num_ty _le>]().unwrap());
                    }
                }
            )*
        }
    }

    test_primitive_vec_read_write![u8, u16, u32, u64, i8, i16, i32, i64, f32, f64];

    #[test]
    pub fn test_peek_write_from_vec_buffer() {
        let mut buf: Vec<u8> = vec![];
        buf.write_from_slice("hello".as_bytes()).unwrap();
        let mut slice = buf.as_slice();
        assert_eq!(104, slice.peek_u8_le().unwrap());
        slice.advance_by(1);
        assert_eq!(101, slice.peek_u8_le().unwrap());
        slice.advance_by(1);
        assert_eq!(108, slice.peek_u8_le().unwrap());
        slice.advance_by(1);
        assert_eq!(108, slice.peek_u8_le().unwrap());
        slice.advance_by(1);
        assert_eq!(111, slice.peek_u8_le().unwrap());
        slice.advance_by(1);
        assert_matches!(slice.read_u8_le(), Err(Overflow { .. }));
    }

    macro_rules! test_primitive_bytes_read_write {
        ( $($num_ty: ty), *) => {
            $(
                paste!{
                    #[test]
                    fn [<test_read_write_ $num_ty _from_bytes>]() {
                        let mut bytes = bytes::Bytes::from($num_ty::MAX.to_le_bytes().to_vec());
                        assert_eq!($num_ty::MAX, bytes.[<read_ $num_ty _le>]().unwrap());

                        let mut bytes = bytes::Bytes::from($num_ty::MIN.to_le_bytes().to_vec());
                        assert_eq!($num_ty::MIN, bytes.[<read_ $num_ty _le>]().unwrap());
                    }
                }
            )*
        }
    }

    test_primitive_bytes_read_write![u8, u16, u32, u64, i8, i16, i32, i64, f32, f64];

    #[test]
    pub fn test_write_overflow() {
        let mut buf = [0u8; 4];
        assert_matches!(
            (&mut buf[..]).write_from_slice("hell".as_bytes()),
            Ok { .. }
        );

        assert_matches!(
            (&mut buf[..]).write_from_slice("hello".as_bytes()),
            Err(common_base::buffer::Error::Overflow { .. })
        );
    }
}
