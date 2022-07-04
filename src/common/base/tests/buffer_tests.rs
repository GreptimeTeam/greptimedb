#![feature(assert_matches)]

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use bytes::{Buf, BytesMut};
    use common_base::buffer::Error::Overflow;
    use common_base::buffer::{Buffer, BufferMut};
    use paste::paste;

    #[test]
    pub fn test_buffer_read_write() {
        let mut buf = BytesMut::with_capacity(16);
        buf.write_u64_le(1234u64).unwrap();
        let result = buf.read_u64_le().unwrap();
        assert_eq!(1234u64, result);

        buf.write_from_slice("hello, world".as_bytes()).unwrap();
        let mut content = vec![0u8; 5];
        buf.read_to_slice(&mut content).unwrap();
        let read = String::from_utf8_lossy(&content);
        assert_eq!("hello", read);

        // after read, buffer should still have 7 bytes to read.
        assert_eq!(7, buf.remaining());
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
        assert_eq!(104, buf.read_u8_le().unwrap());
        assert_eq!(101, buf.read_u8_le().unwrap());
        assert_eq!(108, buf.read_u8_le().unwrap());
        assert_eq!(108, buf.read_u8_le().unwrap());
        assert_eq!(111, buf.read_u8_le().unwrap());
        assert_matches!(buf.read_u8_le(), Err(Overflow { .. }));
    }

    macro_rules! test_primitive_vec_read_write {
        ( $($num_ty: ty), *) => {
            $(
                paste!{
                    #[test]
                    fn [<test_read_write_ $num_ty _from_vec_buffer>]() {
                        let mut buf = vec![];
                        assert!(buf.[<write_ $num_ty _le>]($num_ty::MAX).is_ok());
                        assert_eq!($num_ty::MAX, buf.as_slice().[<read_ $num_ty _le>]().unwrap());
                    }
                }
            )*
        }
    }

    test_primitive_vec_read_write![u8, u16, u32, u64, i8, i16, i32, i64, f32, f64];

    #[test]
    pub fn test_read_write_from_vec_buffer() {
        let mut buf: Vec<u8> = vec![];
        assert!(buf.write_from_slice("hello".as_bytes()).is_ok());
        let mut slice = buf.as_slice();
        assert_eq!(104, slice.read_u8_le().unwrap());
        assert_eq!(101, slice.read_u8_le().unwrap());
        assert_eq!(108, slice.read_u8_le().unwrap());
        assert_eq!(108, slice.read_u8_le().unwrap());
        assert_eq!(111, slice.read_u8_le().unwrap());
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
