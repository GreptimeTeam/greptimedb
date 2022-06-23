#![feature(assert_matches)]

extern crate core;

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_base::buffer::Error::Overflow;
    use common_base::buffer::{Buffer, BufferMut};
    use paste::paste;

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
}
