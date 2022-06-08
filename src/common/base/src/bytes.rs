use std::ops::Deref;

use serde::Serialize;

/// Bytes buffer.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Bytes(Vec<u8>);

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Bytes {
        Bytes(bytes)
    }
}

impl From<&[u8]> for Bytes {
    fn from(bytes: &[u8]) -> Bytes {
        Bytes(bytes.to_vec())
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialEq<Vec<u8>> for Bytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        &self.0 == other
    }
}

impl PartialEq<Bytes> for Vec<u8> {
    fn eq(&self, other: &Bytes) -> bool {
        *self == other.0
    }
}

impl PartialEq<[u8]> for Bytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

impl PartialEq<Bytes> for [u8] {
    fn eq(&self, other: &Bytes) -> bool {
        self == other.0
    }
}

/// String buffer that can hold arbitrary encoding string (only support UTF-8 now).
///
/// Now this buffer is restricted to only hold valid UTF-8 string (only allow constructing `StringBytes`
/// from String or str). We may support other encoding in the future.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct StringBytes(Vec<u8>);

impl StringBytes {
    /// View this string as UTF-8 string slice.
    ///
    /// # Safety
    /// We only allow constructing `StringBytes` from String/str, so the inner
    /// buffer must holds valid UTF-8.
    pub fn as_utf8(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl From<String> for StringBytes {
    fn from(string: String) -> StringBytes {
        StringBytes(string.into_bytes())
    }
}

impl From<&str> for StringBytes {
    fn from(string: &str) -> StringBytes {
        StringBytes(string.as_bytes().to_vec())
    }
}

impl PartialEq<String> for StringBytes {
    fn eq(&self, other: &String) -> bool {
        self.0 == other.as_bytes()
    }
}

impl PartialEq<StringBytes> for String {
    fn eq(&self, other: &StringBytes) -> bool {
        self.as_bytes() == other.0
    }
}

impl PartialEq<str> for StringBytes {
    fn eq(&self, other: &str) -> bool {
        self.0 == other.as_bytes()
    }
}

impl PartialEq<StringBytes> for str {
    fn eq(&self, other: &StringBytes) -> bool {
        self.as_bytes() == other.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_bytes_deref(expect: &[u8], given: &[u8]) {
        assert_eq!(expect, given);
    }

    #[test]
    fn test_bytes_deref() {
        let hello = b"hello";
        let bytes = Bytes::from(hello.to_vec());
        check_bytes_deref(hello, &bytes);
    }

    #[test]
    fn test_bytes_from() {
        let hello = b"hello".to_vec();
        let bytes = Bytes::from(hello.clone());
        assert_eq!(hello, bytes);
        assert_eq!(bytes, hello);

        let world: &[u8] = b"world";
        let bytes = Bytes::from(world);
        assert_eq!(&bytes, world);
        assert_eq!(world, &bytes);
    }

    #[test]
    fn test_string_bytes_from() {
        let hello = "hello".to_string();
        let bytes = StringBytes::from(hello.clone());
        assert_eq!(hello, bytes);
        assert_eq!(bytes, hello);

        let world = "world";
        let bytes = StringBytes::from(world);
        assert_eq!(world, &bytes);
        assert_eq!(&bytes, world);
    }

    fn check_str(expect: &str, given: &str) {
        assert_eq!(expect, given);
    }

    #[test]
    fn test_as_utf8() {
        let hello = "hello";
        let bytes = StringBytes::from(hello);
        check_str(hello, bytes.as_utf8());
    }
}
