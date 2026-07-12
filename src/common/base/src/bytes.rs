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

use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Bytes buffer.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Bytes(bytes::Bytes);

impl From<Bytes> for bytes::Bytes {
    fn from(value: Bytes) -> Self {
        value.0
    }
}

impl From<bytes::Bytes> for Bytes {
    fn from(bytes: bytes::Bytes) -> Bytes {
        Bytes(bytes)
    }
}

impl From<&[u8]> for Bytes {
    fn from(bytes: &[u8]) -> Bytes {
        Bytes(bytes::Bytes::copy_from_slice(bytes))
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Bytes {
        Bytes(bytes::Bytes::from(bytes))
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(bytes: Bytes) -> Vec<u8> {
        bytes.0.into()
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
        self.0 == other
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
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StringBytes(String);

impl StringBytes {
    /// View this string as UTF-8 string slice.
    pub fn as_utf8(&self) -> &str {
        &self.0
    }

    /// Convert this string into owned UTF-8 string.
    pub fn into_string(self) -> String {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<String> for StringBytes {
    fn from(string: String) -> StringBytes {
        StringBytes(string)
    }
}

impl From<&str> for StringBytes {
    fn from(string: &str) -> StringBytes {
        StringBytes(string.to_string())
    }
}

impl PartialEq<String> for StringBytes {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

impl PartialEq<StringBytes> for String {
    fn eq(&self, other: &StringBytes) -> bool {
        self == &other.0
    }
}

impl PartialEq<str> for StringBytes {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}

impl PartialEq<StringBytes> for str {
    fn eq(&self, other: &StringBytes) -> bool {
        self == other.0
    }
}

impl Serialize for StringBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_utf8().serialize(serializer)
    }
}

// Custom Deserialize to ensure UTF-8 check is always done.
impl<'de> Deserialize<'de> for StringBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(StringBytes::from(s))
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
    fn test_bytes_len() {
        let hello = b"hello".to_vec();
        let bytes = Bytes::from(hello.clone());
        assert_eq!(bytes.len(), hello.len());

        let zero = b"".to_vec();
        let bytes = Bytes::from(zero);
        assert!(bytes.is_empty());
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

    #[test]
    fn test_string_bytes_len() {
        let hello = "hello".to_string();
        let bytes = StringBytes::from(hello.clone());
        assert_eq!(bytes.len(), hello.len());

        let zero = String::default();
        let bytes = StringBytes::from(zero);
        assert!(bytes.is_empty());
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
