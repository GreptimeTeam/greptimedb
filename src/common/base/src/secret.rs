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

use std::fmt::{Debug, Display};
use std::ops::Deref;

use serde::{Deserialize, Serialize};

macro_rules! impl_secret_display {
    ($secret_type: ty, $($display: ident),+) => {
        $(impl $display for $secret_type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "******")
            }
        })*
    };
}

impl_secret_display!(SecretString, Display, Debug);
impl_secret_display!(SecretBytes<'_>, Display, Debug);

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretString(String);

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        SecretString(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        SecretString(String::from(value))
    }
}

impl PartialEq<str> for SecretString {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<SecretString> for str {
    fn eq(&self, other: &SecretString) -> bool {
        self == other.0
    }
}

impl PartialEq<String> for SecretString {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

impl PartialEq<SecretString> for String {
    fn eq(&self, other: &SecretString) -> bool {
        self == &other.0
    }
}

impl Deref for SecretString {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretBytes<'a>(&'a [u8]);

impl<'a> From<&'a [u8]> for SecretBytes<'a> {
    fn from(value: &'a [u8]) -> Self {
        SecretBytes(value)
    }
}

impl PartialEq<[u8]> for SecretBytes<'_> {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

impl PartialEq<SecretBytes<'_>> for [u8] {
    fn eq(&self, other: &SecretBytes<'_>) -> bool {
        self == other.0
    }
}

impl Deref for SecretBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_string_display() {
        let password = "password";
        let secret_pwd = SecretString::from(password);

        assert_eq!("******", format!("{}", secret_pwd));
        assert_eq!("******", format!("{:#?}", secret_pwd));
    }

    #[test]
    fn test_secret_string_eq() {
        let secret_pwd = SecretString("password".to_string());

        assert_eq!("password", &secret_pwd);
        assert_eq!("password".to_string(), secret_pwd);
    }

    #[test]
    fn test_secret_string_deref() {
        let secret_str = SecretString::from("World!");
        let mut s = String::from("Hello,");
        s.push_str(&secret_str);

        assert_eq!("Hello,World!", s);
    }

    #[test]
    fn test_secret_string_len() {
        let secret_str = SecretString::from("hello");
        assert_eq!(5, secret_str.len());
        let secret_str = SecretString::from("");
        assert!(secret_str.is_empty());
    }

    fn check_secret_bytes_deref(expect: &[u8], given: &[u8]) {
        assert_eq!(expect, given);
    }

    #[test]
    fn test_secret_bytes_display() {
        let password = b"password";
        let secret_pwd = SecretBytes(password);

        assert_eq!("******", format!("{}", secret_pwd));
        assert_eq!("******", format!("{:#?}", secret_pwd));
    }

    #[test]
    fn test_secret_bytes_eq() {
        let password = "password";
        let secret_pwd = SecretBytes(password.as_bytes());

        assert_eq!(password.as_bytes(), &secret_pwd);
    }

    #[test]
    fn test_secret_bytes_deref() {
        let secret_bytes = SecretBytes("hello".as_bytes());

        check_secret_bytes_deref("hello".as_bytes(), &secret_bytes);
    }

    #[test]
    fn test_secret_bytes_len() {
        let secret_bytes = SecretBytes::from("hello".as_bytes());
        assert_eq!(5, secret_bytes.len());
        let secret_bytes = SecretBytes::from("".as_bytes());
        assert!(secret_bytes.is_empty());
    }

    #[test]
    fn test_secret_bytes_index() {
        let secret_bytes = SecretBytes::from("hello".as_bytes());
        assert_eq!(b'h', secret_bytes[0]);
    }
}
