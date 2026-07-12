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
use std::fmt::Debug;
use std::sync::Arc;

use crate::UserInfoRef;

pub trait UserInfo: Debug + Sync + Send {
    fn as_any(&self) -> &dyn Any;
    fn username(&self) -> &str;
}

/// The user permission mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermissionMode {
    #[default]
    ReadWrite,
    ReadOnly,
    WriteOnly,
}

impl PermissionMode {
    /// Parse permission mode from string.
    /// Supported values are:
    /// - "rw", "readwrite", "read_write" => ReadWrite
    /// - "ro", "readonly", "read_only" => ReadOnly
    /// - "wo", "writeonly", "write_only" => WriteOnly
    ///     Returns None if the input string is not a valid permission mode.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "readwrite" | "read_write" | "rw" => PermissionMode::ReadWrite,
            "readonly" | "read_only" | "ro" => PermissionMode::ReadOnly,
            "writeonly" | "write_only" | "wo" => PermissionMode::WriteOnly,
            _ => PermissionMode::ReadWrite,
        }
    }

    /// Convert permission mode to string.
    /// - ReadWrite => "rw"
    /// - ReadOnly => "ro"
    /// - WriteOnly => "wo"
    ///     The returned string is a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            PermissionMode::ReadWrite => "rw",
            PermissionMode::ReadOnly => "ro",
            PermissionMode::WriteOnly => "wo",
        }
    }

    /// Returns true if the permission mode allows read operations.
    pub fn can_read(&self) -> bool {
        matches!(self, PermissionMode::ReadWrite | PermissionMode::ReadOnly)
    }

    /// Returns true if the permission mode allows write operations.
    pub fn can_write(&self) -> bool {
        matches!(self, PermissionMode::ReadWrite | PermissionMode::WriteOnly)
    }
}

impl std::fmt::Display for PermissionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug)]
pub(crate) struct DefaultUserInfo {
    username: String,
    permission_mode: PermissionMode,
}

impl DefaultUserInfo {
    pub(crate) fn with_name(username: impl Into<String>) -> UserInfoRef {
        Self::with_name_and_permission(username, PermissionMode::default())
    }

    /// Create a UserInfo with specified permission mode.
    pub(crate) fn with_name_and_permission(
        username: impl Into<String>,
        permission_mode: PermissionMode,
    ) -> UserInfoRef {
        Arc::new(Self {
            username: username.into(),
            permission_mode,
        })
    }

    pub(crate) fn permission_mode(&self) -> &PermissionMode {
        &self.permission_mode
    }
}

impl UserInfo for DefaultUserInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn username(&self) -> &str {
        self.username.as_str()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_mode_from_str() {
        // Test ReadWrite variants
        assert_eq!(
            PermissionMode::from_str("readwrite"),
            PermissionMode::ReadWrite
        );
        assert_eq!(
            PermissionMode::from_str("read_write"),
            PermissionMode::ReadWrite
        );
        assert_eq!(PermissionMode::from_str("rw"), PermissionMode::ReadWrite);
        assert_eq!(
            PermissionMode::from_str("ReadWrite"),
            PermissionMode::ReadWrite
        );
        assert_eq!(PermissionMode::from_str("RW"), PermissionMode::ReadWrite);

        // Test ReadOnly variants
        assert_eq!(
            PermissionMode::from_str("readonly"),
            PermissionMode::ReadOnly
        );
        assert_eq!(
            PermissionMode::from_str("read_only"),
            PermissionMode::ReadOnly
        );
        assert_eq!(PermissionMode::from_str("ro"), PermissionMode::ReadOnly);
        assert_eq!(
            PermissionMode::from_str("ReadOnly"),
            PermissionMode::ReadOnly
        );
        assert_eq!(PermissionMode::from_str("RO"), PermissionMode::ReadOnly);

        // Test WriteOnly variants
        assert_eq!(
            PermissionMode::from_str("writeonly"),
            PermissionMode::WriteOnly
        );
        assert_eq!(
            PermissionMode::from_str("write_only"),
            PermissionMode::WriteOnly
        );
        assert_eq!(PermissionMode::from_str("wo"), PermissionMode::WriteOnly);
        assert_eq!(
            PermissionMode::from_str("WriteOnly"),
            PermissionMode::WriteOnly
        );
        assert_eq!(PermissionMode::from_str("WO"), PermissionMode::WriteOnly);

        // Test invalid inputs default to ReadWrite
        assert_eq!(
            PermissionMode::from_str("invalid"),
            PermissionMode::ReadWrite
        );
        assert_eq!(PermissionMode::from_str(""), PermissionMode::ReadWrite);
        assert_eq!(PermissionMode::from_str("xyz"), PermissionMode::ReadWrite);
    }

    #[test]
    fn test_permission_mode_as_str() {
        assert_eq!(PermissionMode::ReadWrite.as_str(), "rw");
        assert_eq!(PermissionMode::ReadOnly.as_str(), "ro");
        assert_eq!(PermissionMode::WriteOnly.as_str(), "wo");
    }

    #[test]
    fn test_permission_mode_default() {
        assert_eq!(PermissionMode::default(), PermissionMode::ReadWrite);
    }

    #[test]
    fn test_permission_mode_round_trip() {
        let modes = [
            PermissionMode::ReadWrite,
            PermissionMode::ReadOnly,
            PermissionMode::WriteOnly,
        ];

        for mode in modes {
            let str_repr = mode.as_str();
            let parsed = PermissionMode::from_str(str_repr);
            assert_eq!(mode, parsed);
        }
    }

    #[test]
    fn test_default_user_info_with_name() {
        let user_info = DefaultUserInfo::with_name("test_user");
        assert_eq!(user_info.username(), "test_user");
    }

    #[test]
    fn test_default_user_info_with_name_and_permission() {
        let user_info =
            DefaultUserInfo::with_name_and_permission("test_user", PermissionMode::ReadOnly);
        assert_eq!(user_info.username(), "test_user");

        // Cast to DefaultUserInfo to access permission_mode
        let default_user = user_info
            .as_any()
            .downcast_ref::<DefaultUserInfo>()
            .unwrap();
        assert_eq!(default_user.permission_mode, PermissionMode::ReadOnly);
    }

    #[test]
    fn test_user_info_as_any() {
        let user_info = DefaultUserInfo::with_name("test_user");
        let any_ref = user_info.as_any();
        assert!(any_ref.downcast_ref::<DefaultUserInfo>().is_some());
    }
}
