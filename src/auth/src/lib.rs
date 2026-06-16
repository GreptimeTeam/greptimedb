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

mod common;
pub mod error;
mod permission;
mod user_info;
mod user_provider;

#[cfg(feature = "testing")]
pub mod tests;

pub use common::{
    DEFAULT_PBKDF2_SHA256_ITERATIONS, HashedPassword, Identity, MAX_PBKDF2_SHA256_ITERATIONS,
    MAX_PBKDF2_SHA256_SALT_LEN, PBKDF2_SHA256_HASH_LEN, PG_SCRAM_SHA256_KEY_LEN, Password,
    PgScramSha256Verifier, auth_mysql, format_mysql_native_password_verifier,
    format_pbkdf2_sha256_password_verifier, format_pg_scram_sha256_password_verifier,
    mysql_native_password_hash, static_user_provider_from_option, user_provider_from_option,
    userinfo_by_name,
};
pub use permission::{DefaultPermissionChecker, PermissionChecker, PermissionReq, PermissionResp};
pub use user_info::UserInfo;
pub use user_provider::static_user_provider::StaticUserProvider;
pub use user_provider::{PgAuthInfo, UserProvider};

/// pub type alias
pub type UserInfoRef = std::sync::Arc<dyn UserInfo>;
pub type UserProviderRef = std::sync::Arc<dyn UserProvider>;
pub type PermissionCheckerRef = std::sync::Arc<dyn PermissionChecker>;
