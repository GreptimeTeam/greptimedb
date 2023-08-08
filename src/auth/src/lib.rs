mod common;
mod error;
mod user_info;
mod user_provider;

use std::sync::Arc;

pub use common::user_provider_from_option;
pub use error::Error;
pub use user_info::UserInfo;
pub use user_provider::{HashedPassword, Identity, Password, UserProvider};

pub type UserProviderRef = Arc<dyn UserProvider>;
