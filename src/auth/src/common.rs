use std::sync::Arc;

use snafu::OptionExt;

use crate::error::{InvalidConfigSnafu, Result};
use crate::user_provider::static_user_provider::{StaticUserProvider, STATIC_USER_PROVIDER};
use crate::UserProviderRef;

pub fn user_provider_from_option(opt: &String) -> Result<UserProviderRef> {
    let (name, content) = opt.split_once(':').context(InvalidConfigSnafu {
        value: opt.to_string(),
        msg: "UserProviderOption must be in format `<option>:<value>`",
    })?;
    match name {
        STATIC_USER_PROVIDER => {
            let provider =
                StaticUserProvider::try_from(content).map(|p| Arc::new(p) as UserProviderRef)?;
            Ok(provider)
        }
        _ => InvalidConfigSnafu {
            value: name.to_string(),
            msg: "Invalid UserProviderOption",
        }
        .fail(),
    }
}
