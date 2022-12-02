// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::pwd::sha1;
use super::AuthMethod;

pub enum PgAuthPlugin {
    PlainText,
}

pub fn auth_pg(
    auth_plugin: PgAuthPlugin,
    hashed_value: &[u8],
    _salt: &[u8],
    auth_method: &AuthMethod,
) -> bool {
    match auth_plugin {
        PgAuthPlugin::PlainText => match auth_method {
            AuthMethod::PlainText(plain_text) => plain_text == hashed_value,

            AuthMethod::DoubleSha1(val) => &sha1(&sha1(hashed_value))[..] == val,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{auth_pg, AuthMethod, PgAuthPlugin};
    use crate::auth::pwd::sha1;

    #[test]
    fn test_auth_pg() {
        let plain_text = b"123456";

        let auth_method = AuthMethod::PlainText(b"123456".to_vec());

        assert!(auth_pg(
            PgAuthPlugin::PlainText,
            plain_text,
            &[],
            &auth_method
        ));

        let auth_method = AuthMethod::DoubleSha1(sha1(&sha1(b"123456")).to_vec());
        assert!(auth_pg(
            PgAuthPlugin::PlainText,
            plain_text,
            &[],
            &auth_method
        ))
    }
}
