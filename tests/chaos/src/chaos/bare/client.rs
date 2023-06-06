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

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Result};

pub mod process;
pub mod recover;

pub type ClientRef = Arc<Client>;

#[derive(Debug)]
/// Client interacts with Chaosd server.
pub struct Client {
    host: String,
    inner: reqwest::Client,
}

impl Default for Client {
    fn default() -> Self {
        Client::new("http://127.0.0.1:31767".to_string())
    }
}

#[derive(Debug, Deserialize)]
pub struct Response {
    pub status: u32,
    pub message: String,
    pub uid: String,
}

impl Response {
    pub fn is_success(&self) -> bool {
        self.status == 200
    }
}

impl Client {
    pub fn new(host: String) -> Self {
        let inner = reqwest::Client::new();
        Client { inner, host }
    }

    fn url(&self, endpoint: &str) -> String {
        format!("{}{}", self.host, endpoint)
    }

    pub async fn post<T, K>(&self, endpoint: &str, json: &T) -> Result<K>
    where
        T: Serialize,
        K: DeserializeOwned,
    {
        self.inner
            .post(self.url(endpoint))
            .json(json)
            .send()
            .await
            .context(error::SendRequestSnafu)?
            .json::<K>()
            .await
            .context(error::DecodeResponseSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::Response;

    #[test]
    fn test_decode_response() {
        let resp = serde_json::from_str::<Response>(
            r#"{"status":200,"message":"attack successfully","uid":"a00cca2b-eba7-4716-86b3-3e66f94880f7"}"#,
        ).unwrap();

        assert_eq!(resp.status, 200);
        assert_eq!(resp.message, "attack successfully");
        assert_eq!(resp.uid, "a00cca2b-eba7-4716-86b3-3e66f94880f7");
    }
}
