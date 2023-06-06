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

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::chaos::bare::client::{Client, Response};
use crate::error::Result;

const ATTACK_PROCESS_ENDPOINT: &str = "/api/attack/process";

/// Signal Currently, only SIGKILL, SIGTERM, and SIGSTOP are supported.
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum Signal {
    Kill = 9,
    Term = 15,
    Stop = 23,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AttackProcessRequest {
    process: String,
    signal: Signal,
}

impl Client {
    pub async fn attack_process(&self, process: i32, sig: Signal) -> Result<Response> {
        let req = AttackProcessRequest {
            process: format!("{}", process),
            signal: sig,
        };

        self.post::<_, Response>(ATTACK_PROCESS_ENDPOINT, &req)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::AttackProcessRequest;

    #[test]
    fn test_attack_process_request() {
        let req = AttackProcessRequest {
            process: "greptime".to_string(),
            signal: super::Signal::Kill,
        };

        let req = serde_json::to_string(&req).unwrap();
        let expected = r#"{"process":"greptime","signal":9}"#;
        assert_eq!(expected, req)
    }
}
