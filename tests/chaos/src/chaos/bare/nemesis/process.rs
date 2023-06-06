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

use snafu::{ensure, OptionExt};

use crate::chaos::bare::client::process::Signal;
use crate::chaos::bare::client::ClientRef;
use crate::chaos::nemesis::Nemesis;
use crate::cluster::bare::node::Node;
use crate::error::{self, Result};

#[derive(Debug)]
pub struct AttackProcess {
    signal: Signal,
    client_ref: ClientRef,
    // Returned by chaosd, used to recovers the attack.
    uid: Option<String>,
}

impl AttackProcess {
    pub fn new(client_ref: ClientRef, signal: Signal) -> Self {
        Self {
            signal,
            client_ref,
            uid: None,
        }
    }
}

const ATTACK_PROCESS_NEMESIS: &str = "attack.process";

#[async_trait::async_trait]
impl Nemesis<Node> for AttackProcess {
    async fn invoke(&mut self, node: &Node) -> Result<()> {
        ensure!(
            self.uid.is_none(),
            error::InvokedNemesisSnafu {
                msg: format!("{} {:?} already invoked", self.name(), node)
            }
        );

        let process = node.process.clone().context(error::UnknownProcessSnafu)?;
        let resp = self
            .client_ref
            .attack_process(process.pid, self.signal)
            .await?;

        if resp.is_success() {
            self.uid = Some(resp.uid);
            Ok(())
        } else {
            error::ErrorResponseSnafu { msg: resp.message }.fail()
        }
    }

    async fn recover(&mut self, _node: &Node) -> Result<()> {
        if let Some(uid) = self.uid.take() {
            let result = self.client_ref.recover(&uid).await?;
            ensure!(
                result,
                error::ErrorResponseSnafu {
                    msg: format!("failed to recover attack: {}", uid)
                }
            );
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        ATTACK_PROCESS_NEMESIS
    }
}
