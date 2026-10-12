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

use common_meta::key::flow::flow_state::FlowStat;
use datatypes::value::Value;
use tokio::sync::{mpsc, oneshot};

use crate::error::InternalSnafu;

pub fn get_value_heap_size(value: &Value) -> usize {
    match value {
        Value::Binary(v) => v.len(),
        Value::String(v) => v.len(),
        Value::List(v) => v.items().iter().map(get_value_heap_size).sum(),
        _ => 0,
    }
}

#[derive(Clone)]
pub struct SizeReportSender {
    inner: mpsc::Sender<oneshot::Sender<FlowStat>>,
}
impl SizeReportSender {
    pub fn new() -> (Self, StateReportHandler) {
        let (tx, rx) = mpsc::channel(1);
        (Self { inner: tx }, rx)
    }
    pub async fn query(&self, timeout: std::time::Duration) -> crate::Result<FlowStat> {
        let (tx, rx) = oneshot::channel();
        self.inner.send(tx).await.map_err(|_| {
            InternalSnafu {
                reason: "failed to send size report request",
            }
            .build()
        })?;
        tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                InternalSnafu {
                    reason: "failed to receive size report after timeout",
                }
                .build()
            })?
            .map_err(|_| {
                InternalSnafu {
                    reason: "size report sender dropped",
                }
                .build()
            })
    }
}
pub type StateReportHandler = mpsc::Receiver<oneshot::Sender<FlowStat>>;
