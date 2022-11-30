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

use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::{ObjectStore, Result};

/// S3 temp folder guard, will try to delete the folder recursively when dropping it.
/// Only for test
pub struct S3TempFolderGuard {
    store: ObjectStore,
    // The path under root.
    path: String,
    tx: Option<Sender<Result<()>>>,
    rx: Option<Receiver<Result<()>>>,
}

impl S3TempFolderGuard {
    pub fn new(store: &ObjectStore, path: &str) -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            store: store.clone(),
            path: path.to_string(),
            tx: Some(tx),
            rx: Some(rx),
        }
    }

    pub async fn remove_all(&mut self) -> Result<()> {
        let store = self.store.clone();
        let path = self.path.clone();
        let tx = match self.tx.take() {
            Some(tx) => tx,
            None => return Ok(()),
        };

        common_runtime::spawn_bg(async move {
            let batch = store.batch();
            tx.send(batch.remove_all(&path).await).unwrap();
        });

        match self.rx.take() {
            Some(rx) => rx.await.expect("fail to recv"),
            _ => Ok(()),
        }
    }
}
