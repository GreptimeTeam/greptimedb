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
