use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_telemetry::info;
use notify::{EventKind, RecursiveMode, Watcher};
use snafu::ResultExt;

use crate::error::{FileWatchSnafu, Result};
use crate::user_provider::{authenticate_with_credential, load_credential_from_file};
use crate::{Identity, Password, UserInfoRef, UserProvider};

pub(crate) const WATCH_FILE_USER_PROVIDER: &str = "watch_file_user_provider";

pub(crate) struct WatchFileUserProvider {
    users: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl WatchFileUserProvider {
    pub fn new(filepath: &str) -> Result<Self> {
        let credential = load_credential_from_file(filepath)?;
        let users = Arc::new(Mutex::new(credential));
        let this = WatchFileUserProvider {
            users: users.clone(),
        };

        let (tx, rx) = channel::<notify::Result<notify::Event>>();
        let mut watcher = notify::recommended_watcher(tx).context(FileWatchSnafu)?;
        watcher
            .watch(Path::new(filepath), RecursiveMode::NonRecursive)
            .context(FileWatchSnafu)?;
        let filepath = filepath.to_string();
        std::thread::spawn(move || {
            let _watcher = watcher;
            while let Ok(res) = rx.recv() {
                if let Ok(event) = res {
                    if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
                        info!("Detected user provider file change: {:?}", event);
                        if let Ok(credential) = load_credential_from_file(&filepath) {
                            *users.lock().expect("users credential must be valid") = credential;
                        }
                    }
                }
            }
        });

        Ok(this)
    }
}

#[async_trait]
impl UserProvider for WatchFileUserProvider {
    fn name(&self) -> &str {
        WATCH_FILE_USER_PROVIDER
    }

    async fn authenticate(&self, id: Identity<'_>, password: Password<'_>) -> Result<UserInfoRef> {
        let users = self.users.lock().expect("users credential must be valid");
        authenticate_with_credential(&users, id, password)
    }

    async fn authorize(&self, _: &str, _: &str, _: &UserInfoRef) -> Result<()> {
        // default allow all
        Ok(())
    }
}
