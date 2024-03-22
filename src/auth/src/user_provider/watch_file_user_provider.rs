use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;
use std::time::Duration;

use async_trait::async_trait;
use notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use snafu::ResultExt;

use common_telemetry::info;

use crate::{Identity, Password, UserInfoRef, UserProvider};
use crate::error::{FileWatchSnafu, Result};
use crate::user_provider::{authenticate_with_credential, load_credential_from_file};

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

        let (tx, rx) = channel::<DebounceEventResult>();

        let mut debouncer =
            new_debouncer(Duration::from_secs(1), None, tx).context(FileWatchSnafu)?;
        debouncer
            .watcher()
            .watch(Path::new(filepath), RecursiveMode::NonRecursive)
            .context(FileWatchSnafu)?;
        let filepath = filepath.to_string();
        std::thread::spawn(move || {
            let _hold = debouncer;
            while let Ok(res) = rx.recv() {
                if let Ok(events) = res {
                    if events
                        .iter()
                        .any(|e| matches!(e.kind, EventKind::Modify(_) | EventKind::Create(_)))
                    {
                        info!("User provider file {} changed", &filepath);
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
