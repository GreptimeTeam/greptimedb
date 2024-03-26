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

use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::info;
use notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::{new_debouncer, DebounceEventResult};
use snafu::ResultExt;

use crate::error::{FileWatchSnafu, Result};
use crate::user_provider::{authenticate_with_credential, load_credential_from_file};
use crate::{Identity, Password, UserInfoRef, UserProvider};

pub(crate) const WATCH_FILE_USER_PROVIDER: &str = "watch_file_user_provider";

/// A user provider that reads user credential from a file and watches the file for changes.
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
        let mut debouncer = new_debouncer(Duration::from_secs(1), None, tx)
            .context(FileWatchSnafu { path: None })?;
        debouncer
            .watcher()
            .watch(Path::new(filepath), RecursiveMode::NonRecursive)
            .context(FileWatchSnafu {
                path: Some(filepath),
            })?;

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

#[cfg(test)]
pub mod test {
    use std::fs::File;
    use std::io::{LineWriter, Write};
    use std::time::Duration;

    use common_test_util::temp_dir::create_temp_dir;
    use tokio::time::sleep;

    use crate::user_provider::watch_file_user_provider::WatchFileUserProvider;
    use crate::user_provider::{Identity, Password};
    use crate::UserProvider;

    async fn test_authenticate(
        provider: &dyn UserProvider,
        username: &str,
        password: &str,
        ok: bool,
    ) {
        let re = provider
            .authenticate(
                Identity::UserId(username, None),
                Password::PlainText(password.to_string().into()),
            )
            .await;
        assert_eq!(
            re.is_ok(),
            ok,
            "username: {}, password: {}",
            username,
            password
        );
    }

    #[tokio::test]
    async fn test_file_provider() {
        let dir = create_temp_dir("test_file_provider");
        let file_path = format!("{}/test_file_provider", dir.path().to_str().unwrap());
        {
            // write a tmp file
            let file = File::create(&file_path).unwrap();
            let mut lw = LineWriter::new(file);
            assert!(lw
                .write_all(
                    b"root=123456
admin=654321",
                )
                .is_ok());
            lw.flush().unwrap();
        }

        let provider = WatchFileUserProvider::new(file_path.as_str()).unwrap();
        test_authenticate(&provider, "root", "123456", true).await;
        test_authenticate(&provider, "admin", "654321", true).await;
        test_authenticate(&provider, "root", "654321", false).await;

        {
            // update the tmp file
            let file = File::create(&file_path).unwrap();
            let mut lw = LineWriter::new(file);
            assert!(lw.write_all(b"root=654321",).is_ok());
            lw.flush().unwrap();
        }
        sleep(Duration::from_secs(2)).await; // wait the watcher to apply the change
        test_authenticate(&provider, "root", "654321", true).await;
        test_authenticate(&provider, "admin", "654321", false).await;
    }
}
