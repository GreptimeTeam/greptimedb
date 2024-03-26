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
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;

use async_trait::async_trait;
use notify::{EventKind, RecursiveMode, Watcher};
use snafu::ResultExt;

use common_telemetry::{info, warn};

use crate::{Identity, Password, UserInfoRef, UserProvider};
use crate::error::{FileWatchSnafu, Result};
use crate::user_info::DefaultUserInfo;
use crate::user_provider::{authenticate_with_credential, load_credential_from_file};

pub(crate) const WATCH_FILE_USER_PROVIDER: &str = "watch_file_user_provider";

type WatchedCredentialRef = Arc<Mutex<Option<HashMap<String, Vec<u8>>>>>;

/// A user provider that reads user credential from a file and watches the file for changes.
///
/// Empty file is invalid; but file not exist means every user can be authenticated.
pub(crate) struct WatchFileUserProvider {
    users: WatchedCredentialRef,
}

impl WatchFileUserProvider {
    pub fn new(filepath: &str) -> Result<Self> {
        let credential = load_credential_from_file(filepath)?;
        let users = Arc::new(Mutex::new(credential));
        let this = WatchFileUserProvider {
            users: users.clone(),
        };

        let (tx, rx) = channel::<notify::Result<notify::Event>>();
        let mut debouncer =
            notify::recommended_watcher(tx).context(FileWatchSnafu { path: "<none>" })?;
        debouncer
            .watch(Path::new(filepath), RecursiveMode::NonRecursive)
            .context(FileWatchSnafu { path: filepath })?;

        let filepath = filepath.to_string();
        std::thread::spawn(move || {
            let _hold = debouncer;
            while let Ok(res) = rx.recv() {
                if let Ok(event) = res {
                    if matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                    ) {
                        info!("User provider file {} changed", &filepath);
                        match load_credential_from_file(&filepath) {
                            Ok(credential) => {
                                *users.lock().expect("users credential must be valid") = credential;
                                info!("User provider file {filepath} reloaded")
                            }
                            Err(err) => {
                                warn!(
                                    ?err,
                                    "Fail to load credential from file {filepath}; keep the old one",
                                )
                            }
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
        if let Some(users) = users.as_ref() {
            authenticate_with_credential(users, id, password)
        } else {
            match id {
                Identity::UserId(id, _) => {
                    warn!(id, "User provider file is empty, allow all users");
                    Ok(DefaultUserInfo::with_name(id))
                }
            }
        }
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
    use std::time::{Duration, Instant};

    use tokio::time::sleep;

    use common_test_util::temp_dir::create_temp_dir;

    use crate::user_provider::{Identity, Password};
    use crate::user_provider::watch_file_user_provider::WatchFileUserProvider;
    use crate::UserProvider;

    async fn test_authenticate(
        provider: &dyn UserProvider,
        username: &str,
        password: &str,
        ok: bool,
        timeout: Option<Duration>,
    ) {
        if let Some(timeout) = timeout {
            let deadline = Instant::now().checked_add(timeout).unwrap();
            loop {
                let re = provider
                    .authenticate(
                        Identity::UserId(username, None),
                        Password::PlainText(password.to_string().into()),
                    )
                    .await;
                if re.is_ok() == ok {
                    break;
                } else if Instant::now() < deadline {
                    sleep(Duration::from_millis(100)).await;
                } else {
                    panic!("timeout (username: {username}, password: {password}, expected: {ok})");
                }
            }
        } else {
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
    }

    #[tokio::test]
    async fn test_file_provider() {
        let dir = create_temp_dir("test_file_provider");
        let file_path = format!("{}/test_file_provider", dir.path().to_str().unwrap());
        {
            // write a tmp file
            let file = File::create(&file_path).unwrap();
            let mut lw = LineWriter::new(file);
            assert!(writeln!(lw, "root=123456").is_ok());
            assert!(writeln!(lw, "admin=654321").is_ok());
            lw.flush().unwrap();
        }

        let provider = WatchFileUserProvider::new(file_path.as_str()).unwrap();
        test_authenticate(&provider, "root", "123456", true, None).await;
        test_authenticate(&provider, "admin", "654321", true, None).await;
        test_authenticate(&provider, "root", "654321", false, None).await;

        let timeout = Duration::from_secs(2);
        {
            // update the tmp file
            let file = File::create(&file_path).unwrap();
            let mut lw = LineWriter::new(file);
            assert!(writeln!(lw, "root=654321").is_ok());
            lw.flush().unwrap();
        }
        test_authenticate(&provider, "root", "123456", false, Some(timeout)).await;
        test_authenticate(&provider, "root", "654321", true, Some(timeout)).await;
        test_authenticate(&provider, "admin", "654321", false, Some(timeout)).await;

        {
            // remove the tmp file
            std::fs::remove_file(&file_path).unwrap();
        }
        sleep(Duration::from_secs(2)).await; // wait the watcher to apply the change
        test_authenticate(&provider, "root", "123456", true, Some(timeout)).await;
        test_authenticate(&provider, "root", "654321", true, Some(timeout)).await;
        test_authenticate(&provider, "admin", "654321", true, Some(timeout)).await;

        {
            // recreate the tmp file
            let file = File::create(&file_path).unwrap();
            let mut lw = LineWriter::new(file);
            assert!(writeln!(lw, "root=123456").is_ok());
            lw.flush().unwrap();
        }
        sleep(Duration::from_secs(2)).await; // wait the watcher to apply the change
        test_authenticate(&provider, "root", "123456", true, Some(timeout)).await;
        test_authenticate(&provider, "root", "654321", false, Some(timeout)).await;
        test_authenticate(&provider, "admin", "654321", false, Some(timeout)).await;
    }
}
