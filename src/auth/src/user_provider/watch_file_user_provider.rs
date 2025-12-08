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

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_config::file_watcher::{FileWatcherBuilder, FileWatcherConfig};
use common_telemetry::{info, warn};
use snafu::ResultExt;

use crate::error::{FileWatchSnafu, Result};
use crate::user_provider::{UserInfoMap, authenticate_with_credential, load_credential_from_file};
use crate::{Identity, Password, UserInfoRef, UserProvider};

pub(crate) const WATCH_FILE_USER_PROVIDER: &str = "watch_file_user_provider";

type WatchedCredentialRef = Arc<Mutex<UserInfoMap>>;

/// A user provider that reads user credential from a file and watches the file for changes.
///
/// Both empty file and non-existent file are invalid and will cause initialization to fail.
#[derive(Debug)]
pub(crate) struct WatchFileUserProvider {
    users: WatchedCredentialRef,
}

impl WatchFileUserProvider {
    pub fn new(filepath: &str) -> Result<Self> {
        let credential = load_credential_from_file(filepath)?;
        let users = Arc::new(Mutex::new(credential));

        let users_clone = users.clone();
        let filepath_owned = filepath.to_string();

        FileWatcherBuilder::new()
            .watch_path(filepath)
            .context(FileWatchSnafu)?
            .config(FileWatcherConfig::modify_and_create())
            .spawn(move || match load_credential_from_file(&filepath_owned) {
                Ok(credential) => {
                    let mut users = users_clone.lock().expect("users credential must be valid");
                    #[cfg(not(test))]
                    info!("User provider file {} reloaded", &filepath_owned);
                    #[cfg(test)]
                    info!(
                        "User provider file {} reloaded: {:?}",
                        &filepath_owned, credential
                    );
                    *users = credential;
                }
                Err(err) => {
                    warn!(
                        ?err,
                        "Fail to load credential from file {}; keep the old one", &filepath_owned
                    )
                }
            })
            .context(FileWatchSnafu)?;

        Ok(WatchFileUserProvider { users })
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
    use std::time::{Duration, Instant};

    use common_test_util::temp_dir::create_temp_dir;
    use tokio::time::sleep;

    use crate::UserProvider;
    use crate::user_provider::watch_file_user_provider::WatchFileUserProvider;
    use crate::user_provider::{Identity, Password};

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
    async fn test_file_provider_initialization_with_missing_file() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_missing_file");
        let file_path = format!("{}/non_existent_file", dir.path().to_str().unwrap());

        // Try to create provider with non-existent file should fail
        let result = WatchFileUserProvider::new(file_path.as_str());
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("UserProvider file must exist"));
    }

    #[tokio::test]
    async fn test_file_provider() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("test_file_provider");
        let file_path = format!("{}/test_file_provider", dir.path().to_str().unwrap());

        // write a tmp file
        assert!(std::fs::write(&file_path, "root=123456\nadmin=654321\n").is_ok());
        let provider = WatchFileUserProvider::new(file_path.as_str()).unwrap();
        let timeout = Duration::from_secs(60);

        test_authenticate(&provider, "root", "123456", true, None).await;
        test_authenticate(&provider, "admin", "654321", true, None).await;
        test_authenticate(&provider, "root", "654321", false, None).await;

        // update the tmp file
        assert!(std::fs::write(&file_path, "root=654321\n").is_ok());
        test_authenticate(&provider, "root", "123456", false, Some(timeout)).await;
        test_authenticate(&provider, "root", "654321", true, Some(timeout)).await;
        test_authenticate(&provider, "admin", "654321", false, Some(timeout)).await;

        // remove the tmp file
        assert!(std::fs::remove_file(&file_path).is_ok());
        // When file is deleted during runtime, keep the last known good credentials
        test_authenticate(&provider, "root", "654321", true, Some(timeout)).await;
        test_authenticate(&provider, "root", "123456", false, Some(timeout)).await;
        test_authenticate(&provider, "admin", "654321", false, Some(timeout)).await;

        // recreate the tmp file
        assert!(std::fs::write(&file_path, "root=123456\n").is_ok());
        test_authenticate(&provider, "root", "123456", true, Some(timeout)).await;
        test_authenticate(&provider, "root", "654321", false, Some(timeout)).await;
        test_authenticate(&provider, "admin", "654321", false, Some(timeout)).await;
    }
}
