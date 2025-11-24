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

use std::path::Path;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};

use common_telemetry::{error, info};
use notify::{EventKind, RecursiveMode, Watcher};
use snafu::ResultExt;

use crate::error::{FileWatchSnafu, Result};

/// A trait for loading TLS configuration from an option type
pub trait TlsConfigLoader<T> {
    type Error;

    /// Load the TLS configuration
    fn load(&self) -> StdResult<Option<T>, Self::Error>;

    /// Get paths to certificate files for watching
    fn watch_paths(&self) -> Vec<&Path>;

    /// Check if watching is enabled
    fn watch_enabled(&self) -> bool;
}

/// A mutable container for TLS config
///
/// This struct allows dynamic reloading of certificates and keys.
/// It's generic over the config type (e.g., ServerConfig, ClientTlsConfig)
/// and the option type (e.g., TlsOption, ClientTlsOption).
#[derive(Debug)]
pub struct ReloadableTlsConfig<T, O>
where
    O: TlsConfigLoader<T>,
{
    tls_option: O,
    config: RwLock<Option<T>>,
    version: AtomicUsize,
}

impl<T, O> ReloadableTlsConfig<T, O>
where
    O: TlsConfigLoader<T>,
{
    /// Create config by loading configuration from the option type
    pub fn try_new(tls_option: O) -> StdResult<Self, O::Error> {
        let config = tls_option.load()?;
        Ok(Self {
            tls_option,
            config: RwLock::new(config),
            version: AtomicUsize::new(0),
        })
    }

    /// Reread certificates and keys from file system.
    pub fn reload(&self) -> StdResult<(), O::Error> {
        let config = self.tls_option.load()?;
        *self.config.write().unwrap() = config;
        self.version.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get the config held by this container
    pub fn get_config(&self) -> Option<T>
    where
        T: Clone,
    {
        self.config.read().unwrap().clone()
    }

    /// Get associated option
    pub fn get_tls_option(&self) -> &O {
        &self.tls_option
    }

    /// Get version of current config
    ///
    /// this version will auto increase when config get reloaded.
    pub fn get_version(&self) -> usize {
        self.version.load(Ordering::Relaxed)
    }
}

/// Watch TLS configuration files for changes and reload automatically
///
/// This is a generic function that works with any ReloadableTlsConfig.
/// When changes are detected, it calls the provided callback after reloading.
///
/// T: the original TLS config
/// O: the compiled TLS option
/// F: the hook function to be called after reloading
/// E: the error type for the loading operation
pub fn maybe_watch_tls_config<T, O, F, E>(
    tls_config: Arc<ReloadableTlsConfig<T, O>>,
    on_reload: F,
) -> Result<()>
where
    T: Send + Sync + 'static,
    O: TlsConfigLoader<T, Error = E> + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
    F: Fn() + Send + 'static,
{
    if !tls_config.get_tls_option().watch_enabled() {
        return Ok(());
    }

    let tls_config_for_watcher = tls_config.clone();

    let (tx, rx) = channel::<notify::Result<notify::Event>>();
    let mut watcher = notify::recommended_watcher(tx).context(FileWatchSnafu { path: "<none>" })?;

    // Watch all paths returned by the TlsConfigLoader
    for path in tls_config.get_tls_option().watch_paths() {
        watcher
            .watch(path, RecursiveMode::NonRecursive)
            .with_context(|_| FileWatchSnafu {
                path: path.display().to_string(),
            })?;
    }

    info!("Spawning background task for watching TLS cert/key file changes");
    std::thread::spawn(move || {
        let _watcher = watcher;
        loop {
            match rx.recv() {
                Ok(Ok(event)) => {
                    if let EventKind::Modify(_) | EventKind::Create(_) = event.kind {
                        info!("Detected TLS cert/key file change: {:?}", event);
                        if let Err(err) = tls_config_for_watcher.reload() {
                            error!("Failed to reload TLS config: {}", err);
                        } else {
                            info!("Reloaded TLS cert/key file successfully.");
                            on_reload();
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("Failed to watch TLS cert/key file: {}", err);
                }
                Err(err) => {
                    error!("TLS cert/key file watcher channel closed: {}", err);
                }
            }
        }
    });

    Ok(())
}
