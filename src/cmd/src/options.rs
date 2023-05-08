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

use common_telemetry::logging::LoggingOptions;
use config::{Config, Environment, File, FileFormat};
use datanode::datanode::DatanodeOptions;
use frontend::frontend::FrontendOptions;
use meta_srv::metasrv::MetaSrvOptions;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{LoadConfigSnafu, Result};

pub struct MixOptions {
    pub fe_opts: FrontendOptions,
    pub dn_opts: DatanodeOptions,
    pub logging: LoggingOptions,
}

pub enum Options {
    Datanode(Box<DatanodeOptions>),
    Frontend(Box<FrontendOptions>),
    Metasrv(Box<MetaSrvOptions>),
    Standalone(Box<MixOptions>),
    Cli(Box<LoggingOptions>),
}

#[derive(Clone, Debug, Default)]
pub struct TopLevelOptions {
    pub log_dir: Option<String>,
    pub log_level: Option<String>,
}

impl Options {
    pub fn logging_options(&self) -> &LoggingOptions {
        match self {
            Options::Datanode(opts) => &opts.logging,
            Options::Frontend(opts) => &opts.logging,
            Options::Metasrv(opts) => &opts.logging,
            Options::Standalone(opts) => &opts.logging,
            Options::Cli(opts) => opts,
        }
    }

    /// Load the configuration from multiple sources and merge them.
    /// The precedence order is: environment variables > config file > default function.
    /// `env_vars_prefix` is the prefix of environment variables, e.g. "FRONTEND-xxx".
    /// The function will use `.` as the separator for environment variables, for example:
    /// `DATANODE-STORAGE.MANIFEST.CHECKPOINT_MARGIN` will be mapped to `DatanodeOptions.storage.manifest.checkpoint_margin` field in the configuration.
    pub fn load_layered_options<'de, T: Serialize + Deserialize<'de> + Default>(
        config_file: Option<String>,
        env_vars_prefix: &str,
    ) -> Result<T> {
        let default_opts = T::default();
        let env_source = Environment::with_prefix(env_vars_prefix)
            .try_parsing(true)
            .prefix_separator("-")
            .separator(".")
            .ignore_empty(true);

        let opts = if let Some(config_file) = config_file {
            Config::builder()
                .add_source(Config::try_from(&default_opts).context(LoadConfigSnafu)?)
                .add_source(File::new(&config_file, FileFormat::Toml))
                .add_source(env_source)
                .build()
                .context(LoadConfigSnafu)?
                .try_deserialize()
                .context(LoadConfigSnafu)?
        } else {
            Config::builder()
                .add_source(Config::try_from(&default_opts).context(LoadConfigSnafu)?)
                .add_source(env_source)
                .build()
                .context(LoadConfigSnafu)?
                .try_deserialize()
                .context(LoadConfigSnafu)?
        };

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use common_test_util::temp_dir::create_named_temp_file;
    use datanode::datanode::{DatanodeOptions, ObjectStoreConfig};

    use super::*;

    #[test]
    fn test_load_layered_options() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "distributed"
            enable_memory_catalog = false
            rpc_addr = "127.0.0.1:3001"
            rpc_hostname = "127.0.0.1"
            rpc_runtime_size = 8
            mysql_addr = "127.0.0.1:4406"
            mysql_runtime_size = 2

            [meta_client_options]
            metasrv_addrs = ["127.0.0.1:3002"]
            timeout_millis = 3000
            connect_timeout_millis = 5000
            tcp_nodelay = true

            [wal]
            dir = "/tmp/greptimedb/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
            type = "File"
            data_dir = "/tmp/greptimedb/data/"

            [storage.compaction]
            max_inflight_tasks = 3
            max_files_in_level0 = 7
            max_purge_tasks = 32

            [storage.manifest]
            checkpoint_margin = 9
            gc_duration = '7s'
            checkpoint_on_startup = true

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        temp_env::with_vars(
            // The following environment variables will be used to override the values in the config file.
            vec![
                ("DATANODE_UT-STORAGE.MANIFEST.CHECKPOINT_MARGIN", Some("99")),
                ("DATANODE_UT-STORAGE.TYPE", Some("S3")),
                ("DATANODE_UT-STORAGE.BUCKET", Some("mybucket")),
                ("DATANODE_UT-STORAGE.MANIFEST.GC_DURATION", Some("42s")),
                (
                    "DATANODE_UT-STORAGE.MANIFEST.CHECKPOINT_ON_STARTUP",
                    Some("true"),
                ),
            ],
            || {
                let opts: DatanodeOptions = Options::load_layered_options(
                    Some(file.path().to_str().unwrap().to_string()),
                    "DATANODE_UT",
                )
                .unwrap();

                // Check the values from environment variables.
                assert_eq!(opts.storage.manifest.checkpoint_margin, Some(99));
                match opts.storage.store {
                    ObjectStoreConfig::S3(s3_config) => {
                        assert_eq!(s3_config.bucket, "mybucket".to_string());
                    }
                    _ => panic!("unexpected store type"),
                }
                assert_eq!(
                    opts.storage.manifest.gc_duration,
                    Some(Duration::from_secs(42))
                );
                assert_eq!(opts.storage.manifest.checkpoint_on_startup, true);

                // Should be the values from config file.
                assert_eq!(opts.wal.dir, "/tmp/greptimedb/wal".to_string());

                // Should be default values.
                assert_eq!(opts.node_id, None);
            },
        );
    }
}
