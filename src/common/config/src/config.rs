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

use config::{Environment, File, FileFormat};
use serde::de::DeserializeOwned;
use serde::Serialize;
use snafu::ResultExt;

use crate::error::{LoadLayeredConfigSnafu, Result, SerdeJsonSnafu, TomlFormatSnafu};

/// Separator for environment variables. For example, `DATANODE__STORAGE__MANIFEST__CHECKPOINT_MARGIN`.
pub const ENV_VAR_SEP: &str = "__";

/// Separator for list values in environment variables. For example, `localhost:3001,localhost:3002,localhost:3003`.
pub const ENV_LIST_SEP: &str = ",";

/// Configuration trait defines the common interface for configuration that can be loaded from multiple sources and serialized to TOML.
pub trait Configurable: Serialize + DeserializeOwned + Default + Sized {
    /// Load the configuration from multiple sources and merge them.
    /// The precedence order is: config file > environment variables > default values.
    /// `env_prefix` is the prefix of environment variables, e.g. "FRONTEND__xxx".
    /// The function will use dunder(double underscore) `__` as the separator for environment variables, for example:
    /// `DATANODE__STORAGE__MANIFEST__CHECKPOINT_MARGIN` will be mapped to `DatanodeOptions.storage.manifest.checkpoint_margin` field in the configuration.
    /// `list_keys` is the list of keys that should be parsed as a list, for example, you can pass `Some(&["meta_client_options.metasrv_addrs"]` to parse `GREPTIMEDB_METASRV__META_CLIENT_OPTIONS__METASRV_ADDRS` as a list.
    /// The function will use comma `,` as the separator for list values, for example: `127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003`.
    fn load_layered_options(config_file: Option<&str>, env_prefix: &str) -> Result<Self> {
        let default_opts = Self::default();

        let env_source = {
            let mut env = Environment::default();

            if !env_prefix.is_empty() {
                env = env.prefix(env_prefix);
            }

            if let Some(list_keys) = Self::env_list_keys() {
                env = env.list_separator(ENV_LIST_SEP);
                for key in list_keys {
                    env = env.with_list_parse_key(key);
                }
            }

            env.try_parsing(true)
                .separator(ENV_VAR_SEP)
                .ignore_empty(true)
        };

        // Workaround: Replacement for `Config::try_from(&default_opts)` due to
        // `ConfigSerializer` cannot handle the case of an empty struct contained
        // within an iterative structure.
        // See: https://github.com/mehcode/config-rs/issues/461
        let json_str = serde_json::to_string(&default_opts).context(SerdeJsonSnafu)?;
        let default_config = File::from_str(&json_str, FileFormat::Json);

        // Add default values and environment variables as the sources of the configuration.
        let mut layered_config = config::Config::builder()
            .add_source(default_config)
            .add_source(env_source);

        // Add config file as the source of the configuration if it is specified.
        if let Some(config_file) = config_file {
            layered_config = layered_config.add_source(File::new(config_file, FileFormat::Toml));
        }

        let mut opts: Self = layered_config
            .build()
            .and_then(|x| x.try_deserialize())
            .context(LoadLayeredConfigSnafu)?;

        opts.validate_sanitize()?;

        Ok(opts)
    }

    /// Validate(and possibly sanitize) the configuration.
    fn validate_sanitize(&mut self) -> Result<()> {
        Ok(())
    }

    /// List of toml keys that should be parsed as a list.
    fn env_list_keys() -> Option<&'static [&'static str]> {
        None
    }

    /// Serialize the configuration to a TOML string.
    fn to_toml(&self) -> Result<String> {
        toml::to_string(&self).context(TomlFormatSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use common_telemetry::logging::LoggingOptions;
    use common_test_util::temp_dir::create_named_temp_file;
    use common_wal::config::DatanodeWalConfig;
    use datanode::config::{ObjectStoreConfig, StorageConfig};
    use meta_client::MetaClientOptions;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, Default)]
    struct TestDatanodeConfig {
        node_id: Option<u64>,
        logging: LoggingOptions,
        meta_client: Option<MetaClientOptions>,
        wal: DatanodeWalConfig,
        storage: StorageConfig,
    }

    impl Configurable for TestDatanodeConfig {
        fn env_list_keys() -> Option<&'static [&'static str]> {
            Some(&["meta_client.metasrv_addrs"])
        }
    }

    #[test]
    fn test_load_layered_options() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            enable_memory_catalog = false
            rpc_addr = "127.0.0.1:3001"
            rpc_hostname = "127.0.0.1"
            rpc_runtime_size = 8
            mysql_addr = "127.0.0.1:4406"
            mysql_runtime_size = 2

            [meta_client]
            timeout = "3s"
            connect_timeout = "5s"
            tcp_nodelay = true

            [wal]
            provider = "raft_engine"
            dir = "./greptimedb_data/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [logging]
            level = "debug"
            dir = "./greptimedb_data/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let env_prefix = "DATANODE_UT";
        temp_env::with_vars(
            // The following environment variables will be used to override the values in the config file.
            [
                (
                    // storage.type = S3
                    [
                        env_prefix.to_string(),
                        "storage".to_uppercase(),
                        "type".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("S3"),
                ),
                (
                    // storage.bucket = mybucket
                    [
                        env_prefix.to_string(),
                        "storage".to_uppercase(),
                        "bucket".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("mybucket"),
                ),
                (
                    // wal.dir = /other/wal/dir
                    [
                        env_prefix.to_string(),
                        "wal".to_uppercase(),
                        "dir".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("/other/wal/dir"),
                ),
                (
                    // meta_client.metasrv_addrs = 127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003
                    [
                        env_prefix.to_string(),
                        "meta_client".to_uppercase(),
                        "metasrv_addrs".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003"),
                ),
            ],
            || {
                let opts = TestDatanodeConfig::load_layered_options(
                    Some(file.path().to_str().unwrap()),
                    env_prefix,
                )
                .unwrap();

                // Check the configs from environment variables.
                match &opts.storage.store {
                    ObjectStoreConfig::S3(s3_config) => {
                        assert_eq!(s3_config.bucket, "mybucket".to_string());
                    }
                    _ => panic!("unexpected store type"),
                }
                assert_eq!(
                    opts.meta_client.unwrap().metasrv_addrs,
                    vec![
                        "127.0.0.1:3001".to_string(),
                        "127.0.0.1:3002".to_string(),
                        "127.0.0.1:3003".to_string()
                    ]
                );

                // Should be the values from config file, not environment variables.
                let DatanodeWalConfig::RaftEngine(raft_engine_config) = opts.wal else {
                    unreachable!()
                };
                assert_eq!(raft_engine_config.dir.unwrap(), "./greptimedb_data/wal");

                // Should be default values.
                assert_eq!(opts.node_id, None);
            },
        );
    }
}
