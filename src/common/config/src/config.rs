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
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::ResultExt;

use crate::error::{
    LoadLayeredConfigSnafu, Result, SerdeJsonSnafu, TaggedSeqNotTopLevelSnafu, TomlFormatSnafu,
};
use crate::tagged_seq::TaggedSeqEnvSource;

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
                .ignore_empty(false)
        };

        // Workaround: Replacement for `Config::try_from(&default_opts)` due to
        // `ConfigSerializer` cannot handle the case of an empty struct contained
        // within an iterative structure.
        // See: https://github.com/mehcode/config-rs/issues/461
        let json_str = serde_json::to_string(&default_opts).context(SerdeJsonSnafu)?;
        let default_config = File::from_str(&json_str, FileFormat::Json);

        // Add default values and environment variables as the sources of the configuration.
        let mut layered_config = config::Config::builder().add_source(default_config);

        // A sequence of tagged tables cannot be addressed by plain dotted env keys: the path
        // walk of `config` drops the default sequence. Such keys are merged onto the default
        // entries by tag name instead.
        layered_config = match Self::env_tagged_seq_keys() {
            Some(keys) if !keys.is_empty() => {
                // The default entries are looked up by literal key, so a dotted key would find
                // none of them. Reject it instead of silently dropping the defaults.
                if let Some(key) = keys.iter().find(|key| key.contains('.')) {
                    return TaggedSeqNotTopLevelSnafu {
                        key: (*key).to_string(),
                    }
                    .fail();
                }

                let defaults = serde_json::to_value(&default_opts).context(SerdeJsonSnafu)?;
                layered_config.add_source(TaggedSeqEnvSource::new(env_source, keys, &defaults))
            }
            _ => layered_config.add_source(env_source),
        };

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

    /// Keys whose value is a sequence of single-key tagged tables (e.g. `region_engine`,
    /// serialized from `Vec<Enum>` with externally tagged variants). Environment variables
    /// address them by tag name (`KEY__<TAG>__<FIELD>`) instead of numeric index, and the
    /// overrides are merged onto the corresponding default entries.
    ///
    /// Keys must name a **top-level** field of the options struct: the default entries to
    /// merge onto are looked up by literal key in the serialized defaults, so a dotted key is
    /// rejected instead of silently leaving the defaults out of the merged sequence.
    ///
    /// A configuration file that configures such a key as a whole replaces the merged value,
    /// so the environment variables for it are ineffective then and the file has to spell out
    /// every entry it wants.
    fn env_tagged_seq_keys() -> Option<&'static [&'static str]> {
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
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use common_telemetry::logging::LoggingOptions;
    use common_test_util::temp_dir::create_named_temp_file;
    use common_wal::config::DatanodeWalConfig;
    use datanode::config::StorageConfig;
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
                    object_store::config::ObjectStoreConfig::S3(s3_config) => {
                        assert_eq!(s3_config.connection.bucket, "mybucket".to_string());
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

    #[derive(Debug, Serialize, Deserialize, Default)]
    struct SimpleConfig {
        name: Option<String>,
        prefix: Option<String>,
    }

    impl Configurable for SimpleConfig {}

    #[test]
    fn test_empty_env_var_is_not_ignored() {
        let env_prefix = "SIMPLE_CFG_UT";
        temp_env::with_vars(
            [(
                [env_prefix.to_string(), "PREFIX".to_string()].join(ENV_VAR_SEP),
                Some(""),
            )],
            || {
                let opts = SimpleConfig::load_layered_options(None, env_prefix).unwrap();
                // With ignore_empty(false), an empty env var should yield Some("")
                // rather than None (which was the previous behavior with ignore_empty(true)).
                assert_eq!(opts.prefix, Some("".to_string()));
                // Unset env var should remain None.
                assert_eq!(opts.name, None);
            },
        );
    }

    /// A sequence of single-key tagged tables, the shape of `Vec<Enum>` with externally
    /// tagged variants.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    struct TestIndexConfig {
        aux_path: String,
        cache_percent: u8,
    }

    impl Default for TestIndexConfig {
        fn default() -> Self {
            Self {
                aux_path: "/default/aux".to_string(),
                cache_percent: 30,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    struct TestMitoConfig {
        global_write_buffer_size: ReadableSize,
        global_write_buffer_reject_size: ReadableSize,
        index: TestIndexConfig,
    }

    impl Default for TestMitoConfig {
        fn default() -> Self {
            Self {
                global_write_buffer_size: ReadableSize::mb(64),
                global_write_buffer_reject_size: ReadableSize::gb(2),
                index: TestIndexConfig::default(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    struct TestFileConfig {
        data_dir: String,
    }

    impl Default for TestFileConfig {
        fn default() -> Self {
            Self {
                data_dir: "/default/data".to_string(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    struct TestMetricConfig {
        #[serde(with = "humantime_serde")]
        flush_metadata_region_interval: Duration,
    }

    impl Default for TestMetricConfig {
        fn default() -> Self {
            Self {
                flush_metadata_region_interval: Duration::from_secs(30),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum TestRegionEngine {
        Mito(TestMitoConfig),
        File(TestFileConfig),
        Metric(TestMetricConfig),
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestTaggedSeqConfig {
        region_engine: Vec<TestRegionEngine>,
    }

    impl Default for TestTaggedSeqConfig {
        fn default() -> Self {
            Self {
                region_engine: vec![
                    TestRegionEngine::Mito(TestMitoConfig::default()),
                    TestRegionEngine::File(TestFileConfig::default()),
                ],
            }
        }
    }

    impl Configurable for TestTaggedSeqConfig {
        fn env_tagged_seq_keys() -> Option<&'static [&'static str]> {
            Some(&["region_engine"])
        }
    }

    /// A config whose tagged sequence key is not a top-level field.
    #[derive(Debug, Serialize, Deserialize, Default)]
    struct TestDottedKeyConfig {
        region_engine: Vec<TestRegionEngine>,
    }

    impl Configurable for TestDottedKeyConfig {
        fn env_tagged_seq_keys() -> Option<&'static [&'static str]> {
            Some(&["region_engine.mito"])
        }
    }

    fn test_mito(config: &TestTaggedSeqConfig) -> &TestMitoConfig {
        config
            .region_engine
            .iter()
            .find_map(|engine| match engine {
                TestRegionEngine::Mito(config) => Some(config),
                _ => None,
            })
            .expect("the mito entry")
    }

    fn test_metric(config: &TestTaggedSeqConfig) -> &TestMetricConfig {
        config
            .region_engine
            .iter()
            .find_map(|engine| match engine {
                TestRegionEngine::Metric(config) => Some(config),
                _ => None,
            })
            .expect("the metric entry")
    }

    /// Loads with the given prefix and returns the message of the error, including the
    /// source of the layered loading error.
    fn load_error_text(env_prefix: &str) -> String {
        let err = TestTaggedSeqConfig::load_layered_options(None, env_prefix).unwrap_err();
        let mut text = err.to_string();
        let mut source = std::error::Error::source(&err);
        while let Some(error) = source {
            text.push_str(&format!(": {error}"));
            source = std::error::Error::source(error);
        }
        text
    }

    #[test]
    fn test_tagged_seq_env_overrides_a_field() {
        let env_prefix = "TAGGED_SEQ_FIELD_UT";
        let env_key = [
            env_prefix,
            "REGION_ENGINE",
            "MITO",
            "GLOBAL_WRITE_BUFFER_REJECT_SIZE",
        ]
        .join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("4GB"), || {
            let opts = TestTaggedSeqConfig::load_layered_options(None, env_prefix).unwrap();
            let defaults = TestTaggedSeqConfig::default();

            // The entries of the sequence that the environment does not mention are intact.
            assert_eq!(opts.region_engine.len(), defaults.region_engine.len());
            assert_eq!(opts.region_engine[1], defaults.region_engine[1]);

            // The addressed field wins, the fields next to it keep their default value.
            let mito = test_mito(&opts);
            assert_eq!(mito.global_write_buffer_reject_size, ReadableSize::gb(4));
            assert_eq!(
                mito.global_write_buffer_size,
                TestMitoConfig::default().global_write_buffer_size
            );
            assert_eq!(mito.index, TestIndexConfig::default());
        });
    }

    #[test]
    fn test_tagged_seq_env_appends_an_entry() {
        let env_prefix = "TAGGED_SEQ_APPEND_UT";
        let env_key = [
            env_prefix,
            "REGION_ENGINE",
            "METRIC",
            "FLUSH_METADATA_REGION_INTERVAL",
        ]
        .join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("1m"), || {
            let opts = TestTaggedSeqConfig::load_layered_options(None, env_prefix).unwrap();
            let defaults = TestTaggedSeqConfig::default();

            // The entry is not in the default sequence, so it is appended.
            assert_eq!(opts.region_engine.len(), defaults.region_engine.len() + 1);
            assert_eq!(opts.region_engine[0], defaults.region_engine[0]);
            assert_eq!(opts.region_engine[1], defaults.region_engine[1]);
            assert_eq!(
                test_metric(&opts).flush_metadata_region_interval,
                Duration::from_secs(60)
            );
        });
    }

    #[test]
    fn test_tagged_seq_env_overrides_a_nested_field() {
        let env_prefix = "TAGGED_SEQ_NESTED_UT";
        let env_key = [env_prefix, "REGION_ENGINE", "MITO", "INDEX", "AUX_PATH"].join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("/tmp/aux"), || {
            let opts = TestTaggedSeqConfig::load_layered_options(None, env_prefix).unwrap();

            let mito = test_mito(&opts);
            assert_eq!(mito.index.aux_path, "/tmp/aux");
            assert_eq!(
                mito.index.cache_percent,
                TestIndexConfig::default().cache_percent
            );
            assert_eq!(
                mito.global_write_buffer_size,
                TestMitoConfig::default().global_write_buffer_size
            );
        });
    }

    #[test]
    fn test_tagged_seq_env_rejects_whole_key() {
        let env_prefix = "TAGGED_SEQ_NO_TAG_UT";
        let env_key = [env_prefix, "REGION_ENGINE"].join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("mito"), || {
            let text = load_error_text(env_prefix);
            assert!(text.contains("region_engine"), "{text}");
            assert!(text.contains("region_engine__<TAG>__<FIELD>"), "{text}");
        });
    }

    #[test]
    fn test_tagged_seq_env_rejects_numeric_index() {
        let env_prefix = "TAGGED_SEQ_NUMERIC_UT";
        let env_key = [
            env_prefix,
            "REGION_ENGINE",
            "0",
            "MITO",
            "GLOBAL_WRITE_BUFFER_SIZE",
        ]
        .join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("4GB"), || {
            let text = load_error_text(env_prefix);
            assert!(
                text.contains("region_engine.0.mito.global_write_buffer_size"),
                "{text}"
            );
            assert!(text.contains("addressed by tag name"), "{text}");
        });
    }

    #[test]
    fn test_tagged_seq_env_rejects_scalar_and_field_under_same_key() {
        let env_prefix = "TAGGED_SEQ_NOT_A_TABLE_UT";
        let whole_key = [env_prefix, "REGION_ENGINE", "MITO", "INDEX"].join(ENV_VAR_SEP);
        let below_key =
            [env_prefix, "REGION_ENGINE", "MITO", "INDEX", "AUX_PATH"].join(ENV_VAR_SEP);

        temp_env::with_vars(
            [
                (whole_key, Some("not-a-table")),
                (below_key, Some("/tmp/aux")),
            ],
            || {
                // One variable sets `index` as a whole, another sets a field below it. The
                // conflict is reported as such instead of failing later inside serde with a
                // type error that does not name the variables involved.
                let text = load_error_text(env_prefix);
                assert!(
                    text.contains("another environment variable sets 'region_engine.mito.index'"),
                    "{text}"
                );
                assert!(text.contains("region_engine.mito.index.aux_path"), "{text}");
            },
        );
    }

    #[test]
    fn test_tagged_seq_env_rejects_signed_and_float_indices() {
        for index in ["-1", "+1", "1e3"] {
            let env_prefix = "TAGGED_SEQ_INDEX_SHAPE_UT";
            let env_key = [
                env_prefix,
                "REGION_ENGINE",
                index,
                "MITO",
                "GLOBAL_WRITE_BUFFER_SIZE",
            ]
            .join(ENV_VAR_SEP);

            temp_env::with_var(env_key, Some("4GB"), || {
                let text = load_error_text(env_prefix);
                assert!(text.contains("addressed by tag name"), "{index}: {text}");
            });
        }
    }

    #[test]
    fn test_tagged_seq_env_rejects_tag_without_field() {
        let env_prefix = "TAGGED_SEQ_NO_FIELD_UT";
        let env_key = [env_prefix, "REGION_ENGINE", "MITO"].join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("mito"), || {
            let text = load_error_text(env_prefix);
            assert!(text.contains("region_engine.mito"), "{text}");
            assert!(text.contains("region_engine__<TAG>__<FIELD>"), "{text}");
        });
    }

    #[test]
    fn test_tagged_seq_env_rejects_field_below_scalar_default() {
        // The `data_dir` field of the file engine defaults to a string, so nothing can be set
        // below it.
        let env_prefix = "TAGGED_SEQ_SCALAR_DEFAULT_UT";
        let env_key = [env_prefix, "REGION_ENGINE", "FILE", "DATA_DIR", "SUB"].join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("/tmp/sub"), || {
            let text = load_error_text(env_prefix);
            assert!(text.contains("region_engine.file.data_dir"), "{text}");
            assert!(text.contains("default value is not a table"), "{text}");
        });
    }

    #[test]
    fn test_tagged_seq_env_type_error_names_the_environment() {
        let env_prefix = "TAGGED_SEQ_TYPE_ERROR_UT";
        let env_key = [
            env_prefix,
            "REGION_ENGINE",
            "MITO",
            "INDEX",
            "CACHE_PERCENT",
        ]
        .join(ENV_VAR_SEP);

        temp_env::with_var(env_key, Some("not-a-number"), || {
            // The override replaces the default value as the environment collected it, so the
            // type error still says where that value comes from.
            let text = load_error_text(env_prefix);
            assert!(text.contains("in the environment"), "{text}");
        });
    }

    #[test]
    fn test_tagged_seq_key_must_be_top_level() {
        let err = TestDottedKeyConfig::load_layered_options(None, "TAGGED_SEQ_DOTTED_KEY_UT")
            .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("region_engine.mito"), "{text}");
        assert!(text.contains("top-level"), "{text}");
    }
}
