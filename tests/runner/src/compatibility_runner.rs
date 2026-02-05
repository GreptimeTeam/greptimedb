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

use std::path::PathBuf;
use std::sync::Arc;

use sqlness::interceptor::Registry;
use sqlness::{ConfigBuilder, Runner};

use crate::cmd::bare::ServerAddr;
use crate::env::bare::{StoreConfig, WalConfig};
use crate::env::compat::Env;
use crate::interceptors::{ignore_result, since, till};
use crate::version::Version;
use crate::{protocol_interceptor, util};

pub struct CompatibilityRunner {
    from_version: Version,
    to_version: Version,
    case_dir: PathBuf,
    data_dir: PathBuf,
    test_filter: String,
    fail_fast: bool,
    server_addr: ServerAddr,
    wal_config: WalConfig,
    store_config: StoreConfig,
    pull_version_on_need: bool,
    extra_args: Vec<String>,
}

impl CompatibilityRunner {
    pub async fn new(
        from_version: Version,
        to_version: Version,
        case_dir: Option<PathBuf>,
        data_dir: PathBuf,
        test_filter: String,
        fail_fast: bool,
    ) -> anyhow::Result<Self> {
        let case_dir = case_dir.unwrap_or_else(|| {
            let mut path = PathBuf::from(util::get_workspace_root());
            path.push("tests");
            path.push("compatibility");
            path
        });

        Ok(Self {
            from_version,
            to_version,
            case_dir,
            data_dir,
            test_filter,
            fail_fast,
            server_addr: ServerAddr {
                server_addr: None,
                pg_server_addr: None,
                mysql_server_addr: None,
            },
            wal_config: WalConfig::RaftEngine,
            store_config: StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
            },
            pull_version_on_need: true,
            extra_args: vec![],
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        self.run_phase(&self.from_version, "1.feature").await?;
        self.run_phase(&self.to_version, "2.verify").await?;
        self.run_phase(&self.to_version, "3.cleanup").await?;

        Ok(())
    }

    async fn run_phase(&self, version: &Version, phase: &str) -> anyhow::Result<()> {
        if !self.case_dir.exists() {
            return Ok(());
        }

        let (mode_filter, remainder) = Self::split_filter(self.test_filter.as_str());
        let case_root = self.case_dir.join(phase);
        if !case_root.exists() {
            return Ok(());
        }

        let bins_dir = Self::resolve_bins_dir(version).await?;
        let env = Env::new_bare(
            self.data_dir.clone(),
            self.server_addr.clone(),
            self.wal_config.clone(),
            self.pull_version_on_need,
            bins_dir.clone(),
            self.store_config.clone(),
            self.extra_args.clone(),
        );

        let mut interceptor_registry: Registry = Default::default();
        interceptor_registry.register(
            protocol_interceptor::PREFIX,
            Arc::new(protocol_interceptor::ProtocolInterceptorFactory),
        );
        interceptor_registry.register(
            ignore_result::PREFIX,
            Arc::new(ignore_result::IgnoreResultInterceptorFactory),
        );
        interceptor_registry.register(
            since::PREFIX,
            Arc::new(since::SinceInterceptorFactory::new(version.clone())),
        );
        interceptor_registry.register(
            till::PREFIX,
            Arc::new(till::TillInterceptorFactory::new(version.clone())),
        );

        let env_filter = Self::env_filter(mode_filter);
        let test_filter = Self::build_test_filter(remainder.as_str());
        let bins_dir_display = bins_dir
            .as_ref()
            .map(|dir| dir.display().to_string())
            .unwrap_or_else(|| "<build>".to_string());
        println!(
            "compat phase={} version={} case_dir={} test_filter={} env_filter={} phase_root={} bins_dir={}",
            phase,
            version,
            self.case_dir.display(),
            self.test_filter,
            env_filter,
            case_root.display(),
            bins_dir_display
        );

        let config = ConfigBuilder::default()
            .case_dir(case_root.to_string_lossy().to_string())
            .fail_fast(self.fail_fast)
            .test_filter(test_filter)
            .env_filter(env_filter)
            .follow_links(true)
            .interceptor_registry(interceptor_registry)
            .parallelism(1)
            .build()?;

        let runner = Runner::new(config, env);
        runner.run().await?;

        Ok(())
    }

    fn split_filter(filter: &str) -> (ModeFilter, String) {
        let trimmed = filter.trim();
        if trimmed.is_empty() || trimmed == ".*" {
            return (ModeFilter::Any, String::new());
        }

        let mut normalized = trimmed;
        if let Some(rest) = normalized.strip_prefix('^') {
            normalized = rest;
        }
        if let Some(rest) = normalized.strip_suffix('$') {
            normalized = rest;
        }

        if let Some(rest) = normalized.strip_prefix("standalone/") {
            return (ModeFilter::Standalone, Self::strip_path_prefixes(rest));
        }
        if let Some(rest) = normalized.strip_prefix("distributed/") {
            return (ModeFilter::Distributed, Self::strip_path_prefixes(rest));
        }
        if normalized == "standalone" {
            return (ModeFilter::Standalone, String::new());
        }
        if normalized == "distributed" {
            return (ModeFilter::Distributed, String::new());
        }

        (ModeFilter::Any, Self::strip_path_prefixes(normalized))
    }

    fn strip_path_prefixes(input: &str) -> String {
        let mut rest = input;
        for prefix in [
            "1.feature/",
            "2.verify/",
            "3.cleanup/",
            "standalone/",
            "distributed/",
            "common/",
            "only/",
        ] {
            if let Some(stripped) = rest.strip_prefix(prefix) {
                rest = stripped;
            }
        }
        rest.to_string()
    }

    fn env_filter(mode_filter: ModeFilter) -> String {
        match mode_filter {
            ModeFilter::Standalone => "^standalone$".to_string(),
            ModeFilter::Distributed => "^distributed$".to_string(),
            ModeFilter::Any => ".*".to_string(),
        }
    }

    fn build_test_filter(remainder: &str) -> String {
        if remainder.is_empty() {
            return ".*".to_string();
        }
        if remainder.contains(':') {
            return remainder.to_string();
        }
        format!(".*:{}", remainder)
    }

    async fn resolve_bins_dir(version: &Version) -> anyhow::Result<Option<PathBuf>> {
        match version {
            Version::Current => Ok(None),
            Version::Semantic(v) => {
                let version_str = format!("v{}", v);
                let dir = PathBuf::from(&version_str);
                let binary_path = dir.join(util::PROGRAM);
                if !binary_path.exists() {
                    util::pull_binary(&version_str).await;
                }
                Ok(Some(dir))
            }
        }
    }
}

#[derive(Copy, Clone)]
enum ModeFilter {
    Any,
    Standalone,
    Distributed,
}
