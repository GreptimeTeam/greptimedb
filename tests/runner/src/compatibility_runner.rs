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
use crate::interceptors::{since, till};
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
                store_addrs: vec!["127.0.0.1:2379".to_string()],
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
        self.run_phase(&self.from_version, Phase::Feature).await?;
        self.run_phase(&self.to_version, Phase::Verify).await?;
        self.run_phase(&self.to_version, Phase::Cleanup).await?;

        Ok(())
    }

    async fn run_phase(&self, version: &Version, phase: Phase) -> anyhow::Result<()> {
        if !self.case_dir.exists() {
            return Ok(());
        }

        let (mode_filter, remainder) = Self::split_filter(self.test_filter.as_str());
        let case_root = self.case_dir.join(phase.dir_name());
        if !case_root.exists() {
            return Ok(());
        }

        let bins_dir = Self::resolve_bins_dir(version).await?;
        let mut store_config = self.store_config.clone();
        store_config.setup_etcd = phase.should_setup_etcd();
        let env = Env::new_bare(
            self.data_dir.clone(),
            self.server_addr.clone(),
            self.wal_config.clone(),
            self.pull_version_on_need,
            bins_dir.clone(),
            store_config,
            self.extra_args.clone(),
        );

        let mut interceptor_registry: Registry = Default::default();
        interceptor_registry.register(
            protocol_interceptor::PREFIX,
            Arc::new(protocol_interceptor::ProtocolInterceptorFactory),
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
            phase.dir_name(),
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

        let normalized = Self::trim_anchors(trimmed);
        let (mode, mut rest) = Self::extract_mode_and_path(normalized);
        rest = Self::trim_case_group_prefixes(rest);

        (mode, rest.to_string())
    }

    fn trim_anchors(filter: &str) -> &str {
        let mut normalized = filter;
        if let Some(rest) = normalized.strip_prefix('^') {
            normalized = rest;
        }
        if let Some(rest) = normalized.strip_suffix('$') {
            normalized = rest;
        }
        normalized
    }

    fn extract_mode_and_path(filter: &str) -> (ModeFilter, &str) {
        if let Some((mode, rest)) = Self::extract_mode_prefix(filter) {
            return (mode, rest);
        }

        if let Some(after_phase) = Self::strip_phase_prefix(filter)
            && let Some((mode, rest)) = Self::extract_mode_prefix(after_phase)
        {
            return (mode, rest);
        }

        if let Some(after_phase) = Self::strip_phase_prefix(filter) {
            return (ModeFilter::Any, after_phase);
        }

        (ModeFilter::Any, filter)
    }

    fn extract_mode_prefix(filter: &str) -> Option<(ModeFilter, &str)> {
        if let Some(rest) = filter.strip_prefix("standalone/") {
            return Some((ModeFilter::Standalone, rest));
        }
        if let Some(rest) = filter.strip_prefix("distributed/") {
            return Some((ModeFilter::Distributed, rest));
        }
        if filter == "standalone" {
            return Some((ModeFilter::Standalone, ""));
        }
        if filter == "distributed" {
            return Some((ModeFilter::Distributed, ""));
        }

        None
    }

    fn strip_phase_prefix(filter: &str) -> Option<&str> {
        for phase in ["1.feature/", "2.verify/", "3.cleanup/"] {
            if let Some(rest) = filter.strip_prefix(phase) {
                return Some(rest);
            }
        }
        None
    }

    fn trim_case_group_prefixes(mut path: &str) -> &str {
        for prefix in ["common/", "only/"] {
            if let Some(rest) = path.strip_prefix(prefix) {
                path = rest;
            }
        }

        path
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

#[derive(Copy, Clone)]
enum Phase {
    Feature,
    Verify,
    Cleanup,
}

impl Phase {
    fn dir_name(self) -> &'static str {
        match self {
            Self::Feature => "1.feature",
            Self::Verify => "2.verify",
            Self::Cleanup => "3.cleanup",
        }
    }

    fn should_setup_etcd(self) -> bool {
        matches!(self, Self::Feature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_filter_mode_only() {
        let (mode, remainder) = CompatibilityRunner::split_filter("standalone");
        assert!(matches!(mode, ModeFilter::Standalone));
        assert!(remainder.is_empty());
    }

    #[test]
    fn test_split_filter_mode_and_case_path() {
        let (mode, remainder) = CompatibilityRunner::split_filter("distributed/common/show.sql");
        assert!(matches!(mode, ModeFilter::Distributed));
        assert_eq!(remainder, "show.sql");
    }

    #[test]
    fn test_split_filter_phase_then_mode() {
        let (mode, remainder) =
            CompatibilityRunner::split_filter("1.feature/standalone/only/a.sql");
        assert!(matches!(mode, ModeFilter::Standalone));
        assert_eq!(remainder, "a.sql");
    }

    #[test]
    fn test_split_filter_regex_anchors() {
        let (mode, remainder) =
            CompatibilityRunner::split_filter("^2.verify/distributed/common/t.sql$");
        assert!(matches!(mode, ModeFilter::Distributed));
        assert_eq!(remainder, "t.sql");
    }

    #[test]
    fn test_split_filter_passthrough_regex_with_colon() {
        let (mode, remainder) = CompatibilityRunner::split_filter(".*:foo/bar");
        assert!(matches!(mode, ModeFilter::Any));
        assert_eq!(
            CompatibilityRunner::build_test_filter(&remainder),
            ".*:foo/bar"
        );
    }
}
