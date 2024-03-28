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

use std::borrow::Cow;
use std::fmt::Display;
use std::sync::OnceLock;

const UNKNOWN: &str = "unknown";

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "codec",
    derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)
)]
pub struct BuildInfo {
    pub branch: Cow<'static, str>,
    pub commit: Cow<'static, str>,
    pub commit_short: Cow<'static, str>,
    pub dirty: Cow<'static, str>,
    pub timestamp: Cow<'static, str>,

    /// Rustc Version
    pub rustc: Cow<'static, str>,
    /// GreptimeDB Version
    pub version: Cow<'static, str>,
}

impl Display for BuildInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            [
                format!("branch: {}", self.branch),
                format!("commit: {}", self.commit),
                format!("commit_short: {}", self.commit_short),
                format!("dirty: {}", self.dirty),
                format!("version: {}", self.version),
            ]
            .join("\n")
        )
    }
}

static BUILD: OnceLock<BuildInfo> = OnceLock::new();

pub fn build_info() -> &'static BuildInfo {
    BUILD.get_or_init(|| {
        let branch = build_data::get_git_branch()
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(UNKNOWN));
        let commit = build_data::get_git_commit()
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(UNKNOWN));
        let commit_short = build_data::get_git_commit_short()
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(UNKNOWN));
        let dirty = build_data::get_git_dirty()
            .map(|b| Cow::Owned(b.to_string()))
            .unwrap_or(Cow::Borrowed(UNKNOWN));
        let timestamp = build_data::get_source_time()
            .map(|ts| Cow::Owned(build_data::format_timestamp(ts)))
            .unwrap_or(Cow::Borrowed(UNKNOWN));
        let rustc = build_data::get_rustc_version()
            .map(Cow::Owned)
            .unwrap_or(Cow::Borrowed(UNKNOWN));
        let version = Cow::Borrowed(env!("CARGO_PKG_VERSION"));

        BuildInfo {
            branch,
            commit,
            commit_short,
            dirty,
            timestamp,
            rustc,
            version,
        }
    })
}

#[allow(clippy::print_stdout)]
pub fn setup_build_info() {
    let build_info = build_info();
    println!("cargo:rustc-env=GIT_COMMIT={}", build_info.commit);
    println!(
        "cargo:rustc-env=GIT_COMMIT_SHORT={}",
        build_info.commit_short
    );
    println!("cargo:rustc-env=GIT_BRANCH={}", build_info.branch);
    println!("cargo:rustc-env=GIT_DIRTY={}", build_info.dirty);
    println!("cargo:rustc-env=GIT_DIRTY={}", build_info.dirty);
    println!("cargo:rustc-env=RUSTC_VERSION={}", build_info.rustc);
    println!("cargo:rustc-env=SOURCE_TIMESTAMP={}", build_info.timestamp);
}
