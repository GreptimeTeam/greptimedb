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

#![allow(clippy::print_stdout)]

use std::fmt::Display;

shadow_rs::shadow!(build);

#[derive(Clone, Debug, PartialEq)]
pub struct BuildInfo {
    pub branch: &'static str,
    pub commit: &'static str,
    pub commit_short: &'static str,
    pub clean: bool,
    pub source_time: &'static str,
    pub build_time: &'static str,
    pub rustc: &'static str,
    pub target: &'static str,
    pub version: &'static str,
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
                format!("clean: {}", self.clean),
                format!("version: {}", self.version),
            ]
            .join("\n")
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "codec", derive(serde::Serialize, serde::Deserialize))]
pub struct OwnedBuildInfo {
    pub branch: String,
    pub commit: String,
    pub commit_short: String,
    pub clean: bool,
    pub source_time: String,
    pub build_time: String,
    pub rustc: String,
    pub target: String,
    pub version: String,
}

impl From<BuildInfo> for OwnedBuildInfo {
    fn from(info: BuildInfo) -> Self {
        OwnedBuildInfo {
            branch: info.branch.to_string(),
            commit: info.commit.to_string(),
            commit_short: info.commit_short.to_string(),
            clean: info.clean,
            source_time: info.source_time.to_string(),
            build_time: info.build_time.to_string(),
            rustc: info.rustc.to_string(),
            target: info.target.to_string(),
            version: info.version.to_string(),
        }
    }
}

impl Display for OwnedBuildInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            [
                format!("branch: {}", self.branch),
                format!("commit: {}", self.commit),
                format!("commit_short: {}", self.commit_short),
                format!("clean: {}", self.clean),
                format!("version: {}", self.version),
            ]
            .join("\n")
        )
    }
}

#[cfg(feature = "refresh_build_info")]
pub const fn build_info() -> BuildInfo {
    BuildInfo {
        branch: build::BRANCH,
        commit: build::COMMIT_HASH,
        commit_short: build::SHORT_COMMIT,
        clean: build::GIT_CLEAN,
        source_time: env!("SOURCE_TIMESTAMP"),
        build_time: env!("BUILD_TIMESTAMP"),
        rustc: build::RUST_VERSION,
        target: build::BUILD_TARGET,
        version: env!("GREPTIME_PRODUCT_VERSION"),
    }
}

#[cfg(not(feature = "refresh_build_info"))]
pub const fn build_info() -> BuildInfo {
    BuildInfo {
        branch: build::BRANCH,
        commit: build::COMMIT_HASH,
        commit_short: build::SHORT_COMMIT,
        clean: build::GIT_CLEAN,
        source_time: "UNKNOWN",
        build_time: "UNKNOWN",
        rustc: build::RUST_VERSION,
        target: build::BUILD_TARGET,
        version: "UNKNOWN",
    }
}

const BUILD_INFO: BuildInfo = build_info();

pub const fn version() -> &'static str {
    BUILD_INFO.version
}

pub const fn verbose_version() -> &'static str {
    const_format::formatcp!(
        "\nbranch: {}\ncommit: {}\nclean: {}\nversion: {}",
        BUILD_INFO.branch,
        BUILD_INFO.commit,
        BUILD_INFO.clean,
        BUILD_INFO.version,
    )
}

pub const fn short_version() -> &'static str {
    const BRANCH: &str = BUILD_INFO.branch;
    const COMMIT_ID: &str = BUILD_INFO.commit_short;

    // When git checkout to a commit, the branch is empty.
    #[allow(clippy::const_is_empty)]
    if !BRANCH.is_empty() {
        const_format::formatcp!("{}-{}", BRANCH, COMMIT_ID)
    } else {
        COMMIT_ID
    }
}
