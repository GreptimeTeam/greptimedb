use std::fmt::Display;

use shadow_rs::shadow;

shadow!(build);

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "codec",
    derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)
)]
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
        version: build::PKG_VERSION,
    }
}

const BUILD_INFO: BuildInfo = build_info();

pub const fn version() -> &'static str {
    const_format::formatcp!(
        "\nbranch: {}\ncommit: {}\nclean: {}\nversion: {}",
        BUILD_INFO.branch,
        BUILD_INFO.commit,
        BUILD_INFO.clean,
        BUILD_INFO.version,
    )
}

pub const fn short_version() -> &'static str {
    const_format::formatcp!("{}-{}", BUILD_INFO.branch, BUILD_INFO.commit_short,)
}
