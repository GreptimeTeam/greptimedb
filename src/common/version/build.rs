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

use std::path::{Path, PathBuf};
use std::{env, fs, io};

use build_data::{format_timestamp, get_source_time};
use cargo_manifest::Manifest;
const SHADOW_FILE_NAME: &str = "shadow.rs";
const BUILD_INFO_CACHE_FILE_NAME: &str = "build-info.cache";
const BUILD_TIMESTAMP_PREFIX: &str = "build_timestamp=";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Refresh timestamps by default in release builds. In non-release builds (debug, bench,
    // etc.), skip refreshing to preserve incremental compilation.
    // Set DISABLE_BUILD_INFO=1 to force-disable refreshing even in release builds.
    let profile = env::var("PROFILE").unwrap_or_default();
    let disabled = env::var("DISABLE_BUILD_INFO")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let refresh = profile == "release" && !disabled;

    println!("cargo:rerun-if-env-changed=DISABLE_BUILD_INFO");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=TARGET");
    println!("cargo:rerun-if-changed=build.rs");

    // The "CARGO_WORKSPACE_DIR" is set manually (not by Rust itself) in Cargo config file, to
    // solve the problem where the "CARGO_MANIFEST_DIR" is not what we want when this repo is
    // made as a submodule in another repo.
    let workspace_dir =
        env::var("CARGO_WORKSPACE_DIR").or_else(|_| env::var("CARGO_MANIFEST_DIR"))?;
    let workspace_root = PathBuf::from(&workspace_dir);
    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("Cargo.toml").display()
    );

    let product_version = load_product_version(&workspace_root);
    println!("cargo:rustc-env=GREPTIME_PRODUCT_VERSION={product_version}");

    if refresh {
        emit_workspace_watch_list(&workspace_root)?;
        emit_git_watch_list(&workspace_root);
    }

    let version_state = VersionState::collect(&workspace_root, &product_version, refresh);
    println!(
        "cargo:rustc-env=SOURCE_TIMESTAMP={}",
        version_state.source_timestamp
    );

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let build_timestamp = if refresh {
        let cache_path = out_dir.join(BUILD_INFO_CACHE_FILE_NAME);
        let signature = version_state.signature();
        let build_timestamp =
            read_cached_build_timestamp(&cache_path, &signature).unwrap_or_else(current_timestamp);
        write_if_changed(
            &cache_path,
            format!("{signature}\n{BUILD_TIMESTAMP_PREFIX}{build_timestamp}\n"),
        )?;
        build_timestamp
    } else {
        String::new()
    };
    println!("cargo:rustc-env=BUILD_TIMESTAMP={build_timestamp}");

    let shadow_file = out_dir.join(SHADOW_FILE_NAME);
    write_if_changed(&shadow_file, render_shadow_rs(&version_state))?;

    Ok(())
}

#[derive(Debug, Clone)]
struct VersionState {
    branch: String,
    commit_hash: String,
    short_commit: String,
    git_clean: bool,
    git_status_hash: u64,
    source_timestamp: String,
    rust_version: String,
    build_target: String,
    product_version: String,
}

impl VersionState {
    fn collect(workspace_root: &Path, product_version: &str, refresh: bool) -> Self {
        let git_status = git(workspace_root, &["status", "--short"])
            .unwrap_or_default()
            .replace('\r', "");

        Self {
            branch: build_data::get_git_branch().unwrap_or_default(),
            commit_hash: build_data::get_git_commit().unwrap_or_default(),
            short_commit: build_data::get_git_commit_short().unwrap_or_default(),
            git_clean: build_data::get_git_dirty()
                .map(|dirty| !dirty)
                .unwrap_or(false),
            git_status_hash: fnv1a64(git_status.as_bytes()),
            source_timestamp: if refresh {
                get_source_time().map(format_timestamp).unwrap_or_default()
            } else {
                String::new()
            },
            rust_version: build_data::get_rustc_version().unwrap_or_default(),
            build_target: env::var("TARGET").unwrap_or_default(),
            product_version: product_version.to_string(),
        }
    }

    fn signature(&self) -> String {
        [
            format!("branch={}", self.branch),
            format!("commit_hash={}", self.commit_hash),
            format!("short_commit={}", self.short_commit),
            format!("git_clean={}", self.git_clean),
            format!("git_status_hash={:016x}", self.git_status_hash),
            format!("source_timestamp={}", self.source_timestamp),
            format!("rust_version={}", self.rust_version),
            format!("build_target={}", self.build_target),
            format!("product_version={}", self.product_version),
        ]
        .join("\n")
    }
}

fn load_product_version(workspace_root: &Path) -> String {
    let manifest =
        Manifest::from_path(workspace_root.join("Cargo.toml")).expect("Failed to parse Cargo.toml");

    manifest
        .workspace
        .as_ref()
        .and_then(|w| {
            w.metadata.as_ref().and_then(|m| {
                m.get("greptime")
                    .and_then(|g| g.get("product_version").and_then(|v| v.as_str()))
            })
        })
        .map(str::to_string)
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").unwrap())
}

fn emit_workspace_watch_list(workspace_root: &Path) -> io::Result<()> {
    let mut paths = git_ls_files(workspace_root).unwrap_or_else(|| {
        fs::read_dir(workspace_root)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name != ".git" && name != "target")
            })
            .collect()
    });

    paths.sort();

    for path in paths {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    Ok(())
}

fn git_ls_files(workspace_root: &Path) -> Option<Vec<PathBuf>> {
    let output = std::process::Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(workspace_root)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    Some(
        output
            .stdout
            .split(|byte| *byte == b'\0')
            .filter(|path| !path.is_empty())
            .filter_map(|path| std::str::from_utf8(path).ok())
            .map(|path| workspace_root.join(path))
            .collect(),
    )
}

fn emit_git_watch_list(workspace_root: &Path) {
    let Some(git_dir) = git(workspace_root, &["rev-parse", "--git-dir"]).map(PathBuf::from) else {
        return;
    };

    let git_dir = if git_dir.is_absolute() {
        git_dir
    } else {
        workspace_root.join(git_dir)
    };

    for path in [
        git_dir.join("HEAD"),
        git_dir.join("logs/HEAD"),
        git_dir.join("index"),
        git_dir.join("packed-refs"),
    ] {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    if let Some(head_ref) = git(workspace_root, &["symbolic-ref", "-q", "HEAD"])
        && head_ref.starts_with("refs/")
    {
        println!(
            "cargo:rerun-if-changed={}",
            git_dir.join(head_ref).display()
        );
    }
}

fn read_cached_build_timestamp(cache_path: &Path, signature: &str) -> Option<String> {
    let cached = fs::read_to_string(cache_path).ok()?;
    let (cached_signature, cached_timestamp) =
        cached.rsplit_once(&format!("\n{BUILD_TIMESTAMP_PREFIX}"))?;

    if cached_signature != signature {
        return None;
    }

    Some(cached_timestamp.trim().to_string())
}

fn current_timestamp() -> String {
    let now = build_data::now();
    format_timestamp(now)
}

fn render_shadow_rs(version_state: &VersionState) -> String {
    format!(
        "// Code automatically generated by src/common/version/build.rs, do not edit.\n\
         pub const BRANCH: &str = {branch:?};\n\
         pub const COMMIT_HASH: &str = {commit_hash:?};\n\
         pub const SHORT_COMMIT: &str = {short_commit:?};\n\
         pub const GIT_CLEAN: bool = {git_clean};\n\
         pub const RUST_VERSION: &str = {rust_version:?};\n\
         pub const BUILD_TARGET: &str = {build_target:?};\n",
        branch = version_state.branch,
        commit_hash = version_state.commit_hash,
        short_commit = version_state.short_commit,
        git_clean = version_state.git_clean,
        rust_version = version_state.rust_version,
        build_target = version_state.build_target,
    )
}

fn write_if_changed(path: &Path, content: String) -> io::Result<()> {
    if fs::read_to_string(path).ok().as_deref() == Some(content.as_str()) {
        return Ok(());
    }

    fs::write(path, content)
}

fn git(workspace_root: &Path, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(workspace_root)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    Some(stdout.trim().to_string())
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}
