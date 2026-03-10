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

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::{env, fs, io};

use cargo_manifest::Manifest;
const SHADOW_FILE_NAME: &str = "shadow.rs";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Refresh VCS-derived build info only in release builds. In non-release builds (debug,
    // bench, etc.), skip refreshing to preserve incremental compilation.
    let profile = env::var("PROFILE").unwrap_or_default();
    let refresh = profile == "release";

    println!("cargo:rerun-if-env-changed=RUSTC");

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

    let version_state = VersionState::collect(&workspace_root);

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
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
    rust_version: String,
    build_target: String,
}

impl VersionState {
    fn collect(_workspace_root: &Path) -> Self {
        Self {
            branch: build_data::get_git_branch().unwrap_or_default(),
            commit_hash: build_data::get_git_commit().unwrap_or_default(),
            short_commit: build_data::get_git_commit_short().unwrap_or_default(),
            git_clean: build_data::get_git_dirty()
                .map(|dirty| !dirty)
                .unwrap_or(false),
            rust_version: build_data::get_rustc_version().unwrap_or_default(),
            build_target: env::var("TARGET").unwrap_or_default(),
        }
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

fn emit_workspace_watch_list(workspace_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let Some(paths) = git_ls_files(workspace_root) else {
        return Ok(());
    };

    for path in tracked_watch_roots(workspace_root, &paths) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    Ok(())
}

fn tracked_watch_roots(workspace_root: &Path, tracked_files: &[PathBuf]) -> BTreeSet<PathBuf> {
    tracked_files
        .iter()
        .filter_map(|path| path.strip_prefix(workspace_root).ok())
        .filter_map(|relative| {
            relative
                .components()
                .next()
                .map(|first| workspace_root.join(first))
        })
        .collect()
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
