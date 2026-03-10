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
use std::process::Command;
use std::{env, fs, io};

use cargo_manifest::Manifest;
use git2::{ErrorCode, Repository, RepositoryOpenFlags, StatusOptions};

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

    let repository = open_repository(&workspace_root);

    if refresh {
        emit_workspace_watch_list(&workspace_root, repository.as_ref())?;
        emit_git_watch_list(repository.as_ref());
    }

    let version_state = VersionState::collect(repository.as_ref());

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
    fn collect(repository: Option<&Repository>) -> Self {
        Self {
            branch: git_branch(repository),
            commit_hash: git_commit_hash(repository),
            short_commit: git_short_commit(repository),
            git_clean: git_clean(repository),
            rust_version: rustc_version(),
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

fn emit_workspace_watch_list(
    workspace_root: &Path,
    repository: Option<&Repository>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut watch_roots = BTreeSet::new();

    if let Some(paths) = git_tracked_files(workspace_root, repository) {
        watch_roots.extend(tracked_watch_roots(workspace_root, &paths));
    }

    if let Some(paths) = git_status_paths(workspace_root, repository) {
        watch_roots.extend(tracked_watch_roots(workspace_root, &paths));
    }

    for path in watch_roots {
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

fn git_tracked_files(
    workspace_root: &Path,
    repository: Option<&Repository>,
) -> Option<Vec<PathBuf>> {
    let repository = repository?;
    let index = match repository.index() {
        Ok(index) => index,
        Err(err) => {
            cargo_warning(format!(
                "Failed to read git index for build watch list: {err}. Git-derived build info may become stale."
            ));
            return None;
        }
    };

    Some(
        index
            .iter()
            .filter_map(|entry| {
                std::str::from_utf8(entry.path.as_ref())
                    .ok()
                    .map(|path| workspace_root.join(path))
            })
            .collect(),
    )
}

fn git_status_paths(
    workspace_root: &Path,
    repository: Option<&Repository>,
) -> Option<Vec<PathBuf>> {
    let repository = repository?;
    let mut options = status_options();

    match repository.statuses(Some(&mut options)) {
        Ok(statuses) => Some(
            statuses
                .iter()
                .filter_map(|entry| entry.path().map(|path| workspace_root.join(path)))
                .collect(),
        ),
        Err(err) => {
            cargo_warning(format!(
                "Failed to read git status for build watch list: {err}. Git-derived build info may become stale."
            ));
            None
        }
    }
}

fn emit_git_watch_list(repository: Option<&Repository>) {
    let Some(repository) = repository else {
        return;
    };
    let git_dir = repository.path();

    for path in [
        git_dir.join("HEAD"),
        git_dir.join("index"),
        git_dir.join("packed-refs"),
    ] {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    if let Some(head_ref) = repository
        .head()
        .ok()
        .and_then(|head| head.name().map(str::to_string))
        .filter(|head_ref| head_ref.starts_with("refs/"))
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

fn open_repository(workspace_root: &Path) -> Option<Repository> {
    match Repository::open_ext(
        workspace_root,
        RepositoryOpenFlags::NO_SEARCH,
        std::iter::empty::<&Path>(),
    ) {
        Ok(repository) => Some(repository),
        Err(err) if err.code() == ErrorCode::NotFound => None,
        Err(err) => {
            cargo_warning(format!(
                "Failed to open git repository at {}: {err}. Git-derived build info may be unavailable.",
                workspace_root.display()
            ));
            None
        }
    }
}

fn git_branch(repository: Option<&Repository>) -> String {
    repository
        .and_then(|repo| repo.head().ok())
        .filter(|head| head.is_branch())
        .and_then(|head| head.shorthand().map(str::to_string))
        .unwrap_or_default()
}

fn git_commit_hash(repository: Option<&Repository>) -> String {
    repository
        .and_then(|repo| head_target(Some(repo)))
        .map(|oid| oid.to_string())
        .unwrap_or_default()
}

fn git_short_commit(repository: Option<&Repository>) -> String {
    repository
        .and_then(|repo| {
            let object = repo.find_object(head_target(Some(repo))?, None).ok()?;
            object.short_id().ok()?.as_str().map(str::to_string)
        })
        .unwrap_or_default()
}

fn git_clean(repository: Option<&Repository>) -> bool {
    let Some(repository) = repository else {
        return false;
    };

    let mut options = status_options();

    repository
        .statuses(Some(&mut options))
        .map(|statuses| statuses.is_empty())
        .unwrap_or(false)
}

fn head_target(repository: Option<&Repository>) -> Option<git2::Oid> {
    repository?.head().ok()?.target()
}

fn rustc_version() -> String {
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|stdout| stdout.trim().to_string())
        .unwrap_or_default()
}

fn status_options() -> StatusOptions {
    let mut options = StatusOptions::new();
    options
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .renames_head_to_index(true);
    options
}

fn cargo_warning(message: impl AsRef<str>) {
    println!("cargo:warning={}", message.as_ref());
}
