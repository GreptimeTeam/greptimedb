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
use std::env;
use std::path::PathBuf;

use build_data::{format_timestamp, get_source_time};
use cargo_manifest::Manifest;
use shadow_rs::{BuildPattern, CARGO_METADATA, CARGO_TREE, ShadowBuilder};

fn main() -> shadow_rs::SdResult<()> {
    // Refresh timestamps by default in release builds. In non-release builds (debug, bench,
    // etc.), skip refreshing to preserve incremental compilation.
    // Set DISABLE_BUILD_INFO=1 to force-disable refreshing even in release builds.
    let profile = env::var("PROFILE").unwrap_or_default();
    let disabled = env::var("DISABLE_BUILD_INFO")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let refresh = profile == "release" && !disabled;

    println!("cargo:rerun-if-env-changed=DISABLE_BUILD_INFO");

    if refresh {
        println!(
            "cargo:rustc-env=SOURCE_TIMESTAMP={}",
            if let Ok(t) = get_source_time() {
                format_timestamp(t)
            } else {
                "".to_string()
            }
        );
        build_data::set_BUILD_TIMESTAMP();
    } else {
        println!("cargo:rustc-env=SOURCE_TIMESTAMP=");
        println!("cargo:rustc-env=BUILD_TIMESTAMP=");
    }

    // The "CARGO_WORKSPACE_DIR" is set manually (not by Rust itself) in Cargo config file, to
    // solve the problem where the "CARGO_MANIFEST_DIR" is not what we want when this repo is
    // made as a submodule in another repo.
    let src_path = env::var("CARGO_WORKSPACE_DIR").or_else(|_| env::var("CARGO_MANIFEST_DIR"))?;

    let manifest = Manifest::from_path(PathBuf::from(&src_path).join("Cargo.toml"))
        .expect("Failed to parse Cargo.toml");
    if let Some(product_version) = manifest.workspace.as_ref().and_then(|w| {
        w.metadata.as_ref().and_then(|m| {
            m.get("greptime")
                .and_then(|g| g.get("product_version").and_then(|v| v.as_str()))
        })
    }) {
        println!(
            "cargo:rustc-env=GREPTIME_PRODUCT_VERSION={}",
            product_version
        );
    } else {
        let version = env::var("CARGO_PKG_VERSION").unwrap();
        println!("cargo:rustc-env=GREPTIME_PRODUCT_VERSION={}", version,);
    }

    let out_path = env::var("OUT_DIR")?;
    let shadow_file = PathBuf::from(&out_path).join("shadow.rs");

    // When not refreshing build info and the shadow.rs already exists, skip regenerating
    // it entirely. shadow_rs always writes new BUILD_TIME* values which would change
    // the file and invalidate incremental compilation even when nothing meaningful changed.
    if !refresh && shadow_file.exists() {
        println!("cargo:rerun-if-changed=build.rs");
        return Ok(());
    }

    let _ = ShadowBuilder::builder()
        .build_pattern(BuildPattern::Lazy)
        .src_path(src_path)
        .out_path(out_path)
        .deny_const(BTreeSet::from([CARGO_METADATA, CARGO_TREE]))
        .build()
        .unwrap();

    Ok(())
}
