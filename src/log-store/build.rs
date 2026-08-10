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

//! Generates Rust protobuf bindings for `proto/logstore.proto`.
//!
//! The protoc compiler is selected in the following order:
//!
//! 1. Use the compiler specified by the `PROTOC` environment variable.
//!    If the configured compiler is unusable or does not meet the minimum
//!    supported version requirement, the build will panic instead of falling
//!    back to another compiler.
//!
//! 2. Use a compatible `protoc` compiler available from `PATH`.
//!    If it is unavailable or does not meet the minimum supported version
//!    requirement, continue with the vendored compiler fallback.
//!
//! 3. Use a prebuilt vendored `protoc` compiler.
//!    This provides a portable fallback when no supported system compiler is
//!    available.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use protoc_rust::{Codegen, Customize};

/// Returns whether `protoc` runs and meets GreptimeDB's minimum supported version.
fn usable_protoc(protoc: &Path) -> bool {
    let Ok(output) = Command::new(protoc).arg("--version").output() else {
        return false;
    };
    if !output.status.success() {
        return false;
    }

    let Some(version) = std::str::from_utf8(&output.stdout)
        .ok()
        .and_then(|output| output.trim().strip_prefix("libprotoc "))
    else {
        return false;
    };
    let mut components = version.split('.');
    matches!(
        (
            components.next().and_then(|part| part.parse::<u32>().ok()),
            components.next().and_then(|part| part.parse::<u32>().ok()),
        ),
        (Some(major), Some(minor)) if (major, minor) >= (3, 15)
    )
}

/// Selects a compiler from `PROTOC`, `PATH`, or the vendored fallback, in that order.
///
/// An invalid explicit `PROTOC` is rejected so configuration errors are not
/// silently masked.
fn protoc_path() -> PathBuf {
    if let Some(protoc) = env::var_os("PROTOC") {
        let protoc = PathBuf::from(protoc);
        assert!(usable_protoc(&protoc), "PROTOC version not usable");
        return protoc;
    }

    let protoc = PathBuf::from("protoc");
    if usable_protoc(&protoc) {
        return protoc;
    }

    protoc_bin_vendored::protoc_bin_path().expect("no bundled protoc for this platform")
}

fn main() {
    let base =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let proto_dir = base.join("proto");
    let proto = proto_dir.join("logstore.proto");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR not set")).join("protos");

    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PATH");
    println!("cargo:rerun-if-changed={}", proto.display());
    std::fs::create_dir_all(&out_dir).expect("failed to create protobuf output directory");

    Codegen::new()
        .protoc_path(protoc_path())
        .out_dir(out_dir)
        .input(proto)
        .include(proto_dir)
        .customize(Customize {
            gen_mod_rs: Some(true),
            ..Default::default()
        })
        .run()
        .expect("failed to generate log-store protobuf bindings")
}
