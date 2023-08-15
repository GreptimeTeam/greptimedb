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

#![feature(lazy_cell)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

pub mod ports;
pub mod temp_dir;

// Rust is working on an env possibly named `CARGO_WORKSPACE_DIR` to find the root path to the
// workspace, see https://github.com/rust-lang/cargo/issues/3946.
// Until then, use this verbose way.
static WORKSPACE_ROOT: LazyLock<PathBuf> = LazyLock::new(|| {
    let output = Command::new(env!("CARGO"))
        .args(["locate-project", "--workspace", "--message-format=plain"])
        .output()
        .unwrap()
        .stdout;
    let cargo_path = Path::new(std::str::from_utf8(&output).unwrap().trim());
    cargo_path.parent().unwrap().to_path_buf()
});

/// Find the absolute path to a file or a directory in the workspace.
/// The input `path` should be the relative path of the file or directory from workspace root.
///
/// For example, if the greptimedb project is placed under directory "/foo/bar/greptimedb/",
/// and this function is invoked with path = "/src/common/test-util/src/lib.rs", you will get the
/// absolute path to this file.
///
/// The return value is [PathBuf]. This is to adapt the Windows file system's style.
/// However, the input argument is Unix style, this is to give user the most convenience.
pub fn find_workspace_path(path: &str) -> PathBuf {
    let mut buf = WORKSPACE_ROOT.clone();

    // Manually "canonicalize" to avoid annoy Windows specific "\\?" path prefix.
    path.split('/').for_each(|x| {
        if x == ".." {
            buf.pop();
        } else if x != "." {
            buf = buf.join(x);
        }
    });

    buf
}
