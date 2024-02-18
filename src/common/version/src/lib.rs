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

const UNKNOWN: &str = "unknown";

#[allow(clippy::print_stdout)]
pub fn setup_git_versions() {
    println!(
        "cargo:rustc-env=GIT_COMMIT={}",
        build_data::get_git_commit()
            .as_ref()
            .map(String::as_str)
            .unwrap_or(UNKNOWN)
    );
    println!(
        "cargo:rustc-env=GIT_COMMIT_SHORT={}",
        build_data::get_git_commit_short()
            .as_ref()
            .map(String::as_str)
            .unwrap_or(UNKNOWN)
    );
    println!(
        "cargo:rustc-env=GIT_BRANCH={}",
        build_data::get_git_branch()
            .as_ref()
            .map(String::as_str)
            .unwrap_or(UNKNOWN)
    );
    println!(
        "cargo:rustc-env=GIT_DIRTY={}",
        build_data::get_git_dirty()
            .map(|b| if b { "true" } else { "false" })
            .unwrap_or(UNKNOWN)
    );
}
