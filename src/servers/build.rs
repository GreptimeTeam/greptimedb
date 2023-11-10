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

fn main() {
    build_data::set_RUSTC_VERSION();
    build_data::set_GIT_BRANCH();
    build_data::set_GIT_COMMIT();
    build_data::set_SOURCE_TIMESTAMP();

    #[cfg(feature = "dashboard")]
    fetch_dashboard_assets();
}

#[cfg(feature = "dashboard")]
fn fetch_dashboard_assets() {
    use std::process::{Command, Stdio};

    let message = "Failed to fetch dashboard assets";
    let help = r#"
You can manually execute "fetch-dashboard-assets.sh" to see why, 
or it's a network error, just try again or enable/disable some proxy."#;

    // It's very unlikely to be failed to get the current dir here, see `current_dir`'s docs.
    let mut dir = std::env::current_dir().unwrap();
    dir.pop();
    dir.pop();
    dir.push("scripts");

    let out_dir = std::env::var("OUT_DIR").unwrap();

    #[cfg(windows)]
    let (program, args) = (
        "bash",
        [
            format!(
                "/mnt/{}/fetch-dashboard-assets.sh",
                dir.display()
                    .to_string()
                    .to_lowercase()
                    .replace(':', "")
                    .replace('\\', "/")
            ),
            format!(
                "/mnt/{}",
                out_dir.to_lowercase().replace(':', "").replace('\\', "/")
            ),
        ],
    );
    #[cfg(not(windows))]
    let (program, args) = ("./fetch-dashboard-assets.sh", [out_dir]);

    let output = Command::new(program)
        .args(args)
        .current_dir(dir)
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|p| p.wait_with_output());
    match output {
        Ok(output) => {
            let script_output = String::from_utf8_lossy(&output.stdout);

            assert!(
                output.status.success(),
                "{message}.\n{script_output}\n{help}"
            );
        }
        Err(e) => {
            let e = format!("{message}: {e}.\n{help}");
            panic!("{}", e);
        }
    }
    println!("cargo:rerun-if-changed=dashboard/VERSION");
}
