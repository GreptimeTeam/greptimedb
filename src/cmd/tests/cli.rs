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

#[cfg(target_os = "macos")]
mod tests {
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use common_test_util::temp_dir::create_temp_dir;
    use rexpect::session::PtyReplSession;

    struct Repl {
        repl: PtyReplSession,
    }

    impl Repl {
        fn send_line(&mut self, line: &str) {
            let _ = self.repl.send_line(line).unwrap();

            // read a line to consume the prompt
            let _ = self.read_line();
        }

        fn read_line(&mut self) -> String {
            self.repl.read_line().unwrap()
        }

        fn read_expect(&mut self, expect: &str) {
            assert_eq!(self.read_line(), expect);
        }

        fn read_contains(&mut self, pat: &str) {
            assert!(self.read_line().contains(pat));
        }
    }

    // TODO(LFC): Un-ignore this REPL test.
    // Ignore this REPL test because some logical plans like create database are not supported yet in Datanode.
    #[ignore]
    #[test]
    fn test_repl() {
        let data_home = create_temp_dir("data");
        let wal_dir = create_temp_dir("wal");

        let mut bin_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        bin_path.push("../../target/debug");
        let bin_path = bin_path.to_str().unwrap();

        let mut datanode = Command::new("./greptime")
            .current_dir(bin_path)
            .args([
                "datanode",
                "start",
                "--rpc-addr=0.0.0.0:4321",
                "--node-id=1",
                &format!("--data-home={}", data_home.path().display()),
                &format!("--wal-dir={}", wal_dir.path().display()),
            ])
            .stdout(Stdio::null())
            .spawn()
            .unwrap();

        // wait for Datanode actually started
        std::thread::sleep(Duration::from_secs(3));

        let mut repl_cmd = Command::new("./greptime");
        let _ = repl_cmd.current_dir(bin_path).args([
            "--log-level=off",
            "cli",
            "attach",
            "--grpc-addr=0.0.0.0:4321",
            // history commands can sneaky into stdout and mess up our tests, so disable it
            "--disable-helper",
        ]);
        let pty_session = rexpect::session::spawn_command(repl_cmd, Some(5_000)).unwrap();
        let repl = PtyReplSession {
            prompt: "> ".to_string(),
            pty_session,
            quit_command: None,
            echo_on: false,
        };
        let repl = &mut Repl { repl };
        repl.read_expect("Ready for commands. (Hint: try 'help')");

        test_create_database(repl);

        test_use_database(repl);

        test_create_table(repl);

        test_insert(repl);

        test_select(repl);

        datanode.kill().unwrap();
        let _ = datanode.wait().unwrap();
    }

    fn test_create_database(repl: &mut Repl) {
        repl.send_line("CREATE DATABASE db;");
        repl.read_expect("Affected Rows: 1");
        repl.read_contains("Cost");
    }

    fn test_use_database(repl: &mut Repl) {
        repl.send_line("USE db");
        repl.read_expect("Total Rows: 0");
        repl.read_contains("Cost");
        repl.read_expect("Using db");
    }

    fn test_create_table(repl: &mut Repl) {
        repl.send_line("CREATE TABLE t(x STRING, ts TIMESTAMP TIME INDEX);");
        repl.read_expect("Affected Rows: 0");
        repl.read_contains("Cost");
    }

    fn test_insert(repl: &mut Repl) {
        repl.send_line("INSERT INTO t(x, ts) VALUES ('hello', 1676895812239);");
        repl.read_expect("Affected Rows: 1");
        repl.read_contains("Cost");
    }

    fn test_select(repl: &mut Repl) {
        repl.send_line("SELECT * FROM t;");

        repl.read_expect("+-------+-------------------------+");
        repl.read_expect("| x     | ts                      |");
        repl.read_expect("+-------+-------------------------+");
        repl.read_expect("| hello | 2023-02-20T12:23:32.239 |");
        repl.read_expect("+-------+-------------------------+");
        repl.read_expect("Total Rows: 1");

        repl.read_contains("Cost");
    }
}
