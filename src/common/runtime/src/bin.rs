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

use clap::Parser;

#[derive(Debug, Default, Parser)]
pub struct Command {
    #[clap(long)]
    loop_cnt: usize,
}

fn main() {
    common_telemetry::init_default_ut_logging();
    let cmd = Command::parse();

    test_diff_priority_cpu::test_diff_workload_priority(cmd.loop_cnt);
}

mod test_diff_priority_cpu {
    use std::path::PathBuf;

    use common_runtime::runtime::{BuilderBuild, Priority, RuntimeTrait};
    use common_runtime::{Builder, Runtime};
    use common_telemetry::debug;
    use tempfile::TempDir;

    fn compute_pi_str(precision: usize) -> String {
        let mut pi = 0.0;
        let mut sign = 1.0;

        for i in 0..precision {
            pi += sign / (2 * i + 1) as f64;
            sign *= -1.0;
        }

        pi *= 4.0;
        format!("{:.prec$}", pi, prec = precision)
    }

    macro_rules! def_workload_enum {
        ($($variant:ident),+) => {
            #[derive(Debug)]
            enum WorkloadType {
                $($variant),+
            }

            /// array of workloads for iteration
            const WORKLOADS: &'static [WorkloadType] = &[
                $( WorkloadType::$variant ),+
            ];
        };
    }

    def_workload_enum!(
        ComputeHeavily,
        ComputeHeavily2,
        WriteFile,
        SpawnBlockingWriteFile
    );

    async fn workload_compute_heavily() {
        let prefix = 10;

        for _ in 0..3000 {
            let _ = compute_pi_str(prefix);
            tokio::task::yield_now().await;
        }
    }
    async fn workload_compute_heavily2() {
        let prefix = 30;
        for _ in 0..2000 {
            let _ = compute_pi_str(prefix);
            tokio::task::yield_now().await;
        }
    }
    async fn workload_write_file(_idx: u64, tempdir: PathBuf) {
        use tokio::io::AsyncWriteExt;
        let prefix = 50;

        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(tempdir.join(format!("pi_{}", prefix)))
            .await
            .unwrap();
        for i in 0..200 {
            let pi = compute_pi_str(prefix);

            if i % 2 == 0 {
                file.write_all(pi.as_bytes()).await.unwrap();
            }
        }
    }
    async fn workload_spawn_blocking_write_file(tempdir: PathBuf) {
        use std::io::Write;
        let prefix = 100;
        let mut file = Some(
            std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(tempdir.join(format!("pi_{}", prefix)))
                .unwrap(),
        );
        for i in 0..100 {
            let pi = compute_pi_str(prefix);
            if i % 2 == 0 {
                let mut file1 = file.take().unwrap();
                file = Some(
                    tokio::task::spawn_blocking(move || {
                        file1.write_all(pi.as_bytes()).unwrap();
                        file1
                    })
                    .await
                    .unwrap(),
                );
            }
        }
    }

    pub fn test_diff_workload_priority(loop_cnt: usize) {
        let tempdir = tempfile::tempdir().unwrap();
        let priorities = [
            Priority::VeryLow,
            Priority::Low,
            Priority::Middle,
            Priority::High,
            Priority::VeryHigh,
        ];
        for wl in WORKLOADS {
            for p in priorities.iter() {
                let runtime: Runtime = Builder::default()
                    .runtime_name("test")
                    .thread_name("test")
                    .worker_threads(8)
                    .priority(*p)
                    .build()
                    .expect("Fail to create runtime");
                let runtime2 = runtime.clone();
                runtime.block_on(test_spec_priority_and_workload(
                    *p, runtime2, wl, &tempdir, loop_cnt,
                ));
            }
        }
    }

    async fn test_spec_priority_and_workload(
        priority: Priority,
        runtime: Runtime,
        workload_id: &WorkloadType,
        tempdir: &TempDir,
        loop_cnt: usize,
    ) {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        debug!(
            "testing cpu usage for priority {:?} workload_id {:?}",
            priority, workload_id,
        );
        // start monitor thread
        let mut tasks = vec![];
        let start = std::time::Instant::now();
        for i in 0..loop_cnt {
            // persist cpu usage in json: {priority}.{workload_id}
            match *workload_id {
                WorkloadType::ComputeHeavily => {
                    tasks.push(runtime.spawn(workload_compute_heavily()));
                }
                WorkloadType::ComputeHeavily2 => {
                    tasks.push(runtime.spawn(workload_compute_heavily2()));
                }
                WorkloadType::SpawnBlockingWriteFile => {
                    tasks.push(runtime.spawn(workload_spawn_blocking_write_file(
                        tempdir.path().to_path_buf(),
                    )));
                }
                WorkloadType::WriteFile => {
                    tasks.push(
                        runtime.spawn(workload_write_file(i as u64, tempdir.path().to_path_buf())),
                    );
                }
            }
        }
        for task in tasks {
            task.await.unwrap();
        }
        let elapsed = start.elapsed();
        debug!(
            "test cpu usage for priority {:?} workload_id {:?} elapsed {}ms",
            priority,
            workload_id,
            elapsed.as_millis()
        );
    }
}
