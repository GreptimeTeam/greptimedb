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

use std::path::PathBuf;
use std::sync::Arc;

use common_query::Output;
use common_recordbatch::util;
use frontend::instance::Instance;
use rstest_reuse::{self, template};

use crate::tests::{
    create_distributed_instance, create_standalone_instance, MockDistributedInstance,
    MockStandaloneInstance,
};

pub(crate) trait MockInstance {
    fn frontend(&self) -> Arc<Instance>;

    fn is_distributed_mode(&self) -> bool;
}

impl MockInstance for MockStandaloneInstance {
    fn frontend(&self) -> Arc<Instance> {
        self.instance.clone()
    }

    fn is_distributed_mode(&self) -> bool {
        false
    }
}

impl MockInstance for MockDistributedInstance {
    fn frontend(&self) -> Arc<Instance> {
        self.frontend.clone()
    }

    fn is_distributed_mode(&self) -> bool {
        true
    }
}

pub(crate) async fn standalone() -> Arc<dyn MockInstance> {
    let test_name = uuid::Uuid::new_v4().to_string();
    let instance = create_standalone_instance(&test_name).await;
    Arc::new(instance)
}

pub(crate) async fn distributed() -> Arc<dyn MockInstance> {
    let test_name = uuid::Uuid::new_v4().to_string();
    let instance = create_distributed_instance(&test_name).await;
    Arc::new(instance)
}

#[template]
#[rstest]
#[case::test_with_standalone(standalone())]
#[case::test_with_distributed(distributed())]
#[awt]
#[tokio::test(flavor = "multi_thread")]
pub(crate) fn both_instances_cases(
    #[future]
    #[case]
    instance: Arc<dyn MockInstance>,
) {
}

#[template]
#[rstest]
#[case::test_with_standalone(standalone())]
#[awt]
#[tokio::test(flavor = "multi_thread")]
pub(crate) fn standalone_instance_case(
    #[future]
    #[case]
    instance: Arc<dyn MockInstance>,
) {
}

pub(crate) async fn check_output_stream(output: Output, expected: &str) {
    let recordbatches = match output {
        Output::Stream(stream) => util::collect_batches(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = recordbatches.pretty_print().unwrap();
    assert_eq!(pretty_print, expected, "actual: \n{}", pretty_print);
}

pub(crate) async fn check_unordered_output_stream(output: Output, expected: &str) {
    let sort_table = |table: &str| -> String {
        let replaced = table.replace("\\n", "\n");
        let mut lines = replaced.split('\n').collect::<Vec<_>>();
        lines.sort();
        lines
            .into_iter()
            .map(|s| s.to_string())
            .reduce(|acc, e| format!("{acc}\\n{e}"))
            .unwrap()
    };

    let recordbatches = match output {
        Output::Stream(stream) => util::collect_batches(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = sort_table(&recordbatches.pretty_print().unwrap());
    let expected = sort_table(expected);
    assert_eq!(pretty_print, expected);
}

pub fn get_data_dir(path: &str) -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");

    PathBuf::from(dir).join(path)
}
