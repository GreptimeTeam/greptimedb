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

use catalog::RegisterTableRequest;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID, MITO_ENGINE,
};
use common_config::WalConfig;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use servers::Mode;
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::{CreateTableRequest, TableOptions};
use table::TableRef;

use crate::datanode::{DatanodeOptions, FileConfig, ObjectStoreConfig, StorageConfig};
use crate::error::{CreateTableSnafu, Result};
use crate::heartbeat::HeartbeatTask;

pub(crate) struct MockInstance {
    instance: InstanceRef,
    _heartbeat: Option<HeartbeatTask>,
    _guard: TestGuard,
}

impl MockInstance {
    pub(crate) async fn new(name: &str) -> Self {
        let (opts, _guard) = create_tmp_dir_and_datanode_opts(name);

        let (instance, heartbeat) = Instance::with_mock_meta_client(&opts).await.unwrap();
        instance.start().await.unwrap();
        if let Some(task) = heartbeat.as_ref() {
            task.start().await.unwrap();
        }

        MockInstance {
            instance,
            _guard,
            _heartbeat: heartbeat,
        }
    }

    pub(crate) fn inner(&self) -> &Instance {
        &self.instance
    }
}

struct TestGuard {
    _wal_tmp_dir: TempDir,
    _data_tmp_dir: TempDir,
}

fn create_tmp_dir_and_datanode_opts(name: &str) -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = create_temp_dir(&format!("gt_wal_{name}"));
    let data_tmp_dir = create_temp_dir(&format!("gt_data_{name}"));
    let opts = DatanodeOptions {
        wal: WalConfig::default(),
        storage: StorageConfig {
            data_home: data_tmp_dir.path().to_str().unwrap().to_string(),
            store: ObjectStoreConfig::File(FileConfig {}),
            ..Default::default()
        },
        mode: Mode::Standalone,
        ..Default::default()
    };
    (
        opts,
        TestGuard {
            _wal_tmp_dir: wal_tmp_dir,
            _data_tmp_dir: data_tmp_dir,
        },
    )
}
