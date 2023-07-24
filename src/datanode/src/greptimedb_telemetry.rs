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

use std::sync::Arc;

use async_trait::async_trait;
use common_greptimedb_telemetry::{
    Collector, GreptimeDBTelemetry, GreptimeDBTelemetryTask, Mode as VersionReporterMode,
    TELEMETRY_INTERVAL, TELEMETRY_UUID_KEY,
};
use object_store::{ErrorKind, ObjectStore};
use servers::Mode;

async fn get_uuid(file_name: &str, object_store: ObjectStore) -> Option<String> {
    let mut uuid = None;
    let bs = object_store.read(file_name).await;
    match bs {
        Ok(bytes) => {
            let result = String::from_utf8(bytes);
            if let Ok(s) = result {
                uuid = Some(s);
            }
        }
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                let bs = uuid::Uuid::new_v4().to_string().into_bytes();
                let write_result = object_store.clone().write(file_name, bs.clone()).await;
                if write_result.is_ok() {
                    uuid = Some(String::from_utf8(bs).unwrap());
                }
            }
        }
    }
    uuid
}

struct FrontendVersionReport {
    object_store: ObjectStore,
    uuid: Option<String>,
    retry: i32,
    uuid_file_name: String,
}
#[async_trait]
impl Collector for FrontendVersionReport {
    fn get_mode(&self) -> VersionReporterMode {
        VersionReporterMode::Standalone
    }
    async fn get_nodes(&self) -> i32 {
        1
    }
    async fn get_uuid(&mut self) -> String {
        if let Some(uuid) = &self.uuid {
            uuid.clone()
        } else {
            if self.retry > 3 {
                return "".to_string();
            }
            let uuid = get_uuid(self.uuid_file_name.as_str(), self.object_store.clone()).await;
            if let Some(_uuid) = uuid {
                self.uuid = Some(_uuid.clone());
                return _uuid;
            } else {
                self.retry += 1;
                return "".to_string();
            }
        }
    }
}

pub async fn get_greptimedb_telemetry_task(
    mode: &Mode,
    object_store: ObjectStore,
) -> Option<Arc<GreptimeDBTelemetryTask>> {
    if cfg!(feature = "greptimedb-telemetry") {
        let uuid_file_name = format!("./.{}", TELEMETRY_UUID_KEY);
        match mode {
            Mode::Standalone => Some(Arc::new(GreptimeDBTelemetryTask::new(
                TELEMETRY_INTERVAL,
                Box::new(GreptimeDBTelemetry::new(Box::new(FrontendVersionReport {
                    object_store: object_store.clone(),
                    uuid: get_uuid(uuid_file_name.as_str(), object_store.clone()).await,
                    retry: 0,
                    uuid_file_name,
                }))),
            ))),
            Mode::Distributed => None,
        }
    } else {
        None
    }
}
