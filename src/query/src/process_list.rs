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

use common_meta::key::process_list::{ProcessKey, ProcessValue};
use common_meta::key::{MetadataKey, MetadataValue, PROCESS_LIST_PREFIX};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::{PutRequest, RangeRequest};
use common_meta::sequence::{SequenceBuilder, SequenceRef};
use common_time::util::current_time_millis;
use snafu::ResultExt;

use crate::error;

/// Sequence key for process list entries.
pub const PROCESS_ID_SEQ: &str = "process_id_seq";

pub struct Process {
    key: ProcessKey,
    value: ProcessValue,
}

pub struct ProcessManager {
    server_addr: String,
    sequencer: SequenceRef,
    kv_client: KvBackendRef,
}

impl ProcessManager {
    pub fn new(server_addr: String, kv_client: KvBackendRef) -> Self {
        let sequencer = Arc::new(
            SequenceBuilder::new(PROCESS_ID_SEQ, kv_client.clone())
                .initial(0)
                .step(100)
                .build(),
        );
        Self {
            server_addr,
            sequencer,
            kv_client,
        }
    }

    pub async fn register_query(&self, query: String) -> error::Result<()> {
        let process_id = self.sequencer.next().await.unwrap();
        let key = ProcessKey {
            frontend_ip: self.server_addr.clone(),
            id: process_id,
        }
        .to_bytes();
        let current_time = current_time_millis();
        let value = ProcessValue {
            query,
            start_timestamp_ms: current_time,
        }
        .try_as_raw_value()
        .context(error::RegisterProcessSnafu)?;

        self.kv_client
            .put(PutRequest {
                key,
                value,
                prev_kv: false,
            })
            .await
            .context(error::RegisterProcessSnafu)?;
        Ok(())
    }

    pub async fn list_all_processes(&self) -> error::Result<Vec<Process>> {
        let mut all_processes = vec![];
        let mut has_nex = true;
        while has_nex {
            let resp = self
                .kv_client
                .range(RangeRequest {
                    key: PROCESS_LIST_PREFIX.to_string().into_bytes(),
                    range_end: vec![],
                    limit: 0,
                    keys_only: false,
                })
                .await
                .context(error::ListProcessesSnafu)?;

            let process = resp
                .kvs
                .into_iter()
                .map(|kv| {
                    Ok(Process {
                        key: ProcessKey::from_bytes(&kv.key)?,
                        value: ProcessValue::try_from_raw_value(&kv.value)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .context(error::ListProcessesSnafu)?;
            all_processes.extend(process);
            has_nex = resp.more;
        }

        Ok(all_processes)
    }
}
