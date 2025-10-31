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

use std::collections::HashMap;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{
    GcRegions, GetFileRefs, GetFileRefsReply, Instruction, InstructionReply,
};
use common_meta::peer::Peer;
use common_telemetry::{error, info, warn};
use snafu::ResultExt;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};

use crate::error::{self, Result};
use crate::gc::scheduler::GcScheduler;
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::Channel;

impl GcScheduler {
    /// Send an instruction to a datanode and wait for the reply.
    ///
    async fn send_instruction(
        &self,
        peer: &Peer,
        instruction: Instruction,
        description: &str,
        timeout: Duration,
    ) -> Result<InstructionReply> {
        let msg = MailboxMessage::json_message(
            &format!("{}: {}", description, instruction),
            &format!("Metasrv@{}", self.server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let mailbox_rx = self
            .mailbox
            .send(&Channel::Datanode(peer.id), msg, timeout)
            .await?;

        match mailbox_rx.await {
            Ok(reply_msg) => {
                let reply = HeartbeatMailbox::json_reply(&reply_msg)?;
                Ok(reply)
            }
            Err(e) => {
                error!(
                    "Failed to receive reply from datanode {} for {}: {}",
                    peer, description, e
                );
                Err(e)
            }
        }
    }

    /// Send GetFileRefs instruction to a datanode for specified regions.
    pub(crate) async fn send_get_file_refs_instruction(
        &self,
        peer: &Peer,
        region_ids: &[RegionId],
    ) -> Result<FileRefsManifest> {
        info!(
            "Sending GetFileRefs instruction to datanode {} for {} regions",
            peer,
            region_ids.len()
        );

        let instruction = Instruction::GetFileRefs(GetFileRefs {
            region_ids: region_ids.to_vec(),
        });

        let reply = self
            .send_instruction(
                peer,
                instruction,
                "Get file references",
                self.config.mailbox_timeout,
            )
            .await?;

        let InstructionReply::GetFileRefs(GetFileRefsReply {
            file_refs_manifest,
            success,
            error,
        }) = reply
        else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: format!("{:?}", reply),
                reason: "Unexpected reply of the GetFileRefs instruction",
            }
            .fail();
        };

        if !success {
            return error::UnexpectedSnafu {
                violated: format!(
                    "Failed to get file references from datanode {}: {:?}",
                    peer, error
                ),
            }
            .fail();
        }

        Ok(file_refs_manifest)
    }

    /// Send GC instruction to a datanode for a specific region.
    pub(crate) async fn send_gc_region_instruction(
        &self,
        peer: Peer,
        region_id: RegionId,
        file_refs_manifest: &FileRefsManifest,
        full_file_listing: bool,
    ) -> Result<GcReport> {
        info!(
            "Sending GC instruction to datanode {} for region {} (full_file_listing: {})",
            peer, region_id, full_file_listing
        );

        let instruction = Instruction::GcRegions(GcRegions {
            regions: vec![region_id],
            file_refs_manifest: file_refs_manifest.clone(),
            full_file_listing,
        });

        let reply = self
            .send_instruction(&peer, instruction, "GC region", self.config.mailbox_timeout)
            .await?;

        let InstructionReply::GcRegions(reply) = reply else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: format!("{:?}", reply),
                reason: "Unexpected reply of the GcRegions instruction",
            }
            .fail();
        };

        let res = reply.result;
        match res {
            Ok(report) => Ok(report),
            Err(e) => {
                error!(
                    "Datanode {} reported error during GC for region {}: {}",
                    peer, region_id, e
                );
                Err(error::UnexpectedSnafu {
                    violated: format!(
                        "Datanode {} reported error during GC for region {}: {}",
                        peer, region_id, e
                    ),
                }
                .fail()?)
            }
        }
    }

    /// Get file references for the specified regions.
    ///
    /// If certain datanodes are unreachable, it logs a warning and skips those regions instead of failing the entire operation.
    pub(crate) async fn get_file_references(
        &self,
        region_ids: &[RegionId],
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<FileRefsManifest> {
        info!("Getting file references for {} regions", region_ids.len());

        // Group regions by datanode to minimize RPC calls
        let mut datanode_regions: HashMap<Peer, Vec<RegionId>> = HashMap::new();

        for region_id in region_ids {
            if let Some(peer) = region_to_peer.get(region_id) {
                datanode_regions
                    .entry(peer.clone())
                    .or_default()
                    .push(*region_id);
            }
        }

        // Send GetFileRefs instructions to each datanode
        let mut all_file_refs = HashMap::new();
        let mut all_manifest_versions = HashMap::new();

        for (peer, regions) in datanode_regions {
            match self.send_get_file_refs_instruction(&peer, &regions).await {
                Ok(manifest) => {
                    // TODO(discord9): if other regions provide file refs for one region on other datanode, and no version,
                    // is it correct to merge manifest_version directly?
                    all_file_refs.extend(manifest.file_refs);
                    all_manifest_versions.extend(manifest.manifest_version);
                }
                Err(e) => {
                    warn!(
                        "Failed to get file refs from datanode {}: {}. Skipping regions on this datanode.",
                        peer, e
                    );
                    // Continue processing other datanodes instead of failing the entire operation
                    continue;
                }
            }
        }

        Ok(FileRefsManifest {
            file_refs: all_file_refs,
            manifest_version: all_manifest_versions,
        })
    }
}
