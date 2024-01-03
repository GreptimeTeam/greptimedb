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

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::peer::Peer;
use common_meta::RegionIdent;
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use super::deactivate_region::DeactivateRegion;
use super::{RegionFailoverContext, State};
use crate::error::{RegionFailoverCandidatesNotFoundSnafu, Result, RetryLaterSnafu};
use crate::selector::SelectorOptions;

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct RegionFailoverStart {
    failover_candidate: Option<Peer>,
}

impl RegionFailoverStart {
    pub(super) fn new() -> Self {
        Self {
            failover_candidate: None,
        }
    }

    async fn choose_candidate(
        &mut self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Peer> {
        if let Some(candidate) = self.failover_candidate.clone() {
            return Ok(candidate);
        }

        let mut selector_ctx = ctx.selector_ctx.clone();
        selector_ctx.table_id = Some(failed_region.table_id);

        let cluster_id = failed_region.cluster_id;
        let opts = SelectorOptions::default();
        let candidates = ctx
            .selector
            .select(cluster_id, &selector_ctx, opts)
            .await?
            .iter()
            .filter_map(|p| {
                if p.id != failed_region.datanode_id {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<Peer>>();
        ensure!(
            !candidates.is_empty(),
            RegionFailoverCandidatesNotFoundSnafu {
                failed_region: format!("{failed_region:?}"),
            }
        );

        // Safety: indexing is guarded by the "ensure!" above.
        let candidate = &candidates[0];
        self.failover_candidate = Some(candidate.clone());
        info!("Choose failover candidate datanode {candidate:?} for region: {failed_region}");
        Ok(candidate.clone())
    }
}

#[async_trait]
#[typetag::serde]
impl State for RegionFailoverStart {
    async fn next(
        &mut self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        let candidate = self
            .choose_candidate(ctx, failed_region)
            .await
            .map_err(|e| {
                if e.status_code() == StatusCode::RuntimeResourcesExhausted {
                    RetryLaterSnafu {
                        reason: format!("{e}"),
                    }
                    .build()
                } else {
                    e
                }
            })?;
        return Ok(Box::new(DeactivateRegion::new(candidate)));
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::TestingEnvBuilder;
    use super::*;

    #[tokio::test]
    async fn test_choose_failover_candidate() {
        common_telemetry::init_default_ut_logging();

        let env = TestingEnvBuilder::new().build().await;
        let failed_region = env.failed_region(1).await;

        let mut state = RegionFailoverStart::new();
        assert!(state.failover_candidate.is_none());

        let candidate = state
            .choose_candidate(&env.context, &failed_region)
            .await
            .unwrap();
        assert_ne!(candidate.id, failed_region.datanode_id);

        let candidate_again = state
            .choose_candidate(&env.context, &failed_region)
            .await
            .unwrap();
        assert_eq!(candidate, candidate_again);
    }
}
