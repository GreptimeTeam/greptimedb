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
use bytes::{Buf, Bytes, BytesMut};
use datafusion::catalog::CatalogProviderList;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::LogicalPlan;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DecodeDfPlanSnafu, DecodeRelSnafu, EncodeDfPlanSnafu, EncodeRelSnafu, Error};
use crate::{SerializerRegistry, SubstraitPlan};

pub struct DFLogicalSubstraitConvertor;

#[async_trait]
impl SubstraitPlan for DFLogicalSubstraitConvertor {
    type Error = Error;

    type Plan = LogicalPlan;

    async fn decode<B: Buf + Send>(
        &self,
        message: B,
        catalog_list: Arc<dyn CatalogProviderList>,
        state: SessionState,
    ) -> Result<Self::Plan, Self::Error> {
        let mut context = SessionContext::new_with_state(state);
        context.register_catalog_list(catalog_list);
        let plan = Plan::decode(message).context(DecodeRelSnafu)?;
        let df_plan = from_substrait_plan(&context, &plan)
            .await
            .context(DecodeDfPlanSnafu)?;
        Ok(df_plan)
    }

    fn encode(
        &self,
        plan: &Self::Plan,
        serializer: impl SerializerRegistry + 'static,
    ) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::new();
        let substrait_plan = self.to_sub_plan(plan, serializer)?;
        substrait_plan.encode(&mut buf).context(EncodeRelSnafu)?;

        Ok(buf.freeze())
    }
}

impl DFLogicalSubstraitConvertor {
    pub fn to_sub_plan(
        &self,
        plan: &LogicalPlan,
        serializer: impl SerializerRegistry + 'static,
    ) -> Result<Box<Plan>, Error> {
        let session_state =
            SessionState::new_with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()))
                .with_serializer_registry(Arc::new(serializer));
        let context = SessionContext::new_with_state(session_state);

        to_substrait_plan(plan, &context).context(EncodeDfPlanSnafu)
    }
}
