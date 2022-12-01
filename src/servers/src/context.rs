// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::auth::UserInfo;
use crate::error::{BuildingContextSnafu, Result};

type CtxFnRef = Arc<dyn Fn(&Context) -> bool + Send + Sync>;

#[derive(Serialize, Deserialize)]
pub struct Context {
    pub exec_info: ExecInfo,
    pub client_info: ClientInfo,
    pub user_info: UserInfo,
    pub quota: Quota,
    #[serde(skip)]
    pub predicates: Vec<CtxFnRef>,
}

impl Context {
    pub fn add_predicate(&mut self, predicate: CtxFnRef) {
        self.predicates.push(predicate);
    }
}

#[derive(Default)]
pub struct CtxBuilder {
    client_addr: Option<String>,
    from_channel: Option<Channel>,
    user_info: Option<UserInfo>,
}

impl CtxBuilder {
    pub fn new() -> CtxBuilder {
        CtxBuilder::default()
    }

    pub fn client_addr(mut self, addr: String) -> CtxBuilder {
        self.client_addr = Some(addr);
        self
    }

    pub fn set_channel(mut self, channel: Channel) -> CtxBuilder {
        self.from_channel = Some(channel);
        self
    }

    pub fn set_user_info(mut self, user_info: UserInfo) -> CtxBuilder {
        self.user_info = Some(user_info);
        self
    }

    pub fn build(self) -> Result<Context> {
        Ok(Context {
            client_info: ClientInfo {
                client_host: self.client_addr.context(BuildingContextSnafu {
                    err_msg: "unknown client addr while building ctx",
                })?,
                channel: self.from_channel.context(BuildingContextSnafu {
                    err_msg: "unknown channel while building ctx",
                })?,
            },
            user_info: self.user_info.context(BuildingContextSnafu {
                err_msg: "missing user info while building ctx",
            })?,

            exec_info: ExecInfo::default(),
            quota: Quota::default(),
            predicates: vec![],
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct ExecInfo {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    // should opts to be thread safe?
    pub extra_opts: HashMap<String, String>,
    pub trace_id: Option<String>,
}

impl Default for ExecInfo {
    fn default() -> Self {
        ExecInfo {
            catalog: Some("greptime".to_string()),
            schema: Some("public".to_string()),
            extra_opts: HashMap::new(),
            trace_id: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ClientInfo {
    pub client_host: String,
    pub channel: Channel,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Channel {
    GRPC,
    HTTP,
    MYSQL,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Quota {
    pub total: u64,
    pub consumed: u64,
    pub estimated: u64,
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::auth::AuthMethod;
    use crate::context::Channel::{self, HTTP};
    use crate::context::{ClientInfo, Context, CtxBuilder, UserInfo};

    #[test]
    fn test_predicate() {
        let mut ctx = Context {
            exec_info: Default::default(),
            client_info: ClientInfo {
                client_host: Default::default(),
                channel: Channel::GRPC,
            },
            user_info: UserInfo::new(None, vec![AuthMethod::PlainPwd(b"123456".to_vec())]),
            quota: Default::default(),
            predicates: vec![],
        };
        ctx.add_predicate(Arc::new(|ctx: &Context| {
            ctx.quota.total > ctx.quota.consumed
        }));
        ctx.quota.total = 10;
        ctx.quota.consumed = 5;

        let predicates = ctx.predicates.clone();
        let mut re = true;
        for predicate in predicates {
            re &= predicate(&ctx);
        }
        assert!(re);
    }

    #[test]
    fn test_build() {
        let ctx = CtxBuilder::new()
            .client_addr("127.0.0.1:4001".to_string())
            .set_channel(HTTP)
            .set_user_info(UserInfo::new(
                None,
                vec![AuthMethod::PlainPwd(b"123456".to_vec())],
            ))
            .build()
            .unwrap();

        assert_eq!(ctx.exec_info.catalog.unwrap(), String::from("greptime"));
        assert_eq!(ctx.exec_info.schema.unwrap(), String::from("public"));
        assert_eq!(ctx.exec_info.extra_opts.len(), 0);
        assert_eq!(ctx.exec_info.trace_id, None);

        assert_eq!(ctx.client_info.client_host, String::from("127.0.0.1:4001"));

        assert_eq!(ctx.quota.total, 0);
        assert_eq!(ctx.quota.consumed, 0);
        assert_eq!(ctx.quota.estimated, 0);

        assert_eq!(ctx.predicates.capacity(), 0);
    }
}
