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

use std::sync::Arc;

use snafu::OptionExt;

use crate::auth::UserInfo;
use crate::error::{BuildingContextSnafu, Result};

type CtxFnRef = Arc<dyn Fn(&Context) -> bool + Send + Sync>;

pub struct Context {
    pub client_info: ClientInfo,
    pub user_info: UserInfo,
    pub quota: Quota,
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
            quota: Quota::default(),
            predicates: vec![],
        })
    }
}

pub struct ClientInfo {
    pub client_host: String,
    pub channel: Channel,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Channel {
    GRPC,
    HTTP,
    MYSQL,
}

#[derive(Default)]
pub struct Quota {
    pub total: u64,
    pub consumed: u64,
    pub estimated: u64,
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::auth::UserInfo;
    use crate::context::Channel::{self, HTTP};
    use crate::context::{ClientInfo, Context, CtxBuilder};

    #[test]
    fn test_predicate() {
        let mut ctx = Context {
            client_info: ClientInfo {
                client_host: Default::default(),
                channel: Channel::GRPC,
            },
            user_info: UserInfo::new("greptime", None),
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
            .set_user_info(UserInfo::new("greptime", None))
            .build()
            .unwrap();

        assert_eq!(ctx.client_info.client_host, String::from("127.0.0.1:4001"));

        assert_eq!(ctx.quota.total, 0);
        assert_eq!(ctx.quota.consumed, 0);
        assert_eq!(ctx.quota.estimated, 0);

        assert_eq!(ctx.predicates.capacity(), 0);
    }
}
