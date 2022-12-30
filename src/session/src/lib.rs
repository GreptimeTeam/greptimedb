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

pub mod context;

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::context::{Channel, ConnInfo, ConnInfoRef, QueryContext, QueryContextRef, UserInfo};

pub struct Session {
    query_ctx: QueryContextRef,
    user_info: ArcSwap<UserInfo>,
    conn_info: ConnInfoRef,
}

impl Session {
    pub fn new(addr: SocketAddr, channel: Channel) -> Self {
        Session {
            query_ctx: Arc::new(QueryContext::new()),
            user_info: ArcSwap::new(Arc::new(UserInfo::default())),
            conn_info: Arc::new(ConnInfo::new(addr, channel)),
        }
    }

    pub fn context(&self) -> QueryContextRef {
        self.query_ctx.clone()
    }
    pub fn conn_info(&self) -> ConnInfoRef {
        self.conn_info.clone()
    }
    pub fn user_info(&self) -> Arc<UserInfo> {
        self.user_info.load().clone()
    }
    pub fn set_user_info(&self, user_info: UserInfo) {
        self.user_info.store(Arc::new(user_info));
    }
}
