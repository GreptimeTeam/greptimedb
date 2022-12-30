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

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_telemetry::info;

pub type QueryContextRef = Arc<QueryContext>;
pub type ConnInfoRef = Arc<ConnInfo>;

pub struct QueryContext {
    current_schema: ArcSwapOption<String>,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryContext {
    pub fn arc() -> QueryContextRef {
        Arc::new(QueryContext::new())
    }

    pub fn new() -> Self {
        Self {
            current_schema: ArcSwapOption::new(None),
        }
    }

    pub fn with_current_schema(schema: String) -> Self {
        Self {
            current_schema: ArcSwapOption::new(Some(Arc::new(schema))),
        }
    }

    pub fn current_schema(&self) -> Option<String> {
        self.current_schema.load().as_deref().cloned()
    }

    pub fn set_current_schema(&self, schema: &str) {
        let last = self.current_schema.swap(Some(Arc::new(schema.to_string())));
        info!(
            "set new session default schema: {:?}, swap old: {:?}",
            schema, last
        )
    }
}

pub const DEFAULT_USERNAME: &str = "greptime";

#[derive(Clone, Debug)]
pub struct UserInfo {
    username: String,
}

impl Default for UserInfo {
    fn default() -> Self {
        Self {
            username: DEFAULT_USERNAME.to_string(),
        }
    }
}

impl UserInfo {
    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
        }
    }
}

pub struct ConnInfo {
    pub client_host: SocketAddr,
    pub channel: Channel,
}

impl ConnInfo {
    pub fn new(client_host: SocketAddr, channel: Channel) -> Self {
        Self {
            client_host,
            channel,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Channel {
    Grpc,
    Http,
    Mysql,
}

#[cfg(test)]
mod test {
    use crate::context::{Channel, UserInfo};
    use crate::Session;

    #[test]
    fn test_session() {
        let session = Session::new("127.0.0.1:9000".parse().unwrap(), Channel::Mysql);
        // test user_info
        assert_eq!(session.user_info().username(), "greptime");
        session.set_user_info(UserInfo::new("root"));
        assert_eq!(session.user_info().username(), "root");

        // test channel
        assert_eq!(session.conn_info().channel, Channel::Mysql);
        assert_eq!(
            session.conn_info().client_host.ip().to_string(),
            "127.0.0.1"
        );
        assert_eq!(session.conn_info().client_host.port(), 9000);
    }
}
