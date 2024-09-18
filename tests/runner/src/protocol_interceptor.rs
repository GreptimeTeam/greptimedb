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

use sqlness::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};
use sqlness::SqlnessError;

pub const PROTOCOL_KEY: &str = "protocol";
pub const POSTGRES: &str = "postgres";
pub const MYSQL: &str = "mysql";
pub const PREFIX: &str = "PROTOCOL";

pub struct ProtocolInterceptor {
    protocol: String,
}

impl Interceptor for ProtocolInterceptor {
    fn before_execute(&self, _: &mut Vec<String>, context: &mut sqlness::QueryContext) {
        context
            .context
            .insert(PROTOCOL_KEY.to_string(), self.protocol.clone());
    }
}

pub struct ProtocolInterceptorFactory;

impl InterceptorFactory for ProtocolInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        let protocol = ctx.to_lowercase();
        match protocol.as_str() {
            POSTGRES | MYSQL => Ok(Box::new(ProtocolInterceptor { protocol })),
            _ => Err(SqlnessError::InvalidContext {
                prefix: PREFIX.to_string(),
                msg: format!("Unsupported protocol: {}", ctx),
            }),
        }
    }
}
