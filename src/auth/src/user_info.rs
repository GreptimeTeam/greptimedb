// Copyright 2023 Greptime Team
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

use std::fmt::Debug;
use std::sync::Arc;

pub trait UserInfo: Debug + Sync + Send {
    fn username(&self) -> &str;
}

#[derive(Debug)]
pub(crate) struct DefaultUserInfo {
    username: String,
}

impl DefaultUserInfo {
    pub(crate) fn with_name(username: impl Into<String>) -> Arc<dyn UserInfo> {
        Arc::new(Self {
            username: username.into(),
        })
    }
}

impl UserInfo for DefaultUserInfo {
    fn username(&self) -> &str {
        self.username.as_str()
    }
}
