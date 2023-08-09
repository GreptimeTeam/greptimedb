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

pub(crate) mod static_user_provider;

use std::sync::Arc;

use crate::common::{Identity, Password};
use crate::error::Result;
use crate::UserInfo;

#[async_trait::async_trait]
pub trait UserProvider: Send + Sync {
    fn name(&self) -> &str;

    /// [`authenticate`] checks whether a user is valid and allowed to access the database.
    async fn authenticate(
        &self,
        id: Identity<'_>,
        password: Password<'_>,
    ) -> Result<Arc<dyn UserInfo>>;

    /// [`authorize`] checks whether a connection request
    /// from a certain user to a certain catalog/schema is legal.
    /// This method should be called after [`authenticate`].
    async fn authorize(
        &self,
        catalog: &str,
        schema: &str,
        user_info: &Arc<dyn UserInfo>,
    ) -> Result<()>;

    /// [`auth`] is a combination of [`authenticate`] and [`authorize`].
    /// In most cases it's preferred for both convenience and performance.
    async fn auth(
        &self,
        id: Identity<'_>,
        password: Password<'_>,
        catalog: &str,
        schema: &str,
    ) -> Result<Arc<dyn UserInfo>> {
        let user_info = self.authenticate(id, password).await?;
        self.authorize(catalog, schema, &user_info).await?;
        Ok(user_info)
    }
}
