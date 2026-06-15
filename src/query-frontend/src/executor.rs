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

//! The boundary trait that runs the underlying query.
//!
//! The query frontend delegates the actual execution of a request to a
//! [`QueryExecutor`]. The response and error types are left associated so this
//! crate stays protocol-neutral and free of dependencies on `servers`,
//! `frontend`, `query`, or `session`. Concrete protocol layers implement this
//! trait over their own response/error types.

use async_trait::async_trait;

use crate::request::QueryFrontendRequest;

/// Executes a [`QueryFrontendRequest`] and produces a protocol-specific
/// response.
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// The protocol-specific successful response type.
    type Response: Send;
    /// The protocol-specific error type.
    type Error: Send;

    /// Executes the request, returning the response or an error.
    async fn execute(&self, request: QueryFrontendRequest) -> Result<Self::Response, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial executor that echoes the request's query back, used to confirm
    /// the trait can be implemented and awaited.
    struct EchoExecutor;

    #[async_trait]
    impl QueryExecutor for EchoExecutor {
        type Response = String;
        type Error = std::convert::Infallible;

        async fn execute(
            &self,
            request: QueryFrontendRequest,
        ) -> Result<Self::Response, Self::Error> {
            Ok(request.query)
        }
    }

    #[tokio::test]
    async fn executor_trait_is_implementable() {
        let executor = EchoExecutor;
        let request = crate::request::test_request();
        assert_eq!("up", executor.execute(request).await.unwrap());
    }
}
