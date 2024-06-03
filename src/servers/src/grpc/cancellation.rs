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

use std::future::Future;

use tokio::select;
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<tonic::Response<T>, tonic::Status>;

pub(crate) async fn with_cancellation_handler<Request, Cancellation, Response>(
    request: Request,
    cancellation: Cancellation,
) -> Result<Response>
where
    Request: Future<Output = Result<Response>> + Send + 'static,
    Cancellation: Future<Output = Result<Response>> + Send + 'static,
    Response: Send + 'static,
{
    let token = CancellationToken::new();
    // Will call token.cancel() when the future is dropped, such as when the client cancels the request
    let _drop_guard = token.clone().drop_guard();
    let select_task = tokio::spawn(async move {
        // Can select on token cancellation on any cancellable future while handling the request,
        // allowing for custom cleanup code or monitoring
        select! {
            res = request => res,
            _ = token.cancelled() => cancellation.await,
        }
    });

    select_task.await.unwrap()
}
