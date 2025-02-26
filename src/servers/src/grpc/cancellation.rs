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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time;
    use tonic::Response;

    use super::*;

    #[tokio::test]
    async fn test_request_completes_first() {
        let request = async { Ok(Response::new("Request Completed")) };

        let cancellation = async {
            time::sleep(Duration::from_secs(1)).await;
            Ok(Response::new("Cancelled"))
        };

        let result = with_cancellation_handler(request, cancellation).await;
        assert_eq!(result.unwrap().into_inner(), "Request Completed");
    }

    #[tokio::test]
    async fn test_cancellation_when_dropped() {
        let (tx, mut rx) = mpsc::channel(2);
        let tx_cloned = tx.clone();
        let request = async move {
            time::sleep(Duration::from_secs(1)).await;
            tx_cloned.send("Request Completed").await.unwrap();
            Ok(Response::new("Completed"))
        };
        let cancellation = async move {
            tx.send("Request Cancelled").await.unwrap();
            Ok(Response::new("Cancelled"))
        };

        let response_future = with_cancellation_handler(request, cancellation);
        // It will drop the `response_future` and then call the `cancellation` future
        let result = time::timeout(Duration::from_millis(50), response_future).await;

        assert!(result.is_err(), "Expected timeout error");
        assert_eq!("Request Cancelled", rx.recv().await.unwrap())
    }
}
