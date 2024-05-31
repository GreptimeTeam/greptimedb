// This file is copied from https://github.com/tokio-rs/axum/blob/axum-v0.6.20/axum/src/test_helpers/test_client.rs

//! Axum Test Client
//!
//! ```rust
//! use axum::Router;
//! use axum::http::StatusCode;
//! use axum::routing::get;
//! use crate::servers::http::test_helpers::TestClient;
//!
//! let async_block = async {
//!     // you can replace this Router with your own app
//!     let app = Router::new().route("/", get(|| async {}));
//!
//!     // initiate the TestClient with the previous declared Router
//!     let client = TestClient::new(app);
//!
//!     let res = client.get("/").await;
//!     assert_eq!(res.status(), StatusCode::OK);
//! };
//!
//! // Create a runtime for executing the async block. This runtime is local
//! // to the main function and does not require any global setup.
//! let runtime = tokio::runtime::Builder::new_current_thread()
//!     .enable_all()
//!     .build()
//!     .unwrap();
//!
//! // Use the local runtime to block on the async block.
//! runtime.block_on(async_block);
//! ```

use std::convert::TryFrom;
use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;

use axum::body::HttpBody;
use axum::BoxError;
use bytes::Bytes;
use common_telemetry::info;
use http::header::{HeaderName, HeaderValue};
use http::{Request, StatusCode};
use hyper::service::Service;
use hyper::{Body, Server};
use tower::make::Shared;

/// Test client to Axum servers.
pub struct TestClient {
    client: reqwest::Client,
    addr: SocketAddr,
}

impl TestClient {
    /// Create a new test client.
    pub fn new<S, ResBody>(svc: S) -> Self
    where
        S: Service<Request<Body>, Response = http::Response<ResBody>> + Clone + Send + 'static,
        ResBody: HttpBody + Send + 'static,
        ResBody::Data: Send,
        ResBody::Error: Into<BoxError>,
        S::Future: Send,
        S::Error: Into<BoxError>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Could not bind ephemeral socket");
        let addr = listener.local_addr().unwrap();
        info!("Listening on {}", addr);

        tokio::spawn(async move {
            let server = Server::from_tcp(listener).unwrap().serve(Shared::new(svc));
            server.await.expect("server error");
        });

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        TestClient { client, addr }
    }

    /// Returns the base URL (http://ip:port) for this TestClient
    ///
    /// this is useful when trying to check if Location headers in responses
    /// are generated correctly as Location contains an absolute URL
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Create a GET request.
    pub fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.get(format!("http://{}{}", self.addr, url)),
        }
    }

    /// Create a HEAD request.
    pub fn head(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.head(format!("http://{}{}", self.addr, url)),
        }
    }

    /// Create a POST request.
    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.post(format!("http://{}{}", self.addr, url)),
        }
    }

    /// Create a PUT request.
    pub fn put(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.put(format!("http://{}{}", self.addr, url)),
        }
    }

    /// Create a PATCH request.
    pub fn patch(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.patch(format!("http://{}{}", self.addr, url)),
        }
    }

    /// Create a DELETE request.
    pub fn delete(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.delete(format!("http://{}{}", self.addr, url)),
        }
    }
}

/// Builder for test requests.
pub struct RequestBuilder {
    builder: reqwest::RequestBuilder,
}

impl RequestBuilder {
    pub async fn send(self) -> TestResponse {
        TestResponse {
            response: self.builder.send().await.unwrap(),
        }
    }

    /// Set the request body.
    pub fn body(mut self, body: impl Into<reqwest::Body>) -> Self {
        self.builder = self.builder.body(body);
        self
    }

    /// Set the request forms.
    pub fn form<T: serde::Serialize + ?Sized>(mut self, form: &T) -> Self {
        self.builder = self.builder.form(&form);
        self
    }

    /// Set the request JSON body.
    pub fn json<T>(mut self, json: &T) -> Self
    where
        T: serde::Serialize,
    {
        self.builder = self.builder.json(json);
        self
    }

    /// Set a request header.
    pub fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        // TODO(tisonkun): revert once http bump to 1.x
        let key: HeaderName = key.try_into().map_err(Into::into).unwrap();
        let key = reqwest::header::HeaderName::from_bytes(key.as_ref()).unwrap();

        let value: HeaderValue = value.try_into().map_err(Into::into).unwrap();
        let value = reqwest::header::HeaderValue::from_bytes(value.as_bytes()).unwrap();

        self.builder = self.builder.header(key, value);

        self
    }

    /// Set a request multipart form.
    pub fn multipart(mut self, form: reqwest::multipart::Form) -> Self {
        self.builder = self.builder.multipart(form);
        self
    }
}

/// A wrapper around [`reqwest::Response`] that provides common methods with internal `unwrap()`s.
///
/// This is convenient for tests where panics are what you want. For access to
/// non-panicking versions or the complete `Response` API use `into_inner()` or
/// `as_ref()`.
pub struct TestResponse {
    response: reqwest::Response,
}

impl TestResponse {
    /// Get the response body as text.
    pub async fn text(self) -> String {
        self.response.text().await.unwrap()
    }

    /// Get the response body as bytes.
    pub async fn bytes(self) -> Bytes {
        self.response.bytes().await.unwrap()
    }

    /// Get the response body as JSON.
    pub async fn json<T>(self) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        self.response.json().await.unwrap()
    }

    /// Get the response status.
    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.response.status().as_u16()).unwrap()
    }

    /// Get the response headers.
    pub fn headers(&self) -> http::HeaderMap {
        // TODO(tisonkun): revert once http bump to 1.x
        let mut headers = http::HeaderMap::new();
        for (key, value) in self.response.headers() {
            let key = HeaderName::from_str(key.as_str()).unwrap();
            let value = HeaderValue::from_bytes(value.as_bytes()).unwrap();
            headers.insert(key, value);
        }
        headers
    }

    /// Get the response in chunks.
    pub async fn chunk(&mut self) -> Option<Bytes> {
        self.response.chunk().await.unwrap()
    }

    /// Get the response in chunks as text.
    pub async fn chunk_text(&mut self) -> Option<String> {
        let chunk = self.chunk().await?;
        Some(String::from_utf8(chunk.to_vec()).unwrap())
    }

    /// Get the inner [`reqwest::Response`] for less convenient but more complete access.
    pub fn into_inner(self) -> reqwest::Response {
        self.response
    }
}

impl AsRef<reqwest::Response> for TestResponse {
    fn as_ref(&self) -> &reqwest::Response {
        &self.response
    }
}
