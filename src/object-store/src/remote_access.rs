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

//! Access policy for object storage locations supplied by SQL statements.

use std::sync::Arc;

use common_telemetry::warn;
use http::{Request, Response};
use opendal::{
    Buffer, Error, ErrorKind, HttpBody, HttpRedirect, HttpTransport, HttpTransporter,
    OperationContext,
};
use opendal_http_transport_reqwest::ReqwestTransport;
use snafu::ResultExt;
use url::Url;

use crate::ObjectStore;
use crate::error::{self, Result};

/// How object stores built from SQL statements, such as `COPY`, `COPY DATABASE`,
/// `EXPORT/IMPORT DATABASE` and external tables, may reach storage.
#[derive(Clone, Debug)]
pub struct RemoteStorageAccess {
    /// Operation context with the restricted HTTP transport, if allowed hosts are set.
    restricted_context: Option<OperationContext>,
    allow_server_credentials: bool,
}

impl RemoteStorageAccess {
    /// No host restriction, and server credentials are allowed.
    pub fn unrestricted() -> Self {
        Self {
            restricted_context: None,
            allow_server_credentials: true,
        }
    }

    /// Creates the access policy. An empty `allowed_hosts` means no host restriction.
    pub fn new(allowed_hosts: &[String], allow_server_credentials: bool) -> Result<Self> {
        let restricted_context = if allowed_hosts.is_empty() {
            None
        } else {
            let allowlist = HostAllowlist::parse(allowed_hosts)?;
            // Redirects are not followed, otherwise the client could reach a host
            // that was never checked. Other settings match the default transport
            // used for SQL object stores without a restriction.
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .context(error::BuildHttpClientSnafu)?;
            let transport = AllowlistTransport {
                inner: ReqwestTransport::new(client),
                allowlist: Arc::new(allowlist),
            };
            Some(OperationContext::new().with_http_transport(HttpTransporter::new(transport)))
        };
        Ok(Self {
            restricted_context,
            allow_server_credentials,
        })
    }

    /// Makes `store` send its requests through the restricted transport, if any.
    pub fn apply(&self, store: ObjectStore) -> ObjectStore {
        match &self.restricted_context {
            Some(context) => store.with_context(context.clone()),
            None => store,
        }
    }

    /// Whether a store whose connection options carry no credentials may fall back to the
    /// server's own credentials.
    pub fn allow_server_credentials(&self) -> bool {
        self.allow_server_credentials
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HostPattern {
    Exact(String),
    /// Matches any subdomain of the domain, not the domain itself.
    Subdomain(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AllowedHost {
    host: HostPattern,
    /// `None` matches any port.
    port: Option<u16>,
}

/// Hosts that SQL object stores may connect to.
///
/// Entries are `host`, `host:port`, `*.domain` or `*.domain:port`. IPv6 addresses
/// are written in brackets, e.g. `[::1]:9000`. Hosts are normalized the same way
/// as request URLs, so `127.1` and `127.0.0.1` are the same host.
#[derive(Debug, Clone)]
pub(crate) struct HostAllowlist {
    entries: Vec<AllowedHost>,
}

impl HostAllowlist {
    pub(crate) fn parse(entries: &[String]) -> Result<Self> {
        let entries = entries
            .iter()
            .map(|entry| {
                parse_allowed_host(entry).map_err(|reason| {
                    error::InvalidAllowedHostSnafu {
                        entry: entry.clone(),
                        reason,
                    }
                    .build()
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { entries })
    }

    /// Checks the URL a request is sent to.
    fn check(&self, target: &str) -> opendal::Result<()> {
        let url = Url::parse(target).map_err(|err| {
            Error::new(ErrorKind::PermissionDenied, "request URL is invalid").set_source(err)
        })?;
        let (Some(host), Some(port)) = (url.host_str(), url.port_or_known_default()) else {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "request URL has no host or port",
            ));
        };
        let is_domain = matches!(url.host(), Some(url::Host::Domain(_)));
        let allowed = self.entries.iter().any(|entry| {
            let host_matches = match &entry.host {
                HostPattern::Exact(expected) => host == expected,
                HostPattern::Subdomain(domain) => {
                    is_domain
                        && host
                            .strip_suffix(domain.as_str())
                            .is_some_and(|prefix| prefix.len() > 1 && prefix.ends_with('.'))
                }
            };
            host_matches && entry.port.is_none_or(|expected| expected == port)
        });
        if allowed {
            Ok(())
        } else {
            // Credential providers swallow this error and try the next one, so
            // the log is the only trace of a blocked credential request.
            warn!("Blocked request to {host}:{port}, not in query.remote_allowed_hosts");
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "host is not in query.remote_allowed_hosts",
            )
            .with_context("host", format!("{host}:{port}")))
        }
    }
}

fn parse_allowed_host(entry: &str) -> std::result::Result<AllowedHost, String> {
    let entry = entry.trim();
    let (wildcard, rest) = match entry.strip_prefix("*.") {
        Some(rest) => (true, rest),
        None => (false, entry),
    };
    if rest.is_empty() || rest.contains(['/', '\\', '@', '*', '?', '#']) {
        return Err("expected `host`, `host:port` or `*.domain`".to_string());
    }

    let (host, port) = if rest.starts_with('[') {
        let end = rest
            .find(']')
            .ok_or_else(|| "unterminated IPv6 address".to_string())?;
        let port = &rest[end + 1..];
        let port = if port.is_empty() {
            None
        } else {
            Some(
                port.strip_prefix(':')
                    .ok_or_else(|| "unexpected characters after IPv6 address".to_string())?,
            )
        };
        (&rest[..=end], port)
    } else {
        match rest.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (rest, None),
        }
    };
    let port = port
        .map(|port| {
            port.parse::<u16>()
                .map_err(|_| format!("invalid port `{port}`"))
        })
        .transpose()?;

    let url = Url::parse(&format!("http://{host}/")).map_err(|err| err.to_string())?;
    let normalized = url
        .host_str()
        .ok_or_else(|| "missing host".to_string())?
        .to_string();
    let host = if wildcard {
        if !matches!(url.host(), Some(url::Host::Domain(_))) {
            return Err("wildcard entries must use a domain name".to_string());
        }
        HostPattern::Subdomain(normalized)
    } else {
        HostPattern::Exact(normalized)
    };
    Ok(AllowedHost { host, port })
}

struct AllowlistTransport {
    inner: ReqwestTransport,
    allowlist: Arc<HostAllowlist>,
}

impl HttpTransport for AllowlistTransport {
    async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.allowlist.check(&req.uri().to_string())?;
        // The reqwest transport sends the request to this URL instead of the
        // request URI when a service attaches the extension.
        if let Some(redirect) = req.extensions().get::<HttpRedirect>() {
            self.allowlist.check(redirect.uri().original_uri())?;
        }
        self.inner.fetch(req).await
    }
}

#[cfg(test)]
mod tests {
    use opendal::services::{Gcs, S3};

    use super::*;
    use crate::ObjectStore;
    use crate::test_util::{MockHttpServer, http_response as response};

    fn allowlist(entries: &[&str]) -> HostAllowlist {
        HostAllowlist::parse(&entries.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn test_allowlist_matching() {
        let list = allowlist(&[
            "S3.US-EAST-1.amazonaws.com",
            "*.storage.example.com",
            "minio.internal:9000",
            "127.1:9000",
            "[::1]:9001",
        ]);
        for allowed in [
            "https://s3.us-east-1.amazonaws.com/bucket/key",
            "http://s3.us-east-1.amazonaws.com:8080/",
            "https://bucket.storage.example.com/key",
            "https://a.b.storage.example.com/key",
            "http://minio.internal:9000/bucket",
            "http://127.0.0.1:9000/bucket",
            "http://2130706433:9000/bucket",
            "http://[::1]:9001/bucket",
        ] {
            assert!(list.check(allowed).is_ok(), "{allowed}");
        }
        for denied in [
            "https://storage.example.com/key",
            "https://evilstorage.example.com/key",
            "https://storage.example.com.evil.com/key",
            "http://minio.internal/bucket",
            "http://minio.internal:9001/bucket",
            "http://127.0.0.1:9001/bucket",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]:9000/bucket",
        ] {
            let err = list.check(denied).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{denied}");
        }
    }

    #[test]
    fn test_allowlist_rejects_invalid_entries() {
        for entry in [
            "",
            "http://example.com",
            "example.com/path",
            "user@example.com",
            "*.127.0.0.1",
            "*.[::1]",
            "example.com:port",
            "example.com:70000",
            "[::1",
            "[::1]9000",
            "minio.internal\\evil.com",
            "a*.example.com",
        ] {
            assert!(
                HostAllowlist::parse(&[entry.to_string()]).is_err(),
                "{entry}"
            );
        }
    }

    fn restricted(entries: &[String]) -> OperationContext {
        RemoteStorageAccess::new(entries, true)
            .unwrap()
            .restricted_context
            .unwrap()
    }

    fn s3_store(endpoint: Option<&str>, region: &str, ctx: OperationContext) -> ObjectStore {
        let mut builder = S3::default()
            .bucket("bucket")
            .region(region)
            .access_key_id("ak")
            .secret_access_key("sk");
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(endpoint);
        }
        ObjectStore::new(builder).unwrap().with_context(ctx)
    }

    #[tokio::test]
    async fn test_restricted_transport_targets() {
        let allowed = MockHttpServer::start(|head| {
            if head.starts_with("GET") {
                response("200 OK", &[], "data")
            } else {
                response("200 OK", &[("etag", "\"e\"".to_string())], "")
            }
        })
        .await;
        let denied = MockHttpServer::start(|_| response("200 OK", &[], "secret")).await;
        let (allowed_port, denied_port) = (allowed.port, denied.port);
        let ctx = restricted(&[format!("127.0.0.1:{allowed_port}")]);

        // An allowed target can be written and read.
        let store = s3_store(
            Some(&format!("http://127.0.0.1:{allowed_port}")),
            "us-east-1",
            ctx.clone(),
        );
        store.write("key", "data").await.unwrap();
        assert_eq!(store.read("key").await.unwrap().to_vec(), b"data");
        assert_eq!(allowed.request_count(), 2);

        // Endpoint pointing to another port.
        let store = s3_store(
            Some(&format!("http://127.0.0.1:{denied_port}")),
            "us-east-1",
            ctx.clone(),
        );
        let err = store.read("key").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);

        // Without an endpoint, the region is substituted into the default endpoint
        // template, which can change the host.
        let store = s3_store(None, &format!("x@127.0.0.1:{denied_port}/#"), ctx.clone());
        let err = store.read("key").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(denied.request_count(), 0);
    }

    #[tokio::test]
    async fn test_restricted_transport_checks_redirect_extension() {
        let denied = MockHttpServer::start(|_| response("200 OK", &[], "secret")).await;
        let allowed = MockHttpServer::start(|_| response("200 OK", &[], "data")).await;
        let ctx = restricted(&[format!("127.0.0.1:{}", allowed.port)]);
        let mut req = Request::get(format!("http://127.0.0.1:{}/key", allowed.port))
            .body(Buffer::new())
            .unwrap();
        req.extensions_mut().insert(HttpRedirect::new(
            format!("http://127.0.0.1:{}/key", denied.port)
                .parse()
                .unwrap(),
        ));
        let Err(err) = ctx.http_transport().fetch(req).await else {
            panic!("the redirect target must be checked");
        };
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(allowed.request_count() + denied.request_count(), 0);
    }

    #[tokio::test]
    async fn test_restricted_transport_does_not_follow_redirects() {
        let denied = MockHttpServer::start(|_| response("200 OK", &[], "secret")).await;
        let location = format!("http://127.0.0.1:{}/bucket/key", denied.port);
        let allowed = MockHttpServer::start(move |_| {
            response("302 Found", &[("location", location.clone())], "")
        })
        .await;
        let allowed_port = allowed.port;
        let ctx = restricted(&[format!("127.0.0.1:{allowed_port}")]);
        let store = s3_store(
            Some(&format!("http://127.0.0.1:{allowed_port}")),
            "us-east-1",
            ctx,
        );
        assert!(store.read("key").await.is_err());
        assert_eq!(allowed.request_count(), 1);
        assert_eq!(denied.request_count(), 0);
    }

    #[tokio::test]
    async fn test_restricted_transport_checks_gcs_resumable_upload_session() {
        let denied = MockHttpServer::start(|_| response("200 OK", &[], "{}")).await;
        let session = format!("http://127.0.0.1:{}/upload/session", denied.port);
        let allowed = MockHttpServer::start(move |_| {
            response("200 OK", &[("location", session.clone())], "")
        })
        .await;
        let allowed_port = allowed.port;
        let ctx = restricted(&[format!("127.0.0.1:{allowed_port}")]);
        let builder = Gcs::default()
            .bucket("bucket")
            .endpoint(&format!("http://127.0.0.1:{allowed_port}"))
            .skip_signature();
        let store = ObjectStore::new(builder).unwrap().with_context(ctx);

        // Conditional writes use resumable uploads, whose later requests go to
        // the session URL from the `Location` header.
        let result = store
            .write_with("key", vec![0u8; 1024])
            .if_not_exists(true)
            .await;
        assert_eq!(result.unwrap_err().kind(), ErrorKind::PermissionDenied);
        // The upload session was created on the allowed host, but no data was
        // sent to the session URL it returned.
        assert_eq!(allowed.request_count(), 1);
        assert_eq!(denied.request_count(), 0);
    }
}
