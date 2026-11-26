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

//! Which identity SQL object stores use. These tests plant usable server
//! credentials in the environment and check that they are only used when the
//! SQL options carry no credentials and the policy allows it. The credentials
//! are set on a child process running the ignored `*_in_child` test, so they
//! never reach other tests in this process.

use std::collections::HashMap;
use std::process::Command;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use object_store::test_util::{MockHttpServer, http_response};

use super::*;
use crate::error::Error;

/// Runs the ignored test `name` of this module in a child process configured by `configure`.
fn run_in_child(name: &str, configure: impl FnOnce(&mut Command)) {
    let module = module_path!().split_once("::").unwrap().1;
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args([
        &format!("{module}::{name}"),
        "--exact",
        "--ignored",
        "--nocapture",
    ]);
    configure(&mut command);
    let output = command.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn access(port: u16, allow_server_credentials: bool) -> RemoteStorageAccess {
    RemoteStorageAccess::new(&[format!("127.0.0.1:{port}")], allow_server_credentials).unwrap()
}

fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

async fn build(
    url: &str,
    connection: &HashMap<String, String>,
    access: &RemoteStorageAccess,
) -> Result<ObjectStore> {
    build_backend(url, connection, &LocalFileAccess::Disabled, access).await
}

#[test]
fn test_s3_identity_selection() {
    run_in_child("s3_identity_selection_in_child", |command| {
        command
            .env("AWS_ACCESS_KEY_ID", "server-ak")
            .env("AWS_SECRET_ACCESS_KEY", "server-sk")
            .env_remove("AWS_SESSION_TOKEN")
            .env("AWS_EC2_METADATA_DISABLED", "true");
    });
}

#[tokio::test]
#[ignore = "run by test_s3_identity_selection with planted credentials"]
async fn s3_identity_selection_in_child() {
    let server = MockHttpServer::start(|head| {
        if head.contains("Credential=bad-ak/") {
            http_response("403 Forbidden", &[], "")
        } else {
            http_response("200 OK", &[], "data")
        }
    })
    .await;
    let endpoint = format!("http://127.0.0.1:{}", server.port);
    let base = [("endpoint", endpoint.as_str()), ("region", "us-east-1")];
    let with = |extra: &[(&str, &str)]| options(&[&base[..], extra].concat());
    let strict = access(server.port, false);
    let permissive = access(server.port, true);
    let url = "s3://bucket/dir/";

    // The planted server credentials work when the policy allows them.
    let store = build(url, &with(&[]), &permissive).await.unwrap();
    store.read("key").await.unwrap();
    assert!(server.request_heads()[0].contains("Credential=server-ak/"));

    let explicit = with(&[("access_key_id", "sql-ak"), ("secret_access_key", "sql-sk")]);
    for access in [&strict, &permissive] {
        let store = build(url, &explicit, access).await.unwrap();
        store.read("key").await.unwrap();
        let heads = server.request_heads();
        assert!(heads.last().unwrap().contains("Credential=sql-ak/"));
    }

    // Wrong explicit credentials fail without trying the server's.
    let bad = with(&[("access_key_id", "bad-ak"), ("secret_access_key", "sk")]);
    let store = build(url, &bad, &permissive).await.unwrap();
    store.read("key").await.unwrap_err();

    let requests = server.request_count();
    assert!(
        server
            .request_heads()
            .iter()
            .skip(1)
            .all(|head| !head.contains("server-ak"))
    );

    // Incomplete credentials and missing credentials in strict mode are
    // rejected before any request.
    for (connection, access) in [
        (with(&[("access_key_id", "sql-ak")]), &permissive),
        (with(&[("session_token", "token")]), &permissive),
    ] {
        assert!(matches!(
            build(url, &connection, access).await,
            Err(Error::InvalidConnection { .. })
        ));
    }
    assert!(matches!(
        build(url, &with(&[]), &strict).await,
        Err(Error::ServerCredentialsDisallowed { .. })
    ));
    assert_eq!(server.request_count(), requests);
}

#[test]
fn test_gcs_identity_selection() {
    let dir = common_test_util::temp_dir::create_temp_dir("gcs_identity");
    run_in_child("gcs_identity_selection_in_child", |command| {
        command.env(
            "GOOGLE_APPLICATION_CREDENTIALS",
            dir.path().join("credential.json"),
        );
    });
}

#[tokio::test]
#[ignore = "run by test_gcs_identity_selection with planted credentials"]
async fn gcs_identity_selection_in_child() {
    let server = MockHttpServer::start(|head| {
        if head.starts_with("POST /token") {
            http_response(
                "200 OK",
                &[("content-type", "application/json".to_string())],
                r#"{"access_token":"server-token","expires_in":3600,"token_type":"Bearer","issued_token_type":"urn:ietf:params:oauth:token-type:access_token"}"#,
            )
        } else {
            http_response("200 OK", &[], "data")
        }
    })
    .await;
    // Server credentials: an external account whose token exchange is served
    // by the mock, so every use of them is visible.
    let credential_file =
        std::path::PathBuf::from(std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").unwrap());
    let subject = credential_file.with_file_name("subject");
    std::fs::write(&subject, "subject-token").unwrap();
    let server_credential = serde_json::json!({
        "type": "external_account",
        "audience": "audience",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": format!("http://127.0.0.1:{}/token", server.port),
        "credential_source": {
            "file": subject.display().to_string(),
            "format": { "type": "text" },
        },
    });
    std::fs::write(&credential_file, server_credential.to_string()).unwrap();

    let endpoint = format!("http://127.0.0.1:{}", server.port);
    let strict = access(server.port, false);
    let permissive = access(server.port, true);
    let url = "gcs://bucket/dir/";
    let with =
        |extra: &[(&str, &str)]| options(&[&[("endpoint", endpoint.as_str())][..], extra].concat());

    let store = build(url, &with(&[]), &permissive).await.unwrap();
    store.read("key").await.unwrap();
    let heads = server.request_heads();
    assert!(heads[0].starts_with("POST /token"));
    assert!(heads[1].contains("Bearer server-token"));

    // A service account key that cannot be used fails without trying the
    // server's credentials.
    let service_account = BASE64_STANDARD.encode(
        r#"{"type":"service_account","private_key":"invalid","client_email":"a@b.iam.gserviceaccount.com"}"#,
    );
    for access in [&strict, &permissive] {
        let store = build(url, &with(&[("credential", &service_account)]), access)
            .await
            .unwrap();
        store.read("key").await.unwrap_err();
    }

    // Credential types other than service accounts are rejected even when they
    // only point to allowed hosts.
    let external_account = BASE64_STANDARD.encode(server_credential.to_string());
    assert!(matches!(
        build(
            url,
            &with(&[("credential", &external_account)]),
            &permissive
        )
        .await,
        Err(Error::InvalidConnection { .. })
    ));
    assert!(matches!(
        build(url, &with(&[]), &strict).await,
        Err(Error::ServerCredentialsDisallowed { .. })
    ));

    // Credential requests go through the same host check as data requests.
    let other_host = access(server.port.wrapping_add(1), true);
    let store = build(url, &with(&[]), &other_host).await.unwrap();
    store.read("key").await.unwrap_err();
    assert_eq!(server.request_count(), 2);
}

#[tokio::test]
async fn test_azblob_and_oss_credential_selection() {
    let strict = access(1, false);

    // The account key is only used with a known account name.
    let key_without_name = options(&[("endpoint", "http://127.0.0.1:1"), ("account_key", "a2V5")]);
    assert!(matches!(
        build("azblob://container/dir/", &key_without_name, &strict).await,
        Err(Error::InvalidConnection { .. })
    ));
    for connection in [
        options(&[
            ("endpoint", "http://127.0.0.1:1"),
            ("account_name", "account"),
            ("account_key", "a2V5"),
        ]),
        options(&[
            ("endpoint", "https://account.blob.core.windows.net"),
            ("account_key", "a2V5"),
        ]),
        options(&[("endpoint", "http://127.0.0.1:1"), ("sas_token", "sig")]),
    ] {
        build("azblob://container/dir/", &connection, &strict)
            .await
            .unwrap();
    }
    assert!(matches!(
        build(
            "azblob://container/dir/",
            &options(&[
                ("endpoint", "http://127.0.0.1:1"),
                ("account_name", "account")
            ]),
            &strict
        )
        .await,
        Err(Error::ServerCredentialsDisallowed { .. })
    ));

    // Anonymous OSS access needs no credentials.
    let endpoint = ("endpoint", "http://127.0.0.1:1");
    build(
        "oss://bucket/dir/",
        &options(&[endpoint, ("allow_anonymous", "true")]),
        &strict,
    )
    .await
    .unwrap();
    assert!(matches!(
        build("oss://bucket/dir/", &options(&[endpoint]), &strict).await,
        Err(Error::ServerCredentialsDisallowed { .. })
    ));
    assert!(matches!(
        build(
            "oss://bucket/dir/",
            &options(&[endpoint, ("access_key_id", "ak")]),
            &strict
        )
        .await,
        Err(Error::InvalidConnection { .. })
    ));
}

#[tokio::test]
async fn test_allowed_hosts_apply_to_every_backend() {
    let denied = MockHttpServer::start(|_| http_response("200 OK", &[], "data")).await;
    let endpoint = format!("http://127.0.0.1:{}", denied.port);
    let access = access(denied.port.wrapping_add(1), true);
    for (url, connection) in [
        (
            "s3://bucket/dir/",
            options(&[
                ("endpoint", &endpoint),
                ("region", "us-east-1"),
                ("access_key_id", "ak"),
                ("secret_access_key", "sk"),
            ]),
        ),
        (
            "oss://bucket/dir/",
            options(&[("endpoint", &endpoint), ("allow_anonymous", "true")]),
        ),
        (
            "azblob://container/dir/",
            options(&[("endpoint", &endpoint), ("sas_token", "sig")]),
        ),
    ] {
        let store = build(url, &connection, &access).await.unwrap();
        let err = store.read("key").await.unwrap_err();
        assert_eq!(
            err.kind(),
            object_store::ErrorKind::PermissionDenied,
            "{url}: {err}"
        );
    }
    assert_eq!(denied.request_count(), 0);
}
