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

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use auth::tests::{DatabaseAuthInfo, MockUserProvider};
use auth::{
    BEARER_TOKEN_USER, Identity, Password, UserInfoRef, UserProvider, UserProviderRef,
    format_pg_scram_sha256_password_verifier, user_provider_from_option,
};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_runtime::Builder as RuntimeBuilder;
use common_runtime::runtime::BuilderBuild;
use pgwire::api::Type;
use postgres_types::FromSql;
use rand::Rng;
use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::{Error, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName};
use servers::error::Result;
use servers::install_default_crypto_provider;
use servers::postgres::PostgresServer;
use servers::server::Server;
use servers::tls::{ReloadableTlsServerConfig, TlsOption};
use table::TableRef;
use table::test_util::MemTable;
use tokio_postgres::{Client, Error as PgError, NoTls, SimpleQueryMessage};

use crate::create_testing_instance;

#[derive(Default)]
struct BearerProvider {
    authentications: AtomicUsize,
    authorizations: AtomicUsize,
}

#[async_trait]
impl UserProvider for BearerProvider {
    fn name(&self) -> &str {
        "bearer-test"
    }

    async fn authenticate(
        &self,
        _: Identity<'_>,
        _: Password<'_>,
    ) -> auth::error::Result<UserInfoRef> {
        unreachable!("the bearer sentinel must not use password authentication")
    }

    async fn authenticate_bearer_token(
        &self,
        token: &str,
        catalog: &str,
    ) -> auth::error::Result<UserInfoRef> {
        assert_eq!("signed-token", token);
        assert_eq!(DEFAULT_CATALOG_NAME, catalog);
        self.authentications.fetch_add(1, Ordering::Relaxed);
        Ok(auth::userinfo_by_name(Some("alice".to_string())))
    }

    async fn authorize(
        &self,
        catalog: &str,
        schema: &str,
        user_info: &UserInfoRef,
    ) -> auth::error::Result<()> {
        assert_eq!(DEFAULT_CATALOG_NAME, catalog);
        assert_eq!(DEFAULT_SCHEMA_NAME, schema);
        assert_eq!("alice", user_info.username());
        self.authorizations.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn create_postgres_server(
    table: TableRef,
    check_pwd: bool,
    tls: TlsOption,
    auth_info: Option<DatabaseAuthInfo>,
) -> Result<Box<dyn Server>> {
    let user_provider: Option<UserProviderRef> = if check_pwd {
        let mut provider = MockUserProvider::default();
        if let Some(info) = auth_info {
            provider.set_authorization_info(info);
        }
        Some(Arc::new(provider))
    } else {
        None
    };
    create_postgres_server_with_user_provider(table, tls, user_provider)
}

fn create_postgres_server_with_user_provider(
    table: TableRef,
    tls: TlsOption,
    user_provider: Option<UserProviderRef>,
) -> Result<Box<dyn Server>> {
    create_postgres_server_inner(table, tls, user_provider).map(|(server, _)| server)
}

fn create_postgres_server_inner(
    table: TableRef,
    tls: TlsOption,
    user_provider: Option<UserProviderRef>,
) -> Result<(Box<dyn Server>, Arc<crate::RecordingCopyInHandler>)> {
    let instance = Arc::new(create_testing_instance(table));
    let copy_in_handler = Arc::new(crate::RecordingCopyInHandler::new(
        instance.catalog_manager(),
    ));
    let io_runtime = RuntimeBuilder::default()
        .worker_threads(4)
        .thread_name("postgres-io-handlers")
        .build()
        .unwrap();

    let tls_server_config = Arc::new(
        ReloadableTlsServerConfig::try_new(tls.clone())
            .expect("Failed to load certificates and keys"),
    );

    Ok((
        Box::new(PostgresServer::new(
            instance,
            copy_in_handler.clone(),
            tls.should_force_tls(),
            tls_server_config,
            0,
            io_runtime,
            user_provider,
            None,
        )),
        copy_in_handler,
    ))
}

async fn start_test_server_with_user_provider(
    user_provider: UserProviderRef,
    tls: TlsOption,
) -> Result<(Box<dyn Server>, u16)> {
    common_telemetry::init_default_ut_logging();
    let _ = install_default_crypto_provider();

    let table = MemTable::default_numbers_table();
    let mut postgres_server =
        create_postgres_server_with_user_provider(table, tls, Some(user_provider))?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    postgres_server.start(listening).await.unwrap();
    let server_addr = postgres_server.bind_addr().unwrap();
    Ok((postgres_server, server_addr.port()))
}

#[tokio::test]
pub async fn test_start_postgres_server() -> Result<()> {
    let table = MemTable::default_numbers_table();

    let mut pg_server = create_postgres_server(table, false, Default::default(), None)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    pg_server.start(listening).await.unwrap();

    let result = pg_server.start(listening).await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Postgres server has been started.")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_pg_server_range() -> Result<()> {
    test_shutdown_pg_server(false).await.unwrap();
    test_shutdown_pg_server(true).await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_schema_validating() -> Result<()> {
    async fn generate_server(auth_info: DatabaseAuthInfo<'_>) -> Result<(Box<dyn Server>, u16)> {
        let table = MemTable::default_numbers_table();
        let mut postgres_server =
            create_postgres_server(table, true, Default::default(), Some(auth_info))?;
        let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        postgres_server.start(listening).await.unwrap();
        let server_addr = postgres_server.bind_addr().unwrap();
        let server_port = server_addr.port();
        Ok((postgres_server, server_port))
    }

    common_telemetry::init_default_ut_logging();
    let (pg_server, server_port) = generate_server(DatabaseAuthInfo {
        catalog: DEFAULT_CATALOG_NAME,
        schema: DEFAULT_SCHEMA_NAME,
        username: "greptime",
    })
    .await?;

    let _ = create_plain_connection(server_port, true).await.unwrap();
    pg_server.shutdown().await.unwrap();

    let (pg_server, server_port) = generate_server(DatabaseAuthInfo {
        catalog: DEFAULT_CATALOG_NAME,
        schema: DEFAULT_SCHEMA_NAME,
        username: "no_right_user",
    })
    .await?;

    let fail = create_plain_connection(server_port, true).await;
    assert!(fail.is_err());
    pg_server.shutdown().await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pg_scram_sha256_auth() -> Result<()> {
    let verifier =
        format_pg_scram_sha256_password_verifier(b"greptime", b"pg-scram-salt", 4096).unwrap();
    let user_provider =
        user_provider_from_option(&format!("static_user_provider:cmd:greptime={verifier}"))
            .unwrap();
    let (postgres_server, server_port) =
        start_test_server_with_user_provider(user_provider, Default::default()).await?;

    let client = create_plain_connection_with_credentials(server_port, "greptime", "greptime")
        .await
        .unwrap();
    let rows = client
        .simple_query("SELECT uint32s FROM numbers LIMIT 1")
        .await;
    assert_eq!(unwrap_results(rows.unwrap().as_ref())[0], "0");

    let wrong_password =
        create_plain_connection_with_credentials(server_port, "greptime", "wrong").await;
    assert!(wrong_password.is_err());

    let unknown_user =
        create_plain_connection_with_credentials(server_port, "not_found", "greptime").await;
    assert!(unknown_user.is_err());

    postgres_server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pg_cleartext_auth_fallback() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let table = MemTable::default_numbers_table();
    let mut postgres_server = create_postgres_server(table, true, Default::default(), None)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    postgres_server.start(listening).await.unwrap();
    let server_port = postgres_server.bind_addr().unwrap().port();

    let client = create_plain_connection_with_credentials(server_port, "greptime", "greptime")
        .await
        .unwrap();
    let rows = client
        .simple_query("SELECT uint32s FROM numbers LIMIT 1")
        .await;
    assert_eq!(unwrap_results(rows.unwrap().as_ref())[0], "0");

    let wrong_password =
        create_plain_connection_with_credentials(server_port, "greptime", "wrong").await;
    assert!(wrong_password.is_err());

    postgres_server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bearer_token_auth_over_cleartext() -> Result<()> {
    let provider = Arc::new(BearerProvider::default());
    let (server, port) =
        start_test_server_with_user_provider(provider.clone(), TlsOption::default()).await?;

    let client = create_plain_connection_with_credentials(port, BEARER_TOKEN_USER, "signed-token")
        .await
        .unwrap();
    let rows = client
        .simple_query("SELECT uint32s FROM numbers LIMIT 1")
        .await
        .unwrap();
    assert_eq!("0", unwrap_results(&rows)[0]);
    assert_eq!(1, provider.authentications.load(Ordering::Relaxed));
    assert_eq!(1, provider.authorizations.load(Ordering::Relaxed));

    server.shutdown().await?;
    Ok(())
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_pg_server(with_pwd: bool) -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();
    let mut postgres_server = create_postgres_server(table, with_pwd, Default::default(), None)?;
    let result = postgres_server.shutdown().await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Postgres server is not started.")
    );

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    postgres_server.start(listening).await.unwrap();
    let server_addr = postgres_server.bind_addr().unwrap();
    let server_port = server_addr.port();

    let mut join_handles = vec![];
    for _ in 0..2 {
        join_handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                match create_plain_connection(server_port, with_pwd).await {
                    Ok(connection) => {
                        match connection
                            .simple_query("SELECT uint32s FROM numbers LIMIT 1")
                            .await
                        {
                            Ok(rows) => {
                                let result_text = unwrap_results(&rows)[0];
                                let result: i32 = result_text.parse().unwrap();
                                assert_eq!(result, 0);
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(())
        }))
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    postgres_server.shutdown().await.unwrap();

    for handle in join_handles.iter_mut() {
        let result = handle.await.unwrap();
        assert!(result.is_err());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_query_pg_concurrently() -> Result<()> {
    let server_port = start_test_server(Default::default()).await?;

    let threads = 4;
    let expect_executed_queries_per_worker = 300;
    let mut join_handles = vec![];
    for _i in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut client = create_plain_connection(server_port, false).await.unwrap();

            for _k in 0..expect_executed_queries_per_worker {
                let expected: u32 = rand::rng().random_range(0..100);
                let result: u32 = unwrap_results(
                    client
                        .simple_query(&format!(
                            "SELECT uint32s FROM numbers WHERE uint32s = {expected}"
                        ))
                        .await
                        .unwrap()
                        .as_ref(),
                )[0]
                .parse()
                .unwrap();
                assert_eq!(result, expected);

                // 1/100 chance to reconnect
                let should_recreate_conn = expected == 1;
                if should_recreate_conn {
                    client = create_plain_connection(server_port, false).await.unwrap();
                }
            }
            expect_executed_queries_per_worker
        }))
    }
    let mut total_pending_queries = threads * expect_executed_queries_per_worker;
    for handle in join_handles.iter_mut() {
        total_pending_queries -= handle.await.unwrap();
    }
    assert_eq!(0, total_pending_queries);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_secure_prefer_client_plain() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    do_simple_query_with_secure_server(servers::tls::TlsMode::Prefer, false, false).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_secure_prefer_client_plain_with_pkcs8_priv_key() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    do_simple_query_with_secure_server(servers::tls::TlsMode::Prefer, false, true).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_secure_require_client_secure() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    do_simple_query_with_secure_server(servers::tls::TlsMode::Require, true, false).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_secure_require_client_secure_with_pkcs8_priv_key() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    do_simple_query_with_secure_server(servers::tls::TlsMode::Require, true, true).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_secure_require_client_plain() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let server_tls = TlsOption {
        mode: servers::tls::TlsMode::Require,
        cert_path: "tests/ssl/server.crt".to_owned(),
        key_path: "tests/ssl/server-rsa.key".to_owned(),
        ca_cert_path: String::new(),
        watch: false,
    };
    let server_port = start_test_server(server_tls).await?;
    let r = create_plain_connection(server_port, false).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_secure_require_client_plain_with_pkcs8_priv_key() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let server_tls = TlsOption {
        mode: servers::tls::TlsMode::Require,
        cert_path: "tests/ssl/server.crt".to_owned(),
        key_path: "tests/ssl/server-pkcs8.key".to_owned(),
        ca_cert_path: String::new(),
        watch: false,
    };
    let server_port = start_test_server(server_tls).await?;
    let r = create_plain_connection(server_port, false).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_using_db() -> Result<()> {
    let server_port = start_test_server(TlsOption::default()).await?;

    let client = create_connection_with_given_db(server_port, "testdb").await;
    assert!(client.is_err());

    let client = create_connection_without_db(server_port).await;
    assert!(client.is_err());

    let client = create_connection_with_given_db(server_port, DEFAULT_SCHEMA_NAME)
        .await
        .unwrap();
    let result = client.simple_query("SELECT uint32s FROM numbers").await;
    let _ = result.unwrap();

    let client = create_connection_with_given_catalog_schema(
        server_port,
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
    )
    .await;
    let _ = client.unwrap();

    let client =
        create_connection_with_given_catalog_schema(server_port, "notfound", DEFAULT_SCHEMA_NAME)
            .await;
    assert!(client.is_err());

    let client =
        create_connection_with_given_catalog_schema(server_port, DEFAULT_CATALOG_NAME, "notfound")
            .await;
    assert!(client.is_err());
    Ok(())
}

struct RegprocOid(u32);

impl<'a> FromSql<'a> for RegprocOid {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if ty != &Type::REGPROC {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected REGPROC, got {ty}"),
            )));
        }

        let oid = raw.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected a four-byte OID, got {} bytes", raw.len()),
            )
        })?;
        Ok(Self(u32::from_be_bytes(oid)))
    }

    fn accepts(ty: &Type) -> bool {
        ty == &Type::REGPROC
    }
}

#[tokio::test]
async fn test_extended_query_regproc_response() -> Result<()> {
    let server_port = start_test_server(TlsOption::default()).await?;
    let client = create_connection_with_given_db(server_port, DEFAULT_SCHEMA_NAME)
        .await
        .unwrap();
    let stmt = client
        .prepare("SELECT typreceive FROM pg_catalog.pg_type WHERE oid = 16")
        .await
        .unwrap();
    assert_eq!(stmt.columns()[0].type_(), &Type::REGPROC);

    let rows = client.query(&stmt, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<usize, RegprocOid>(0).0, 2436);

    let result = client
        .simple_query("SELECT typreceive FROM pg_catalog.pg_type WHERE oid = 16")
        .await
        .unwrap();
    let row = result
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(row.get(0), Some("boolrecv"));

    Ok(())
}

#[tokio::test]
async fn test_extended_query() -> Result<()> {
    let server_port = start_test_server(TlsOption::default()).await?;
    let client = create_connection_with_given_db(server_port, DEFAULT_SCHEMA_NAME)
        .await
        .unwrap();
    let stmt = client
        .prepare_typed(
            "SELECT uint32s, uint32s+1 FROM numbers WHERE uint32s = $1",
            &[Type::INT4],
        )
        .await
        .unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0].get::<usize, i64>(0usize), 1);
    assert_eq!(rows[0].get::<&str, i64>("uint32s"), 1);
    assert_eq!(rows[0].get::<usize, i64>(1usize), 2);
    assert_eq!(rows[0].get::<&str, i64>("numbers.uint32s + Int64(1)"), 2);

    Ok(())
}

async fn start_test_server(server_tls: TlsOption) -> Result<u16> {
    common_telemetry::init_default_ut_logging();
    let _ = install_default_crypto_provider();

    let table = MemTable::default_numbers_table();
    let mut pg_server = create_postgres_server(table, false, server_tls, None)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    pg_server.start(listening).await.unwrap();
    let server_addr = pg_server.bind_addr().unwrap();
    Ok(server_addr.port())
}

const EMPTY_QUERIES: &[&str] = &[
    "",
    " \t\r\n",
    ";",
    ";;;",
    "-- ping",
    "-- ping\n",
    "/* ping */",
    "/* outer /* inner */ comment */",
    "; -- ping\r\n /* comment */ ;",
];

#[tokio::test]
async fn test_simple_query_empty_statements() -> Result<()> {
    let server_port = start_test_server(Default::default()).await?;
    let client = create_plain_connection(server_port, false).await.unwrap();

    for query in EMPTY_QUERIES {
        let messages = client
            .simple_query(query)
            .await
            .unwrap_or_else(|err| panic!("query {query:?} failed: {err}"));
        // tokio-postgres exposes EmptyQueryResponse as CommandComplete(0).
        assert!(
            matches!(
                messages.as_slice(),
                [SimpleQueryMessage::CommandComplete(0)]
            ),
            "expected an empty query response for {query:?}, got {messages:?}"
        );
    }

    let messages = client.simple_query("SELECT 1").await.unwrap();
    assert_eq!(unwrap_results(&messages), vec!["1"]);
    Ok(())
}

#[tokio::test]
async fn test_extended_query_empty_statements() -> Result<()> {
    let server_port = start_test_server(Default::default()).await?;
    let client = create_plain_connection(server_port, false).await.unwrap();

    for query in EMPTY_QUERIES {
        let statement = client
            .prepare(query)
            .await
            .unwrap_or_else(|err| panic!("prepare {query:?} failed: {err}"));
        assert_eq!(client.execute(&statement, &[]).await.unwrap(), 0);
        assert!(client.query(&statement, &[]).await.unwrap().is_empty());
    }

    let row = client
        .query_one("SELECT $1::BIGINT", &[&42i64])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 42);
    Ok(())
}

#[tokio::test]
async fn test_simple_query_with_comments() -> Result<()> {
    let server_port = start_test_server(Default::default()).await?;
    let client = create_plain_connection(server_port, false).await.unwrap();

    for (query, expected) in [
        ("-- ping\nSELECT 1", vec!["1"]),
        ("/* comment */ SELECT 1; -- trailing", vec!["1"]),
        ("SELECT '-- ping'", vec!["-- ping"]),
        ("SELECT '/* comment */'", vec!["/* comment */"]),
        ("SELECT 1; /* between */ SELECT 2;", vec!["1", "2"]),
    ] {
        let messages = client.simple_query(query).await.unwrap();
        assert_eq!(unwrap_results(&messages), expected, "query: {query}");
    }

    assert!(
        client
            .simple_query("/* comment */ SELECT missing_column FROM numbers")
            .await
            .is_err()
    );
    let messages = client.simple_query("SELECT 1").await.unwrap();
    assert_eq!(unwrap_results(&messages), vec!["1"]);
    Ok(())
}

async fn do_simple_query(server_tls: TlsOption, client_tls: bool) -> Result<()> {
    let server_port = start_test_server(server_tls).await?;

    if !client_tls {
        let client = create_plain_connection(server_port, false).await.unwrap();
        let result = client.simple_query("SELECT uint32s FROM numbers").await;
        let _ = result.unwrap();
    } else {
        let client = create_secure_connection(server_port, None).await.unwrap();
        let result = client.simple_query("SELECT uint32s FROM numbers").await;
        let _ = result.unwrap();
    }

    Ok(())
}

async fn create_secure_connection(
    port: u16,
    credentials: Option<(&str, &str)>,
) -> std::result::Result<Client, PgError> {
    let url = match credentials {
        Some((user, password)) => format!(
            "sslmode=require host=127.0.0.1 port={port} user={user} password={password} connect_timeout=2 dbname={DEFAULT_SCHEMA_NAME}",
        ),
        None => {
            format!("host=127.0.0.1 port={port} connect_timeout=2 dbname={DEFAULT_SCHEMA_NAME}")
        }
    };

    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(AcceptAllVerifier {}));

    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);
    let (client, conn) = tokio_postgres::connect(&url, tls).await.expect("connect");

    let _handle = tokio::spawn(conn);
    Ok(client)
}

async fn create_plain_connection(
    port: u16,
    with_pwd: bool,
) -> std::result::Result<Client, PgError> {
    let url = if with_pwd {
        format!(
            "host=127.0.0.1 port={port} user=greptime password=greptime connect_timeout=2 dbname={DEFAULT_SCHEMA_NAME}",
        )
    } else {
        format!("host=127.0.0.1 port={port} connect_timeout=2 dbname={DEFAULT_SCHEMA_NAME}")
    };
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await?;
    let _handle = tokio::spawn(conn);
    Ok(client)
}

async fn create_plain_connection_with_credentials(
    port: u16,
    username: &str,
    password: &str,
) -> std::result::Result<Client, PgError> {
    let url = format!(
        "host=127.0.0.1 port={port} user={username} password={password} connect_timeout=2 dbname={DEFAULT_SCHEMA_NAME}",
    );
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await?;
    let _handle = tokio::spawn(conn);
    Ok(client)
}

async fn create_connection_with_given_db(
    port: u16,
    db: &str,
) -> std::result::Result<Client, PgError> {
    let url = format!("host=127.0.0.1 port={port} connect_timeout=2 dbname={db}");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await?;
    let _handle = tokio::spawn(conn);
    Ok(client)
}

async fn create_connection_with_given_catalog_schema(
    port: u16,
    catalog: &str,
    schema: &str,
) -> std::result::Result<Client, PgError> {
    let url = format!("host=127.0.0.1 port={port} connect_timeout=2 dbname={catalog}-{schema}");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await?;
    let _handle = tokio::spawn(conn);
    Ok(client)
}

async fn create_connection_without_db(port: u16) -> std::result::Result<Client, PgError> {
    let url = format!("host=127.0.0.1 port={port} connect_timeout=2");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await?;
    let _handle = tokio::spawn(conn);
    Ok(client)
}

fn resolve_result(resp: &SimpleQueryMessage, col_index: usize) -> Option<&str> {
    match resp {
        SimpleQueryMessage::Row(r) => r.get(col_index),
        _ => None,
    }
}

fn unwrap_results(resp: &[SimpleQueryMessage]) -> Vec<&str> {
    resp.iter().filter_map(|m| resolve_result(m, 0)).collect()
}

#[derive(Debug)]
struct AcceptAllVerifier {}
impl ServerCertVerifier for AcceptAllVerifier {
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }

    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
}

async fn do_simple_query_with_secure_server(
    server_tls_mode: servers::tls::TlsMode,
    client_tls: bool,
    is_pkcs8_priv_key: bool,
) -> Result<()> {
    let server_tls = TlsOption {
        mode: server_tls_mode,
        cert_path: "tests/ssl/server.crt".to_owned(),
        key_path: {
            if is_pkcs8_priv_key {
                "tests/ssl/server-pkcs8.key".to_owned()
            } else {
                "tests/ssl/server-rsa.key".to_owned()
            }
        },
        ca_cert_path: String::new(),
        watch: false,
    };

    do_simple_query(server_tls, client_tls).await
}

// ---------------------------------------------------------------------------
// COPY FROM STDIN tests
// ---------------------------------------------------------------------------

use futures::SinkExt;

fn copy_in_test_table() -> TableRef {
    use common_recordbatch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::types::TimestampType;
    use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector, VectorRef};

    let column_schemas = vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::Timestamp(TimestampType::Millisecond(Default::default())),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("val", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
    ];
    let schema = Arc::new(datatypes::schema::Schema::new(column_schemas));
    let columns: Vec<VectorRef> = vec![
        Arc::new(TimestampMillisecondVector::from_slice([0i64])),
        Arc::new(StringVector::from_slice(&["localhost"])),
        Arc::new(Float64Vector::from_slice([1.0f64])),
        Arc::new(StringVector::from_slice(&["seed"])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    MemTable::table("metrics", recordbatch)
}

async fn start_copy_in_test_server(
    table: TableRef,
) -> Result<(Box<dyn Server>, u16, Arc<crate::RecordingCopyInHandler>)> {
    common_telemetry::init_default_ut_logging();
    let _ = install_default_crypto_provider();
    let (mut server, handler) = create_postgres_server_inner(table, Default::default(), None)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    server.start(listening).await.unwrap();
    let port = server.bind_addr().unwrap().port();
    Ok((server, port, handler))
}

fn recorded_rows(handler: &crate::RecordingCopyInHandler) -> Vec<api::v1::Row> {
    handler
        .requests
        .lock()
        .unwrap()
        .iter()
        .flat_map(|request| {
            request.inserts.iter().flat_map(|insert| {
                insert
                    .rows
                    .clone()
                    .map(|rows| rows.rows)
                    .unwrap_or_default()
            })
        })
        .collect()
}

async fn feed_copy_in(
    client: &Client,
    sql: &str,
    chunks: Vec<bytes::Bytes>,
) -> std::result::Result<u64, PgError> {
    let mut sink = Box::pin(client.copy_in(sql).await?);
    for chunk in chunks {
        sink.as_mut().send(chunk).await?;
    }
    sink.as_mut().finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_csv() -> Result<()> {
    let (server, port, handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let client = create_plain_connection(port, false).await.unwrap();

    let rows = feed_copy_in(
        &client,
        "COPY metrics FROM STDIN WITH (FORMAT csv)",
        vec![bytes::Bytes::from_static(
            b"2023-11-14 22:13:20,host1,1.5,hello\n2023-11-14 22:13:21,host2,,\"quoted, comma\"\n",
        )],
    )
    .await
    .unwrap();
    assert_eq!(rows, 2);

    let rows = recorded_rows(&handler);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values[1].value_data,
        Some(api::v1::value::ValueData::StringValue("host1".to_string()))
    );
    assert_eq!(
        rows[0].values[2].value_data,
        Some(api::v1::value::ValueData::F64Value(1.5))
    );
    // Unquoted empty field is NULL, quoted field keeps its content.
    assert_eq!(rows[1].values[2].value_data, None);
    assert_eq!(
        rows[1].values[3].value_data,
        Some(api::v1::value::ValueData::StringValue(
            "quoted, comma".to_string()
        ))
    );

    server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_text_format() -> Result<()> {
    let (server, port, handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let client = create_plain_connection(port, false).await.unwrap();

    let mut sink = Box::pin(client.copy_in("COPY metrics FROM STDIN").await.unwrap());
    sink.as_mut()
        .send(bytes::Bytes::from_static(
            b"2023-11-14 22:13:22\thost3\t2.5\twith\\ttab\n2023-11-14 22:13:23\thost4\t\\N\t\\N\n",
        ))
        .await
        .unwrap();
    let rows = sink.as_mut().finish().await.unwrap();
    assert_eq!(rows, 2);

    let rows = recorded_rows(&handler);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values[3].value_data,
        Some(api::v1::value::ValueData::StringValue(
            "with\ttab".to_string()
        ))
    );
    assert_eq!(rows[1].values[2].value_data, None);

    server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_column_list_and_batches() -> Result<()> {
    let (server, port, handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let client = create_plain_connection(port, false).await.unwrap();

    let mut chunks = Vec::new();
    let mut payload = String::new();
    for i in 0..20000u64 {
        payload.push_str(&format!("{i},2023-11-14 22:13:20,host{i},0.5\n"));
        if (i + 1) % 100 == 0 {
            chunks.push(bytes::Bytes::from(std::mem::take(&mut payload)));
        }
    }
    let rows = feed_copy_in(
        &client,
        "COPY metrics (note, ts, host, val) FROM STDIN (FORMAT csv)",
        chunks,
    )
    .await
    .unwrap();
    assert_eq!(rows, 20000);

    let recorded = handler.requests.lock().unwrap().clone();
    // The 20000 rows must have been flushed in multiple batches.
    assert!(recorded.len() > 1);
    let total: usize = recorded
        .iter()
        .map(|request| {
            request.inserts[0]
                .rows
                .as_ref()
                .map(|rows| rows.rows.len())
                .unwrap_or(0)
        })
        .sum();
    assert_eq!(total, 20000);

    let rows = recorded_rows(&handler);
    // The column list reorders the fields: first field lands in `note`.
    assert_eq!(
        rows[0].values[0].value_data,
        Some(api::v1::value::ValueData::StringValue("0".to_string()))
    );

    server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_unknown_table() -> Result<()> {
    let (server, port, _handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let client = create_plain_connection(port, false).await.unwrap();

    let err = match client
        .copy_in::<_, bytes::Bytes>("COPY not_exist FROM STDIN")
        .await
    {
        Ok(_) => panic!("expected COPY to unknown table to fail"),
        Err(e) => e,
    };
    let message = err.as_db_error().expect("expected db error").message();
    assert!(message.contains("does not exist"), "{message}");

    server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_bad_value() -> Result<()> {
    let (server, port, handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let client = create_plain_connection(port, false).await.unwrap();

    let err = match feed_copy_in(
        &client,
        "COPY metrics FROM STDIN WITH (FORMAT csv)",
        vec![bytes::Bytes::from_static(b"not_a_timestamp,host1,1.5,x\n")],
    )
    .await
    {
        Ok(_) => panic!("expected COPY to fail"),
        Err(e) => e,
    };
    let message = err.as_db_error().expect("expected db error").message();
    assert!(message.contains("invalid input syntax"), "{message}");
    // The failed batch must not be written.
    assert!(handler.requests.lock().unwrap().is_empty());

    server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_multi_statement_rejected() -> Result<()> {
    let (server, port, _handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let client = create_plain_connection(port, false).await.unwrap();

    let err = match feed_copy_in(
        &client,
        "COPY metrics FROM STDIN; SELECT 1",
        vec![bytes::Bytes::from_static(b"1,2,3,4\n")],
    )
    .await
    {
        Ok(_) => panic!("expected multi-statement COPY to fail"),
        Err(e) => e,
    };
    let message = err
        .as_db_error()
        .expect("expected db error")
        .message()
        .to_string();
    assert!(message.contains("alone"), "{message}");

    server.shutdown().await.unwrap();
    Ok(())
}

/// A minimal raw PostgreSQL client to drive the SIMPLE query protocol
/// (psql-compatible) for COPY FROM STDIN, which tokio-postgres cannot do.
struct RawPgClient {
    stream: tokio::net::TcpStream,
    buffer: Vec<u8>,
}

impl RawPgClient {
    async fn connect(port: u16) -> std::io::Result<Self> {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        // StartupMessage: protocol 3.0, user=greptime, database=public.
        let mut startup = Vec::new();
        startup.extend_from_slice(&196608u32.to_be_bytes());
        startup.extend_from_slice(b"user\0greptime\0");
        startup.extend_from_slice(b"database\0public\0\0");
        let mut message = (startup.len() as u32 + 4).to_be_bytes().to_vec();
        message.extend_from_slice(&startup);
        use tokio::io::AsyncWriteExt;
        stream.write_all(&message).await?;
        Ok(Self {
            stream,
            buffer: Vec::new(),
        })
    }

    async fn send(&mut self, type_byte: u8, payload: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut message = vec![type_byte];
        message.extend_from_slice(&(payload.len() as u32 + 4).to_be_bytes());
        message.extend_from_slice(payload);
        self.stream.write_all(&message).await
    }

    async fn query(&mut self, sql: &str) -> std::io::Result<()> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        self.send(b'Q', &payload).await
    }

    /// Reads the next backend message: `(type, payload)`.
    async fn read_message(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        use tokio::io::AsyncReadExt;
        while self.buffer.len() < 5 {
            let n = self.buffer.len();
            self.buffer.resize(n + 1024, 0);
            let read = self.stream.read(&mut self.buffer[n..]).await?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            self.buffer.truncate(n + read);
        }
        let type_byte = self.buffer[0];
        let len = u32::from_be_bytes([
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
            self.buffer[4],
        ]) as usize;
        while self.buffer.len() < len + 1 {
            let n = self.buffer.len();
            self.buffer.resize(n + 1024, 0);
            let read = self.stream.read(&mut self.buffer[n..]).await?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            self.buffer.truncate(n + read);
        }
        let payload = self.buffer[5..len + 1].to_vec();
        self.buffer.drain(..len + 1);
        Ok((type_byte, payload))
    }

    /// Reads messages until one of `wanted` types shows up; returns it.
    async fn expect(&mut self, wanted: &str) -> std::io::Result<(u8, Vec<u8>)> {
        loop {
            let (type_byte, payload) = self.read_message().await?;
            if wanted.contains(type_byte as char) {
                return Ok((type_byte, payload));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_simple_protocol() -> Result<()> {
    let (server, port, handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let mut client = RawPgClient::connect(port).await.unwrap();

    // Consume the ReadyForQuery after startup.
    client.expect("Z").await.unwrap();

    client
        .query("COPY metrics FROM STDIN WITH (FORMAT csv)")
        .await
        .unwrap();
    // CopyInResponse: format byte + column count.
    let (type_byte, payload) = client.expect("GE").await.unwrap();
    assert_eq!(type_byte as char, 'G');
    assert_eq!(payload[0], 0);
    assert_eq!(u16::from_be_bytes([payload[1], payload[2]]), 4);

    client
        .send(b'd', b"2023-11-14 22:13:20,host1,1.5,x\n")
        .await
        .unwrap();
    client
        .send(b'd', b"2023-11-14 22:13:21,host2,2.5,y\n")
        .await
        .unwrap();
    client.send(b'c', &[]).await.unwrap();

    let (type_byte, payload) = client.expect("CE").await.unwrap();
    assert_eq!(type_byte as char, 'C');
    let tag = std::str::from_utf8(&payload[..payload.len() - 1]).unwrap();
    assert_eq!(tag, "COPY 2");

    // The connection must return to normal operation.
    client.expect("Z").await.unwrap();
    client.query("SELECT 1").await.unwrap();
    client.expect("Z").await.unwrap();

    let rows = recorded_rows(&handler);
    assert_eq!(rows.len(), 2);

    server.shutdown().await.unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_copy_in_simple_protocol_copy_fail() -> Result<()> {
    let (server, port, handler) = start_copy_in_test_server(copy_in_test_table()).await?;
    let mut client = RawPgClient::connect(port).await.unwrap();
    client.expect("Z").await.unwrap();

    client.query("COPY metrics FROM STDIN").await.unwrap();
    client.expect("G").await.unwrap();
    client.send(b'd', b"partial row without").await.unwrap();
    // CopyFail aborts the copy; buffered partial data is discarded.
    let mut fail = b"client changed its mind\0".to_vec();
    client.send(b'f', &fail).await.unwrap();
    fail.clear();

    client.expect("E").await.unwrap();
    // The connection stays usable and nothing was written.
    client.expect("Z").await.unwrap();
    assert!(handler.requests.lock().unwrap().is_empty());

    server.shutdown().await.unwrap();
    Ok(())
}
