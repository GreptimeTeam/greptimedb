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

use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::sync::Arc;

use common_telemetry::debug;
use deadpool_postgres::{Config, Pool, Runtime};
// TLS-related imports (feature-gated)
use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{DigitallySignedStruct, Error as TlsError, SignatureScheme};
use rustls_pemfile::{certs, private_key};
use snafu::ResultExt;
use strum::AsRefStr;
use tokio_postgres::types::ToSql;
use tokio_postgres::{IsolationLevel, NoTls, Row};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{
    CreatePostgresPoolSnafu, GetPostgresConnectionSnafu, LoadTlsCertificateSnafu,
    PostgresExecutionSnafu, PostgresTlsConfigSnafu, PostgresTransactionSnafu, Result,
};
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::rds::{
    Executor, ExecutorFactory, ExecutorImpl, KvQueryExecutor, RDS_STORE_OP_BATCH_DELETE,
    RDS_STORE_OP_BATCH_GET, RDS_STORE_OP_BATCH_PUT, RDS_STORE_OP_RANGE_DELETE,
    RDS_STORE_OP_RANGE_QUERY, RDS_STORE_TXN_RETRY_COUNT, RdsStore, Transaction,
};
use crate::rpc::KeyValue;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, RangeRequest, RangeResponse,
};

/// TLS mode configuration for PostgreSQL connections.
/// This mirrors the TlsMode from servers::tls to avoid circular dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TlsMode {
    Disable,
    #[default]
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// TLS configuration for PostgreSQL connections.
/// This mirrors the TlsOption from servers::tls to avoid circular dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsOption {
    pub mode: TlsMode,
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
    pub watch: bool,
}

impl Default for TlsOption {
    fn default() -> Self {
        TlsOption {
            mode: TlsMode::Prefer,
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: String::new(),
            watch: false,
        }
    }
}

const PG_STORE_NAME: &str = "pg_store";

pub struct PgClient(deadpool::managed::Object<deadpool_postgres::Manager>);
pub struct PgTxnClient<'a>(deadpool_postgres::Transaction<'a>);

/// Converts a row to a [`KeyValue`].
fn key_value_from_row(r: Row) -> KeyValue {
    KeyValue {
        key: r.get(0),
        value: r.get(1),
    }
}

const EMPTY: &[u8] = &[0];

/// Type of range template.
#[derive(Debug, Clone, Copy, AsRefStr)]
enum RangeTemplateType {
    Point,
    Range,
    Full,
    LeftBounded,
    Prefix,
}

/// Builds params for the given range template type.
impl RangeTemplateType {
    /// Builds the parameters for the given range template type.
    /// You can check out the conventions at [RangeRequest]
    fn build_params(&self, mut key: Vec<u8>, range_end: Vec<u8>) -> Vec<Vec<u8>> {
        match self {
            RangeTemplateType::Point => vec![key],
            RangeTemplateType::Range => vec![key, range_end],
            RangeTemplateType::Full => vec![],
            RangeTemplateType::LeftBounded => vec![key],
            RangeTemplateType::Prefix => {
                key.push(b'%');
                vec![key]
            }
        }
    }
}

/// Templates for range request.
#[derive(Debug, Clone)]
struct RangeTemplate {
    point: String,
    range: String,
    full: String,
    left_bounded: String,
    prefix: String,
}

impl RangeTemplate {
    /// Gets the template for the given type.
    fn get(&self, typ: RangeTemplateType) -> &str {
        match typ {
            RangeTemplateType::Point => &self.point,
            RangeTemplateType::Range => &self.range,
            RangeTemplateType::Full => &self.full,
            RangeTemplateType::LeftBounded => &self.left_bounded,
            RangeTemplateType::Prefix => &self.prefix,
        }
    }

    /// Adds limit to the template.
    fn with_limit(template: &str, limit: i64) -> String {
        if limit == 0 {
            return format!("{};", template);
        }
        format!("{} LIMIT {};", template, limit)
    }
}

fn is_prefix_range(start: &[u8], end: &[u8]) -> bool {
    if start.len() != end.len() {
        return false;
    }
    let l = start.len();
    let same_prefix = start[0..l - 1] == end[0..l - 1];
    if let (Some(rhs), Some(lhs)) = (start.last(), end.last()) {
        return same_prefix && (*rhs + 1) == *lhs;
    }
    false
}

/// Determine the template type for range request.
fn range_template(key: &[u8], range_end: &[u8]) -> RangeTemplateType {
    match (key, range_end) {
        (_, &[]) => RangeTemplateType::Point,
        (EMPTY, EMPTY) => RangeTemplateType::Full,
        (_, EMPTY) => RangeTemplateType::LeftBounded,
        (start, end) => {
            if is_prefix_range(start, end) {
                RangeTemplateType::Prefix
            } else {
                RangeTemplateType::Range
            }
        }
    }
}

/// Generate in placeholders for PostgreSQL.
fn pg_generate_in_placeholders(from: usize, to: usize) -> Vec<String> {
    (from..=to).map(|i| format!("${}", i)).collect()
}

/// Factory for building sql templates.
struct PgSqlTemplateFactory<'a> {
    schema_name: Option<&'a str>,
    table_name: &'a str,
}

impl<'a> PgSqlTemplateFactory<'a> {
    /// Creates a new factory with optional schema.
    fn new(schema_name: Option<&'a str>, table_name: &'a str) -> Self {
        Self {
            schema_name,
            table_name,
        }
    }

    /// Builds the template set for the given table name.
    fn build(&self) -> PgSqlTemplateSet {
        let table_ident = Self::format_table_ident(self.schema_name, self.table_name);
        // Some of queries don't end with `;`, because we need to add `LIMIT` clause.
        PgSqlTemplateSet {
            table_ident: table_ident.clone(),
            // Do not attempt to create schema implicitly to avoid extra privileges requirement.
            create_table_statement: format!(
                "CREATE TABLE IF NOT EXISTS {table_ident}(k bytea PRIMARY KEY, v bytea)",
            ),
            range_template: RangeTemplate {
                point: format!("SELECT k, v FROM {table_ident} WHERE k = $1"),
                range: format!(
                    "SELECT k, v FROM {table_ident} WHERE k >= $1 AND k < $2 ORDER BY k"
                ),
                full: format!("SELECT k, v FROM {table_ident} ORDER BY k"),
                left_bounded: format!("SELECT k, v FROM {table_ident} WHERE k >= $1 ORDER BY k"),
                prefix: format!("SELECT k, v FROM {table_ident} WHERE k LIKE $1 ORDER BY k"),
            },
            delete_template: RangeTemplate {
                point: format!("DELETE FROM {table_ident} WHERE k = $1 RETURNING k,v;"),
                range: format!("DELETE FROM {table_ident} WHERE k >= $1 AND k < $2 RETURNING k,v;"),
                full: format!("DELETE FROM {table_ident} RETURNING k,v"),
                left_bounded: format!("DELETE FROM {table_ident} WHERE k >= $1 RETURNING k,v;"),
                prefix: format!("DELETE FROM {table_ident} WHERE k LIKE $1 RETURNING k,v;"),
            },
        }
    }

    /// Formats the table reference with schema if provided.
    fn format_table_ident(schema_name: Option<&str>, table_name: &str) -> String {
        match schema_name {
            Some(s) if !s.is_empty() => format!("\"{}\".\"{}\"", s, table_name),
            _ => format!("\"{}\"", table_name),
        }
    }
}

/// Templates for the given table name.
#[derive(Debug, Clone)]
pub struct PgSqlTemplateSet {
    table_ident: String,
    create_table_statement: String,
    range_template: RangeTemplate,
    delete_template: RangeTemplate,
}

impl PgSqlTemplateSet {
    /// Generates the sql for batch get.
    fn generate_batch_get_query(&self, key_len: usize) -> String {
        let in_clause = pg_generate_in_placeholders(1, key_len).join(", ");
        format!(
            "SELECT k, v FROM {} WHERE k in ({});",
            self.table_ident, in_clause
        )
    }

    /// Generates the sql for batch delete.
    fn generate_batch_delete_query(&self, key_len: usize) -> String {
        let in_clause = pg_generate_in_placeholders(1, key_len).join(", ");
        format!(
            "DELETE FROM {} WHERE k in ({}) RETURNING k,v;",
            self.table_ident, in_clause
        )
    }

    /// Generates the sql for batch upsert.
    fn generate_batch_upsert_query(&self, kv_len: usize) -> String {
        let in_placeholders: Vec<String> = (1..=kv_len).map(|i| format!("${}", i)).collect();
        let in_clause = in_placeholders.join(", ");
        let mut param_index = kv_len + 1;
        let mut values_placeholders = Vec::new();
        for _ in 0..kv_len {
            values_placeholders.push(format!("(${0}, ${1})", param_index, param_index + 1));
            param_index += 2;
        }
        let values_clause = values_placeholders.join(", ");

        format!(
            r#"
    WITH prev AS (
        SELECT k,v FROM {table} WHERE k IN ({in_clause})
    ), update AS (
    INSERT INTO {table} (k, v) VALUES
        {values_clause}
    ON CONFLICT (
        k
    ) DO UPDATE SET
        v = excluded.v
    )

    SELECT k, v FROM prev;
    "#,
            table = self.table_ident,
            in_clause = in_clause,
            values_clause = values_clause
        )
    }
}

#[async_trait::async_trait]
impl Executor for PgClient {
    type Transaction<'a>
        = PgTxnClient<'a>
    where
        Self: 'a;

    fn name() -> &'static str {
        "Postgres"
    }

    async fn query(&mut self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
        let stmt = self
            .0
            .prepare_cached(query)
            .await
            .context(PostgresExecutionSnafu { sql: query })?;
        let rows = self
            .0
            .query(&stmt, &params)
            .await
            .context(PostgresExecutionSnafu { sql: query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn txn_executor<'a>(&'a mut self) -> Result<Self::Transaction<'a>> {
        let txn = self
            .0
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .context(PostgresTransactionSnafu {
                operation: "begin".to_string(),
            })?;
        Ok(PgTxnClient(txn))
    }
}

#[async_trait::async_trait]
impl<'a> Transaction<'a> for PgTxnClient<'a> {
    async fn query(&mut self, query: &str, params: &[&Vec<u8>]) -> Result<Vec<KeyValue>> {
        let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
        let stmt = self
            .0
            .prepare_cached(query)
            .await
            .context(PostgresExecutionSnafu { sql: query })?;
        let rows = self
            .0
            .query(&stmt, &params)
            .await
            .context(PostgresExecutionSnafu { sql: query })?;
        Ok(rows.into_iter().map(key_value_from_row).collect())
    }

    async fn commit(self) -> Result<()> {
        self.0.commit().await.context(PostgresTransactionSnafu {
            operation: "commit",
        })?;
        Ok(())
    }
}

pub struct PgExecutorFactory {
    pool: Pool,
}

impl PgExecutorFactory {
    async fn client(&self) -> Result<PgClient> {
        match self.pool.get().await {
            Ok(client) => Ok(PgClient(client)),
            Err(e) => GetPostgresConnectionSnafu {
                reason: e.to_string(),
            }
            .fail(),
        }
    }
}

#[async_trait::async_trait]
impl ExecutorFactory<PgClient> for PgExecutorFactory {
    async fn default_executor(&self) -> Result<PgClient> {
        self.client().await
    }

    async fn txn_executor<'a>(
        &self,
        default_executor: &'a mut PgClient,
    ) -> Result<PgTxnClient<'a>> {
        default_executor.txn_executor().await
    }
}

/// A PostgreSQL-backed key-value store for metasrv.
/// It uses [deadpool_postgres::Pool] as the connection pool for [RdsStore].
pub type PgStore = RdsStore<PgClient, PgExecutorFactory, PgSqlTemplateSet>;

/// Creates a PostgreSQL TLS connector based on the provided configuration.
///
/// This function creates a rustls-based TLS connector for PostgreSQL connections,
/// following PostgreSQL's TLS mode specifications exactly:
///
/// # TLS Modes (PostgreSQL Specification)
///
/// - `Disable`: No TLS connection attempted
/// - `Prefer`: Try TLS first, fallback to plaintext if TLS fails (handled by connection logic)
/// - `Require`: Only TLS connections, but NO certificate verification (accept any cert)
/// - `VerifyCa`: TLS + verify certificate is signed by trusted CA (no hostname verification)
/// - `VerifyFull`: TLS + verify CA + verify hostname matches certificate SAN
///
pub fn create_postgres_tls_connector(tls_config: &TlsOption) -> Result<MakeRustlsConnect> {
    common_telemetry::info!(
        "Creating PostgreSQL TLS connector with mode: {:?}",
        tls_config.mode
    );

    let config_builder = match tls_config.mode {
        TlsMode::Disable => {
            return PostgresTlsConfigSnafu {
                reason: "Cannot create TLS connector for Disable mode".to_string(),
            }
            .fail();
        }
        TlsMode::Prefer | TlsMode::Require => {
            // For Prefer/Require: Accept any certificate (no verification)
            let verifier = Arc::new(AcceptAnyVerifier);
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(verifier)
        }
        TlsMode::VerifyCa => {
            // For VerifyCa: Verify server cert against CA store, but skip hostname verification
            let ca_store = load_ca(&tls_config.ca_cert_path)?;
            let verifier = Arc::new(NoHostnameVerification { roots: ca_store });
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(verifier)
        }
        TlsMode::VerifyFull => {
            let ca_store = load_ca(&tls_config.ca_cert_path)?;
            ClientConfig::builder().with_root_certificates(ca_store)
        }
    };

    // Create the TLS client configuration based on the mode and client cert requirements
    let client_config = if !tls_config.cert_path.is_empty() && !tls_config.key_path.is_empty() {
        // Client certificate authentication required
        common_telemetry::info!("Loading client certificate for mutual TLS");
        let cert_chain = load_certs(&tls_config.cert_path)?;
        let private_key = load_private_key(&tls_config.key_path)?;

        config_builder
            .with_client_auth_cert(cert_chain, private_key)
            .map_err(|e| {
                PostgresTlsConfigSnafu {
                    reason: format!("Failed to configure client authentication: {}", e),
                }
                .build()
            })?
    } else {
        common_telemetry::info!("No client certificate provided, skip client authentication");
        config_builder.with_no_client_auth()
    };

    common_telemetry::info!("Successfully created PostgreSQL TLS connector");
    Ok(MakeRustlsConnect::new(client_config))
}

/// For Prefer/Require mode, we accept any server certificate without verification.
#[derive(Debug)]
struct AcceptAnyVerifier;

impl ServerCertVerifier for AcceptAnyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        common_telemetry::debug!(
            "Accepting server certificate without verification (Prefer/Require mode)"
        );
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        // Accept any signature without verification
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        // Accept any signature without verification
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Support all signature schemes
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// For VerifyCa mode, we verify the server certificate against our CA store
/// and skip verify server's HostName.
#[derive(Debug)]
struct NoHostnameVerification {
    roots: Arc<rustls::RootCertStore>,
}

impl ServerCertVerifier for NoHostnameVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .all,
        )?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Support all signature schemes
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = File::open(path).context(LoadTlsCertificateSnafu { path })?;
    let mut reader = BufReader::new(file);
    let certs = certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            PostgresTlsConfigSnafu {
                reason: format!("Failed to parse certificates from {}: {}", path, e),
            }
            .build()
        })?;
    Ok(certs)
}

fn load_private_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let file = File::open(path).context(LoadTlsCertificateSnafu { path })?;
    let mut reader = BufReader::new(file);
    let key = private_key(&mut reader)
        .map_err(|e| {
            PostgresTlsConfigSnafu {
                reason: format!("Failed to parse private key from {}: {}", path, e),
            }
            .build()
        })?
        .ok_or_else(|| {
            PostgresTlsConfigSnafu {
                reason: format!("No private key found in {}", path),
            }
            .build()
        })?;
    Ok(key)
}

fn load_ca(path: &str) -> Result<Arc<rustls::RootCertStore>> {
    let mut root_store = rustls::RootCertStore::empty();

    // Add system root certificates
    match rustls_native_certs::load_native_certs() {
        Ok(certs) => {
            let num_certs = certs.len();
            for cert in certs {
                if let Err(e) = root_store.add(cert) {
                    return PostgresTlsConfigSnafu {
                        reason: format!("Failed to add root certificate: {}", e),
                    }
                    .fail();
                }
            }
            common_telemetry::info!("Loaded {num_certs} system root certificates successfully");
        }
        Err(e) => {
            return PostgresTlsConfigSnafu {
                reason: format!("Failed to load system root certificates: {}", e),
            }
            .fail();
        }
    }

    // Try add custom CA certificate if provided
    if !path.is_empty() {
        let ca_certs = load_certs(path)?;
        for cert in ca_certs {
            if let Err(e) = root_store.add(cert) {
                return PostgresTlsConfigSnafu {
                    reason: format!("Failed to add custom CA certificate: {}", e),
                }
                .fail();
            }
        }
        common_telemetry::info!("Added custom CA certificate from {}", path);
    }

    Ok(Arc::new(root_store))
}

#[async_trait::async_trait]
impl KvQueryExecutor<PgClient> for PgStore {
    async fn range_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, PgClient>,
        req: RangeRequest,
    ) -> Result<RangeResponse> {
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.range_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().collect::<Vec<_>>();
        // Always add 1 to limit to check if there is more data
        let query =
            RangeTemplate::with_limit(template, if req.limit == 0 { 0 } else { req.limit + 1 });
        let limit = req.limit as usize;
        debug!("query: {:?}, params: {:?}", query, params);
        let mut kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&query, &params_ref).await,
            PG_STORE_NAME,
            RDS_STORE_OP_RANGE_QUERY,
            template_type.as_ref()
        )?;

        if req.keys_only {
            kvs.iter_mut().for_each(|kv| kv.value = vec![]);
        }
        // If limit is 0, we always return all data
        if limit == 0 || kvs.len() <= limit {
            return Ok(RangeResponse { kvs, more: false });
        }
        // If limit is greater than the number of rows, we remove the last row and set more to true
        let removed = kvs.pop();
        debug_assert!(removed.is_some());
        Ok(RangeResponse { kvs, more: true })
    }

    async fn batch_put_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, PgClient>,
        req: BatchPutRequest,
    ) -> Result<BatchPutResponse> {
        let mut in_params = Vec::with_capacity(req.kvs.len() * 3);
        let mut values_params = Vec::with_capacity(req.kvs.len() * 2);

        for kv in &req.kvs {
            let processed_key = &kv.key;
            in_params.push(processed_key);

            let processed_value = &kv.value;
            values_params.push(processed_key);
            values_params.push(processed_value);
        }
        in_params.extend(values_params);
        let params = in_params.iter().map(|x| x as _).collect::<Vec<_>>();
        let query = self
            .sql_template_set
            .generate_batch_upsert_query(req.kvs.len());

        let kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&query, &params).await,
            PG_STORE_NAME,
            RDS_STORE_OP_BATCH_PUT,
            ""
        )?;
        if req.prev_kv {
            Ok(BatchPutResponse { prev_kvs: kvs })
        } else {
            Ok(BatchPutResponse::default())
        }
    }

    /// Batch get with certain client. It's needed for a client with transaction.
    async fn batch_get_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, PgClient>,
        req: BatchGetRequest,
    ) -> Result<BatchGetResponse> {
        if req.keys.is_empty() {
            return Ok(BatchGetResponse { kvs: vec![] });
        }
        let query = self
            .sql_template_set
            .generate_batch_get_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();
        let kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&query, &params).await,
            PG_STORE_NAME,
            RDS_STORE_OP_BATCH_GET,
            ""
        )?;
        Ok(BatchGetResponse { kvs })
    }

    async fn delete_range_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, PgClient>,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse> {
        let template_type = range_template(&req.key, &req.range_end);
        let template = self.sql_template_set.delete_template.get(template_type);
        let params = template_type.build_params(req.key, req.range_end);
        let params_ref = params.iter().map(|x| x as _).collect::<Vec<_>>();
        let kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(template, &params_ref).await,
            PG_STORE_NAME,
            RDS_STORE_OP_RANGE_DELETE,
            template_type.as_ref()
        )?;
        let mut resp = DeleteRangeResponse::new(kvs.len() as i64);
        if req.prev_kv {
            resp.with_prev_kvs(kvs);
        }
        Ok(resp)
    }

    async fn batch_delete_with_query_executor(
        &self,
        query_executor: &mut ExecutorImpl<'_, PgClient>,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse> {
        if req.keys.is_empty() {
            return Ok(BatchDeleteResponse::default());
        }
        let query = self
            .sql_template_set
            .generate_batch_delete_query(req.keys.len());
        let params = req.keys.iter().map(|x| x as _).collect::<Vec<_>>();

        let kvs = crate::record_rds_sql_execute_elapsed!(
            query_executor.query(&query, &params).await,
            PG_STORE_NAME,
            RDS_STORE_OP_BATCH_DELETE,
            ""
        )?;
        if req.prev_kv {
            Ok(BatchDeleteResponse { prev_kvs: kvs })
        } else {
            Ok(BatchDeleteResponse::default())
        }
    }
}

impl PgStore {
    /// Create [PgStore] impl of [KvBackendRef] from url with optional TLS support.
    ///
    /// # Arguments
    ///
    /// * `url` - PostgreSQL connection URL
    /// * `table_name` - Name of the table to use for key-value storage
    /// * `max_txn_ops` - Maximum number of operations per transaction
    /// * `tls_config` - Optional TLS configuration. If None, uses plaintext connection.
    pub async fn with_url_and_tls(
        url: &str,
        table_name: &str,
        max_txn_ops: usize,
        tls_config: Option<TlsOption>,
    ) -> Result<KvBackendRef> {
        let mut cfg = Config::new();
        cfg.url = Some(url.to_string());

        let pool = match tls_config {
            Some(tls_config) if tls_config.mode != TlsMode::Disable => {
                match create_postgres_tls_connector(&tls_config) {
                    Ok(tls_connector) => cfg
                        .create_pool(Some(Runtime::Tokio1), tls_connector)
                        .context(CreatePostgresPoolSnafu)?,
                    Err(e) => {
                        if tls_config.mode == TlsMode::Prefer {
                            // Fallback to insecure connection if TLS fails
                            common_telemetry::info!(
                                "Failed to create TLS connector, falling back to insecure connection"
                            );
                            cfg.create_pool(Some(Runtime::Tokio1), NoTls)
                                .context(CreatePostgresPoolSnafu)?
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
            _ => cfg
                .create_pool(Some(Runtime::Tokio1), NoTls)
                .context(CreatePostgresPoolSnafu)?,
        };

        Self::with_pg_pool(pool, None, table_name, max_txn_ops, false).await
    }

    /// Create [PgStore] impl of [KvBackendRef] from url (backward compatibility).
    pub async fn with_url(url: &str, table_name: &str, max_txn_ops: usize) -> Result<KvBackendRef> {
        Self::with_url_and_tls(url, table_name, max_txn_ops, None).await
    }

    /// Create [PgStore] impl of [KvBackendRef] from [deadpool_postgres::Pool] with optional schema.
    pub async fn with_pg_pool(
        pool: Pool,
        schema_name: Option<&str>,
        table_name: &str,
        max_txn_ops: usize,
        auto_create_schema: bool,
    ) -> Result<KvBackendRef> {
        // Ensure the postgres metadata backend is ready to use.
        let client = match pool.get().await {
            Ok(client) => client,
            Err(e) => {
                // We need to log the debug for the error to help diagnose the issue.
                common_telemetry::error!("Failed to get Postgres connection: {:?}", e);
                return GetPostgresConnectionSnafu {
                    reason: e.to_string(),
                }
                .fail();
            }
        };

        // Automatically create schema if enabled and schema_name is provided.
        if auto_create_schema
            && let Some(schema) = schema_name
            && !schema.is_empty()
        {
            let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema);
            client
                .execute(&create_schema_sql, &[])
                .await
                .with_context(|_| PostgresExecutionSnafu {
                    sql: create_schema_sql.clone(),
                })?;
        }

        let template_factory = PgSqlTemplateFactory::new(schema_name, table_name);
        let sql_template_set = template_factory.build();
        client
            .execute(&sql_template_set.create_table_statement, &[])
            .await
            .with_context(|_| PostgresExecutionSnafu {
                sql: sql_template_set.create_table_statement.clone(),
            })?;
        Ok(Arc::new(Self {
            max_txn_ops,
            sql_template_set,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: PgExecutorFactory { pool },
            _phantom: PhantomData,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::test::{
        prepare_kv_with_prefix, test_kv_batch_delete_with_prefix, test_kv_batch_get_with_prefix,
        test_kv_compare_and_put_with_prefix, test_kv_delete_range_with_prefix,
        test_kv_put_with_prefix, test_kv_range_2_with_prefix, test_kv_range_with_prefix,
        test_simple_kv_range, test_txn_compare_equal, test_txn_compare_greater,
        test_txn_compare_less, test_txn_compare_not_equal, test_txn_one_compare_op,
        text_txn_multi_compare_op, unprepare_kv,
    };
    use crate::test_util::test_certs_dir;
    use crate::{maybe_skip_postgres_integration_test, maybe_skip_postgres15_integration_test};

    async fn build_pg_kv_backend(table_name: &str) -> Option<PgStore> {
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap_or_default();
        if endpoints.is_empty() {
            return None;
        }

        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();
        let client = pool.get().await.unwrap();
        // use the default schema (i.e., public)
        let template_factory = PgSqlTemplateFactory::new(None, table_name);
        let sql_templates = template_factory.build();
        // Do not attempt to create schema implicitly.
        client
            .execute(&sql_templates.create_table_statement, &[])
            .await
            .with_context(|_| PostgresExecutionSnafu {
                sql: sql_templates.create_table_statement.clone(),
            })
            .unwrap();
        Some(PgStore {
            max_txn_ops: 128,
            sql_template_set: sql_templates,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: PgExecutorFactory { pool },
            _phantom: PhantomData,
        })
    }

    async fn build_pg15_pool() -> Option<Pool> {
        let url = std::env::var("GT_POSTGRES15_ENDPOINTS").unwrap_or_default();
        if url.is_empty() {
            return None;
        }
        let mut cfg = Config::new();
        cfg.url = Some(url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .ok()?;
        Some(pool)
    }

    #[tokio::test]
    async fn test_pg15_create_table_in_public_should_fail() {
        maybe_skip_postgres15_integration_test!();
        let Some(pool) = build_pg15_pool().await else {
            return;
        };
        let res = PgStore::with_pg_pool(pool, None, "pg15_public_should_fail", 128, false).await;
        assert!(
            res.is_err(),
            "creating table in public should fail for test_user"
        );
    }

    #[tokio::test]
    async fn test_pg15_create_table_in_test_schema_and_crud_should_succeed() {
        maybe_skip_postgres15_integration_test!();
        let Some(pool) = build_pg15_pool().await else {
            return;
        };
        let schema_name = std::env::var("GT_POSTGRES15_SCHEMA").unwrap();
        let client = pool.get().await.unwrap();
        let factory = PgSqlTemplateFactory::new(Some(&schema_name), "pg15_ok");
        let templates = factory.build();
        client
            .execute(&templates.create_table_statement, &[])
            .await
            .unwrap();
        let kv = PgStore {
            max_txn_ops: 128,
            sql_template_set: templates,
            txn_retry_count: RDS_STORE_TXN_RETRY_COUNT,
            executor_factory: PgExecutorFactory { pool },
            _phantom: PhantomData,
        };
        let prefix = b"pg15_crud/";
        prepare_kv_with_prefix(&kv, prefix.to_vec()).await;
        test_kv_put_with_prefix(&kv, prefix.to_vec()).await;
        test_kv_batch_get_with_prefix(&kv, prefix.to_vec()).await;
        unprepare_kv(&kv, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_with_tls() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let tls_connector = create_postgres_tls_connector(&TlsOption {
            mode: TlsMode::Require,
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: String::new(),
            watch: false,
        })
        .unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), tls_connector)
            .unwrap();
        let client = pool.get().await.unwrap();
        client.execute("SELECT 1", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_pg_with_mtls() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let certs_dir = test_certs_dir();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let tls_connector = create_postgres_tls_connector(&TlsOption {
            mode: TlsMode::Require,
            cert_path: certs_dir.join("client.crt").display().to_string(),
            key_path: certs_dir.join("client.key").display().to_string(),
            ca_cert_path: String::new(),
            watch: false,
        })
        .unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), tls_connector)
            .unwrap();
        let client = pool.get().await.unwrap();
        client.execute("SELECT 1", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_pg_verify_ca() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let certs_dir = test_certs_dir();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let tls_connector = create_postgres_tls_connector(&TlsOption {
            mode: TlsMode::VerifyCa,
            cert_path: certs_dir.join("client.crt").display().to_string(),
            key_path: certs_dir.join("client.key").display().to_string(),
            ca_cert_path: certs_dir.join("root.crt").display().to_string(),
            watch: false,
        })
        .unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), tls_connector)
            .unwrap();
        let client = pool.get().await.unwrap();
        client.execute("SELECT 1", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_pg_verify_full() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let certs_dir = test_certs_dir();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let tls_connector = create_postgres_tls_connector(&TlsOption {
            mode: TlsMode::VerifyFull,
            cert_path: certs_dir.join("client.crt").display().to_string(),
            key_path: certs_dir.join("client.key").display().to_string(),
            ca_cert_path: certs_dir.join("root.crt").display().to_string(),
            watch: false,
        })
        .unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), tls_connector)
            .unwrap();
        let client = pool.get().await.unwrap();
        client.execute("SELECT 1", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_pg_put() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("put_test").await.unwrap();
        let prefix = b"put/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_put_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_range() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("range_test").await.unwrap();
        let prefix = b"range/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_range_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_range_2() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("range2_test").await.unwrap();
        let prefix = b"range2/";
        test_kv_range_2_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_all_range() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("simple_range_test").await.unwrap();
        let prefix = b"";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_simple_kv_range(&kv_backend).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_batch_get() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("batch_get_test").await.unwrap();
        let prefix = b"batch_get/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_batch_get_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_batch_delete() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("batch_delete_test").await.unwrap();
        let prefix = b"batch_delete/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_delete_range_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_batch_delete_with_prefix() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("batch_delete_with_prefix_test")
            .await
            .unwrap();
        let prefix = b"batch_delete/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_batch_delete_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_delete_range() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("delete_range_test").await.unwrap();
        let prefix = b"delete_range/";
        prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
        test_kv_delete_range_with_prefix(&kv_backend, prefix.to_vec()).await;
        unprepare_kv(&kv_backend, prefix).await;
    }

    #[tokio::test]
    async fn test_pg_compare_and_put() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("compare_and_put_test").await.unwrap();
        let prefix = b"compare_and_put/";
        let kv_backend = Arc::new(kv_backend);
        test_kv_compare_and_put_with_prefix(kv_backend.clone(), prefix.to_vec()).await;
    }

    #[tokio::test]
    async fn test_pg_txn() {
        maybe_skip_postgres_integration_test!();
        let kv_backend = build_pg_kv_backend("txn_test").await.unwrap();
        test_txn_one_compare_op(&kv_backend).await;
        text_txn_multi_compare_op(&kv_backend).await;
        test_txn_compare_equal(&kv_backend).await;
        test_txn_compare_greater(&kv_backend).await;
        test_txn_compare_less(&kv_backend).await;
        test_txn_compare_not_equal(&kv_backend).await;
    }

    #[test]
    fn test_pg_template_with_schema() {
        let factory = PgSqlTemplateFactory::new(Some("test_schema"), "greptime_metakv");
        let t = factory.build();
        assert!(
            t.create_table_statement
                .contains("\"test_schema\".\"greptime_metakv\"")
        );
        let upsert = t.generate_batch_upsert_query(1);
        assert!(upsert.contains("\"test_schema\".\"greptime_metakv\""));
        let get = t.generate_batch_get_query(1);
        assert!(get.contains("\"test_schema\".\"greptime_metakv\""));
        let del = t.generate_batch_delete_query(1);
        assert!(del.contains("\"test_schema\".\"greptime_metakv\""));
    }

    #[test]
    fn test_format_table_ident() {
        let t = PgSqlTemplateFactory::format_table_ident(None, "test_table");
        assert_eq!(t, "\"test_table\"");

        let t = PgSqlTemplateFactory::format_table_ident(Some("test_schema"), "test_table");
        assert_eq!(t, "\"test_schema\".\"test_table\"");

        let t = PgSqlTemplateFactory::format_table_ident(Some(""), "test_table");
        assert_eq!(t, "\"test_table\"");
    }

    #[tokio::test]
    async fn test_auto_create_schema_enabled() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();

        let schema_name = "test_auto_create_enabled";
        let table_name = "test_table";

        // Drop the schema if it exists to start clean
        let client = pool.get().await.unwrap();
        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name),
                &[],
            )
            .await;

        // Create store with auto_create_schema enabled
        let _ = PgStore::with_pg_pool(pool.clone(), Some(schema_name), table_name, 128, true)
            .await
            .unwrap();

        // Verify schema was created
        let row = client
            .query_one(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                &[&schema_name],
            )
            .await
            .unwrap();
        let created_schema: String = row.get(0);
        assert_eq!(created_schema, schema_name);

        // Verify table was created in the schema
        let row = client
            .query_one(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
                &[&schema_name, &table_name],
            )
            .await
            .unwrap();
        let created_table_schema: String = row.get(0);
        let created_table_name: String = row.get(1);
        assert_eq!(created_table_schema, schema_name);
        assert_eq!(created_table_name, table_name);

        // Cleanup
        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn test_auto_create_schema_disabled() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();

        let schema_name = "test_auto_create_disabled";
        let table_name = "test_table";

        // Drop the schema if it exists to start clean
        let client = pool.get().await.unwrap();
        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name),
                &[],
            )
            .await;

        // Try to create store with auto_create_schema disabled (should fail)
        let result =
            PgStore::with_pg_pool(pool.clone(), Some(schema_name), table_name, 128, false).await;

        // Verify it failed because schema doesn't exist
        assert!(
            result.is_err(),
            "Expected error when schema doesn't exist and auto_create_schema is disabled"
        );
    }

    #[tokio::test]
    async fn test_auto_create_schema_already_exists() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();

        let schema_name = "test_auto_create_existing";
        let table_name = "test_table";

        // Manually create the schema first
        let client = pool.get().await.unwrap();
        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name),
                &[],
            )
            .await;
        client
            .execute(&format!("CREATE SCHEMA \"{}\"", schema_name), &[])
            .await
            .unwrap();

        // Create store with auto_create_schema enabled (should succeed idempotently)
        let _ = PgStore::with_pg_pool(pool.clone(), Some(schema_name), table_name, 128, true)
            .await
            .unwrap();

        // Verify schema still exists
        let row = client
            .query_one(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                &[&schema_name],
            )
            .await
            .unwrap();
        let created_schema: String = row.get(0);
        assert_eq!(created_schema, schema_name);

        // Verify table was created in the schema
        let row = client
            .query_one(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
                &[&schema_name, &table_name],
            )
            .await
            .unwrap();
        let created_table_schema: String = row.get(0);
        let created_table_name: String = row.get(1);
        assert_eq!(created_table_schema, schema_name);
        assert_eq!(created_table_name, table_name);

        // Cleanup
        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn test_auto_create_schema_no_schema_name() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();

        let table_name = "test_table_no_schema";

        // Create store with auto_create_schema enabled but no schema name (should succeed)
        // This should create the table in the default schema (public)
        let _ = PgStore::with_pg_pool(pool.clone(), None, table_name, 128, true)
            .await
            .unwrap();

        // Verify table was created in public schema
        let client = pool.get().await.unwrap();
        let row = client
            .query_one(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = $1",
                &[&table_name],
            )
            .await
            .unwrap();
        let created_table_schema: String = row.get(0);
        let created_table_name: String = row.get(1);
        assert_eq!(created_table_name, table_name);
        // Verify it's in public schema (or whichever is the default)
        assert!(created_table_schema == "public" || !created_table_schema.is_empty());

        // Cleanup
        let _ = client
            .execute(&format!("DROP TABLE IF EXISTS \"{}\"", table_name), &[])
            .await;
    }

    #[tokio::test]
    async fn test_auto_create_schema_with_empty_schema_name() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_postgres_integration_test!();
        let endpoints = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let mut cfg = Config::new();
        cfg.url = Some(endpoints);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(CreatePostgresPoolSnafu)
            .unwrap();

        let table_name = "test_table_empty_schema";

        // Create store with auto_create_schema enabled but empty schema name (should succeed)
        // This should create the table in the default schema (public)
        let _ = PgStore::with_pg_pool(pool.clone(), Some(""), table_name, 128, true)
            .await
            .unwrap();

        // Verify table was created in public schema
        let client = pool.get().await.unwrap();
        let row = client
            .query_one(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = $1",
                &[&table_name],
            )
            .await
            .unwrap();
        let created_table_schema: String = row.get(0);
        let created_table_name: String = row.get(1);
        assert_eq!(created_table_name, table_name);
        // Verify it's in public schema (or whichever is the default)
        assert!(created_table_schema == "public" || !created_table_schema.is_empty());

        // Cleanup
        let _ = client
            .execute(&format!("DROP TABLE IF EXISTS \"{}\"", table_name), &[])
            .await;
    }
}
