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

//! Endpoint-only query regression data plane.
//!
//! This module deliberately owns no lifecycle, local-storage, or controller
//! behaviour. It consumes sealed controller artifacts and only uses their
//! declared HTTP endpoints.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::query_perf::case::{self, ValidatedCase};
use crate::query_perf::error::{Error, Result};
use crate::query_perf::fixture::{RemoteWriteRequest, run_prom_remote_write};
use crate::query_perf::manifest::{
    ArtifactReference, Capability, EvaluationState, FailedMeasurementAttempt, FixtureManifest,
    FooterRule, MANIFEST_VERSION, MeasurementBundle, MeasurementManifest, ObservationBundle,
    ObservationPayload, ObservationRequest, ObservationRequestManifest, QueryMeasurement,
    QueryStatus, ReadBenchRequest, ReportFailure, ReportFailureStage, ReportManifest,
    ReportOutcome, ResolvedQueryIdentity, RunnerFixtureProvider, Sha256Digest, TargetBinding,
    TargetEnvironmentManifest, TargetRole, ThresholdEvidence, ThresholdOutcome,
    ThresholdOutcomeKey, ThresholdResult, TimestampUnit,
};

/// Inputs accepted by the endpoint measurement command.
#[derive(Debug, Clone)]
pub struct MeasureArgs {
    pub case: PathBuf,
    pub targets: PathBuf,
    pub fixture: PathBuf,
    pub out: PathBuf,
}

/// Inputs accepted by the endpoint finalization command.
#[derive(Debug, Clone)]
pub struct FinalizeArgs {
    pub case: PathBuf,
    pub targets: PathBuf,
    pub fixture: PathBuf,
    pub measurements: PathBuf,
    pub observations: PathBuf,
    pub report: PathBuf,
    pub summary: PathBuf,
}

/// The one immutable output of `measure`; both embedded artifacts are sealed.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum MeasureArtifact {
    Success {
        measurement_bundle: MeasurementBundle,
        observation_request: ObservationRequestManifest,
    },
    Failed {
        attempt: FailedMeasurementAttempt,
    },
}

#[derive(Debug)]
struct Preflight {
    case: ValidatedCase,
    case_digest: Sha256Digest,
    queries: Vec<ResolvedQueryIdentity>,
    targets: TargetEnvironmentManifest,
    fixture: FixtureManifest,
}

#[derive(Default)]
struct TrustedEvidence {
    measurements: Vec<MeasurementManifest>,
    observations: Vec<crate::query_perf::manifest::ObservationManifest>,
    measurement_bundle: Option<MeasurementBundle>,
    observation_bundle: Option<ObservationBundle>,
}

struct FinalizeFailure {
    stage: ReportFailureStage,
    error: Error,
}

impl FinalizeFailure {
    fn new(stage: ReportFailureStage, error: Error) -> Self {
        Self { stage, error }
    }
}

/// Minimal request seam used by focused tests and by the reqwest adapter.
#[async_trait]
pub trait EndpointClient: Send + Sync {
    async fn health(&self, target: &TargetBinding) -> Result<()>;
    async fn statement(
        &self,
        target: &TargetBinding,
        sql: &str,
        database: &str,
    ) -> Result<serde_json::Value>;
}

/// Injection boundary for remote-write side effects; endpoint orchestration remains local.
#[async_trait]
trait RemoteWriteExecutor: Send + Sync {
    async fn execute(&self, request: RemoteWriteRequest) -> Result<()>;
}

struct PromRemoteWriteExecutor;

#[async_trait]
impl RemoteWriteExecutor for PromRemoteWriteExecutor {
    async fn execute(&self, request: RemoteWriteRequest) -> Result<()> {
        run_prom_remote_write(request).await.map(|_| ())
    }
}

/// Reusable endpoint client. It never alters declared endpoint paths or TLS settings.
pub struct ReqwestEndpointClient {
    client: reqwest::Client,
}

const RUNNER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RUNNER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

impl ReqwestEndpointClient {
    pub fn new() -> Result<Self> {
        Self::build(RUNNER_CONNECT_TIMEOUT, RUNNER_REQUEST_TIMEOUT)
    }

    fn build(connect_timeout: Duration, request_timeout: Duration) -> Result<Self> {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map(|client| Self { client })
            .map_err(|err| Error::new(format!("failed to build endpoint client: {err}")))
    }

    #[cfg(test)]
    fn with_timeouts(connect_timeout: Duration, request_timeout: Duration) -> Result<Self> {
        Self::build(connect_timeout, request_timeout)
    }
}

#[async_trait]
impl EndpointClient for ReqwestEndpointClient {
    async fn health(&self, target: &TargetBinding) -> Result<()> {
        let response = self
            .client
            .get(target.health_endpoint.as_str())
            .send()
            .await
            .map_err(|err| Error::new(format!("health request failed: {err}")))?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(Error::new(format!(
                "health endpoint returned {}, expected 200",
                response.status()
            )));
        }
        Ok(())
    }

    async fn statement(
        &self,
        target: &TargetBinding,
        sql: &str,
        database: &str,
    ) -> Result<serde_json::Value> {
        let response = self
            .client
            .post(target.sql_endpoint.as_str())
            .form(&[("sql", sql), ("db", database), ("format", "json")])
            .send()
            .await
            .map_err(|err| Error::new(format!("SQL request failed: {err}")))?;
        if !response.status().is_success() {
            return Err(Error::new(format!(
                "SQL endpoint returned {}",
                response.status()
            )));
        }
        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(|err| Error::new(format!("SQL response is not JSON: {err}")))?;
        valid_response_envelope(&body)?;
        Ok(body)
    }
}

/// Measures both targets in base then candidate order and atomically writes a sealed artifact.
pub async fn measure(args: MeasureArgs) -> Result<()> {
    let preflight = load_preflight(&args.case, &args.targets, &args.fixture)?;
    let client = ReqwestEndpointClient::new()?;
    match measure_with_client(&preflight, &client).await {
        Ok(artifact) => write_json_atomic(&args.out, &artifact),
        Err(err) => {
            let mut attempt = FailedMeasurementAttempt {
                version: MANIFEST_VERSION,
                run_id: preflight.targets.run_id.clone(),
                fixture_id: preflight.fixture.fixture_id.clone(),
                case_digest: preflight.case_digest.clone(),
                target_manifest_digest: preflight.targets.completion_digest.clone(),
                fixture_manifest_digest: preflight.fixture.completion_digest.clone(),
                failure_detail: err.to_string(),
                completion_digest: Sha256Digest::compute("unsealed"),
            };
            attempt.seal()?;
            write_json_atomic(&args.out, &MeasureArtifact::Failed { attempt })?;
            Err(err)
        }
    }
}

async fn measure_with_client(
    preflight: &Preflight,
    client: &dyn EndpointClient,
) -> Result<MeasureArtifact> {
    let waiter = TokioVisibilityWaiter;
    measure_with_client_and_waiter(preflight, client, &waiter).await
}

async fn measure_with_client_and_waiter(
    preflight: &Preflight,
    client: &dyn EndpointClient,
    waiter: &dyn VisibilityWaiter,
) -> Result<MeasureArtifact> {
    for target in ordered_targets(&preflight.targets)? {
        client.health(target).await?;
    }
    if matches!(preflight.fixture.provider, RunnerFixtureProvider::ApiWrite) {
        let remote_write = PromRemoteWriteExecutor;
        let plan = preflight
            .case
            .remote_write()
            .ok_or_else(|| Error::new("api_write fixture requires a remote-write case"))?;
        for target in ordered_targets(&preflight.targets)? {
            let endpoint = target
                .remote_write_endpoint
                .as_ref()
                .ok_or_else(|| Error::new("api_write target has no remote-write endpoint"))?;
            write_api_fixture(client, &remote_write, target, endpoint.as_str(), plan).await?;
            verify_visible_rows_until(
                client,
                target,
                plan.database(),
                plan.metric(),
                expected_remote_rows(plan)?,
                Duration::from_secs(plan.visibility_timeout_seconds()),
                waiter,
            )
            .await?;
        }
    } else {
        for target in ordered_targets(&preflight.targets)? {
            let direct = preflight.case.direct_readable_sst()?;
            for table in direct.tables() {
                let binding = preflight
                    .fixture
                    .target_bindings
                    .iter()
                    .find(|binding| {
                        binding.target_id == target.target_id
                            && binding.database == table.database()
                            && binding.logical_table == table.name()
                    })
                    .ok_or_else(|| Error::new("fixture binding is missing target table"))?;
                verify_external_prepared(
                    client,
                    target,
                    binding,
                    table,
                    &preflight.fixture.logical_summary,
                )
                .await?;
            }
        }
    }

    let mut request = ObservationRequestManifest {
        version: MANIFEST_VERSION,
        run_id: preflight.targets.run_id.clone(),
        request_id: format!("{}-observations", preflight.fixture.fixture_id),
        fixture_id: preflight.fixture.fixture_id.clone(),
        case_digest: preflight.case_digest.clone(),
        target_manifest_digest: preflight.targets.completion_digest.clone(),
        fixture_manifest_digest: preflight.fixture.completion_digest.clone(),
        requests: ordered_targets(&preflight.targets)?
            .iter()
            .flat_map(|target| {
                preflight
                    .case
                    .observation_kinds()
                    .into_iter()
                    .map(move |kind| ObservationRequest {
                        role: target.role,
                        target_id: target.target_id.clone(),
                        read_bench: if kind
                            == crate::query_perf::manifest::ObservationKind::ReadBench
                        {
                            preflight.case.remote_write().map(|plan| ReadBenchRequest {
                                parquetbench: plan.read_bench_parquet(),
                                scanbench: plan.read_bench_scan(),
                            })
                        } else {
                            None
                        },
                        kind,
                    })
            })
            .collect(),
        completion_digest: Sha256Digest::compute("unsealed"),
    };
    request.seal()?;
    validate_case_observation_request(&request, &preflight.case, &preflight.targets)?;

    let mut measurements = Vec::with_capacity(2);
    for target in ordered_targets(&preflight.targets)? {
        measurements.push(
            measure_target(
                client,
                target,
                &preflight.case,
                &preflight.queries,
                &preflight.targets.run_id,
                &preflight.fixture.fixture_id,
            )
            .await?,
        );
    }
    let mut bundle = MeasurementBundle {
        version: MANIFEST_VERSION,
        run_id: preflight.targets.run_id.clone(),
        fixture_id: preflight.fixture.fixture_id.clone(),
        case_digest: preflight.case_digest.clone(),
        target_manifest_digest: preflight.targets.completion_digest.clone(),
        fixture_manifest_digest: preflight.fixture.completion_digest.clone(),
        measurements,
        observation_request: ArtifactReference {
            id: request.request_id.clone(),
            digest: request.completion_digest.clone(),
        },
        completion_digest: Sha256Digest::compute("unsealed"),
    };
    bundle.seal()?;
    bundle.validate_against(
        &preflight.queries,
        &preflight.case_digest,
        &preflight.targets,
        &preflight.fixture,
        &request,
    )?;
    Ok(MeasureArtifact::Success {
        measurement_bundle: bundle,
        observation_request: request,
    })
}

/// Finalizes controller-supplied observation results into typed JSON and Markdown.
pub fn finalize(args: FinalizeArgs) -> Result<()> {
    let preflight = load_preflight(&args.case, &args.targets, &args.fixture)?;
    validate_finalize_destinations(&args.report, &args.summary)?;
    let mut evidence = TrustedEvidence::default();
    let mut report = match finalize_inner(&args, &preflight, &mut evidence) {
        Ok(report) => report,
        Err(failure) => return publish_failed_report(&args, &preflight, &evidence, failure),
    };
    let threshold_failed = report.outcome == ReportOutcome::Failed;
    let markdown = match prepare_report(&mut report, &preflight, &evidence) {
        Ok(markdown) => markdown,
        Err(err) => {
            return publish_failed_report(
                &args,
                &preflight,
                &evidence,
                FinalizeFailure::new(ReportFailureStage::ReportInvariant, err),
            );
        }
    };
    publish_prepared_report(
        &args.report,
        &report,
        &args.summary,
        markdown.as_bytes(),
        threshold_failed,
    )
}

fn finalize_inner(
    args: &FinalizeArgs,
    preflight: &Preflight,
    evidence: &mut TrustedEvidence,
) -> std::result::Result<ReportManifest, FinalizeFailure> {
    let artifact: MeasureArtifact = read_json(&args.measurements)
        .map_err(|err| FinalizeFailure::new(ReportFailureStage::MeasurementArtifact, err))?;
    let (bundle, request) = match artifact {
        MeasureArtifact::Success {
            measurement_bundle,
            observation_request,
        } => (measurement_bundle, observation_request),
        MeasureArtifact::Failed { attempt } => {
            validate_failed_attempt(&attempt, preflight)
                .map_err(|err| FinalizeFailure::new(ReportFailureStage::MeasurementAttempt, err))?;
            return Err(FinalizeFailure::new(
                ReportFailureStage::MeasurementAttempt,
                Error::new(format!(
                    "cannot finalize failed measurement attempt: {}",
                    attempt.failure_detail
                )),
            ));
        }
    };
    request
        .validate_against(
            &preflight.case_digest,
            &preflight.targets,
            &preflight.fixture,
        )
        .and_then(|_| {
            validate_case_observation_request(&request, &preflight.case, &preflight.targets)
        })
        .map_err(|err| FinalizeFailure::new(ReportFailureStage::ObservationRequest, err))?;
    for measurement in &bundle.measurements {
        if measurement
            .validate_against(&preflight.queries, &preflight.targets, &preflight.fixture)
            .is_ok()
        {
            evidence.measurements.push(measurement.clone());
        }
    }
    bundle
        .validate_against(
            &preflight.queries,
            &preflight.case_digest,
            &preflight.targets,
            &preflight.fixture,
            &request,
        )
        .map_err(|err| FinalizeFailure::new(ReportFailureStage::MeasurementBundle, err))?;
    evidence.measurements = bundle.measurements.clone();
    evidence.measurement_bundle = Some(bundle.clone());
    let observations: ObservationBundle = read_json(&args.observations)
        .map_err(|err| FinalizeFailure::new(ReportFailureStage::ObservationArtifact, err))?;
    observations
        .validate_against(
            &preflight.case_digest,
            &preflight.targets,
            &preflight.fixture,
            &request,
        )
        .map_err(|err| FinalizeFailure::new(ReportFailureStage::ObservationBundle, err))?;
    evidence.observations = observations.observations.clone();
    evidence.observation_bundle = Some(observations.clone());
    evaluate_report(preflight, &bundle, &observations)
        .map_err(|err| FinalizeFailure::new(ReportFailureStage::Evaluation, err))
}

fn validate_failed_attempt(
    attempt: &FailedMeasurementAttempt,
    preflight: &Preflight,
) -> Result<()> {
    attempt.validate()?;
    if attempt.run_id != preflight.targets.run_id
        || attempt.fixture_id != preflight.fixture.fixture_id
        || attempt.case_digest != preflight.case_digest
        || attempt.target_manifest_digest != preflight.targets.completion_digest
        || attempt.fixture_manifest_digest != preflight.fixture.completion_digest
    {
        return Err(Error::new("failed measurement attempt identity mismatch"));
    }
    Ok(())
}

fn failed_report(
    preflight: &Preflight,
    evidence: &TrustedEvidence,
    failure: &FinalizeFailure,
) -> ReportManifest {
    ReportManifest {
        version: MANIFEST_VERSION,
        run_id: preflight.targets.run_id.clone(),
        report_id: format!("report-{}", preflight.fixture.fixture_id),
        target_ids: preflight
            .targets
            .targets
            .iter()
            .map(|target| target.target_id.clone())
            .collect(),
        fixture_id: preflight.fixture.fixture_id.clone(),
        queries: preflight.queries.clone(),
        threshold_results: vec![],
        threshold_outcomes: vec![],
        evaluation_state: if failure.stage == ReportFailureStage::Evaluation {
            EvaluationState::Incomplete
        } else {
            EvaluationState::NotStarted
        },
        outcome: ReportOutcome::Failed,
        failures: vec![failure.error.to_string()],
        failure_details: vec![ReportFailure {
            stage: failure.stage,
            detail: failure.error.to_string(),
        }],
        measurements: evidence
            .measurements
            .iter()
            .map(|value| ArtifactReference {
                id: value.measurement_id.clone(),
                digest: value.completion_digest.clone(),
            })
            .collect(),
        observations: evidence
            .observations
            .iter()
            .map(|value| ArtifactReference {
                id: value.observation_id.clone(),
                digest: value.completion_digest.clone(),
            })
            .collect(),
        measurement_bundle: evidence
            .measurement_bundle
            .as_ref()
            .map(|bundle| ArtifactReference {
                id: "measurement_bundle".to_string(),
                digest: bundle.completion_digest.clone(),
            }),
        observation_bundle: evidence
            .observation_bundle
            .as_ref()
            .map(|bundle| ArtifactReference {
                id: "observation_bundle".to_string(),
                digest: bundle.completion_digest.clone(),
            }),
        target_manifest_digest: preflight.targets.completion_digest.clone(),
        fixture_manifest_digest: preflight.fixture.completion_digest.clone(),
        provenance: preflight.case_digest.clone(),
        summary_digest: Sha256Digest::compute([]),
        completion_digest: Sha256Digest::compute("unsealed"),
    }
}

fn prepare_report(
    report: &mut ReportManifest,
    preflight: &Preflight,
    evidence: &TrustedEvidence,
) -> Result<String> {
    let markdown = markdown_summary(report);
    report.summary_digest = Sha256Digest::compute(markdown.as_bytes());
    report.seal()?;
    report.validate_against(
        &preflight.queries,
        &preflight.targets,
        &preflight.fixture,
        &evidence.measurements,
        &evidence.observations,
    )?;
    report.validate_provenance(&preflight.case_digest)?;
    report.validate_threshold_coverage(&preflight.case, &preflight.targets)?;
    report.validate_bundle_references(
        evidence.measurement_bundle.as_ref(),
        evidence.observation_bundle.as_ref(),
    )?;
    report.validate_summary(markdown.as_bytes())?;
    Ok(markdown)
}

fn publish_prepared_report(
    report_path: &Path,
    report: &impl Serialize,
    summary_path: &Path,
    markdown: &[u8],
    failed: bool,
) -> Result<()> {
    match write_pair_atomic(report_path, report, summary_path, markdown) {
        Ok(()) if failed => Err(Error::new("query regression thresholds failed")),
        Ok(()) => Ok(()),
        Err(publication) if failed => Err(Error::new(format!(
            "query regression thresholds failed; failed-report publication failed: {publication}"
        ))),
        Err(publication) => Err(publication),
    }
}

fn publish_failed_report(
    args: &FinalizeArgs,
    preflight: &Preflight,
    evidence: &TrustedEvidence,
    failure: FinalizeFailure,
) -> Result<()> {
    let mut report = failed_report(preflight, evidence, &failure);
    let markdown = prepare_report(&mut report, preflight, evidence).map_err(|publication| {
        Error::new(format!(
            "{}; failed-report publication failed: {publication}",
            failure.error
        ))
    })?;
    write_pair_atomic(&args.report, &report, &args.summary, markdown.as_bytes()).map_err(
        |publication| {
            Error::new(format!(
                "{}; failed-report publication failed: {publication}",
                failure.error
            ))
        },
    )?;
    Err(failure.error)
}

fn validate_finalize_destinations(report: &Path, summary: &Path) -> Result<()> {
    if report == summary
        || report.file_name() == summary.file_name() && report.parent() == summary.parent()
    {
        return Err(Error::new("report and summary destinations must differ"));
    }
    for path in [report, summary] {
        let parent = path
            .parent()
            .ok_or_else(|| Error::new("output path has no parent"))?;
        fs::create_dir_all(parent)
            .map_err(|err| Error::new(format!("failed to create output directory: {err}")))?;
        if !parent.is_dir()
            || path.is_dir()
            || path.file_name().and_then(|name| name.to_str()).is_none()
        {
            return Err(Error::new("output destination is unusable"));
        }
        let temporary = stage_atomic(path, b"")?;
        fs::remove_file(temporary)
            .map_err(|err| Error::new(format!("failed to validate output destination: {err}")))?;
    }
    Ok(())
}

fn load_preflight(case_path: &Path, targets_path: &Path, fixture_path: &Path) -> Result<Preflight> {
    let case = case::load_case(case_path)?;
    let targets: TargetEnvironmentManifest = read_json(targets_path)?;
    let fixture: FixtureManifest = read_json(fixture_path)?;
    validate_preflight(case, targets, fixture)
}

fn validate_preflight(
    case: ValidatedCase,
    targets: TargetEnvironmentManifest,
    fixture: FixtureManifest,
) -> Result<Preflight> {
    let queries = case.manifest_queries()?;
    let case_digest = case.executable_digest()?;
    targets.validate()?;
    fixture.validate_against_case(&targets, &case)?;
    if fixture.data_plan_digest != case.fixture_plan_digest()? {
        return Err(Error::new(
            "fixture data_plan_digest does not match normalized fixture workload",
        ));
    }
    if matches!(fixture.provider, RunnerFixtureProvider::ExternalPrepared) {
        validate_external_case_bindings(&case, &targets, &fixture)?;
    }
    for target in &targets.targets {
        if !target.capabilities.contains(&Capability::Sql) {
            return Err(Error::new(
                "all SQL and TQL queries require Capability::Sql",
            ));
        }
    }
    if matches!(fixture.provider, RunnerFixtureProvider::ApiWrite)
        && (case.remote_write().is_none()
            || targets.targets.iter().any(|target| {
                !target.capabilities.contains(&Capability::RemoteWrite)
                    || target.remote_write_endpoint.is_none()
            }))
    {
        return Err(Error::new(
            "api_write requires remote-write capabilities and endpoints",
        ));
    }
    for kind in case.observation_kinds() {
        let capability = match kind {
            crate::query_perf::manifest::ObservationKind::Footer => Capability::FooterObservation,
            crate::query_perf::manifest::ObservationKind::ReadBench => Capability::ReadBench,
        };
        if targets
            .targets
            .iter()
            .any(|target| !target.capabilities.contains(&capability))
        {
            return Err(Error::new(
                "case observation requires an undeclared target capability",
            ));
        }
    }
    Ok(Preflight {
        case,
        case_digest,
        queries,
        targets,
        fixture,
    })
}

fn ordered_targets(targets: &TargetEnvironmentManifest) -> Result<[&TargetBinding; 2]> {
    let base = targets
        .targets
        .iter()
        .find(|target| target.role == TargetRole::Base);
    let candidate = targets
        .targets
        .iter()
        .find(|target| target.role == TargetRole::Candidate);
    match (base, candidate) {
        (Some(base), Some(candidate)) => Ok([base, candidate]),
        _ => Err(Error::new("targets must contain base and candidate")),
    }
}
fn validate_case_observation_request(
    request: &ObservationRequestManifest,
    case: &ValidatedCase,
    targets: &TargetEnvironmentManifest,
) -> Result<()> {
    let expected: std::collections::HashSet<_> = ordered_targets(targets)?
        .iter()
        .flat_map(|target| {
            case.observation_kinds()
                .into_iter()
                .map(move |kind| (target.role, target.target_id.as_str(), kind))
        })
        .collect();
    let actual: std::collections::HashSet<_> = request
        .requests
        .iter()
        .map(|value| (value.role, value.target_id.as_str(), value.kind))
        .collect();
    if request.requests.len() != actual.len() || actual != expected {
        return Err(Error::new(
            "observation request does not exactly match case requirements",
        ));
    }
    Ok(())
}

async fn measure_target(
    client: &dyn EndpointClient,
    target: &TargetBinding,
    case: &ValidatedCase,
    identities: &[ResolvedQueryIdentity],
    run_id: &str,
    fixture_id: &str,
) -> Result<MeasurementManifest> {
    let mut queries = Vec::with_capacity(identities.len());
    for (query, identity) in case.queries().iter().zip(identities) {
        for _ in 0..query.warmup() {
            client
                .statement(target, query.text(), query.resolved_identity().database())
                .await?;
        }
        let mut samples = Vec::with_capacity(identity.iterations as usize);
        let mut bodies = Vec::with_capacity(identity.iterations as usize);
        for _ in 0..identity.iterations {
            let started = Instant::now();
            let body = client
                .statement(target, query.text(), query.resolved_identity().database())
                .await?;
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            if !elapsed.is_finite() || elapsed < 0.0 {
                return Err(Error::new("query latency is not finite"));
            }
            samples.push(crate::query_perf::manifest::LatencySample {
                milliseconds: elapsed,
            });
            bodies.push(body);
        }
        queries.push(QueryMeasurement {
            identity: identity.clone(),
            status: QueryStatus::Passed,
            samples,
            result_digest: Sha256Digest::compute(canonical_json_bytes(&serde_json::Value::Array(
                bodies,
            ))?),
        });
    }
    let mut measurement = MeasurementManifest {
        version: MANIFEST_VERSION,
        run_id: run_id.to_string(),
        measurement_id: format!("measurement-{}", target.target_id),
        target_id: target.target_id.clone(),
        fixture_id: fixture_id.to_string(),
        queries,
        completion_digest: Sha256Digest::compute("unsealed"),
    };
    measurement.seal()?;
    Ok(measurement)
}

async fn write_api_fixture(
    client: &dyn EndpointClient,
    remote_write: &dyn RemoteWriteExecutor,
    target: &TargetBinding,
    endpoint: &str,
    plan: &case::ValidatedRemoteWritePlan,
) -> Result<()> {
    let total = plan.samples_per_series();
    let chunk = plan.sample_chunk_size().unwrap_or(total);
    let mut offset = 0_u64;
    let mut chunks = 0_u64;
    while offset < total {
        let count = chunk.min(total - offset);
        let request = RemoteWriteRequest {
            endpoint: endpoint.to_string(),
            database: plan.database().to_string(),
            metric: plan.metric().to_string(),
            physical_table: plan.physical_table().to_string(),
            series_count: plan.series_count(),
            samples_per_series: count,
            start_unix_millis: plan
                .start_unix_millis()
                .checked_add(
                    i64::try_from(offset)
                        .map_err(|_| Error::new("sample offset exceeds i64"))?
                        .checked_mul(plan.step_millis())
                        .ok_or_else(|| Error::new("sample offset timestamp overflows"))?,
                )
                .ok_or_else(|| Error::new("sample offset timestamp overflows"))?,
            step_millis: plan.step_millis(),
            chunk_series_count: plan.chunk_series_count(),
            timeout_seconds: plan.timeout_seconds(),
            value_pattern: plan.value().pattern(),
            value_base: plan.value().base(),
            value_step: plan.value().step(),
            value_cardinality: plan.value().cardinality(),
            value_seed: plan.value().seed(),
            value_run_length: plan.value().run_length(),
            value_stall_every: plan.value().stall_every(),
            value_stall_length: plan.value().stall_length(),
            value_mixed_every: plan.value().mixed_every(),
            value_sample_offset: offset,
            value_total_samples_per_series: Some(total),
        };
        remote_write.execute(request).await?;
        offset = offset
            .checked_add(count)
            .ok_or_else(|| Error::new("sample chunk offset overflows"))?;
        chunks += 1;
        if chunks.is_multiple_of(plan.flush_every_sample_chunks()) {
            flush_table(client, target, plan.database(), plan.physical_table()).await?;
        }
    }
    if !chunks.is_multiple_of(plan.flush_every_sample_chunks()) {
        flush_table(client, target, plan.database(), plan.physical_table()).await?;
    }
    Ok(())
}

async fn flush_table(
    client: &dyn EndpointClient,
    target: &TargetBinding,
    database: &str,
    table: &str,
) -> Result<()> {
    let escaped = table.replace('\'', "''");
    client
        .statement(target, &format!("ADMIN FLUSH_TABLE('{escaped}')"), database)
        .await
        .map(|_| ())
}

fn expected_remote_rows(plan: &case::ValidatedRemoteWritePlan) -> Result<u64> {
    plan.series_count()
        .checked_mul(plan.samples_per_series())
        .ok_or_else(|| Error::new("remote row count overflows"))
}

async fn verify_visible_rows_until(
    client: &dyn EndpointClient,
    target: &TargetBinding,
    database: &str,
    table: &str,
    expected: u64,
    timeout: Duration,
    waiter: &dyn VisibilityWaiter,
) -> Result<()> {
    let table = quote_identifier(table);
    let deadline = waiter.now() + timeout;
    loop {
        let body = client
            .statement(
                target,
                &format!("SELECT COUNT(*) AS count FROM {table}"),
                database,
            )
            .await?;
        if extract_single_u64(&body)? == expected {
            return Ok(());
        }
        if waiter.now() >= deadline {
            return Err(Error::new(
                "server-visible row count does not match fixture summary",
            ));
        }
        waiter.sleep(Duration::from_millis(200)).await;
    }
}

#[async_trait]
trait VisibilityWaiter: Send + Sync {
    fn now(&self) -> Instant;
    async fn sleep(&self, duration: Duration);
}

struct TokioVisibilityWaiter;

#[async_trait]
impl VisibilityWaiter for TokioVisibilityWaiter {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

async fn verify_external_prepared(
    client: &dyn EndpointClient,
    target: &TargetBinding,
    binding: &crate::query_perf::manifest::FixtureTargetBinding,
    case_table: &crate::query_perf::case::ValidatedTable,
    summary: &crate::query_perf::manifest::LogicalFixtureSummary,
) -> Result<()> {
    let external = binding
        .external
        .as_ref()
        .ok_or_else(|| Error::new("external fixture binding evidence is missing"))?;
    let catalog = binding
        .catalog
        .as_deref()
        .ok_or_else(|| Error::new("external fixture binding catalog is missing"))?;
    let table = qualified_table(catalog, &binding.database, &binding.logical_table);
    let show = client
        .statement(
            target,
            &format!("SHOW CREATE TABLE {table}"),
            &binding.database,
        )
        .await?;
    validate_show_create(&show, binding, case_table)?;
    let db = quote_sql_string(&binding.database);
    let catalog = quote_sql_string(&external.catalog);
    let name = quote_sql_string(&binding.logical_table);
    let table_info = client.statement(target, &format!("SELECT table_id AS table_id FROM information_schema.tables WHERE table_catalog = {catalog} AND table_schema = {db} AND table_name = {name}"), &binding.database).await?;
    if exact_u64_field(&table_info, "table_id")? != external.table_id {
        return Err(Error::new(
            "information_schema table_id differs from binding",
        ));
    }
    let peers = client.statement(target, &format!("SELECT region_id AS region_id, is_leader AS is_leader, status AS status FROM information_schema.region_peers WHERE table_catalog = {catalog} AND table_schema = {db} AND table_name = {name}"), &binding.database).await?;
    let peer_rows = data_rows(&peers)?;
    if target.topology != "single" {
        return Err(Error::new(
            "external_prepared supports only the declared single-region topology",
        ));
    }
    if peer_rows.len() != 1 {
        return Err(Error::new(
            "region peer evidence has unexpected cardinality",
        ));
    }
    let peer = peer_rows
        .iter()
        .find(|row| {
            row.get("region_id").and_then(serde_json::Value::as_u64) == Some(external.region_id)
        })
        .ok_or_else(|| Error::new("expected region_id is absent"))?;
    if peer.get("is_leader").and_then(serde_json::Value::as_bool) != Some(true)
        || peer.get("status").and_then(serde_json::Value::as_str) != Some("ALIVE")
    {
        return Err(Error::new(
            "single topology requires one alive leader region",
        ));
    }
    let time = quote_identifier(&external.time_index);
    let evidence = client.statement(target, &format!("SELECT COUNT(*) AS count, MIN({time}) AS min_time, MAX({time}) AS max_time FROM {table}"), &binding.database).await?;
    let row = exact_row(&evidence, &["count", "min_time", "max_time"])?;
    let count = row
        .get("count")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::new("count must be unsigned"))?;
    let min = row
        .get("min_time")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| Error::new("min_time must be integer"))?;
    let max = row
        .get("max_time")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| Error::new("max_time must be integer"))?;
    if count != summary.rows
        || to_nanos(min, external.time_unit)? != summary.time_start_unix_nanos
        || to_nanos(max, external.time_unit)? != summary.time_end_unix_nanos
    {
        return Err(Error::new(
            "external fixture row or time evidence differs from logical summary",
        ));
    }
    Ok(())
}

fn validate_show_create(
    value: &serde_json::Value,
    binding: &crate::query_perf::manifest::FixtureTargetBinding,
    case_table: &crate::query_perf::case::ValidatedTable,
) -> Result<()> {
    let row = exact_row(value, &["Table", "Create Table"])?;
    let table = row
        .get("Table")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::new("SHOW CREATE TABLE Table must be a string"))?;
    let create = row
        .get("Create Table")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::new("SHOW CREATE TABLE Create Table must be a string"))?;
    if table != binding.logical_table
        || binding.logical_table != case_table.name()
        || binding.database != case_table.database()
    {
        return Err(Error::new(
            "SHOW CREATE TABLE Table identity does not match case table",
        ));
    }
    validate_create_table_ddl(create, case_table)
}

fn qualified_table(catalog: &str, database: &str, table: &str) -> String {
    [catalog, database, table]
        .into_iter()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DdlToken {
    Atom(String),
    Quoted(String),
    LParen,
    RParen,
    Comma,
    Equals,
}

fn validate_create_table_ddl(
    ddl: &str,
    table: &crate::query_perf::case::ValidatedTable,
) -> Result<()> {
    let tokens = tokenize_ddl(ddl)?;
    let expected_prefix = ["CREATE", "TABLE", "IF", "NOT", "EXISTS"];
    if tokens.len() < expected_prefix.len() + 2
        || !expected_prefix
            .iter()
            .enumerate()
            .all(|(index, expected)| token_is(&tokens[index], expected))
        || token_quoted(&tokens[5]) != Some(table.name())
        || tokens.get(6) != Some(&DdlToken::LParen)
    {
        return Err(Error::new(
            "SHOW CREATE TABLE DDL must use CREATE TABLE IF NOT EXISTS with the exact unqualified table name",
        ));
    }
    let close = matching_rparen(&tokens, 6)?;
    let entries = split_ddl_entries(&tokens[7..close])?;
    if entries.len() != table.columns().len() + 2 {
        return Err(Error::new(
            "SHOW CREATE TABLE has unexpected columns or constraints",
        ));
    }
    for (entry, column) in entries
        .iter()
        .take(table.columns().len())
        .zip(table.columns())
    {
        validate_ddl_column(entry, column)?;
    }
    validate_time_index_entry(&entries[table.columns().len()], table.time_index())?;
    validate_primary_key_entry(&entries[table.columns().len() + 1], table.primary_key())?;
    if !token_is(
        tokens
            .get(close + 1)
            .ok_or_else(|| Error::new("SHOW CREATE TABLE lacks engine"))?,
        "ENGINE",
    ) || tokens.get(close + 2) != Some(&DdlToken::Equals)
        || !token_is(
            tokens
                .get(close + 3)
                .ok_or_else(|| Error::new("SHOW CREATE TABLE lacks engine value"))?,
            table.engine(),
        )
    {
        return Err(Error::new("SHOW CREATE TABLE engine does not match case"));
    }
    let options = ddl_options(&tokens[close + 4..])?;
    if table.append_mode().is_some_and(|append| {
        options.get("append_mode").map(String::as_str)
            != Some(if append { "true" } else { "false" })
    }) || table
        .sst_format_option()
        .is_some_and(|format| options.get("sst_format").map(String::as_str) != Some(format))
    {
        return Err(Error::new("SHOW CREATE TABLE options do not match case"));
    }
    Ok(())
}

fn tokenize_ddl(ddl: &str) -> Result<Vec<DdlToken>> {
    let mut tokens = Vec::new();
    let mut chars = ddl.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            character if character.is_whitespace() || character == ';' => {}
            '(' => tokens.push(DdlToken::LParen),
            ')' => tokens.push(DdlToken::RParen),
            ',' => tokens.push(DdlToken::Comma),
            '=' => tokens.push(DdlToken::Equals),
            '\'' | '"' => {
                let quote = character;
                let mut value = String::new();
                loop {
                    let current = chars
                        .next()
                        .ok_or_else(|| Error::new("SHOW CREATE TABLE has an unterminated quote"))?;
                    if current == quote {
                        if chars.peek() == Some(&quote) {
                            chars.next();
                            value.push(quote);
                        } else {
                            break;
                        }
                    } else {
                        value.push(current);
                    }
                }
                tokens.push(DdlToken::Quoted(value));
            }
            _ => {
                let mut value = character.to_string();
                while chars.peek().is_some_and(|next| {
                    !next.is_whitespace()
                        && !matches!(next, '(' | ')' | ',' | '=' | '\'' | '"' | ';')
                }) {
                    value.push(chars.next().unwrap_or_default());
                }
                tokens.push(DdlToken::Atom(value));
            }
        }
    }
    Ok(tokens)
}

fn token_is(token: &DdlToken, expected: &str) -> bool {
    matches!(token, DdlToken::Atom(value) if value.eq_ignore_ascii_case(expected))
}

fn token_quoted(token: &DdlToken) -> Option<&str> {
    match token {
        DdlToken::Quoted(value) => Some(value),
        _ => None,
    }
}

fn matching_rparen(tokens: &[DdlToken], open: usize) -> Result<usize> {
    let mut depth = 0_u32;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token {
            DdlToken::LParen => depth += 1,
            DdlToken::RParen => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| Error::new("SHOW CREATE TABLE has unmatched parentheses"))?;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err(Error::new("SHOW CREATE TABLE has unclosed parentheses"))
}

fn split_ddl_entries(tokens: &[DdlToken]) -> Result<Vec<&[DdlToken]>> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut depth = 0_u32;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            DdlToken::LParen => depth += 1,
            DdlToken::RParen => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| Error::new("SHOW CREATE TABLE has unmatched parentheses"))?;
            }
            DdlToken::Comma if depth == 0 => {
                if start == index {
                    return Err(Error::new("SHOW CREATE TABLE has an empty entry"));
                }
                entries.push(&tokens[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if start >= tokens.len() || depth != 0 {
        return Err(Error::new("SHOW CREATE TABLE has malformed entries"));
    }
    entries.push(&tokens[start..]);
    Ok(entries)
}

fn validate_ddl_column(
    entry: &[DdlToken],
    column: &crate::query_perf::case::ValidatedColumn,
) -> Result<()> {
    if entry.first().and_then(token_quoted) != Some(column.name()) {
        return Err(Error::new(
            "SHOW CREATE TABLE column name does not match case",
        ));
    }
    let expected: Vec<DdlToken> = match column.ty() {
        crate::query_perf::case::ValidatedColumnType::String => {
            vec![DdlToken::Atom("STRING".into())]
        }
        crate::query_perf::case::ValidatedColumnType::Float64 => {
            vec![DdlToken::Atom("DOUBLE".into())]
        }
        crate::query_perf::case::ValidatedColumnType::Uint64 => vec![
            DdlToken::Atom("BIGINT".into()),
            DdlToken::Atom("UNSIGNED".into()),
        ],
        crate::query_perf::case::ValidatedColumnType::TimestampNanosecond => vec![
            DdlToken::Atom("TIMESTAMP".into()),
            DdlToken::LParen,
            DdlToken::Atom("9".into()),
            DdlToken::RParen,
        ],
        crate::query_perf::case::ValidatedColumnType::TimestampMillisecond => vec![
            DdlToken::Atom("TIMESTAMP".into()),
            DdlToken::LParen,
            DdlToken::Atom("3".into()),
            DdlToken::RParen,
        ],
    };
    if entry.len() < expected.len() + 1
        || !entry[1..=expected.len()]
            .iter()
            .zip(&expected)
            .all(|(actual, expected)| match (actual, expected) {
                (DdlToken::Atom(actual), DdlToken::Atom(expected)) => {
                    actual.eq_ignore_ascii_case(expected)
                }
                _ => actual == expected,
            })
    {
        return Err(Error::new(
            "SHOW CREATE TABLE column type does not match case",
        ));
    }
    let modifiers = &entry[expected.len() + 1..];
    if !matches!(modifiers, [DdlToken::Atom(value)] if value.eq_ignore_ascii_case("NULL"))
        && !matches!(modifiers, [DdlToken::Atom(not), DdlToken::Atom(null)] if not.eq_ignore_ascii_case("NOT") && null.eq_ignore_ascii_case("NULL"))
    {
        return Err(Error::new(
            "SHOW CREATE TABLE column modifiers are unexpected",
        ));
    }
    Ok(())
}

fn validate_time_index_entry(entry: &[DdlToken], expected: &str) -> Result<()> {
    if entry.len() != 5
        || !token_is(&entry[0], "TIME")
        || !token_is(&entry[1], "INDEX")
        || entry[2] != DdlToken::LParen
        || entry[3] != DdlToken::Quoted(expected.into())
        || entry[4] != DdlToken::RParen
    {
        return Err(Error::new(
            "SHOW CREATE TABLE TIME INDEX does not match case",
        ));
    }
    Ok(())
}

fn validate_primary_key_entry(entry: &[DdlToken], expected: &[String]) -> Result<()> {
    if entry.len() != expected.len() * 2 + 3
        || !token_is(&entry[0], "PRIMARY")
        || !token_is(&entry[1], "KEY")
        || entry[2] != DdlToken::LParen
        || entry.last() != Some(&DdlToken::RParen)
    {
        return Err(Error::new(
            "SHOW CREATE TABLE PRIMARY KEY does not match case",
        ));
    }
    for (index, column) in expected.iter().enumerate() {
        let position = 3 + index * 2;
        if token_quoted(&entry[position]) != Some(column)
            || (index + 1 < expected.len() && entry.get(position + 1) != Some(&DdlToken::Comma))
        {
            return Err(Error::new(
                "SHOW CREATE TABLE PRIMARY KEY does not match case",
            ));
        }
    }
    Ok(())
}

fn ddl_options(tokens: &[DdlToken]) -> Result<std::collections::HashMap<String, String>> {
    if tokens.is_empty() {
        return Ok(Default::default());
    }
    if !token_is(&tokens[0], "WITH") || tokens.get(1) != Some(&DdlToken::LParen) {
        return Err(Error::new(
            "SHOW CREATE TABLE has unexpected content after ENGINE",
        ));
    }
    let close = matching_rparen(tokens, 1)?;
    if close + 1 != tokens.len() {
        return Err(Error::new(
            "SHOW CREATE TABLE has trailing content after options",
        ));
    }
    let mut options = std::collections::HashMap::new();
    for entry in split_ddl_entries(&tokens[2..close])? {
        if entry.len() != 3 || entry[1] != DdlToken::Equals {
            return Err(Error::new("SHOW CREATE TABLE option is malformed"));
        }
        let key = token_quoted(&entry[0]).or_else(|| match &entry[0] {
            DdlToken::Atom(value) => Some(value),
            _ => None,
        });
        let value = token_quoted(&entry[2]).or_else(|| match &entry[2] {
            DdlToken::Atom(value) => Some(value),
            _ => None,
        });
        let (Some(key), Some(value)) = (key, value) else {
            return Err(Error::new("SHOW CREATE TABLE option is malformed"));
        };
        if options
            .insert(key.to_ascii_lowercase(), value.to_ascii_lowercase())
            .is_some()
        {
            return Err(Error::new("SHOW CREATE TABLE has duplicate option"));
        }
    }
    Ok(options)
}

fn validate_external_case_bindings(
    case: &ValidatedCase,
    targets: &TargetEnvironmentManifest,
    fixture: &FixtureManifest,
) -> Result<()> {
    let direct = case
        .direct_readable_sst()
        .map_err(|_| Error::new("external_prepared requires a direct-SST case table contract"))?;
    let mut expected = Vec::new();
    for target in ordered_targets(targets)? {
        for table in direct.tables() {
            expected.push((
                target.role,
                target.target_id.as_str(),
                table.database(),
                table.name(),
            ));
        }
        let target_order: Vec<_> = target
            .fixture_bindings
            .iter()
            .map(|binding| (binding.database.as_str(), binding.logical_table.as_str()))
            .collect();
        let case_order: Vec<_> = direct
            .tables()
            .iter()
            .map(|table| (table.database(), table.name()))
            .collect();
        if target_order != case_order {
            return Err(Error::new(
                "external target fixture bindings must follow direct-SST case table order",
            ));
        }
    }
    let actual_order: Vec<_> = fixture
        .target_bindings
        .iter()
        .map(|binding| {
            (
                binding.role,
                binding.target_id.as_str(),
                binding.database.as_str(),
                binding.logical_table.as_str(),
            )
        })
        .collect();
    if actual_order != expected {
        return Err(Error::new(
            "external bindings must deterministically cover every target and direct-SST case table",
        ));
    }
    let mut present = std::collections::HashSet::new();
    for binding in &fixture.target_bindings {
        let external = binding
            .external
            .as_ref()
            .ok_or_else(|| Error::new("external fixture evidence missing"))?;
        let table = direct
            .tables()
            .iter()
            .find(|table| {
                table.database() == binding.database && table.name() == binding.logical_table
            })
            .ok_or_else(|| Error::new("external binding table is absent from case"))?;
        let column = table
            .columns()
            .iter()
            .find(|column| column.name() == external.time_index)
            .ok_or_else(|| Error::new("external binding time index is absent from case"))?;
        let unit = match column.ty() {
            crate::query_perf::case::ValidatedColumnType::TimestampNanosecond => {
                TimestampUnit::Nanosecond
            }
            crate::query_perf::case::ValidatedColumnType::TimestampMillisecond => {
                TimestampUnit::Millisecond
            }
            _ => {
                return Err(Error::new(
                    "external binding time index must have supported timestamp type",
                ));
            }
        };
        if column.semantic() != crate::query_perf::case::ValidatedSemanticType::Timestamp
            || external.time_unit != unit
            || table.time_index() != external.time_index
        {
            return Err(Error::new(
                "external binding time index does not match case",
            ));
        }
        let target = targets
            .targets
            .iter()
            .find(|target| target.target_id == binding.target_id)
            .ok_or_else(|| Error::new("external binding target is absent"))?;
        if target.topology != "single" || direct.layout().regions() != 1 {
            return Err(Error::new(
                "external_prepared requires the declared single target topology and one case region",
            ));
        }
        if !present.insert((
            binding.role,
            binding.target_id.as_str(),
            binding.database.as_str(),
            binding.logical_table.as_str(),
        )) {
            return Err(Error::new(
                "external bindings must be uniquely keyed by target and table",
            ));
        }
    }
    if present.len() != expected.len() {
        return Err(Error::new(
            "external bindings must cover every target and direct-SST case table",
        ));
    }
    Ok(())
}

fn data_rows(value: &serde_json::Value) -> Result<&Vec<serde_json::Value>> {
    valid_response_envelope(value)?;
    value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::new("SQL evidence requires data rows"))
}
fn exact_u64_field(value: &serde_json::Value, field: &str) -> Result<u64> {
    let row = exact_row(value, &[field])?;
    row.get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::new("SQL evidence field must be unsigned"))
}
fn exact_row<'a>(
    value: &'a serde_json::Value,
    fields: &[&str],
) -> Result<&'a serde_json::Map<String, serde_json::Value>> {
    let rows = data_rows(value)?;
    if rows.len() != 1 {
        return Err(Error::new("SQL evidence requires exactly one row"));
    }
    let row = rows[0]
        .as_object()
        .ok_or_else(|| Error::new("SQL evidence row must be an object"))?;
    if row.len() != fields.len() || fields.iter().any(|field| !row.contains_key(*field)) {
        return Err(Error::new(
            "SQL evidence fields do not exactly match contract",
        ));
    }
    Ok(row)
}
fn to_nanos(value: i64, unit: TimestampUnit) -> Result<i64> {
    match unit {
        TimestampUnit::Nanosecond => Ok(value),
        TimestampUnit::Millisecond => value
            .checked_mul(1_000_000)
            .ok_or_else(|| Error::new("timestamp conversion overflows nanoseconds")),
    }
}

fn evaluate_report(
    preflight: &Preflight,
    bundle: &MeasurementBundle,
    observations: &ObservationBundle,
) -> Result<ReportManifest> {
    let mut threshold_results = Vec::new();
    let mut outcomes = Vec::new();
    let targets = ordered_targets(&preflight.targets)?;
    let base = bundle
        .measurements
        .iter()
        .find(|value| value.target_id == targets[0].target_id)
        .ok_or_else(|| Error::new("base measurement missing"))?;
    let candidate = bundle
        .measurements
        .iter()
        .find(|value| value.target_id == targets[1].target_id)
        .ok_or_else(|| Error::new("candidate measurement missing"))?;
    for query in preflight.case.queries() {
        let base_measurement = base
            .queries
            .iter()
            .find(|value| value.identity.name == query.resolved_identity().name())
            .ok_or_else(|| Error::new("base query missing"))?;
        let candidate_measurement = candidate
            .queries
            .iter()
            .find(|value| value.identity.name == query.resolved_identity().name())
            .ok_or_else(|| Error::new("candidate query missing"))?;
        let base_median = median(
            &base_measurement
                .samples
                .iter()
                .map(|sample| sample.milliseconds)
                .collect::<Vec<_>>(),
        )?;
        let candidate_median = median(
            &candidate_measurement
                .samples
                .iter()
                .map(|sample| sample.milliseconds)
                .collect::<Vec<_>>(),
        )?;
        if base_median <= 0.0 {
            return Err(Error::new("base median latency must be positive"));
        }
        if let Some(limit) = query.max_candidate_latency_regression_pct() {
            let regression = (candidate_median - base_median) / base_median * 100.0;
            let passed = regression.is_finite() && regression <= limit;
            threshold_results.push(ThresholdResult { query_name: query.resolved_identity().name().to_string(), threshold: crate::query_perf::case::QueryThresholds { max_candidate_latency_regression_pct: Some(limit) }, passed, detail: format!("base={base_median:.6}ms candidate={candidate_median:.6}ms regression={regression:.6}%") });
            outcomes.push(ThresholdOutcome {
                key: ThresholdOutcomeKey::QueryLatency {
                    query_name: query.resolved_identity().name().to_string(),
                },
                passed,
                evidence: ThresholdEvidence::QueryLatency {
                    base_milliseconds: base_median,
                    candidate_milliseconds: candidate_median,
                    limit_pct: limit,
                    regression_pct: regression,
                },
            });
        }
    }
    if let Some(thresholds) = preflight.case.storage_thresholds() {
        let base_footer = footer_for(observations, &targets[0].target_id)?;
        let candidate_footer = footer_for(observations, &targets[1].target_id)?;
        for (target, footer) in [(targets[0], base_footer), (targets[1], candidate_footer)] {
            for (rule, actual, limit) in [
                (
                    FooterRule::MinFiles,
                    footer.file_count,
                    thresholds.min_files(),
                ),
                (
                    FooterRule::MinFilesWithColumn,
                    footer.files_with_column,
                    thresholds.min_files_with_column(),
                ),
            ] {
                push_footer_value(&mut outcomes, target, rule, actual, limit, actual >= limit);
            }
            for encoding in thresholds.require_encodings() {
                outcomes.push(ThresholdOutcome {
                    key: ThresholdOutcomeKey::FooterTarget {
                        rule: FooterRule::RequiredEncoding,
                        role: target.role,
                        target_id: target.target_id.clone(),
                        encoding: Some(encoding.clone()),
                    },
                    passed: footer.encodings.contains(encoding),
                    evidence: ThresholdEvidence::FooterEncoding {
                        encoding_present: footer.encodings.contains(encoding),
                    },
                });
            }
            for encoding in thresholds.forbid_encodings() {
                outcomes.push(ThresholdOutcome {
                    key: ThresholdOutcomeKey::FooterTarget {
                        rule: FooterRule::ForbiddenEncoding,
                        role: target.role,
                        target_id: target.target_id.clone(),
                        encoding: Some(encoding.clone()),
                    },
                    passed: !footer.encodings.contains(encoding),
                    evidence: ThresholdEvidence::FooterEncoding {
                        encoding_present: footer.encodings.contains(encoding),
                    },
                });
            }
            for (rule, limit, actual) in [
                (
                    FooterRule::MaxTotalFileSizeBytes,
                    thresholds.max_total_file_size_bytes(),
                    footer.total_file_size_bytes,
                ),
                (
                    FooterRule::MaxColumnCompressedSizeBytes,
                    thresholds.max_column_compressed_size_bytes(),
                    footer.column_compressed_size_bytes,
                ),
                (
                    FooterRule::MaxColumnUncompressedSizeBytes,
                    thresholds.max_column_uncompressed_size_bytes(),
                    footer.column_uncompressed_size_bytes,
                ),
            ] {
                if let Some(limit) = limit {
                    push_footer_value(&mut outcomes, target, rule, actual, limit, actual <= limit);
                }
            }
        }
        for (rule, limit, base_value, candidate_value) in [
            (
                FooterRule::MaxCandidateTotalFileSizeRegressionPct,
                thresholds.max_candidate_total_file_size_regression_pct(),
                base_footer.total_file_size_bytes,
                candidate_footer.total_file_size_bytes,
            ),
            (
                FooterRule::MaxCandidateColumnCompressedSizeRegressionPct,
                thresholds.max_candidate_column_compressed_size_regression_pct(),
                base_footer.column_compressed_size_bytes,
                candidate_footer.column_compressed_size_bytes,
            ),
            (
                FooterRule::MaxCandidateColumnUncompressedSizeRegressionPct,
                thresholds.max_candidate_column_uncompressed_size_regression_pct(),
                base_footer.column_uncompressed_size_bytes,
                candidate_footer.column_uncompressed_size_bytes,
            ),
        ] {
            if let Some(limit) = limit {
                let regression = regression_pct(base_value, candidate_value, limit)?;
                outcomes.push(ThresholdOutcome {
                    key: ThresholdOutcomeKey::FooterComparison {
                        rule,
                        base_target_id: targets[0].target_id.clone(),
                        candidate_target_id: targets[1].target_id.clone(),
                    },
                    passed: regression <= limit,
                    evidence: ThresholdEvidence::FooterComparison {
                        base_actual: base_value,
                        candidate_actual: candidate_value,
                        limit_pct: limit,
                        regression_pct: regression,
                    },
                });
            }
        }
    }
    let failures: Vec<_> = outcomes
        .iter()
        .filter(|outcome| !outcome.passed)
        .map(|outcome| format!("failed threshold {:?}", outcome.key))
        .collect();
    let failure_details: Vec<_> = failures
        .iter()
        .map(|detail| ReportFailure {
            stage: ReportFailureStage::Threshold,
            detail: detail.clone(),
        })
        .collect();
    let mut report = ReportManifest {
        version: MANIFEST_VERSION,
        run_id: preflight.targets.run_id.clone(),
        report_id: format!("report-{}", preflight.fixture.fixture_id),
        target_ids: ordered_targets(&preflight.targets)?
            .iter()
            .map(|target| target.target_id.clone())
            .collect(),
        fixture_id: preflight.fixture.fixture_id.clone(),
        queries: preflight.queries.clone(),
        threshold_results,
        threshold_outcomes: outcomes,
        evaluation_state: EvaluationState::Complete,
        outcome: if failures.is_empty() {
            ReportOutcome::Success
        } else {
            ReportOutcome::Failed
        },
        failures,
        failure_details,
        measurements: bundle
            .measurements
            .iter()
            .map(|value| ArtifactReference {
                id: value.measurement_id.clone(),
                digest: value.completion_digest.clone(),
            })
            .collect(),
        observations: observations
            .observations
            .iter()
            .map(|value| ArtifactReference {
                id: value.observation_id.clone(),
                digest: value.completion_digest.clone(),
            })
            .collect(),
        measurement_bundle: Some(ArtifactReference {
            id: "measurement_bundle".to_string(),
            digest: bundle.completion_digest.clone(),
        }),
        observation_bundle: Some(ArtifactReference {
            id: "observation_bundle".to_string(),
            digest: observations.completion_digest.clone(),
        }),
        target_manifest_digest: preflight.targets.completion_digest.clone(),
        fixture_manifest_digest: preflight.fixture.completion_digest.clone(),
        provenance: preflight.case_digest.clone(),
        summary_digest: Sha256Digest::compute([]),
        completion_digest: Sha256Digest::compute("unsealed"),
    };
    report.seal()?;
    report.validate_against(
        &preflight.queries,
        &preflight.targets,
        &preflight.fixture,
        &bundle.measurements,
        &observations.observations,
    )?;
    report.validate_threshold_coverage(&preflight.case, &preflight.targets)?;
    Ok(report)
}

fn valid_response_envelope(body: &serde_json::Value) -> Result<()> {
    let object = body
        .as_object()
        .ok_or_else(|| Error::new("SQL response must be an object"))?;
    for key in ["error", "err_msg", "error_msg"] {
        if let Some(value) = object.get(key)
            && !value.is_null()
            && value.as_str().is_none_or(|message| !message.is_empty())
        {
            return Err(Error::new("SQL response contains a top-level error"));
        }
    }
    if let Some(value) = unique_alias(object, &["code", "error_code", "errorCode"], "error code")?
        && value.as_u64() != Some(0)
    {
        return Err(Error::new("SQL response contains a non-success code"));
    }
    let data = object.get("data");
    if data.is_some_and(|value| !value.is_array()) {
        return Err(Error::new("SQL response data must be an array"));
    }
    let affected_rows = unique_alias(
        object,
        &["affected_rows", "affectedRows", "AffectedRows"],
        "affected_rows",
    )?;
    if affected_rows.is_some_and(|value| value.as_u64().is_none()) {
        return Err(Error::new("SQL response affected_rows must be unsigned"));
    }
    if data.is_none() && affected_rows.is_none() {
        return Err(Error::new(
            "SQL response must contain data array or unsigned affected_rows",
        ));
    }
    if let Some(value) = unique_alias(
        object,
        &[
            "execution_time_ms",
            "execution_time",
            "elapsed_ms",
            "executionTimeMs",
            "executionTime",
            "elapsedMs",
        ],
        "execution time",
    )? && value
        .as_f64()
        .is_none_or(|value| !value.is_finite() || value < 0.0)
    {
        return Err(Error::new("SQL response execution time is invalid"));
    }
    Ok(())
}

fn unique_alias<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<&'a serde_json::Value>> {
    let mut present = aliases.iter().filter_map(|alias| object.get(*alias));
    let value = present.next();
    if present.next().is_some() {
        return Err(Error::new(format!(
            "SQL response contains conflicting {field} aliases"
        )));
    }
    Ok(value)
}

fn extract_single_u64(body: &serde_json::Value) -> Result<u64> {
    valid_response_envelope(body)?;
    let rows = body
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::new("count response must contain data rows"))?;
    if rows.len() != 1 {
        return Err(Error::new("count response must contain exactly one row"));
    }
    let row = rows[0]
        .as_object()
        .ok_or_else(|| Error::new("count response row must be an object"))?;
    if row.len() != 1 {
        return Err(Error::new("count response row must contain exactly count"));
    }
    row.get("count")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::new("count response field count must be unsigned"))
}
fn median(values: &[f64]) -> Result<f64> {
    if values.is_empty()
        || values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(Error::new("latency samples must be finite and non-empty"));
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Ok(if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}
fn footer_for<'a>(
    observations: &'a ObservationBundle,
    target_id: &str,
) -> Result<&'a crate::query_perf::manifest::FooterObservation> {
    observations
        .observations
        .iter()
        .find_map(|observation| {
            (observation.target_id == target_id).then(|| match &observation.payload {
                ObservationPayload::Footer(value) => Some(value),
                _ => None,
            })?
        })
        .ok_or_else(|| Error::new("footer observation is missing"))
}
fn regression_pct(base: u64, candidate: u64, limit: f64) -> Result<f64> {
    if base == 0 || !limit.is_finite() || limit < 0.0 {
        return Err(Error::new(
            "storage regression requires a positive base and finite limit",
        ));
    }
    let regression = (candidate as f64 - base as f64) / base as f64 * 100.0;
    if !regression.is_finite() {
        return Err(Error::new("storage regression is not finite"));
    }
    Ok(regression)
}
fn push_footer_value(
    outcomes: &mut Vec<ThresholdOutcome>,
    target: &TargetBinding,
    rule: FooterRule,
    actual: u64,
    limit: u64,
    passed: bool,
) {
    outcomes.push(ThresholdOutcome {
        key: ThresholdOutcomeKey::FooterTarget {
            rule,
            role: target.role,
            target_id: target.target_id.clone(),
            encoding: None,
        },
        passed,
        evidence: ThresholdEvidence::FooterValue { actual, limit },
    });
}
fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn canonical_json_bytes(value: &serde_json::Value) -> Result<Vec<u8>> {
    fn canonical(value: &serde_json::Value) -> Result<serde_json::Value> {
        match value {
            serde_json::Value::Array(values) => Ok(serde_json::Value::Array(
                values.iter().map(canonical).collect::<Result<_>>()?,
            )),
            serde_json::Value::Object(values) => {
                let mut ordered = BTreeMap::new();
                for (key, value) in values {
                    ordered.insert(key, canonical(value)?);
                }
                serde_json::to_value(ordered)
                    .map_err(|err| Error::new(format!("failed to canonicalize JSON: {err}")))
            }
            value => Ok(value.clone()),
        }
    }
    serde_json::to_vec(&canonical(value)?)
        .map_err(|err| Error::new(format!("failed to serialize canonical JSON: {err}")))
}
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    serde_json::from_slice(
        &fs::read(path)
            .map_err(|err| Error::new(format!("failed to read {}: {err}", path.display())))?,
    )
    .map_err(|err| Error::new(format!("failed to parse {}: {err}", path.display())))
}
fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    write_atomic(
        path,
        &serde_json::to_vec_pretty(value).map_err(|err| Error::new(err.to_string()))?,
    )
}
fn write_pair_atomic(
    report: &Path,
    value: &impl Serialize,
    summary: &Path,
    markdown: &[u8],
) -> Result<()> {
    write_pair_with_fs(&StdPublisherFs, report, value, summary, markdown)
}

#[derive(Debug)]
struct StagedPath {
    destination: PathBuf,
    temporary: PathBuf,
}

/// File-system operations used only by the paired report publisher.
trait PublisherFs {
    fn stage(&self, destination: &Path, bytes: &[u8]) -> Result<StagedPath>;
    fn publish(&self, staged: &StagedPath, destination: &Path) -> Result<()>;
    fn sync_parent(&self, path: &Path) -> Result<()>;
    fn remove(&self, staged: &StagedPath) -> Result<()>;
}

struct StdPublisherFs;

impl PublisherFs for StdPublisherFs {
    fn stage(&self, destination: &Path, bytes: &[u8]) -> Result<StagedPath> {
        let parent = destination
            .parent()
            .ok_or_else(|| Error::new("output path has no parent"))?;
        fs::create_dir_all(parent)
            .map_err(|err| Error::new(format!("failed to create output directory: {err}")))?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| Error::new(err.to_string()))?
            .as_nanos();
        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            destination
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| Error::new("output filename is invalid"))?,
            nonce
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|err| Error::new(format!("failed to create temporary output: {err}")))?;
        if let Err(err) = file.write_all(bytes).and_then(|_| file.sync_all()) {
            let _ = fs::remove_file(&temporary);
            return Err(Error::new(format!(
                "failed to write temporary output: {err}"
            )));
        }
        Ok(StagedPath {
            destination: destination.to_path_buf(),
            temporary,
        })
    }

    fn publish(&self, staged: &StagedPath, destination: &Path) -> Result<()> {
        if staged.destination != destination {
            return Err(Error::new("staged output destination mismatch"));
        }
        fs::rename(&staged.temporary, destination).map_err(|err| Error::new(err.to_string()))
    }

    fn sync_parent(&self, path: &Path) -> Result<()> {
        sync_parent(path)
    }

    fn remove(&self, staged: &StagedPath) -> Result<()> {
        fs::remove_file(&staged.temporary).map_err(|err| Error::new(err.to_string()))
    }
}

fn write_pair_with_fs(
    publisher_fs: &dyn PublisherFs,
    report: &Path,
    value: &impl Serialize,
    summary: &Path,
    markdown: &[u8],
) -> Result<()> {
    if report == summary
        || report.file_name() == summary.file_name() && report.parent() == summary.parent()
    {
        return Err(Error::new("report and summary destinations must differ"));
    }
    let report_bytes =
        serde_json::to_vec_pretty(value).map_err(|err| Error::new(err.to_string()))?;
    let summary_staged = publisher_fs
        .stage(summary, markdown)
        .map_err(|err| Error::new(format!("failed to stage summary: {err}")))?;
    let report_staged = match publisher_fs.stage(report, &report_bytes) {
        Ok(value) => value,
        Err(err) => {
            let _ = publisher_fs.remove(&summary_staged);
            return Err(Error::new(format!("failed to stage report: {err}")));
        }
    };
    if let Err(err) = publisher_fs.publish(&summary_staged, summary) {
        let _ = publisher_fs.remove(&summary_staged);
        let _ = publisher_fs.remove(&report_staged);
        return Err(Error::new(format!("failed to publish summary: {err}")));
    }
    if let Err(err) = publisher_fs.sync_parent(summary) {
        let _ = publisher_fs.remove(&report_staged);
        return Err(Error::new(format!("failed to sync summary parent: {err}")));
    }
    if let Err(err) = publisher_fs.publish(&report_staged, report) {
        let _ = publisher_fs.remove(&report_staged);
        return Err(Error::new(format!("failed to publish report: {err}")));
    }
    publisher_fs
        .sync_parent(report)
        .map_err(|err| Error::new(format!("failed to sync report parent: {err}")))
}
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = stage_atomic(path, bytes)?;
    fs::rename(&temporary, path)
        .map_err(|err| Error::new(format!("failed to replace output: {err}")))?;
    sync_parent(path)
}
fn stage_atomic(path: &Path, bytes: &[u8]) -> Result<PathBuf> {
    StdPublisherFs
        .stage(path, bytes)
        .map(|staged| staged.temporary)
}
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new("output path has no parent"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| Error::new(format!("failed to sync output directory: {err}")))
}
fn markdown_summary(report: &ReportManifest) -> String {
    let status = match report.outcome {
        ReportOutcome::Success => "success",
        ReportOutcome::Failed => "failed",
    };
    format!(
        "# Query regression report\n\nOutcome: **{status}**\n\nFailures: {}\n",
        report.failures.len()
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;

    const REMOTE_CASE: &str = r#"
[scenario]
kind = "prom_remote_write_then_query"
[scenario.remote_write]
metric = "m"
[scenario.remote_write.storage]
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "select 1"
iterations = 1
"#;

    const FINALIZE_QUERY_CASE: &str = r#"
[scenario]
kind = "prom_remote_write_then_query"
[scenario.remote_write]
metric = "m"
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "SELECT 1"
iterations = 1
[scenario.queries.thresholds]
max_candidate_latency_regression_pct = 10
"#;

    const FINALIZE_FOOTER_CASE: &str = r#"
[scenario]
kind = "prom_remote_write_then_query"
[scenario.remote_write]
metric = "m"
[scenario.remote_write.storage]
max_candidate_total_file_size_regression_pct = 10
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "SELECT 1"
iterations = 1
[scenario.queries.thresholds]
max_candidate_latency_regression_pct = 10
"#;

    const DIRECT_CASE: &str = r#"
[scenario]
kind = "direct_readable_sst"
[[scenario.tables]]
database = "public"
name = "t"
engine = "mito"
primary_key = ["host"]
time_index = "ts"
[[scenario.tables.columns]]
name = "host"
type = "STRING"
semantic = "tag"
distribution = { kind = "cardinality", values = 1, prefix = "host" }
[[scenario.tables.columns]]
name = "value"
type = "DOUBLE"
semantic = "field"
[[scenario.tables.columns]]
name = "ts"
type = "TIMESTAMP(9)"
semantic = "timestamp"
[scenario.layout]
regions = 1
sst_count = 1
rows_per_sst = 1
row_group_size = 1
series_count = 1
start_unix_nanos = 0
step_nanos = 1
time_range_layout = "non_overlapping_per_sst"
series_layout = "round_robin"
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "select 1"
iterations = 1
"#;

    const EXTERNAL_MEASURE_CASE: &str = r#"
[scenario]
kind = "direct_readable_sst"
[[scenario.tables]]
database = "public"
name = "t"
engine = "mito"
primary_key = ["host"]
time_index = "ts"
[[scenario.tables.columns]]
name = "host"
type = "STRING"
semantic = "tag"
distribution = { kind = "cardinality", values = 1, prefix = "host" }
[[scenario.tables.columns]]
name = "value"
type = "DOUBLE"
semantic = "field"
[[scenario.tables.columns]]
name = "ts"
type = "TIMESTAMP(9)"
semantic = "timestamp"
[scenario.layout]
regions = 1
sst_count = 1
rows_per_sst = 1
row_group_size = 1
series_count = 1
start_unix_nanos = 0
step_nanos = 1
time_range_layout = "non_overlapping_per_sst"
series_layout = "round_robin"
[[scenario.queries]]
name = "sql_query"
kind = "sql"
database = "public"
query = "SELECT * FROM t WHERE host = 'host-0'"
warmup = 1
iterations = 2
[[scenario.queries]]
name = "tql_query"
kind = "tql"
database = "public"
query = "TQL EVAL (0, 1, '1s') t"
warmup = 2
iterations = 1
"#;

    struct MockEndpointClient {
        calls: Mutex<Vec<String>>,
        responses: Mutex<VecDeque<Result<serde_json::Value>>>,
    }

    #[derive(Debug)]
    struct CapturedHttpRequest {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl CapturedHttpRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    struct ScriptedHttpResponse {
        status: u16,
        body: String,
        delay: Duration,
        headers: Vec<(String, String)>,
    }

    impl ScriptedHttpResponse {
        fn json(status: u16, body: impl Into<String>) -> Self {
            Self {
                status,
                body: body.into(),
                delay: Duration::ZERO,
                headers: vec![],
            }
        }

        fn delayed(status: u16, body: impl Into<String>, delay: Duration) -> Self {
            Self {
                status,
                body: body.into(),
                delay,
                headers: vec![],
            }
        }

        fn redirect(location: impl Into<String>) -> Self {
            Self {
                status: 302,
                body: String::new(),
                delay: Duration::ZERO,
                headers: vec![("Location".into(), location.into())],
            }
        }
    }

    struct LocalHttpServer {
        address: std::net::SocketAddr,
        requests: Arc<Mutex<Vec<CapturedHttpRequest>>>,
        task: JoinHandle<()>,
    }

    impl LocalHttpServer {
        async fn start(responses: Vec<ScriptedHttpResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap_or_else(|err| panic!("bind local HTTP server: {err}"));
            let address = listener
                .local_addr()
                .unwrap_or_else(|err| panic!("local HTTP server address: {err}"));
            let requests = Arc::new(Mutex::new(vec![]));
            let captured_requests = Arc::clone(&requests);
            let task = tokio::spawn(async move {
                for response in responses {
                    let (mut stream, _) = listener
                        .accept()
                        .await
                        .unwrap_or_else(|err| panic!("accept local HTTP request: {err}"));
                    let request = read_http_request(&mut stream).await;
                    captured_requests
                        .lock()
                        .unwrap_or_else(|_| panic!("captured HTTP requests"))
                        .push(request);
                    tokio::time::sleep(response.delay).await;
                    let headers = response
                        .headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .collect::<String>();
                    let wire = format!(
                        "HTTP/1.1 {} scripted\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.status,
                        response.body.len(),
                        response.body
                    );
                    let _ = stream.write_all(wire.as_bytes()).await;
                }
            });
            Self {
                address,
                requests,
                task,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.address)
        }

        async fn captured_requests(self) -> Vec<CapturedHttpRequest> {
            self.task
                .await
                .unwrap_or_else(|err| panic!("local HTTP server: {err}"));
            self.requests
                .lock()
                .unwrap_or_else(|_| panic!("captured HTTP requests"))
                .drain(..)
                .collect()
        }
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> CapturedHttpRequest {
        let mut bytes = Vec::new();
        let header_end = loop {
            if let Some(offset) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break offset + 4;
            }
            assert!(
                bytes.len() < 64 * 1024,
                "HTTP request headers are too large"
            );
            let mut buffer = [0; 1024];
            let read = stream
                .read(&mut buffer)
                .await
                .unwrap_or_else(|err| panic!("read HTTP request: {err}"));
            assert_ne!(read, 0, "HTTP client closed before sending headers");
            bytes.extend_from_slice(&buffer[..read]);
        };
        let headers = String::from_utf8(bytes[..header_end].to_vec())
            .unwrap_or_else(|err| panic!("HTTP request headers are UTF-8: {err}"));
        let mut lines = headers.split("\r\n");
        let request_line = lines.next().unwrap_or_else(|| panic!("HTTP request line"));
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .unwrap_or_else(|| panic!("HTTP request method"))
            .to_string();
        let path = request_parts
            .next()
            .unwrap_or_else(|| panic!("HTTP request path"))
            .to_string();
        let headers: Vec<_> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_string(), value.trim().to_string()))
            .collect();
        let content_length = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map(|(_, value)| {
                value
                    .parse::<usize>()
                    .unwrap_or_else(|err| panic!("HTTP content length: {err}"))
            })
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let mut buffer = [0; 1024];
            let read = stream
                .read(&mut buffer)
                .await
                .unwrap_or_else(|err| panic!("read HTTP request body: {err}"));
            assert_ne!(read, 0, "HTTP client closed before sending body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        CapturedHttpRequest {
            method,
            path,
            headers,
            body: bytes[header_end..header_end + content_length].to_vec(),
        }
    }

    impl MockEndpointClient {
        fn new(responses: impl IntoIterator<Item = Result<serde_json::Value>>) -> Self {
            Self {
                calls: Mutex::new(vec![]),
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }

        fn remaining_responses(&self) -> usize {
            self.responses
                .lock()
                .unwrap_or_else(|_| panic!("responses"))
                .len()
        }
    }

    struct RecordingPublisherFs {
        fail_stage_for: Option<String>,
        fail_publish_at: Option<usize>,
        fail_sync_at: Option<usize>,
        publish_count: Mutex<usize>,
        sync_count: Mutex<usize>,
        operations: Mutex<Vec<String>>,
    }

    impl RecordingPublisherFs {
        fn new(
            fail_stage_for: Option<&str>,
            fail_publish_at: Option<usize>,
            fail_sync_at: Option<usize>,
        ) -> Self {
            Self {
                fail_stage_for: fail_stage_for.map(str::to_string),
                fail_publish_at,
                fail_sync_at,
                publish_count: Mutex::new(0),
                sync_count: Mutex::new(0),
                operations: Mutex::new(vec![]),
            }
        }

        fn record(&self, operation: &str, path: &Path) {
            self.operations
                .lock()
                .unwrap_or_else(|_| panic!("publisher operations"))
                .push(format!(
                    "{operation}:{}",
                    path.file_name().unwrap_or_default().to_string_lossy()
                ));
        }

        fn operations(&self) -> Vec<String> {
            self.operations
                .lock()
                .unwrap_or_else(|_| panic!("publisher operations"))
                .clone()
        }
    }

    impl PublisherFs for RecordingPublisherFs {
        fn stage(&self, destination: &Path, bytes: &[u8]) -> Result<StagedPath> {
            self.record("stage", destination);
            if self.fail_stage_for.as_deref()
                == destination.file_name().and_then(|name| name.to_str())
            {
                return Err(Error::new("injected staging failure"));
            }
            StdPublisherFs.stage(destination, bytes)
        }

        fn publish(&self, staged: &StagedPath, destination: &Path) -> Result<()> {
            self.record("publish", destination);
            let mut count = self
                .publish_count
                .lock()
                .unwrap_or_else(|_| panic!("publish count"));
            *count += 1;
            if self.fail_publish_at == Some(*count) {
                return Err(Error::new("injected publish failure"));
            }
            StdPublisherFs.publish(staged, destination)
        }

        fn sync_parent(&self, path: &Path) -> Result<()> {
            self.record("sync_parent", path);
            let mut count = self
                .sync_count
                .lock()
                .unwrap_or_else(|_| panic!("sync count"));
            *count += 1;
            if self.fail_sync_at == Some(*count) {
                return Err(Error::new("injected parent sync failure"));
            }
            StdPublisherFs.sync_parent(path)
        }

        fn remove(&self, staged: &StagedPath) -> Result<()> {
            self.record("remove", &staged.destination);
            StdPublisherFs.remove(staged)
        }
    }

    #[async_trait]
    impl EndpointClient for MockEndpointClient {
        async fn health(&self, target: &TargetBinding) -> Result<()> {
            self.calls
                .lock()
                .unwrap_or_else(|_| panic!("calls"))
                .push(format!("health:{}", target.target_id));
            Ok(())
        }
        async fn statement(
            &self,
            target: &TargetBinding,
            sql: &str,
            database: &str,
        ) -> Result<serde_json::Value> {
            self.calls
                .lock()
                .unwrap_or_else(|_| panic!("calls"))
                .push(format!("statement:{}:{database}:{sql}", target.target_id));
            self.responses
                .lock()
                .unwrap_or_else(|_| panic!("responses"))
                .pop_front()
                .unwrap_or_else(|| Err(Error::new("unexpected statement")))
        }
    }

    fn test_case(contents: &str) -> ValidatedCase {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = directory.path().join("case.toml");
        fs::write(&path, contents).unwrap_or_else(|err| panic!("write case: {err}"));
        case::load_case(&path).unwrap_or_else(|err| panic!("load case: {err}"))
    }

    fn test_target(
        role: TargetRole,
        target_id: &str,
        table: &str,
        capabilities: Vec<Capability>,
    ) -> TargetBinding {
        TargetBinding {
            role,
            target_id: target_id.to_string(),
            identity: Sha256Digest::compute(format!("identity-{target_id}")),
            git_identity: Sha256Digest::compute(format!("git-{target_id}")),
            build_profile: crate::query_perf::manifest::BuildProfile::Release,
            features: vec![],
            remote_write_endpoint: capabilities.contains(&Capability::RemoteWrite).then(|| {
                crate::query_perf::manifest::EndpointUrl::parse(format!(
                    "http://example.com/{target_id}/write"
                ))
                .unwrap_or_else(|err| panic!("remote-write endpoint: {err}"))
            }),
            capabilities,
            health_endpoint: crate::query_perf::manifest::EndpointUrl::parse(format!(
                "http://example.com/{target_id}/health"
            ))
            .unwrap_or_else(|err| panic!("health endpoint: {err}")),
            sql_endpoint: crate::query_perf::manifest::EndpointUrl::parse(format!(
                "http://example.com/{target_id}/sql"
            ))
            .unwrap_or_else(|err| panic!("SQL endpoint: {err}")),
            namespace: "public".to_string(),
            topology: "single".to_string(),
            fixture_bindings: vec![crate::query_perf::manifest::TargetFixtureBinding {
                fixture_id: "fixture".to_string(),
                catalog: Some("greptime".to_string()),
                database: "public".to_string(),
                logical_table: table.to_string(),
                physical_table: None,
                region: "0".to_string(),
            }],
        }
    }

    fn endpoint_target(
        server: &LocalHttpServer,
        health_path: &str,
        sql_path: &str,
    ) -> TargetBinding {
        let mut target = test_target(TargetRole::Base, "endpoint", "t", vec![Capability::Sql]);
        target.health_endpoint =
            crate::query_perf::manifest::EndpointUrl::parse(server.url(health_path))
                .unwrap_or_else(|err| panic!("health endpoint: {err}"));
        target.sql_endpoint = crate::query_perf::manifest::EndpointUrl::parse(server.url(sql_path))
            .unwrap_or_else(|err| panic!("SQL endpoint: {err}"));
        target
    }

    #[tokio::test]
    async fn reqwest_endpoint_health_uses_declared_path_and_requires_200() {
        for (status, succeeds) in [(200, true), (204, false), (400, false), (503, false)] {
            let server = LocalHttpServer::start(vec![ScriptedHttpResponse::json(status, "")]).await;
            let target = endpoint_target(&server, "/declared/health/path", "/declared/sql/path");
            let result = ReqwestEndpointClient::new()
                .unwrap_or_else(|err| panic!("endpoint client: {err}"))
                .health(&target)
                .await;
            assert_eq!(result.is_ok(), succeeds, "health status {status}");
            let requests = server.captured_requests().await;
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, "GET");
            assert_eq!(requests[0].path, "/declared/health/path");
            assert!(requests[0].body.is_empty());
        }
    }

    #[tokio::test]
    async fn reqwest_endpoint_redirects_are_not_followed_for_health_sql_or_tql() {
        let location = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|err| panic!("bind redirect location: {err}"));
        let location_url = format!(
            "http://{}/must-not-be-requested",
            location
                .local_addr()
                .unwrap_or_else(|err| panic!("redirect location address: {err}"))
        );
        let server = LocalHttpServer::start(vec![
            ScriptedHttpResponse::redirect(&location_url),
            ScriptedHttpResponse::redirect(&location_url),
            ScriptedHttpResponse::redirect(&location_url),
        ])
        .await;
        let target = endpoint_target(&server, "/declared/health", "/declared/sql");
        let client = ReqwestEndpointClient::new().unwrap_or_else(|err| panic!("client: {err}"));

        assert!(client.health(&target).await.is_err());
        assert!(
            client
                .statement(&target, "SELECT 1", "public")
                .await
                .is_err()
        );
        assert!(
            client
                .statement(&target, "TQL EVAL (0, 1, '1s') t", "public")
                .await
                .is_err()
        );
        let requests = server.captured_requests().await;
        assert_eq!(
            requests
                .iter()
                .map(|request| request.path.as_str())
                .collect::<Vec<_>>(),
            ["/declared/health", "/declared/sql", "/declared/sql"]
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), location.accept())
                .await
                .is_err(),
            "redirect Location endpoint received a request"
        );
    }

    #[tokio::test]
    async fn reqwest_endpoint_statement_preserves_sql_and_tql_form_requests() {
        let server = LocalHttpServer::start(vec![
            ScriptedHttpResponse::json(200, r#"{"code":0,"data":[],"execution_time_ms":0.1}"#),
            ScriptedHttpResponse::json(200, r#"{"affected_rows":7}"#),
        ])
        .await;
        let target = endpoint_target(&server, "/declared/health/path", "/declared/sql/path");
        let client =
            ReqwestEndpointClient::new().unwrap_or_else(|err| panic!("endpoint client: {err}"));
        let sql = "SELECT * FROM metric WHERE host = 'a b'";
        let tql = "TQL EVAL (0, 1, '1s') cpu";

        assert!(client.statement(&target, sql, "db sql").await.is_ok());
        assert!(client.statement(&target, tql, "db tql").await.is_ok());

        let requests = server.captured_requests().await;
        assert_eq!(requests.len(), 2);
        for (request, (expected_sql, expected_database)) in
            requests.iter().zip([(sql, "db sql"), (tql, "db tql")])
        {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, "/declared/sql/path");
            assert!(
                request
                    .header("content-type")
                    .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"))
            );
            assert_eq!(
                url::form_urlencoded::parse(&request.body).collect::<Vec<_>>(),
                vec![
                    ("sql".into(), expected_sql.into()),
                    ("db".into(), expected_database.into()),
                    ("format".into(), "json".into()),
                ]
            );
        }
    }

    #[tokio::test]
    async fn reqwest_endpoint_accepts_data_and_unsigned_affected_rows_envelopes() {
        let server = LocalHttpServer::start(vec![
            ScriptedHttpResponse::json(200, r#"{"code":0,"data":[{"value":1}]}"#),
            ScriptedHttpResponse::json(200, r#"{"affected_rows":0}"#),
        ])
        .await;
        let target = endpoint_target(&server, "/health", "/sql");
        let client =
            ReqwestEndpointClient::new().unwrap_or_else(|err| panic!("endpoint client: {err}"));

        assert_eq!(
            client
                .statement(&target, "SELECT 1", "public")
                .await
                .unwrap_or_else(|err| panic!("data envelope: {err}")),
            serde_json::json!({"code":0,"data":[{"value":1}]})
        );
        assert_eq!(
            client
                .statement(&target, "DELETE FROM t", "public")
                .await
                .unwrap_or_else(|err| panic!("affected rows envelope: {err}")),
            serde_json::json!({"affected_rows":0})
        );
        assert_eq!(server.captured_requests().await.len(), 2);
    }

    #[tokio::test]
    async fn reqwest_endpoint_rejects_invalid_response_envelopes() {
        let server = LocalHttpServer::start(vec![
            ScriptedHttpResponse::json(503, "{}"),
            ScriptedHttpResponse::json(200, "not JSON"),
            ScriptedHttpResponse::json(200, r#"{"data":[],"error":"failed"}"#),
            ScriptedHttpResponse::json(200, r#"{"data":[],"code":1}"#),
            ScriptedHttpResponse::json(200, r#"{"data":{}}"#),
            ScriptedHttpResponse::json(200, r#"{"data":[],"execution_time_ms":-1}"#),
            ScriptedHttpResponse::json(200, r#"{"data":[],"execution_time_ms":"NaN"}"#),
        ])
        .await;
        let target = endpoint_target(&server, "/health", "/sql");
        let client =
            ReqwestEndpointClient::new().unwrap_or_else(|err| panic!("endpoint client: {err}"));

        for statement in [
            "status",
            "malformed JSON",
            "top-level error",
            "non-success code",
            "malformed data",
            "negative execution time",
            "nonfinite execution time",
        ] {
            assert!(
                client
                    .statement(&target, statement, "public")
                    .await
                    .is_err(),
                "{statement} response must fail"
            );
        }
        assert_eq!(server.captured_requests().await.len(), 7);
    }

    #[tokio::test]
    async fn reqwest_endpoint_rejects_mixed_and_conflicting_success_envelopes() {
        let server = LocalHttpServer::start(vec![
            ScriptedHttpResponse::json(200, r#"{"data":{},"affected_rows":1}"#),
            ScriptedHttpResponse::json(200, r#"{"affected_rows":1,"affectedRows":1}"#),
            ScriptedHttpResponse::json(200, r#"{"data":[],"execution_time_ms":"NaN"}"#),
            ScriptedHttpResponse::json(200, r#"{"data":[],"execution_time_ms":1,"elapsed_ms":1}"#),
        ])
        .await;
        let target = endpoint_target(&server, "/health", "/sql");
        let client = ReqwestEndpointClient::new().unwrap_or_else(|err| panic!("client: {err}"));
        for statement in [
            "mixed",
            "affected aliases",
            "invalid timing",
            "timing aliases",
        ] {
            assert!(
                client
                    .statement(&target, statement, "public")
                    .await
                    .is_err(),
                "{statement} must fail closed"
            );
        }
        assert_eq!(server.captured_requests().await.len(), 4);
    }

    #[tokio::test]
    async fn reqwest_endpoint_request_timeout_is_bounded() {
        let server = LocalHttpServer::start(vec![ScriptedHttpResponse::delayed(
            200,
            "",
            Duration::from_millis(200),
        )])
        .await;
        let target = endpoint_target(&server, "/slow/non-default/health", "/sql");
        let client = ReqwestEndpointClient::with_timeouts(
            Duration::from_millis(20),
            Duration::from_millis(40),
        )
        .unwrap_or_else(|err| panic!("endpoint client: {err}"));
        let started = Instant::now();
        let error = client
            .health(&target)
            .await
            .expect_err("delayed health response must time out");

        assert!(error.to_string().contains("health request failed"));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "request timeout exceeded deterministic upper bound: {:?}",
            started.elapsed()
        );
        let requests = server.captured_requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/slow/non-default/health");
    }

    fn test_targets(table: &str, provider: RunnerFixtureProvider) -> TargetEnvironmentManifest {
        let capabilities = vec![
            Capability::Sql,
            Capability::RemoteWrite,
            Capability::FooterObservation,
            Capability::ReadBench,
        ];
        let mut targets = TargetEnvironmentManifest {
            version: MANIFEST_VERSION,
            run_id: "run".to_string(),
            controller_id: "controller".to_string(),
            targets: vec![
                test_target(TargetRole::Base, "base", table, capabilities.clone()),
                test_target(TargetRole::Candidate, "candidate", table, capabilities),
            ],
            completion_digest: Sha256Digest::compute("unsealed"),
        };
        targets
            .seal()
            .unwrap_or_else(|err| panic!("seal targets: {err}"));
        if matches!(provider, RunnerFixtureProvider::ApiWrite) {
            for target in &mut targets.targets {
                target.fixture_bindings[0].catalog = None;
                target.fixture_bindings[0].physical_table = Some("greptime_physical_table".into());
            }
            targets
                .seal()
                .unwrap_or_else(|err| panic!("reseal API targets: {err}"));
        }
        targets
    }

    fn test_fixture(
        case: &ValidatedCase,
        targets: &TargetEnvironmentManifest,
        provider: RunnerFixtureProvider,
    ) -> FixtureManifest {
        let external = matches!(provider, RunnerFixtureProvider::ExternalPrepared);
        let mut fixture = FixtureManifest {
            version: MANIFEST_VERSION,
            run_id: "run".to_string(),
            fixture_id: "fixture".to_string(),
            provider: provider.clone(),
            data_plan_digest: case
                .fixture_plan_digest()
                .unwrap_or_else(|err| panic!("fixture plan digest: {err}")),
            generator_identity: Sha256Digest::compute("generator"),
            logical_summary: match provider {
                RunnerFixtureProvider::ApiWrite => {
                    let expected = case.remote_write_fixture_expectation().ok();
                    crate::query_perf::manifest::LogicalFixtureSummary {
                        rows: expected
                            .as_ref()
                            .map_or(1, |value| value.logical_summary().rows()),
                        series: expected
                            .as_ref()
                            .map_or(1, |value| value.logical_summary().series()),
                        time_start_unix_nanos: expected
                            .as_ref()
                            .map_or(0, |value| value.logical_summary().time_start_unix_nanos()),
                        time_end_unix_nanos: expected
                            .as_ref()
                            .map_or(0, |value| value.logical_summary().time_end_unix_nanos()),
                    }
                }
                RunnerFixtureProvider::ExternalPrepared => {
                    let expected = case
                        .direct_fixture_logical_summary()
                        .unwrap_or_else(|err| panic!("direct summary: {err}"));
                    crate::query_perf::manifest::LogicalFixtureSummary {
                        rows: expected.rows(),
                        series: expected.series(),
                        time_start_unix_nanos: expected.time_start_unix_nanos(),
                        time_end_unix_nanos: expected.time_end_unix_nanos(),
                    }
                }
            },
            storage_prefix: "fixtures/run".to_string(),
            files: external
                .then(|| {
                    vec![crate::query_perf::manifest::FixtureFile {
                        file_id: "file".to_string(),
                        uri: "s3://fixtures/file".to_string(),
                        size_bytes: 1,
                        digest: Sha256Digest::compute("file"),
                    }]
                })
                .unwrap_or_default(),
            target_bindings: targets
                .targets
                .iter()
                .map(|target| crate::query_perf::manifest::FixtureTargetBinding {
                    role: target.role,
                    target_id: target.target_id.clone(),
                    catalog: external.then(|| "greptime".to_string()),
                    database: "public".to_string(),
                    logical_table: target.fixture_bindings[0].logical_table.clone(),
                    physical_table: (!external).then(|| "greptime_physical_table".to_string()),
                    region: "0".to_string(),
                    file_ids: external
                        .then(|| vec!["file".to_string()])
                        .unwrap_or_default(),
                    external: external.then(|| {
                        crate::query_perf::manifest::ExternalPreparedTargetBinding {
                            catalog: "greptime".to_string(),
                            schema: "public".to_string(),
                            table_id: 1,
                            region_id: 0,
                            time_index: "ts".to_string(),
                            time_unit: TimestampUnit::Nanosecond,
                        }
                    }),
                })
                .collect(),
            completion_digest: Sha256Digest::compute("unsealed"),
        };
        fixture
            .seal()
            .unwrap_or_else(|err| panic!("seal fixture: {err}"));
        fixture
    }

    fn finalization_artifacts(
        case: &ValidatedCase,
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
        base_latency: f64,
        base_footer_total: u64,
    ) -> (MeasureArtifact, ObservationBundle) {
        let queries = case
            .manifest_queries()
            .unwrap_or_else(|err| panic!("finalize queries: {err}"));
        let mut request = ObservationRequestManifest {
            version: MANIFEST_VERSION,
            run_id: targets.run_id.clone(),
            request_id: "request".into(),
            fixture_id: fixture.fixture_id.clone(),
            case_digest: case
                .executable_digest()
                .unwrap_or_else(|err| panic!("case digest: {err}")),
            target_manifest_digest: targets.completion_digest.clone(),
            fixture_manifest_digest: fixture.completion_digest.clone(),
            requests: targets
                .targets
                .iter()
                .flat_map(|target| {
                    case.observation_kinds()
                        .into_iter()
                        .map(move |kind| ObservationRequest {
                            role: target.role,
                            target_id: target.target_id.clone(),
                            kind,
                            read_bench: (kind
                                == crate::query_perf::manifest::ObservationKind::ReadBench)
                                .then(|| {
                                    let plan = case
                                        .remote_write()
                                        .unwrap_or_else(|| panic!("remote-write plan"));
                                    ReadBenchRequest {
                                        parquetbench: plan.read_bench_parquet(),
                                        scanbench: plan.read_bench_scan(),
                                    }
                                }),
                        })
                })
                .collect(),
            completion_digest: Sha256Digest::compute("unsealed"),
        };
        request
            .seal()
            .unwrap_or_else(|err| panic!("request seal: {err}"));
        let mut measurements = Vec::new();
        for target in &targets.targets {
            let latency = (target.role == TargetRole::Base)
                .then_some(base_latency)
                .unwrap_or(1.0);
            let mut measurement = MeasurementManifest {
                version: MANIFEST_VERSION,
                run_id: targets.run_id.clone(),
                measurement_id: format!("measurement-{}", target.target_id),
                target_id: target.target_id.clone(),
                fixture_id: fixture.fixture_id.clone(),
                queries: queries
                    .iter()
                    .cloned()
                    .map(|identity| QueryMeasurement {
                        samples: vec![crate::query_perf::manifest::LatencySample {
                            milliseconds: latency,
                        }],
                        identity,
                        status: QueryStatus::Passed,
                        result_digest: Sha256Digest::compute(target.target_id.as_bytes()),
                    })
                    .collect(),
                completion_digest: Sha256Digest::compute("unsealed"),
            };
            measurement
                .seal()
                .unwrap_or_else(|err| panic!("measurement seal: {err}"));
            measurements.push(measurement);
        }
        let mut bundle = MeasurementBundle {
            version: MANIFEST_VERSION,
            run_id: targets.run_id.clone(),
            fixture_id: fixture.fixture_id.clone(),
            case_digest: case
                .executable_digest()
                .unwrap_or_else(|err| panic!("case digest: {err}")),
            target_manifest_digest: targets.completion_digest.clone(),
            fixture_manifest_digest: fixture.completion_digest.clone(),
            measurements,
            observation_request: ArtifactReference {
                id: request.request_id.clone(),
                digest: request.completion_digest.clone(),
            },
            completion_digest: Sha256Digest::compute("unsealed"),
        };
        bundle
            .seal()
            .unwrap_or_else(|err| panic!("measurement bundle seal: {err}"));
        let mut observations = Vec::new();
        for target in &targets.targets {
            for kind in case.observation_kinds() {
                let payload = match kind {
                    crate::query_perf::manifest::ObservationKind::Footer => {
                        ObservationPayload::Footer(crate::query_perf::manifest::FooterObservation {
                            file_count: 1,
                            total_file_size_bytes: (target.role == TargetRole::Base)
                                .then_some(base_footer_total)
                                .unwrap_or(1),
                            files_with_column: 1,
                            column_compressed_size_bytes: 1,
                            column_uncompressed_size_bytes: 1,
                            encodings: vec![],
                        })
                    }
                    crate::query_perf::manifest::ObservationKind::ReadBench => {
                        ObservationPayload::ReadBench(
                            crate::query_perf::manifest::ReadBenchObservation {
                                parquet_median_milliseconds: Some(1.0),
                                scan_median_milliseconds: Some(1.0),
                            },
                        )
                    }
                };
                let mut observation = crate::query_perf::manifest::ObservationManifest {
                    version: MANIFEST_VERSION,
                    run_id: targets.run_id.clone(),
                    observation_id: format!("{}-{:?}", target.target_id, kind),
                    target_id: target.target_id.clone(),
                    fixture_id: fixture.fixture_id.clone(),
                    status: crate::query_perf::manifest::ObservationStatus::Passed,
                    payload,
                    failure_detail: None,
                    completion_digest: Sha256Digest::compute("unsealed"),
                };
                observation
                    .seal()
                    .unwrap_or_else(|err| panic!("observation seal: {err}"));
                observations.push(observation);
            }
        }
        let mut observation_bundle = ObservationBundle {
            version: MANIFEST_VERSION,
            run_id: targets.run_id.clone(),
            fixture_id: fixture.fixture_id.clone(),
            request: ArtifactReference {
                id: request.request_id.clone(),
                digest: request.completion_digest.clone(),
            },
            target_manifest_digest: targets.completion_digest.clone(),
            fixture_manifest_digest: fixture.completion_digest.clone(),
            observations,
            completion_digest: Sha256Digest::compute("unsealed"),
        };
        observation_bundle
            .seal()
            .unwrap_or_else(|err| panic!("observation bundle seal: {err}"));
        (
            MeasureArtifact::Success {
                measurement_bundle: bundle,
                observation_request: request,
            },
            observation_bundle,
        )
    }

    fn write_finalize_inputs(
        directory: &Path,
        source: &str,
        base_latency: f64,
        base_footer_total: u64,
    ) -> FinalizeArgs {
        let case_path = directory.join("case.toml");
        fs::write(&case_path, source).unwrap_or_else(|err| panic!("case write: {err}"));
        let case = case::load_case(&case_path).unwrap_or_else(|err| panic!("case load: {err}"));
        let targets = test_targets("m", RunnerFixtureProvider::ApiWrite);
        let fixture = test_fixture(&case, &targets, RunnerFixtureProvider::ApiWrite);
        let (measurements, observations) =
            finalization_artifacts(&case, &targets, &fixture, base_latency, base_footer_total);
        let targets_path = directory.join("targets.json");
        let fixture_path = directory.join("fixture.json");
        let measurements_path = directory.join("measurements.json");
        let observations_path = directory.join("observations.json");
        for (path, value) in [
            (&targets_path, serde_json::to_vec(&targets)),
            (&fixture_path, serde_json::to_vec(&fixture)),
            (&measurements_path, serde_json::to_vec(&measurements)),
            (&observations_path, serde_json::to_vec(&observations)),
        ] {
            fs::write(
                path,
                value.unwrap_or_else(|err| panic!("artifact serialize: {err}")),
            )
            .unwrap_or_else(|err| panic!("artifact write: {err}"));
        }
        FinalizeArgs {
            case: case_path,
            targets: targets_path,
            fixture: fixture_path,
            measurements: measurements_path,
            observations: observations_path,
            report: directory.join("report.json"),
            summary: directory.join("summary.md"),
        }
    }

    fn target_mut(targets: &mut TargetEnvironmentManifest, role: TargetRole) -> &mut TargetBinding {
        targets
            .targets
            .iter_mut()
            .find(|target| target.role == role)
            .unwrap_or_else(|| panic!("target role"))
    }

    fn remove_sql_from_base(targets: &mut TargetEnvironmentManifest, _: &mut FixtureManifest) {
        target_mut(targets, TargetRole::Base)
            .capabilities
            .retain(|capability| capability != &Capability::Sql);
    }

    fn remove_sql_from_candidate(targets: &mut TargetEnvironmentManifest, _: &mut FixtureManifest) {
        target_mut(targets, TargetRole::Candidate)
            .capabilities
            .retain(|capability| capability != &Capability::Sql);
    }

    fn remove_remote_write_from_base(
        targets: &mut TargetEnvironmentManifest,
        _: &mut FixtureManifest,
    ) {
        let target = target_mut(targets, TargetRole::Base);
        target
            .capabilities
            .retain(|capability| capability != &Capability::RemoteWrite);
    }

    fn remove_remote_write_endpoint(
        targets: &mut TargetEnvironmentManifest,
        _: &mut FixtureManifest,
    ) {
        target_mut(targets, TargetRole::Candidate).remote_write_endpoint = None;
    }

    fn remove_footer_observation(targets: &mut TargetEnvironmentManifest, _: &mut FixtureManifest) {
        target_mut(targets, TargetRole::Candidate)
            .capabilities
            .retain(|capability| capability != &Capability::FooterObservation);
    }

    fn remove_read_bench(targets: &mut TargetEnvironmentManifest, _: &mut FixtureManifest) {
        target_mut(targets, TargetRole::Candidate)
            .capabilities
            .retain(|capability| capability != &Capability::ReadBench);
    }

    fn mismatch_external_case_binding(
        _: &mut TargetEnvironmentManifest,
        fixture: &mut FixtureManifest,
    ) {
        fixture.target_bindings[0]
            .external
            .as_mut()
            .unwrap_or_else(|| panic!("external binding"))
            .time_index = "other_ts".to_string();
    }

    fn benchmark_response(value: &str) -> serde_json::Value {
        serde_json::json!({"data": [{"result": value}]})
    }

    fn show_create_response(table: &str, create: impl Into<String>) -> Result<serde_json::Value> {
        Ok(serde_json::json!({"data":[{"Table":table,"Create Table":create.into()}]}))
    }

    fn direct_table_ddl(table: &str) -> String {
        match table {
            "t" => r#"CREATE TABLE IF NOT EXISTS "t" (
  "host" STRING NULL,
  "value" DOUBLE NULL,
  "ts" TIMESTAMP(9) NOT NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("host")
)
ENGINE=mito"#
                .into(),
            "sql_join_fact" => r#"CREATE TABLE IF NOT EXISTS "sql_join_fact" (
  "host" STRING NULL,
  "instance" STRING NULL,
  "value" DOUBLE NULL,
  "ts" TIMESTAMP(9) NOT NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("host", "instance")
)
ENGINE=mito
WITH(
  'append_mode' = 'true',
  'sst_format' = 'flat'
)"#
            .into(),
            "sql_join_dim" => r#"CREATE TABLE IF NOT EXISTS "sql_join_dim" (
  "host" STRING NULL,
  "zone" STRING NULL,
  "weight" DOUBLE NULL,
  "ts" TIMESTAMP(9) NOT NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("host", "zone")
)
ENGINE=mito
WITH(
  'append_mode' = 'true',
  'sst_format' = 'flat'
)"#
            .into(),
            other => panic!("missing production-shaped DDL mock for {other}"),
        }
    }

    fn benchmark_digest(values: &[&str]) -> Sha256Digest {
        Sha256Digest::compute(
            canonical_json_bytes(&serde_json::Value::Array(
                values
                    .iter()
                    .map(|value| benchmark_response(value))
                    .collect(),
            ))
            .unwrap_or_else(|err| panic!("canonical response: {err}")),
        )
    }

    fn append_benchmark_responses(responses: &mut Vec<Result<serde_json::Value>>, target: &str) {
        for value in [
            format!("{target}-sql-warmup"),
            format!("{target}-sql-1"),
            format!("{target}-sql-2"),
            format!("{target}-tql-warmup-1"),
            format!("{target}-tql-warmup-2"),
            format!("{target}-tql-1"),
        ] {
            responses.push(Ok(benchmark_response(&value)));
        }
    }

    fn external_evidence_calls(target: &str) -> Vec<String> {
        vec![
            format!("statement:{target}:public:SHOW CREATE TABLE \"greptime\".\"public\".\"t\""),
            format!(
                "statement:{target}:public:SELECT table_id AS table_id FROM information_schema.tables WHERE table_catalog = 'greptime' AND table_schema = 'public' AND table_name = 't'"
            ),
            format!(
                "statement:{target}:public:SELECT region_id AS region_id, is_leader AS is_leader, status AS status FROM information_schema.region_peers WHERE table_catalog = 'greptime' AND table_schema = 'public' AND table_name = 't'"
            ),
            format!(
                "statement:{target}:public:SELECT COUNT(*) AS count, MIN(\"ts\") AS min_time, MAX(\"ts\") AS max_time FROM \"greptime\".\"public\".\"t\""
            ),
        ]
    }

    fn benchmark_calls(target: &str) -> Vec<String> {
        let sql = "SELECT * FROM t WHERE host = 'host-0'";
        let tql = "TQL EVAL (0, 1, '1s') t";
        let mut calls = Vec::with_capacity(6);
        calls.extend((0..3).map(|_| format!("statement:{target}:public:{sql}")));
        calls.extend((0..3).map(|_| format!("statement:{target}:public:{tql}")));
        calls
    }

    struct MockRemoteWriteExecutor {
        requests: Mutex<Vec<RemoteWriteRequest>>,
        fail_at: Option<usize>,
    }

    #[async_trait]
    impl RemoteWriteExecutor for MockRemoteWriteExecutor {
        async fn execute(&self, request: RemoteWriteRequest) -> Result<()> {
            let mut requests = self.requests.lock().unwrap_or_else(|_| panic!("requests"));
            let index = requests.len();
            requests.push(request);
            if self.fail_at == Some(index) {
                Err(Error::new("remote write failed"))
            } else {
                Ok(())
            }
        }
    }

    struct ImmediateVisibilityWaiter {
        now: Mutex<Instant>,
        sleeps: Mutex<u64>,
    }

    #[async_trait]
    impl VisibilityWaiter for ImmediateVisibilityWaiter {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap_or_else(|_| panic!("clock"))
        }

        async fn sleep(&self, duration: Duration) {
            *self.sleeps.lock().unwrap_or_else(|_| panic!("sleeps")) += 1;
            let mut now = self.now.lock().unwrap_or_else(|_| panic!("clock"));
            *now += duration;
        }
    }

    fn api_case(samples: u64, chunk: u64, flush_every: u64) -> ValidatedCase {
        test_case(&format!(
            r#"
[scenario]
kind = "prom_remote_write_then_query"
[scenario.remote_write]
database = "public"
metric = "m"
physical_table = "p"
series_count = 2
samples_per_series = {samples}
sample_chunk_size = {chunk}
flush_every_sample_chunks = {flush_every}
start_unix_millis = 1000
step_millis = 10
[[scenario.queries]]
name = "q"
kind = "sql"
database = "public"
query = "SELECT 1"
iterations = 1
"#
        ))
    }

    fn api_target() -> TargetBinding {
        test_target(
            TargetRole::Base,
            "base",
            "p",
            vec![Capability::Sql, Capability::RemoteWrite],
        )
    }

    fn count_response(count: u64) -> Result<serde_json::Value> {
        Ok(serde_json::json!({"data": [{"count": count}]}))
    }

    #[test]
    fn api_write_fixture_case_binding_rejects_every_resealed_identity_mutation() {
        let case = test_case(REMOTE_CASE);
        let targets = test_targets("m", RunnerFixtureProvider::ApiWrite);
        let fixture = test_fixture(&case, &targets, RunnerFixtureProvider::ApiWrite);
        assert!(validate_preflight(case, targets, fixture).is_ok());
        let case = test_case(REMOTE_CASE);
        let targets = test_targets("m", RunnerFixtureProvider::ApiWrite);
        let fixture: FixtureManifest = serde_json::from_str(
            &serde_json::to_string(&test_fixture(
                &case,
                &targets,
                RunnerFixtureProvider::ApiWrite,
            ))
            .unwrap_or_else(|err| panic!("API fixture serialize: {err}")),
        )
        .unwrap_or_else(|err| panic!("API fixture deserialize: {err}"));
        validate_preflight(case, targets, fixture)
            .unwrap_or_else(|err| panic!("API fixture roundtrip preflight: {err}"));

        let mutations: [(&str, fn(&mut FixtureManifest)); 7] = [
            ("database", |fixture: &mut FixtureManifest| {
                for binding in &mut fixture.target_bindings {
                    binding.database = "other".into();
                }
            }),
            ("logical metric table", |fixture: &mut FixtureManifest| {
                for binding in &mut fixture.target_bindings {
                    binding.logical_table = "other_metric".into();
                }
            }),
            ("physical table", |fixture: &mut FixtureManifest| {
                for binding in &mut fixture.target_bindings {
                    binding.physical_table = Some("other_physical".into());
                }
            }),
            ("rows", |fixture: &mut FixtureManifest| {
                fixture.logical_summary.rows += 1
            }),
            ("series", |fixture: &mut FixtureManifest| {
                fixture.logical_summary.series += 1
            }),
            ("start", |fixture: &mut FixtureManifest| {
                fixture.logical_summary.time_start_unix_nanos += 1
            }),
            ("end", |fixture: &mut FixtureManifest| {
                fixture.logical_summary.time_end_unix_nanos += 1
            }),
        ];
        for (name, mutate) in mutations {
            let case = test_case(REMOTE_CASE);
            let mut targets = test_targets("m", RunnerFixtureProvider::ApiWrite);
            let mut fixture = test_fixture(&case, &targets, RunnerFixtureProvider::ApiWrite);
            mutate(&mut fixture);
            for target in &mut targets.targets {
                for target_binding in &mut target.fixture_bindings {
                    if let Some(binding) = fixture.target_bindings.iter().find(|binding| {
                        binding.role == target.role
                            && binding.target_id == target.target_id
                            && binding.logical_table != "m"
                    }) {
                        target_binding.logical_table = binding.logical_table.clone();
                    }
                    if let Some(binding) = fixture.target_bindings.iter().find(|binding| {
                        binding.role == target.role
                            && binding.target_id == target.target_id
                            && binding.database != "public"
                    }) {
                        target_binding.database = binding.database.clone();
                    }
                    if let Some(binding) = fixture.target_bindings.iter().find(|binding| {
                        binding.role == target.role
                            && binding.target_id == target.target_id
                            && binding.physical_table.as_deref() != Some("greptime_physical_table")
                    }) {
                        target_binding.physical_table = binding.physical_table.clone();
                    }
                }
            }
            targets
                .seal()
                .unwrap_or_else(|err| panic!("{name} target seal: {err}"));
            fixture
                .seal()
                .unwrap_or_else(|err| panic!("{name} fixture seal: {err}"));
            assert!(
                validate_preflight(case, targets, fixture).is_err(),
                "resealed {name} mutation must fail before endpoint use"
            );
        }

        let case = test_case(REMOTE_CASE);
        let mut targets = test_targets("m", RunnerFixtureProvider::ApiWrite);
        let mut fixture = test_fixture(&case, &targets, RunnerFixtureProvider::ApiWrite);
        fixture.provider = RunnerFixtureProvider::ExternalPrepared;
        fixture.files = vec![crate::query_perf::manifest::FixtureFile {
            file_id: "file".into(),
            uri: "s3://fixture/file".into(),
            size_bytes: 1,
            digest: Sha256Digest::compute("file"),
        }];
        for (binding, target) in fixture.target_bindings.iter_mut().zip(&mut targets.targets) {
            binding.catalog = Some("greptime".into());
            binding.physical_table = None;
            binding.file_ids = vec!["file".into()];
            binding.external = Some(crate::query_perf::manifest::ExternalPreparedTargetBinding {
                catalog: "greptime".into(),
                schema: "public".into(),
                table_id: 1,
                region_id: 0,
                time_index: "ts".into(),
                time_unit: TimestampUnit::Nanosecond,
            });
            target.fixture_bindings[0].catalog = binding.catalog.clone();
            target.fixture_bindings[0].physical_table = None;
        }
        targets
            .seal()
            .unwrap_or_else(|err| panic!("provider target seal: {err}"));
        fixture
            .seal()
            .unwrap_or_else(|err| panic!("provider fixture seal: {err}"));
        assert!(validate_preflight(case, targets, fixture).is_err());
    }

    #[tokio::test]
    async fn api_write_divisible_chunks_flush_and_visibility_are_exact() {
        let case = api_case(6, 3, 2);
        let plan = case.remote_write().unwrap_or_else(|| panic!("remote plan"));
        let target = api_target();
        let client = MockEndpointClient::new([
            Ok(serde_json::json!({"affected_rows": 0})),
            count_response(12),
        ]);
        let remote = MockRemoteWriteExecutor {
            requests: Mutex::new(vec![]),
            fail_at: None,
        };
        let waiter = ImmediateVisibilityWaiter {
            now: Mutex::new(Instant::now()),
            sleeps: Mutex::new(0),
        };
        write_api_fixture(&client, &remote, &target, "http://example.com/write", plan)
            .await
            .unwrap_or_else(|err| panic!("write: {err}"));
        verify_visible_rows_until(
            &client,
            &target,
            "public",
            "m",
            expected_remote_rows(plan).unwrap_or(0),
            Duration::from_secs(1),
            &waiter,
        )
        .await
        .unwrap_or_else(|err| panic!("visibility: {err}"));
        let requests = remote
            .requests
            .lock()
            .unwrap_or_else(|_| panic!("requests"));
        assert_eq!(requests.len(), 2);
        for (request, offset) in requests.iter().zip([0, 3]) {
            assert_eq!(
                (
                    request.samples_per_series,
                    request.start_unix_millis,
                    request.value_sample_offset
                ),
                (3, 1000 + i64::try_from(offset).unwrap_or(-1) * 10, offset)
            );
            assert_eq!(
                (
                    request.value_total_samples_per_series,
                    request.series_count,
                    request.database.as_str(),
                    request.metric.as_str(),
                    request.physical_table.as_str()
                ),
                (Some(6), 2, "public", "m", "p")
            );
            assert_eq!(
                (
                    request.value_base,
                    request.value_step,
                    request.value_cardinality,
                    request.value_seed,
                    request.value_run_length,
                    request.value_stall_every,
                    request.value_stall_length,
                    request.value_mixed_every
                ),
                (0.0, 0.125, 97, 0, 8, 100, 16, 5)
            );
        }
        assert_eq!(
            *client.calls.lock().unwrap_or_else(|_| panic!("calls")),
            vec![
                "statement:base:public:ADMIN FLUSH_TABLE('p')",
                "statement:base:public:SELECT COUNT(*) AS count FROM \"m\""
            ]
        );
    }

    #[tokio::test]
    async fn api_write_non_divisible_chunks_require_final_flush() {
        let case = api_case(8, 3, 2);
        let plan = case.remote_write().unwrap_or_else(|| panic!("remote plan"));
        let target = api_target();
        let client = MockEndpointClient::new(
            std::iter::repeat_with(|| Ok(serde_json::json!({"affected_rows": 0}))).take(2),
        );
        let remote = MockRemoteWriteExecutor {
            requests: Mutex::new(vec![]),
            fail_at: None,
        };
        write_api_fixture(&client, &remote, &target, "http://example.com/write", plan)
            .await
            .unwrap_or_else(|err| panic!("write: {err}"));
        let requests = remote
            .requests
            .lock()
            .unwrap_or_else(|_| panic!("requests"));
        assert_eq!(
            requests
                .iter()
                .map(|request| (
                    request.samples_per_series,
                    request.start_unix_millis,
                    request.value_sample_offset
                ))
                .collect::<Vec<_>>(),
            vec![(3, 1000, 0), (3, 1030, 3), (2, 1060, 6)]
        );
        assert_eq!(
            *client.calls.lock().unwrap_or_else(|_| panic!("calls")),
            vec![
                "statement:base:public:ADMIN FLUSH_TABLE('p')",
                "statement:base:public:ADMIN FLUSH_TABLE('p')"
            ]
        );
    }

    #[tokio::test]
    async fn api_write_visibility_retries_before_benchmarking() {
        let target = api_target();
        let client = MockEndpointClient::new([count_response(3), count_response(4)]);
        let waiter = ImmediateVisibilityWaiter {
            now: Mutex::new(Instant::now()),
            sleeps: Mutex::new(0),
        };
        verify_visible_rows_until(
            &client,
            &target,
            "public",
            "m",
            4,
            Duration::from_secs(1),
            &waiter,
        )
        .await
        .unwrap_or_else(|err| panic!("visibility: {err}"));
        assert_eq!(
            *waiter.sleeps.lock().unwrap_or_else(|_| panic!("sleeps")),
            1
        );
        assert_eq!(
            *client.calls.lock().unwrap_or_else(|_| panic!("calls")),
            vec![
                "statement:base:public:SELECT COUNT(*) AS count FROM \"m\"",
                "statement:base:public:SELECT COUNT(*) AS count FROM \"m\""
            ]
        );
    }

    #[tokio::test]
    async fn api_write_visibility_timeout_is_immediate_and_stops_before_benchmarks() {
        let target = api_target();
        let client = MockEndpointClient::new([count_response(3), count_response(3)]);
        let waiter = ImmediateVisibilityWaiter {
            now: Mutex::new(Instant::now()),
            sleeps: Mutex::new(0),
        };
        assert!(
            verify_visible_rows_until(
                &client,
                &target,
                "public",
                "m",
                4,
                Duration::from_millis(1),
                &waiter
            )
            .await
            .is_err()
        );
        assert_eq!(
            *waiter.sleeps.lock().unwrap_or_else(|_| panic!("sleeps")),
            1
        );
        assert_eq!(
            *client.calls.lock().unwrap_or_else(|_| panic!("calls")),
            vec![
                "statement:base:public:SELECT COUNT(*) AS count FROM \"m\"",
                "statement:base:public:SELECT COUNT(*) AS count FROM \"m\""
            ]
        );
    }

    #[tokio::test]
    async fn api_write_executor_failure_stops_before_flush_or_later_chunks() {
        let case = api_case(8, 3, 10);
        let plan = case.remote_write().unwrap_or_else(|| panic!("remote plan"));
        let target = api_target();
        let client = MockEndpointClient::new(std::iter::empty());
        let remote = MockRemoteWriteExecutor {
            requests: Mutex::new(vec![]),
            fail_at: Some(1),
        };
        assert!(
            write_api_fixture(&client, &remote, &target, "http://example.com/write", plan)
                .await
                .is_err()
        );
        assert_eq!(
            remote
                .requests
                .lock()
                .unwrap_or_else(|_| panic!("requests"))
                .len(),
            2
        );
        assert!(
            client
                .calls
                .lock()
                .unwrap_or_else(|_| panic!("calls"))
                .is_empty()
        );
    }

    #[test]
    fn api_write_arithmetic_boundaries_fail_closed() {
        assert!(case::validate_remote_write_totals(u64::MAX, 2, 0, 2).is_err());
        assert!(case::validate_remote_write_totals(1, 1, 1, 1).is_err());
    }

    #[tokio::test]
    async fn external_prepared_measure_orchestrates_evidence_and_benchmarks_in_order() {
        let case = test_case(EXTERNAL_MEASURE_CASE);
        let targets = test_targets("t", RunnerFixtureProvider::ExternalPrepared);
        let fixture = test_fixture(&case, &targets, RunnerFixtureProvider::ExternalPrepared);
        let preflight = validate_preflight(case, targets, fixture)
            .unwrap_or_else(|err| panic!("preflight: {err}"));
        let mut responses = Vec::new();
        responses.extend(external_responses());
        responses.extend(external_responses());
        append_benchmark_responses(&mut responses, "base");
        append_benchmark_responses(&mut responses, "candidate");
        let client = MockEndpointClient::new(responses);

        let artifact = measure_with_client(&preflight, &client)
            .await
            .unwrap_or_else(|err| panic!("measure: {err}"));
        let (measurement_bundle, observation_request) = match artifact {
            MeasureArtifact::Success {
                measurement_bundle,
                observation_request,
            } => (measurement_bundle, observation_request),
            MeasureArtifact::Failed { .. } => panic!("external prepared measure must succeed"),
        };

        let mut expected_calls = vec!["health:base".to_string(), "health:candidate".to_string()];
        expected_calls.extend(external_evidence_calls("base"));
        expected_calls.extend(external_evidence_calls("candidate"));
        expected_calls.extend(benchmark_calls("base"));
        expected_calls.extend(benchmark_calls("candidate"));
        assert_eq!(
            *client.calls.lock().unwrap_or_else(|_| panic!("calls")),
            expected_calls
        );
        assert_eq!(client.remaining_responses(), 0);

        observation_request
            .validate_against(
                &preflight.case_digest,
                &preflight.targets,
                &preflight.fixture,
            )
            .unwrap_or_else(|err| panic!("observation request: {err}"));
        measurement_bundle
            .validate_against(
                &preflight.queries,
                &preflight.case_digest,
                &preflight.targets,
                &preflight.fixture,
                &observation_request,
            )
            .unwrap_or_else(|err| panic!("measurement bundle: {err}"));
        assert_eq!(
            measurement_bundle
                .measurements
                .iter()
                .map(|measurement| measurement.target_id.as_str())
                .collect::<Vec<_>>(),
            ["base", "candidate"]
        );
        for (measurement, target) in measurement_bundle
            .measurements
            .iter()
            .zip(["base", "candidate"])
        {
            assert_eq!(measurement.queries.len(), 2);
            assert_eq!(measurement.queries[0].identity.name, "sql_query");
            assert_eq!(measurement.queries[0].identity.database, "public");
            assert_eq!(
                measurement.queries[0].identity.query,
                "SELECT * FROM t WHERE host = 'host-0'"
            );
            assert_eq!(measurement.queries[0].identity.warmup, 1);
            assert_eq!(measurement.queries[0].identity.iterations, 2);
            assert_eq!(measurement.queries[0].samples.len(), 2);
            assert_eq!(
                measurement.queries[0].result_digest,
                benchmark_digest(&[&format!("{target}-sql-1"), &format!("{target}-sql-2")])
            );
            assert_eq!(measurement.queries[1].identity.name, "tql_query");
            assert_eq!(measurement.queries[1].identity.database, "public");
            assert_eq!(
                measurement.queries[1].identity.query,
                "TQL EVAL (0, 1, '1s') t"
            );
            assert_eq!(measurement.queries[1].identity.warmup, 2);
            assert_eq!(measurement.queries[1].identity.iterations, 1);
            assert_eq!(measurement.queries[1].samples.len(), 1);
            assert_eq!(
                measurement.queries[1].result_digest,
                benchmark_digest(&[&format!("{target}-tql-1")])
            );
        }
    }

    fn join_case() -> ValidatedCase {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/perf/query_cases/sql_join_filter_order/case.toml");
        case::load_case(&path).unwrap_or_else(|err| panic!("join case: {err}"))
    }

    fn join_targets_and_fixture(
        case: &ValidatedCase,
    ) -> (TargetEnvironmentManifest, FixtureManifest) {
        let direct = case
            .direct_readable_sst()
            .unwrap_or_else(|err| panic!("join direct case: {err}"));
        let mut targets = test_targets(
            direct.tables()[0].name(),
            RunnerFixtureProvider::ExternalPrepared,
        );
        for target in &mut targets.targets {
            target
                .fixture_bindings
                .push(crate::query_perf::manifest::TargetFixtureBinding {
                    fixture_id: "fixture".into(),
                    catalog: Some("greptime".into()),
                    database: direct.tables()[1].database().into(),
                    logical_table: direct.tables()[1].name().into(),
                    physical_table: None,
                    region: "0".into(),
                });
        }
        targets
            .seal()
            .unwrap_or_else(|err| panic!("join targets: {err}"));
        let summary = case
            .direct_fixture_logical_summary()
            .unwrap_or_else(|err| panic!("join summary: {err}"));
        let mut files = Vec::new();
        let mut bindings = Vec::new();
        for target in &targets.targets {
            for (index, table) in direct.tables().iter().enumerate() {
                let file_id = format!("{}-{}", target.target_id, table.name());
                files.push(crate::query_perf::manifest::FixtureFile {
                    file_id: file_id.clone(),
                    uri: format!("s3://fixture/{file_id}"),
                    size_bytes: 1,
                    digest: Sha256Digest::compute(&file_id),
                });
                bindings.push(crate::query_perf::manifest::FixtureTargetBinding {
                    role: target.role,
                    target_id: target.target_id.clone(),
                    catalog: Some("greptime".into()),
                    database: table.database().into(),
                    logical_table: table.name().into(),
                    physical_table: None,
                    region: "0".into(),
                    file_ids: vec![file_id],
                    external: Some(crate::query_perf::manifest::ExternalPreparedTargetBinding {
                        catalog: "greptime".into(),
                        schema: table.database().into(),
                        table_id: (index + 1) as u64,
                        region_id: 0,
                        time_index: table.time_index().into(),
                        time_unit: TimestampUnit::Nanosecond,
                    }),
                });
            }
        }
        let mut fixture = FixtureManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            fixture_id: "fixture".into(),
            provider: RunnerFixtureProvider::ExternalPrepared,
            data_plan_digest: case
                .fixture_plan_digest()
                .unwrap_or_else(|err| panic!("join plan: {err}")),
            generator_identity: Sha256Digest::compute("join-generator"),
            logical_summary: crate::query_perf::manifest::LogicalFixtureSummary {
                rows: summary.rows(),
                series: summary.series(),
                time_start_unix_nanos: summary.time_start_unix_nanos(),
                time_end_unix_nanos: summary.time_end_unix_nanos(),
            },
            storage_prefix: "fixtures/join".into(),
            files,
            target_bindings: bindings,
            completion_digest: Sha256Digest::compute("unsealed"),
        };
        fixture
            .seal()
            .unwrap_or_else(|err| panic!("join fixture: {err}"));
        (targets, fixture)
    }

    fn join_evidence_responses(
        case: &ValidatedCase,
        fixture: &FixtureManifest,
        target_id: &str,
    ) -> Vec<Result<serde_json::Value>> {
        case.direct_readable_sst()
            .unwrap_or_else(|err| panic!("join direct: {err}"))
            .tables()
            .iter()
            .map(|table| {
                fixture
                    .target_bindings
                    .iter()
                    .find(|binding| binding.target_id == target_id && binding.logical_table == table.name())
                    .unwrap_or_else(|| panic!("join binding"))
            })
            .flat_map(|binding| {
                let external = binding.external.as_ref().unwrap_or_else(|| panic!("external"));
                [
                    show_create_response(
                        &binding.logical_table,
                        direct_table_ddl(&binding.logical_table),
                    ),
                    Ok(serde_json::json!({"data":[{"table_id":external.table_id}]})),
                    Ok(serde_json::json!({"data":[{"region_id":0,"is_leader":true,"status":"ALIVE"}]})),
                    Ok(serde_json::json!({"data":[{"count":16384,"min_time":1704067200000000000i64,"max_time":1704083583000000000i64}]})),
                ]
            })
            .collect()
    }

    #[tokio::test]
    async fn external_prepared_join_binds_and_verifies_every_target_table_before_benchmarking() {
        let case = join_case();
        let (targets, fixture) = join_targets_and_fixture(&case);
        let preflight = validate_preflight(case, targets, fixture)
            .unwrap_or_else(|err| panic!("join preflight: {err}"));
        let mut responses = join_evidence_responses(&preflight.case, &preflight.fixture, "base");
        responses.extend(join_evidence_responses(
            &preflight.case,
            &preflight.fixture,
            "candidate",
        ));
        for _ in 0..8 {
            responses.push(Ok(benchmark_response("join")));
        }
        let client = MockEndpointClient::new(responses);
        measure_with_client(&preflight, &client)
            .await
            .unwrap_or_else(|err| panic!("join measure: {err}"));
        let calls = client.calls.lock().unwrap_or_else(|_| panic!("calls"));
        let first_benchmark = calls
            .iter()
            .position(|call| call.contains("SELECT f.host"))
            .unwrap_or_else(|| panic!("benchmark call"));
        assert_eq!(
            first_benchmark, 18,
            "health plus all eight evidence calls precede queries"
        );
        for target in ["base", "candidate"] {
            for table in ["sql_join_fact", "sql_join_dim"] {
                assert!(calls[..first_benchmark].iter().any(|call| {
                    call == &format!(
                        "statement:{target}:public:SHOW CREATE TABLE \"greptime\".\"public\".\"{table}\""
                    )
                }));
            }
        }
    }

    #[test]
    fn external_prepared_join_rejects_missing_duplicate_extra_and_crossed_bindings() {
        let case = join_case();
        let mutations: [(&str, fn(&mut FixtureManifest)); 5] = [
            ("missing", |fixture: &mut FixtureManifest| {
                fixture.target_bindings.pop();
            }),
            ("duplicate", |fixture: &mut FixtureManifest| {
                fixture
                    .target_bindings
                    .push(fixture.target_bindings[0].clone());
            }),
            ("extra", |fixture: &mut FixtureManifest| {
                let mut extra = fixture.target_bindings[0].clone();
                extra.logical_table = "stale".into();
                fixture.target_bindings.push(extra);
            }),
            ("crossed", |fixture: &mut FixtureManifest| {
                fixture.target_bindings[0].target_id = "candidate".into();
            }),
            ("reordered", |fixture: &mut FixtureManifest| {
                fixture.target_bindings.reverse();
            }),
        ];
        for (name, mutate) in mutations {
            let (targets, mut fixture) = join_targets_and_fixture(&case);
            mutate(&mut fixture);
            fixture
                .seal()
                .unwrap_or_else(|err| panic!("{name} seal: {err}"));
            assert!(
                validate_preflight(join_case(), targets, fixture).is_err(),
                "{name} must fail"
            );
        }
    }

    #[tokio::test]
    async fn external_prepared_join_identity_mutations_fail_before_benchmark_queries() {
        let mutations: [(&str, fn(&mut FixtureManifest)); 4] = [
            ("catalog", |fixture: &mut FixtureManifest| {
                fixture.target_bindings[0].catalog = Some("other".into());
            }),
            ("schema", |fixture: &mut FixtureManifest| {
                fixture.target_bindings[0]
                    .external
                    .as_mut()
                    .unwrap_or_else(|| panic!("external"))
                    .schema = "other".into();
            }),
            ("region", |fixture: &mut FixtureManifest| {
                fixture.target_bindings[0]
                    .external
                    .as_mut()
                    .unwrap_or_else(|| panic!("external"))
                    .region_id = 9;
            }),
            ("time", |fixture: &mut FixtureManifest| {
                fixture.target_bindings[0]
                    .external
                    .as_mut()
                    .unwrap_or_else(|| panic!("external"))
                    .time_index = "host".into();
            }),
        ];
        for (name, mutate) in mutations {
            let (targets, mut fixture) = join_targets_and_fixture(&join_case());
            mutate(&mut fixture);
            fixture
                .seal()
                .unwrap_or_else(|err| panic!("{name} seal: {err}"));
            assert!(
                validate_preflight(join_case(), targets, fixture).is_err(),
                "{name} must fail"
            );
        }

        let case = join_case();
        let (targets, mut fixture) = join_targets_and_fixture(&case);
        fixture.target_bindings[0]
            .external
            .as_mut()
            .unwrap_or_else(|| panic!("external"))
            .table_id = 99;
        fixture
            .seal()
            .unwrap_or_else(|err| panic!("table ID seal: {err}"));
        let preflight = validate_preflight(case, targets, fixture)
            .unwrap_or_else(|err| panic!("table ID preflight: {err}"));
        let client = MockEndpointClient::new(join_evidence_responses(
            &preflight.case,
            &preflight.fixture,
            "base",
        ));
        assert!(measure_with_client(&preflight, &client).await.is_err());
        assert!(
            client
                .calls
                .lock()
                .unwrap_or_else(|_| panic!("calls"))
                .iter()
                .all(|call| !call.contains("SELECT f.host"))
        );
    }

    #[test]
    fn preflight_capability_and_provider_rejections_precede_endpoint_orchestration() {
        struct InvalidInput {
            name: &'static str,
            case: &'static str,
            table: &'static str,
            provider: RunnerFixtureProvider,
            error_category: &'static str,
            mutate: fn(&mut TargetEnvironmentManifest, &mut FixtureManifest),
        }

        let baseline_case = test_case(REMOTE_CASE);
        let baseline_targets = test_targets("m", RunnerFixtureProvider::ApiWrite);
        let baseline_fixture = test_fixture(
            &baseline_case,
            &baseline_targets,
            RunnerFixtureProvider::ApiWrite,
        );
        assert!(validate_preflight(baseline_case, baseline_targets, baseline_fixture,).is_ok());

        let inputs = [
            InvalidInput {
                name: "base SQL capability",
                case: REMOTE_CASE,
                table: "m",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "all SQL and TQL queries require Capability::Sql",
                mutate: remove_sql_from_base,
            },
            InvalidInput {
                name: "candidate SQL capability",
                case: REMOTE_CASE,
                table: "m",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "all SQL and TQL queries require Capability::Sql",
                mutate: remove_sql_from_candidate,
            },
            InvalidInput {
                name: "API write capability",
                case: REMOTE_CASE,
                table: "m",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "remote-write capability and endpoint must agree",
                mutate: remove_remote_write_from_base,
            },
            InvalidInput {
                name: "API write endpoint",
                case: REMOTE_CASE,
                table: "m",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "remote-write capability and endpoint must agree",
                mutate: remove_remote_write_endpoint,
            },
            InvalidInput {
                name: "footer observation capability",
                case: REMOTE_CASE,
                table: "m",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "case observation requires an undeclared target capability",
                mutate: remove_footer_observation,
            },
            InvalidInput {
                name: "read bench capability",
                case: REMOTE_CASE,
                table: "m",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "case observation requires an undeclared target capability",
                mutate: remove_read_bench,
            },
            InvalidInput {
                name: "API write provider with direct case",
                case: DIRECT_CASE,
                table: "t",
                provider: RunnerFixtureProvider::ApiWrite,
                error_category: "case is not a remote-write fixture",
                mutate: |_, _| {},
            },
            InvalidInput {
                name: "external prepared binding",
                case: DIRECT_CASE,
                table: "t",
                provider: RunnerFixtureProvider::ExternalPrepared,
                error_category: "external binding time index is absent from case",
                mutate: mismatch_external_case_binding,
            },
        ];

        for input in inputs {
            let case = test_case(input.case);
            let mut targets = test_targets(input.table, input.provider.clone());
            let mut fixture = test_fixture(&case, &targets, input.provider);
            (input.mutate)(&mut targets, &mut fixture);
            targets
                .seal()
                .unwrap_or_else(|err| panic!("seal targets for {}: {err}", input.name));
            fixture
                .seal()
                .unwrap_or_else(|err| panic!("seal fixture for {}: {err}", input.name));

            let client = MockEndpointClient::new(std::iter::empty());
            let error = validate_preflight(case, targets, fixture)
                .expect_err(input.error_category)
                .to_string();
            assert!(
                error.contains(input.error_category),
                "{} returned unexpected error: {error}",
                input.name
            );
            assert!(
                client
                    .calls
                    .lock()
                    .unwrap_or_else(|_| panic!("calls"))
                    .is_empty(),
                "{} reached endpoint orchestration",
                input.name
            );
        }
    }

    fn external_target() -> TargetBinding {
        TargetBinding {
            role: TargetRole::Base,
            target_id: "base".into(),
            identity: Sha256Digest::compute("id"),
            git_identity: Sha256Digest::compute("git"),
            build_profile: crate::query_perf::manifest::BuildProfile::Release,
            features: vec![],
            capabilities: vec![Capability::Sql],
            health_endpoint: crate::query_perf::manifest::EndpointUrl::parse(
                "http://example.com/health",
            )
            .unwrap_or_else(|err| panic!("endpoint: {err}")),
            sql_endpoint: crate::query_perf::manifest::EndpointUrl::parse("http://example.com/sql")
                .unwrap_or_else(|err| panic!("endpoint: {err}")),
            remote_write_endpoint: None,
            namespace: "ns".into(),
            topology: "single".into(),
            fixture_bindings: vec![],
        }
    }
    fn external_binding() -> crate::query_perf::manifest::FixtureTargetBinding {
        crate::query_perf::manifest::FixtureTargetBinding {
            role: TargetRole::Base,
            target_id: "base".into(),
            catalog: Some("greptime".into()),
            database: "public".into(),
            logical_table: "t".into(),
            physical_table: None,
            region: "0".into(),
            file_ids: vec![],
            external: Some(crate::query_perf::manifest::ExternalPreparedTargetBinding {
                catalog: "greptime".into(),
                schema: "public".into(),
                table_id: 1,
                region_id: 0,
                time_index: "ts".into(),
                time_unit: TimestampUnit::Nanosecond,
            }),
        }
    }
    fn external_responses() -> Vec<Result<serde_json::Value>> {
        vec![
            show_create_response("t", direct_table_ddl("t")),
            Ok(serde_json::json!({"data":[{"table_id":1}]})),
            Ok(serde_json::json!({"data":[{"region_id":0,"is_leader":true,"status":"ALIVE"}]})),
            Ok(serde_json::json!({"data":[{"count":1,"min_time":0,"max_time":0}]})),
        ]
    }

    #[tokio::test]
    async fn external_prepared_evidence_is_exact_and_ordered() {
        let client = MockEndpointClient::new(external_responses());
        verify_external_prepared(
            &client,
            &external_target(),
            &external_binding(),
            test_case(EXTERNAL_MEASURE_CASE)
                .direct_readable_sst()
                .unwrap_or_else(|err| panic!("direct case: {err}"))
                .tables()
                .first()
                .unwrap_or_else(|| panic!("case table")),
            &crate::query_perf::manifest::LogicalFixtureSummary {
                rows: 1,
                series: 1,
                time_start_unix_nanos: 0,
                time_end_unix_nanos: 0,
            },
        )
        .await
        .unwrap_or_else(|err| panic!("evidence: {err}"));
        let calls = client.calls.lock().unwrap_or_else(|_| panic!("calls"));
        assert_eq!(calls.len(), 4);
        assert!(calls[0].contains("SHOW CREATE TABLE \"greptime\".\"public\".\"t\""));
        assert!(calls[1].contains("information_schema.tables"));
        assert!(calls[2].contains("information_schema.region_peers"));
        assert!(calls[3].contains("SELECT COUNT(*) AS count"));
    }

    #[tokio::test]
    async fn external_prepared_mutations_fail_before_queries() {
        for response in [
            serde_json::json!({"data":[{"table_id":2}]}),
            serde_json::json!({"data":[]}),
        ] {
            let mut responses = external_responses();
            responses[1] = Ok(response);
            let client = MockEndpointClient::new(responses);
            assert!(
                verify_external_prepared(
                    &client,
                    &external_target(),
                    &external_binding(),
                    test_case(EXTERNAL_MEASURE_CASE)
                        .direct_readable_sst()
                        .unwrap_or_else(|err| panic!("direct case: {err}"))
                        .tables()
                        .first()
                        .unwrap_or_else(|| panic!("case table")),
                    &crate::query_perf::manifest::LogicalFixtureSummary {
                        rows: 1,
                        series: 1,
                        time_start_unix_nanos: 0,
                        time_end_unix_nanos: 0
                    }
                )
                .await
                .is_err()
            );
            assert!(
                client
                    .calls
                    .lock()
                    .unwrap_or_else(|_| panic!("calls"))
                    .len()
                    <= 2
            );
        }
    }

    #[tokio::test]
    async fn external_prepared_rejects_each_production_show_create_schema_mutation() {
        let case = join_case();
        let (targets, fixture) = join_targets_and_fixture(&case);
        let table = case
            .direct_readable_sst()
            .unwrap_or_else(|err| panic!("direct case: {err}"))
            .tables()
            .first()
            .unwrap_or_else(|| panic!("case table"));
        let binding = fixture
            .target_bindings
            .iter()
            .find(|binding| binding.target_id == "base" && binding.logical_table == table.name())
            .unwrap_or_else(|| panic!("fixture binding"));
        let summary = &fixture.logical_summary;
        let mutations: [(&str, bool, fn(&str) -> String); 8] = [
            ("Table field", true, str::to_string),
            ("column name", false, |ddl| {
                ddl.replacen("\"host\"", "\"hostname\"", 1)
            }),
            ("column type", false, |ddl| {
                ddl.replacen("\"host\" STRING", "\"host\" DOUBLE", 1)
            }),
            ("time index", false, |ddl| {
                ddl.replacen("TIME INDEX (\"ts\")", "TIME INDEX (\"other\")", 1)
            }),
            ("primary key", false, |ddl| {
                ddl.replacen(
                    "PRIMARY KEY (\"host\", \"instance\")",
                    "PRIMARY KEY (\"instance\", \"host\")",
                    1,
                )
            }),
            ("engine", false, |ddl| {
                ddl.replacen("ENGINE=mito", "ENGINE=file", 1)
            }),
            ("append_mode", false, |ddl| {
                ddl.replacen("'append_mode' = 'true'", "'append_mode' = 'false'", 1)
            }),
            ("sst_format", false, |ddl| {
                ddl.replacen("'sst_format' = 'flat'", "'sst_format' = 'primary_key'", 1)
            }),
        ];
        for (name, wrong_table, mutate) in mutations {
            let mut responses = join_evidence_responses(&case, &fixture, "base");
            responses[0] = show_create_response(
                if wrong_table { "stale" } else { table.name() },
                mutate(&direct_table_ddl(table.name())),
            );
            let client = MockEndpointClient::new(responses);
            assert!(
                verify_external_prepared(&client, &targets.targets[0], binding, table, summary)
                    .await
                    .is_err(),
                "{name} mutation must fail"
            );
            assert_eq!(
                client
                    .calls
                    .lock()
                    .unwrap_or_else(|_| panic!("calls"))
                    .len(),
                1,
                "{name} mutation reached non-SHOW CREATE evidence"
            );
        }
    }
    #[test]
    fn canonical_digest_is_ordered_and_stable() {
        let left = serde_json::json!({"b": [2, 1], "a": {"z": 1, "x": 2}});
        let right = serde_json::json!({"a": {"x": 2, "z": 1}, "b": [2, 1]});
        assert_eq!(
            canonical_json_bytes(&left).unwrap_or_default(),
            canonical_json_bytes(&right).unwrap_or_default()
        );
        assert_ne!(
            canonical_json_bytes(&left).unwrap_or_default(),
            canonical_json_bytes(&serde_json::json!({"b": [1, 2], "a": {"x": 2, "z": 1}}))
                .unwrap_or_default()
        );
    }
    #[test]
    fn protocol_envelope_and_median_are_fail_closed() {
        assert!(valid_response_envelope(&serde_json::json!({"data": []})).is_ok());
        assert!(
            valid_response_envelope(&serde_json::json!({"error": "bad", "records": []})).is_err()
        );
        assert!(valid_response_envelope(&serde_json::json!({"ok": true})).is_err());
        assert_eq!(median(&[1.0, 4.0, 2.0, 3.0]).unwrap_or(-1.0), 2.5);
        assert!(median(&[0.0]).is_ok());
        assert!(median(&[f64::NAN]).is_err());
    }
    #[test]
    fn atomic_output_replaces_complete_json() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let output = directory.path().join("report.json");
        write_atomic(&output, b"one").unwrap_or_else(|err| panic!("write: {err}"));
        write_atomic(&output, b"two").unwrap_or_else(|err| panic!("replace: {err}"));
        assert_eq!(fs::read(&output).unwrap_or_default(), b"two");
    }

    #[test]
    fn pair_publisher_rejects_identical_destinations_without_writing() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = directory.path().join("same");
        let publisher_fs = RecordingPublisherFs::new(None, None, None);
        assert!(
            write_pair_with_fs(
                &publisher_fs,
                &path,
                &serde_json::json!({"ok": true}),
                &path,
                b"x"
            )
            .is_err()
        );
        assert!(!path.exists());
        assert!(publisher_fs.operations().is_empty());
    }

    #[test]
    fn pair_publisher_preserves_existing_pair_when_summary_staging_fails() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        fs::write(&report, b"existing report").unwrap_or_else(|err| panic!("report: {err}"));
        fs::write(&summary, b"existing summary").unwrap_or_else(|err| panic!("summary: {err}"));
        let publisher_fs = RecordingPublisherFs::new(Some("summary.md"), None, None);

        let err = write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"ok": true}),
            &summary,
            b"new summary",
        )
        .expect_err("summary staging must fail");

        assert!(err.to_string().contains("failed to stage summary"));
        assert_eq!(fs::read(&report).unwrap_or_default(), b"existing report");
        assert_eq!(fs::read(&summary).unwrap_or_default(), b"existing summary");
        assert_eq!(publisher_fs.operations(), vec!["stage:summary.md"]);
        assert_no_temporary_outputs(directory.path());
    }

    #[test]
    fn pair_publisher_cleans_staged_summary_when_report_staging_fails() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        fs::write(&report, b"existing report").unwrap_or_else(|err| panic!("report: {err}"));
        fs::write(&summary, b"existing summary").unwrap_or_else(|err| panic!("summary: {err}"));
        let publisher_fs = RecordingPublisherFs::new(Some("report.json"), None, None);

        let err = write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"ok": true}),
            &summary,
            b"new summary",
        )
        .expect_err("report staging must fail");

        assert!(err.to_string().contains("failed to stage report"));
        assert_eq!(fs::read(&report).unwrap_or_default(), b"existing report");
        assert_eq!(fs::read(&summary).unwrap_or_default(), b"existing summary");
        assert_eq!(
            publisher_fs.operations(),
            vec!["stage:summary.md", "stage:report.json", "remove:summary.md"]
        );
        assert_no_temporary_outputs(directory.path());
    }

    #[test]
    fn pair_publisher_cleans_both_temps_when_summary_publish_fails() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        fs::write(&report, b"existing report").unwrap_or_else(|err| panic!("report: {err}"));
        fs::write(&summary, b"existing summary").unwrap_or_else(|err| panic!("summary: {err}"));
        let publisher_fs = RecordingPublisherFs::new(None, Some(1), None);

        let err = write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"ok": true}),
            &summary,
            b"new summary",
        )
        .expect_err("summary publication must fail");

        assert!(err.to_string().contains("failed to publish summary"));
        assert_eq!(fs::read(&report).unwrap_or_default(), b"existing report");
        assert_eq!(fs::read(&summary).unwrap_or_default(), b"existing summary");
        assert_eq!(
            publisher_fs.operations(),
            vec![
                "stage:summary.md",
                "stage:report.json",
                "publish:summary.md",
                "remove:summary.md",
                "remove:report.json",
            ]
        );
        assert_no_temporary_outputs(directory.path());
    }

    #[test]
    fn pair_publisher_keeps_old_report_when_report_publish_fails() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        let old_summary = b"old summary";
        let new_summary = b"new summary";
        fs::write(
            &report,
            serde_json::to_vec_pretty(&serde_json::json!({
                "summary_digest": Sha256Digest::compute(old_summary)
            }))
            .unwrap_or_else(|err| panic!("old report: {err}")),
        )
        .unwrap_or_else(|err| panic!("report: {err}"));
        fs::write(&summary, old_summary).unwrap_or_else(|err| panic!("summary: {err}"));
        let publisher_fs = RecordingPublisherFs::new(None, Some(2), None);

        let err = write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"summary_digest": Sha256Digest::compute(new_summary)}),
            &summary,
            new_summary,
        )
        .expect_err("report publication must fail");

        let persisted_report: serde_json::Value =
            serde_json::from_slice(&fs::read(&report).unwrap_or_default())
                .unwrap_or_else(|err| panic!("report JSON: {err}"));
        assert!(err.to_string().contains("failed to publish report"));
        assert_eq!(fs::read(&summary).unwrap_or_default(), new_summary);
        assert_eq!(
            persisted_report["summary_digest"],
            serde_json::json!(Sha256Digest::compute(old_summary).as_str())
        );
        assert_ne!(
            persisted_report["summary_digest"],
            serde_json::json!(Sha256Digest::compute(new_summary).as_str())
        );
        assert_eq!(
            publisher_fs.operations(),
            vec![
                "stage:summary.md",
                "stage:report.json",
                "publish:summary.md",
                "sync_parent:summary.md",
                "publish:report.json",
                "remove:report.json",
            ]
        );
        assert_no_temporary_outputs(directory.path());
    }

    #[test]
    fn pair_publisher_reports_summary_parent_sync_failure_without_rollback() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        fs::write(&report, b"existing report").unwrap_or_else(|err| panic!("report: {err}"));
        fs::write(&summary, b"existing summary").unwrap_or_else(|err| panic!("summary: {err}"));
        let publisher_fs = RecordingPublisherFs::new(None, None, Some(1));

        let err = write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"ok": true}),
            &summary,
            b"new summary",
        )
        .expect_err("summary parent sync must fail");

        assert!(err.to_string().contains("failed to sync summary parent"));
        assert_eq!(fs::read(&report).unwrap_or_default(), b"existing report");
        assert_eq!(fs::read(&summary).unwrap_or_default(), b"new summary");
        assert_eq!(
            publisher_fs.operations(),
            vec![
                "stage:summary.md",
                "stage:report.json",
                "publish:summary.md",
                "sync_parent:summary.md",
                "remove:report.json",
            ]
        );
        assert_no_temporary_outputs(directory.path());
    }

    #[test]
    fn pair_publisher_reports_report_parent_sync_failure_without_rollback() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        let new_summary = b"new summary";
        fs::write(&report, b"existing report").unwrap_or_else(|err| panic!("report: {err}"));
        fs::write(&summary, b"existing summary").unwrap_or_else(|err| panic!("summary: {err}"));
        let publisher_fs = RecordingPublisherFs::new(None, None, Some(2));

        let err = write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"summary_digest": Sha256Digest::compute(new_summary)}),
            &summary,
            new_summary,
        )
        .expect_err("report parent sync must fail");

        assert!(err.to_string().contains("failed to sync report parent"));
        assert_eq!(fs::read(&summary).unwrap_or_default(), new_summary);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&report).unwrap_or_default())
                .unwrap_or_else(|err| panic!("report JSON: {err}"))["summary_digest"],
            serde_json::json!(Sha256Digest::compute(new_summary).as_str())
        );
        assert_eq!(
            publisher_fs.operations(),
            vec![
                "stage:summary.md",
                "stage:report.json",
                "publish:summary.md",
                "sync_parent:summary.md",
                "publish:report.json",
                "sync_parent:report.json",
            ]
        );
        assert_no_temporary_outputs(directory.path());
    }

    #[test]
    fn pair_publisher_leaves_no_temps_after_success() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        let publisher_fs = RecordingPublisherFs::new(None, None, None);
        let summary_bytes = b"summary";
        write_pair_with_fs(
            &publisher_fs,
            &report,
            &serde_json::json!({"summary_digest": Sha256Digest::compute(summary_bytes)}),
            &summary,
            summary_bytes,
        )
        .unwrap_or_else(|err| panic!("publish: {err}"));
        let persisted_summary = fs::read(&summary).unwrap_or_default();
        let persisted_report: serde_json::Value =
            serde_json::from_slice(&fs::read(&report).unwrap_or_default())
                .unwrap_or_else(|err| panic!("report JSON: {err}"));
        assert_eq!(persisted_summary, summary_bytes);
        assert_eq!(
            persisted_report["summary_digest"],
            serde_json::json!(Sha256Digest::compute(&persisted_summary).as_str())
        );
        assert_eq!(
            publisher_fs.operations(),
            vec![
                "stage:summary.md",
                "stage:report.json",
                "publish:summary.md",
                "sync_parent:summary.md",
                "publish:report.json",
                "sync_parent:report.json",
            ]
        );
        assert_no_temporary_outputs(directory.path());
    }

    fn assert_no_temporary_outputs(directory: &Path) {
        assert!(
            fs::read_dir(directory).is_ok_and(|mut entries| entries.all(|entry| !entry
                .is_ok_and(|entry| entry.file_name().to_string_lossy().contains(".tmp"))))
        );
    }

    #[test]
    fn threshold_failed_report_is_published_before_returning_error() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let report = directory.path().join("report.json");
        let summary = directory.path().join("summary.md");
        let result = publish_prepared_report(
            &report,
            &serde_json::json!({"outcome": "failed"}),
            &summary,
            b"# failed\n",
            true,
        );
        assert!(result.is_err());
        assert_eq!(
            fs::read(&report).unwrap_or_default(),
            br#"{
  "outcome": "failed"
}"#
        );
        assert_eq!(fs::read(&summary).unwrap_or_default(), b"# failed\n");
    }

    fn assert_incomplete_evaluation_report(args: &FinalizeArgs) {
        assert!(
            finalize(args.clone()).is_err(),
            "evaluation failure must return nonzero"
        );
        let report: ReportManifest = serde_json::from_slice(
            &fs::read(&args.report).unwrap_or_else(|err| panic!("report read: {err}")),
        )
        .unwrap_or_else(|err| panic!("report parse: {err}"));
        let summary = fs::read(&args.summary).unwrap_or_else(|err| panic!("summary read: {err}"));
        let targets: TargetEnvironmentManifest = serde_json::from_slice(
            &fs::read(&args.targets).unwrap_or_else(|err| panic!("targets read: {err}")),
        )
        .unwrap_or_else(|err| panic!("targets parse: {err}"));
        let fixture: FixtureManifest = serde_json::from_slice(
            &fs::read(&args.fixture).unwrap_or_else(|err| panic!("fixture read: {err}")),
        )
        .unwrap_or_else(|err| panic!("fixture parse: {err}"));
        let measure: MeasureArtifact = serde_json::from_slice(
            &fs::read(&args.measurements).unwrap_or_else(|err| panic!("measurements read: {err}")),
        )
        .unwrap_or_else(|err| panic!("measurements parse: {err}"));
        let observations: ObservationBundle = serde_json::from_slice(
            &fs::read(&args.observations).unwrap_or_else(|err| panic!("observations read: {err}")),
        )
        .unwrap_or_else(|err| panic!("observations parse: {err}"));
        let MeasureArtifact::Success {
            measurement_bundle,
            observation_request,
        } = measure
        else {
            panic!("trusted measurement bundle")
        };
        let case = case::load_case(&args.case).unwrap_or_else(|err| panic!("case load: {err}"));

        assert_eq!(report.outcome, ReportOutcome::Failed);
        assert_eq!(report.evaluation_state, EvaluationState::Incomplete);
        assert!(
            report
                .failure_details
                .iter()
                .any(|failure| failure.stage == ReportFailureStage::Evaluation
                    && !failure.detail.trim().is_empty())
        );
        assert_eq!(report.measurements.len(), 2);
        assert_eq!(report.observations.len(), observations.observations.len());
        report
            .validate_against(
                &case
                    .manifest_queries()
                    .unwrap_or_else(|err| panic!("queries: {err}")),
                &targets,
                &fixture,
                &measurement_bundle.measurements,
                &observations.observations,
            )
            .unwrap_or_else(|err| panic!("incomplete report validation: {err}"));
        report
            .validate_threshold_coverage(&case, &targets)
            .unwrap_or_else(|err| panic!("incomplete coverage: {err}"));
        report
            .validate_bundle_references(Some(&measurement_bundle), Some(&observations))
            .unwrap_or_else(|err| panic!("bundle references: {err}"));
        report
            .validate_summary(summary)
            .unwrap_or_else(|err| panic!("summary digest: {err}"));
        assert_eq!(
            measurement_bundle.observation_request.digest,
            observation_request.completion_digest
        );
    }

    #[test]
    fn finalize_zero_base_query_publishes_sealed_incomplete_evaluation_report() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let args = write_finalize_inputs(directory.path(), FINALIZE_QUERY_CASE, 0.0, 1);
        assert_incomplete_evaluation_report(&args);
    }

    #[test]
    fn finalize_zero_base_footer_publishes_sealed_incomplete_evaluation_report() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let args = write_finalize_inputs(directory.path(), FINALIZE_FOOTER_CASE, 1.0, 0);
        assert_incomplete_evaluation_report(&args);
    }

    #[test]
    fn comparative_footer_rejects_zero_base() {
        assert!(regression_pct(0, 1, 1.0).is_err());
        assert_eq!(regression_pct(10, 12, 25.0).unwrap_or(-1.0), 20.0);
    }

    #[test]
    fn external_timestamp_units_and_evidence_rows_are_strict() {
        assert_eq!(to_nanos(7, TimestampUnit::Nanosecond).unwrap_or(-1), 7);
        assert_eq!(
            to_nanos(7, TimestampUnit::Millisecond).unwrap_or(-1),
            7_000_000
        );
        assert!(to_nanos(i64::MAX, TimestampUnit::Millisecond).is_err());
        let valid = serde_json::json!({"data": [{"count": 3, "min_time": 1, "max_time": 2}]});
        assert!(exact_row(&valid, &["count", "min_time", "max_time"]).is_ok());
        assert!(
            exact_row(
                &serde_json::json!({"data": [{"count": 3}, {"count": 3}]}),
                &["count"]
            )
            .is_err()
        );
        assert!(
            exact_row(
                &serde_json::json!({"data": [{"count": 3, "other": 1}]}),
                &["count"]
            )
            .is_err()
        );
    }
}
