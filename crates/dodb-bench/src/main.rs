use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dodb_client::{ClientError, ClientTlsConfig, DodbClient};
use dodb_core::{
    DocumentKey, Error, PrimaryKey, Revision, ShardId, TenantId, TransactionCondition,
    TransactionMutation, TransactionRequest,
};
use dodb_protocol::ApplicationErrorKind;
use dodb_server::{
    DodbServer, DodbServerConfig, LocalTenantService, LocalTenantServiceConfig,
    ServerMetricsSnapshot, ServerTlsConfig,
};
use dodb_service::{DodbService, ExecutionBudget, Request};
use dodb_storage::{
    AsyncShard, BTreeStore, BatchRequest, CoordinatorConfig, CoordinatorMetrics, DatabaseConfig,
    InvariantReport, ProductionFile, StorageMetrics, WalMetrics,
};
use rcgen::generate_simple_self_signed;
use serde::Serialize;

const TENANT: TenantId = TenantId::new(1);
const DEFAULT_KEY_COUNT: usize = 4_096;
const LARGE_VALUE_SIZE: usize = 65_536;
const WARMUP_MS: u64 = 100;
const MEASURE_MS: u64 = 350;
const RAW_SAMPLE_LIMIT: usize = 1_024;
const REQUEST_BUDGET: usize = 68 * 1024 * 1024;
static PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
struct Settings {
    warmup: Duration,
    measure: Duration,
}

#[derive(Clone, Debug)]
struct Dataset {
    key_count: usize,
    value_size: usize,
    keys: Vec<DocumentKey>,
    initial_hot_revision: Revision,
}

#[derive(Clone, Debug)]
struct SeededDatabase {
    dir: PathBuf,
    db_path: PathBuf,
    logical_key_count: usize,
    db_size_bytes: u64,
    invariant: InvariantReport,
    hot_revision: Revision,
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    GetHot,
    GetDistributed,
    GetMissing,
    PutOverwrite,
    DeleteReinsert,
    TransactionOne,
    TransactionFourConditions,
    TransactionFourMutations,
    HighConflict,
    Query10,
    Query100,
    Scan10,
    Scan100,
    Mixed90Get10Put,
    Mixed50Get50Put,
    TransactionHeavy,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::GetHot => "get_existing_hot",
            Self::GetDistributed => "get_existing_distributed",
            Self::GetMissing => "get_missing",
            Self::PutOverwrite => "put_overwrite",
            Self::DeleteReinsert => "delete_reinsert_bounded",
            Self::TransactionOne => "transact_1_condition_1_mutation",
            Self::TransactionFourConditions => "transact_4_conditions_4_mutations",
            Self::TransactionFourMutations => "transact_4_mutations",
            Self::HighConflict => "transact_high_conflict",
            Self::Query10 => "query_limit_10",
            Self::Query100 => "query_limit_100",
            Self::Scan10 => "scan_limit_10",
            Self::Scan100 => "scan_limit_100",
            Self::Mixed90Get10Put => "mixed_90_get_10_put",
            Self::Mixed50Get50Put => "mixed_50_get_50_put",
            Self::TransactionHeavy => "mixed_transaction_heavy",
        }
    }

    fn is_expected_conflict(self) -> bool {
        matches!(self, Self::HighConflict)
    }

    fn uses_global_schedule(self) -> bool {
        matches!(
            self,
            Self::Mixed90Get10Put | Self::Mixed50Get50Put | Self::TransactionHeavy
        )
    }
}

#[derive(Clone, Debug)]
enum Plan {
    Get(DocumentKey),
    Put(DocumentKey, Vec<u8>),
    Delete(DocumentKey),
    Query(PrimaryKey, usize),
    Scan(usize),
    Transact(TransactionRequest),
}

#[derive(Clone, Debug)]
enum CallStatus {
    Success,
    Conflict,
    Error(String),
}

#[derive(Clone, Debug, Default)]
struct CallSummary {
    status: Option<CallStatus>,
    requests: u64,
}

#[derive(Clone, Debug, Default)]
struct Accumulator {
    operations: u64,
    requests: u64,
    successes: u64,
    conflicts: u64,
    errors: u64,
    latencies_ns: Vec<u64>,
    first_error: Option<String>,
}

impl Accumulator {
    fn record(&mut self, summary: CallSummary, elapsed: Duration) {
        self.operations = self.operations.saturating_add(1);
        self.requests = self.requests.saturating_add(summary.requests);
        match summary.status {
            Some(CallStatus::Success) | None => self.successes = self.successes.saturating_add(1),
            Some(CallStatus::Conflict) => self.conflicts = self.conflicts.saturating_add(1),
            Some(CallStatus::Error(error)) => {
                self.errors = self.errors.saturating_add(1);
                if self.first_error.is_none() {
                    self.first_error = Some(error);
                }
            }
        }
        self.latencies_ns
            .push(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
    }

    fn merge(&mut self, other: Self) {
        self.operations = self.operations.saturating_add(other.operations);
        self.requests = self.requests.saturating_add(other.requests);
        self.successes = self.successes.saturating_add(other.successes);
        self.conflicts = self.conflicts.saturating_add(other.conflicts);
        self.errors = self.errors.saturating_add(other.errors);
        self.latencies_ns.extend(other.latencies_ns);
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct LatencySummary {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
    samples: usize,
    raw_samples_us: Vec<f64>,
}

#[derive(Clone, Debug, Serialize)]
struct StorageMetricReport {
    validation_us: f64,
    btree_preparation_us: f64,
    publication_us: f64,
}

#[derive(Clone, Debug, Serialize)]
struct WalMetricReport {
    wal_bytes: u64,
    wal_bytes_per_sync: f64,
    syncs: u64,
    committed_batches: u64,
    page_images: u64,
    page_images_per_sync: f64,
    append_us_total: f64,
    append_us_per_commit: f64,
    sync_us_total: f64,
    sync_us_per_sync: f64,
    commits_per_sync: f64,
}

#[derive(Clone, Debug, Serialize)]
struct CoordinatorMetricReport {
    groups: u64,
    queued_requests: u64,
    logical_transactions: u64,
    overloaded_requests: u64,
    mean_group_requests: f64,
    p50_group_requests: f64,
    p95_group_requests: f64,
    max_group_requests: usize,
    max_group_bytes: usize,
    queue_wait_us_per_request: f64,
    collection_us_per_group: f64,
    processing_us_per_group: f64,
}

#[derive(Clone, Debug, Serialize)]
struct ServerMetricReport {
    connections_total: u64,
    requests_total: u64,
    request_bytes: u64,
    response_bytes: u64,
    protocol_errors: u64,
    transport_errors: u64,
    application_errors: u64,
    overloaded_responses: u64,
    server_request_latency_us: f64,
    operation_counts: [u64; 8],
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkResult {
    layer: String,
    workload: String,
    value_size: usize,
    logical_key_count: usize,
    cache_capacity: Option<usize>,
    concurrency: usize,
    warmup_ms: u64,
    measurement_ms: u64,
    logical_operations: u64,
    protocol_or_engine_requests: u64,
    successful_operations: u64,
    conflicts: u64,
    errors: u64,
    throughput_ops_per_sec: f64,
    throughput_requests_per_sec: f64,
    latency: LatencySummary,
    db_size_bytes: u64,
    reachable_pages: usize,
    storage: Option<StorageMetricReport>,
    wal: Option<WalMetricReport>,
    coordinator: Option<CoordinatorMetricReport>,
    server: Option<ServerMetricReport>,
    first_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceSnapshot {
    vm_peak_kib: Option<u64>,
    vm_hwm_kib: Option<u64>,
    vm_rss_kib: Option<u64>,
    read_bytes: Option<u64>,
    write_bytes: Option<u64>,
    cgroup_memory_current: Option<u64>,
    cgroup_memory_peak: Option<u64>,
    cgroup_memory_high_events: Option<u64>,
    cgroup_memory_max_events: Option<u64>,
    cgroup_oom_kills: Option<u64>,
    load_average: String,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkEnvironment {
    git_sha: String,
    release_build: bool,
    cpu_model: String,
    logical_cpu_count: usize,
    allocated_logical_cpus: String,
    allocated_physical_cores: String,
    cpu_affinity: String,
    cargo_build_jobs: String,
    memory_high: String,
    memory_max: String,
    memory_swap_max: String,
    cgroup_path: String,
    memory_total_kib: Option<u64>,
    os_kernel: String,
    filesystem_device: String,
    initial_swap_used_kib: Option<u64>,
    started_unix_ms: u128,
    resources_before: ResourceSnapshot,
    resources_after: ResourceSnapshot,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkReport {
    environment: BenchmarkEnvironment,
    datasets: Vec<DatasetReport>,
    results: Vec<BenchmarkResult>,
    notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct DatasetReport {
    layer: String,
    value_size: usize,
    logical_key_count: usize,
    cache_capacity: Option<usize>,
    db_size_bytes: u64,
    reachable_pages: usize,
}

#[derive(Clone, Debug)]
struct LiveMetrics {
    storage: Option<StorageMetrics>,
    wal: Option<WalMetrics>,
    coordinator: Option<CoordinatorMetrics>,
    server: Option<ServerMetricsSnapshot>,
}

#[derive(Clone)]
enum AsyncBackend {
    Coordinator(Arc<AsyncShard<ProductionFile, ProductionFile>>),
    Service(Arc<LocalTenantService>),
    Quic {
        client: DodbClient,
        server: Arc<DodbServer<LocalTenantService>>,
    },
}

struct AsyncEnvironment {
    backend: AsyncBackend,
    seeded: SeededDatabase,
}

impl AsyncBackend {
    async fn call(&self, plan: Plan) -> CallSummary {
        let status = match self {
            Self::Coordinator(shard) => execute_coordinator(shard, plan).await,
            Self::Service(service) => execute_service(service, plan).await,
            Self::Quic { client, .. } => execute_quic(client, plan).await,
        };
        CallSummary {
            status: Some(status),
            requests: 1,
        }
    }

    fn metrics(&self) -> LiveMetrics {
        match self {
            Self::Coordinator(shard) => LiveMetrics {
                storage: shard.storage_metrics(),
                wal: shard.wal_metrics(),
                coordinator: Some(shard.coordinator_metrics()),
                server: None,
            },
            Self::Service(_) => LiveMetrics {
                storage: None,
                wal: None,
                coordinator: None,
                server: None,
            },
            Self::Quic { server, .. } => LiveMetrics {
                storage: None,
                wal: None,
                coordinator: None,
                server: Some(server.metrics().snapshot()),
            },
        }
    }

    async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Coordinator(shard) => shard.shutdown().await?,
            Self::Service(service) => service.shutdown().await,
            Self::Quic { client, server } => {
                client.connection().close();
                server.shutdown().await;
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = parse_output_path();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async_main(output))
}

async fn async_main(output: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings {
        warmup: Duration::from_millis(parse_env_u64("DODB_BENCH_WARMUP_MS", WARMUP_MS)),
        measure: Duration::from_millis(parse_env_u64("DODB_BENCH_MEASURE_MS", MEASURE_MS)),
    };
    let resources_before = resource_snapshot();
    let mut report = BenchmarkReport {
        environment: environment(resources_before.clone(), resources_before.clone()),
        datasets: Vec::new(),
        results: Vec::new(),
        notes: vec![
            "Primary writes use the real WAL and normal sync_data durability point.".to_owned(),
            "Direct, coordinator, service, and QUIC results use separate fresh preloaded databases.".to_owned(),
            "The service layer is in-process LocalTenantService without protocol framing or QUIC.".to_owned(),
            "CPU attribution uses storage/coordinator/server timing counters because perf is unavailable.".to_owned(),
        ],
    };

    run_direct_suite(&settings, &mut report)?;
    run_async_suite(&settings, &mut report, "coordinator", true, 512).await?;
    run_async_suite(&settings, &mut report, "service", false, 512).await?;
    run_async_suite(&settings, &mut report, "quic", true, 512).await?;
    run_async_suite(&settings, &mut report, "quic", false, LARGE_VALUE_SIZE).await?;

    let resources_after = resource_snapshot();
    report.environment = environment(report.environment.resources_before.clone(), resources_after);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "wrote {} benchmark results to {}",
        report.results.len(),
        output.display()
    );
    print_summary(&report);
    Ok(())
}

fn run_direct_suite(
    settings: &Settings,
    report: &mut BenchmarkReport,
) -> Result<(), Box<dyn std::error::Error>> {
    let full = [
        Workload::GetHot,
        Workload::GetDistributed,
        Workload::GetMissing,
        Workload::PutOverwrite,
        Workload::DeleteReinsert,
        Workload::TransactionOne,
        Workload::TransactionFourConditions,
        Workload::TransactionFourMutations,
        Workload::Query10,
        Workload::Query100,
        Workload::Scan10,
        Workload::Scan100,
    ];
    for (value_size, cache_capacity) in [(512, 256), (512, 0), (64, 256), (4_096, 256)] {
        let key_count = DEFAULT_KEY_COUNT;
        let seeded = seed_database(value_size, key_count, cache_capacity)?;
        report.datasets.push(dataset_report(
            "storage",
            value_size,
            Some(cache_capacity),
            &seeded,
        ));
        let mut store = BTreeStore::<ProductionFile, ProductionFile>::open_path(
            &seeded.db_path,
            benchmark_database_config(cache_capacity),
        )?;
        let workloads: Vec<Workload> = if value_size == 512 && cache_capacity == 256 {
            full.to_vec()
        } else {
            vec![Workload::GetDistributed, Workload::PutOverwrite]
        };
        for workload in workloads {
            let before = direct_metrics(&store)?;
            let accumulator = measure_direct(
                &mut store,
                &Dataset::new(key_count, value_size).with_hot_revision(seeded.hot_revision),
                workload,
                *settings,
            )?;
            let after = direct_metrics(&store)?;
            report.results.push(make_result(
                "storage",
                workload,
                value_size,
                key_count,
                Some(cache_capacity),
                1,
                *settings,
                accumulator,
                seeded.db_size_bytes,
                seeded.invariant.reachable_pages,
                Some(storage_report_delta(before.0, after.0)),
                wal_report_delta(before.1, after.1),
                None,
                None,
            ));
        }
        let invariant = store.check_invariants()?;
        if !invariant.leaked_pages.is_empty() {
            return Err(format!(
                "direct benchmark invariant leak: {:?}",
                invariant.leaked_pages
            )
            .into());
        }
        drop(store);
        fs::remove_dir_all(&seeded.dir)?;
    }

    let seeded = seed_database(LARGE_VALUE_SIZE, 256, 256)?;
    report.datasets.push(dataset_report(
        "storage",
        LARGE_VALUE_SIZE,
        Some(256),
        &seeded,
    ));
    let mut store = BTreeStore::<ProductionFile, ProductionFile>::open_path(
        &seeded.db_path,
        benchmark_database_config(256),
    )?;
    let before = direct_metrics(&store)?;
    let accumulator = measure_direct(
        &mut store,
        &Dataset::new(256, LARGE_VALUE_SIZE).with_hot_revision(seeded.hot_revision),
        Workload::PutOverwrite,
        *settings,
    )?;
    let after = direct_metrics(&store)?;
    report.results.push(make_result(
        "storage",
        Workload::PutOverwrite,
        LARGE_VALUE_SIZE,
        256,
        Some(256),
        1,
        *settings,
        accumulator,
        seeded.db_size_bytes,
        seeded.invariant.reachable_pages,
        Some(storage_report_delta(before.0, after.0)),
        wal_report_delta(before.1, after.1),
        None,
        None,
    ));
    drop(store);
    fs::remove_dir_all(&seeded.dir)?;
    Ok(())
}

async fn run_async_suite(
    settings: &Settings,
    report: &mut BenchmarkReport,
    layer: &'static str,
    full_matrix: bool,
    value_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let key_count = if value_size == LARGE_VALUE_SIZE {
        256
    } else {
        DEFAULT_KEY_COUNT
    };
    let environment = setup_async_environment(layer, value_size, key_count).await?;
    report
        .datasets
        .push(dataset_report(layer, value_size, None, &environment.seeded));
    let concurrency = [1, 4, 16, 64];
    let workloads = if full_matrix {
        let mut cases = vec![(Workload::HighConflict, 16)];
        for workload in [
            Workload::GetDistributed,
            Workload::PutOverwrite,
            Workload::Mixed90Get10Put,
            Workload::Mixed50Get50Put,
            Workload::TransactionHeavy,
        ] {
            for level in concurrency {
                cases.push((workload, level));
            }
        }
        for workload in [
            Workload::GetHot,
            Workload::GetMissing,
            Workload::Query10,
            Workload::Query100,
            Workload::Scan10,
            Workload::Scan100,
        ] {
            cases.push((workload, 16));
        }
        for workload in [
            Workload::DeleteReinsert,
            Workload::TransactionOne,
            Workload::TransactionFourConditions,
            Workload::TransactionFourMutations,
        ] {
            for level in concurrency {
                cases.push((workload, level));
            }
        }
        cases
    } else {
        [
            (Workload::GetDistributed, 1),
            (Workload::GetDistributed, 16),
            (Workload::GetDistributed, 64),
            (Workload::PutOverwrite, 1),
            (Workload::PutOverwrite, 16),
            (Workload::PutOverwrite, 64),
            (Workload::TransactionOne, 16),
            (Workload::Query100, 16),
            (Workload::Scan100, 16),
        ]
        .into_iter()
        .collect()
    };
    for (workload, level) in workloads {
        let before = environment.backend.metrics();
        let accumulator = measure_async(
            &environment.backend,
            &Dataset::new(key_count, value_size).with_hot_revision(environment.seeded.hot_revision),
            workload,
            level,
            *settings,
        )
        .await?;
        let after = environment.backend.metrics();
        let (storage, wal, coordinator, server) = metric_reports(before, after);
        report.results.push(make_result(
            layer,
            workload,
            value_size,
            key_count,
            None,
            level,
            *settings,
            accumulator,
            environment.seeded.db_size_bytes,
            environment.seeded.invariant.reachable_pages,
            storage,
            wal,
            coordinator,
            server,
        ));
    }
    environment.backend.shutdown().await?;
    fs::remove_dir_all(&environment.seeded.dir)?;
    Ok(())
}

async fn setup_async_environment(
    layer: &'static str,
    value_size: usize,
    key_count: usize,
) -> Result<AsyncEnvironment, Box<dyn std::error::Error>> {
    let seeded = seed_database(value_size, key_count, 256)?;
    let backend = match layer {
        "coordinator" => {
            let store = BTreeStore::<ProductionFile, ProductionFile>::open_path(
                &seeded.db_path,
                benchmark_database_config(256),
            )?;
            AsyncBackend::Coordinator(Arc::new(AsyncShard::start_with_config(
                store,
                CoordinatorConfig {
                    queue_capacity: 1_024,
                    max_group_requests: 64,
                    ..CoordinatorConfig::default()
                },
            )))
        }
        "service" => AsyncBackend::Service(Arc::new(LocalTenantService::new(
            LocalTenantServiceConfig {
                data_dir: seeded.dir.clone(),
                database_config: benchmark_database_config(256),
                coordinator_config: CoordinatorConfig {
                    queue_capacity: 1_024,
                    ..CoordinatorConfig::default()
                },
                ..LocalTenantServiceConfig::default()
            },
        )?)),
        "quic" => {
            let service = Arc::new(LocalTenantService::new(LocalTenantServiceConfig {
                data_dir: seeded.dir.clone(),
                database_config: benchmark_database_config(256),
                coordinator_config: CoordinatorConfig {
                    queue_capacity: 1_024,
                    ..CoordinatorConfig::default()
                },
                ..LocalTenantServiceConfig::default()
            })?);
            let certificate = generate_simple_self_signed(vec!["localhost".to_owned()])?;
            let server = Arc::new(DodbServer::bind(
                Arc::clone(&service),
                DodbServerConfig {
                    listen_addr: "127.0.0.1:0".parse()?,
                    tls: ServerTlsConfig::from_der(
                        vec![certificate.cert.der().to_vec()],
                        certificate.signing_key.serialize_der(),
                    )?,
                    protocol_limits: dodb_protocol::ProtocolLimits::default(),
                    max_connections: 2,
                    max_concurrent_streams: 4_096,
                    max_concurrent_requests: 4_096,
                },
            )?);
            let task_server = Arc::clone(&server);
            tokio::spawn(async move { task_server.run().await });
            let client = DodbClient::connect(
                "0.0.0.0:0".parse()?,
                server.local_addr()?,
                "localhost",
                TENANT,
                ClientTlsConfig::from_der(vec![certificate.cert.der().to_vec()])?,
                dodb_protocol::ProtocolLimits::default(),
            )
            .await?;
            AsyncBackend::Quic { client, server }
        }
        other => return Err(format!("unknown benchmark layer {other}").into()),
    };
    Ok(AsyncEnvironment { backend, seeded })
}

fn measure_direct(
    store: &mut BTreeStore<ProductionFile, ProductionFile>,
    dataset: &Dataset,
    workload: Workload,
    settings: Settings,
) -> Result<Accumulator, Box<dyn std::error::Error>> {
    let warmup_deadline = Instant::now() + settings.warmup;
    while Instant::now() < warmup_deadline {
        let summary = execute_direct(store, plan_sequence(dataset, workload, 0, 0))?;
        if let Some(CallStatus::Error(error)) = summary.status {
            return Err(format!("direct benchmark warmup returned an error: {error}").into());
        }
    }
    let deadline = Instant::now() + settings.measure;
    let mut accumulator = Accumulator::default();
    let mut operation = 0u64;
    while Instant::now() < deadline {
        let started = Instant::now();
        let summary = execute_direct(
            store,
            plan_sequence(dataset, workload, 0, operation as usize),
        )?;
        accumulator.record(summary, started.elapsed());
        operation = operation.saturating_add(1);
    }
    Ok(accumulator)
}

async fn measure_async(
    backend: &AsyncBackend,
    dataset: &Dataset,
    workload: Workload,
    concurrency: usize,
    settings: Settings,
) -> Result<Accumulator, Box<dyn std::error::Error>> {
    let concurrency = concurrency.max(1);
    let warmup_deadline = Instant::now() + settings.warmup;
    let mut warmup_tasks = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let backend = backend.clone();
        let dataset = dataset.clone();
        warmup_tasks.push(tokio::spawn(async move {
            let mut operation = worker;
            let warmup_workload = if workload.is_expected_conflict() {
                Workload::GetDistributed
            } else {
                workload
            };
            while Instant::now() < warmup_deadline {
                let plans = plan_sequence(&dataset, warmup_workload, worker, operation);
                for plan in plans {
                    let summary = backend.call(plan).await;
                    if let Some(CallStatus::Error(error)) = summary.status
                        && !workload.is_expected_conflict()
                    {
                        return Some(format!("warmup request failed: {error}"));
                    }
                }
                operation = operation.saturating_add(concurrency);
            }
            None
        }));
    }
    for task in warmup_tasks {
        if let Some(error) = task.await? {
            return Err(error.into());
        }
    }

    let deadline = Instant::now() + settings.measure;
    let schedule_ticket = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let backend = backend.clone();
        let dataset = dataset.clone();
        let schedule_ticket = Arc::clone(&schedule_ticket);
        tasks.push(tokio::spawn(async move {
            let mut local = Accumulator::default();
            let mut operation = worker;
            while Instant::now() < deadline {
                let started = Instant::now();
                let mut combined = CallSummary::default();
                let mut error = None;
                let selection = if workload.uses_global_schedule() {
                    schedule_ticket.fetch_add(1, Ordering::Relaxed) as usize
                } else {
                    operation
                };
                for plan in plan_sequence(&dataset, workload, worker, selection) {
                    let summary = backend.call(plan).await;
                    combined.requests = combined.requests.saturating_add(summary.requests);
                    if matches!(summary.status, Some(CallStatus::Conflict)) {
                        combined.status = Some(CallStatus::Conflict);
                        break;
                    }
                    if let Some(CallStatus::Error(message)) = summary.status {
                        error = Some(message);
                        break;
                    }
                    combined.status = Some(CallStatus::Success);
                }
                if let Some(error) = error {
                    combined.status = Some(CallStatus::Error(error));
                }
                local.record(combined, started.elapsed());
                operation = operation.saturating_add(concurrency);
            }
            local
        }));
    }
    let mut accumulator = Accumulator::default();
    for task in tasks {
        accumulator.merge(task.await?);
    }
    Ok(accumulator)
}

fn execute_direct(
    store: &mut BTreeStore<ProductionFile, ProductionFile>,
    plans: Vec<Plan>,
) -> Result<CallSummary, Box<dyn std::error::Error>> {
    let mut summary = CallSummary {
        status: Some(CallStatus::Success),
        requests: 0,
    };
    for plan in plans {
        summary.requests = summary.requests.saturating_add(1);
        let result = match plan {
            Plan::Get(key) => store.get(&key).map(|_| ()),
            Plan::Put(key, value) => store.put(key, value).map(|_| ()),
            Plan::Delete(key) => store.delete(key).map(|_| ()),
            Plan::Query(pk, limit) => store.query(&pk, None, limit).map(|_| ()),
            Plan::Scan(limit) => store.scan(None, limit).map(|_| ()),
            Plan::Transact(request) => store.transact(request).map(|_| ()),
        };
        if let Err(error) = result {
            summary.status = Some(match error {
                Error::Conflict(_) => CallStatus::Conflict,
                other => CallStatus::Error(other.to_string()),
            });
            break;
        }
    }
    Ok(summary)
}

async fn execute_coordinator(
    shard: &Arc<AsyncShard<ProductionFile, ProductionFile>>,
    plan: Plan,
) -> CallStatus {
    let result = match plan {
        Plan::Get(key) => shard.execute(BatchRequest::Get { key }).await.map(|_| ()),
        Plan::Put(key, value) => shard
            .execute(BatchRequest::Put { key, value })
            .await
            .map(|_| ()),
        Plan::Delete(key) => shard
            .execute(BatchRequest::Delete { key })
            .await
            .map(|_| ()),
        Plan::Query(pk, limit) => shard
            .execute(BatchRequest::Query {
                pk,
                exclusive_after_sk: None,
                limit,
            })
            .await
            .map(|_| ()),
        Plan::Scan(limit) => shard
            .execute(BatchRequest::Scan {
                exclusive_after_key: None,
                limit,
            })
            .await
            .map(|_| ()),
        Plan::Transact(request) => shard.execute_transaction(request).await.map(|_| ()),
    };
    core_status(result)
}

async fn execute_service(service: &Arc<LocalTenantService>, plan: Plan) -> CallStatus {
    let request = plan_to_service_request(plan);
    let result = service
        .execute(TENANT, request, ExecutionBudget::new(REQUEST_BUDGET))
        .await
        .map(|_| ());
    core_status(result)
}

async fn execute_quic(client: &DodbClient, plan: Plan) -> CallStatus {
    let result: Result<(), ClientError> = match plan {
        Plan::Get(key) => client.get(key).await.map(|_| ()),
        Plan::Put(key, value) => client.put(key, value).await.map(|_| ()),
        Plan::Delete(key) => client.delete(key).await.map(|_| ()),
        Plan::Query(pk, limit) => client.query(pk, None, limit).await.map(|_| ()),
        Plan::Scan(limit) => client.scan(None, limit).await.map(|_| ()),
        Plan::Transact(request) => client.transact(request).await.map(|_| ()),
    };
    match result {
        Ok(()) => CallStatus::Success,
        Err(ClientError::Application(error)) if error.kind == ApplicationErrorKind::Conflict => {
            CallStatus::Conflict
        }
        Err(error) => CallStatus::Error(error.to_string()),
    }
}

fn core_status(result: dodb_core::Result<()>) -> CallStatus {
    match result {
        Ok(()) => CallStatus::Success,
        Err(Error::Conflict(_)) => CallStatus::Conflict,
        Err(error) => CallStatus::Error(error.to_string()),
    }
}

fn plan_to_service_request(plan: Plan) -> Request {
    match plan {
        Plan::Get(key) => Request::Get { key },
        Plan::Put(key, value) => Request::Put { key, value },
        Plan::Delete(key) => Request::Delete { key },
        Plan::Query(pk, limit) => Request::Query {
            pk,
            exclusive_after_sk: None,
            limit,
        },
        Plan::Scan(limit) => Request::Scan {
            exclusive_after_key: None,
            limit,
        },
        Plan::Transact(request) => Request::Transact { request },
    }
}

fn plan_sequence(
    dataset: &Dataset,
    workload: Workload,
    worker: usize,
    operation: usize,
) -> Vec<Plan> {
    match workload {
        Workload::DeleteReinsert => vec![
            Plan::Delete(dataset.key(worker_key_index(dataset, worker, operation))),
            Plan::Put(
                dataset.key(worker_key_index(dataset, worker, operation)),
                value_bytes(dataset.value_size, operation),
            ),
        ],
        Workload::Mixed90Get10Put => {
            if operation.is_multiple_of(10) {
                vec![Plan::Put(
                    dataset.key(worker_key_index(dataset, worker, operation)),
                    value_bytes(dataset.value_size, operation),
                )]
            } else {
                vec![Plan::Get(distributed_key(dataset, worker, operation))]
            }
        }
        Workload::Mixed50Get50Put => {
            if operation.is_multiple_of(2) {
                vec![Plan::Put(
                    dataset.key(worker_key_index(dataset, worker, operation)),
                    value_bytes(dataset.value_size, operation),
                )]
            } else {
                vec![Plan::Get(distributed_key(dataset, worker, operation))]
            }
        }
        Workload::TransactionHeavy => {
            if operation.is_multiple_of(2) {
                plan_sequence(dataset, Workload::TransactionOne, worker, operation)
            } else {
                vec![Plan::Get(distributed_key(dataset, worker, operation))]
            }
        }
        Workload::GetHot => vec![Plan::Get(dataset.key(0))],
        Workload::GetDistributed => vec![Plan::Get(distributed_key(dataset, worker, operation))],
        Workload::GetMissing => vec![Plan::Get(missing_key(dataset, worker, operation))],
        Workload::PutOverwrite => vec![Plan::Put(
            dataset.key(worker_key_index(dataset, worker, operation)),
            value_bytes(dataset.value_size, operation),
        )],
        Workload::TransactionOne => vec![Plan::Transact(TransactionRequest::new(
            vec![TransactionCondition::Exists {
                key: dataset.key(0),
            }],
            vec![TransactionMutation::Put {
                key: transaction_key(dataset, worker, operation, 0),
                value: value_bytes(dataset.value_size, operation),
            }],
        ))],
        Workload::TransactionFourConditions => vec![Plan::Transact(TransactionRequest::new(
            (0..4)
                .map(|index| TransactionCondition::Exists {
                    key: dataset.key(index),
                })
                .collect(),
            (0..4)
                .map(|offset| TransactionMutation::Put {
                    key: transaction_key(dataset, worker, operation, offset),
                    value: value_bytes(dataset.value_size, operation + offset),
                })
                .collect(),
        ))],
        Workload::TransactionFourMutations => vec![Plan::Transact(TransactionRequest::new(
            Vec::new(),
            (0..4)
                .map(|offset| TransactionMutation::Put {
                    key: transaction_key(dataset, worker, operation, offset),
                    value: value_bytes(dataset.value_size, operation + offset),
                })
                .collect(),
        ))],
        Workload::HighConflict => vec![Plan::Transact(TransactionRequest::new(
            vec![TransactionCondition::RevisionEquals {
                key: dataset.key(0),
                expected_revision: dataset.initial_hot_revision,
            }],
            vec![TransactionMutation::Put {
                key: dataset.key(0),
                value: value_bytes(dataset.value_size, operation),
            }],
        ))],
        Workload::Query10 => vec![Plan::Query(PrimaryKey::new(0u32.to_le_bytes()), 10)],
        Workload::Query100 => vec![Plan::Query(PrimaryKey::new(0u32.to_le_bytes()), 100)],
        Workload::Scan10 => vec![Plan::Scan(10)],
        Workload::Scan100 => vec![Plan::Scan(100)],
    }
}

fn worker_key_index(dataset: &Dataset, worker: usize, operation: usize) -> usize {
    (worker
        .wrapping_mul(131)
        .wrapping_add(operation.wrapping_mul(17)))
        % dataset.key_count
}

fn transaction_key(
    dataset: &Dataset,
    worker: usize,
    operation: usize,
    offset: usize,
) -> DocumentKey {
    let span = dataset.key_count.saturating_sub(104).max(1);
    dataset.key(
        100 + (worker
            .wrapping_mul(131)
            .wrapping_add(operation.wrapping_mul(17) + offset)
            % span),
    )
}

fn distributed_key(dataset: &Dataset, worker: usize, operation: usize) -> DocumentKey {
    dataset.key(worker_key_index(dataset, worker, operation))
}

fn missing_key(dataset: &Dataset, worker: usize, operation: usize) -> DocumentKey {
    key_for_index(
        dataset
            .key_count
            .saturating_add(worker.wrapping_mul(131).wrapping_add(operation)),
    )
}

impl Dataset {
    fn new(key_count: usize, value_size: usize) -> Self {
        Self {
            key_count,
            value_size,
            keys: (0..key_count).map(key_for_index).collect(),
            initial_hot_revision: Revision::ZERO,
        }
    }

    fn with_hot_revision(mut self, revision: Revision) -> Self {
        self.initial_hot_revision = revision;
        self
    }

    fn key(&self, index: usize) -> DocumentKey {
        self.keys[index % self.keys.len()].clone()
    }
}

fn key_for_index(index: usize) -> DocumentKey {
    DocumentKey::new(
        (index % 64).to_le_bytes().to_vec(),
        (index as u64).to_le_bytes().to_vec(),
    )
}

fn value_bytes(length: usize, seed: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(seed.wrapping_mul(17))) % 251) as u8)
        .collect()
}

fn seed_database(
    value_size: usize,
    key_count: usize,
    cache_capacity: usize,
) -> Result<SeededDatabase, Box<dyn std::error::Error>> {
    let id = PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let root = std::env::var_os("DODB_BENCH_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = root.join(format!("dodb-baseline-{}-{id}", std::process::id()));
    fs::create_dir_all(&dir)?;
    let db_path = dir.join("tenant-1-shard-1.db");
    let dataset = Dataset::new(key_count, value_size);
    let mut store = BTreeStore::<ProductionFile, ProductionFile>::open_path(
        &db_path,
        benchmark_database_config(cache_capacity),
    )?;
    for chunk in dataset.keys.chunks(256) {
        let requests = chunk
            .iter()
            .enumerate()
            .map(|(offset, key)| BatchRequest::Put {
                key: key.clone(),
                value: value_bytes(value_size, offset),
            })
            .collect::<Vec<_>>();
        store.apply_batch(&requests)?;
    }
    let invariant = store.check_invariants()?;
    let db_size_bytes = fs::metadata(&db_path)?.len();
    let hot_revision = store.get(&dataset.keys[0])?.revision();
    drop(store);
    Ok(SeededDatabase {
        dir,
        db_path,
        logical_key_count: key_count,
        db_size_bytes,
        invariant,
        hot_revision,
    })
}

fn benchmark_database_config(cache_capacity: usize) -> DatabaseConfig {
    DatabaseConfig {
        database_uuid: benchmark_database_uuid(),
        tenant_id: TENANT,
        shard_id: ShardId::new(1),
        cache_capacity,
        ..DatabaseConfig::default()
    }
}

fn benchmark_database_uuid() -> [u8; 16] {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut first = FNV_OFFSET;
    let mut second = FNV_OFFSET ^ 0x9e3779b97f4a7c15;
    let mut input = Vec::with_capacity(24);
    input.extend_from_slice(b"DODB logical database identity");
    input.extend_from_slice(&TENANT.get().to_be_bytes());
    input.extend_from_slice(&ShardId::new(1).get().to_be_bytes());
    for (index, byte) in input.into_iter().enumerate() {
        first ^= u64::from(byte);
        first = first.wrapping_mul(FNV_PRIME);
        second ^= u64::from(byte).wrapping_add(index as u64);
        second = second.rotate_left(5).wrapping_mul(FNV_PRIME);
    }
    let mut uuid = [0u8; 16];
    uuid[..8].copy_from_slice(&first.to_be_bytes());
    uuid[8..].copy_from_slice(&second.to_be_bytes());
    uuid
}

fn direct_metrics(
    store: &BTreeStore<ProductionFile, ProductionFile>,
) -> Result<(StorageMetrics, Option<WalMetrics>), Box<dyn std::error::Error>> {
    Ok((store.storage_metrics(), store.wal_metrics()?))
}

fn dataset_report(
    layer: &str,
    value_size: usize,
    cache_capacity: Option<usize>,
    seeded: &SeededDatabase,
) -> DatasetReport {
    DatasetReport {
        layer: layer.to_owned(),
        value_size,
        logical_key_count: seeded.logical_key_count,
        cache_capacity,
        db_size_bytes: seeded.db_size_bytes,
        reachable_pages: seeded.invariant.reachable_pages,
    }
}

#[allow(clippy::too_many_arguments)]
fn make_result(
    layer: &str,
    workload: Workload,
    value_size: usize,
    key_count: usize,
    cache_capacity: Option<usize>,
    concurrency: usize,
    settings: Settings,
    accumulator: Accumulator,
    db_size_bytes: u64,
    reachable_pages: usize,
    storage: Option<StorageMetricReport>,
    wal: Option<WalMetricReport>,
    coordinator: Option<CoordinatorMetricReport>,
    server: Option<ServerMetricReport>,
) -> BenchmarkResult {
    let measurement_seconds = settings.measure.as_secs_f64().max(f64::EPSILON);
    BenchmarkResult {
        layer: layer.to_owned(),
        workload: workload.name().to_owned(),
        value_size,
        logical_key_count: key_count,
        cache_capacity,
        concurrency,
        warmup_ms: settings.warmup.as_millis() as u64,
        measurement_ms: settings.measure.as_millis() as u64,
        logical_operations: accumulator.operations,
        protocol_or_engine_requests: accumulator.requests,
        successful_operations: accumulator.successes,
        conflicts: accumulator.conflicts,
        errors: accumulator.errors,
        throughput_ops_per_sec: accumulator.operations as f64 / measurement_seconds,
        throughput_requests_per_sec: accumulator.requests as f64 / measurement_seconds,
        latency: latency_summary(&accumulator.latencies_ns),
        db_size_bytes,
        reachable_pages,
        storage,
        wal,
        coordinator,
        server,
        first_error: accumulator.first_error,
    }
}

fn latency_summary(samples: &[u64]) -> LatencySummary {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let percentile = |fraction: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
        sorted[index] as f64 / 1_000.0
    };
    let raw_samples_us = compact_samples(&sorted)
        .into_iter()
        .map(|sample| sample as f64 / 1_000.0)
        .collect();
    LatencySummary {
        p50_us: percentile(0.50),
        p95_us: percentile(0.95),
        p99_us: percentile(0.99),
        max_us: sorted.last().copied().unwrap_or_default() as f64 / 1_000.0,
        samples: sorted.len(),
        raw_samples_us,
    }
}

fn compact_samples(sorted: &[u64]) -> Vec<u64> {
    if sorted.len() <= RAW_SAMPLE_LIMIT {
        return sorted.to_vec();
    }
    (0..RAW_SAMPLE_LIMIT)
        .map(|index| sorted[index * (sorted.len() - 1) / (RAW_SAMPLE_LIMIT - 1)])
        .collect()
}

fn storage_report_delta(before: StorageMetrics, after: StorageMetrics) -> StorageMetricReport {
    StorageMetricReport {
        validation_us: delta(after.validation_nanos, before.validation_nanos) as f64 / 1_000.0,
        btree_preparation_us: delta(
            after.btree_preparation_nanos,
            before.btree_preparation_nanos,
        ) as f64
            / 1_000.0,
        publication_us: delta(after.publication_nanos, before.publication_nanos) as f64 / 1_000.0,
    }
}

fn wal_report_delta(
    before: Option<WalMetrics>,
    after: Option<WalMetrics>,
) -> Option<WalMetricReport> {
    let (before, after) = (before?, after?);
    let syncs = delta(after.wal_syncs, before.wal_syncs);
    let commits = delta(
        after.committed_batches as u64,
        before.committed_batches as u64,
    );
    Some(WalMetricReport {
        wal_bytes: delta(after.wal_bytes, before.wal_bytes),
        wal_bytes_per_sync: delta(after.wal_bytes, before.wal_bytes) as f64 / syncs.max(1) as f64,
        syncs,
        committed_batches: commits,
        page_images: delta(after.page_images as u64, before.page_images as u64),
        page_images_per_sync: delta(after.page_images as u64, before.page_images as u64) as f64
            / syncs.max(1) as f64,
        append_us_total: delta(after.append_nanos, before.append_nanos) as f64 / 1_000.0,
        append_us_per_commit: delta(after.append_nanos, before.append_nanos) as f64
            / commits.max(1) as f64
            / 1_000.0,
        sync_us_total: delta(after.sync_nanos, before.sync_nanos) as f64 / 1_000.0,
        sync_us_per_sync: delta(after.sync_nanos, before.sync_nanos) as f64
            / syncs.max(1) as f64
            / 1_000.0,
        commits_per_sync: commits as f64 / syncs.max(1) as f64,
    })
}

fn metric_reports(
    before: LiveMetrics,
    after: LiveMetrics,
) -> (
    Option<StorageMetricReport>,
    Option<WalMetricReport>,
    Option<CoordinatorMetricReport>,
    Option<ServerMetricReport>,
) {
    let storage = match (before.storage, after.storage) {
        (Some(before), Some(after)) => Some(storage_report_delta(before, after)),
        _ => None,
    };
    let wal = wal_report_delta(before.wal, after.wal);
    let coordinator = match (before.coordinator, after.coordinator) {
        (Some(before), Some(after)) => Some(coordinator_report(&before, &after)),
        _ => None,
    };
    let server = match (before.server, after.server) {
        (Some(before), Some(after)) => Some(server_report(&before, &after)),
        _ => None,
    };
    (storage, wal, coordinator, server)
}

fn coordinator_report(
    before: &CoordinatorMetrics,
    after: &CoordinatorMetrics,
) -> CoordinatorMetricReport {
    let groups = delta(after.groups, before.groups);
    let queued_requests = delta(after.queued_requests, before.queued_requests);
    let logical_transactions = delta(after.logical_transactions, before.logical_transactions);
    let counts = std::array::from_fn(|index| {
        delta(
            after.group_size_counts[index],
            before.group_size_counts[index],
        )
    });
    CoordinatorMetricReport {
        groups,
        queued_requests,
        logical_transactions,
        overloaded_requests: delta(after.overloaded_requests, before.overloaded_requests),
        mean_group_requests: queued_requests as f64 / groups.max(1) as f64,
        p50_group_requests: percentile_counts(&counts, 0.50),
        p95_group_requests: percentile_counts(&counts, 0.95),
        max_group_requests: after.max_group_requests,
        max_group_bytes: after.max_group_bytes,
        queue_wait_us_per_request: delta(after.queue_wait_nanos, before.queue_wait_nanos) as f64
            / queued_requests.max(1) as f64
            / 1_000.0,
        collection_us_per_group: delta(after.batch_collection_nanos, before.batch_collection_nanos)
            as f64
            / groups.max(1) as f64
            / 1_000.0,
        processing_us_per_group: delta(after.processing_nanos, before.processing_nanos) as f64
            / groups.max(1) as f64
            / 1_000.0,
    }
}

fn percentile_counts(counts: &[u64; 65], fraction: f64) -> f64 {
    let total = counts.iter().sum::<u64>();
    if total == 0 {
        return 0.0;
    }
    let target = ((total - 1) as f64 * fraction).round() as u64;
    let mut seen = 0u64;
    for (index, count) in counts.iter().enumerate().skip(1) {
        seen = seen.saturating_add(*count);
        if seen > target {
            return index as f64;
        }
    }
    64.0
}

fn server_report(
    before: &ServerMetricsSnapshot,
    after: &ServerMetricsSnapshot,
) -> ServerMetricReport {
    let operation_counts =
        std::array::from_fn(|index| delta(after.operations[index], before.operations[index]));
    let requests = delta(after.requests_total, before.requests_total);
    ServerMetricReport {
        connections_total: delta(after.connections_total, before.connections_total),
        requests_total: requests,
        request_bytes: delta(after.request_bytes, before.request_bytes),
        response_bytes: delta(after.response_bytes, before.response_bytes),
        protocol_errors: delta(after.protocol_errors, before.protocol_errors),
        transport_errors: delta(after.transport_errors, before.transport_errors),
        application_errors: delta(after.application_errors, before.application_errors),
        overloaded_responses: delta(after.overloaded_responses, before.overloaded_responses),
        server_request_latency_us: delta(after.request_latency_nanos, before.request_latency_nanos)
            as f64
            / requests.max(1) as f64
            / 1_000.0,
        operation_counts,
    }
}

fn delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

fn parse_output_path() -> PathBuf {
    let mut args = std::env::args_os().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--output"
            && let Some(path) = args.next()
        {
            return PathBuf::from(path);
        }
    }
    let sha = command_output("git", &["rev-parse", "--short", "HEAD"]);
    PathBuf::from("target/dodb-bench").join(format!("baseline-{}.json", sha.trim()))
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn environment(before: ResourceSnapshot, after: ResourceSnapshot) -> BenchmarkEnvironment {
    BenchmarkEnvironment {
        git_sha: command_output("git", &["rev-parse", "HEAD"])
            .trim()
            .to_owned(),
        release_build: true,
        cpu_model: read_cpu_model(),
        logical_cpu_count: std::thread::available_parallelism().map_or(0, |value| value.get()),
        allocated_logical_cpus: std::env::var("DODB_ALLOWED_CPUS")
            .unwrap_or_else(|_| "0,1,6,7".to_owned()),
        allocated_physical_cores: "core 0 + core 1 (SMT siblings 0,6 and 1,7)".to_owned(),
        cpu_affinity: command_output("taskset", &["-pc", &std::process::id().to_string()]),
        cargo_build_jobs: std::env::var("CARGO_BUILD_JOBS")
            .unwrap_or_else(|_| "not-set-at-runtime".to_owned()),
        memory_high: std::env::var("DODB_MEMORY_HIGH")
            .unwrap_or_else(|_| "not-recorded".to_owned()),
        memory_max: std::env::var("DODB_MEMORY_MAX").unwrap_or_else(|_| "not-recorded".to_owned()),
        memory_swap_max: std::env::var("DODB_MEMORY_SWAP_MAX")
            .unwrap_or_else(|_| "not-recorded".to_owned()),
        cgroup_path: fs::read_to_string("/proc/self/cgroup")
            .unwrap_or_default()
            .trim()
            .to_owned(),
        memory_total_kib: proc_meminfo_value("MemTotal"),
        os_kernel: command_output("uname", &["-sr"]).trim().to_owned(),
        filesystem_device: command_output("findmnt", &["-T", ".", "-o", "SOURCE,FSTYPE", "-n"])
            .trim()
            .to_owned(),
        initial_swap_used_kib: proc_meminfo_value("SwapTotal")
            .zip(proc_meminfo_value("SwapFree"))
            .map(|(total, free)| total.saturating_sub(free)),
        started_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis()),
        resources_before: before,
        resources_after: after,
    }
}

fn resource_snapshot() -> ResourceSnapshot {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let io = fs::read_to_string("/proc/self/io").unwrap_or_default();
    let cgroup = cgroup_directory();
    let events = read_keyed_file(&cgroup.join("memory.events"));
    ResourceSnapshot {
        vm_peak_kib: keyed_value(&status, "VmPeak:"),
        vm_hwm_kib: keyed_value(&status, "VmHWM:"),
        vm_rss_kib: keyed_value(&status, "VmRSS:"),
        read_bytes: keyed_value(&io, "read_bytes:"),
        write_bytes: keyed_value(&io, "write_bytes:"),
        cgroup_memory_current: fs::read_to_string(cgroup.join("memory.current"))
            .ok()
            .and_then(|value| value.trim().parse().ok()),
        cgroup_memory_peak: fs::read_to_string(cgroup.join("memory.peak"))
            .ok()
            .and_then(|value| value.trim().parse().ok()),
        cgroup_memory_high_events: events.get("high").copied(),
        cgroup_memory_max_events: events.get("max").copied(),
        cgroup_oom_kills: events.get("oom_kill").copied(),
        load_average: fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .trim()
            .to_owned(),
    }
}

fn keyed_value(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
}

fn cgroup_directory() -> PathBuf {
    let relative = fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .find_map(|line| line.strip_prefix("0::"))
                .map(|path| path.trim_start_matches('/').to_owned())
        });
    let mut directory = PathBuf::from("/sys/fs/cgroup");
    if let Some(relative) = relative {
        directory.push(relative);
    }
    directory
}

fn read_keyed_file(path: &Path) -> BTreeMap<String, u64> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((fields.next()?.to_owned(), fields.next()?.parse().ok()?))
        })
        .collect()
}

fn proc_meminfo_value(key: &str) -> Option<u64> {
    fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
}

fn read_cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split_once(':'))
        .map_or_else(
            || "unknown".to_owned(),
            |(_, value)| value.trim().to_owned(),
        )
}

fn command_output(command: &str, arguments: &[&str]) -> String {
    Command::new(command)
        .args(arguments)
        .output()
        .ok()
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn print_summary(report: &BenchmarkReport) {
    println!("layer workload value concurrency ops/s p50_us p95_us p99_us errors");
    for result in &report.results {
        println!(
            "{} {} {} {} {:.0} {:.1} {:.1} {:.1} {}",
            result.layer,
            result.workload,
            result.value_size,
            result.concurrency,
            result.throughput_ops_per_sec,
            result.latency.p50_us,
            result.latency.p95_us,
            result.latency.p99_us,
            result.errors,
        );
    }
}
