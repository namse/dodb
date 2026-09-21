use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dodb_core::{
    DocumentKey, Error, RevisionState, ShardId, TenantId, TransactionCondition,
    TransactionConflict, TransactionRequest,
};
use dodb_protocol::{
    ApplicationError, ProtocolError, ProtocolLimits, ResponseEnvelope, decode_header,
    decode_request_parts, encode_response,
};
use dodb_service::{Document, DodbService, Request, Response, ServiceFuture, TransactionOutcome};
use dodb_storage::{
    AsyncShard, BTreeStore, BatchRequest, BatchResponse, CoordinatorConfig, DatabaseConfig,
    ProductionFile,
};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use quinn::{Connection, Endpoint, Incoming, ServerConfig as QuinnServerConfig, VarInt};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Tls(String),
    Endpoint(String),
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Tls(error) => write!(formatter, "TLS configuration error: {error}"),
            Self::Endpoint(error) => write!(formatter, "QUIC endpoint error: {error}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerTlsConfig {
    certificate_chain: Vec<Vec<u8>>,
    private_key: Vec<u8>,
}

impl ServerTlsConfig {
    pub fn from_der(
        certificate_chain: Vec<Vec<u8>>,
        private_key: Vec<u8>,
    ) -> Result<Self, ServerError> {
        if certificate_chain.is_empty() || private_key.is_empty() {
            return Err(ServerError::Tls(
                "certificate chain and private key are required".to_owned(),
            ));
        }
        Ok(Self {
            certificate_chain,
            private_key,
        })
    }

    pub fn from_pem(certificate_pem: &[u8], private_key_pem: &[u8]) -> Result<Self, ServerError> {
        let mut certificate_reader = Cursor::new(certificate_pem);
        let certificates = rustls_pemfile::certs(&mut certificate_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ServerError::Tls(error.to_string()))?
            .into_iter()
            .map(|certificate| certificate.to_vec())
            .collect::<Vec<_>>();
        let mut key_reader = Cursor::new(private_key_pem);
        let private_key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|error| ServerError::Tls(error.to_string()))?
            .ok_or_else(|| ServerError::Tls("private key PEM section is missing".to_owned()))?
            .secret_der()
            .to_vec();
        Self::from_der(certificates, private_key)
    }

    fn to_quinn_config(
        &self,
        max_concurrent_streams: usize,
    ) -> Result<QuinnServerConfig, ServerError> {
        let certificates = self
            .certificate_chain
            .iter()
            .cloned()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();
        let private_key = PrivateKeyDer::try_from(self.private_key.clone())
            .map_err(|error| ServerError::Tls(error.to_string()))?;
        let mut config = QuinnServerConfig::with_single_cert(certificates, private_key)
            .map_err(|error| ServerError::Tls(error.to_string()))?;
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_bidi_streams(
            VarInt::try_from(max_concurrent_streams)
                .map_err(|error| ServerError::Tls(error.to_string()))?,
        );
        config.transport_config(Arc::new(transport));
        Ok(config)
    }
}

#[derive(Clone, Debug)]
pub struct DodbServerConfig {
    pub listen_addr: std::net::SocketAddr,
    pub tls: ServerTlsConfig,
    pub protocol_limits: ProtocolLimits,
    pub max_connections: usize,
    pub max_concurrent_streams: usize,
}

impl DodbServerConfig {
    pub fn validate(&self) -> Result<(), ServerError> {
        self.protocol_limits
            .validate()
            .map_err(|error| ServerError::Endpoint(error.to_string()))?;
        if self.max_connections == 0 || self.max_concurrent_streams == 0 {
            return Err(ServerError::Endpoint(
                "connection and stream limits must be nonzero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct ServerMetrics {
    inner: Arc<ServerMetricsInner>,
}

#[derive(Debug)]
struct ServerMetricsInner {
    connections_total: AtomicU64,
    active_connections: AtomicU64,
    active_streams: AtomicU64,
    requests_total: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    protocol_errors: AtomicU64,
    transport_errors: AtomicU64,
    application_errors: AtomicU64,
    overloaded_responses: AtomicU64,
    request_latency_nanos: AtomicU64,
    operations: [AtomicU64; 8],
}

impl Default for ServerMetricsInner {
    fn default() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            active_streams: AtomicU64::new(0),
            requests_total: AtomicU64::new(0),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            protocol_errors: AtomicU64::new(0),
            transport_errors: AtomicU64::new(0),
            application_errors: AtomicU64::new(0),
            overloaded_responses: AtomicU64::new(0),
            request_latency_nanos: AtomicU64::new(0),
            operations: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerMetricsSnapshot {
    pub connections_total: u64,
    pub active_connections: u64,
    pub active_streams: u64,
    pub requests_total: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub protocol_errors: u64,
    pub transport_errors: u64,
    pub application_errors: u64,
    pub overloaded_responses: u64,
    pub request_latency_nanos: u64,
    pub operations: [u64; 8],
}

impl ServerMetrics {
    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        let inner = &self.inner;
        ServerMetricsSnapshot {
            connections_total: inner.connections_total.load(Ordering::Relaxed),
            active_connections: inner.active_connections.load(Ordering::Relaxed),
            active_streams: inner.active_streams.load(Ordering::Relaxed),
            requests_total: inner.requests_total.load(Ordering::Relaxed),
            request_bytes: inner.request_bytes.load(Ordering::Relaxed),
            response_bytes: inner.response_bytes.load(Ordering::Relaxed),
            protocol_errors: inner.protocol_errors.load(Ordering::Relaxed),
            transport_errors: inner.transport_errors.load(Ordering::Relaxed),
            application_errors: inner.application_errors.load(Ordering::Relaxed),
            overloaded_responses: inner.overloaded_responses.load(Ordering::Relaxed),
            request_latency_nanos: inner.request_latency_nanos.load(Ordering::Relaxed),
            operations: std::array::from_fn(|operation_index| {
                inner.operations[operation_index].load(Ordering::Relaxed)
            }),
        }
    }
}

pub struct DodbServer<S> {
    endpoint: Endpoint,
    service: Arc<S>,
    protocol_limits: ProtocolLimits,
    connection_slots: Arc<Semaphore>,
    stream_slots: Arc<Semaphore>,
    metrics: ServerMetrics,
}

impl<S: DodbService + 'static> DodbServer<S> {
    pub fn bind(service: Arc<S>, config: DodbServerConfig) -> Result<Self, ServerError> {
        config.validate()?;
        let quinn_config = config.tls.to_quinn_config(config.max_concurrent_streams)?;
        let endpoint = Endpoint::server(quinn_config, config.listen_addr)?;
        Ok(Self {
            endpoint,
            service,
            protocol_limits: config.protocol_limits,
            connection_slots: Arc::new(Semaphore::new(config.max_connections)),
            stream_slots: Arc::new(Semaphore::new(config.max_concurrent_streams)),
            metrics: ServerMetrics::default(),
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr, ServerError> {
        self.endpoint.local_addr().map_err(ServerError::Io)
    }

    pub fn metrics(&self) -> ServerMetrics {
        self.metrics.clone()
    }

    pub fn close(&self) {
        self.endpoint.close(VarInt::from_u32(0), b"server shutdown");
    }

    pub async fn run(&self) -> Result<(), ServerError> {
        while let Some(incoming) = self.endpoint.accept().await {
            let Ok(connection_permit) = self.connection_slots.clone().try_acquire_owned() else {
                incoming.refuse();
                self.metrics
                    .inner
                    .overloaded_responses
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let service = Arc::clone(&self.service);
            let stream_slots = Arc::clone(&self.stream_slots);
            let limits = self.protocol_limits;
            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                serve_connection(
                    incoming,
                    service,
                    stream_slots,
                    limits,
                    metrics,
                    connection_permit,
                )
                .await;
            });
        }
        Ok(())
    }
}

async fn serve_connection<S: DodbService + 'static>(
    incoming: Incoming,
    service: Arc<S>,
    stream_slots: Arc<Semaphore>,
    limits: ProtocolLimits,
    metrics: ServerMetrics,
    _connection_permit: OwnedSemaphorePermit,
) {
    let connection = match incoming.await {
        Ok(connection) => connection,
        Err(_) => {
            metrics
                .inner
                .transport_errors
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    metrics
        .inner
        .connections_total
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .inner
        .active_connections
        .fetch_add(1, Ordering::Relaxed);
    accept_streams(connection, service, stream_slots, limits, metrics.clone()).await;
    metrics
        .inner
        .active_connections
        .fetch_sub(1, Ordering::Relaxed);
}

async fn accept_streams<S: DodbService + 'static>(
    connection: Connection,
    service: Arc<S>,
    stream_slots: Arc<Semaphore>,
    limits: ProtocolLimits,
    metrics: ServerMetrics,
) {
    loop {
        let stream = match connection.accept_bi().await {
            Ok(stream) => stream,
            Err(_) => {
                metrics
                    .inner
                    .transport_errors
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let Ok(stream_permit) = stream_slots.clone().try_acquire_owned() else {
            connection.close(VarInt::from_u32(1), b"too many active streams");
            metrics
                .inner
                .overloaded_responses
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let service = Arc::clone(&service);
        let metrics = metrics.clone();
        tokio::spawn(async move {
            metrics.inner.active_streams.fetch_add(1, Ordering::Relaxed);
            serve_stream(stream, service, limits, metrics.clone()).await;
            metrics.inner.active_streams.fetch_sub(1, Ordering::Relaxed);
            drop(stream_permit);
        });
    }
}

async fn serve_stream<S: DodbService + 'static>(
    (mut send, mut receive): (quinn::SendStream, quinn::RecvStream),
    service: Arc<S>,
    limits: ProtocolLimits,
    metrics: ServerMetrics,
) {
    let mut header_bytes = [0u8; dodb_protocol::HEADER_SIZE];
    if receive.read_exact(&mut header_bytes).await.is_err() {
        metrics
            .inner
            .transport_errors
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let header = match decode_header(&header_bytes) {
        Ok(header) => header,
        Err(error) => {
            metrics
                .inner
                .protocol_errors
                .fetch_add(1, Ordering::Relaxed);
            let _ = send_protocol_error(&mut send, error, limits).await;
            return;
        }
    };
    let frame_length = dodb_protocol::HEADER_SIZE.saturating_add(header.payload_length);
    if frame_length > limits.max_request_frame_size {
        metrics
            .inner
            .protocol_errors
            .fetch_add(1, Ordering::Relaxed);
        let _ = send_protocol_error(
            &mut send,
            ProtocolError::PayloadTooLarge {
                length: frame_length,
                maximum: limits.max_request_frame_size,
            },
            limits,
        )
        .await;
        return;
    }
    let mut payload = vec![0u8; header.payload_length];
    if receive.read_exact(&mut payload).await.is_err() {
        metrics
            .inner
            .protocol_errors
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let trailing = receive.read_to_end(1).await;
    if trailing.as_ref().is_err() || trailing.as_ref().is_ok_and(|bytes| !bytes.is_empty()) {
        metrics
            .inner
            .protocol_errors
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let (tenant, request) = match decode_request_parts(header, &payload, limits) {
        Ok(request) => request,
        Err(error) => {
            metrics
                .inner
                .protocol_errors
                .fetch_add(1, Ordering::Relaxed);
            let _ = send_protocol_error(&mut send, error, limits).await;
            return;
        }
    };
    let operation_index = usize::from(dodb_protocol::request_opcode(&request).saturating_sub(1));
    metrics.inner.requests_total.fetch_add(1, Ordering::Relaxed);
    metrics.inner.request_bytes.fetch_add(
        (dodb_protocol::HEADER_SIZE + payload.len()) as u64,
        Ordering::Relaxed,
    );
    if let Some(counter) = metrics.inner.operations.get(operation_index) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    let started = Instant::now();
    let envelope = match service.execute(tenant, request).await {
        Ok(response) => ResponseEnvelope::Success(response),
        Err(error) => {
            let application_error = ApplicationError::from_core(&error);
            if application_error.kind == dodb_protocol::ApplicationErrorKind::Overloaded {
                metrics
                    .inner
                    .overloaded_responses
                    .fetch_add(1, Ordering::Relaxed);
            }
            ResponseEnvelope::Error(application_error)
        }
    };
    if matches!(envelope, ResponseEnvelope::Error(_)) {
        metrics
            .inner
            .application_errors
            .fetch_add(1, Ordering::Relaxed);
    }
    let encoded = match encode_response(&envelope, limits) {
        Ok(encoded) => encoded,
        Err(error) => {
            metrics
                .inner
                .protocol_errors
                .fetch_add(1, Ordering::Relaxed);
            let _ = send_protocol_error(&mut send, error, limits).await;
            return;
        }
    };
    if send.write_all(&encoded).await.is_err() || send.finish().is_err() {
        metrics
            .inner
            .transport_errors
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    metrics
        .inner
        .response_bytes
        .fetch_add(encoded.len() as u64, Ordering::Relaxed);
    metrics
        .inner
        .request_latency_nanos
        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
}

async fn send_protocol_error(
    send: &mut quinn::SendStream,
    error: ProtocolError,
    limits: ProtocolLimits,
) -> Result<(), quinn::WriteError> {
    let response = encode_response(
        &ResponseEnvelope::Error(ApplicationError::from_protocol(&error)),
        limits,
    )
    .map_err(|_| quinn::WriteError::Stopped(VarInt::from_u32(1)))?;
    send.write_all(&response).await?;
    send.finish()
        .map_err(|_| quinn::WriteError::Stopped(VarInt::from_u32(1)))
}

#[derive(Clone, Debug)]
pub struct LocalTenantServiceConfig {
    pub data_dir: PathBuf,
    pub max_open_shards: usize,
    pub database_config: DatabaseConfig,
    pub coordinator_config: CoordinatorConfig,
}

impl Default for LocalTenantServiceConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("."),
            max_open_shards: 1_024,
            database_config: DatabaseConfig::default(),
            coordinator_config: CoordinatorConfig::default(),
        }
    }
}

pub trait TenantShardResolver: Send + Sync {
    fn resolve(&self, tenant: TenantId) -> ShardId;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OneTenantOneShard;

impl TenantShardResolver for OneTenantOneShard {
    fn resolve(&self, tenant: TenantId) -> ShardId {
        ShardId::new(tenant.get())
    }
}

pub struct LocalTenantService {
    data_dir: PathBuf,
    max_open_shards: usize,
    database_config: DatabaseConfig,
    coordinator_config: CoordinatorConfig,
    resolver: Arc<dyn TenantShardResolver>,
    shards: Mutex<BTreeMap<TenantId, Arc<AsyncShard<ProductionFile, ProductionFile>>>>,
}

impl LocalTenantService {
    pub fn new(config: LocalTenantServiceConfig) -> Result<Self, Error> {
        if config.max_open_shards == 0 {
            return Err(Error::invalid_request("max_open_shards must be nonzero"));
        }
        std::fs::create_dir_all(&config.data_dir)?;
        Ok(Self {
            data_dir: config.data_dir,
            max_open_shards: config.max_open_shards,
            database_config: config.database_config,
            coordinator_config: config.coordinator_config,
            resolver: Arc::new(OneTenantOneShard),
            shards: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn with_resolver(
        config: LocalTenantServiceConfig,
        resolver: Arc<dyn TenantShardResolver>,
    ) -> Result<Self, Error> {
        let service = Self::new(config)?;
        Ok(Self {
            resolver,
            ..service
        })
    }

    pub async fn shutdown(self) {
        let mut shards = self.shards.lock().await;
        shards.clear();
    }

    async fn execute_request(&self, tenant: TenantId, request: Request) -> Result<Response, Error> {
        match request {
            Request::Get { key } => {
                let state = match self.read_shard(tenant).await? {
                    Some(shard) => match shard.execute(BatchRequest::Get { key }).await? {
                        BatchResponse::Get(state) => state,
                        _ => return Err(Error::invariant("local get returned the wrong response")),
                    },
                    None => RevisionState::missing(dodb_core::Revision::ZERO),
                };
                Ok(Response::Get(state))
            }
            Request::Put { key, value } => {
                let shard = self.open_shard(tenant).await?;
                let revision = match shard.execute(BatchRequest::Put { key, value }).await? {
                    BatchResponse::Put(revision) => revision,
                    _ => return Err(Error::invariant("local put returned the wrong response")),
                };
                Ok(Response::Put(revision))
            }
            Request::Delete { key } => {
                let shard = self.open_shard(tenant).await?;
                let revision = match shard.execute(BatchRequest::Delete { key }).await? {
                    BatchResponse::Delete(revision) => revision,
                    _ => return Err(Error::invariant("local delete returned the wrong response")),
                };
                Ok(Response::Delete(revision))
            }
            Request::Query {
                pk,
                exclusive_after_sk,
                limit,
            } => {
                let rows = match self.read_shard(tenant).await? {
                    Some(shard) => match shard
                        .execute(BatchRequest::Query {
                            pk,
                            exclusive_after_sk,
                            limit,
                        })
                        .await?
                    {
                        BatchResponse::Query(rows) => rows,
                        _ => {
                            return Err(Error::invariant(
                                "local query returned the wrong response",
                            ));
                        }
                    },
                    None => Vec::new(),
                };
                Ok(Response::Query(
                    rows.into_iter().map(storage_document).collect(),
                ))
            }
            Request::Scan {
                exclusive_after_key,
                limit,
            } => {
                let rows = match self.read_shard(tenant).await? {
                    Some(shard) => match shard
                        .execute(BatchRequest::Scan {
                            exclusive_after_key,
                            limit,
                        })
                        .await?
                    {
                        BatchResponse::Scan(rows) => rows,
                        _ => {
                            return Err(Error::invariant("local scan returned the wrong response"));
                        }
                    },
                    None => Vec::new(),
                };
                Ok(Response::Scan(
                    rows.into_iter().map(storage_document).collect(),
                ))
            }
            Request::Batch { mutations } => {
                let shard = self.open_shard(tenant).await?;
                let result = shard
                    .execute_transaction(TransactionRequest::new(Vec::new(), mutations))
                    .await?;
                Ok(Response::Batch(TransactionOutcome::committed(
                    result.commit_lsn,
                )))
            }
            Request::TransactGet { keys } => Ok(Response::TransactGet(
                self.read_states(tenant, &keys).await?,
            )),
            Request::Transact { request } => self.execute_transaction(tenant, request).await,
        }
    }

    async fn execute_transaction(
        &self,
        tenant: TenantId,
        request: TransactionRequest,
    ) -> Result<Response, Error> {
        if request.mutations.is_empty() {
            validate_condition_keys(&request.conditions)?;
            if request.conditions.is_empty() {
                return Err(Error::invalid_request(
                    "a condition-only transaction must contain at least one condition",
                ));
            }
            let keys = request
                .conditions
                .iter()
                .map(|condition| condition.key().clone())
                .collect::<Vec<_>>();
            let states = self.read_states(tenant, &keys).await?;
            let state_map = keys.into_iter().zip(states).collect::<BTreeMap<_, _>>();
            for condition in &request.conditions {
                let actual = state_map
                    .get(condition.key())
                    .ok_or_else(|| Error::invariant("condition key missing from point snapshot"))?;
                if !condition_matches(condition, actual) {
                    return Err(Error::conflict(TransactionConflict {
                        key: condition.key().clone(),
                        expected: condition.expectation(),
                        actual: actual.clone(),
                    }));
                }
            }
            return Ok(Response::Transact(
                TransactionOutcome::conditions_satisfied(),
            ));
        }
        let shard = self.open_shard(tenant).await?;
        let result = shard.execute_transaction(request).await?;
        Ok(Response::Transact(TransactionOutcome::committed(
            result.commit_lsn,
        )))
    }

    async fn read_states(
        &self,
        tenant: TenantId,
        keys: &[DocumentKey],
    ) -> Result<Vec<RevisionState>, Error> {
        match self.read_shard(tenant).await? {
            Some(shard) => shard.transact_get(keys.to_vec()).await,
            None => Ok(keys
                .iter()
                .map(|_| RevisionState::missing(dodb_core::Revision::ZERO))
                .collect()),
        }
    }

    async fn read_shard(
        &self,
        tenant: TenantId,
    ) -> Result<Option<Arc<AsyncShard<ProductionFile, ProductionFile>>>, Error> {
        let shard_id = self.resolver.resolve(tenant);
        let database_path = self.database_path(tenant, shard_id);
        let wal_path = database_path.with_extension("wal");
        if !database_path.exists() && !wal_path.exists() {
            return Ok(None);
        }
        Ok(Some(self.open_shard(tenant).await?))
    }

    async fn open_shard(
        &self,
        tenant: TenantId,
    ) -> Result<Arc<AsyncShard<ProductionFile, ProductionFile>>, Error> {
        let mut shards = self.shards.lock().await;
        if let Some(shard) = shards.get(&tenant) {
            return Ok(Arc::clone(shard));
        }
        if shards.len() >= self.max_open_shards {
            return Err(Error::overloaded("open shard cache is full"));
        }
        let shard_id = self.resolver.resolve(tenant);
        let database_path = self.database_path(tenant, shard_id);
        std::fs::create_dir_all(&self.data_dir)?;
        let mut database_config = self.database_config.clone();
        database_config.tenant_id = tenant;
        database_config.shard_id = shard_id;
        database_config.database_uuid = database_uuid(tenant, shard_id);
        let store = BTreeStore::<ProductionFile, ProductionFile>::open_path(
            &database_path,
            database_config,
        )?;
        let shard = Arc::new(AsyncShard::start_with_config(
            store,
            self.coordinator_config,
        ));
        shards.insert(tenant, Arc::clone(&shard));
        Ok(shard)
    }

    fn database_path(&self, tenant: TenantId, shard: ShardId) -> PathBuf {
        self.data_dir
            .join(format!("tenant-{}-shard-{}.db", tenant.get(), shard.get()))
    }
}

impl DodbService for LocalTenantService {
    fn execute<'service>(
        &'service self,
        tenant: TenantId,
        request: Request,
    ) -> ServiceFuture<'service> {
        Box::pin(async move { self.execute_request(tenant, request).await })
    }
}

fn storage_document(document: dodb_storage::Document) -> Document {
    Document {
        key: document.key,
        value: document.value,
        revision: document.revision,
    }
}

fn database_uuid(tenant: TenantId, shard: ShardId) -> [u8; 16] {
    let mut uuid = [0u8; 16];
    uuid[..4].copy_from_slice(b"DODB");
    uuid[4..12].copy_from_slice(&tenant.get().to_be_bytes());
    uuid[12..].copy_from_slice(&shard.get().to_be_bytes()[..4]);
    uuid
}

fn validate_condition_keys(conditions: &[TransactionCondition]) -> Result<(), Error> {
    let mut keys = BTreeSet::new();
    for condition in conditions {
        if !keys.insert(condition.key().clone()) {
            return Err(Error::invalid_request(
                "a transaction contains multiple conditions for one key",
            ));
        }
    }
    Ok(())
}

fn condition_matches(condition: &TransactionCondition, actual: &RevisionState) -> bool {
    match condition {
        TransactionCondition::RevisionEquals {
            expected_revision, ..
        } => actual.revision() == *expected_revision,
        TransactionCondition::Exists { .. } => !actual.is_missing(),
        TransactionCondition::NotExists { .. } => actual.is_missing(),
    }
}
