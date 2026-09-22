use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use dodb_core::{
    DocumentKey, Revision, RevisionState, TenantId, TransactionMutation, TransactionRequest,
};
use dodb_protocol::{
    ApplicationError, MutationOutcome, ProtocolError, ProtocolLimits, ResponseEnvelope,
    decode_header, decode_response_parts, encode_request,
};
use dodb_service::{Document, Response, TransactionOutcome};
use quinn::rustls::pki_types::CertificateDer;
use quinn::{ClientConfig as QuinnClientConfig, Endpoint, VarInt};

#[derive(Debug)]
pub enum ClientError {
    InvalidInput(String),
    Tls(String),
    Endpoint(String),
    Transport(String),
    Protocol(ProtocolError),
    Application(Box<ApplicationError>),
    UnknownMutationOutcome {
        detail: String,
        cause: Option<Box<ApplicationError>>,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(detail) => write!(formatter, "invalid client input: {detail}"),
            Self::Tls(detail) => write!(formatter, "TLS configuration error: {detail}"),
            Self::Endpoint(detail) => write!(formatter, "QUIC endpoint error: {detail}"),
            Self::Transport(detail) => write!(formatter, "transport error: {detail}"),
            Self::Protocol(error) => write!(formatter, "protocol error: {error}"),
            Self::Application(error) => {
                write!(
                    formatter,
                    "application error {:?}: {}",
                    error.kind, error.detail
                )
            }
            Self::UnknownMutationOutcome { detail, .. } => {
                write!(formatter, "mutation outcome is unknown: {detail}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientTlsConfig {
    root_certificates: Vec<Vec<u8>>,
}

impl ClientTlsConfig {
    pub fn from_der(root_certificates: Vec<Vec<u8>>) -> Result<Self, ClientError> {
        if root_certificates.is_empty() {
            return Err(ClientError::Tls(
                "at least one trusted root certificate is required".to_owned(),
            ));
        }
        Ok(Self { root_certificates })
    }

    pub fn from_pem(root_certificates_pem: &[u8]) -> Result<Self, ClientError> {
        let mut reader = Cursor::new(root_certificates_pem);
        let certificates = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ClientError::Tls(error.to_string()))?
            .into_iter()
            .map(|certificate| certificate.to_vec())
            .collect::<Vec<_>>();
        Self::from_der(certificates)
    }

    fn to_quinn_config(&self) -> Result<QuinnClientConfig, ClientError> {
        let mut roots = quinn::rustls::RootCertStore::empty();
        for certificate in &self.root_certificates {
            roots
                .add(CertificateDer::from(certificate.clone()))
                .map_err(|error| ClientError::Tls(error.to_string()))?;
        }
        QuinnClientConfig::with_root_certificates(Arc::new(roots))
            .map_err(|error| ClientError::Tls(error.to_string()))
    }
}

#[derive(Clone)]
pub struct DodbConnection {
    inner: Arc<DodbConnectionInner>,
}

struct DodbConnectionInner {
    endpoint: Endpoint,
    connection: quinn::Connection,
    limits: ProtocolLimits,
}

impl DodbConnection {
    /// Opens one authenticated QUIC connection that can serve any number of
    /// tenant-scoped handles.
    pub async fn connect(
        bind_addr: std::net::SocketAddr,
        server_addr: std::net::SocketAddr,
        server_name: &str,
        tls: ClientTlsConfig,
        limits: ProtocolLimits,
    ) -> Result<Self, ClientError> {
        limits.validate().map_err(ClientError::Protocol)?;
        let mut endpoint = Endpoint::client(bind_addr)
            .map_err(|error| ClientError::Endpoint(error.to_string()))?;
        endpoint.set_default_client_config(tls.to_quinn_config()?);
        let connection = endpoint
            .connect(server_addr, server_name)
            .map_err(|error| ClientError::Endpoint(error.to_string()))?
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(DodbConnectionInner {
                endpoint,
                connection,
                limits,
            }),
        })
    }

    pub async fn connect_with_endpoint(
        mut endpoint: Endpoint,
        server_addr: std::net::SocketAddr,
        server_name: &str,
        tls: ClientTlsConfig,
        limits: ProtocolLimits,
    ) -> Result<Self, ClientError> {
        limits.validate().map_err(ClientError::Protocol)?;
        endpoint.set_default_client_config(tls.to_quinn_config()?);
        let connection = endpoint
            .connect(server_addr, server_name)
            .map_err(|error| ClientError::Endpoint(error.to_string()))?
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(DodbConnectionInner {
                endpoint,
                connection,
                limits,
            }),
        })
    }

    pub fn for_tenant(&self, tenant: TenantId) -> DodbClient {
        DodbClient {
            connection: self.clone(),
            tenant,
        }
    }

    pub fn remote_addr(&self) -> std::net::SocketAddr {
        self.inner.connection.remote_address()
    }

    /// Explicitly shuts down the shared endpoint and connection.
    pub fn close(&self) {
        self.inner
            .connection
            .close(VarInt::from_u32(0), b"client shutdown");
        self.inner
            .endpoint
            .close(VarInt::from_u32(0), b"client shutdown");
    }
}

#[derive(Clone)]
pub struct DodbClient {
    connection: DodbConnection,
    tenant: TenantId,
}

impl DodbClient {
    /// Opens a connection and returns a tenant-scoped handle.
    ///
    /// New code serving multiple tenants should use [`DodbConnection::connect`]
    /// once and call [`DodbConnection::for_tenant`] for each tenant.
    pub async fn connect(
        bind_addr: std::net::SocketAddr,
        server_addr: std::net::SocketAddr,
        server_name: &str,
        tenant: TenantId,
        tls: ClientTlsConfig,
        limits: ProtocolLimits,
    ) -> Result<Self, ClientError> {
        Ok(
            DodbConnection::connect(bind_addr, server_addr, server_name, tls, limits)
                .await?
                .for_tenant(tenant),
        )
    }

    pub async fn connect_with_endpoint(
        endpoint: Endpoint,
        server_addr: std::net::SocketAddr,
        server_name: &str,
        tenant: TenantId,
        tls: ClientTlsConfig,
        limits: ProtocolLimits,
    ) -> Result<Self, ClientError> {
        Ok(
            DodbConnection::connect_with_endpoint(endpoint, server_addr, server_name, tls, limits)
                .await?
                .for_tenant(tenant),
        )
    }

    pub fn connection(&self) -> &DodbConnection {
        &self.connection
    }

    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    pub fn remote_addr(&self) -> std::net::SocketAddr {
        self.connection.remote_addr()
    }

    /// Releases this tenant handle without shutting down other handles.
    ///
    /// Call [`DodbConnection::close`] on the shared connection when the
    /// owner is ready to shut down the transport.
    pub fn close(&self) {
        // Kept as a source-compatible convenience for the original
        // tenant-bound client API. The shared connection has explicit
        // ownership and is closed through DodbConnection.
    }

    pub async fn get(&self, key: DocumentKey) -> Result<RevisionState, ClientError> {
        match self.execute(dodb_service::Request::Get { key }).await? {
            Response::Get(state) => Ok(state),
            _ => Err(unexpected_response(1, 0)),
        }
    }

    pub async fn put(&self, key: DocumentKey, value: Vec<u8>) -> Result<Revision, ClientError> {
        match self
            .execute(dodb_service::Request::Put { key, value })
            .await?
        {
            Response::Put(revision) => Ok(revision),
            _ => Err(unexpected_response(2, 0)),
        }
    }

    pub async fn delete(&self, key: DocumentKey) -> Result<Revision, ClientError> {
        match self.execute(dodb_service::Request::Delete { key }).await? {
            Response::Delete(revision) => Ok(revision),
            _ => Err(unexpected_response(3, 0)),
        }
    }

    pub async fn query(
        &self,
        pk: dodb_core::PrimaryKey,
        exclusive_after_sk: Option<dodb_core::SortKey>,
        limit: usize,
    ) -> Result<Vec<Document>, ClientError> {
        match self
            .execute(dodb_service::Request::Query {
                pk,
                exclusive_after_sk,
                limit,
            })
            .await?
        {
            Response::Query(rows) => Ok(rows),
            _ => Err(unexpected_response(4, 0)),
        }
    }

    pub async fn scan(
        &self,
        exclusive_after_key: Option<DocumentKey>,
        limit: usize,
    ) -> Result<Vec<Document>, ClientError> {
        match self
            .execute(dodb_service::Request::Scan {
                exclusive_after_key,
                limit,
            })
            .await?
        {
            Response::Scan(rows) => Ok(rows),
            _ => Err(unexpected_response(5, 0)),
        }
    }

    pub async fn batch(
        &self,
        mutations: Vec<TransactionMutation>,
    ) -> Result<TransactionOutcome, ClientError> {
        match self
            .execute(dodb_service::Request::Batch { mutations })
            .await?
        {
            Response::Batch(outcome) => Ok(outcome),
            _ => Err(unexpected_response(6, 0)),
        }
    }

    pub async fn transact_get(
        &self,
        keys: Vec<DocumentKey>,
    ) -> Result<Vec<RevisionState>, ClientError> {
        match self
            .execute(dodb_service::Request::TransactGet { keys })
            .await?
        {
            Response::TransactGet(states) => Ok(states),
            _ => Err(unexpected_response(7, 0)),
        }
    }

    pub async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionOutcome, ClientError> {
        match self
            .execute(dodb_service::Request::Transact { request })
            .await?
        {
            Response::Transact(outcome) => Ok(outcome),
            _ => Err(unexpected_response(8, 0)),
        }
    }

    async fn execute(&self, request: dodb_service::Request) -> Result<Response, ClientError> {
        let mutation = request.is_mutation();
        let expected_response_type = response_opcode_for_request(&request);
        let encoded_request = encode_request(self.tenant, &request, self.connection.inner.limits)
            .map_err(ClientError::Protocol)?;
        let (mut send, mut receive) = self
            .connection
            .inner
            .connection
            .open_bi()
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        if let Err(error) = send.write_all(&encoded_request).await {
            return Err(uncertain_mutation(
                mutation,
                format!("request write failed: {error}"),
            ));
        }
        if let Err(error) = send.finish() {
            return Err(uncertain_mutation(
                mutation,
                format!("request finish failed: {error}"),
            ));
        }
        let mut header_bytes = [0u8; dodb_protocol::HEADER_SIZE];
        if let Err(error) = receive.read_exact(&mut header_bytes).await {
            return Err(uncertain_mutation(
                mutation,
                format!("response header failed: {error}"),
            ));
        }
        let header =
            decode_header(&header_bytes).map_err(|error| uncertain_protocol(mutation, error))?;
        let frame_length = dodb_protocol::HEADER_SIZE.saturating_add(header.payload_length);
        if frame_length > self.connection.inner.limits.max_response_frame_size {
            return Err(uncertain_protocol(
                mutation,
                ProtocolError::PayloadTooLarge {
                    length: frame_length,
                    maximum: self.connection.inner.limits.max_response_frame_size,
                },
            ));
        }
        let mut payload = vec![0u8; header.payload_length];
        if let Err(error) = receive.read_exact(&mut payload).await {
            return Err(uncertain_mutation(
                mutation,
                format!("response payload failed: {error}"),
            ));
        }
        let trailing = receive
            .read_to_end(1)
            .await
            .map_err(|_| uncertain_protocol(mutation, ProtocolError::TrailingBytes))?;
        if !trailing.is_empty() {
            return Err(uncertain_protocol(mutation, ProtocolError::TrailingBytes));
        }
        match decode_response_parts(
            header,
            &payload,
            Some(expected_response_type),
            self.connection.inner.limits,
        )
        .map_err(|error| uncertain_protocol(mutation, error))?
        {
            ResponseEnvelope::Success(response) => Ok(response),
            ResponseEnvelope::Error(error) => {
                if mutation && error.mutation_outcome == MutationOutcome::Unknown {
                    return Err(ClientError::UnknownMutationOutcome {
                        detail: error.detail.clone(),
                        cause: Some(Box::new(error)),
                    });
                }
                Err(ClientError::Application(Box::new(error)))
            }
        }
    }
}

fn response_opcode_for_request(request: &dodb_service::Request) -> u8 {
    dodb_protocol::request_opcode(request)
}

fn unexpected_response(expected: u8, actual: u8) -> ClientError {
    ClientError::Protocol(ProtocolError::InvalidResponseType { expected, actual })
}

fn uncertain_mutation(mutation: bool, detail: String) -> ClientError {
    if mutation {
        ClientError::UnknownMutationOutcome {
            detail,
            cause: None,
        }
    } else {
        ClientError::Transport(detail)
    }
}

fn uncertain_protocol(mutation: bool, error: ProtocolError) -> ClientError {
    if mutation {
        ClientError::UnknownMutationOutcome {
            detail: error.to_string(),
            cause: None,
        }
    } else {
        ClientError::Protocol(error)
    }
}
