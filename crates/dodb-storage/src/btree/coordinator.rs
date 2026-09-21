use std::time::Duration;

use dodb_core::{Error, Result};

use super::{BTreeStore, BatchRequest, BatchResponse};
use crate::DurableFile;
use crate::wal::WalMetrics;

const MAX_BATCH_REQUESTS: usize = 64;
const BATCH_COLLECTION_WINDOW: Duration = Duration::from_millis(1);

/// Async single-shard facade. The coordinator is deliberately thin; all tree
/// work remains in the synchronous engine and is directly testable.
pub struct AsyncShard<
    F: DurableFile + Send + 'static,
    W: DurableFile + Send + 'static = super::NoWal,
> {
    request_tx: tokio::sync::mpsc::Sender<QueuedRequest>,
    close_tx: Option<tokio::sync::oneshot::Sender<()>>,
    metrics: std::sync::Arc<std::sync::Mutex<Option<WalMetrics>>>,
    _marker: std::marker::PhantomData<(F, W)>,
}

impl<F: DurableFile + Send + 'static, W: DurableFile + Send + 'static> AsyncShard<F, W> {
    pub fn start(store: BTreeStore<F, W>, queue_capacity: usize) -> Self {
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(queue_capacity.max(1));
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let metrics = std::sync::Arc::new(std::sync::Mutex::new(None));
        tokio::spawn(coordinator(
            store,
            request_rx,
            close_rx,
            std::sync::Arc::clone(&metrics),
        ));
        Self {
            request_tx,
            close_tx: Some(close_tx),
            metrics,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn execute(&self, request: BatchRequest) -> Result<BatchResponse> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.request_tx
            .send(QueuedRequest {
                request,
                response_tx,
            })
            .await
            .map_err(|_| Error::invariant("storage coordinator is closed"))?;
        response_rx
            .await
            .map_err(|_| Error::invariant("storage coordinator dropped a response"))?
    }

    pub async fn close(mut self) -> Result<()> {
        self.close_tx.take();
        Ok(())
    }

    pub fn wal_metrics(&self) -> Option<WalMetrics> {
        self.metrics.lock().ok().and_then(|metrics| metrics.clone())
    }
}

struct QueuedRequest {
    request: BatchRequest,
    response_tx: tokio::sync::oneshot::Sender<Result<BatchResponse>>,
}

async fn coordinator<F: DurableFile + Send + 'static, W: DurableFile + Send + 'static>(
    mut store: BTreeStore<F, W>,
    mut request_rx: tokio::sync::mpsc::Receiver<QueuedRequest>,
    mut close_rx: tokio::sync::oneshot::Receiver<()>,
    metrics: std::sync::Arc<std::sync::Mutex<Option<WalMetrics>>>,
) {
    loop {
        let first = tokio::select! {
            request = request_rx.recv() => request,
            _ = &mut close_rx => return,
        };
        let Some(first) = first else { return };
        let mut requests = vec![first];
        let deadline = tokio::time::sleep(BATCH_COLLECTION_WINDOW);
        tokio::pin!(deadline);
        while requests.len() < MAX_BATCH_REQUESTS {
            tokio::select! {
                request = request_rx.recv() => {
                    let Some(request) = request else { break };
                    requests.push(request);
                }
                _ = &mut deadline => break,
                _ = &mut close_rx => break,
            }
        }
        let logical_requests: Vec<_> = requests
            .iter()
            .map(|request| request.request.clone())
            .collect();
        let result = store.apply_batch(&logical_requests);
        if let Ok(mut current) = metrics.lock() {
            *current = store.wal_metrics().ok().flatten();
        }
        match result {
            Ok(responses) => {
                for (queued, response) in requests.into_iter().zip(responses) {
                    let _ = queued.response_tx.send(Ok(response));
                }
            }
            Err(error) => {
                for queued in requests {
                    let _ = queued.response_tx.send(Err(batch_error(&error)));
                }
            }
        }
    }
}

fn batch_error(error: &Error) -> Error {
    match error {
        Error::InvalidInput(message) => {
            Error::invalid_input(format!("batch was not published: {message}"))
        }
        Error::Conflict(message) => Error::Conflict(format!("batch was not published: {message}")),
        Error::Corruption(message) => {
            Error::corruption(format!("batch was not published: {message}"))
        }
        Error::Io(error) => Error::Io(std::io::Error::new(
            error.kind(),
            format!("batch was not published: {error}"),
        )),
        Error::UnsupportedFormat(message) => {
            Error::unsupported_format(format!("batch was not published: {message}"))
        }
        Error::DurabilityFailure(message) => {
            Error::durability(format!("batch was not published: {message}"))
        }
        Error::RecoveryFailure(message) => {
            Error::recovery(format!("batch was not published: {message}"))
        }
        Error::InternalInvariantViolation(message) => {
            Error::invariant(format!("batch was not published: {message}"))
        }
    }
}
