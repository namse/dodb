use std::future::Future;
use std::pin::Pin;

use dodb_core::{
    DocumentKey, Lsn, PrimaryKey, Result, Revision, RevisionState, SortKey, TenantId,
    TransactionRequest,
};

pub type ServiceFuture<'service> =
    Pin<Box<dyn Future<Output = Result<Response>> + Send + 'service>>;

pub trait DodbService: Send + Sync {
    fn execute<'service>(
        &'service self,
        tenant: TenantId,
        request: Request,
    ) -> ServiceFuture<'service>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    pub key: DocumentKey,
    pub value: Vec<u8>,
    pub revision: Revision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Get {
        key: DocumentKey,
    },
    Put {
        key: DocumentKey,
        value: Vec<u8>,
    },
    Delete {
        key: DocumentKey,
    },
    Query {
        pk: PrimaryKey,
        exclusive_after_sk: Option<SortKey>,
        limit: usize,
    },
    Scan {
        exclusive_after_key: Option<DocumentKey>,
        limit: usize,
    },
    Batch {
        mutations: Vec<dodb_core::TransactionMutation>,
    },
    TransactGet {
        keys: Vec<DocumentKey>,
    },
    Transact {
        request: TransactionRequest,
    },
}

impl Request {
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::Get { .. } => "get",
            Self::Put { .. } => "put",
            Self::Delete { .. } => "delete",
            Self::Query { .. } => "query",
            Self::Scan { .. } => "scan",
            Self::Batch { .. } => "batch",
            Self::TransactGet { .. } => "transact_get",
            Self::Transact { .. } => "transact",
        }
    }

    pub fn is_mutation(&self) -> bool {
        match self {
            Self::Put { .. } | Self::Delete { .. } | Self::Batch { .. } => true,
            Self::Transact { request } => !request.mutations.is_empty(),
            Self::Get { .. }
            | Self::Query { .. }
            | Self::Scan { .. }
            | Self::TransactGet { .. } => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Response {
    Get(RevisionState),
    Put(Revision),
    Delete(Revision),
    Query(Vec<Document>),
    Scan(Vec<Document>),
    Batch(TransactionOutcome),
    TransactGet(Vec<RevisionState>),
    Transact(TransactionOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionOutcome {
    pub commit_lsn: Option<Lsn>,
}

impl TransactionOutcome {
    pub const fn committed(commit_lsn: Lsn) -> Self {
        Self {
            commit_lsn: Some(commit_lsn),
        }
    }

    pub const fn conditions_satisfied() -> Self {
        Self { commit_lsn: None }
    }
}
