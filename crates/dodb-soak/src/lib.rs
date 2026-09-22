//! Deterministic workload, reference-model, and reporting primitives for the
//! `dodb-soak` executable.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::time::Duration;

use dodb_core::{DocumentKey, PrimaryKey, Revision, RevisionState, TenantId};
use dodb_service::Document;
use serde::Serialize;

/// The operation mix is deliberately explicit so a run can be reproduced
/// without relying on process-global or thread-local entropy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Get,
    Put,
    Delete,
    Query,
    Scan,
    TransactGet,
    Transact,
}

impl OperationKind {
    pub const ALL: [Self; 7] = [
        Self::Get,
        Self::Put,
        Self::Delete,
        Self::Query,
        Self::Scan,
        Self::TransactGet,
        Self::Transact,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::Query => "query",
            Self::Scan => "scan",
            Self::TransactGet => "transact_get",
            Self::Transact => "transact",
        }
    }

    pub const fn counter_index(self) -> usize {
        match self {
            Self::Get => 0,
            Self::Put => 1,
            Self::Delete => 2,
            Self::Query => 3,
            Self::Scan => 4,
            Self::TransactGet => 5,
            Self::Transact => 6,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannedMutationKind {
    Put,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedMutation {
    pub key: DocumentKey,
    pub kind: PlannedMutationKind,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GeneratedOperation {
    Get {
        tenant: TenantId,
        key: DocumentKey,
    },
    Put {
        tenant: TenantId,
        key: DocumentKey,
        value: Vec<u8>,
    },
    Delete {
        tenant: TenantId,
        key: DocumentKey,
    },
    Query {
        tenant: TenantId,
        pk: PrimaryKey,
        exclusive_after_sk: Option<dodb_core::SortKey>,
        limit: usize,
    },
    Scan {
        tenant: TenantId,
        exclusive_after_key: Option<DocumentKey>,
        limit: usize,
    },
    TransactGet {
        tenant: TenantId,
        keys: Vec<DocumentKey>,
    },
    Transact {
        tenant: TenantId,
        mutations: Vec<PlannedMutation>,
    },
}

impl GeneratedOperation {
    pub const fn kind(&self) -> OperationKind {
        match self {
            Self::Get { .. } => OperationKind::Get,
            Self::Put { .. } => OperationKind::Put,
            Self::Delete { .. } => OperationKind::Delete,
            Self::Query { .. } => OperationKind::Query,
            Self::Scan { .. } => OperationKind::Scan,
            Self::TransactGet { .. } => OperationKind::TransactGet,
            Self::Transact { .. } => OperationKind::Transact,
        }
    }

    pub fn tenant(&self) -> TenantId {
        match self {
            Self::Get { tenant, .. }
            | Self::Put { tenant, .. }
            | Self::Delete { tenant, .. }
            | Self::Query { tenant, .. }
            | Self::Scan { tenant, .. }
            | Self::TransactGet { tenant, .. }
            | Self::Transact { tenant, .. } => *tenant,
        }
    }

    pub fn primary_key(&self) -> Option<&PrimaryKey> {
        match self {
            Self::Query { pk, .. } => Some(pk),
            _ => None,
        }
    }

    pub fn keys(&self) -> Vec<&DocumentKey> {
        match self {
            Self::Get { key, .. } | Self::Put { key, .. } | Self::Delete { key, .. } => vec![key],
            Self::Query { .. } => Vec::new(),
            Self::Scan {
                exclusive_after_key,
                ..
            } => exclusive_after_key.iter().collect(),
            Self::TransactGet { keys, .. } => keys.iter().collect(),
            Self::Transact { mutations, .. } => {
                mutations.iter().map(|mutation| &mutation.key).collect()
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadConfig {
    pub tenant_count: u64,
    pub hot_key_count: u32,
    pub wide_key_count: u32,
    pub hot_percent: u32,
    pub operation_weights: [u32; 7],
    pub max_query_limit: usize,
    pub max_scan_limit: usize,
    pub max_transaction_mutations: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            tenant_count: 16,
            hot_key_count: 8,
            wide_key_count: 16_384,
            hot_percent: 80,
            operation_weights: [35, 20, 10, 10, 5, 10, 10],
            max_query_limit: 32,
            max_scan_limit: 64,
            max_transaction_mutations: 3,
        }
    }
}

impl WorkloadConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.tenant_count == 0
            || self.hot_key_count == 0
            || self.wide_key_count == 0
            || self.max_query_limit == 0
            || self.max_scan_limit == 0
            || self.max_transaction_mutations == 0
        {
            return Err("tenant, key, limit, and transaction counts must be nonzero".to_owned());
        }
        if self.hot_percent > 100 {
            return Err("hot-percent must be between 0 and 100".to_owned());
        }
        if self.operation_weights.iter().sum::<u32>() == 0 {
            return Err("at least one operation weight must be nonzero".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 7;
        value ^= value >> 9;
        value ^= value << 8;
        self.state = value;
        value
    }

    fn below(&mut self, upper: u64) -> u64 {
        if upper == 0 {
            0
        } else {
            self.next_u64() % upper
        }
    }
}

pub struct OperationGenerator {
    rng: DeterministicRng,
    config: WorkloadConfig,
}

impl OperationGenerator {
    pub fn new(seed: u64, config: WorkloadConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            rng: DeterministicRng::new(seed),
            config,
        })
    }

    pub fn next(&mut self, operation_index: u64) -> GeneratedOperation {
        if let Some(operation) = self.transition_operation(operation_index) {
            return operation;
        }
        let kind = self.choose_kind();
        let tenant = TenantId::new(self.rng.below(self.config.tenant_count) + 1);
        match kind {
            OperationKind::Get => GeneratedOperation::Get {
                tenant,
                key: self.key(tenant),
            },
            OperationKind::Put => {
                let mut key = self.key(tenant);
                let value = self.value_for_key(operation_index, &key);
                if value.len() > 512 {
                    key = self.unique_value_key(tenant, operation_index, 0);
                }
                GeneratedOperation::Put { tenant, key, value }
            }
            OperationKind::Delete => GeneratedOperation::Delete {
                tenant,
                key: self.wide_key(tenant),
            },
            OperationKind::Query => {
                let pk = self.primary_key(tenant);
                let exclusive_after_sk = (self.rng.below(4) == 0).then(|| {
                    dodb_core::SortKey::new(format!(
                        "hot-sk-{}",
                        self.rng.below(u64::from(self.config.hot_key_count))
                    ))
                });
                GeneratedOperation::Query {
                    tenant,
                    pk,
                    exclusive_after_sk,
                    limit: self.limit(self.config.max_query_limit),
                }
            }
            OperationKind::Scan => GeneratedOperation::Scan {
                tenant,
                exclusive_after_key: (self.rng.below(5) == 0).then(|| self.key(tenant)),
                limit: self.limit(self.config.max_scan_limit),
            },
            OperationKind::TransactGet => {
                let key_count = 1 + self.rng.below(4) as usize;
                GeneratedOperation::TransactGet {
                    tenant,
                    keys: (0..key_count).map(|_| self.key(tenant)).collect(),
                }
            }
            OperationKind::Transact => {
                let mutation_count =
                    1 + self.rng.below(self.config.max_transaction_mutations as u64) as usize;
                let mut mutations = Vec::with_capacity(mutation_count);
                while mutations.len() < mutation_count {
                    let key = if mutation_count > 1 {
                        self.unique_value_key(tenant, operation_index, mutations.len() as u64)
                    } else {
                        self.key(tenant)
                    };
                    if mutations
                        .iter()
                        .any(|mutation: &PlannedMutation| mutation.key == key)
                    {
                        continue;
                    }
                    let kind = if self.rng.below(4) == 0 {
                        PlannedMutationKind::Delete
                    } else {
                        PlannedMutationKind::Put
                    };
                    let key = if matches!(kind, PlannedMutationKind::Delete)
                        && key
                            .pk
                            .as_bytes()
                            .windows(5)
                            .any(|window| window == b"-hot-")
                    {
                        self.unique_value_key(tenant, operation_index, mutations.len() as u64)
                    } else {
                        key
                    };
                    let value = self.value_for_key(operation_index, &key);
                    let key = if value.len() > 512 {
                        self.unique_value_key(tenant, operation_index, mutations.len() as u64)
                    } else {
                        key
                    };
                    mutations.push(PlannedMutation { key, kind, value });
                }
                GeneratedOperation::Transact { tenant, mutations }
            }
        }
    }

    fn transition_operation(&mut self, operation_index: u64) -> Option<GeneratedOperation> {
        let step = operation_index % 64;
        if step >= 4 {
            return None;
        }
        let tenant = TenantId::new((operation_index % self.config.tenant_count) + 1);
        let key = DocumentKey::new(
            format!(
                "tenant-{}-hot-transition-pk-{}",
                tenant.get(),
                operation_index / 64
            ),
            "transition-sk",
        );
        let value = self.value_for_key(operation_index, &key);
        Some(match step {
            0 | 2 => GeneratedOperation::Put { tenant, key, value },
            _ => GeneratedOperation::Delete { tenant, key },
        })
    }

    fn choose_kind(&mut self) -> OperationKind {
        let total = self.config.operation_weights.iter().sum::<u32>();
        let mut choice = self.rng.below(u64::from(total)) as u32;
        for (index, weight) in self.config.operation_weights.iter().enumerate() {
            if choice < *weight {
                return OperationKind::ALL[index];
            }
            choice -= *weight;
        }
        OperationKind::Transact
    }

    fn key(&mut self, tenant: TenantId) -> DocumentKey {
        if self.rng.below(100) < u64::from(self.config.hot_percent) {
            let index = self.rng.below(u64::from(self.config.hot_key_count)) as u32;
            self.hot_key(tenant, index)
        } else {
            self.wide_key(tenant)
        }
    }

    fn wide_key(&mut self, tenant: TenantId) -> DocumentKey {
        let index = self.rng.below(u64::from(self.config.wide_key_count));
        DocumentKey::new(
            format!("tenant-{}-wide-pk-{}", tenant.get(), index % 64),
            format!("wide-sk-{index}"),
        )
    }

    fn hot_key(&self, tenant: TenantId, index: u32) -> DocumentKey {
        DocumentKey::new(
            format!("tenant-{}-hot-pk-{}", tenant.get(), index % 8),
            format!("hot-sk-{index}"),
        )
    }

    fn unique_value_key(&self, tenant: TenantId, operation_index: u64, salt: u64) -> DocumentKey {
        DocumentKey::new(
            format!("tenant-{}-wide-value-pk", tenant.get()),
            format!("wide-value-{operation_index}-{salt}"),
        )
    }

    fn primary_key(&mut self, tenant: TenantId) -> PrimaryKey {
        if self.rng.below(100) < u64::from(self.config.hot_percent) {
            PrimaryKey::new(format!(
                "tenant-{}-hot-pk-{}",
                tenant.get(),
                self.rng.below(8)
            ))
        } else {
            PrimaryKey::new(format!(
                "tenant-{}-wide-pk-{}",
                tenant.get(),
                self.rng.below(64)
            ))
        }
    }

    fn limit(&mut self, maximum: usize) -> usize {
        1 + self.rng.below(maximum as u64) as usize
    }

    fn value(&mut self, operation_index: u64) -> Vec<u8> {
        let selector = self.rng.below(10_000);
        let size = if selector < 7_000 {
            32 + self.rng.below(224) as usize
        } else if selector < 9_200 {
            512 + self.rng.below(4 * 1024) as usize
        } else if selector < 9_850 {
            8 * 1024 + self.rng.below(128 * 1024) as usize
        } else if selector < 9_995 {
            256 * 1024 + self.rng.below(2 * 1024 * 1024) as usize
        } else {
            32 * 1024 * 1024 + self.rng.below(8 * 1024 * 1024) as usize
        };
        let prefix = format!("soak_operation_id={operation_index};payload=");
        let mut value = Vec::with_capacity(size.max(prefix.len()));
        value.extend_from_slice(prefix.as_bytes());
        while value.len() < size {
            value.push((operation_index as u8).wrapping_add(value.len() as u8));
        }
        value.truncate(size);
        value
    }

    fn value_for_key(&mut self, operation_index: u64, key: &DocumentKey) -> Vec<u8> {
        if key
            .pk
            .as_bytes()
            .windows(5)
            .any(|window| window == b"-hot-")
        {
            let size = 64;
            let prefix = format!("soak_operation_id={operation_index};payload=");
            let mut value = Vec::with_capacity(size.max(prefix.len()));
            value.extend_from_slice(prefix.as_bytes());
            while value.len() < size {
                value.push((operation_index as u8).wrapping_add(value.len() as u8));
            }
            value.truncate(size);
            value
        } else {
            self.value(operation_index)
        }
    }
}

/// A logical model of the committed state. Revisions are copied from actual
/// successful dodb responses; this model never predicts a future revision.
#[derive(Clone, Debug, Default)]
pub struct ReferenceModel {
    states: BTreeMap<TenantId, BTreeMap<DocumentKey, RevisionState>>,
    history: BTreeMap<(TenantId, DocumentKey), Vec<RevisionState>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownMutationResolution {
    Applied,
    NotApplied,
    Partial,
}

pub fn classify_unknown_mutation(
    operation: &GeneratedOperation,
    before: &[RevisionState],
    actual: &[RevisionState],
    current: &[RevisionState],
) -> UnknownMutationResolution {
    if actual == current {
        return UnknownMutationResolution::NotApplied;
    }
    let applied = match operation {
        GeneratedOperation::Put { value, .. } => {
            actual.len() == 1
                && matches!(&actual[0], RevisionState::Present { value: actual_value, .. } if actual_value == value)
        }
        GeneratedOperation::Delete { .. } => actual.len() == 1 && actual[0].is_missing(),
        GeneratedOperation::Transact { mutations, .. } => {
            actual.len() == mutations.len()
                && mutations.iter().zip(actual.iter()).all(|(mutation, state)| match mutation.kind {
                    PlannedMutationKind::Put => matches!(state, RevisionState::Present { value, .. } if value == &mutation.value),
                    PlannedMutationKind::Delete => state.is_missing(),
                })
        }
        _ => false,
    };
    if applied {
        UnknownMutationResolution::Applied
    } else if actual == before {
        UnknownMutationResolution::NotApplied
    } else {
        UnknownMutationResolution::Partial
    }
}

impl ReferenceModel {
    pub fn state(&self, tenant: TenantId, key: &DocumentKey) -> RevisionState {
        self.states
            .get(&tenant)
            .and_then(|states| states.get(key))
            .cloned()
            .unwrap_or_else(|| RevisionState::missing(Revision::ZERO))
    }

    pub fn apply(&mut self, tenant: TenantId, key: DocumentKey, state: RevisionState) {
        let history = self.history.entry((tenant, key.clone())).or_default();
        if history.last() != Some(&state) {
            history.push(state.clone());
            if history.len() > 64 {
                history.remove(0);
            }
        }
        let states = self.states.entry(tenant).or_default();
        let should_publish = states
            .get(&key)
            .is_none_or(|current| current.revision() <= state.revision());
        if should_publish {
            states.insert(key, state);
        }
    }

    pub fn apply_put(
        &mut self,
        tenant: TenantId,
        key: DocumentKey,
        value: Vec<u8>,
        revision: Revision,
    ) {
        self.apply(tenant, key, RevisionState::present(value, revision));
    }

    pub fn apply_delete(&mut self, tenant: TenantId, key: DocumentKey, revision: Revision) {
        self.apply(tenant, key, RevisionState::missing(revision));
    }

    pub fn knows_state(&self, tenant: TenantId, key: &DocumentKey, state: &RevisionState) -> bool {
        (*state == RevisionState::missing(Revision::ZERO))
            || self
                .history
                .get(&(tenant, key.clone()))
                .is_some_and(|history| history.iter().any(|known| known == state))
            || self.state(tenant, key) == *state
    }

    pub fn knows_revision(&self, tenant: TenantId, key: &DocumentKey, revision: Revision) -> bool {
        self.history
            .get(&(tenant, key.clone()))
            .is_some_and(|history| history.iter().any(|state| state.revision() == revision))
            || self.state(tenant, key).revision() == revision
    }

    pub fn scan(
        &self,
        tenant: TenantId,
        cursor: Option<&DocumentKey>,
        limit: usize,
    ) -> Vec<Document> {
        self.states
            .get(&tenant)
            .into_iter()
            .flat_map(|states| states.iter())
            .filter_map(|(key, state)| {
                if cursor.is_some_and(|cursor| key <= cursor) {
                    return None;
                }
                match state {
                    RevisionState::Present { value, revision } => Some(Document {
                        key: key.clone(),
                        value: value.clone(),
                        revision: *revision,
                    }),
                    RevisionState::Missing { .. } => None,
                }
            })
            .take(limit)
            .collect()
    }

    pub fn query(
        &self,
        tenant: TenantId,
        pk: &PrimaryKey,
        cursor: Option<&dodb_core::SortKey>,
        limit: usize,
    ) -> Vec<Document> {
        self.scan(tenant, None, usize::MAX)
            .into_iter()
            .filter(|document| {
                &document.key.pk == pk && cursor.is_none_or(|cursor| &document.key.sk > cursor)
            })
            .take(limit)
            .collect()
    }

    pub fn documents(&self, tenant: TenantId) -> Vec<Document> {
        self.scan(tenant, None, usize::MAX)
    }

    pub fn keys(&self, tenant: TenantId) -> Vec<DocumentKey> {
        self.states
            .get(&tenant)
            .into_iter()
            .flat_map(|states| states.keys().cloned())
            .collect()
    }

    pub fn all_documents(&self) -> Vec<(TenantId, Document)> {
        self.tenants()
            .flat_map(|(tenant, documents)| {
                documents
                    .into_iter()
                    .map(move |document| (tenant, document))
            })
            .collect()
    }

    pub fn tenants(&self) -> impl Iterator<Item = (TenantId, Vec<Document>)> + '_ {
        self.states
            .keys()
            .copied()
            .map(|tenant| (tenant, self.scan(tenant, None, usize::MAX)))
    }

    pub fn known_key_count(&self) -> usize {
        self.states.values().map(BTreeMap::len).sum()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct OperationRecord {
    pub index: u64,
    pub elapsed_ms: u128,
    pub tenant: u64,
    pub operation: String,
    pub status: String,
}

#[derive(Clone, Debug)]
pub struct RecentOperations {
    capacity: usize,
    entries: VecDeque<OperationRecord>,
}

impl RecentOperations {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::with_capacity(capacity.max(1)),
        }
    }

    pub fn push(&mut self, record: OperationRecord) {
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(record);
    }

    pub fn as_vec(&self) -> Vec<OperationRecord> {
        self.entries.iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct LatencyStats {
    samples: Vec<u64>,
    count: u64,
    maximum: u64,
}

impl LatencyStats {
    pub fn record(&mut self, nanos: u64) {
        self.count = self.count.saturating_add(1);
        self.maximum = self.maximum.max(nanos);
        const SAMPLE_LIMIT: usize = 200_000;
        if self.samples.len() < SAMPLE_LIMIT {
            self.samples.push(nanos);
        } else {
            self.samples[(self.count as usize) % SAMPLE_LIMIT] = nanos;
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn maximum(&self) -> u64 {
        self.maximum
    }

    pub fn percentile(&self, percentile: f64) -> u64 {
        percentile_value(&self.samples, percentile)
    }
}

pub fn percentile_value(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let clamped = percentile.clamp(0.0, 100.0) / 100.0;
    let position = ((sorted.len() - 1) as f64 * clamped).round() as usize;
    sorted[position]
}

#[derive(Clone, Debug, Serialize)]
pub struct ResourceSample {
    pub elapsed_ms: u128,
    pub rss_bytes: Option<u64>,
    pub virtual_bytes: Option<u64>,
    pub fd_count: Option<u64>,
    pub thread_count: Option<u64>,
    pub wal_bytes: u64,
    pub database_bytes: u64,
    pub active_connections: Option<u64>,
    pub active_streams: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrendDiagnostic {
    pub metric: String,
    pub warning: bool,
    pub warmup_ignored: bool,
    pub first_late_window: u64,
    pub last_late_window: u64,
    pub slope_per_second: f64,
    pub detail: String,
}

pub fn trend_diagnostic(metric: &str, samples: &[(f64, u64)], warmup: Duration) -> TrendDiagnostic {
    if samples.len() < 6 {
        return TrendDiagnostic {
            metric: metric.to_owned(),
            warning: false,
            warmup_ignored: false,
            first_late_window: samples.first().map_or(0, |sample| sample.1),
            last_late_window: samples.last().map_or(0, |sample| sample.1),
            slope_per_second: 0.0,
            detail: "insufficient samples".to_owned(),
        };
    }
    let cutoff = samples.first().map_or(0.0, |sample| sample.0) + warmup.as_secs_f64();
    let late = samples
        .iter()
        .filter(|sample| sample.0 >= cutoff)
        .collect::<Vec<_>>();
    let window = (late.len() / 3).max(1);
    let means = [
        mean_window(&late[..window]),
        mean_window(
            &late[late.len().saturating_sub(2 * window)..late.len().saturating_sub(window)],
        ),
        mean_window(&late[late.len().saturating_sub(window)..]),
    ];
    let first = means[0].round() as u64;
    let last = means[2].round() as u64;
    let growth = last.saturating_sub(first);
    let threshold = first.max(4) / 4;
    let warning = means[1] >= means[0] && means[2] >= means[1] && growth > threshold;
    let elapsed =
        late.last().map_or(1.0, |sample| sample.0) - late.first().map_or(0.0, |sample| sample.0);
    TrendDiagnostic {
        metric: metric.to_owned(),
        warning,
        warmup_ignored: true,
        first_late_window: first,
        last_late_window: last,
        slope_per_second: if elapsed > 0.0 {
            (last as f64 - first as f64) / elapsed
        } else {
            0.0
        },
        detail: if warning {
            "later windows continue to rise after warm-up".to_owned()
        } else {
            "no sustained late-window growth detected".to_owned()
        },
    }
}

fn mean_window(samples: &[&(f64, u64)]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        samples.iter().map(|sample| sample.1 as f64).sum::<f64>() / samples.len() as f64
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(pk: &[u8], sk: &[u8]) -> DocumentKey {
        DocumentKey::new(pk.to_vec(), sk.to_vec())
    }

    #[test]
    fn same_seed_produces_same_logical_sequence() {
        let config = WorkloadConfig {
            tenant_count: 3,
            hot_key_count: 5,
            wide_key_count: 20,
            ..WorkloadConfig::default()
        };
        let mut left = OperationGenerator::new(7, config.clone()).unwrap();
        let mut right = OperationGenerator::new(7, config).unwrap();
        for index in 0..200 {
            assert_eq!(left.next(index), right.next(index));
        }
    }

    #[test]
    fn model_preserves_missing_delete_revision_and_aba_history() {
        let tenant = TenantId::new(1);
        let document = key(b"p", b"s");
        let mut model = ReferenceModel::default();
        assert_eq!(
            model.state(tenant, &document),
            RevisionState::missing(Revision::ZERO)
        );
        model.apply_put(tenant, document.clone(), b"a".to_vec(), Revision::new(4));
        model.apply_delete(tenant, document.clone(), Revision::new(5));
        assert_eq!(
            model.state(tenant, &document),
            RevisionState::missing(Revision::new(5))
        );
        assert!(model.knows_state(tenant, &document, &RevisionState::missing(Revision::ZERO)));
        assert!(model.knows_state(
            tenant,
            &document,
            &RevisionState::present(b"a".to_vec(), Revision::new(4))
        ));
    }

    #[test]
    fn unknown_multi_key_mutation_accepts_only_all_or_none() {
        let tenant = TenantId::new(1);
        let first = key(b"p", b"a");
        let second = key(b"p", b"b");
        let operation = GeneratedOperation::Transact {
            tenant,
            mutations: vec![
                PlannedMutation {
                    key: first.clone(),
                    kind: PlannedMutationKind::Put,
                    value: b"first".to_vec(),
                },
                PlannedMutation {
                    key: second.clone(),
                    kind: PlannedMutationKind::Put,
                    value: b"second".to_vec(),
                },
            ],
        };
        let before = vec![RevisionState::missing(Revision::ZERO); 2];
        let current = before.clone();
        let applied = vec![
            RevisionState::present(b"first".to_vec(), Revision::new(10)),
            RevisionState::present(b"second".to_vec(), Revision::new(10)),
        ];
        assert_eq!(
            classify_unknown_mutation(&operation, &before, &applied, &current),
            UnknownMutationResolution::Applied
        );
        assert_eq!(
            classify_unknown_mutation(
                &operation,
                &before,
                &[applied[0].clone(), before[1].clone()],
                &current,
            ),
            UnknownMutationResolution::Partial
        );
    }

    #[test]
    fn percentile_and_recent_operations_are_bounded() {
        assert_eq!(percentile_value(&[1, 2, 3, 4, 5], 50.0), 3);
        let mut recent = RecentOperations::new(2);
        for index in 0..5 {
            recent.push(OperationRecord {
                index,
                elapsed_ms: 0,
                tenant: 1,
                operation: "get".to_owned(),
                status: "ok".to_owned(),
            });
        }
        assert_eq!(recent.len(), 2);
        assert_eq!(recent.as_vec()[0].index, 3);
    }

    #[test]
    fn trend_ignores_warmup_and_detects_sustained_growth() {
        let samples = (0..12)
            .map(|index| (index as f64, if index < 3 { 10 } else { 10 + index * 10 }))
            .collect::<Vec<_>>();
        assert!(trend_diagnostic("fd", &samples, Duration::from_secs(2)).warning);
    }
}
