//! Experimental serial B-link tree.
//!
//! This module is deliberately independent from [`crate::btree`].  It is the
//! Phase 1 control implementation: one caller mutates the tree at a time, but
//! pages already carry the fences and sibling links that later phases will
//! use for optimistic reads and parallel execution.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use dodb_core::{
    DocumentKey, Error, Lsn, ObservedState, PageId, PrimaryKey, Result, Revision, RevisionState,
    ShardEpoch, ShardId, SortKey, TenantId, TransactionCondition, TransactionConflict,
    TransactionMutation, TransactionRequest, TransactionResult,
};

use crate::btree::{
    BatchRequest, BatchResponse, DatabaseConfig, Document, InvariantReport, StorageLimits,
    StorageMetrics,
};
use crate::durable_file::{DurableFile, ProductionFile};
use crate::fault::FaultInjector;
use crate::page::{PAGE_HEADER_SIZE, PAGE_SIZE, PageHeader, PageType, decode_page_at, encode_page};
use crate::wal::{
    CommittedWalBatch, WalCommit, WalIdentity, WalLog, WalMetrics, WalPageImage, WalPageImageFormat,
};

const FIRST_DATA_PAGE: u64 = 2;
const NULL_PAGE_ID: u64 = u64::MAX;
const BLINK_SUPERBLOCK_MAGIC: [u8; 4] = *b"DBLK";
const BLINK_SUPERBLOCK_VERSION: u16 = 2;
const BLINK_ENGINE_TAG: [u8; 4] = *b"BLNK";
const SUPERBLOCK_CHECKSUM_OFFSET: usize = 100;
const SUPERBLOCK_ENGINE_OFFSET: usize = 96;
const BODY_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
const BODY_VERSION: u16 = 2;
const LEAF_MAGIC: [u8; 4] = *b"BLKL";
const INTERNAL_MAGIC: [u8; 4] = *b"BLKI";
const OVERFLOW_MAGIC: [u8; 4] = *b"BLKO";
const FREE_MAGIC: [u8; 4] = *b"BLKF";
const LEAF_HEADER_SIZE: usize = 48;
const INTERNAL_HEADER_SIZE: usize = 56;
const SLOT_SIZE: usize = 8;
const LEAF_RECORD_HEADER_SIZE: usize = 32;
const INTERNAL_RECORD_HEADER_SIZE: usize = 16;
const OVERFLOW_HEADER_SIZE: usize = 32;
const INLINE_VALUE_LIMIT: usize = 512;
const MAX_VALUE_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlinkSplitMetrics {
    pub leaf_splits: u64,
    pub internal_splits: u64,
    pub root_splits: u64,
    pub right_link_corrections: u64,
    pub pages_touched: u64,
    pub page_images: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProvisionalRevisionToken {
    pub transaction_position: usize,
    pub ordinal: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyKind {
    SameKey,
    ConditionKey,
    SameTargetPage,
    SameTransaction,
    StructuralRoute,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyEdge {
    pub predecessor: usize,
    pub successor: usize,
    pub kind: DependencyKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DependencyMetadata {
    pub same_key_predecessors: Vec<usize>,
    pub condition_key_predecessors: Vec<usize>,
    pub same_target_page_predecessors: Vec<usize>,
    pub same_transaction_positions: Vec<usize>,
    pub structural_route_predecessors: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteHint {
    pub encoded_key: Vec<u8>,
    pub leaf_id: PageId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedMutation {
    pub mutation: TransactionMutation,
    pub encoded_key: Vec<u8>,
    pub route_hint: RouteHint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalTransactionPlan {
    pub fifo_position: usize,
    pub provisional_revision: ProvisionalRevisionToken,
    pub mutations: Vec<PlannedMutation>,
    pub encoded_keys: Vec<Vec<u8>>,
    pub mutated_key_set: BTreeSet<Vec<u8>>,
    pub dependency_metadata: DependencyMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeafGroupPlan {
    pub leaf_hint: PageId,
    pub mutations: Vec<(usize, usize)>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchPlan {
    pub transactions: Vec<PhysicalTransactionPlan>,
    pub leaf_groups: Vec<LeafGroupPlan>,
    pub dependencies: Vec<DependencyEdge>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlinkBatchMetrics {
    pub logical_groups: u64,
    pub logical_transactions: u64,
    pub admitted_transactions: u64,
    pub conflicted_transactions: u64,
    pub rejected_transactions: u64,
    pub logical_admission_nanos: u64,
    pub planning_nanos: u64,
    pub state_clone_nanos: u64,
    pub physical_execution_nanos: u64,
    pub dirty_union_nanos: u64,
    pub full_state_clones: u64,
    pub mutations_planned: u64,
    pub routes_calculated: u64,
    pub route_reuses: u64,
    pub route_invalidations: u64,
    pub reroutes: u64,
    pub leaf_groups: u64,
    pub same_leaf_groups: u64,
    pub mutations_per_leaf_group: u64,
    pub coalesced_mutations: u64,
    pub independent_leaf_groups: u64,
    pub dependency_edges: u64,
    pub leaf_loads: u64,
    pub leaf_encodes: u64,
    pub structural_fallbacks: u64,
    pub split_triggered_reroutes: u64,
    pub coalescing_interruptions: u64,
    pub page_images: u64,
    pub wal_bytes: u64,
    pub catalog_construction_nanos: u64,
    pub catalog_map_clone_nanos: u64,
    pub catalog_state_scan_nanos: u64,
    pub wal_assembly_nanos: u64,
    pub state_install_nanos: u64,
    pub generation_publication_nanos: u64,
    pub publication_swap_nanos: u64,
    pub retired_generation_drop_nanos: u64,
    pub dirty_tracking_nanos: u64,
}

impl Default for BlinkSplitMetrics {
    fn default() -> Self {
        Self {
            leaf_splits: 0,
            internal_splits: 0,
            root_splits: 0,
            right_link_corrections: 0,
            pages_touched: 0,
            page_images: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlinkCheckpointReport {
    pub checkpoint_lsn: Lsn,
    pub pages_flushed: usize,
    pub bytes_written: u64,
    pub wal_bytes_reclaimed: u64,
    pub duration_nanos: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlinkSuperblock {
    generation: u64,
    database_uuid: [u8; 16],
    tenant_id: TenantId,
    shard_id: ShardId,
    shard_epoch: ShardEpoch,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
    checkpoint_lsn: Lsn,
}

impl BlinkSuperblock {
    fn new(config: &DatabaseConfig, root_page_id: PageId) -> Self {
        Self {
            generation: 1,
            database_uuid: config.database_uuid,
            tenant_id: config.tenant_id,
            shard_id: config.shard_id,
            shard_epoch: config.shard_epoch,
            root_page_id,
            free_list_head: None,
            high_water_page_id: root_page_id,
            checkpoint_lsn: Lsn::ZERO,
        }
    }
}

pub(crate) fn decode_blink_superblock_image(bytes: &[u8]) -> Result<BlinkSuperblock> {
    if bytes.len() != PAGE_SIZE {
        return Err(Error::corruption(
            "experimental superblock has invalid length",
        ));
    }
    if bytes[0..4] != BLINK_SUPERBLOCK_MAGIC {
        return Err(Error::corruption("experimental superblock magic mismatch"));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    if version != BLINK_SUPERBLOCK_VERSION {
        return Err(Error::unsupported_format(format!(
            "experimental superblock version {version}, supported {BLINK_SUPERBLOCK_VERSION}"
        )));
    }
    if bytes[6..8].iter().any(|byte| *byte != 0)
        || bytes[SUPERBLOCK_ENGINE_OFFSET..SUPERBLOCK_ENGINE_OFFSET + 4] != BLINK_ENGINE_TAG
        || bytes[104..].iter().any(|byte| *byte != 0)
    {
        return Err(Error::corruption(
            "experimental superblock reserved bytes or engine tag are invalid",
        ));
    }
    let stored = u32::from_le_bytes(bytes[SUPERBLOCK_CHECKSUM_OFFSET..104].try_into().unwrap());
    let mut checksum_input = [0u8; PAGE_SIZE];
    checksum_input.copy_from_slice(bytes);
    checksum_input[SUPERBLOCK_CHECKSUM_OFFSET..104].fill(0);
    if stored != crc32c::crc32c(&checksum_input) {
        return Err(Error::corruption(
            "experimental superblock checksum mismatch",
        ));
    }
    let page_size = u32::from_le_bytes(bytes[56..60].try_into().unwrap());
    if page_size as usize != PAGE_SIZE {
        return Err(Error::unsupported_format(
            "experimental superblock page size mismatch",
        ));
    }
    let root = PageId::new(u64::from_le_bytes(bytes[60..68].try_into().unwrap()));
    if root.get() < FIRST_DATA_PAGE {
        return Err(Error::corruption("experimental root page id is invalid"));
    }
    let high = PageId::new(u64::from_le_bytes(bytes[76..84].try_into().unwrap()));
    if high < root {
        return Err(Error::corruption(
            "experimental high-water mark precedes root",
        ));
    }
    Ok(BlinkSuperblock {
        generation: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        database_uuid: bytes[16..32].try_into().unwrap(),
        tenant_id: TenantId::new(u64::from_le_bytes(bytes[32..40].try_into().unwrap())),
        shard_id: ShardId::new(u64::from_le_bytes(bytes[40..48].try_into().unwrap())),
        shard_epoch: ShardEpoch::new(u64::from_le_bytes(bytes[48..56].try_into().unwrap())),
        root_page_id: root,
        free_list_head: decode_page_id(u64::from_le_bytes(bytes[68..76].try_into().unwrap())),
        high_water_page_id: high,
        checkpoint_lsn: Lsn::new(u64::from_le_bytes(bytes[84..92].try_into().unwrap())),
    })
}

fn encode_blink_superblock(sb: &BlinkSuperblock) -> Result<[u8; PAGE_SIZE]> {
    if sb.root_page_id.get() < FIRST_DATA_PAGE || sb.high_water_page_id < sb.root_page_id {
        return Err(Error::invalid_input(
            "experimental superblock page metadata is invalid",
        ));
    }
    let mut bytes = [0u8; PAGE_SIZE];
    bytes[0..4].copy_from_slice(&BLINK_SUPERBLOCK_MAGIC);
    bytes[4..6].copy_from_slice(&BLINK_SUPERBLOCK_VERSION.to_le_bytes());
    bytes[8..16].copy_from_slice(&sb.generation.to_le_bytes());
    bytes[16..32].copy_from_slice(&sb.database_uuid);
    bytes[32..40].copy_from_slice(&sb.tenant_id.get().to_le_bytes());
    bytes[40..48].copy_from_slice(&sb.shard_id.get().to_le_bytes());
    bytes[48..56].copy_from_slice(&sb.shard_epoch.get().to_le_bytes());
    bytes[56..60].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    bytes[60..68].copy_from_slice(&sb.root_page_id.get().to_le_bytes());
    bytes[68..76].copy_from_slice(&encode_page_id(sb.free_list_head).to_le_bytes());
    bytes[76..84].copy_from_slice(&sb.high_water_page_id.get().to_le_bytes());
    bytes[84..92].copy_from_slice(&sb.checkpoint_lsn.get().to_le_bytes());
    bytes[SUPERBLOCK_ENGINE_OFFSET..SUPERBLOCK_ENGINE_OFFSET + 4]
        .copy_from_slice(&BLINK_ENGINE_TAG);
    let checksum = crc32c::crc32c(&bytes);
    bytes[SUPERBLOCK_CHECKSUM_OFFSET..104].copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BlinkValueRef {
    Inline(Vec<u8>),
    Overflow { head: PageId, length: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LeafEntry {
    key: Vec<u8>,
    revision: Revision,
    value: Option<BlinkValueRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InternalEntry {
    key: Vec<u8>,
    right_child: PageId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BlinkPage {
    Leaf {
        lsn: Lsn,
        high_key: Option<Vec<u8>>,
        right_sibling: Option<PageId>,
        entries: Vec<LeafEntry>,
    },
    Internal {
        lsn: Lsn,
        level: u16,
        high_key: Option<Vec<u8>>,
        right_sibling: Option<PageId>,
        leftmost_child: PageId,
        entries: Vec<InternalEntry>,
    },
    Overflow {
        lsn: Lsn,
        next: Option<PageId>,
        total_length: u64,
        chunk: Vec<u8>,
    },
    Free {
        lsn: Lsn,
        next: Option<PageId>,
    },
}

impl BlinkPage {
    fn page_type(&self) -> PageType {
        match self {
            Self::Leaf { .. } => PageType::Leaf,
            Self::Internal { .. } => PageType::Internal,
            Self::Overflow { .. } => PageType::Overflow,
            Self::Free { .. } => PageType::Free,
        }
    }

    fn lsn(&self) -> Lsn {
        match self {
            Self::Leaf { lsn, .. }
            | Self::Internal { lsn, .. }
            | Self::Overflow { lsn, .. }
            | Self::Free { lsn, .. } => *lsn,
        }
    }

    fn restamp(&mut self, provisional: Revision, committed: Lsn, mutated_keys: &BTreeSet<Vec<u8>>) {
        match self {
            Self::Leaf { lsn, entries, .. } => {
                *lsn = committed;
                for entry in entries {
                    if entry.revision == provisional && mutated_keys.contains(&entry.key) {
                        entry.revision = Revision::from(committed);
                    }
                }
            }
            Self::Internal { lsn, .. } | Self::Overflow { lsn, .. } | Self::Free { lsn, .. } => {
                *lsn = committed
            }
        }
    }
}

#[derive(Clone, Debug)]
struct BlinkState {
    pages: BTreeMap<PageId, BlinkPage>,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
    allow_page_reuse: bool,
}

trait BlinkMutationState {
    fn root_page_id(&self) -> PageId;
    fn set_root_page_id(&mut self, value: PageId);
    fn free_list_head(&self) -> Option<PageId>;
    fn set_free_list_head(&mut self, value: Option<PageId>);
    fn high_water_page_id(&self) -> PageId;
    fn set_high_water_page_id(&mut self, value: PageId);
    fn allow_page_reuse(&self) -> bool;
    fn page(&self, page_id: PageId) -> Option<&BlinkPage>;
    fn insert_page(&mut self, page_id: PageId, page: BlinkPage);
}

impl BlinkMutationState for BlinkState {
    fn root_page_id(&self) -> PageId {
        self.root_page_id
    }
    fn set_root_page_id(&mut self, value: PageId) {
        self.root_page_id = value;
    }
    fn free_list_head(&self) -> Option<PageId> {
        self.free_list_head
    }
    fn set_free_list_head(&mut self, value: Option<PageId>) {
        self.free_list_head = value;
    }
    fn high_water_page_id(&self) -> PageId {
        self.high_water_page_id
    }
    fn set_high_water_page_id(&mut self, value: PageId) {
        self.high_water_page_id = value;
    }
    fn allow_page_reuse(&self) -> bool {
        self.allow_page_reuse
    }
    fn page(&self, page_id: PageId) -> Option<&BlinkPage> {
        self.pages.get(&page_id)
    }
    fn insert_page(&mut self, page_id: PageId, page: BlinkPage) {
        self.pages.insert(page_id, page);
    }
}

struct WorkingBlinkState<'a> {
    base: &'a BlinkState,
    pages: BTreeMap<PageId, BlinkPage>,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
    allow_page_reuse: bool,
}

impl<'a> WorkingBlinkState<'a> {
    fn new(base: &'a BlinkState, allow_page_reuse: bool) -> Self {
        Self {
            base,
            pages: BTreeMap::new(),
            root_page_id: base.root_page_id,
            free_list_head: base.free_list_head,
            high_water_page_id: base.high_water_page_id,
            allow_page_reuse,
        }
    }

    fn overlay_page_mut(&mut self, page_id: PageId) -> Option<&mut BlinkPage> {
        self.pages.get_mut(&page_id)
    }

    fn into_delta(self) -> BlinkStateDelta {
        BlinkStateDelta {
            pages: self.pages,
            root_page_id: self.root_page_id,
            free_list_head: self.free_list_head,
            high_water_page_id: self.high_water_page_id,
            allow_page_reuse: self.allow_page_reuse,
        }
    }
}

impl BlinkMutationState for WorkingBlinkState<'_> {
    fn root_page_id(&self) -> PageId {
        self.root_page_id
    }
    fn set_root_page_id(&mut self, value: PageId) {
        self.root_page_id = value;
    }
    fn free_list_head(&self) -> Option<PageId> {
        self.free_list_head
    }
    fn set_free_list_head(&mut self, value: Option<PageId>) {
        self.free_list_head = value;
    }
    fn high_water_page_id(&self) -> PageId {
        self.high_water_page_id
    }
    fn set_high_water_page_id(&mut self, value: PageId) {
        self.high_water_page_id = value;
    }
    fn allow_page_reuse(&self) -> bool {
        self.allow_page_reuse
    }
    fn page(&self, page_id: PageId) -> Option<&BlinkPage> {
        self.pages
            .get(&page_id)
            .or_else(|| self.base.pages.get(&page_id))
    }
    fn insert_page(&mut self, page_id: PageId, page: BlinkPage) {
        self.pages.insert(page_id, page);
    }
}

struct BlinkStateDelta {
    pages: BTreeMap<PageId, BlinkPage>,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
    allow_page_reuse: bool,
}

#[derive(Clone, Debug)]
enum LogicalRevision {
    Provisional(ProvisionalRevisionToken),
}

#[derive(Clone, Debug)]
struct LogicalEntry {
    present: bool,
    value: Option<Vec<u8>>,
    revision: LogicalRevision,
    originating_transaction_position: usize,
}

struct LogicalOverlay<'a> {
    committed: &'a BlinkState,
    entries: BTreeMap<Vec<u8>, LogicalEntry>,
}

struct AdmittedTransaction {
    fifo_position: usize,
    request: TransactionRequest,
    provisional_revision: ProvisionalRevisionToken,
}

struct CachedLeaf {
    leaf_id: PageId,
    page: BlinkPage,
}

struct PhysicalExecutionState {
    cached_leaf: Option<CachedLeaf>,
}

#[derive(Clone, Debug)]
struct PageVersion {
    epoch: u64,
    page: Arc<BlinkPage>,
}

/// A generation-scoped immutable page cell. A later generation may replace the
/// cell for the same logical PageId, but a pinned older generation retains this
/// cell and therefore the old page object until its last reader releases it.
#[derive(Debug)]
struct PageCell {
    version: PageVersion,
    metrics: Arc<PublicationMetrics>,
}

impl PageCell {
    fn page_at(&self, epoch: u64) -> Result<Arc<BlinkPage>> {
        if self.version.epoch > epoch {
            return Err(Error::corruption(
                "published page version is newer than its generation",
            ));
        }
        Ok(Arc::clone(&self.version.page))
    }
}

impl Drop for PageCell {
    fn drop(&mut self) {
        self.metrics
            .versions_reclaimed
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct PageCatalog {
    pages: BTreeMap<PageId, Arc<PageCell>>,
}

#[derive(Debug)]
struct PublishedGeneration {
    epoch: u64,
    root_page_id: PageId,
    high_water_page_id: PageId,
    catalog: Arc<PageCatalog>,
}

#[derive(Debug, Default)]
struct PublicationMetrics {
    generation_pins: AtomicU64,
    active_generation_pins: AtomicU64,
    max_concurrent_pins: AtomicU64,
    page_version_installs: AtomicU64,
    versions_reclaimed: AtomicU64,
    published_generations: AtomicU64,
    right_link_corrections: AtomicU64,
    read_operations: AtomicU64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlinkVersionedReadMetrics {
    pub published_generations: u64,
    pub active_generation_pins: u64,
    pub max_concurrent_pins: u64,
    pub generation_pins: u64,
    pub page_version_installs: u64,
    pub versions_retained: u64,
    pub versions_reclaimed: u64,
    pub retired_page_ids: u64,
    pub reusable_page_ids: u64,
    pub read_operations: u64,
    pub right_link_corrections: u64,
}

impl PublicationMetrics {
    fn snapshot(&self, retired_page_ids: u64, reusable_page_ids: u64) -> BlinkVersionedReadMetrics {
        let installed = self.page_version_installs.load(Ordering::Relaxed);
        let reclaimed = self.versions_reclaimed.load(Ordering::Relaxed);
        BlinkVersionedReadMetrics {
            published_generations: self.published_generations.load(Ordering::Relaxed),
            active_generation_pins: self.active_generation_pins.load(Ordering::Relaxed),
            max_concurrent_pins: self.max_concurrent_pins.load(Ordering::Relaxed),
            generation_pins: self.generation_pins.load(Ordering::Relaxed),
            page_version_installs: installed,
            versions_retained: installed.saturating_sub(reclaimed),
            versions_reclaimed: reclaimed,
            retired_page_ids,
            reusable_page_ids,
            read_operations: self.read_operations.load(Ordering::Relaxed),
            right_link_corrections: self.right_link_corrections.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PublicationPrepareTiming {
    catalog_map_clone_nanos: u64,
    catalog_state_scan_nanos: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct PublicationPublishTiming {
    swap_nanos: u64,
    retired_generation_drop_nanos: u64,
}

#[derive(Debug)]
struct GenerationPublisher {
    current: RwLock<Arc<PublishedGeneration>>,
    metrics: Arc<PublicationMetrics>,
}

impl GenerationPublisher {
    fn new(state: &BlinkState, epoch: u64) -> Arc<Self> {
        let metrics = Arc::new(PublicationMetrics::default());
        let mut pages = BTreeMap::new();
        for (page_id, page) in &state.pages {
            pages.insert(
                *page_id,
                Arc::new(PageCell {
                    version: PageVersion {
                        epoch,
                        page: Arc::new(page.clone()),
                    },
                    metrics: Arc::clone(&metrics),
                }),
            );
            metrics
                .page_version_installs
                .fetch_add(1, Ordering::Relaxed);
        }
        let generation = Arc::new(PublishedGeneration {
            epoch,
            root_page_id: state.root_page_id,
            high_water_page_id: state.high_water_page_id,
            catalog: Arc::new(PageCatalog { pages }),
        });
        Arc::new(Self {
            current: RwLock::new(generation),
            metrics,
        })
    }

    fn pin(&self) -> GenerationPin {
        let generation = Arc::clone(&self.current.read().expect("generation lock poisoned"));
        let active = self
            .metrics
            .active_generation_pins
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.metrics.generation_pins.fetch_add(1, Ordering::Relaxed);
        let mut observed = self.metrics.max_concurrent_pins.load(Ordering::Relaxed);
        while active > observed {
            match self.metrics.max_concurrent_pins.compare_exchange_weak(
                observed,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
        GenerationPin {
            generation,
            metrics: Arc::clone(&self.metrics),
        }
    }

    fn prepare(
        &self,
        state: &BlinkState,
        superblock: &BlinkSuperblock,
        dirty: &BTreeSet<PageId>,
    ) -> Result<(Arc<PublishedGeneration>, PublicationPrepareTiming)> {
        let base = self.pin();
        let catalog_map_clone_started = Instant::now();
        let mut pages = base.generation.catalog.pages.clone();
        let catalog_map_clone_nanos = elapsed_nanos(catalog_map_clone_started);
        let catalog_state_scan_started = Instant::now();
        for (page_id, page) in &state.pages {
            if dirty.contains(page_id) || !pages.contains_key(page_id) {
                pages.insert(
                    *page_id,
                    Arc::new(PageCell {
                        version: PageVersion {
                            epoch: superblock.generation,
                            page: Arc::new(page.clone()),
                        },
                        metrics: Arc::clone(&self.metrics),
                    }),
                );
                self.metrics
                    .page_version_installs
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        let timing = PublicationPrepareTiming {
            catalog_map_clone_nanos,
            catalog_state_scan_nanos: elapsed_nanos(catalog_state_scan_started),
        };
        Ok((
            Arc::new(PublishedGeneration {
                epoch: superblock.generation,
                root_page_id: state.root_page_id,
                high_water_page_id: state.high_water_page_id,
                catalog: Arc::new(PageCatalog { pages }),
            }),
            timing,
        ))
    }

    fn prepare_delta<S: BlinkMutationState>(
        &self,
        state: &S,
        superblock: &BlinkSuperblock,
        dirty: &BTreeSet<PageId>,
    ) -> Result<(Arc<PublishedGeneration>, PublicationPrepareTiming)> {
        let base = self.pin();
        let catalog_map_clone_started = Instant::now();
        let mut pages = base.generation.catalog.pages.clone();
        let catalog_map_clone_nanos = elapsed_nanos(catalog_map_clone_started);
        let catalog_state_scan_started = Instant::now();
        for page_id in dirty {
            let page = state
                .page(*page_id)
                .ok_or_else(|| Error::invariant("dirty planned page is missing"))?;
            pages.insert(
                *page_id,
                Arc::new(PageCell {
                    version: PageVersion {
                        epoch: superblock.generation,
                        page: Arc::new(page.clone()),
                    },
                    metrics: Arc::clone(&self.metrics),
                }),
            );
            self.metrics
                .page_version_installs
                .fetch_add(1, Ordering::Relaxed);
        }
        let timing = PublicationPrepareTiming {
            catalog_map_clone_nanos,
            catalog_state_scan_nanos: elapsed_nanos(catalog_state_scan_started),
        };
        Ok((
            Arc::new(PublishedGeneration {
                epoch: superblock.generation,
                root_page_id: state.root_page_id(),
                high_water_page_id: state.high_water_page_id(),
                catalog: Arc::new(PageCatalog { pages }),
            }),
            timing,
        ))
    }

    fn publish(&self, generation: Arc<PublishedGeneration>) -> PublicationPublishTiming {
        let mut current = self.current.write().expect("generation lock poisoned");
        let swap_started = Instant::now();
        let retired_generation = std::mem::replace(&mut *current, generation);
        let swap_nanos = elapsed_nanos(swap_started);
        let retired_generation_drop_started = Instant::now();
        drop(retired_generation);
        let retired_generation_drop_nanos = elapsed_nanos(retired_generation_drop_started);
        drop(current);
        self.metrics
            .published_generations
            .fetch_add(1, Ordering::Relaxed);
        PublicationPublishTiming {
            swap_nanos,
            retired_generation_drop_nanos,
        }
    }

    fn can_reuse_pages(&self) -> bool {
        self.metrics.active_generation_pins.load(Ordering::Acquire) == 0
    }

    fn metrics(&self, retired_page_ids: u64, reusable_page_ids: u64) -> BlinkVersionedReadMetrics {
        self.metrics.snapshot(retired_page_ids, reusable_page_ids)
    }
}

#[derive(Debug)]
struct GenerationPin {
    generation: Arc<PublishedGeneration>,
    metrics: Arc<PublicationMetrics>,
}

impl Drop for GenerationPin {
    fn drop(&mut self) {
        self.metrics
            .active_generation_pins
            .fetch_sub(1, Ordering::Release);
    }
}

#[derive(Clone, Debug)]
pub struct BlinkReadHandle {
    publisher: Arc<GenerationPublisher>,
}

impl BlinkReadHandle {
    pub fn get(&self, key: &DocumentKey) -> Result<RevisionState> {
        let pin = self.publisher.pin();
        self.publisher
            .metrics
            .read_operations
            .fetch_add(1, Ordering::Relaxed);
        let mut corrections = 0;
        let result = read_state(&pin, key, &mut corrections);
        self.publisher
            .metrics
            .right_link_corrections
            .fetch_add(corrections, Ordering::Relaxed);
        result
    }

    pub fn query(
        &self,
        pk: &PrimaryKey,
        exclusive_after_sk: Option<&SortKey>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let pin = self.publisher.pin();
        self.publisher
            .metrics
            .read_operations
            .fetch_add(1, Ordering::Relaxed);
        let mut corrections = 0;
        let result = query_state(&pin, pk, exclusive_after_sk, limit, &mut corrections);
        self.publisher
            .metrics
            .right_link_corrections
            .fetch_add(corrections, Ordering::Relaxed);
        result
    }

    pub fn scan(&self, cursor: Option<&DocumentKey>, limit: usize) -> Result<Vec<Document>> {
        let pin = self.publisher.pin();
        self.publisher
            .metrics
            .read_operations
            .fetch_add(1, Ordering::Relaxed);
        let mut corrections = 0;
        let result = scan_state(&pin, cursor, limit, &mut corrections);
        self.publisher
            .metrics
            .right_link_corrections
            .fetch_add(corrections, Ordering::Relaxed);
        result
    }

    pub fn metrics(&self) -> BlinkVersionedReadMetrics {
        self.publisher.metrics(0, 0)
    }
}

#[derive(Clone, Debug)]
struct Candidate {
    state: BlinkState,
    dirty: BTreeSet<PageId>,
    result: TransactionResult,
    superblock: BlinkSuperblock,
    slot: SuperblockSlot,
    next_revision: Revision,
    next_lsn: Lsn,
    next_batch_id: u64,
}

struct ExecutedPlanTransaction {
    batch_id: u64,
    result: TransactionResult,
    dirty: BTreeSet<PageId>,
    images: Vec<WalPageImage>,
    superblock: BlinkSuperblock,
    slot: SuperblockSlot,
    next_revision: Revision,
    next_lsn: Lsn,
    next_batch_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SuperblockSlot {
    A,
    B,
}

/// A serial, durable B-link tree. It is intentionally not a drop-in alias for
/// `BTreeStore`: opening and decoding are format-specific and explicit.
pub struct BlinkStore<F: DurableFile, W: DurableFile = crate::btree::NoWal> {
    file: F,
    wal: Option<WalLog<W>>,
    state: BlinkState,
    publisher: Arc<GenerationPublisher>,
    current_superblock: BlinkSuperblock,
    active_slot: SuperblockSlot,
    next_revision: Revision,
    next_lsn: Lsn,
    next_batch_id: u64,
    config: DatabaseConfig,
    dirty_pages: BTreeMap<PageId, [u8; PAGE_SIZE]>,
    dirty_superblock: Option<[u8; PAGE_SIZE]>,
    storage_metrics: StorageMetrics,
    split_metrics: BlinkSplitMetrics,
    batch_metrics: BlinkBatchMetrics,
    planned_execution: bool,
    broken: Option<String>,
    fault_injector: Option<Box<dyn FaultInjector + Send>>,
}

impl<F: DurableFile, W: DurableFile> BlinkStore<F, W> {
    pub fn open_with_wal(file: F, wal_file: W, config: DatabaseConfig) -> Result<Self> {
        Self::open_internal(file, wal_file, config, None)
    }

    pub fn open_with_wal_and_fault_injector<I>(
        file: F,
        wal_file: W,
        config: DatabaseConfig,
        injector: I,
    ) -> Result<Self>
    where
        I: FaultInjector + Send + 'static,
    {
        Self::open_internal(file, wal_file, config, Some(Box::new(injector)))
    }

    pub fn open_path(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<BlinkStore<ProductionFile, ProductionFile>> {
        let path = path.as_ref();
        BlinkStore::<ProductionFile, ProductionFile>::open_with_wal(
            ProductionFile::open(path)?,
            ProductionFile::open(path.with_extension("wal"))?,
            config,
        )
    }

    fn open_internal(
        mut file: F,
        wal_file: W,
        config: DatabaseConfig,
        mut fault_injector: Option<Box<dyn FaultInjector + Send>>,
    ) -> Result<Self> {
        let identity = WalIdentity::new(
            config.database_uuid,
            config.tenant_id,
            config.shard_id,
            config.shard_epoch,
        );
        let checkpoint_hint = existing_checkpoint(&mut file, &identity, wal_file.len()?)?;
        let wal = WalLog::open_with_page_image_format_and_fault_injector_and_start_lsn(
            wal_file,
            identity,
            WalPageImageFormat::ExperimentalBlink,
            checkpoint_hint,
            fault_injector.as_deref_mut(),
        )?;
        if file.is_empty()? && wal.committed_batches().is_empty() {
            let mut store = Self::initialize(file, config)?;
            store.wal = Some(wal);
            store.next_lsn = store.wal.as_ref().unwrap().next_lsn();
            store.next_batch_id = store.wal.as_ref().unwrap().next_batch_id();
            store.fault_injector = fault_injector;
            return Ok(store);
        }
        recover_data_file(&mut file, wal.committed_batches(), checkpoint_hint)?;
        let (mut store, selected) = Self::load_file(file, config)?;
        let mut wal = wal;
        wal.resume_after(selected.checkpoint_lsn)?;
        store.wal = Some(wal);
        store.next_lsn = store.wal.as_ref().unwrap().next_lsn();
        store.next_batch_id = store.wal.as_ref().unwrap().next_batch_id();
        store.next_revision = Revision::new(
            store
                .check_invariants()?
                .max_revision
                .get()
                .max(store.next_lsn.get())
                .checked_add(1)
                .ok_or_else(|| Error::invariant("experimental revision exhausted"))?,
        );
        store.fault_injector = fault_injector;
        Ok(store)
    }

    fn initialize(mut file: F, config: DatabaseConfig) -> Result<Self> {
        let root = PageId::new(FIRST_DATA_PAGE);
        let state = BlinkState {
            pages: BTreeMap::from([(
                root,
                BlinkPage::Leaf {
                    lsn: Lsn::ZERO,
                    high_key: None,
                    right_sibling: None,
                    entries: Vec::new(),
                },
            )]),
            root_page_id: root,
            free_list_head: None,
            high_water_page_id: root,
            allow_page_reuse: true,
        };
        let sb = BlinkSuperblock::new(&config, root);
        let sb_bytes = encode_blink_superblock(&sb)?;
        let root_bytes = encode_blink_page(root, state.pages.get(&root).unwrap())?;
        file.set_len((FIRST_DATA_PAGE + 1) * PAGE_SIZE as u64)?;
        write_all_at(&mut file, 0, &sb_bytes)?;
        write_all_at(&mut file, PAGE_SIZE as u64, &sb_bytes)?;
        write_all_at(&mut file, root.get() * PAGE_SIZE as u64, &root_bytes)?;
        file.sync_all()?;
        let publisher = GenerationPublisher::new(&state, sb.generation);
        Ok(Self {
            file,
            wal: None,
            state,
            publisher,
            current_superblock: sb,
            active_slot: SuperblockSlot::B,
            next_revision: Revision::new(1),
            next_lsn: Lsn::new(1),
            next_batch_id: 1,
            config,
            dirty_pages: BTreeMap::new(),
            dirty_superblock: None,
            storage_metrics: StorageMetrics::default(),
            split_metrics: BlinkSplitMetrics::default(),
            batch_metrics: BlinkBatchMetrics::default(),
            planned_execution: false,
            broken: None,
            fault_injector: None,
        })
    }

    fn load_file(mut file: F, config: DatabaseConfig) -> Result<(Self, BlinkSuperblock)> {
        let length = file.len()?;
        if length < FIRST_DATA_PAGE * PAGE_SIZE as u64 || !length.is_multiple_of(PAGE_SIZE as u64) {
            return Err(Error::corruption(
                "experimental file length is not page aligned",
            ));
        }
        let a = read_exact_at(&mut file, 0, PAGE_SIZE)?;
        let b = read_exact_at(&mut file, PAGE_SIZE as u64, PAGE_SIZE)?;
        let sb_a = decode_blink_superblock_image(&a);
        let sb_b = decode_blink_superblock_image(&b);
        let (active_slot, sb) = match (sb_a, sb_b) {
            (Ok(a), Ok(b)) if a.generation >= b.generation => (SuperblockSlot::A, a),
            (Ok(_), Ok(b)) => (SuperblockSlot::B, b),
            (Ok(a), Err(_)) => (SuperblockSlot::A, a),
            (Err(_), Ok(b)) => (SuperblockSlot::B, b),
            (Err(a), Err(b)) => {
                return Err(Error::corruption(format!(
                    "no valid experimental superblock: {a}; {b}"
                )));
            }
        };
        if sb.database_uuid != config.database_uuid
            || sb.tenant_id != config.tenant_id
            || sb.shard_id != config.shard_id
            || sb.shard_epoch != config.shard_epoch
        {
            return Err(Error::corruption(
                "experimental superblock identity mismatch",
            ));
        }
        let mut pages = BTreeMap::new();
        for id in FIRST_DATA_PAGE..=sb.high_water_page_id.get() {
            let page_id = PageId::new(id);
            let bytes = read_exact_at(&mut file, id * PAGE_SIZE as u64, PAGE_SIZE)?;
            pages.insert(page_id, decode_blink_page(&bytes, page_id)?);
        }
        let state = BlinkState {
            pages,
            root_page_id: sb.root_page_id,
            free_list_head: sb.free_list_head,
            high_water_page_id: sb.high_water_page_id,
            allow_page_reuse: true,
        };
        let publisher = GenerationPublisher::new(&state, sb.generation);
        let store = Self {
            file,
            wal: None,
            state,
            publisher,
            current_superblock: sb.clone(),
            active_slot,
            next_revision: Revision::new(1),
            next_lsn: Lsn::new(1),
            next_batch_id: 1,
            config,
            dirty_pages: BTreeMap::new(),
            dirty_superblock: None,
            storage_metrics: StorageMetrics::default(),
            split_metrics: BlinkSplitMetrics::default(),
            batch_metrics: BlinkBatchMetrics::default(),
            planned_execution: false,
            broken: None,
            fault_injector: None,
        };
        store.check_invariants()?;
        Ok((store, sb))
    }

    pub fn storage_metrics(&self) -> StorageMetrics {
        self.storage_metrics.clone()
    }

    pub fn wal_metrics(&self) -> Result<Option<WalMetrics>> {
        self.wal.as_ref().map(WalLog::metrics).transpose()
    }

    pub fn split_metrics(&self) -> BlinkSplitMetrics {
        self.split_metrics.clone()
    }

    pub fn batch_metrics(&self) -> BlinkBatchMetrics {
        self.batch_metrics.clone()
    }

    pub fn enable_planned_execution(&mut self) {
        self.planned_execution = true;
    }

    pub fn current_superblock_generation(&self) -> u64 {
        self.current_superblock.generation
    }

    /// Returns a read-only handle whose operations pin one immutable committed
    /// generation for their full duration. The handle does not borrow or lock
    /// the serial writer store while traversing the tree.
    pub fn versioned_read_handle(&self) -> BlinkReadHandle {
        BlinkReadHandle {
            publisher: Arc::clone(&self.publisher),
        }
    }

    pub fn versioned_read_metrics(&self) -> BlinkVersionedReadMetrics {
        let retired_page_ids = count_free_pages(&self.state).unwrap_or(0);
        let reusable_page_ids = if self.publisher.can_reuse_pages() {
            retired_page_ids
        } else {
            0
        };
        self.publisher.metrics(retired_page_ids, reusable_page_ids)
    }

    pub fn set_fault_injector<I>(&mut self, injector: I)
    where
        I: FaultInjector + Send + 'static,
    {
        self.fault_injector = Some(Box::new(injector));
    }

    pub fn get(&mut self, key: &DocumentKey) -> Result<RevisionState> {
        match self
            .apply_batch(&[BatchRequest::Get { key: key.clone() }])?
            .remove(0)
        {
            BatchResponse::Get(value) => Ok(value),
            _ => Err(Error::invariant("Blink get response mismatch")),
        }
    }

    pub fn query(
        &mut self,
        pk: &PrimaryKey,
        exclusive_after_sk: Option<&SortKey>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        match self
            .apply_batch(&[BatchRequest::Query {
                pk: pk.clone(),
                exclusive_after_sk: exclusive_after_sk.cloned(),
                limit,
            }])?
            .remove(0)
        {
            BatchResponse::Query(rows) => Ok(rows),
            _ => Err(Error::invariant("Blink query response mismatch")),
        }
    }

    pub fn scan(&mut self, cursor: Option<&DocumentKey>, limit: usize) -> Result<Vec<Document>> {
        match self
            .apply_batch(&[BatchRequest::Scan {
                exclusive_after_key: cursor.cloned(),
                limit,
            }])?
            .remove(0)
        {
            BatchResponse::Scan(rows) => Ok(rows),
            _ => Err(Error::invariant("Blink scan response mismatch")),
        }
    }

    pub fn put(&mut self, key: DocumentKey, value: impl Into<Vec<u8>>) -> Result<Revision> {
        match self
            .apply_batch(&[BatchRequest::Put {
                key,
                value: value.into(),
            }])?
            .remove(0)
        {
            BatchResponse::Put(revision) => Ok(revision),
            _ => Err(Error::invariant("Blink put response mismatch")),
        }
    }

    pub fn delete(&mut self, key: DocumentKey) -> Result<Revision> {
        match self.apply_batch(&[BatchRequest::Delete { key }])?.remove(0) {
            BatchResponse::Delete(revision) => Ok(revision),
            _ => Err(Error::invariant("Blink delete response mismatch")),
        }
    }

    pub fn apply_batch(&mut self, requests: &[BatchRequest]) -> Result<Vec<BatchResponse>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut responses = Vec::with_capacity(requests.len());
        let mut right_link_corrections = 0;
        let started = Instant::now();
        for request in requests {
            match request {
                BatchRequest::Get { key } => responses.push(BatchResponse::Get(read_state(
                    &self.state,
                    key,
                    &mut right_link_corrections,
                )?)),
                BatchRequest::Query {
                    pk,
                    exclusive_after_sk,
                    limit,
                } => responses.push(BatchResponse::Query(query_state(
                    &self.state,
                    pk,
                    exclusive_after_sk.as_ref(),
                    *limit,
                    &mut right_link_corrections,
                )?)),
                BatchRequest::Scan {
                    exclusive_after_key,
                    limit,
                } => responses.push(BatchResponse::Scan(scan_state(
                    &self.state,
                    exclusive_after_key.as_ref(),
                    *limit,
                    &mut right_link_corrections,
                )?)),
                BatchRequest::Put { key, value } => {
                    let result = self.transact(TransactionRequest::new(
                        Vec::new(),
                        vec![TransactionMutation::Put {
                            key: key.clone(),
                            value: value.clone(),
                        }],
                    ))?;
                    responses.push(BatchResponse::Put(Revision::from(result.commit_lsn)));
                }
                BatchRequest::Delete { key } => {
                    let result = self.transact(TransactionRequest::new(
                        Vec::new(),
                        vec![TransactionMutation::Delete { key: key.clone() }],
                    ))?;
                    responses.push(BatchResponse::Delete(Revision::from(result.commit_lsn)));
                }
            }
        }
        self.split_metrics.right_link_corrections = self
            .split_metrics
            .right_link_corrections
            .saturating_add(right_link_corrections);
        self.storage_metrics.btree_preparation_nanos = self
            .storage_metrics
            .btree_preparation_nanos
            .saturating_add(elapsed_nanos(started));
        Ok(responses)
    }

    pub fn transact(&mut self, request: TransactionRequest) -> Result<TransactionResult> {
        self.apply_transaction_group(std::slice::from_ref(&request))?
            .into_iter()
            .next()
            .ok_or_else(|| Error::invariant("Blink transaction group returned no result"))?
    }

    /// Serializes the logical requests in order. Every accepted request gets
    /// its own commit LSN and commit marker, while the physical WAL sync is
    /// shared by the group.
    pub fn apply_transaction_group(
        &mut self,
        requests: &[TransactionRequest],
    ) -> Result<Vec<Result<TransactionResult>>> {
        if self.planned_execution {
            self.apply_planned_transaction_group(requests)
        } else {
            self.apply_serial_transaction_group(requests)
        }
    }

    fn apply_serial_transaction_group(
        &mut self,
        requests: &[TransactionRequest],
    ) -> Result<Vec<Result<TransactionResult>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if self.broken.is_some() {
            return Err(Error::durability(
                "experimental storage shard is degraded after an uncertain write",
            ));
        }
        let started = Instant::now();
        let mut working = self.state.clone();
        self.batch_metrics.full_state_clones =
            self.batch_metrics.full_state_clones.saturating_add(1);
        // A free page is reusable only when no reader can still reference the
        // generation that contains its previous contents. Otherwise the
        // allocator conservatively grows the file and leaves the ID retired.
        working.allow_page_reuse = self.publisher.can_reuse_pages();
        let mut working_sb = self.current_superblock.clone();
        let mut working_slot = self.active_slot;
        let mut next_lsn = self.wal.as_ref().map_or(self.next_lsn, WalLog::next_lsn);
        let mut next_batch = self
            .wal
            .as_ref()
            .map_or(self.next_batch_id, WalLog::next_batch_id);
        let mut next_revision = self.next_revision;
        let mut candidates = Vec::new();
        let mut results = Vec::with_capacity(requests.len());

        for request in requests {
            let validation_started = Instant::now();
            let candidate_state = working.clone();
            self.batch_metrics.full_state_clones =
                self.batch_metrics.full_state_clones.saturating_add(1);
            if let Err(error) = request.validate() {
                if matches!(error, Error::InvalidInput(_) | Error::InvalidRequest(_)) {
                    results.push(Err(error));
                    continue;
                }
                return Err(error);
            }
            if let Err(error) = validate_request_values(request, &self.config.limits) {
                results.push(Err(error));
                continue;
            }
            if let Err(error) = validate_conditions(&candidate_state, &request.conditions) {
                results.push(Err(error));
                continue;
            }
            self.storage_metrics.validation_nanos = self
                .storage_metrics
                .validation_nanos
                .saturating_add(elapsed_nanos(validation_started));

            let provisional = next_revision;
            next_revision = Revision::new(
                next_revision
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental revision exhausted"))?,
            );
            let mut dirty = BTreeSet::new();
            let mutated_keys = request
                .mutations
                .iter()
                .map(|mutation| mutation.key().encode())
                .collect::<BTreeSet<_>>();
            let mut candidate = candidate_state;
            for mutation in &request.mutations {
                apply_mutation(
                    &mut candidate,
                    &mut dirty,
                    &mut self.split_metrics,
                    mutation,
                    provisional,
                )?;
            }
            let commit_lsn = Lsn::new(
                next_lsn
                    .get()
                    .checked_add(
                        u64::try_from(dirty.len() + 1)
                            .map_err(|_| Error::invariant("Blink page count overflows LSN"))?,
                    )
                    .ok_or_else(|| Error::invariant("experimental LSN exhausted"))?,
            );
            for page_id in &dirty {
                candidate
                    .pages
                    .get_mut(page_id)
                    .ok_or_else(|| Error::invariant("dirty experimental page disappeared"))?
                    .restamp(provisional, commit_lsn, &mutated_keys);
            }
            working_sb = BlinkSuperblock {
                generation: working_sb
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental generation exhausted"))?,
                root_page_id: candidate.root_page_id,
                free_list_head: candidate.free_list_head,
                high_water_page_id: candidate.high_water_page_id,
                ..working_sb
            };
            working_slot = match working_slot {
                SuperblockSlot::A => SuperblockSlot::B,
                SuperblockSlot::B => SuperblockSlot::A,
            };
            let result = TransactionResult { commit_lsn };
            candidates.push(Candidate {
                state: {
                    self.batch_metrics.full_state_clones =
                        self.batch_metrics.full_state_clones.saturating_add(1);
                    candidate.clone()
                },
                dirty,
                result,
                superblock: working_sb.clone(),
                slot: working_slot,
                next_revision,
                next_lsn: Lsn::new(
                    commit_lsn
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| Error::invariant("experimental LSN exhausted"))?,
                ),
                next_batch_id: next_batch
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental batch id exhausted"))?,
            });
            results.push(Ok(result));
            working = candidate;
            next_lsn = Lsn::new(
                commit_lsn
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental LSN exhausted"))?,
            );
            next_batch = next_batch
                .checked_add(1)
                .ok_or_else(|| Error::invariant("experimental batch id exhausted"))?;
        }
        if candidates.is_empty() {
            return Ok(results);
        }

        let final_candidate = candidates.last().unwrap();
        // Build the immutable catalog before WAL append. It is not visible
        // until the durable append/sync below succeeds.
        let all_dirty = candidates
            .iter()
            .flat_map(|candidate| candidate.dirty.iter().copied())
            .collect::<BTreeSet<_>>();
        let (published_generation, _) = self.publisher.prepare(
            &final_candidate.state,
            &final_candidate.superblock,
            &all_dirty,
        )?;

        let mut wal_commits = Vec::with_capacity(candidates.len());
        let mut final_images = BTreeMap::new();
        for candidate in &candidates {
            let mut images = Vec::with_capacity(candidate.dirty.len() + 1);
            for page_id in &candidate.dirty {
                let page = candidate
                    .state
                    .pages
                    .get(page_id)
                    .ok_or_else(|| Error::invariant("candidate page is missing"))?;
                let image = encode_blink_page(*page_id, page)?;
                images.push(WalPageImage {
                    page_id: *page_id,
                    image,
                });
            }
            let sb_image = encode_blink_superblock(&candidate.superblock)?;
            images.push(WalPageImage {
                page_id: match candidate.slot {
                    SuperblockSlot::A => PageId::ZERO,
                    SuperblockSlot::B => PageId::new(1),
                },
                image: sb_image,
            });
            if self.wal.is_some() {
                wal_commits.push(WalCommit {
                    batch_id: next_batch - (candidates.len() as u64) + wal_commits.len() as u64,
                    commit_lsn: candidate.result.commit_lsn,
                    pages: images.clone(),
                });
            }
            for image in images {
                final_images.insert(image.page_id, image.image);
            }
        }
        if let Some(wal) = self.wal.as_mut() {
            if let Err(error) = wal.append_group(&wal_commits, self.fault_injector.as_deref_mut()) {
                self.broken = Some(error.to_string());
                return Err(error);
            }
        } else {
            for (page_id, image) in &final_images {
                write_all_at(&mut self.file, page_id.get() * PAGE_SIZE as u64, image)?;
            }
            self.file.sync_data()?;
        }
        let final_sb = encode_blink_superblock(&final_candidate.superblock)?;
        self.state = final_candidate.state.clone();
        self.current_superblock = final_candidate.superblock.clone();
        self.active_slot = final_candidate.slot;
        self.next_revision = final_candidate.next_revision;
        self.next_lsn = final_candidate.next_lsn;
        self.next_batch_id = final_candidate.next_batch_id;
        self.publisher.publish(published_generation);
        self.dirty_pages.extend(
            final_images
                .iter()
                .filter(|(id, _)| **id != PageId::ZERO && **id != PageId::new(1))
                .map(|(id, image)| (*id, *image)),
        );
        self.dirty_superblock = Some(final_sb);
        self.split_metrics.pages_touched = self
            .split_metrics
            .pages_touched
            .saturating_add(final_images.len() as u64);
        self.split_metrics.page_images = self
            .split_metrics
            .page_images
            .saturating_add(final_images.len() as u64);
        self.storage_metrics.publication_nanos = self
            .storage_metrics
            .publication_nanos
            .saturating_add(elapsed_nanos(started));
        Ok(results)
    }

    fn apply_planned_transaction_group(
        &mut self,
        requests: &[TransactionRequest],
    ) -> Result<Vec<Result<TransactionResult>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if self.broken.is_some() {
            return Err(Error::durability(
                "experimental storage shard is degraded after an uncertain write",
            ));
        }

        self.batch_metrics.logical_groups = self.batch_metrics.logical_groups.saturating_add(1);
        self.batch_metrics.logical_transactions = self
            .batch_metrics
            .logical_transactions
            .saturating_add(requests.len() as u64);
        let admission_started = Instant::now();
        let committed_state = &self.state;
        let mut overlay = LogicalOverlay::new(committed_state);
        let provisional_start = self.next_revision.get();
        let mut admitted = Vec::new();
        let mut results = Vec::with_capacity(requests.len());
        let mut accepted_count = 0u64;
        for (fifo_position, request) in requests.iter().enumerate() {
            if let Err(error) = request.validate() {
                if matches!(error, Error::InvalidInput(_) | Error::InvalidRequest(_)) {
                    self.batch_metrics.rejected_transactions =
                        self.batch_metrics.rejected_transactions.saturating_add(1);
                    results.push(Err(error));
                    continue;
                }
                return Err(error);
            }
            if let Err(error) = validate_request_values(request, &self.config.limits) {
                self.batch_metrics.rejected_transactions =
                    self.batch_metrics.rejected_transactions.saturating_add(1);
                results.push(Err(error));
                continue;
            }
            if let Err(error) = overlay.validate_conditions(&request.conditions) {
                if matches!(error, Error::Conflict(_)) {
                    self.batch_metrics.conflicted_transactions =
                        self.batch_metrics.conflicted_transactions.saturating_add(1);
                    results.push(Err(error));
                    continue;
                }
                return Err(error);
            }
            let ordinal = provisional_start
                .checked_add(accepted_count)
                .ok_or_else(|| Error::invariant("experimental revision exhausted"))?;
            let provisional_revision = ProvisionalRevisionToken {
                transaction_position: fifo_position,
                ordinal,
            };
            overlay.accept(request, provisional_revision);
            admitted.push(AdmittedTransaction {
                fifo_position,
                request: request.clone(),
                provisional_revision,
            });
            accepted_count = accepted_count.saturating_add(1);
            results.push(Ok(TransactionResult {
                commit_lsn: Lsn::ZERO,
            }));
            self.batch_metrics.admitted_transactions =
                self.batch_metrics.admitted_transactions.saturating_add(1);
        }
        let admission_nanos = elapsed_nanos(admission_started);
        self.batch_metrics.logical_admission_nanos = self
            .batch_metrics
            .logical_admission_nanos
            .saturating_add(admission_nanos);
        self.storage_metrics.validation_nanos = self
            .storage_metrics
            .validation_nanos
            .saturating_add(admission_nanos);
        drop(overlay);
        if admitted.is_empty() {
            return Ok(results);
        }

        let planning_started = Instant::now();
        let plan = plan_batch(&self.state, &admitted, &mut self.batch_metrics)?;
        self.batch_metrics.planning_nanos = self
            .batch_metrics
            .planning_nanos
            .saturating_add(elapsed_nanos(planning_started));

        let allow_page_reuse = self.publisher.can_reuse_pages();
        let mut working = WorkingBlinkState::new(&self.state, allow_page_reuse);
        let mut working_superblock = self.current_superblock.clone();
        let mut working_slot = self.active_slot;
        let mut next_lsn = self.wal.as_ref().map_or(self.next_lsn, WalLog::next_lsn);
        let mut next_batch_id = self
            .wal
            .as_ref()
            .map_or(self.next_batch_id, WalLog::next_batch_id);
        let mut execution_state = PhysicalExecutionState { cached_leaf: None };
        let physical_started = Instant::now();
        let mut executed = Vec::with_capacity(plan.transactions.len());
        for transaction_plan in &plan.transactions {
            let mut dirty = BTreeSet::new();
            for planned_mutation in &transaction_plan.mutations {
                apply_planned_mutation(
                    &mut working,
                    &mut dirty,
                    &mut self.split_metrics,
                    &mut self.batch_metrics,
                    &mut execution_state,
                    planned_mutation,
                    Revision::new(transaction_plan.provisional_revision.ordinal),
                )?;
            }
            let commit_lsn = Lsn::new(
                next_lsn
                    .get()
                    .checked_add(
                        u64::try_from(dirty.len() + 1)
                            .map_err(|_| Error::invariant("Blink page count overflows LSN"))?,
                    )
                    .ok_or_else(|| Error::invariant("experimental LSN exhausted"))?,
            );
            for page_id in &dirty {
                working
                    .overlay_page_mut(*page_id)
                    .ok_or_else(|| Error::invariant("dirty experimental page disappeared"))?
                    .restamp(
                        Revision::new(transaction_plan.provisional_revision.ordinal),
                        commit_lsn,
                        &transaction_plan.mutated_key_set,
                    );
            }
            if let Some(cached_leaf) = execution_state.cached_leaf.as_mut()
                && dirty.contains(&cached_leaf.leaf_id)
            {
                cached_leaf.page = working
                    .page(cached_leaf.leaf_id)
                    .cloned()
                    .ok_or_else(|| Error::invariant("cached planned leaf disappeared"))?;
            }
            working_superblock = BlinkSuperblock {
                generation: working_superblock
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental generation exhausted"))?,
                root_page_id: working.root_page_id(),
                free_list_head: working.free_list_head(),
                high_water_page_id: working.high_water_page_id(),
                ..working_superblock
            };
            working_slot = match working_slot {
                SuperblockSlot::A => SuperblockSlot::B,
                SuperblockSlot::B => SuperblockSlot::A,
            };
            let mut images = Vec::with_capacity(dirty.len() + 1);
            for page_id in &dirty {
                let page = working
                    .page(*page_id)
                    .ok_or_else(|| Error::invariant("planned page is missing"))?;
                if matches!(page, BlinkPage::Leaf { .. }) {
                    self.batch_metrics.leaf_encodes =
                        self.batch_metrics.leaf_encodes.saturating_add(1);
                }
                images.push(WalPageImage {
                    page_id: *page_id,
                    image: encode_blink_page(*page_id, page)?,
                });
            }
            images.push(WalPageImage {
                page_id: match working_slot {
                    SuperblockSlot::A => PageId::ZERO,
                    SuperblockSlot::B => PageId::new(1),
                },
                image: encode_blink_superblock(&working_superblock)?,
            });
            let next_lsn_after = Lsn::new(
                commit_lsn
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental LSN exhausted"))?,
            );
            let next_revision = Revision::from(next_lsn_after);
            executed.push(ExecutedPlanTransaction {
                batch_id: next_batch_id,
                result: TransactionResult { commit_lsn },
                dirty,
                images,
                superblock: working_superblock.clone(),
                slot: working_slot,
                next_revision,
                next_lsn: next_lsn_after,
                next_batch_id: next_batch_id
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("experimental batch id exhausted"))?,
            });
            next_lsn = next_lsn_after;
            next_batch_id = next_batch_id
                .checked_add(1)
                .ok_or_else(|| Error::invariant("experimental batch id exhausted"))?;
        }
        self.batch_metrics.physical_execution_nanos = self
            .batch_metrics
            .physical_execution_nanos
            .saturating_add(elapsed_nanos(physical_started));

        let final_execution = executed
            .last()
            .ok_or_else(|| Error::invariant("planned execution produced no transaction"))?;
        let dirty_union_started = Instant::now();
        let all_dirty = executed
            .iter()
            .flat_map(|transaction| transaction.dirty.iter().copied())
            .collect::<BTreeSet<_>>();
        self.batch_metrics.dirty_union_nanos = self
            .batch_metrics
            .dirty_union_nanos
            .saturating_add(elapsed_nanos(dirty_union_started));
        let catalog_started = Instant::now();
        if !working
            .pages
            .keys()
            .all(|page_id| all_dirty.contains(page_id))
        {
            return Err(Error::invariant(
                "working overlay contains a non-dirty page",
            ));
        }
        let (published_generation, prepare_timing) =
            self.publisher
                .prepare_delta(&working, &final_execution.superblock, &all_dirty)?;
        self.batch_metrics.catalog_construction_nanos = self
            .batch_metrics
            .catalog_construction_nanos
            .saturating_add(elapsed_nanos(catalog_started));
        self.batch_metrics.catalog_map_clone_nanos = self
            .batch_metrics
            .catalog_map_clone_nanos
            .saturating_add(prepare_timing.catalog_map_clone_nanos);
        self.batch_metrics.catalog_state_scan_nanos = self
            .batch_metrics
            .catalog_state_scan_nanos
            .saturating_add(prepare_timing.catalog_state_scan_nanos);

        let wal_assembly_started = Instant::now();
        let mut wal_commits = Vec::with_capacity(executed.len());
        let mut final_images = BTreeMap::new();
        let mut wal_bytes = 0u64;
        for transaction in &executed {
            if self.wal.is_some() {
                wal_commits.push(WalCommit {
                    batch_id: transaction.batch_id,
                    commit_lsn: transaction.result.commit_lsn,
                    pages: transaction.images.clone(),
                });
            }
            for image in &transaction.images {
                final_images.insert(image.page_id, image.image);
            }
        }
        self.batch_metrics.wal_assembly_nanos = self
            .batch_metrics
            .wal_assembly_nanos
            .saturating_add(elapsed_nanos(wal_assembly_started));
        let delta = working.into_delta();
        if let Some(wal) = self.wal.as_mut() {
            let reports = match wal.append_group(&wal_commits, self.fault_injector.as_deref_mut()) {
                Ok(reports) => reports,
                Err(error) => {
                    self.broken = Some(error.to_string());
                    return Err(error);
                }
            };
            wal_bytes = reports
                .iter()
                .map(|report| report.bytes_written as u64)
                .sum();
        } else {
            for (page_id, image) in &final_images {
                write_all_at(&mut self.file, page_id.get() * PAGE_SIZE as u64, image)?;
            }
            self.file.sync_data()?;
        }

        let state_install_started = Instant::now();
        for (page_id, page) in delta.pages {
            self.state.pages.insert(page_id, page);
        }
        self.state.root_page_id = delta.root_page_id;
        self.state.free_list_head = delta.free_list_head;
        self.state.high_water_page_id = delta.high_water_page_id;
        self.state.allow_page_reuse = delta.allow_page_reuse;
        self.current_superblock = final_execution.superblock.clone();
        self.active_slot = final_execution.slot;
        self.next_revision = final_execution.next_revision;
        self.next_lsn = final_execution.next_lsn;
        self.next_batch_id = final_execution.next_batch_id;
        self.batch_metrics.state_install_nanos = self
            .batch_metrics
            .state_install_nanos
            .saturating_add(elapsed_nanos(state_install_started));
        let publication_started = Instant::now();
        let publish_timing = self.publisher.publish(published_generation);
        self.batch_metrics.generation_publication_nanos = self
            .batch_metrics
            .generation_publication_nanos
            .saturating_add(elapsed_nanos(publication_started));
        self.batch_metrics.publication_swap_nanos = self
            .batch_metrics
            .publication_swap_nanos
            .saturating_add(publish_timing.swap_nanos);
        self.batch_metrics.retired_generation_drop_nanos = self
            .batch_metrics
            .retired_generation_drop_nanos
            .saturating_add(publish_timing.retired_generation_drop_nanos);
        let dirty_tracking_started = Instant::now();
        self.dirty_pages.extend(
            final_images
                .iter()
                .filter(|(page_id, _)| **page_id != PageId::ZERO && **page_id != PageId::new(1))
                .map(|(page_id, image)| (*page_id, *image)),
        );
        self.dirty_superblock = Some(
            final_images
                .get(&match final_execution.slot {
                    SuperblockSlot::A => PageId::ZERO,
                    SuperblockSlot::B => PageId::new(1),
                })
                .copied()
                .ok_or_else(|| Error::invariant("planned superblock image is missing"))?,
        );
        self.batch_metrics.dirty_tracking_nanos = self
            .batch_metrics
            .dirty_tracking_nanos
            .saturating_add(elapsed_nanos(dirty_tracking_started));
        self.split_metrics.pages_touched = self
            .split_metrics
            .pages_touched
            .saturating_add(final_images.len() as u64);
        self.split_metrics.page_images = self
            .split_metrics
            .page_images
            .saturating_add(final_images.len() as u64);
        self.batch_metrics.page_images = self.batch_metrics.page_images.saturating_add(
            executed
                .iter()
                .map(|transaction| transaction.images.len() as u64)
                .sum(),
        );
        self.batch_metrics.wal_bytes = self.batch_metrics.wal_bytes.saturating_add(wal_bytes);
        self.storage_metrics.btree_preparation_nanos = self
            .storage_metrics
            .btree_preparation_nanos
            .saturating_add(elapsed_nanos(admission_started));
        for (planned, transaction) in plan.transactions.iter().zip(&executed) {
            results[planned.fifo_position] = Ok(transaction.result);
        }
        self.storage_metrics.publication_nanos = self
            .storage_metrics
            .publication_nanos
            .saturating_add(elapsed_nanos(physical_started));
        Ok(results)
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.wal.is_none() {
            return self.file.sync_data();
        }
        let mut bytes = 0u64;
        for (page_id, image) in std::mem::take(&mut self.dirty_pages) {
            write_all_at(&mut self.file, page_id.get() * PAGE_SIZE as u64, &image)?;
            bytes = bytes.saturating_add(PAGE_SIZE as u64);
        }
        if let Some(image) = self.dirty_superblock.take() {
            let slot = match self.active_slot {
                SuperblockSlot::A => 0,
                SuperblockSlot::B => PAGE_SIZE as u64,
            };
            write_all_at(&mut self.file, slot, &image)?;
            bytes = bytes.saturating_add(PAGE_SIZE as u64);
        }
        if bytes > 0 {
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<BlinkCheckpointReport> {
        let started = Instant::now();
        let before = self.wal_metrics()?.map_or(0, |metrics| metrics.wal_bytes);
        let checkpoint_lsn = self
            .wal
            .as_ref()
            .and_then(|wal| wal.committed_batches().last().map(|batch| batch.commit_lsn))
            .unwrap_or(self.current_superblock.checkpoint_lsn);
        self.flush()?;
        if checkpoint_lsn > self.current_superblock.checkpoint_lsn {
            let sb = BlinkSuperblock {
                generation: self.current_superblock.generation + 1,
                checkpoint_lsn,
                ..self.current_superblock.clone()
            };
            let slot = match self.active_slot {
                SuperblockSlot::A => SuperblockSlot::B,
                SuperblockSlot::B => SuperblockSlot::A,
            };
            let image = encode_blink_superblock(&sb)?;
            write_all_at(
                &mut self.file,
                match slot {
                    SuperblockSlot::A => 0,
                    SuperblockSlot::B => PAGE_SIZE as u64,
                },
                &image,
            )?;
            self.file.sync_data()?;
            self.current_superblock = sb;
            self.active_slot = slot;
            if let Some(wal) = self.wal.as_mut() {
                wal.reset(checkpoint_lsn, self.fault_injector.as_deref_mut())?;
            }
        }
        let after = self.wal_metrics()?.map_or(0, |metrics| metrics.wal_bytes);
        self.check_invariants()?;
        Ok(BlinkCheckpointReport {
            checkpoint_lsn,
            pages_flushed: self.state.pages.len(),
            bytes_written: 0,
            wal_bytes_reclaimed: before.saturating_sub(after),
            duration_nanos: elapsed_nanos(started),
        })
    }

    pub fn check_invariants(&self) -> Result<InvariantReport> {
        check_state(&self.state)
    }

    pub fn into_files(self) -> (F, Option<W>) {
        (self.file, self.wal.map(WalLog::into_file))
    }
}

fn validate_request_values(request: &TransactionRequest, limits: &StorageLimits) -> Result<()> {
    for mutation in &request.mutations {
        let key = mutation.key().encode();
        validate_encoded_key(&key)?;
        if let TransactionMutation::Put { value, .. } = mutation
            && value.len() > limits.max_value_size
        {
            return Err(Error::invalid_input("value exceeds Blink maximum"));
        }
    }
    Ok(())
}

fn validate_encoded_key(key: &[u8]) -> Result<()> {
    if key.len() > crate::MAX_ENCODED_KEY_SIZE {
        return Err(Error::invalid_input("encoded document key is too large"));
    }
    DocumentKey::decode(key)
        .map(|_| ())
        .map_err(|error| Error::invalid_input(format!("document key is not canonical: {error}")))
}

trait ReadPageSource {
    fn root_page_id(&self) -> PageId;
    fn page(&self, page_id: PageId) -> Result<Arc<BlinkPage>>;
}

impl ReadPageSource for BlinkState {
    fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    fn page(&self, page_id: PageId) -> Result<Arc<BlinkPage>> {
        self.pages
            .get(&page_id)
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| Error::corruption("Blink page is missing"))
    }
}

impl ReadPageSource for GenerationPin {
    fn root_page_id(&self) -> PageId {
        self.generation.root_page_id
    }

    fn page(&self, page_id: PageId) -> Result<Arc<BlinkPage>> {
        if page_id > self.generation.high_water_page_id {
            return Err(Error::corruption(
                "published Blink page exceeds high-water mark",
            ));
        }
        self.generation
            .catalog
            .pages
            .get(&page_id)
            .ok_or_else(|| Error::corruption("published Blink page is missing"))?
            .page_at(self.generation.epoch)
    }
}

impl<'a> LogicalOverlay<'a> {
    fn new(committed: &'a BlinkState) -> Self {
        Self {
            committed,
            entries: BTreeMap::new(),
        }
    }

    fn observed_state(&self, key: &DocumentKey) -> Result<ObservedState> {
        let encoded = key.encode();
        validate_encoded_key(&encoded)?;
        if let Some(entry) = self.entries.get(&encoded) {
            let LogicalRevision::Provisional(token) = entry.revision;
            debug_assert_eq!(
                entry.originating_transaction_position,
                token.transaction_position
            );
            let _ = entry.value.as_ref();
            let revision = Revision::new(token.ordinal);
            return Ok(if entry.present {
                ObservedState::present(revision)
            } else {
                ObservedState::missing(revision)
            });
        }
        observed_state(self.committed, key)
    }

    fn validate_conditions(&self, conditions: &[TransactionCondition]) -> Result<()> {
        for condition in conditions {
            let actual = self.observed_state(condition.key())?;
            let matches = match condition {
                TransactionCondition::RevisionEquals {
                    expected_revision, ..
                } => actual.revision() == *expected_revision,
                TransactionCondition::Exists { .. } => !actual.is_missing(),
                TransactionCondition::NotExists { .. } => actual.is_missing(),
            };
            if !matches {
                return Err(Error::conflict(TransactionConflict {
                    key: condition.key().clone(),
                    expected: condition.expectation(),
                    actual,
                }));
            }
        }
        Ok(())
    }

    fn accept(&mut self, request: &TransactionRequest, token: ProvisionalRevisionToken) {
        for mutation in &request.mutations {
            let (present, value) = match mutation {
                TransactionMutation::Put { value, .. } => (true, Some(value.clone())),
                TransactionMutation::Delete { .. } => (false, None),
            };
            self.entries.insert(
                mutation.key().encode(),
                LogicalEntry {
                    present,
                    value,
                    revision: LogicalRevision::Provisional(token),
                    originating_transaction_position: token.transaction_position,
                },
            );
        }
    }
}

fn validate_conditions<S: ReadPageSource>(
    state: &S,
    conditions: &[TransactionCondition],
) -> Result<()> {
    for condition in conditions {
        let actual = observed_state(state, condition.key())?;
        let matches = match condition {
            TransactionCondition::RevisionEquals {
                expected_revision, ..
            } => actual.revision() == *expected_revision,
            TransactionCondition::Exists { .. } => !actual.is_missing(),
            TransactionCondition::NotExists { .. } => actual.is_missing(),
        };
        if !matches {
            return Err(Error::conflict(TransactionConflict {
                key: condition.key().clone(),
                expected: condition.expectation(),
                actual,
            }));
        }
    }
    Ok(())
}

fn plan_batch(
    state: &BlinkState,
    admitted: &[AdmittedTransaction],
    metrics: &mut BlinkBatchMetrics,
) -> Result<BatchPlan> {
    let mut plan = BatchPlan::default();
    let mut last_key_writer = BTreeMap::<Vec<u8>, usize>::new();
    let mut last_leaf_writer = BTreeMap::<PageId, usize>::new();
    let mut leaf_group_indices = BTreeMap::<PageId, usize>::new();
    let mut transaction_groups = BTreeMap::<usize, BTreeSet<usize>>::new();

    for transaction in admitted {
        let mut dependency_metadata = DependencyMetadata::default();
        let mut mutations = Vec::with_capacity(transaction.request.mutations.len());
        for condition in &transaction.request.conditions {
            let encoded = condition.key().encode();
            validate_encoded_key(&encoded)?;
            if let Some(predecessor) = last_key_writer.get(&encoded).copied() {
                dependency_metadata
                    .condition_key_predecessors
                    .push(predecessor);
                plan.dependencies.push(DependencyEdge {
                    predecessor,
                    successor: transaction.fifo_position,
                    kind: DependencyKind::ConditionKey,
                });
            }
        }
        for (mutation_index, mutation) in transaction.request.mutations.iter().enumerate() {
            let encoded_key = mutation.key().encode();
            validate_encoded_key(&encoded_key)?;
            let mut route_corrections = 0;
            let leaf_id = find_leaf_with_metrics(state, &encoded_key, &mut route_corrections)?;
            let route_hint = RouteHint {
                encoded_key: encoded_key.clone(),
                leaf_id,
            };
            mutations.push(PlannedMutation {
                mutation: mutation.clone(),
                encoded_key: encoded_key.clone(),
                route_hint,
            });
            metrics.routes_calculated = metrics.routes_calculated.saturating_add(1);
            if let Some(predecessor) = last_key_writer.get(&encoded_key).copied() {
                dependency_metadata.same_key_predecessors.push(predecessor);
                plan.dependencies.push(DependencyEdge {
                    predecessor,
                    successor: transaction.fifo_position,
                    kind: DependencyKind::SameKey,
                });
            }
            if let Some(predecessor) = last_leaf_writer.get(&leaf_id).copied() {
                dependency_metadata
                    .same_target_page_predecessors
                    .push(predecessor);
                dependency_metadata
                    .structural_route_predecessors
                    .push(predecessor);
                plan.dependencies.push(DependencyEdge {
                    predecessor,
                    successor: transaction.fifo_position,
                    kind: DependencyKind::SameTargetPage,
                });
                plan.dependencies.push(DependencyEdge {
                    predecessor,
                    successor: transaction.fifo_position,
                    kind: DependencyKind::StructuralRoute,
                });
            }
            last_key_writer.insert(encoded_key, transaction.fifo_position);
            last_leaf_writer.insert(leaf_id, transaction.fifo_position);
            let leaf_group_index = *leaf_group_indices.entry(leaf_id).or_insert_with(|| {
                let index = plan.leaf_groups.len();
                plan.leaf_groups.push(LeafGroupPlan {
                    leaf_hint: leaf_id,
                    mutations: Vec::new(),
                });
                index
            });
            plan.leaf_groups[leaf_group_index]
                .mutations
                .push((transaction.fifo_position, mutation_index));
            transaction_groups
                .entry(transaction.fifo_position)
                .or_default()
                .insert(leaf_group_index);
        }
        let same_transaction_positions = if mutations.len() > 1 {
            vec![transaction.fifo_position; mutations.len()]
        } else {
            Vec::new()
        };
        let encoded_keys = mutations
            .iter()
            .map(|mutation| mutation.encoded_key.clone())
            .collect::<Vec<_>>();
        let mutated_key_set = encoded_keys.iter().cloned().collect::<BTreeSet<_>>();
        plan.transactions.push(PhysicalTransactionPlan {
            fifo_position: transaction.fifo_position,
            provisional_revision: transaction.provisional_revision,
            mutations,
            encoded_keys,
            mutated_key_set,
            dependency_metadata: DependencyMetadata {
                same_transaction_positions,
                ..dependency_metadata
            },
        });
    }

    metrics.mutations_planned = metrics.mutations_planned.saturating_add(
        plan.transactions
            .iter()
            .map(|transaction| transaction.mutations.len() as u64)
            .sum(),
    );
    metrics.leaf_groups = metrics
        .leaf_groups
        .saturating_add(plan.leaf_groups.len() as u64);
    metrics.same_leaf_groups = metrics.same_leaf_groups.saturating_add(
        plan.leaf_groups
            .iter()
            .filter(|group| group.mutations.len() > 1)
            .count() as u64,
    );
    metrics.mutations_per_leaf_group = metrics.mutations_per_leaf_group.saturating_add(
        plan.leaf_groups
            .iter()
            .map(|group| group.mutations.len() as u64)
            .sum::<u64>(),
    );
    metrics.route_reuses = metrics.route_reuses.saturating_add(
        plan.leaf_groups
            .iter()
            .map(|group| group.mutations.len().saturating_sub(1) as u64)
            .sum::<u64>(),
    );
    metrics.dependency_edges = metrics
        .dependency_edges
        .saturating_add(plan.dependencies.len() as u64);
    let mut dependent_group_indices = BTreeSet::new();
    for groups in transaction_groups.values() {
        if groups.len() > 1 {
            dependent_group_indices.extend(groups.iter().copied());
        }
    }
    for dependency in &plan.dependencies {
        let Some(predecessor_groups) = transaction_groups.get(&dependency.predecessor) else {
            continue;
        };
        let Some(successor_groups) = transaction_groups.get(&dependency.successor) else {
            continue;
        };
        for predecessor_group in predecessor_groups {
            for successor_group in successor_groups {
                if predecessor_group != successor_group {
                    dependent_group_indices.insert(*predecessor_group);
                    dependent_group_indices.insert(*successor_group);
                }
            }
        }
    }
    let dependent_groups = dependent_group_indices.len() as u64;
    metrics.independent_leaf_groups = metrics
        .independent_leaf_groups
        .saturating_add((plan.leaf_groups.len() as u64).saturating_sub(dependent_groups));
    Ok(plan)
}

fn apply_planned_mutation<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    split_metrics: &mut BlinkSplitMetrics,
    batch_metrics: &mut BlinkBatchMetrics,
    execution_state: &mut PhysicalExecutionState,
    planned_mutation: &PlannedMutation,
    revision: Revision,
) -> Result<()> {
    let cached_matches = execution_state
        .cached_leaf
        .as_ref()
        .is_some_and(|cached| cached_leaf_contains(cached, &planned_mutation.encoded_key));
    if cached_matches {
        batch_metrics.coalesced_mutations = batch_metrics.coalesced_mutations.saturating_add(1);
        let mut cached = execution_state.cached_leaf.take().unwrap();
        let split = apply_cached_leaf_mutation(
            state,
            dirty,
            split_metrics,
            batch_metrics,
            &mut cached,
            planned_mutation,
            revision,
        )?;
        if split {
            batch_metrics.route_invalidations = batch_metrics.route_invalidations.saturating_add(1);
            batch_metrics.split_triggered_reroutes =
                batch_metrics.split_triggered_reroutes.saturating_add(1);
        } else {
            execution_state.cached_leaf = Some(cached);
        }
        return Ok(());
    }

    let mut route_corrections = 0;
    let leaf_id = find_leaf_from_hint(
        state,
        &planned_mutation.encoded_key,
        planned_mutation.route_hint.leaf_id,
        &mut route_corrections,
    )?;
    if leaf_id != planned_mutation.route_hint.leaf_id {
        batch_metrics.reroutes = batch_metrics.reroutes.saturating_add(1);
        batch_metrics.split_triggered_reroutes =
            batch_metrics.split_triggered_reroutes.saturating_add(1);
    }
    let page = state
        .page(leaf_id)
        .cloned()
        .ok_or_else(|| Error::corruption("planned Blink leaf is missing"))?;
    if !matches!(page, BlinkPage::Leaf { .. }) {
        return Err(Error::corruption("planned Blink route ended at non-leaf"));
    }
    batch_metrics.leaf_loads = batch_metrics.leaf_loads.saturating_add(1);
    let mut cached = CachedLeaf { leaf_id, page };
    let split = apply_cached_leaf_mutation(
        state,
        dirty,
        split_metrics,
        batch_metrics,
        &mut cached,
        planned_mutation,
        revision,
    )?;
    if split {
        batch_metrics.route_invalidations = batch_metrics.route_invalidations.saturating_add(1);
        batch_metrics.split_triggered_reroutes =
            batch_metrics.split_triggered_reroutes.saturating_add(1);
    } else {
        execution_state.cached_leaf = Some(cached);
    }
    Ok(())
}

fn cached_leaf_contains(cached: &CachedLeaf, encoded_key: &[u8]) -> bool {
    let BlinkPage::Leaf {
        high_key, entries, ..
    } = &cached.page
    else {
        return false;
    };
    if high_key
        .as_ref()
        .is_some_and(|high_key| encoded_key >= high_key.as_slice())
    {
        return false;
    }
    entries
        .first()
        .is_none_or(|entry| encoded_key >= entry.key.as_slice())
}

fn apply_cached_leaf_mutation<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    split_metrics: &mut BlinkSplitMetrics,
    batch_metrics: &mut BlinkBatchMetrics,
    cached: &mut CachedLeaf,
    planned_mutation: &PlannedMutation,
    revision: Revision,
) -> Result<bool> {
    let value = match &planned_mutation.mutation {
        TransactionMutation::Put { value, .. } => Some(value.as_slice()),
        TransactionMutation::Delete { .. } => None,
    };
    let encoded = &planned_mutation.encoded_key;
    let BlinkPage::Leaf {
        high_key,
        right_sibling,
        entries,
        ..
    } = &cached.page
    else {
        return Err(Error::corruption("cached Blink page is not a leaf"));
    };
    let mut next_entries = entries.clone();
    let value_ref = match value {
        Some(bytes) => Some(allocate_value(state, dirty, bytes)?),
        None => None,
    };
    match next_entries.binary_search_by(|entry| entry.key.cmp(encoded)) {
        Ok(entry_index) => {
            let old = next_entries[entry_index].value.clone();
            next_entries[entry_index] = LeafEntry {
                key: encoded.clone(),
                revision,
                value: value_ref,
            };
            if !leaf_fits(&next_entries, high_key.as_deref(), *right_sibling) {
                return Err(Error::invalid_input(
                    "document key and value cannot fit in a Blink leaf",
                ));
            }
            free_value(state, dirty, old)?;
        }
        Err(entry_index) => {
            next_entries.insert(
                entry_index,
                LeafEntry {
                    key: encoded.clone(),
                    revision,
                    value: value_ref,
                },
            );
            if !leaf_fits(&next_entries, high_key.as_deref(), *right_sibling) {
                let mut path = Vec::new();
                let routed_leaf = find_leaf_with_path(state, encoded, &mut path, split_metrics)?;
                if routed_leaf != cached.leaf_id {
                    batch_metrics.reroutes = batch_metrics.reroutes.saturating_add(1);
                    batch_metrics.split_triggered_reroutes =
                        batch_metrics.split_triggered_reroutes.saturating_add(1);
                    return Err(Error::invariant(
                        "planned leaf cache became stale before split",
                    ));
                }
                batch_metrics.structural_fallbacks =
                    batch_metrics.structural_fallbacks.saturating_add(1);
                batch_metrics.coalescing_interruptions =
                    batch_metrics.coalescing_interruptions.saturating_add(1);
                split_leaf(
                    state,
                    dirty,
                    split_metrics,
                    cached.leaf_id,
                    path,
                    high_key.clone(),
                    *right_sibling,
                    next_entries,
                    revision,
                )?;
                return Ok(true);
            }
        }
    }
    let next_page = BlinkPage::Leaf {
        lsn: Lsn::new(revision.get()),
        high_key: high_key.clone(),
        right_sibling: *right_sibling,
        entries: next_entries,
    };
    state.insert_page(cached.leaf_id, next_page.clone());
    cached.page = next_page;
    dirty.insert(cached.leaf_id);
    Ok(false)
}

fn find_leaf_from_hint<S: BlinkMutationState>(
    state: &S,
    key: &[u8],
    hint: PageId,
    right_link_corrections: &mut u64,
) -> Result<PageId> {
    let Some(BlinkPage::Leaf { .. }) = state.page(hint) else {
        return find_mutation_leaf_with_metrics(state, key, right_link_corrections);
    };
    let mut page_id = hint;
    let mut visited = HashSet::new();
    loop {
        if !visited.insert(page_id) {
            return Err(Error::corruption("planned Blink route contains a cycle"));
        }
        let page = state
            .page(page_id)
            .ok_or_else(|| Error::corruption("planned Blink route page is missing"))?;
        let BlinkPage::Leaf {
            high_key,
            right_sibling,
            entries,
            ..
        } = page
        else {
            return find_mutation_leaf_with_metrics(state, key, right_link_corrections);
        };
        if entries
            .first()
            .is_some_and(|entry| key < entry.key.as_slice())
        {
            return find_mutation_leaf_with_metrics(state, key, right_link_corrections);
        }
        if high_key
            .as_ref()
            .is_some_and(|high_key| key >= high_key.as_slice())
        {
            let Some(next) = *right_sibling else {
                return find_mutation_leaf_with_metrics(state, key, right_link_corrections);
            };
            *right_link_corrections = right_link_corrections.saturating_add(1);
            page_id = next;
            continue;
        }
        return Ok(page_id);
    }
}

fn find_mutation_leaf_with_metrics<S: BlinkMutationState>(
    state: &S,
    key: &[u8],
    right_link_corrections: &mut u64,
) -> Result<PageId> {
    let mut page_id = state.root_page_id();
    let mut visited = HashSet::new();
    loop {
        if !visited.insert(page_id) {
            return Err(Error::corruption("Blink tree route contains a cycle"));
        }
        let page = state
            .page(page_id)
            .ok_or_else(|| Error::corruption("Blink page is missing"))?;
        let (high_key, right_sibling) = match page {
            BlinkPage::Leaf {
                high_key,
                right_sibling,
                ..
            }
            | BlinkPage::Internal {
                high_key,
                right_sibling,
                ..
            } => (high_key, right_sibling),
            _ => return Err(Error::corruption("Blink route reached non-tree page")),
        };
        if high_key.as_ref().is_some_and(|high| key >= high.as_slice()) {
            let Some(next) = *right_sibling else {
                return Err(Error::corruption("Blink finite fence has no right sibling"));
            };
            *right_link_corrections = right_link_corrections.saturating_add(1);
            page_id = next;
            continue;
        }
        match page {
            BlinkPage::Leaf { .. } => return Ok(page_id),
            BlinkPage::Internal {
                leftmost_child,
                entries,
                ..
            } => {
                let index = entries.partition_point(|entry| key >= entry.key.as_slice());
                page_id = if index == 0 {
                    *leftmost_child
                } else {
                    entries[index - 1].right_child
                };
            }
            _ => unreachable!(),
        }
    }
}

fn observed_state<S: ReadPageSource>(state: &S, key: &DocumentKey) -> Result<ObservedState> {
    match find_entry(state, &key.encode())? {
        Some(entry) if entry.value.is_some() => Ok(ObservedState::present(entry.revision)),
        Some(entry) => Ok(ObservedState::missing(entry.revision)),
        None => Ok(ObservedState::missing(Revision::ZERO)),
    }
}

fn read_state<S: ReadPageSource>(
    state: &S,
    key: &DocumentKey,
    right_link_corrections: &mut u64,
) -> Result<RevisionState> {
    let encoded = key.encode();
    validate_encoded_key(&encoded)?;
    let Some(entry) = find_entry_with_metrics(state, &encoded, right_link_corrections)? else {
        return Ok(RevisionState::missing(Revision::ZERO));
    };
    match &entry.value {
        Some(value) => Ok(RevisionState::present(
            materialize_value(state, value)?,
            entry.revision,
        )),
        None => Ok(RevisionState::missing(entry.revision)),
    }
}

fn query_state<S: ReadPageSource>(
    state: &S,
    pk: &PrimaryKey,
    exclusive_after_sk: Option<&SortKey>,
    limit: usize,
    right_link_corrections: &mut u64,
) -> Result<Vec<Document>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let start = DocumentKey::new(pk.as_bytes().to_vec(), Vec::new());
    let cursor = exclusive_after_sk
        .map(|sk| DocumentKey::new(pk.as_bytes().to_vec(), sk.as_bytes().to_vec()));
    let mut leaf_id = find_leaf_with_metrics(state, &start.encode(), right_link_corrections)?;
    let mut first = true;
    let mut visited = HashSet::new();
    let mut output = Vec::new();
    while output.len() < limit {
        if !visited.insert(leaf_id) {
            return Err(Error::corruption("Blink leaf chain cycle during query"));
        }
        let page = state.page(leaf_id)?;
        let BlinkPage::Leaf {
            entries,
            right_sibling,
            ..
        } = page.as_ref()
        else {
            return Err(Error::corruption("Blink query reached non-leaf page"));
        };
        for entry in entries {
            let key = DocumentKey::decode(&entry.key)
                .map_err(|error| Error::corruption(format!("Blink key decode failed: {error}")))?;
            if first && cursor.as_ref().is_some_and(|cursor| key <= cursor.clone()) {
                continue;
            }
            first = false;
            if key.pk != *pk {
                if key.pk > *pk {
                    return Ok(output);
                }
                continue;
            }
            if cursor.as_ref().is_some_and(|cursor| key <= cursor.clone()) {
                continue;
            }
            if let Some(value) = &entry.value {
                output.push(Document {
                    key,
                    value: materialize_value(state, value)?,
                    revision: entry.revision,
                });
                if output.len() == limit {
                    return Ok(output);
                }
            }
        }
        let Some(next) = *right_sibling else { break };
        leaf_id = next;
        first = false;
    }
    Ok(output)
}

fn scan_state<S: ReadPageSource>(
    state: &S,
    cursor: Option<&DocumentKey>,
    limit: usize,
    right_link_corrections: &mut u64,
) -> Result<Vec<Document>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut leaf_id = match cursor {
        Some(key) => find_leaf_with_metrics(state, &key.encode(), right_link_corrections)?,
        None => leftmost_leaf(state)?,
    };
    let cursor = cursor.map(DocumentKey::encode);
    let mut visited = HashSet::new();
    let mut output = Vec::new();
    loop {
        if !visited.insert(leaf_id) {
            return Err(Error::corruption("Blink leaf chain cycle during scan"));
        }
        let page = state.page(leaf_id)?;
        let BlinkPage::Leaf {
            entries,
            right_sibling,
            ..
        } = page.as_ref()
        else {
            return Err(Error::corruption("Blink scan reached non-leaf page"));
        };
        for entry in entries {
            if cursor.as_ref().is_some_and(|cursor| entry.key <= *cursor) {
                continue;
            }
            let key = DocumentKey::decode(&entry.key)
                .map_err(|error| Error::corruption(format!("Blink key decode failed: {error}")))?;
            if let Some(value) = &entry.value {
                output.push(Document {
                    key,
                    value: materialize_value(state, value)?,
                    revision: entry.revision,
                });
                if output.len() == limit {
                    return Ok(output);
                }
            }
        }
        let Some(next) = *right_sibling else { break };
        leaf_id = next;
    }
    Ok(output)
}

fn find_entry<S: ReadPageSource>(state: &S, key: &[u8]) -> Result<Option<LeafEntry>> {
    let mut corrections = 0;
    find_entry_with_metrics(state, key, &mut corrections)
}

fn find_entry_with_metrics<S: ReadPageSource>(
    state: &S,
    key: &[u8],
    right_link_corrections: &mut u64,
) -> Result<Option<LeafEntry>> {
    let leaf_id = find_leaf_with_metrics(state, key, right_link_corrections)?;
    let page = state.page(leaf_id)?;
    let BlinkPage::Leaf { entries, .. } = page.as_ref() else {
        return Err(Error::corruption("Blink route ended at non-leaf"));
    };
    Ok(entries.iter().find(|entry| entry.key == key).cloned())
}

fn find_leaf_with_metrics<S: ReadPageSource>(
    state: &S,
    key: &[u8],
    right_link_corrections: &mut u64,
) -> Result<PageId> {
    let mut page_id = state.root_page_id();
    let mut guard = HashSet::new();
    loop {
        if !guard.insert(page_id) {
            return Err(Error::corruption("Blink tree route contains a cycle"));
        }
        let page = state.page(page_id)?;
        let (high_key, right_sibling) = match page.as_ref() {
            BlinkPage::Leaf {
                high_key,
                right_sibling,
                ..
            }
            | BlinkPage::Internal {
                high_key,
                right_sibling,
                ..
            } => (high_key, right_sibling),
            _ => return Err(Error::corruption("Blink route reached non-tree page")),
        };
        if high_key.as_ref().is_some_and(|high| key >= high.as_slice()) {
            let Some(next) = *right_sibling else {
                return Err(Error::corruption("Blink finite fence has no right sibling"));
            };
            *right_link_corrections = right_link_corrections.saturating_add(1);
            page_id = next;
            continue;
        }
        match page.as_ref() {
            BlinkPage::Leaf { .. } => return Ok(page_id),
            BlinkPage::Internal {
                leftmost_child,
                entries,
                ..
            } => {
                let index = entries.partition_point(|entry| key >= entry.key.as_slice());
                page_id = if index == 0 {
                    *leftmost_child
                } else {
                    entries[index - 1].right_child
                };
            }
            _ => unreachable!(),
        }
    }
}

fn leftmost_leaf<S: ReadPageSource>(state: &S) -> Result<PageId> {
    let mut page_id = state.root_page_id();
    loop {
        match state.page(page_id)?.as_ref() {
            BlinkPage::Leaf { .. } => return Ok(page_id),
            BlinkPage::Internal { leftmost_child, .. } => page_id = *leftmost_child,
            _ => return Err(Error::corruption("Blink leftmost path is invalid")),
        }
    }
}

fn materialize_value<S: ReadPageSource>(state: &S, value: &BlinkValueRef) -> Result<Vec<u8>> {
    match value {
        BlinkValueRef::Inline(value) => Ok(value.clone()),
        BlinkValueRef::Overflow { head, length } => {
            let mut output = Vec::with_capacity(*length as usize);
            let mut page_id = Some(*head);
            let mut visited = HashSet::new();
            while let Some(id) = page_id {
                if !visited.insert(id) {
                    return Err(Error::corruption("Blink overflow cycle"));
                }
                let page = state.page(id)?;
                let BlinkPage::Overflow { next, chunk, .. } = page.as_ref() else {
                    return Err(Error::corruption(
                        "Blink overflow reference has wrong page type",
                    ));
                };
                output.extend_from_slice(chunk);
                page_id = *next;
            }
            output.truncate(*length as usize);
            if output.len() != *length as usize {
                return Err(Error::corruption("Blink overflow length mismatch"));
            }
            Ok(output)
        }
    }
}

fn apply_mutation<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    metrics: &mut BlinkSplitMetrics,
    mutation: &TransactionMutation,
    revision: Revision,
) -> Result<()> {
    let (key, value) = match mutation {
        TransactionMutation::Put { key, value } => (key, Some(value.as_slice())),
        TransactionMutation::Delete { key } => (key, None),
    };
    let encoded = key.encode();
    validate_encoded_key(&encoded)?;
    let mut path = Vec::new();
    let leaf_id = find_leaf_with_path(state, &encoded, &mut path, metrics)?;
    let BlinkPage::Leaf {
        high_key,
        right_sibling,
        mut entries,
        ..
    } = state
        .page(leaf_id)
        .cloned()
        .ok_or_else(|| Error::corruption("Blink mutation leaf is missing"))?
    else {
        return Err(Error::corruption("Blink mutation route ended at non-leaf"));
    };
    let existing = entries.binary_search_by(|entry| entry.key.cmp(&encoded));
    let value_ref = match value {
        Some(bytes) => Some(allocate_value(state, dirty, bytes)?),
        None => None,
    };
    match existing {
        Ok(index) => {
            let old = entries[index].value.clone();
            entries[index] = LeafEntry {
                key: encoded,
                revision,
                value: value_ref,
            };
            if !leaf_fits(&entries, high_key.as_deref(), right_sibling) {
                return Err(Error::invalid_input(
                    "document key and value cannot fit in a Blink leaf",
                ));
            }
            free_value(state, dirty, old)?;
            state.insert_page(
                leaf_id,
                BlinkPage::Leaf {
                    lsn: Lsn::new(revision.get()),
                    high_key,
                    right_sibling,
                    entries,
                },
            );
            dirty.insert(leaf_id);
        }
        Err(index) => {
            entries.insert(
                index,
                LeafEntry {
                    key: encoded,
                    revision,
                    value: value_ref,
                },
            );
            if leaf_fits(&entries, high_key.as_deref(), right_sibling) {
                state.insert_page(
                    leaf_id,
                    BlinkPage::Leaf {
                        lsn: Lsn::new(revision.get()),
                        high_key,
                        right_sibling,
                        entries,
                    },
                );
                dirty.insert(leaf_id);
            } else {
                split_leaf(
                    state,
                    dirty,
                    metrics,
                    leaf_id,
                    path,
                    high_key,
                    right_sibling,
                    entries,
                    revision,
                )?;
            }
        }
    }
    Ok(())
}

fn find_leaf_with_path<S: BlinkMutationState>(
    state: &S,
    key: &[u8],
    path: &mut Vec<PageId>,
    metrics: &mut BlinkSplitMetrics,
) -> Result<PageId> {
    let mut page_id = state.root_page_id();
    let mut guard = HashSet::new();
    loop {
        if !guard.insert(page_id) {
            return Err(Error::corruption("Blink route contains a cycle"));
        }
        let page = state
            .page(page_id)
            .ok_or_else(|| Error::corruption("Blink route page is missing"))?;
        let (high_key, right_sibling) = match page {
            BlinkPage::Leaf {
                high_key,
                right_sibling,
                ..
            }
            | BlinkPage::Internal {
                high_key,
                right_sibling,
                ..
            } => (high_key, right_sibling),
            _ => return Err(Error::corruption("Blink route reached non-tree page")),
        };
        if high_key.as_ref().is_some_and(|high| key >= high.as_slice()) {
            let Some(next) = *right_sibling else {
                return Err(Error::corruption("Blink finite fence has no right sibling"));
            };
            metrics.right_link_corrections = metrics.right_link_corrections.saturating_add(1);
            page_id = next;
            continue;
        }
        match page {
            BlinkPage::Leaf { .. } => return Ok(page_id),
            BlinkPage::Internal {
                leftmost_child,
                entries,
                ..
            } => {
                path.push(page_id);
                let index = entries.partition_point(|entry| key >= entry.key.as_slice());
                page_id = if index == 0 {
                    *leftmost_child
                } else {
                    entries[index - 1].right_child
                };
            }
            _ => unreachable!(),
        }
    }
}

fn allocate_value<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    bytes: &[u8],
) -> Result<BlinkValueRef> {
    if bytes.len() <= INLINE_VALUE_LIMIT {
        return Ok(BlinkValueRef::Inline(bytes.to_vec()));
    }
    if bytes.len() > MAX_VALUE_SIZE {
        return Err(Error::invalid_input("value exceeds Blink maximum"));
    }
    let chunk_size = BODY_SIZE - OVERFLOW_HEADER_SIZE;
    let mut chunks = Vec::new();
    for chunk in bytes.chunks(chunk_size) {
        let id = allocate_page(state, dirty);
        chunks.push((id, chunk.to_vec()));
    }
    for (index, (id, chunk)) in chunks.iter().enumerate() {
        let next = chunks.get(index + 1).map(|(id, _)| *id);
        state.insert_page(
            *id,
            BlinkPage::Overflow {
                lsn: Lsn::ZERO,
                next,
                total_length: bytes.len() as u64,
                chunk: chunk.clone(),
            },
        );
        dirty.insert(*id);
    }
    Ok(BlinkValueRef::Overflow {
        head: chunks
            .first()
            .map(|(id, _)| *id)
            .ok_or_else(|| Error::invariant("overflow allocation produced no pages"))?,
        length: bytes.len() as u64,
    })
}

fn free_value<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    value: Option<BlinkValueRef>,
) -> Result<()> {
    let Some(BlinkValueRef::Overflow { head, .. }) = value else {
        return Ok(());
    };
    let mut page_id = Some(head);
    let mut visited = HashSet::new();
    while let Some(id) = page_id {
        if !visited.insert(id) {
            return Err(Error::corruption("Blink overflow cycle while freeing"));
        }
        let next = match state.page(id) {
            Some(BlinkPage::Overflow { next, .. }) => *next,
            Some(_) => return Err(Error::corruption("Blink overflow free has wrong page type")),
            None => return Err(Error::corruption("Blink overflow free page is missing")),
        };
        state.insert_page(
            id,
            BlinkPage::Free {
                lsn: Lsn::ZERO,
                next: state.free_list_head(),
            },
        );
        state.set_free_list_head(Some(id));
        dirty.insert(id);
        page_id = next;
    }
    Ok(())
}

fn allocate_page<S: BlinkMutationState>(state: &mut S, dirty: &mut BTreeSet<PageId>) -> PageId {
    if state.allow_page_reuse()
        && let Some(id) = state.free_list_head()
    {
        let next = match state.page(id) {
            Some(BlinkPage::Free { next, .. }) => *next,
            _ => None,
        };
        state.set_free_list_head(next);
        dirty.insert(id);
        return id;
    }
    let id = PageId::new(state.high_water_page_id().get() + 1);
    state.set_high_water_page_id(id);
    dirty.insert(id);
    id
}

fn split_leaf<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    metrics: &mut BlinkSplitMetrics,
    leaf_id: PageId,
    path: Vec<PageId>,
    old_high: Option<Vec<u8>>,
    old_right: Option<PageId>,
    entries: Vec<LeafEntry>,
    revision: Revision,
) -> Result<()> {
    let split = choose_leaf_split(&entries, old_high.as_deref(), old_right);
    let right_id = allocate_page(state, dirty);
    let separator = entries[split].key.clone();
    state.insert_page(
        leaf_id,
        BlinkPage::Leaf {
            lsn: Lsn::new(revision.get()),
            high_key: Some(separator.clone()),
            right_sibling: Some(right_id),
            entries: entries[..split].to_vec(),
        },
    );
    state.insert_page(
        right_id,
        BlinkPage::Leaf {
            lsn: Lsn::new(revision.get()),
            high_key: old_high,
            right_sibling: old_right,
            entries: entries[split..].to_vec(),
        },
    );
    dirty.insert(leaf_id);
    dirty.insert(right_id);
    metrics.leaf_splits = metrics.leaf_splits.saturating_add(1);
    install_separator(
        state, dirty, metrics, path, leaf_id, separator, right_id, revision,
    )
}

fn install_separator<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    metrics: &mut BlinkSplitMetrics,
    path: Vec<PageId>,
    left_child: PageId,
    separator: Vec<u8>,
    right_child: PageId,
    revision: Revision,
) -> Result<()> {
    let Some(parent_id) = path.last().copied() else {
        let old_root = state.root_page_id();
        let level = page_level(state, old_root)? + 1;
        let root = allocate_page(state, dirty);
        state.insert_page(
            root,
            BlinkPage::Internal {
                lsn: Lsn::new(revision.get()),
                level,
                high_key: None,
                right_sibling: None,
                leftmost_child: left_child,
                entries: vec![InternalEntry {
                    key: separator,
                    right_child,
                }],
            },
        );
        state.set_root_page_id(root);
        dirty.insert(root);
        metrics.root_splits = metrics.root_splits.saturating_add(1);
        return Ok(());
    };
    let BlinkPage::Internal {
        lsn: _,
        level,
        high_key,
        right_sibling,
        leftmost_child,
        mut entries,
    } = state
        .page(parent_id)
        .cloned()
        .ok_or_else(|| Error::corruption("Blink parent is missing"))?
    else {
        return Err(Error::corruption("Blink separator parent is not internal"));
    };
    let index = if leftmost_child == left_child {
        0
    } else {
        entries
            .iter()
            .position(|entry| entry.right_child == left_child)
            .map(|index| index + 1)
            .ok_or_else(|| Error::corruption("Blink parent does not contain split child"))?
    };
    entries.insert(
        index,
        InternalEntry {
            key: separator,
            right_child,
        },
    );
    if internal_fits(
        leftmost_child,
        &entries,
        high_key.as_deref(),
        right_sibling,
        level,
    ) {
        state.insert_page(
            parent_id,
            BlinkPage::Internal {
                lsn: Lsn::new(revision.get()),
                level,
                high_key,
                right_sibling,
                leftmost_child,
                entries,
            },
        );
        dirty.insert(parent_id);
        return Ok(());
    }
    let (promote, new_right) = split_internal(
        state,
        dirty,
        metrics,
        parent_id,
        level,
        high_key,
        right_sibling,
        leftmost_child,
        entries,
        revision,
    )?;
    let mut ancestor_path = path;
    ancestor_path.pop();
    install_separator(
        state,
        dirty,
        metrics,
        ancestor_path,
        parent_id,
        promote,
        new_right,
        revision,
    )
}

fn split_internal<S: BlinkMutationState>(
    state: &mut S,
    dirty: &mut BTreeSet<PageId>,
    metrics: &mut BlinkSplitMetrics,
    page_id: PageId,
    level: u16,
    old_high: Option<Vec<u8>>,
    old_right: Option<PageId>,
    leftmost_child: PageId,
    entries: Vec<InternalEntry>,
    revision: Revision,
) -> Result<(Vec<u8>, PageId)> {
    if entries.len() < 2 {
        return Err(Error::invalid_input("Blink internal entry cannot fit"));
    }
    let middle = entries.len() / 2;
    let promote = entries[middle].key.clone();
    let new_right = allocate_page(state, dirty);
    let right_leftmost = entries[middle].right_child;
    let left_entries = entries[..middle].to_vec();
    let right_entries = entries[middle + 1..].to_vec();
    if !internal_fits(
        leftmost_child,
        &left_entries,
        Some(&promote),
        Some(new_right),
        level,
    ) || !internal_fits(
        right_leftmost,
        &right_entries,
        old_high.as_deref(),
        old_right,
        level,
    ) {
        return Err(Error::invalid_input(
            "Blink internal split halves do not fit",
        ));
    }
    state.insert_page(
        page_id,
        BlinkPage::Internal {
            lsn: Lsn::new(revision.get()),
            level,
            high_key: Some(promote.clone()),
            right_sibling: Some(new_right),
            leftmost_child,
            entries: left_entries,
        },
    );
    state.insert_page(
        new_right,
        BlinkPage::Internal {
            lsn: Lsn::new(revision.get()),
            level,
            high_key: old_high,
            right_sibling: old_right,
            leftmost_child: right_leftmost,
            entries: right_entries,
        },
    );
    dirty.insert(page_id);
    dirty.insert(new_right);
    metrics.internal_splits = metrics.internal_splits.saturating_add(1);
    Ok((promote, new_right))
}

fn encode_blink_page(page_id: PageId, page: &BlinkPage) -> Result<[u8; PAGE_SIZE]> {
    let body = match page {
        BlinkPage::Leaf {
            high_key,
            right_sibling,
            entries,
            ..
        } => encode_leaf_body(high_key.as_deref(), *right_sibling, entries)?,
        BlinkPage::Internal {
            level,
            high_key,
            right_sibling,
            leftmost_child,
            entries,
            ..
        } => encode_internal_body(
            *level,
            high_key.as_deref(),
            *right_sibling,
            *leftmost_child,
            entries,
        )?,
        BlinkPage::Overflow {
            next,
            total_length,
            chunk,
            ..
        } => encode_overflow_body(*next, *total_length, chunk)?,
        BlinkPage::Free { next, .. } => encode_free_body(*next)?,
    };
    encode_page(
        PageHeader::new(page.page_type(), page_id, page.lsn()),
        &body,
    )
}

fn decode_blink_page(bytes: &[u8], expected_page_id: PageId) -> Result<BlinkPage> {
    let decoded = decode_page_at(bytes, Some(expected_page_id))?;
    if decoded.header.flags != 0 {
        return Err(Error::corruption("Blink page flags are non-zero"));
    }
    match decoded.header.page_type {
        PageType::Leaf => decode_leaf_body(decoded.header.page_lsn, &decoded.body),
        PageType::Internal => decode_internal_body(decoded.header.page_lsn, &decoded.body),
        PageType::Overflow => decode_overflow_body(decoded.header.page_lsn, &decoded.body),
        PageType::Free => decode_free_body(decoded.header.page_lsn, &decoded.body),
        PageType::Test => Err(Error::corruption("Blink test page is not a database page")),
    }
}

pub(crate) fn validate_blink_page_image(bytes: &[u8], expected_page_id: PageId) -> Result<()> {
    decode_blink_page(bytes, expected_page_id).map(|_| ())
}

fn encode_leaf_body(
    high_key: Option<&[u8]>,
    right_sibling: Option<PageId>,
    entries: &[LeafEntry],
) -> Result<Vec<u8>> {
    ensure_sorted_leaf(entries)?;
    let slot_end = LEAF_HEADER_SIZE
        .checked_add(
            entries
                .len()
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| Error::invalid_input("Blink leaf slot overflow"))?,
        )
        .ok_or_else(|| Error::invalid_input("Blink leaf slot overflow"))?;
    let high_len = high_key.map_or(0, <[u8]>::len);
    let records_start = slot_end
        .checked_add(high_len)
        .ok_or_else(|| Error::invalid_input("Blink leaf fence overflow"))?;
    if entries.len() > u16::MAX as usize || records_start > BODY_SIZE {
        return Err(Error::invalid_input("Blink leaf header exceeds page"));
    }
    let mut body = vec![0u8; BODY_SIZE];
    body[0..4].copy_from_slice(&LEAF_MAGIC);
    body[4..6].copy_from_slice(&BODY_VERSION.to_le_bytes());
    body[8..10].copy_from_slice(&(entries.len() as u16).to_le_bytes());
    body[10..12].copy_from_slice(&(slot_end as u16).to_le_bytes());
    body[12..14].copy_from_slice(&(records_start as u16).to_le_bytes());
    body[14..16]
        .copy_from_slice(&(if high_key.is_some() { slot_end } else { 0 } as u16).to_le_bytes());
    body[16..18].copy_from_slice(&(high_len as u16).to_le_bytes());
    body[20..28].copy_from_slice(&encode_page_id(right_sibling).to_le_bytes());
    if let Some(high_key) = high_key {
        validate_encoded_key(high_key)?;
        body[slot_end..records_start].copy_from_slice(high_key);
    }
    encode_leaf_records(&mut body, slot_end, records_start, entries)
}

fn encode_leaf_records(
    body: &mut [u8],
    slot_end: usize,
    records_start: usize,
    entries: &[LeafEntry],
) -> Result<Vec<u8>> {
    let mut upper = BODY_SIZE;
    let mut slots = Vec::with_capacity(entries.len());
    for entry in entries.iter().rev() {
        let record = encode_leaf_record(entry)?;
        upper = upper
            .checked_sub(record.len())
            .ok_or_else(|| Error::invalid_input("Blink leaf records exceed page"))?;
        if upper < records_start {
            return Err(Error::invalid_input("Blink leaf records exceed page"));
        }
        body[upper..upper + record.len()].copy_from_slice(&record);
        slots.push((upper, record.len()));
    }
    slots.reverse();
    body[18..20].copy_from_slice(&(upper as u16).to_le_bytes());
    for (index, (offset, length)) in slots.into_iter().enumerate() {
        let slot = slot_end - entries.len() * SLOT_SIZE + index * SLOT_SIZE;
        body[slot..slot + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        body[slot + 2..slot + 4].copy_from_slice(&(length as u16).to_le_bytes());
        body[slot + 4..slot + 6].copy_from_slice(&(entries[index].key.len() as u16).to_le_bytes());
    }
    Ok(body.to_vec())
}

fn encode_leaf_record(entry: &LeafEntry) -> Result<Vec<u8>> {
    validate_encoded_key(&entry.key)?;
    let (flags, value_length, aux, inline) = match &entry.value {
        None => (0u8, 0u64, NULL_PAGE_ID, &[][..]),
        Some(BlinkValueRef::Inline(value)) => (1u8, value.len() as u64, 0, value.as_slice()),
        Some(BlinkValueRef::Overflow { head, length }) => (2u8, *length, head.get(), &[][..]),
    };
    let length = LEAF_RECORD_HEADER_SIZE
        .checked_add(entry.key.len())
        .and_then(|length| length.checked_add(inline.len()))
        .ok_or_else(|| Error::invalid_input("Blink leaf record size overflow"))?;
    let mut record = vec![0u8; length];
    record[0..8].copy_from_slice(&entry.revision.get().to_le_bytes());
    record[8..16].copy_from_slice(&value_length.to_le_bytes());
    record[16..24].copy_from_slice(&aux.to_le_bytes());
    record[24] = flags;
    record[26..28].copy_from_slice(&(entry.key.len() as u16).to_le_bytes());
    record[LEAF_RECORD_HEADER_SIZE..LEAF_RECORD_HEADER_SIZE + entry.key.len()]
        .copy_from_slice(&entry.key);
    record[LEAF_RECORD_HEADER_SIZE + entry.key.len()..].copy_from_slice(inline);
    Ok(record)
}

fn encode_internal_body(
    level: u16,
    high_key: Option<&[u8]>,
    right_sibling: Option<PageId>,
    leftmost_child: PageId,
    entries: &[InternalEntry],
) -> Result<Vec<u8>> {
    if leftmost_child.get() < FIRST_DATA_PAGE {
        return Err(Error::invalid_input(
            "Blink internal leftmost child is invalid",
        ));
    }
    ensure_sorted_internal(entries)?;
    let slot_end = INTERNAL_HEADER_SIZE
        .checked_add(
            entries
                .len()
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| Error::invalid_input("Blink internal slot overflow"))?,
        )
        .ok_or_else(|| Error::invalid_input("Blink internal slot overflow"))?;
    let high_len = high_key.map_or(0, <[u8]>::len);
    let records_start = slot_end
        .checked_add(high_len)
        .ok_or_else(|| Error::invalid_input("Blink internal fence overflow"))?;
    if entries.len() > u16::MAX as usize || records_start > BODY_SIZE {
        return Err(Error::invalid_input("Blink internal header exceeds page"));
    }
    let mut body = vec![0u8; BODY_SIZE];
    body[0..4].copy_from_slice(&INTERNAL_MAGIC);
    body[4..6].copy_from_slice(&BODY_VERSION.to_le_bytes());
    body[8..10].copy_from_slice(&level.to_le_bytes());
    body[10..12].copy_from_slice(&(entries.len() as u16).to_le_bytes());
    body[12..14].copy_from_slice(&(slot_end as u16).to_le_bytes());
    body[14..16].copy_from_slice(&(records_start as u16).to_le_bytes());
    body[16..18]
        .copy_from_slice(&(if high_key.is_some() { slot_end } else { 0 } as u16).to_le_bytes());
    body[18..20].copy_from_slice(&(high_len as u16).to_le_bytes());
    body[20..28].copy_from_slice(&encode_page_id(right_sibling).to_le_bytes());
    body[28..36].copy_from_slice(&leftmost_child.get().to_le_bytes());
    if let Some(high_key) = high_key {
        validate_encoded_key(high_key)?;
        body[slot_end..records_start].copy_from_slice(high_key);
    }
    let mut upper = BODY_SIZE;
    let mut slots = Vec::with_capacity(entries.len());
    for entry in entries.iter().rev() {
        if entry.key.len() > u16::MAX as usize {
            return Err(Error::invalid_input("Blink separator is too large"));
        }
        let length = INTERNAL_RECORD_HEADER_SIZE + entry.key.len();
        upper = upper
            .checked_sub(length)
            .ok_or_else(|| Error::invalid_input("Blink internal records exceed page"))?;
        if upper < records_start {
            return Err(Error::invalid_input("Blink internal records exceed page"));
        }
        body[upper..upper + 8].copy_from_slice(&entry.right_child.get().to_le_bytes());
        body[upper + 12..upper + 14].copy_from_slice(&(entry.key.len() as u16).to_le_bytes());
        body[upper + INTERNAL_RECORD_HEADER_SIZE..upper + length].copy_from_slice(&entry.key);
        slots.push((upper, length));
    }
    slots.reverse();
    body[36..38].copy_from_slice(&(upper as u16).to_le_bytes());
    for (index, (offset, length)) in slots.into_iter().enumerate() {
        let slot = slot_end - entries.len() * SLOT_SIZE + index * SLOT_SIZE;
        body[slot..slot + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        body[slot + 2..slot + 4].copy_from_slice(&(length as u16).to_le_bytes());
        body[slot + 4..slot + 6].copy_from_slice(&(entries[index].key.len() as u16).to_le_bytes());
    }
    Ok(body)
}

fn encode_overflow_body(next: Option<PageId>, total_length: u64, chunk: &[u8]) -> Result<Vec<u8>> {
    if chunk.len() > BODY_SIZE - OVERFLOW_HEADER_SIZE {
        return Err(Error::invalid_input("Blink overflow chunk is too large"));
    }
    let mut body = vec![0u8; BODY_SIZE];
    body[0..4].copy_from_slice(&OVERFLOW_MAGIC);
    body[4..6].copy_from_slice(&BODY_VERSION.to_le_bytes());
    body[8..16].copy_from_slice(&encode_page_id(next).to_le_bytes());
    body[16..24].copy_from_slice(&total_length.to_le_bytes());
    body[24..28].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
    body[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);
    Ok(body)
}

fn encode_free_body(next: Option<PageId>) -> Result<Vec<u8>> {
    let mut body = vec![0u8; BODY_SIZE];
    body[0..4].copy_from_slice(&FREE_MAGIC);
    body[4..6].copy_from_slice(&BODY_VERSION.to_le_bytes());
    body[8..16].copy_from_slice(&encode_page_id(next).to_le_bytes());
    Ok(body)
}

fn decode_leaf_body(lsn: Lsn, body: &[u8]) -> Result<BlinkPage> {
    if body[0..4] != LEAF_MAGIC {
        return Err(Error::corruption("Blink leaf magic mismatch"));
    }
    check_body_version(body)?;
    if body[6..8].iter().any(|byte| *byte != 0) || body[20..48].iter().any(|byte| *byte != 0) {
        // bytes 20..28 contain the sibling, so only the rest of the header is reserved.
        if body[6..8].iter().any(|byte| *byte != 0) || body[28..48].iter().any(|byte| *byte != 0) {
            return Err(Error::corruption("Blink leaf reserved bytes are non-zero"));
        }
    }
    let count = u16::from_le_bytes(body[8..10].try_into().unwrap()) as usize;
    let slot_end = u16::from_le_bytes(body[10..12].try_into().unwrap()) as usize;
    let records_start = u16::from_le_bytes(body[12..14].try_into().unwrap()) as usize;
    let high_offset = u16::from_le_bytes(body[14..16].try_into().unwrap()) as usize;
    let high_len = u16::from_le_bytes(body[16..18].try_into().unwrap()) as usize;
    let records_end = u16::from_le_bytes(body[18..20].try_into().unwrap()) as usize;
    let right = decode_page_id(u64::from_le_bytes(body[20..28].try_into().unwrap()));
    validate_slot_header(
        count,
        slot_end,
        records_start,
        records_end,
        LEAF_HEADER_SIZE,
        body.len(),
    )?;
    let high_key = decode_high_key(body, high_offset, high_len, slot_end, records_start)?;
    let entries = decode_leaf_entries(body, count, slot_end, records_end)?;
    Ok(BlinkPage::Leaf {
        lsn,
        high_key,
        right_sibling: right,
        entries,
    })
}

fn decode_leaf_entries(
    body: &[u8],
    count: usize,
    slot_end: usize,
    records_end: usize,
) -> Result<Vec<LeafEntry>> {
    let mut ranges = Vec::with_capacity(count);
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let slot = LEAF_HEADER_SIZE + index * SLOT_SIZE;
        let offset = u16::from_le_bytes(body[slot..slot + 2].try_into().unwrap()) as usize;
        let length = u16::from_le_bytes(body[slot + 2..slot + 4].try_into().unwrap()) as usize;
        let key_length = u16::from_le_bytes(body[slot + 4..slot + 6].try_into().unwrap()) as usize;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::corruption("Blink leaf record overflow"))?;
        if offset < records_end || end > body.len() || length < LEAF_RECORD_HEADER_SIZE {
            return Err(Error::corruption("Blink leaf record range is invalid"));
        }
        if offset < slot_end || end > body.len() {
            return Err(Error::corruption("Blink leaf record overlaps header"));
        }
        ranges.push((offset, end));
        entries.push(decode_leaf_record(&body[offset..end], key_length)?);
    }
    ensure_non_overlapping(&mut ranges)?;
    ensure_sorted_leaf(&entries)?;
    Ok(entries)
}

fn decode_leaf_record(bytes: &[u8], slot_key_length: usize) -> Result<LeafEntry> {
    let revision = Revision::new(u64::from_le_bytes(bytes[0..8].try_into().unwrap()));
    let value_length = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let aux = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    if bytes[25] != 0 || bytes[28..32].iter().any(|byte| *byte != 0) {
        return Err(Error::corruption(
            "Blink leaf record reserved bytes are non-zero",
        ));
    }
    let flags = bytes[24];
    let key_length = u16::from_le_bytes(bytes[26..28].try_into().unwrap()) as usize;
    if key_length != slot_key_length {
        return Err(Error::corruption("Blink leaf slot key length mismatch"));
    }
    let key_end = LEAF_RECORD_HEADER_SIZE
        .checked_add(key_length)
        .ok_or_else(|| Error::corruption("Blink leaf key length overflow"))?;
    if key_end > bytes.len() {
        return Err(Error::corruption("Blink leaf key exceeds record"));
    }
    let key = bytes[LEAF_RECORD_HEADER_SIZE..key_end].to_vec();
    validate_encoded_key(&key).map_err(|_| Error::corruption("Blink leaf key is not canonical"))?;
    let value = match flags {
        0 if value_length == 0 && aux == NULL_PAGE_ID && bytes.len() == key_end => None,
        1 => {
            let end = key_end
                .checked_add(value_length as usize)
                .ok_or_else(|| Error::corruption("Blink inline value overflow"))?;
            if end != bytes.len() || aux != 0 {
                return Err(Error::corruption("Blink inline value record is invalid"));
            }
            Some(BlinkValueRef::Inline(bytes[key_end..end].to_vec()))
        }
        2 if bytes.len() == key_end && aux != NULL_PAGE_ID && value_length > 0 => {
            Some(BlinkValueRef::Overflow {
                head: PageId::new(aux),
                length: value_length,
            })
        }
        _ => return Err(Error::corruption("Blink leaf value record is invalid")),
    };
    Ok(LeafEntry {
        key,
        revision,
        value,
    })
}

fn decode_internal_body(lsn: Lsn, body: &[u8]) -> Result<BlinkPage> {
    if body[0..4] != INTERNAL_MAGIC {
        return Err(Error::corruption("Blink internal magic mismatch"));
    }
    check_body_version(body)?;
    if body[6..8].iter().any(|byte| *byte != 0) || body[40..56].iter().any(|byte| *byte != 0) {
        return Err(Error::corruption(
            "Blink internal reserved bytes are non-zero",
        ));
    }
    let level = u16::from_le_bytes(body[8..10].try_into().unwrap());
    if level == 0 {
        return Err(Error::corruption("Blink internal level is zero"));
    }
    let count = u16::from_le_bytes(body[10..12].try_into().unwrap()) as usize;
    let slot_end = u16::from_le_bytes(body[12..14].try_into().unwrap()) as usize;
    let records_start = u16::from_le_bytes(body[14..16].try_into().unwrap()) as usize;
    let high_offset = u16::from_le_bytes(body[16..18].try_into().unwrap()) as usize;
    let high_len = u16::from_le_bytes(body[18..20].try_into().unwrap()) as usize;
    let records_end = u16::from_le_bytes(body[36..38].try_into().unwrap()) as usize;
    let right = decode_page_id(u64::from_le_bytes(body[20..28].try_into().unwrap()));
    let leftmost = PageId::new(u64::from_le_bytes(body[28..36].try_into().unwrap()));
    validate_slot_header(
        count,
        slot_end,
        records_start,
        records_end,
        INTERNAL_HEADER_SIZE,
        body.len(),
    )?;
    let high_key = decode_high_key(body, high_offset, high_len, slot_end, records_start)?;
    let mut ranges = Vec::with_capacity(count);
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let slot = INTERNAL_HEADER_SIZE + index * SLOT_SIZE;
        let offset = u16::from_le_bytes(body[slot..slot + 2].try_into().unwrap()) as usize;
        let length = u16::from_le_bytes(body[slot + 2..slot + 4].try_into().unwrap()) as usize;
        let key_length = u16::from_le_bytes(body[slot + 4..slot + 6].try_into().unwrap()) as usize;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::corruption("Blink internal record overflow"))?;
        if offset < records_end
            || end > body.len()
            || length != INTERNAL_RECORD_HEADER_SIZE + key_length
        {
            return Err(Error::corruption("Blink internal record range is invalid"));
        }
        ranges.push((offset, end));
        let key_start = offset + INTERNAL_RECORD_HEADER_SIZE;
        let key = body[key_start..end].to_vec();
        validate_encoded_key(&key)
            .map_err(|_| Error::corruption("Blink separator is not canonical"))?;
        entries.push(InternalEntry {
            key,
            right_child: PageId::new(u64::from_le_bytes(
                body[offset..offset + 8].try_into().unwrap(),
            )),
        });
    }
    ensure_non_overlapping(&mut ranges)?;
    ensure_sorted_internal(&entries)?;
    Ok(BlinkPage::Internal {
        lsn,
        level,
        high_key,
        right_sibling: right,
        leftmost_child: leftmost,
        entries,
    })
}

fn decode_overflow_body(lsn: Lsn, body: &[u8]) -> Result<BlinkPage> {
    if body[0..4] != OVERFLOW_MAGIC {
        return Err(Error::corruption("Blink overflow magic mismatch"));
    }
    check_body_version(body)?;
    if body[6..8].iter().any(|byte| *byte != 0) || body[28..32].iter().any(|byte| *byte != 0) {
        return Err(Error::corruption(
            "Blink overflow reserved bytes are non-zero",
        ));
    }
    let next = decode_page_id(u64::from_le_bytes(body[8..16].try_into().unwrap()));
    let total_length = u64::from_le_bytes(body[16..24].try_into().unwrap());
    let chunk_length = u32::from_le_bytes(body[24..28].try_into().unwrap()) as usize;
    if chunk_length > BODY_SIZE - OVERFLOW_HEADER_SIZE {
        return Err(Error::corruption("Blink overflow chunk length is invalid"));
    }
    if body[OVERFLOW_HEADER_SIZE + chunk_length..]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(Error::corruption(
            "Blink overflow trailing bytes are non-zero",
        ));
    }
    Ok(BlinkPage::Overflow {
        lsn,
        next,
        total_length,
        chunk: body[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_length].to_vec(),
    })
}

fn decode_free_body(lsn: Lsn, body: &[u8]) -> Result<BlinkPage> {
    if body[0..4] != FREE_MAGIC {
        return Err(Error::corruption("Blink free magic mismatch"));
    }
    check_body_version(body)?;
    if body[6..8].iter().any(|byte| *byte != 0) || body[16..].iter().any(|byte| *byte != 0) {
        return Err(Error::corruption("Blink free reserved bytes are non-zero"));
    }
    Ok(BlinkPage::Free {
        lsn,
        next: decode_page_id(u64::from_le_bytes(body[8..16].try_into().unwrap())),
    })
}

fn check_body_version(body: &[u8]) -> Result<()> {
    let version = u16::from_le_bytes(body[4..6].try_into().unwrap());
    if version != BODY_VERSION {
        return Err(Error::unsupported_format(format!(
            "Blink body version {version}, supported {BODY_VERSION}"
        )));
    }
    Ok(())
}

fn validate_slot_header(
    count: usize,
    slot_end: usize,
    records_start: usize,
    records_end: usize,
    header_size: usize,
    body_size: usize,
) -> Result<()> {
    if slot_end != header_size + count * SLOT_SIZE
        || slot_end > records_start
        || records_start > records_end
        || records_end > body_size
    {
        return Err(Error::corruption(
            "Blink slotted page boundaries are invalid",
        ));
    }
    Ok(())
}

fn decode_high_key(
    body: &[u8],
    offset: usize,
    length: usize,
    slot_end: usize,
    records_start: usize,
) -> Result<Option<Vec<u8>>> {
    if length == 0 {
        if offset != 0 {
            return Err(Error::corruption("Blink +infinity fence has an offset"));
        }
        return Ok(None);
    }
    if offset != slot_end || offset + length != records_start {
        return Err(Error::corruption("Blink high-key range is invalid"));
    }
    let key = body[offset..records_start].to_vec();
    validate_encoded_key(&key).map_err(|_| Error::corruption("Blink high key is not canonical"))?;
    Ok(Some(key))
}

fn ensure_sorted_leaf(entries: &[LeafEntry]) -> Result<()> {
    for pair in entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(Error::corruption(
                "Blink leaf entries are not strictly ordered",
            ));
        }
    }
    for entry in entries {
        validate_encoded_key(&entry.key)?;
    }
    Ok(())
}

fn ensure_sorted_internal(entries: &[InternalEntry]) -> Result<()> {
    for pair in entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(Error::corruption(
                "Blink separators are not strictly ordered",
            ));
        }
    }
    for entry in entries {
        validate_encoded_key(&entry.key)?;
        if entry.right_child.get() < FIRST_DATA_PAGE {
            return Err(Error::corruption(
                "Blink separator child page id is invalid",
            ));
        }
    }
    Ok(())
}

fn ensure_non_overlapping(ranges: &mut [(usize, usize)]) -> Result<()> {
    ranges.sort_unstable();
    for pair in ranges.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(Error::corruption("Blink records overlap"));
        }
    }
    Ok(())
}

fn leaf_fits(entries: &[LeafEntry], high_key: Option<&[u8]>, right: Option<PageId>) -> bool {
    encode_leaf_body(high_key, right, entries).is_ok()
}

fn internal_fits(
    leftmost: PageId,
    entries: &[InternalEntry],
    high_key: Option<&[u8]>,
    right: Option<PageId>,
    level: u16,
) -> bool {
    encode_internal_body(level, high_key, right, leftmost, entries).is_ok()
}

fn choose_leaf_split(
    entries: &[LeafEntry],
    high_key: Option<&[u8]>,
    right: Option<PageId>,
) -> usize {
    let middle = entries.len() / 2;
    (1..entries.len())
        .min_by_key(|index| {
            let left = leaf_fits(&entries[..*index], Some(&entries[*index].key), right);
            let right_fits = leaf_fits(&entries[*index..], high_key, right);
            if left && right_fits {
                (*index as isize - middle as isize).unsigned_abs()
            } else {
                usize::MAX
            }
        })
        .unwrap_or(middle)
}

fn page_level<S: BlinkMutationState>(state: &S, page_id: PageId) -> Result<u16> {
    match state.page(page_id) {
        Some(BlinkPage::Leaf { .. }) => Ok(0),
        Some(BlinkPage::Internal { level, .. }) => Ok(*level),
        _ => Err(Error::corruption("Blink page is not a tree page")),
    }
}

fn count_free_pages(state: &BlinkState) -> Result<u64> {
    let mut count = 0;
    let mut current = state.free_list_head;
    let mut visited = HashSet::new();
    while let Some(page_id) = current {
        if !visited.insert(page_id) {
            return Err(Error::corruption("Blink free list cycle"));
        }
        let Some(BlinkPage::Free { next, .. }) = state.pages.get(&page_id) else {
            return Err(Error::corruption(
                "Blink free list points to a non-free page",
            ));
        };
        count += 1;
        current = *next;
    }
    Ok(count)
}

fn check_state(state: &BlinkState) -> Result<InvariantReport> {
    if state.root_page_id.get() < FIRST_DATA_PAGE
        || state.high_water_page_id.get() < state.root_page_id.get()
    {
        return Err(Error::corruption("Blink root/high-water invariant failed"));
    }
    let mut reachable = BTreeSet::new();
    let mut leaves = Vec::new();
    let mut max_revision = Revision::ZERO;
    walk_tree(
        state,
        state.root_page_id,
        None,
        None,
        &mut reachable,
        &mut leaves,
        &mut max_revision,
    )?;
    let mut overflow_owned = BTreeSet::new();
    for leaf_id in &leaves {
        let BlinkPage::Leaf { entries, .. } = state.pages.get(leaf_id).unwrap() else {
            unreachable!()
        };
        for entry in entries {
            max_revision = max_revision.max(entry.revision);
            if let Some(BlinkValueRef::Overflow { head, length }) = &entry.value {
                let mut current = Some(*head);
                let mut total = 0u64;
                let mut local = HashSet::new();
                while let Some(id) = current {
                    if !local.insert(id) || !overflow_owned.insert(id) {
                        return Err(Error::corruption(
                            "Blink overflow chain is cyclic or multiply owned",
                        ));
                    }
                    let BlinkPage::Overflow {
                        next,
                        total_length,
                        chunk,
                        ..
                    } = state
                        .pages
                        .get(&id)
                        .ok_or_else(|| Error::corruption("Blink overflow page is missing"))?
                    else {
                        return Err(Error::corruption(
                            "Blink overflow owner points to wrong page",
                        ));
                    };
                    if *total_length != *length {
                        return Err(Error::corruption("Blink overflow total length mismatch"));
                    }
                    total = total.saturating_add(chunk.len() as u64);
                    current = *next;
                }
                if total < *length {
                    return Err(Error::corruption(
                        "Blink overflow chain is shorter than value",
                    ));
                }
            }
        }
    }
    reachable.extend(overflow_owned);
    let mut free = BTreeSet::new();
    let mut current = state.free_list_head;
    while let Some(id) = current {
        if !free.insert(id) || reachable.contains(&id) {
            return Err(Error::corruption(
                "Blink free list cycles or overlaps reachable pages",
            ));
        }
        let Some(BlinkPage::Free { next, .. }) = state.pages.get(&id) else {
            return Err(Error::corruption(
                "Blink free list points to a non-free page",
            ));
        };
        current = *next;
    }
    let expected_ids = (FIRST_DATA_PAGE..=state.high_water_page_id.get())
        .map(PageId::new)
        .collect::<BTreeSet<_>>();
    let known = reachable.union(&free).copied().collect::<BTreeSet<_>>();
    let leaked = expected_ids.difference(&known).copied().collect::<Vec<_>>();
    if !leaked.is_empty() || state.pages.keys().any(|id| !expected_ids.contains(id)) {
        return Err(Error::corruption(format!(
            "Blink leaked or out-of-range pages: {leaked:?}"
        )));
    }
    check_sibling_links(state, &leaves)?;
    Ok(InvariantReport {
        reachable_pages: reachable.len(),
        free_pages: free.len(),
        leaked_pages: leaked,
        max_revision,
    })
}

fn walk_tree(
    state: &BlinkState,
    page_id: PageId,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    reachable: &mut BTreeSet<PageId>,
    leaves: &mut Vec<PageId>,
    max_revision: &mut Revision,
) -> Result<()> {
    if !reachable.insert(page_id) {
        return Err(Error::corruption(
            "Blink tree contains a multiply reachable page or cycle",
        ));
    }
    let page = state
        .pages
        .get(&page_id)
        .ok_or_else(|| Error::corruption("Blink tree points outside the file"))?;
    match page {
        BlinkPage::Leaf {
            high_key, entries, ..
        } => {
            ensure_sorted_leaf(entries)?;
            if let Some(high) = high_key.as_deref()
                && upper.is_some_and(|upper| high > upper)
            {
                return Err(Error::corruption(
                    "Blink leaf fence exceeds parent boundary",
                ));
            }
            for entry in entries {
                if lower.is_some_and(|lower| entry.key.as_slice() < lower)
                    || upper.is_some_and(|upper| entry.key.as_slice() >= upper)
                    || high_key
                        .as_ref()
                        .is_some_and(|high| entry.key.as_slice() >= high.as_slice())
                {
                    return Err(Error::corruption(format!(
                        "Blink leaf key violates fence or parent range: key={:?} lower={:?} upper={:?} high={:?}",
                        entry.key, lower, upper, high_key
                    )));
                }
                *max_revision = (*max_revision).max(entry.revision);
            }
            leaves.push(page_id);
        }
        BlinkPage::Internal {
            level,
            high_key,
            leftmost_child,
            entries,
            ..
        } => {
            ensure_sorted_internal(entries)?;
            if *level == 0
                || high_key
                    .as_ref()
                    .is_some_and(|high| upper.is_some_and(|upper| high.as_slice() > upper))
            {
                return Err(Error::corruption(
                    "Blink internal fence or level is invalid",
                ));
            }
            let mut child_lower = lower.map(ToOwned::to_owned);
            walk_tree(
                state,
                *leftmost_child,
                child_lower.as_deref(),
                entries.first().map(|entry| entry.key.as_slice()).or(upper),
                reachable,
                leaves,
                max_revision,
            )?;
            for (index, entry) in entries.iter().enumerate() {
                child_lower = Some(entry.key.clone());
                let child_upper = entries
                    .get(index + 1)
                    .map(|next| next.key.as_slice())
                    .or(upper);
                walk_tree(
                    state,
                    entry.right_child,
                    child_lower.as_deref(),
                    child_upper,
                    reachable,
                    leaves,
                    max_revision,
                )?;
            }
        }
        BlinkPage::Overflow { .. } | BlinkPage::Free { .. } => {
            return Err(Error::corruption(
                "Blink tree route reaches data-management page",
            ));
        }
    }
    Ok(())
}

fn check_sibling_links(state: &BlinkState, leaves: &[PageId]) -> Result<()> {
    let leaf_set = leaves.iter().copied().collect::<BTreeSet<_>>();
    let first = *leaves
        .first()
        .ok_or_else(|| Error::corruption("Blink tree has no leaf"))?;
    let mut chain = Vec::new();
    let mut current = Some(first);
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return Err(Error::corruption("Blink leaf sibling chain cycles"));
        }
        if !leaf_set.contains(&id) {
            return Err(Error::corruption(
                "Blink leaf sibling points outside leaf set",
            ));
        }
        chain.push(id);
        current = match state.pages.get(&id) {
            Some(BlinkPage::Leaf { right_sibling, .. }) => *right_sibling,
            _ => return Err(Error::corruption("Blink leaf chain points to non-leaf")),
        };
    }
    if chain != leaves {
        return Err(Error::corruption(
            "Blink leaf chain order differs from tree order",
        ));
    }
    for (index, id) in leaves.iter().enumerate() {
        let Some(BlinkPage::Leaf {
            right_sibling,
            high_key,
            entries,
            ..
        }) = state.pages.get(id)
        else {
            unreachable!()
        };
        if let Some(next) = right_sibling {
            if *next == *id || page_level(state, *next)? != 0 {
                return Err(Error::corruption(
                    "Blink leaf right link has invalid target",
                ));
            }
            let Some(BlinkPage::Leaf {
                entries: next_entries,
                ..
            }) = state.pages.get(next)
            else {
                unreachable!()
            };
            if next_entries
                .first()
                .is_some_and(|next_key| entries.last().is_some_and(|last| next_key.key <= last.key))
            {
                return Err(Error::corruption(
                    "Blink sibling key ranges are not increasing",
                ));
            }
            if index + 1 == leaves.len() {
                return Err(Error::corruption("Blink rightmost leaf has a sibling"));
            }
        } else if index + 1 != leaves.len() || high_key.is_some() {
            return Err(Error::corruption(
                "Blink finite/rightmost leaf fence is invalid",
            ));
        }
    }
    for (id, page) in &state.pages {
        let BlinkPage::Internal {
            level,
            right_sibling,
            high_key,
            ..
        } = page
        else {
            continue;
        };
        match right_sibling {
            Some(next) => {
                if *next == *id {
                    return Err(Error::corruption("Blink internal right link self-links"));
                }
                let Some(BlinkPage::Internal {
                    level: next_level, ..
                }) = state.pages.get(next)
                else {
                    return Err(Error::corruption(
                        "Blink internal right link targets non-internal",
                    ));
                };
                if next_level != level {
                    return Err(Error::corruption(
                        "Blink internal siblings have different levels",
                    ));
                }
                if let Some(high) = high_key {
                    let next_min = subtree_min_key(state, *next)?;
                    if next_min.as_deref() != Some(high.as_slice()) {
                        return Err(Error::corruption(
                            "Blink internal sibling fence is not continuous",
                        ));
                    }
                } else {
                    return Err(Error::corruption(
                        "Blink internal right-linked page has an infinite fence",
                    ));
                }
            }
            None if high_key.is_some() => {
                return Err(Error::corruption(
                    "Blink finite internal fence has no right sibling",
                ));
            }
            None => {}
        }
    }
    Ok(())
}

fn subtree_min_key(state: &BlinkState, mut page_id: PageId) -> Result<Option<Vec<u8>>> {
    loop {
        match BlinkMutationState::page(state, page_id) {
            Some(BlinkPage::Leaf { entries, .. }) => {
                return Ok(entries.first().map(|entry| entry.key.clone()));
            }
            Some(BlinkPage::Internal { leftmost_child, .. }) => page_id = *leftmost_child,
            _ => {
                return Err(Error::corruption(
                    "Blink subtree minimum reaches non-tree page",
                ));
            }
        }
    }
}

fn recover_data_file<F: DurableFile>(
    file: &mut F,
    batches: &[CommittedWalBatch],
    checkpoint_hint: Lsn,
) -> Result<()> {
    let mut images = Vec::new();
    let mut high_water = FIRST_DATA_PAGE;
    for batch in batches {
        if batch.commit_lsn <= checkpoint_hint {
            continue;
        }
        for image in &batch.pages {
            high_water = high_water.max(image.page_id.get());
            images.push(image);
        }
    }
    if images.is_empty() {
        return Ok(());
    }
    let length = high_water
        .checked_add(1)
        .and_then(|pages| pages.checked_mul(PAGE_SIZE as u64))
        .ok_or_else(|| Error::recovery("Blink recovery file length overflows"))?;
    if file.len()? < length {
        file.set_len(length)?;
    }
    for image in images {
        // The WAL validator already checked the format and checksum. Decode
        // again here so recovery never writes an image to the wrong physical
        // slot if the caller bypasses the normal open path in a test.
        if image.page_id.get() >= FIRST_DATA_PAGE {
            decode_blink_page(&image.image, image.page_id)?;
        } else {
            decode_blink_superblock_image(&image.image)?;
        }
        write_all_at(file, image.page_id.get() * PAGE_SIZE as u64, &image.image)?;
    }
    file.sync_data()
}

fn existing_checkpoint<F: DurableFile>(
    file: &mut F,
    identity: &WalIdentity,
    wal_length: u64,
) -> Result<Lsn> {
    if file.is_empty()? {
        return Ok(Lsn::ZERO);
    }
    if file.len()? < 2 * PAGE_SIZE as u64 {
        return Err(Error::corruption(
            "experimental database has no superblock pair",
        ));
    }
    let a = read_exact_at(file, 0, PAGE_SIZE)?;
    let b = read_exact_at(file, PAGE_SIZE as u64, PAGE_SIZE)?;
    let sb_a = decode_blink_superblock_image(&a);
    let sb_b = decode_blink_superblock_image(&b);
    let sb = match (sb_a, sb_b) {
        (Ok(a), Ok(b)) => {
            if a.generation >= b.generation {
                a
            } else {
                b
            }
        }
        (Ok(a), Err(_)) => a,
        (Err(_), Ok(b)) => b,
        (Err(a), Err(b)) => {
            return Err(Error::corruption(format!(
                "experimental superblock pair invalid: {a}; {b}"
            )));
        }
    };
    if sb.database_uuid != identity.database_uuid
        || sb.tenant_id != identity.tenant_id
        || sb.shard_id != identity.shard_id
        || sb.shard_epoch != identity.shard_epoch
    {
        return Err(Error::corruption("experimental database identity mismatch"));
    }
    // A non-empty data file with an empty WAL is valid only if it was already
    // checkpointed. The length is otherwise useful to keep this argument from
    // becoming an accidental unused assertion in format tests.
    if wal_length == 0 && sb.checkpoint_lsn > Lsn::ZERO {
        return Ok(sb.checkpoint_lsn);
    }
    Ok(sb.checkpoint_lsn)
}

fn encode_page_id(page_id: Option<PageId>) -> u64 {
    page_id.map_or(NULL_PAGE_ID, PageId::get)
}

fn decode_page_id(value: u64) -> Option<PageId> {
    (value != NULL_PAGE_ID).then(|| PageId::new(value))
}

fn elapsed_nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}

fn read_exact_at<F: DurableFile>(file: &mut F, offset: u64, length: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0u8; length];
    let mut position = 0usize;
    while position < length {
        let count = file.read_at(
            offset
                .checked_add(position as u64)
                .ok_or_else(|| Error::corruption("Blink read offset overflows"))?,
            &mut bytes[position..],
        )?;
        if count == 0 {
            return Err(Error::Io(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "unexpected end of Blink database",
            )));
        }
        position += count;
    }
    Ok(bytes)
}

fn write_all_at<F: DurableFile>(file: &mut F, offset: u64, bytes: &[u8]) -> Result<()> {
    let mut position = 0usize;
    while position < bytes.len() {
        let count = file.write_at(
            offset
                .checked_add(position as u64)
                .ok_or_else(|| Error::invalid_input("Blink write offset overflows"))?,
            &bytes[position..],
        )?;
        if count == 0 {
            return Err(Error::Io(std::io::Error::new(
                ErrorKind::WriteZero,
                "Blink file returned a zero-byte write",
            )));
        }
        position += count;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MemoryFile(Vec<u8>);

    impl DurableFile for MemoryFile {
        fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
            let offset = offset as usize;
            if offset >= self.0.len() {
                return Ok(0);
            }
            let count = buffer.len().min(self.0.len() - offset);
            buffer[..count].copy_from_slice(&self.0[offset..offset + count]);
            Ok(count)
        }
        fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
            let offset = offset as usize;
            let end = offset + bytes.len();
            if self.0.len() < end {
                self.0.resize(end, 0);
            }
            self.0[offset..end].copy_from_slice(bytes);
            Ok(bytes.len())
        }
        fn len(&self) -> Result<u64> {
            Ok(self.0.len() as u64)
        }
        fn set_len(&mut self, length: u64) -> Result<()> {
            self.0.resize(length as usize, 0);
            Ok(())
        }
        fn sync_data(&mut self) -> Result<()> {
            Ok(())
        }
        fn sync_all(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn planned_store() -> BlinkStore<MemoryFile, MemoryFile> {
        let mut store = BlinkStore::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            DatabaseConfig::default(),
        )
        .unwrap();
        store.enable_planned_execution();
        store
    }

    fn wide_key(index: u64) -> DocumentKey {
        let mut primary = vec![0x51; 280];
        primary.extend_from_slice(&index.to_be_bytes());
        DocumentKey::new(primary, vec![0x61; 280])
    }

    struct FailOnOccurrence {
        point: &'static str,
        remaining: usize,
    }

    impl FaultInjector for FailOnOccurrence {
        fn hit(&mut self, point: &str) -> Result<()> {
            if point == self.point {
                self.remaining = self.remaining.saturating_sub(1);
                if self.remaining == 0 {
                    return Err(Error::recovery(format!("injected failure at {point}")));
                }
            }
            Ok(())
        }
    }

    fn reference_apply(
        states: &mut BTreeMap<DocumentKey, RevisionState>,
        request: &TransactionRequest,
        commit_lsn: Lsn,
    ) -> bool {
        if request.validate().is_err() {
            return false;
        }
        for condition in &request.conditions {
            let observed = states
                .get(condition.key())
                .cloned()
                .unwrap_or_else(|| RevisionState::missing(Revision::ZERO));
            let matches = match condition {
                TransactionCondition::RevisionEquals {
                    expected_revision, ..
                } => observed.revision() == *expected_revision,
                TransactionCondition::Exists { .. } => !observed.is_missing(),
                TransactionCondition::NotExists { .. } => observed.is_missing(),
            };
            if !matches {
                return false;
            }
        }
        for mutation in &request.mutations {
            let state = match mutation {
                TransactionMutation::Put { value, .. } => {
                    RevisionState::present(value.clone(), Revision::from(commit_lsn))
                }
                TransactionMutation::Delete { .. } => {
                    RevisionState::missing(Revision::from(commit_lsn))
                }
            };
            states.insert(mutation.key().clone(), state);
        }
        true
    }

    #[test]
    fn experimental_superblock_is_not_baseline_format() {
        let config = DatabaseConfig::default();
        let sb = BlinkSuperblock::new(&config, PageId::new(FIRST_DATA_PAGE));
        let image = encode_blink_superblock(&sb).unwrap();
        assert!(crate::decode_superblock(&image).is_err());
        assert!(decode_blink_superblock_image(&image).is_ok());
    }

    #[test]
    fn high_key_right_link_correction_finds_stale_route() {
        let left = PageId::new(2);
        let right = PageId::new(3);
        let key = DocumentKey::new(b"p".to_vec(), b"z".to_vec());
        let left_key = DocumentKey::new(b"p".to_vec(), b"a".to_vec()).encode();
        let right_key = key.encode();
        let state = BlinkState {
            pages: BTreeMap::from([
                (
                    left,
                    BlinkPage::Leaf {
                        lsn: Lsn::ZERO,
                        high_key: Some(right_key.clone()),
                        right_sibling: Some(right),
                        entries: vec![LeafEntry {
                            key: left_key,
                            revision: Revision::new(1),
                            value: Some(BlinkValueRef::Inline(vec![1])),
                        }],
                    },
                ),
                (
                    right,
                    BlinkPage::Leaf {
                        lsn: Lsn::ZERO,
                        high_key: None,
                        right_sibling: None,
                        entries: vec![LeafEntry {
                            key: right_key,
                            revision: Revision::new(2),
                            value: Some(BlinkValueRef::Inline(vec![2])),
                        }],
                    },
                ),
            ]),
            root_page_id: left,
            free_list_head: None,
            high_water_page_id: right,
            allow_page_reuse: true,
        };
        let mut corrections = 0;
        let value = read_state(&state, &key, &mut corrections).unwrap();
        assert_eq!(value.value(), Some(&[2][..]));
        assert_eq!(corrections, 1);
    }

    #[test]
    fn page_codec_rejects_version_checksum_and_reserved_bytes() {
        let page_id = PageId::new(2);
        let page = BlinkPage::Leaf {
            lsn: Lsn::ZERO,
            high_key: None,
            right_sibling: None,
            entries: Vec::new(),
        };
        let image = encode_blink_page(page_id, &page).unwrap();
        assert!(decode_blink_page(&image, page_id).is_ok());
        let mut bad_version = image;
        bad_version[PAGE_HEADER_SIZE + 4..PAGE_HEADER_SIZE + 6]
            .copy_from_slice(&99u16.to_le_bytes());
        assert!(decode_blink_page(&bad_version, page_id).is_err());
        let mut bad_checksum = image;
        bad_checksum[100] ^= 1;
        assert!(decode_blink_page(&bad_checksum, page_id).is_err());
    }

    #[test]
    fn serial_insert_reopen_and_checker() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config.clone(),
        )
        .unwrap();
        for index in 0usize..200 {
            store
                .put(
                    DocumentKey::new(b"p".to_vec(), index.to_be_bytes().to_vec()),
                    vec![index as u8; 32],
                )
                .unwrap();
        }
        assert!(store.split_metrics().leaf_splits > 0);
        store.check_invariants().unwrap();
        store.flush().unwrap();
        let (file, wal) = store.into_files();
        let mut reopened = BlinkStore::open_with_wal(file, wal.unwrap(), config).unwrap();
        assert_eq!(reopened.scan(None, 1_000).unwrap().len(), 200);
        reopened.check_invariants().unwrap();
    }

    #[test]
    fn transaction_group_preserves_logical_order_and_failed_atomicity() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let a = DocumentKey::new(b"p".to_vec(), b"a".to_vec());
        let b = DocumentKey::new(b"p".to_vec(), b"b".to_vec());
        let c = DocumentKey::new(b"p".to_vec(), b"c".to_vec());
        let d = DocumentKey::new(b"p".to_vec(), b"d".to_vec());
        let results = store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: a.clone(),
                        value: b"A".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::Exists { key: a.clone() }],
                    vec![TransactionMutation::Put {
                        key: b.clone(),
                        value: b"B".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::Exists { key: b.clone() }],
                    vec![TransactionMutation::Put {
                        key: c.clone(),
                        value: b"C".to_vec(),
                    }],
                ),
            ])
            .unwrap();
        assert!(results.iter().all(Result::is_ok));
        assert!(store.get(&a).unwrap().value().is_some());
        assert!(store.get(&b).unwrap().value().is_some());
        assert!(store.get(&c).unwrap().value().is_some());
        let failed = store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    vec![TransactionCondition::NotExists { key: a.clone() }],
                    vec![TransactionMutation::Put {
                        key: d.clone(),
                        value: b"D".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::Exists { key: c.clone() }],
                    vec![TransactionMutation::Put {
                        key: d.clone(),
                        value: b"D2".to_vec(),
                    }],
                ),
            ])
            .unwrap();
        assert!(failed[0].is_err());
        assert!(failed[1].is_ok());
        assert_eq!(store.get(&d).unwrap().value(), Some(&b"D2"[..]));
    }

    #[test]
    fn planned_admission_stages_dependencies_and_skips_failed_deltas() {
        let mut store = planned_store();
        let k1 = DocumentKey::new(b"planned".to_vec(), b"k1".to_vec());
        let k2 = DocumentKey::new(b"planned".to_vec(), b"k2".to_vec());
        let k3 = DocumentKey::new(b"planned".to_vec(), b"k3".to_vec());
        let results = store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: k1.clone(),
                        value: b"one".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::Exists { key: k1.clone() }],
                    vec![TransactionMutation::Put {
                        key: k2.clone(),
                        value: b"two".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::Exists { key: k2.clone() }],
                    vec![TransactionMutation::Put {
                        key: k3.clone(),
                        value: b"three".to_vec(),
                    }],
                ),
            ])
            .unwrap();
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(store.get(&k1).unwrap().value(), Some(&b"one"[..]));
        assert_eq!(store.get(&k2).unwrap().value(), Some(&b"two"[..]));
        assert_eq!(store.get(&k3).unwrap().value(), Some(&b"three"[..]));

        let failed_key = DocumentKey::new(b"planned".to_vec(), b"failed".to_vec());
        let after_failed_key = DocumentKey::new(b"planned".to_vec(), b"after".to_vec());
        let failed_results = store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: failed_key.clone(),
                        value: b"first".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::NotExists {
                        key: failed_key.clone(),
                    }],
                    vec![TransactionMutation::Put {
                        key: after_failed_key.clone(),
                        value: b"must-not-appear".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    vec![TransactionCondition::Exists {
                        key: failed_key.clone(),
                    }],
                    vec![TransactionMutation::Put {
                        key: after_failed_key.clone(),
                        value: b"after".to_vec(),
                    }],
                ),
            ])
            .unwrap();
        assert!(failed_results[0].is_ok());
        assert!(matches!(failed_results[1], Err(Error::Conflict(_))));
        assert!(failed_results[2].is_ok());
        assert_eq!(
            store.get(&after_failed_key).unwrap().value(),
            Some(&b"after"[..])
        );
        let metrics = store.batch_metrics();
        assert!(metrics.conflicted_transactions >= 1);
        assert!(metrics.dependency_edges >= 2);
        assert!(metrics.same_leaf_groups > 0);
        assert_eq!(metrics.full_state_clones, 0);
    }

    #[test]
    fn planned_group_uses_sparse_working_state() {
        let mut store = planned_store();
        for index in 0..512u64 {
            store
                .put(
                    DocumentKey::new(b"sparse".to_vec(), index.to_be_bytes().to_vec()),
                    vec![index as u8; 24],
                )
                .unwrap();
        }
        let base_page_count = store.state.pages.len();
        assert!(base_page_count > 20);
        let working = WorkingBlinkState::new(&store.state, false);
        assert!(working.pages.is_empty());
        assert_eq!(working.base.pages.len(), base_page_count);
        drop(working);

        let metrics_before = store.batch_metrics();
        store
            .apply_transaction_group(&[TransactionRequest::new(
                Vec::new(),
                vec![TransactionMutation::Put {
                    key: DocumentKey::new(b"sparse".to_vec(), 64u64.to_be_bytes().to_vec()),
                    value: b"changed".to_vec(),
                }],
            )])
            .unwrap();
        let metrics_after = store.batch_metrics();
        assert_eq!(
            metrics_after.full_state_clones - metrics_before.full_state_clones,
            0
        );
        assert_eq!(
            metrics_after.state_clone_nanos - metrics_before.state_clone_nanos,
            0
        );

        let base = &store.state;
        let mut working = WorkingBlinkState::new(base, false);
        let mut dirty = BTreeSet::new();
        let mut split_metrics = BlinkSplitMetrics::default();
        apply_mutation(
            &mut working,
            &mut dirty,
            &mut split_metrics,
            &TransactionMutation::Put {
                key: DocumentKey::new(b"sparse".to_vec(), 64u64.to_be_bytes().to_vec()),
                value: b"overlay".to_vec(),
            },
            Revision::new(999),
        )
        .unwrap();
        assert!(working.pages.len() <= 3);
        assert!(working.pages.len() < base_page_count / 4);
        assert!(working.pages.keys().all(|page_id| dirty.contains(page_id)));
        assert_eq!(base.pages.len(), base_page_count);
    }

    #[test]
    fn planned_same_leaf_chain_preserves_wal_boundaries_and_revisions() {
        let mut store = planned_store();
        let key = DocumentKey::new(b"chain".to_vec(), b"key".to_vec());
        let other_key = DocumentKey::new(b"chain".to_vec(), b"other".to_vec());
        let before = store.batch_metrics();
        let results = store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: key.clone(),
                        value: b"put".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Delete { key: key.clone() }],
                ),
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: key.clone(),
                        value: b"final".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: other_key.clone(),
                        value: b"other".to_vec(),
                    }],
                ),
            ])
            .unwrap();
        let first_lsn = results[0].as_ref().unwrap().commit_lsn;
        let second_lsn = results[1].as_ref().unwrap().commit_lsn;
        let third_lsn = results[2].as_ref().unwrap().commit_lsn;
        assert!(first_lsn < second_lsn && second_lsn < third_lsn);
        assert_eq!(
            store.get(&key).unwrap(),
            RevisionState::present(b"final", third_lsn.into())
        );
        assert_eq!(
            store.get(&other_key).unwrap().revision(),
            results[3].as_ref().unwrap().commit_lsn.into()
        );
        let metrics = store.batch_metrics();
        assert_eq!(metrics.full_state_clones - before.full_state_clones, 0);
        assert!(metrics.same_leaf_groups > before.same_leaf_groups);
        assert!(metrics.coalesced_mutations > before.coalesced_mutations);
        assert!(metrics.leaf_loads < metrics.mutations_planned);
        let committed = store.wal.as_ref().unwrap().committed_batches();
        assert_eq!(committed.len(), 4);
        assert!(committed.iter().all(|batch| !batch.pages.is_empty()));
        store.check_invariants().unwrap();
    }

    #[test]
    fn planned_independent_leaf_groups_are_exposed() {
        let mut store = BlinkStore::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            DatabaseConfig::default(),
        )
        .unwrap();
        for index in 0..120u64 {
            store.put(wide_key(index), vec![index as u8; 8]).unwrap();
        }
        store.enable_planned_execution();
        let first = wide_key(0);
        let last = wide_key(10_000);
        store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: first.clone(),
                        value: b"first".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: last.clone(),
                        value: b"last".to_vec(),
                    }],
                ),
            ])
            .unwrap();
        let metrics = store.batch_metrics();
        assert!(metrics.independent_leaf_groups >= 2);
        assert!(metrics.dependency_edges < metrics.mutations_planned);
        assert_eq!(store.get(&first).unwrap().value(), Some(&b"first"[..]));
        assert_eq!(store.get(&last).unwrap().value(), Some(&b"last"[..]));
        store.check_invariants().unwrap();
    }

    #[test]
    fn planned_stale_route_reroutes_after_a_split() {
        let mut store = BlinkStore::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            DatabaseConfig::default(),
        )
        .unwrap();
        for index in 0..6u64 {
            store.put(wide_key(index), vec![index as u8; 8]).unwrap();
        }
        store.enable_planned_execution();
        let split_key = wide_key(6);
        let stale_route_key = wide_key(7);
        let results = store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: split_key.clone(),
                        value: vec![6; 8],
                    }],
                ),
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: stale_route_key.clone(),
                        value: vec![7; 8],
                    }],
                ),
            ])
            .unwrap();
        assert!(results.iter().all(Result::is_ok));
        let metrics = store.batch_metrics();
        assert!(metrics.structural_fallbacks > 0);
        assert!(metrics.reroutes > 0 || metrics.split_triggered_reroutes > 0);
        assert_eq!(
            store.get(&stale_route_key).unwrap().value(),
            Some(&vec![7; 8][..])
        );
        store.check_invariants().unwrap();
    }

    #[test]
    fn planned_structural_fallback_handles_internal_and_root_splits() {
        let mut store = BlinkStore::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            DatabaseConfig::default(),
        )
        .unwrap();
        for index in 0..500u64 {
            store.put(wide_key(index), vec![index as u8; 8]).unwrap();
        }
        let before = store.split_metrics();
        store.enable_planned_execution();
        let requests = (500..5_000u64)
            .map(|index| {
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: wide_key(index),
                        value: vec![index as u8; 8],
                    }],
                )
            })
            .collect::<Vec<_>>();
        let results = store.apply_transaction_group(&requests).unwrap();
        assert!(results.iter().all(Result::is_ok));
        let after = store.split_metrics();
        assert!(after.leaf_splits > before.leaf_splits);
        assert!(after.internal_splits > before.internal_splits);
        assert!(
            after.root_splits > before.root_splits,
            "before={before:?} after={after:?}"
        );
        assert!(store.batch_metrics().structural_fallbacks > 0);
        store.check_invariants().unwrap();
    }

    #[test]
    fn planned_multi_leaf_transaction_has_one_commit_and_atomic_publication() {
        let mut store = planned_store();
        for index in 0..120u64 {
            store.put(wide_key(index), vec![index as u8; 8]).unwrap();
        }
        let first = wide_key(0);
        let last = wide_key(10_000);
        let old_pin = store.publisher.pin();
        let read_handle = store.versioned_read_handle();
        let before_batches = store.wal.as_ref().unwrap().committed_batches().len();
        let result = store
            .transact(TransactionRequest::new(
                Vec::new(),
                vec![
                    TransactionMutation::Put {
                        key: first.clone(),
                        value: b"multi-first".to_vec(),
                    },
                    TransactionMutation::Put {
                        key: last.clone(),
                        value: b"multi-last".to_vec(),
                    },
                ],
            ))
            .unwrap();
        let mut corrections = 0;
        assert_eq!(
            read_state(&old_pin, &first, &mut corrections)
                .unwrap()
                .value(),
            Some(&vec![0; 8][..])
        );
        assert_eq!(
            read_state(&old_pin, &last, &mut corrections)
                .unwrap()
                .value(),
            None
        );
        assert_eq!(
            store.get(&first).unwrap(),
            RevisionState::present(b"multi-first", result.commit_lsn.into())
        );
        assert_eq!(
            store.get(&last).unwrap(),
            RevisionState::present(b"multi-last", result.commit_lsn.into())
        );
        assert_eq!(
            read_handle.get(&first).unwrap().value(),
            Some(&b"multi-first"[..])
        );
        assert_eq!(
            read_handle.get(&last).unwrap().value(),
            Some(&b"multi-last"[..])
        );
        assert_eq!(
            store.wal.as_ref().unwrap().committed_batches().len(),
            before_batches + 1
        );
        store.check_invariants().unwrap();
    }

    #[test]
    fn planned_wal_tail_keeps_the_first_logical_commit_boundary() {
        let mut store = planned_store();
        store.set_fault_injector(FailOnOccurrence {
            point: "before_commit_record",
            remaining: 2,
        });
        let first = DocumentKey::new(b"wal-boundary".to_vec(), b"first".to_vec());
        let second = DocumentKey::new(b"wal-boundary".to_vec(), b"second".to_vec());
        assert!(
            store
                .apply_transaction_group(&[
                    TransactionRequest::new(
                        Vec::new(),
                        vec![TransactionMutation::Put {
                            key: first.clone(),
                            value: b"first".to_vec(),
                        }],
                    ),
                    TransactionRequest::new(
                        Vec::new(),
                        vec![TransactionMutation::Put {
                            key: second.clone(),
                            value: b"second".to_vec(),
                        }],
                    ),
                ])
                .is_err()
        );
        let (data, wal) = store.into_files();
        let mut reopened =
            BlinkStore::open_with_wal(data, wal.unwrap(), DatabaseConfig::default()).unwrap();
        assert_eq!(reopened.get(&first).unwrap().value(), Some(&b"first"[..]));
        assert!(reopened.get(&second).unwrap().is_missing());
        reopened.check_invariants().unwrap();
    }

    #[test]
    fn query_scan_tombstone_overflow_checkpoint_and_reopen() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config.clone(),
        )
        .unwrap();
        let first = DocumentKey::new(b"p".to_vec(), b"a".to_vec());
        let second = DocumentKey::new(b"p".to_vec(), b"b".to_vec());
        store.put(first.clone(), vec![7; 2_000]).unwrap();
        store.put(second.clone(), b"small".to_vec()).unwrap();
        store.delete(first.clone()).unwrap();
        let query = store
            .query(&PrimaryKey::new(b"p".to_vec()), None, 10)
            .unwrap();
        assert_eq!(query.len(), 1);
        assert_eq!(query[0].key, second);
        assert_eq!(store.scan(None, 10).unwrap(), query);
        let checkpoint_pin = store.publisher.pin();
        store.checkpoint().unwrap();
        let mut corrections = 0;
        assert_eq!(
            scan_state(&checkpoint_pin, None, 10, &mut corrections).unwrap(),
            query
        );
        let (file, wal) = store.into_files();
        let mut reopened = BlinkStore::open_with_wal(file, wal.unwrap(), config).unwrap();
        assert!(reopened.get(&first).unwrap().is_missing());
        assert_eq!(reopened.scan(None, 10).unwrap(), query);
        assert_eq!(
            reopened.versioned_read_handle().scan(None, 10).unwrap(),
            query
        );
        reopened.check_invariants().unwrap();
    }

    #[test]
    fn versioned_generation_is_atomic_for_multi_page_transaction() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let a = DocumentKey::new(b"atomic".to_vec(), b"a".to_vec());
        let b = DocumentKey::new(b"atomic".to_vec(), b"b".to_vec());
        store.put(a.clone(), b"old-a".to_vec()).unwrap();
        store.put(b.clone(), b"old-b".to_vec()).unwrap();
        let old_pin = store.publisher.pin();
        let handle = store.versioned_read_handle();
        store
            .apply_transaction_group(&[
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: a.clone(),
                        value: b"new-a".to_vec(),
                    }],
                ),
                TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: b.clone(),
                        value: b"new-b".to_vec(),
                    }],
                ),
            ])
            .unwrap();

        let mut corrections = 0;
        assert_eq!(
            read_state(&old_pin, &a, &mut corrections).unwrap().value(),
            Some(&b"old-a"[..])
        );
        assert_eq!(
            read_state(&old_pin, &b, &mut corrections).unwrap().value(),
            Some(&b"old-b"[..])
        );
        assert_eq!(handle.get(&a).unwrap().value(), Some(&b"new-a"[..]));
        assert_eq!(handle.get(&b).unwrap().value(), Some(&b"new-b"[..]));
        assert_eq!(store.versioned_read_metrics().active_generation_pins, 1);
    }

    #[test]
    fn reader_during_unpublished_install_sees_only_old_generation() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let key = DocumentKey::new(b"install".to_vec(), b"key".to_vec());
        store.put(key.clone(), b"old".to_vec()).unwrap();
        let old_pin = store.publisher.pin();
        let handle = store.versioned_read_handle();

        let mut candidate = store.state.clone();
        candidate.allow_page_reuse = false;
        let mut dirty = BTreeSet::new();
        let mut split_metrics = BlinkSplitMetrics::default();
        apply_mutation(
            &mut candidate,
            &mut dirty,
            &mut split_metrics,
            &TransactionMutation::Put {
                key: key.clone(),
                value: b"prepared-but-not-published".to_vec(),
            },
            Revision::new(2),
        )
        .unwrap();
        let prepared_sb = BlinkSuperblock {
            generation: store.current_superblock.generation + 1,
            root_page_id: candidate.root_page_id,
            free_list_head: candidate.free_list_head,
            high_water_page_id: candidate.high_water_page_id,
            ..store.current_superblock.clone()
        };
        let (unpublished, _) = store
            .publisher
            .prepare(&candidate, &prepared_sb, &dirty)
            .unwrap();

        // The immutable page cells exist, but the generation pointer has not
        // moved. This is the install-before-publication window.
        assert_eq!(handle.get(&key).unwrap().value(), Some(&b"old"[..]));
        let mut corrections = 0;
        assert_eq!(
            read_state(&old_pin, &key, &mut corrections)
                .unwrap()
                .value(),
            Some(&b"old"[..])
        );
        store.publisher.publish(unpublished);
        assert_eq!(
            handle.get(&key).unwrap().value(),
            Some(&b"prepared-but-not-published"[..])
        );
    }

    #[test]
    fn root_split_and_leaf_links_are_safe_for_pinned_reader() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let first = DocumentKey::new(b"root".to_vec(), 0u64.to_be_bytes().to_vec());
        store.put(first.clone(), b"before".to_vec()).unwrap();
        let old_pin = store.publisher.pin();
        let old_root = old_pin.generation.root_page_id;
        let handle = store.versioned_read_handle();
        for index in 1..600u64 {
            store
                .put(
                    DocumentKey::new(b"root".to_vec(), index.to_be_bytes().to_vec()),
                    vec![(index & 0xff) as u8; 32],
                )
                .unwrap();
        }
        assert_ne!(store.current_superblock.root_page_id, old_root);
        let mut corrections = 0;
        assert_eq!(
            read_state(&old_pin, &first, &mut corrections)
                .unwrap()
                .value(),
            Some(&b"before"[..])
        );
        assert_eq!(handle.scan(None, 1_000).unwrap().len(), 600);
        store.check_invariants().unwrap();
    }

    #[test]
    fn page_reuse_is_delayed_until_pinned_generation_releases() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let old_key = DocumentKey::new(b"reuse".to_vec(), b"old".to_vec());
        let new_key = DocumentKey::new(b"reuse".to_vec(), b"new".to_vec());
        let value = vec![7; 2_000];
        store.put(old_key.clone(), value.clone()).unwrap();
        let old_head = match find_entry(&store.state, &old_key.encode())
            .unwrap()
            .unwrap()
            .value
        {
            Some(BlinkValueRef::Overflow { head, .. }) => head,
            _ => panic!("expected overflow value"),
        };
        let old_pin = store.publisher.pin();
        store.delete(old_key.clone()).unwrap();
        store.put(new_key.clone(), value.clone()).unwrap();
        let new_head = match find_entry(&store.state, &new_key.encode())
            .unwrap()
            .unwrap()
            .value
        {
            Some(BlinkValueRef::Overflow { head, .. }) => head,
            _ => panic!("expected overflow value"),
        };
        assert_ne!(old_head, new_head);
        assert_eq!(store.versioned_read_metrics().reusable_page_ids, 0);
        let mut corrections = 0;
        assert_eq!(
            read_state(&old_pin, &old_key, &mut corrections)
                .unwrap()
                .value(),
            Some(&value[..])
        );
        drop(old_pin);
        assert_eq!(store.versioned_read_metrics().active_generation_pins, 0);
        assert!(store.versioned_read_metrics().versions_reclaimed > 0);
        let after_key = DocumentKey::new(b"reuse".to_vec(), b"after".to_vec());
        store.put(after_key.clone(), value).unwrap();
        let after_head = match find_entry(&store.state, &after_key.encode())
            .unwrap()
            .unwrap()
            .value
        {
            Some(BlinkValueRef::Overflow { head, .. }) => head,
            _ => panic!("expected overflow value"),
        };
        assert_eq!(after_head, old_head);
    }

    #[test]
    fn query_and_scan_keep_one_generation_during_publication() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        for index in 0..40u64 {
            store
                .put(
                    DocumentKey::new(b"query".to_vec(), index.to_be_bytes().to_vec()),
                    vec![1],
                )
                .unwrap();
        }
        let old_pin = store.publisher.pin();
        let handle = store.versioned_read_handle();
        store
            .transact(TransactionRequest::new(
                Vec::new(),
                vec![TransactionMutation::Put {
                    key: DocumentKey::new(b"query".to_vec(), 10u64.to_be_bytes().to_vec()),
                    value: vec![9],
                }],
            ))
            .unwrap();
        let mut corrections = 0;
        let old_query = query_state(
            &old_pin,
            &PrimaryKey::new(b"query".to_vec()),
            None,
            40,
            &mut corrections,
        )
        .unwrap();
        let old_scan = scan_state(&old_pin, None, 40, &mut corrections).unwrap();
        assert!(old_query.iter().all(|row| row.value == vec![1]));
        assert!(old_scan.iter().all(|row| row.value == vec![1]));
        assert_eq!(
            handle
                .query(&PrimaryKey::new(b"query".to_vec()), None, 40)
                .unwrap()
                .iter()
                .find(|row| row.key.sk == SortKey::new(10u64.to_be_bytes().to_vec()))
                .unwrap()
                .value,
            vec![9]
        );
    }

    struct FailOnce {
        point: &'static str,
        fired: bool,
    }

    impl FaultInjector for FailOnce {
        fn hit(&mut self, point: &str) -> Result<()> {
            if !self.fired && point == self.point {
                self.fired = true;
                return Err(Error::recovery(format!("injected failure at {point}")));
            }
            Ok(())
        }
    }

    #[test]
    fn wal_sync_failure_does_not_publish_a_generation() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let handle = store.versioned_read_handle();
        store.set_fault_injector(FailOnce {
            point: "before_wal_sync",
            fired: false,
        });
        let key = DocumentKey::new(b"wal".to_vec(), b"failed".to_vec());
        assert!(store.put(key.clone(), b"not-visible".to_vec()).is_err());
        assert!(handle.get(&key).unwrap().is_missing());
        assert_eq!(store.versioned_read_metrics().published_generations, 0);
    }

    #[test]
    fn planned_wal_failure_does_not_install_working_delta() {
        let mut store = planned_store();
        let existing_key = DocumentKey::new(b"wal-atomic".to_vec(), b"existing".to_vec());
        store.put(existing_key.clone(), b"before".to_vec()).unwrap();
        let before_contents = store.scan(None, 100).unwrap();
        let before_metadata = (
            store.state.root_page_id,
            store.state.free_list_head,
            store.state.high_water_page_id,
            store.current_superblock.clone(),
            store.next_revision,
            store.next_lsn,
            store.next_batch_id,
            store.versioned_read_metrics().published_generations,
        );
        let before_pages = store
            .state
            .pages
            .iter()
            .map(|(page_id, page)| (*page_id, encode_blink_page(*page_id, page).unwrap()))
            .collect::<Vec<_>>();
        store.set_fault_injector(FailOnce {
            point: "before_wal_sync",
            fired: false,
        });
        let failed_key = DocumentKey::new(b"wal-atomic".to_vec(), b"failed".to_vec());
        assert!(
            store
                .apply_transaction_group(&[TransactionRequest::new(
                    Vec::new(),
                    vec![
                        TransactionMutation::Put {
                            key: existing_key,
                            value: b"after".to_vec(),
                        },
                        TransactionMutation::Put {
                            key: failed_key,
                            value: vec![8; 4_000],
                        },
                    ],
                )])
                .is_err()
        );
        assert_eq!(store.scan(None, 100).unwrap(), before_contents);
        assert_eq!(
            (
                store.state.root_page_id,
                store.state.free_list_head,
                store.state.high_water_page_id,
                store.current_superblock.clone(),
                store.next_revision,
                store.next_lsn,
                store.next_batch_id,
                store.versioned_read_metrics().published_generations,
            ),
            before_metadata
        );
        let after_pages = store
            .state
            .pages
            .iter()
            .map(|(page_id, page)| (*page_id, encode_blink_page(*page_id, page).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(after_pages, before_pages);
    }

    #[test]
    fn concurrent_versioned_readers_survive_serial_writer_publications() {
        use std::sync::Mutex as StdMutex;
        use std::thread;

        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let key = DocumentKey::new(b"stress".to_vec(), b"key".to_vec());
        store.put(key.clone(), vec![0]).unwrap();
        let handle = store.versioned_read_handle();
        let writer = Arc::new(StdMutex::new(store));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let handle = handle.clone();
            let key = key.clone();
            readers.push(thread::spawn(move || {
                for _ in 0..500 {
                    assert!(handle.get(&key).unwrap().value().is_some());
                    assert_eq!(handle.scan(None, 1).unwrap().len(), 1);
                }
            }));
        }
        for value in 1..100u8 {
            writer
                .lock()
                .unwrap()
                .put(key.clone(), vec![value])
                .unwrap();
        }
        for reader in readers {
            reader.join().unwrap();
        }
        let store = match Arc::try_unwrap(writer) {
            Ok(writer) => writer.into_inner().unwrap(),
            Err(_) => panic!("writer Arc should have no remaining references"),
        };
        assert_eq!(store.versioned_read_metrics().active_generation_pins, 0);
        store.check_invariants().unwrap();
    }

    #[test]
    fn randomized_concurrent_reads_and_serial_writes_preserve_ordering() {
        use std::sync::Mutex as StdMutex;
        use std::thread;

        let seed = 0x2a02_2026_u64;
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        for index in 0..128u64 {
            store
                .put(
                    DocumentKey::new(b"random".to_vec(), index.to_be_bytes().to_vec()),
                    vec![index as u8],
                )
                .unwrap();
        }
        let handle = store.versioned_read_handle();
        let writer = Arc::new(StdMutex::new(store));
        let mut readers = Vec::new();
        for reader_id in 0..6u64 {
            let handle = handle.clone();
            readers.push(thread::spawn(move || {
                let mut state = seed ^ reader_id;
                for _ in 0..1_000 {
                    state = splitmix_for_test(state);
                    let key =
                        DocumentKey::new(b"random".to_vec(), (state % 128).to_be_bytes().to_vec());
                    match state % 3 {
                        0 => {
                            let _ = handle.get(&key).unwrap();
                        }
                        1 => {
                            let rows = handle.scan(None, 16).unwrap();
                            assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
                        }
                        _ => {
                            let rows = handle
                                .query(&PrimaryKey::new(b"random".to_vec()), None, 16)
                                .unwrap();
                            assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
                        }
                    }
                }
            }));
        }
        let mut state = seed;
        for operation in 0..300u64 {
            state = splitmix_for_test(state);
            let key = DocumentKey::new(b"random".to_vec(), (state % 128).to_be_bytes().to_vec());
            let mut store = writer.lock().unwrap();
            if state & 1 == 0 {
                store.put(key, vec![(operation & 0xff) as u8]).unwrap();
            } else {
                store.delete(key).unwrap();
            }
        }
        for reader in readers {
            reader.join().unwrap();
        }
        let store = match Arc::try_unwrap(writer) {
            Ok(writer) => writer.into_inner().unwrap(),
            Err(_) => panic!("writer Arc should have no remaining references"),
        };
        assert_eq!(store.versioned_read_metrics().active_generation_pins, 0);
        store.check_invariants().unwrap();
    }

    #[test]
    fn baseline_and_experimental_formats_reject_each_other() {
        let config = DatabaseConfig::default();
        let mut blink = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config.clone(),
        )
        .unwrap();
        blink
            .put(
                DocumentKey::new(b"p".to_vec(), b"a".to_vec()),
                b"v".to_vec(),
            )
            .unwrap();
        blink.flush().unwrap();
        let (blink_file, blink_wal) = blink.into_files();
        assert!(
            crate::BTreeStore::open_with_wal(blink_file, blink_wal.unwrap(), config.clone())
                .is_err()
        );

        let mut baseline = crate::BTreeStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config.clone(),
        )
        .unwrap();
        baseline
            .put(
                DocumentKey::new(b"p".to_vec(), b"a".to_vec()),
                b"v".to_vec(),
            )
            .unwrap();
        baseline.flush().unwrap();
        let (baseline_file, baseline_wal) = baseline.into_files().unwrap();
        assert!(BlinkStore::open_with_wal(baseline_file, baseline_wal, config).is_err());
    }

    #[test]
    fn planned_randomized_transaction_differential_reports_seed_and_operation() {
        let seed = 0x3a03_2026_u64;
        let mut state = seed;
        let mut store = planned_store();
        let mut reference = BTreeMap::<DocumentKey, RevisionState>::new();
        for operation_index in 0..300usize {
            state = splitmix_for_test(state);
            let mut requests = Vec::new();
            if operation_index % 7 == 0 {
                let first_key =
                    DocumentKey::new(b"staged".to_vec(), (state % 64).to_be_bytes().to_vec());
                let second_key = DocumentKey::new(
                    b"staged".to_vec(),
                    ((state + 1) % 64).to_be_bytes().to_vec(),
                );
                requests.push(TransactionRequest::new(
                    Vec::new(),
                    vec![TransactionMutation::Put {
                        key: first_key.clone(),
                        value: vec![operation_index as u8],
                    }],
                ));
                requests.push(TransactionRequest::new(
                    vec![TransactionCondition::Exists { key: first_key }],
                    vec![TransactionMutation::Put {
                        key: second_key,
                        value: vec![(operation_index + 1) as u8],
                    }],
                ));
            } else {
                let key_index = state % 64;
                let key = if state & 8 == 0 {
                    wide_key(key_index)
                } else {
                    DocumentKey::new(b"random".to_vec(), key_index.to_be_bytes().to_vec())
                };
                state = splitmix_for_test(state);
                let condition = match state % 4 {
                    0 => None,
                    1 => Some(
                        if reference.get(&key).is_some_and(|value| !value.is_missing()) {
                            TransactionCondition::Exists { key: key.clone() }
                        } else {
                            TransactionCondition::NotExists { key: key.clone() }
                        },
                    ),
                    2 => Some(TransactionCondition::RevisionEquals {
                        key: key.clone(),
                        expected_revision: reference
                            .get(&key)
                            .map(RevisionState::revision)
                            .unwrap_or(Revision::ZERO),
                    }),
                    _ => Some(TransactionCondition::RevisionEquals {
                        key: key.clone(),
                        expected_revision: Revision::new(9_000_000 + operation_index as u64),
                    }),
                };
                state = splitmix_for_test(state);
                let mutation = if state & 1 == 0 {
                    TransactionMutation::Put {
                        key: key.clone(),
                        value: vec![(operation_index & 0xff) as u8; 8],
                    }
                } else {
                    TransactionMutation::Delete { key: key.clone() }
                };
                let mut mutations = vec![mutation];
                if state & 2 == 0 {
                    let second_key = DocumentKey::new(
                        b"random".to_vec(),
                        ((key_index + 1) % 64).to_be_bytes().to_vec(),
                    );
                    if second_key != key {
                        mutations.push(TransactionMutation::Put {
                            key: second_key,
                            value: vec![operation_index as u8; 4],
                        });
                    }
                }
                requests.push(TransactionRequest::new(
                    condition.into_iter().collect(),
                    mutations,
                ));
            }

            let actual_results = store
                .apply_transaction_group(&requests)
                .unwrap_or_else(|error| {
                    panic!("seed={seed:#x} operation={operation_index} error={error}")
                });
            assert_eq!(actual_results.len(), requests.len());
            for (request, actual_result) in requests.iter().zip(actual_results) {
                let expected_accept = {
                    let mut candidate = reference.clone();
                    reference_apply(&mut candidate, request, Lsn::new(1))
                };
                match (expected_accept, actual_result) {
                    (true, Ok(result)) => {
                        assert!(reference_apply(&mut reference, request, result.commit_lsn));
                    }
                    (false, Err(_)) => {}
                    (expected, actual) => panic!(
                        "seed={seed:#x} operation={operation_index} expected_accept={expected} actual={actual:?}"
                    ),
                }
            }
            if operation_index % 25 == 0 {
                for key_index in 0..64u64 {
                    let key =
                        DocumentKey::new(b"random".to_vec(), key_index.to_be_bytes().to_vec());
                    assert_eq!(
                        store.get(&key).unwrap(),
                        reference
                            .get(&key)
                            .cloned()
                            .unwrap_or_else(|| RevisionState::missing(Revision::ZERO)),
                        "seed={seed:#x} operation={operation_index} key={key:?}"
                    );
                }
            }
        }
        store.flush().unwrap();
        let (data, wal) = store.into_files();
        let mut reopened =
            BlinkStore::open_with_wal(data, wal.unwrap(), DatabaseConfig::default()).unwrap();
        for (key, expected) in &reference {
            assert_eq!(
                reopened.get(key).unwrap(),
                *expected,
                "seed={seed:#x} reopened key={key:?}"
            );
        }
        reopened.check_invariants().unwrap();
    }

    #[test]
    fn randomized_differential_seed_is_reproducible() {
        let seed = 0x51a1_2026_u64;
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        let mut reference = BTreeMap::<DocumentKey, (Vec<u8>, Revision)>::new();
        let mut state = seed;
        for operation in 0..500usize {
            state = splitmix_for_test(state);
            let index = (state as usize) % 100;
            let key = DocumentKey::new(b"p".to_vec(), index.to_be_bytes().to_vec());
            state = splitmix_for_test(state);
            if state & 1 == 0 {
                let value = vec![(operation & 0xff) as u8; 8 + operation % 16];
                let revision = store.put(key.clone(), value.clone()).unwrap();
                reference.insert(key, (value, revision));
            } else {
                store.delete(key.clone()).unwrap();
                reference.remove(&key);
            }
            state = splitmix_for_test(state);
            let get_key = DocumentKey::new(
                b"p".to_vec(),
                ((state as usize) % 100).to_be_bytes().to_vec(),
            );
            let actual = store.get(&get_key).unwrap();
            match reference.get(&get_key) {
                Some((value, revision)) => assert_eq!(
                    actual,
                    RevisionState::present(value.clone(), *revision),
                    "seed={seed:#x} operation={operation} key={get_key:?}"
                ),
                None => assert!(actual.is_missing(), "seed={seed:#x} operation={operation}"),
            }
        }
        store.check_invariants().unwrap();
    }

    #[test]
    fn structural_stress_reaches_internal_and_root_splits() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        for index in 0usize..1_000 {
            let mut pk = vec![0x51; 280];
            pk.extend_from_slice(&(index as u64).to_be_bytes());
            let key = DocumentKey::new(pk, vec![0x61; 280]);
            store.put(key, vec![index as u8; 8]).unwrap();
        }
        let metrics = store.split_metrics();
        assert!(metrics.leaf_splits > 0);
        assert!(metrics.internal_splits > 0);
        assert!(metrics.root_splits > 1);
        store.check_invariants().unwrap();
    }

    #[test]
    fn checker_rejects_sibling_cycle_wrong_level_and_unordered_separator() {
        let config = DatabaseConfig::default();
        let mut store = BlinkStore::<MemoryFile, MemoryFile>::open_with_wal(
            MemoryFile::default(),
            MemoryFile::default(),
            config,
        )
        .unwrap();
        for index in 0usize..300 {
            store
                .put(
                    DocumentKey::new(b"p".to_vec(), index.to_be_bytes().to_vec()),
                    vec![index as u8; 32],
                )
                .unwrap();
        }
        let leaf_id = store
            .state
            .pages
            .iter()
            .find_map(|(id, page)| {
                matches!(
                    page,
                    BlinkPage::Leaf {
                        right_sibling: Some(_),
                        ..
                    }
                )
                .then_some(*id)
            })
            .unwrap();
        let mut cycle = store.state.clone();
        if let Some(BlinkPage::Leaf { right_sibling, .. }) = cycle.pages.get_mut(&leaf_id) {
            *right_sibling = Some(leaf_id);
        }
        assert!(check_state(&cycle).is_err());

        let mut wrong_level = store.state.clone();
        let root = wrong_level.root_page_id;
        if let Some(BlinkPage::Leaf { right_sibling, .. }) = wrong_level.pages.get_mut(&leaf_id) {
            *right_sibling = Some(root);
        }
        assert!(check_state(&wrong_level).is_err());

        let internal_id = store
            .state
            .pages
            .iter()
            .find_map(|(id, page)| {
                matches!(page, BlinkPage::Internal { entries, .. } if entries.len() > 1)
                    .then_some(*id)
            })
            .unwrap();
        let mut unordered = store.state.clone();
        if let Some(BlinkPage::Internal { entries, .. }) = unordered.pages.get_mut(&internal_id) {
            entries.swap(0, 1);
        }
        assert!(check_state(&unordered).is_err());
    }

    fn splitmix_for_test(mut state: u64) -> u64 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}
