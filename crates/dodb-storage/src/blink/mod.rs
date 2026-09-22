//! Experimental serial B-link tree.
//!
//! This module is deliberately independent from [`crate::btree`].  It is the
//! Phase 1 control implementation: one caller mutates the tree at a time, but
//! pages already carry the fences and sibling links that later phases will
//! use for optimistic reads and parallel execution.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::ErrorKind;
use std::path::Path;
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
        };
        let sb = BlinkSuperblock::new(&config, root);
        let sb_bytes = encode_blink_superblock(&sb)?;
        let root_bytes = encode_blink_page(root, state.pages.get(&root).unwrap())?;
        file.set_len((FIRST_DATA_PAGE + 1) * PAGE_SIZE as u64)?;
        write_all_at(&mut file, 0, &sb_bytes)?;
        write_all_at(&mut file, PAGE_SIZE as u64, &sb_bytes)?;
        write_all_at(&mut file, root.get() * PAGE_SIZE as u64, &root_bytes)?;
        file.sync_all()?;
        Ok(Self {
            file,
            wal: None,
            state,
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
        };
        let store = Self {
            file,
            wal: None,
            state,
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

    pub fn current_superblock_generation(&self) -> u64 {
        self.current_superblock.generation
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
        let started = Instant::now();
        for request in requests {
            match request {
                BatchRequest::Get { key } => responses.push(BatchResponse::Get(read_state(
                    &self.state,
                    key,
                    &mut self.split_metrics,
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
                    &mut self.split_metrics,
                )?)),
                BatchRequest::Scan {
                    exclusive_after_key,
                    limit,
                } => responses.push(BatchResponse::Scan(scan_state(
                    &self.state,
                    exclusive_after_key.as_ref(),
                    *limit,
                    &mut self.split_metrics,
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
                state: candidate.clone(),
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
        let final_candidate = candidates.last().unwrap();
        let final_sb = encode_blink_superblock(&final_candidate.superblock)?;
        self.state = final_candidate.state.clone();
        self.current_superblock = final_candidate.superblock.clone();
        self.active_slot = final_candidate.slot;
        self.next_revision = final_candidate.next_revision;
        self.next_lsn = final_candidate.next_lsn;
        self.next_batch_id = final_candidate.next_batch_id;
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

fn validate_conditions(state: &BlinkState, conditions: &[TransactionCondition]) -> Result<()> {
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

fn observed_state(state: &BlinkState, key: &DocumentKey) -> Result<ObservedState> {
    match find_entry(state, &key.encode())? {
        Some(entry) if entry.value.is_some() => Ok(ObservedState::present(entry.revision)),
        Some(entry) => Ok(ObservedState::missing(entry.revision)),
        None => Ok(ObservedState::missing(Revision::ZERO)),
    }
}

fn read_state(
    state: &BlinkState,
    key: &DocumentKey,
    metrics: &mut BlinkSplitMetrics,
) -> Result<RevisionState> {
    let encoded = key.encode();
    validate_encoded_key(&encoded)?;
    let Some(entry) = find_entry_with_metrics(state, &encoded, metrics)? else {
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

fn query_state(
    state: &BlinkState,
    pk: &PrimaryKey,
    exclusive_after_sk: Option<&SortKey>,
    limit: usize,
    metrics: &mut BlinkSplitMetrics,
) -> Result<Vec<Document>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let start = DocumentKey::new(pk.as_bytes().to_vec(), Vec::new());
    let cursor = exclusive_after_sk
        .map(|sk| DocumentKey::new(pk.as_bytes().to_vec(), sk.as_bytes().to_vec()));
    let mut leaf_id = find_leaf_with_metrics(state, &start.encode(), metrics)?;
    let mut first = true;
    let mut visited = HashSet::new();
    let mut output = Vec::new();
    while output.len() < limit {
        if !visited.insert(leaf_id) {
            return Err(Error::corruption("Blink leaf chain cycle during query"));
        }
        let BlinkPage::Leaf {
            entries,
            right_sibling,
            ..
        } = state
            .pages
            .get(&leaf_id)
            .ok_or_else(|| Error::corruption("Blink query leaf is missing"))?
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

fn scan_state(
    state: &BlinkState,
    cursor: Option<&DocumentKey>,
    limit: usize,
    metrics: &mut BlinkSplitMetrics,
) -> Result<Vec<Document>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut leaf_id = match cursor {
        Some(key) => find_leaf_with_metrics(state, &key.encode(), metrics)?,
        None => leftmost_leaf(state)?,
    };
    let cursor = cursor.map(DocumentKey::encode);
    let mut visited = HashSet::new();
    let mut output = Vec::new();
    loop {
        if !visited.insert(leaf_id) {
            return Err(Error::corruption("Blink leaf chain cycle during scan"));
        }
        let BlinkPage::Leaf {
            entries,
            right_sibling,
            ..
        } = state
            .pages
            .get(&leaf_id)
            .ok_or_else(|| Error::corruption("Blink scan leaf is missing"))?
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

fn find_entry<'a>(state: &'a BlinkState, key: &[u8]) -> Result<Option<&'a LeafEntry>> {
    let mut metrics = BlinkSplitMetrics::default();
    find_entry_with_metrics(state, key, &mut metrics)
}

fn find_entry_with_metrics<'a>(
    state: &'a BlinkState,
    key: &[u8],
    metrics: &mut BlinkSplitMetrics,
) -> Result<Option<&'a LeafEntry>> {
    let leaf_id = find_leaf_with_metrics(state, key, metrics)?;
    let BlinkPage::Leaf { entries, .. } = state
        .pages
        .get(&leaf_id)
        .ok_or_else(|| Error::corruption("Blink leaf is missing"))?
    else {
        return Err(Error::corruption("Blink route ended at non-leaf"));
    };
    Ok(entries.iter().find(|entry| entry.key == key))
}

fn find_leaf_with_metrics(
    state: &BlinkState,
    key: &[u8],
    metrics: &mut BlinkSplitMetrics,
) -> Result<PageId> {
    let mut page_id = state.root_page_id;
    let mut guard = HashSet::new();
    loop {
        if !guard.insert(page_id) {
            return Err(Error::corruption("Blink tree route contains a cycle"));
        }
        let page = state
            .pages
            .get(&page_id)
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

fn leftmost_leaf(state: &BlinkState) -> Result<PageId> {
    let mut page_id = state.root_page_id;
    loop {
        match state.pages.get(&page_id) {
            Some(BlinkPage::Leaf { .. }) => return Ok(page_id),
            Some(BlinkPage::Internal { leftmost_child, .. }) => page_id = *leftmost_child,
            _ => return Err(Error::corruption("Blink leftmost path is invalid")),
        }
    }
}

fn materialize_value(state: &BlinkState, value: &BlinkValueRef) -> Result<Vec<u8>> {
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
                let BlinkPage::Overflow { next, chunk, .. } = state
                    .pages
                    .get(&id)
                    .ok_or_else(|| Error::corruption("Blink overflow page is missing"))?
                else {
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

fn apply_mutation(
    state: &mut BlinkState,
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
        .pages
        .get(&leaf_id)
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
            state.pages.insert(
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
                state.pages.insert(
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

fn find_leaf_with_path(
    state: &BlinkState,
    key: &[u8],
    path: &mut Vec<PageId>,
    metrics: &mut BlinkSplitMetrics,
) -> Result<PageId> {
    let mut page_id = state.root_page_id;
    let mut guard = HashSet::new();
    loop {
        if !guard.insert(page_id) {
            return Err(Error::corruption("Blink route contains a cycle"));
        }
        let page = state
            .pages
            .get(&page_id)
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

fn allocate_value(
    state: &mut BlinkState,
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
        state.pages.insert(
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

fn free_value(
    state: &mut BlinkState,
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
        let next = match state.pages.get(&id) {
            Some(BlinkPage::Overflow { next, .. }) => *next,
            Some(_) => return Err(Error::corruption("Blink overflow free has wrong page type")),
            None => return Err(Error::corruption("Blink overflow free page is missing")),
        };
        state.pages.insert(
            id,
            BlinkPage::Free {
                lsn: Lsn::ZERO,
                next: state.free_list_head,
            },
        );
        state.free_list_head = Some(id);
        dirty.insert(id);
        page_id = next;
    }
    Ok(())
}

fn allocate_page(state: &mut BlinkState, dirty: &mut BTreeSet<PageId>) -> PageId {
    if let Some(id) = state.free_list_head {
        let next = match state.pages.get(&id) {
            Some(BlinkPage::Free { next, .. }) => *next,
            _ => None,
        };
        state.free_list_head = next;
        dirty.insert(id);
        return id;
    }
    let id = PageId::new(state.high_water_page_id.get() + 1);
    state.high_water_page_id = id;
    dirty.insert(id);
    id
}

fn split_leaf(
    state: &mut BlinkState,
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
    state.pages.insert(
        leaf_id,
        BlinkPage::Leaf {
            lsn: Lsn::new(revision.get()),
            high_key: Some(separator.clone()),
            right_sibling: Some(right_id),
            entries: entries[..split].to_vec(),
        },
    );
    state.pages.insert(
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

fn install_separator(
    state: &mut BlinkState,
    dirty: &mut BTreeSet<PageId>,
    metrics: &mut BlinkSplitMetrics,
    path: Vec<PageId>,
    left_child: PageId,
    separator: Vec<u8>,
    right_child: PageId,
    revision: Revision,
) -> Result<()> {
    let Some(parent_id) = path.last().copied() else {
        let old_root = state.root_page_id;
        let level = page_level(state, old_root)? + 1;
        let root = allocate_page(state, dirty);
        state.pages.insert(
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
        state.root_page_id = root;
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
        .pages
        .get(&parent_id)
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
        state.pages.insert(
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

fn split_internal(
    state: &mut BlinkState,
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
    state.pages.insert(
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
    state.pages.insert(
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

fn page_level(state: &BlinkState, page_id: PageId) -> Result<u16> {
    match state.pages.get(&page_id) {
        Some(BlinkPage::Leaf { .. }) => Ok(0),
        Some(BlinkPage::Internal { level, .. }) => Ok(*level),
        _ => Err(Error::corruption("Blink page is not a tree page")),
    }
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
        match state.pages.get(&page_id) {
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
        };
        let mut metrics = BlinkSplitMetrics::default();
        let value = read_state(&state, &key, &mut metrics).unwrap();
        assert_eq!(value.value(), Some(&[2][..]));
        assert_eq!(metrics.right_link_corrections, 1);
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
        store.checkpoint().unwrap();
        let (file, wal) = store.into_files();
        let mut reopened = BlinkStore::open_with_wal(file, wal.unwrap(), config).unwrap();
        assert!(reopened.get(&first).unwrap().is_missing());
        assert_eq!(reopened.scan(None, 10).unwrap(), query);
        reopened.check_invariants().unwrap();
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
