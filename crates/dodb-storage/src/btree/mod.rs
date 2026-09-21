//! Phase 1 single-file mutable B+Tree.
//!
//! The engine owns one [`DurableFile`], uses the Phase 0 double superblock,
//! and publishes a prepared operation batch as full page images.  The direct
//! publisher is intentionally not a WAL: Phase 1 has clean-reopen support but
//! makes no crash-safety or commit-durability claim.

mod cache;
mod checker;
mod coordinator;
mod format;

pub use coordinator::AsyncShard;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use dodb_core::{
    DocumentKey, Error, Lsn, PageId, PrimaryKey, Result, Revision, RevisionState, ShardEpoch,
    ShardId, SortKey, TenantId,
};

use self::cache::PageCache;
use self::format::{
    INLINE_VALUE_LIMIT, MAX_OVERFLOW_PAGES, MAX_VALUE_SIZE, PageData, ValueRef, internal_fits,
    leaf_fits,
};
use crate::PAGE_SIZE;
use crate::{
    DurableFile, ProductionFile, Superblock, SuperblockSlot, choose_superblock, decode_page_at,
    decode_superblock, encode_superblock,
};

const FIRST_DATA_PAGE: u64 = 2;
/// Maximum canonical key length supported by a leaf record, in bytes.
pub const MAX_ENCODED_KEY_SIZE: usize = 3992;
const SUPERBLOCK_A_OFFSET: u64 = 0;
const SUPERBLOCK_B_OFFSET: u64 = PAGE_SIZE as u64;

/// The supported value and page-layout limits for Phase 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageLimits {
    /// Values at or below this size are stored inline when the leaf record fits.
    pub inline_value_limit: usize,
    /// Larger values are stored in overflow pages up to this size.
    pub max_value_size: usize,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            inline_value_limit: INLINE_VALUE_LIMIT,
            max_value_size: MAX_VALUE_SIZE,
        }
    }
}

/// Configuration used only when creating a new file and for cache sizing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseConfig {
    pub database_uuid: [u8; 16],
    pub tenant_id: TenantId,
    pub shard_id: ShardId,
    pub shard_epoch: ShardEpoch,
    pub cache_capacity: usize,
    pub limits: StorageLimits,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_uuid: [0; 16],
            tenant_id: TenantId::ZERO,
            shard_id: ShardId::ZERO,
            shard_epoch: ShardEpoch::ZERO,
            cache_capacity: 128,
            limits: StorageLimits::default(),
        }
    }
}

impl DatabaseConfig {
    pub fn with_cache_capacity(mut self, capacity: usize) -> Self {
        self.cache_capacity = capacity;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    pub key: DocumentKey,
    pub value: Vec<u8>,
    pub revision: Revision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put { key: DocumentKey, value: Vec<u8> },
    Delete { key: DocumentKey },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchRequest {
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchResponse {
    Get(RevisionState),
    Put(Revision),
    Delete(Revision),
    Query(Vec<Document>),
    Scan(Vec<Document>),
}

pub type EngineRequest = BatchRequest;
pub type EngineResponse = BatchResponse;

/// A complete page-oriented result of preparing one request batch.
///
/// The changed pages are full encoded page images.  Phase 2 can consume this
/// map as WAL after-images before calling [`BTreeStore::publish_prepared`].
#[derive(Debug)]
pub struct PreparedBatch {
    responses: Vec<BatchResponse>,
    changed_pages: BTreeMap<PageId, [u8; PAGE_SIZE]>,
    changed_decoded: BTreeMap<PageId, PageData>,
    read_pages: BTreeMap<PageId, PageData>,
    new_superblock: Superblock,
    new_slot: SuperblockSlot,
    base_generation: u64,
    next_revision: Revision,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
}

impl PreparedBatch {
    pub fn responses(&self) -> &[BatchResponse] {
        &self.responses
    }

    pub fn changed_pages(&self) -> &BTreeMap<PageId, [u8; PAGE_SIZE]> {
        &self.changed_pages
    }

    pub fn superblock(&self) -> &Superblock {
        &self.new_superblock
    }

    pub fn superblock_image(&self) -> Result<[u8; PAGE_SIZE]> {
        encode_superblock(&self.new_superblock)
    }

    pub fn superblock_slot(&self) -> SuperblockSlot {
        self.new_slot
    }

    pub fn read_page_ids(&self) -> impl Iterator<Item = PageId> + '_ {
        self.read_pages.keys().copied()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantReport {
    pub reachable_pages: usize,
    pub free_pages: usize,
    pub leaked_pages: Vec<PageId>,
    pub max_revision: Revision,
}

/// A mutable, single-file B+Tree over canonical encoded document keys.
pub struct BTreeStore<F: DurableFile> {
    file: F,
    current_superblock: Superblock,
    active_slot: SuperblockSlot,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
    next_revision: Revision,
    cache: PageCache,
    config: DatabaseConfig,
}

impl<F: DurableFile> BTreeStore<F> {
    /// Opens an existing database or initializes a truly empty file.
    ///
    /// A non-empty file is never recreated.  It must contain two complete
    /// superblock pages and a valid selected tree.
    pub fn open(mut file: F, config: DatabaseConfig) -> Result<Self> {
        let length = file.len()?;
        if length == 0 {
            return Self::initialize(file, config);
        }
        if length < (FIRST_DATA_PAGE * PAGE_SIZE as u64) || !length.is_multiple_of(PAGE_SIZE as u64)
        {
            return Err(Error::corruption(
                "database file length is not page aligned",
            ));
        }

        let slot_a = read_exact_at(&mut file, SUPERBLOCK_A_OFFSET, PAGE_SIZE)?;
        let slot_b = read_exact_at(&mut file, SUPERBLOCK_B_OFFSET, PAGE_SIZE)?;
        let selected = choose_superblock(&slot_a, &slot_b)?;
        let (root_page_id, free_list_head, high_water_page_id) =
            metadata_from_superblock(&selected.superblock, length)?;
        let mut store = Self {
            file,
            current_superblock: selected.superblock,
            active_slot: selected.slot,
            root_page_id,
            free_list_head,
            high_water_page_id,
            next_revision: Revision::new(1),
            cache: PageCache::new(config.cache_capacity),
            config,
        };
        let report = store.check_invariants()?;
        store.next_revision = Revision::new(
            report
                .max_revision
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::invariant("storage revision exhausted"))?,
        );
        Ok(store)
    }

    pub fn open_path(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<BTreeStore<ProductionFile>> {
        BTreeStore::open(ProductionFile::open(path)?, config)
    }

    pub fn current_superblock(&self) -> &Superblock {
        &self.current_superblock
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&mut self, key: &DocumentKey) -> Result<RevisionState> {
        let request = BatchRequest::Get { key: key.clone() };
        match self.apply_batch(std::slice::from_ref(&request))?.remove(0) {
            BatchResponse::Get(state) => Ok(state),
            _ => Err(Error::invariant(
                "get produced an unexpected batch response",
            )),
        }
    }

    pub fn put(&mut self, key: DocumentKey, value: impl Into<Vec<u8>>) -> Result<Revision> {
        let request = BatchRequest::Put {
            key,
            value: value.into(),
        };
        match self.apply_batch(std::slice::from_ref(&request))?.remove(0) {
            BatchResponse::Put(revision) => Ok(revision),
            _ => Err(Error::invariant(
                "put produced an unexpected batch response",
            )),
        }
    }

    pub fn delete(&mut self, key: DocumentKey) -> Result<Revision> {
        let request = BatchRequest::Delete { key };
        match self.apply_batch(std::slice::from_ref(&request))?.remove(0) {
            BatchResponse::Delete(revision) => Ok(revision),
            _ => Err(Error::invariant(
                "delete produced an unexpected batch response",
            )),
        }
    }

    pub fn query(
        &mut self,
        pk: &PrimaryKey,
        exclusive_after_sk: Option<&SortKey>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let request = BatchRequest::Query {
            pk: pk.clone(),
            exclusive_after_sk: exclusive_after_sk.cloned(),
            limit,
        };
        match self.apply_batch(std::slice::from_ref(&request))?.remove(0) {
            BatchResponse::Query(rows) => Ok(rows),
            _ => Err(Error::invariant(
                "query produced an unexpected batch response",
            )),
        }
    }

    pub fn scan(
        &mut self,
        exclusive_after_key: Option<&DocumentKey>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let request = BatchRequest::Scan {
            exclusive_after_key: exclusive_after_key.cloned(),
            limit,
        };
        match self.apply_batch(std::slice::from_ref(&request))?.remove(0) {
            BatchResponse::Scan(rows) => Ok(rows),
            _ => Err(Error::invariant(
                "scan produced an unexpected batch response",
            )),
        }
    }

    pub fn apply_batch(&mut self, requests: &[BatchRequest]) -> Result<Vec<BatchResponse>> {
        let prepared = self.prepare_batch(requests)?;
        self.publish_prepared(prepared)
    }

    pub fn prepare_batch(&mut self, requests: &[BatchRequest]) -> Result<PreparedBatch> {
        let mut overlay = Overlay::new(self);
        let mut responses = Vec::with_capacity(requests.len());
        for request in requests {
            responses.push(overlay.execute(request)?);
        }
        overlay.finish(responses)
    }

    /// Publishes a prepared batch directly to the data file and syncs it.
    /// This is deliberately isolated so Phase 2 can put WAL persistence here.
    pub fn publish_prepared(&mut self, prepared: PreparedBatch) -> Result<Vec<BatchResponse>> {
        if prepared.base_generation != self.current_superblock.generation {
            return Err(Error::Conflict(
                "prepared batch was based on an older superblock generation".to_owned(),
            ));
        }
        if prepared.changed_pages.is_empty() {
            return Ok(prepared.responses);
        }
        let target_length = prepared
            .high_water_page_id
            .get()
            .checked_add(1)
            .ok_or_else(|| Error::invalid_input("database page id is exhausted"))?
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| Error::invalid_input("database file length overflows"))?;
        if self.file.len()? < target_length {
            self.file.set_len(target_length)?;
        }
        for (page_id, bytes) in &prepared.changed_pages {
            write_all_at(&mut self.file, page_id.get() * PAGE_SIZE as u64, bytes)?;
        }
        let superblock_bytes = encode_superblock(&prepared.new_superblock)?;
        let superblock_offset = match prepared.new_slot {
            SuperblockSlot::A => SUPERBLOCK_A_OFFSET,
            SuperblockSlot::B => SUPERBLOCK_B_OFFSET,
        };
        write_all_at(&mut self.file, superblock_offset, &superblock_bytes)?;
        self.file.sync_data()?;

        self.current_superblock = crate::decode_superblock(&superblock_bytes)?;
        self.active_slot = prepared.new_slot;
        self.root_page_id = prepared.root_page_id;
        self.free_list_head = prepared.free_list_head;
        self.high_water_page_id = prepared.high_water_page_id;
        self.next_revision = prepared.next_revision;
        self.cache.insert_many(prepared.read_pages);
        self.cache.insert_many(prepared.changed_decoded);
        Ok(prepared.responses)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_data()
    }

    pub fn check_invariants(&mut self) -> Result<InvariantReport> {
        let length = self.file.len()?;
        if length < (FIRST_DATA_PAGE * PAGE_SIZE as u64) || !length.is_multiple_of(PAGE_SIZE as u64)
        {
            return Err(Error::corruption(
                "database file length is not page aligned",
            ));
        }
        let slot_a = read_exact_at(&mut self.file, SUPERBLOCK_A_OFFSET, PAGE_SIZE)?;
        let slot_b = read_exact_at(&mut self.file, SUPERBLOCK_B_OFFSET, PAGE_SIZE)?;
        let selected = choose_superblock(&slot_a, &slot_b)?;
        if selected.slot != self.active_slot || selected.superblock != self.current_superblock {
            return Err(Error::corruption(
                "in-memory superblock is not the selected file copy",
            ));
        }
        let (root, free_head, high_water) = metadata_from_superblock(&selected.superblock, length)?;
        if root != self.root_page_id
            || free_head != self.free_list_head
            || high_water != self.high_water_page_id
        {
            return Err(Error::corruption(
                "in-memory allocator metadata does not match superblock",
            ));
        }
        checker::check(&mut self.file, root, free_head, high_water)
    }

    pub fn into_file(self) -> F {
        self.file
    }

    fn initialize(mut file: F, config: DatabaseConfig) -> Result<Self> {
        let root_page_id = PageId::new(FIRST_DATA_PAGE);
        let superblock = Superblock {
            root_page_id: Some(root_page_id),
            high_water_page_id: Some(root_page_id),
            ..Superblock::new(
                1,
                config.database_uuid,
                config.tenant_id,
                config.shard_id,
                config.shard_epoch,
            )
        };
        let root = PageData::Leaf {
            lsn: Lsn::ZERO,
            next_leaf: None,
            entries: Vec::new(),
        };
        let root_bytes = root.encode(root_page_id)?;
        let superblock_bytes = encode_superblock(&superblock)?;
        file.set_len(3 * PAGE_SIZE as u64)?;
        write_all_at(&mut file, SUPERBLOCK_A_OFFSET, &superblock_bytes)?;
        write_all_at(&mut file, SUPERBLOCK_B_OFFSET, &superblock_bytes)?;
        write_all_at(
            &mut file,
            root_page_id.get() * PAGE_SIZE as u64,
            &root_bytes,
        )?;
        file.sync_all()?;
        Ok(Self {
            file,
            current_superblock: superblock,
            active_slot: SuperblockSlot::A,
            root_page_id,
            free_list_head: None,
            high_water_page_id: root_page_id,
            next_revision: Revision::new(1),
            cache: PageCache::new(config.cache_capacity),
            config,
        })
    }

    fn read_page_from_file(&mut self, page_id: PageId) -> Result<PageData> {
        let bytes = read_exact_at(&mut self.file, page_id.get() * PAGE_SIZE as u64, PAGE_SIZE)?;
        let decoded = decode_page_at(&bytes, Some(page_id))?;
        PageData::decode(decoded)
    }
}

struct Overlay<'a, F: DurableFile> {
    store: &'a mut BTreeStore<F>,
    pages: BTreeMap<PageId, PageData>,
    dirty: BTreeSet<PageId>,
    read_pages: BTreeMap<PageId, PageData>,
    allocated: HashSet<PageId>,
    root_page_id: PageId,
    free_list_head: Option<PageId>,
    high_water_page_id: PageId,
    next_revision: Revision,
    last_lsn: Option<Lsn>,
}

impl<'a, F: DurableFile> Overlay<'a, F> {
    fn new(store: &'a mut BTreeStore<F>) -> Self {
        Self {
            root_page_id: store.root_page_id,
            free_list_head: store.free_list_head,
            high_water_page_id: store.high_water_page_id,
            next_revision: store.next_revision,
            store,
            pages: BTreeMap::new(),
            dirty: BTreeSet::new(),
            read_pages: BTreeMap::new(),
            allocated: HashSet::new(),
            last_lsn: None,
        }
    }

    fn execute(&mut self, request: &BatchRequest) -> Result<BatchResponse> {
        match request {
            BatchRequest::Get { key } => Ok(BatchResponse::Get(self.get_state(key)?)),
            BatchRequest::Put { key, value } => Ok(BatchResponse::Put(self.put(key, value)?)),
            BatchRequest::Delete { key } => Ok(BatchResponse::Delete(self.delete(key)?)),
            BatchRequest::Query {
                pk,
                exclusive_after_sk,
                limit,
            } => Ok(BatchResponse::Query(self.query(
                pk,
                exclusive_after_sk.as_ref(),
                *limit,
            )?)),
            BatchRequest::Scan {
                exclusive_after_key,
                limit,
            } => Ok(BatchResponse::Scan(
                self.scan(exclusive_after_key.as_ref(), *limit)?,
            )),
        }
    }

    fn finish(self, responses: Vec<BatchResponse>) -> Result<PreparedBatch> {
        let Overlay {
            store,
            pages,
            dirty,
            read_pages,
            root_page_id,
            free_list_head,
            high_water_page_id,
            next_revision,
            last_lsn,
            ..
        } = self;
        let new_superblock = if let Some(lsn) = last_lsn {
            let candidate = Superblock {
                generation: store
                    .current_superblock
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Error::invariant("superblock generation exhausted"))?,
                root_page_id: Some(root_page_id),
                free_list_head,
                high_water_page_id: Some(high_water_page_id),
                checkpoint_lsn: lsn,
                ..store.current_superblock.clone()
            };
            decode_superblock(&encode_superblock(&candidate)?)?
        } else {
            store.current_superblock.clone()
        };
        let new_slot = match store.active_slot {
            SuperblockSlot::A => SuperblockSlot::B,
            SuperblockSlot::B => SuperblockSlot::A,
        };
        let mut changed_pages = BTreeMap::new();
        let mut changed_decoded = BTreeMap::new();
        for page_id in dirty {
            let page = pages
                .get(&page_id)
                .ok_or_else(|| Error::invariant("dirty page missing from overlay"))?;
            changed_pages.insert(page_id, page.encode(page_id)?);
            changed_decoded.insert(page_id, page.clone());
        }
        Ok(PreparedBatch {
            responses,
            changed_pages,
            changed_decoded,
            read_pages,
            new_superblock,
            new_slot,
            base_generation: store.current_superblock.generation,
            next_revision,
            root_page_id,
            free_list_head,
            high_water_page_id,
        })
    }

    fn get_state(&mut self, key: &DocumentKey) -> Result<RevisionState> {
        let encoded = key.encode();
        validate_encoded_key(&encoded)?;
        let leaf_id = self.find_leaf(&encoded)?.0;
        let leaf = self.leaf(leaf_id)?;
        let Some(entry) = leaf.entries.iter().find(|entry| entry.key == encoded) else {
            return Ok(RevisionState::missing(Revision::ZERO));
        };
        match &entry.value {
            Some(value_ref) => Ok(RevisionState::present(
                self.read_value(value_ref)?,
                entry.revision,
            )),
            None => Ok(RevisionState::missing(entry.revision)),
        }
    }

    fn put(&mut self, key: &DocumentKey, value: &[u8]) -> Result<Revision> {
        self.validate_value(value)?;
        let encoded = key.encode();
        validate_encoded_key(&encoded)?;
        let revision = self.allocate_revision()?;
        let (leaf_id, route) = self.find_leaf(&encoded)?;
        let mut leaf = self.leaf(leaf_id)?;
        let index = leaf
            .entries
            .binary_search_by(|entry| entry.key.cmp(&encoded));
        let value_ref = self.make_value(value, revision)?;
        match index {
            Ok(index) => {
                let old_value = leaf.entries[index].value.clone();
                let LeafPage {
                    next_leaf,
                    mut entries,
                } = leaf;
                entries[index] = format::LeafEntry {
                    key: encoded,
                    revision,
                    value: Some(value_ref),
                };
                if !leaf_fits(&entries) {
                    return Err(Error::invalid_input(
                        "document key and value cannot fit in a leaf",
                    ));
                }
                self.replace_page(
                    leaf_id,
                    PageData::Leaf {
                        lsn: Lsn::new(revision.get()),
                        next_leaf,
                        entries,
                    },
                );
                self.free_value(old_value, revision)?;
            }
            Err(index) => {
                leaf.entries.insert(
                    index,
                    format::LeafEntry {
                        key: encoded,
                        revision,
                        value: Some(value_ref),
                    },
                );
                self.insert_leaf_entries(leaf_id, route, leaf, revision)?;
            }
        }
        Ok(revision)
    }

    fn delete(&mut self, key: &DocumentKey) -> Result<Revision> {
        let encoded = key.encode();
        validate_encoded_key(&encoded)?;
        let revision = self.allocate_revision()?;
        let (leaf_id, route) = self.find_leaf(&encoded)?;
        let mut leaf = self.leaf(leaf_id)?;
        let index = leaf
            .entries
            .binary_search_by(|entry| entry.key.cmp(&encoded));
        match index {
            Ok(index) => {
                let old_value = leaf.entries[index].value.clone();
                let LeafPage {
                    next_leaf,
                    mut entries,
                } = leaf;
                entries[index] = format::LeafEntry {
                    key: encoded,
                    revision,
                    value: None,
                };
                self.replace_page(
                    leaf_id,
                    PageData::Leaf {
                        lsn: Lsn::new(revision.get()),
                        next_leaf,
                        entries,
                    },
                );
                self.free_value(old_value, revision)?;
            }
            Err(index) => {
                leaf.entries.insert(
                    index,
                    format::LeafEntry {
                        key: encoded,
                        revision,
                        value: None,
                    },
                );
                self.insert_leaf_entries(leaf_id, route, leaf, revision)?;
            }
        }
        Ok(revision)
    }

    fn query(
        &mut self,
        pk: &PrimaryKey,
        exclusive_after_sk: Option<&SortKey>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start_key = DocumentKey::new(pk.as_bytes().to_vec(), Vec::new()).encode();
        validate_encoded_key(&start_key)?;
        let (mut leaf_id, _) = self.find_leaf(&start_key)?;
        let cursor = exclusive_after_sk
            .map(|sk| DocumentKey::new(pk.as_bytes().to_vec(), sk.as_bytes().to_vec()).encode());
        if let Some(cursor) = &cursor {
            validate_encoded_key(cursor)?;
        }
        let mut first = true;
        let mut visited = HashSet::new();
        let mut rows = Vec::new();
        while rows.len() < limit {
            if !visited.insert(leaf_id) {
                return Err(Error::corruption(
                    "leaf chain contains a cycle during query",
                ));
            }
            let leaf = self.leaf(leaf_id)?;
            for entry in &leaf.entries {
                if first && cursor.as_ref().is_some_and(|cursor| entry.key <= *cursor) {
                    continue;
                }
                first = false;
                let document_key = DocumentKey::decode(&entry.key).map_err(|error| {
                    Error::corruption(format!("leaf key decode failed: {error}"))
                })?;
                if document_key.pk != *pk {
                    if document_key.pk > *pk {
                        return Ok(rows);
                    }
                    continue;
                }
                if let Some(cursor) = exclusive_after_sk
                    && document_key.sk <= *cursor
                {
                    continue;
                }
                if let Some(value) = &entry.value {
                    rows.push(Document {
                        key: document_key,
                        value: self.read_value(value)?,
                        revision: entry.revision,
                    });
                    if rows.len() == limit {
                        return Ok(rows);
                    }
                }
            }
            let next = leaf.next_leaf;
            let Some(next) = next else {
                break;
            };
            leaf_id = next;
            first = false;
        }
        Ok(rows)
    }

    fn scan(&mut self, cursor: Option<&DocumentKey>, limit: usize) -> Result<Vec<Document>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (mut leaf_id, _) = match cursor {
            Some(key) => {
                let encoded = key.encode();
                validate_encoded_key(&encoded)?;
                self.find_leaf(&encoded)?
            }
            None => (self.leftmost_leaf()?, Vec::new()),
        };
        let cursor = cursor.map(DocumentKey::encode);
        let mut rows = Vec::new();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(leaf_id) {
                return Err(Error::corruption("leaf chain contains a cycle during scan"));
            }
            let leaf = self.leaf(leaf_id)?;
            for entry in &leaf.entries {
                if cursor.as_ref().is_some_and(|cursor| entry.key <= *cursor) {
                    continue;
                }
                let document_key = DocumentKey::decode(&entry.key).map_err(|error| {
                    Error::corruption(format!("leaf key decode failed: {error}"))
                })?;
                if let Some(value) = &entry.value {
                    rows.push(Document {
                        key: document_key,
                        value: self.read_value(value)?,
                        revision: entry.revision,
                    });
                    if rows.len() == limit {
                        return Ok(rows);
                    }
                }
            }
            let Some(next) = leaf.next_leaf else {
                break;
            };
            leaf_id = next;
        }
        Ok(rows)
    }

    fn insert_leaf_entries(
        &mut self,
        leaf_id: PageId,
        route: Vec<PageId>,
        leaf: LeafPage,
        lsn: Revision,
    ) -> Result<()> {
        let LeafPage { next_leaf, entries } = leaf;
        if leaf_fits(&entries) {
            self.replace_page(
                leaf_id,
                PageData::Leaf {
                    lsn: Lsn::new(lsn.get()),
                    next_leaf,
                    entries,
                },
            );
            return Ok(());
        }
        let split = choose_leaf_split(&entries)?;
        let right_id = self.allocate_page()?;
        let left_entries = entries[..split].to_vec();
        let right_entries = entries[split..].to_vec();
        let separator = right_entries
            .first()
            .map(|entry| entry.key.clone())
            .ok_or_else(|| Error::invariant("leaf split produced an empty right page"))?;
        self.replace_page(
            leaf_id,
            PageData::Leaf {
                lsn: Lsn::new(lsn.get()),
                next_leaf: Some(right_id),
                entries: left_entries,
            },
        );
        self.replace_page(
            right_id,
            PageData::Leaf {
                lsn: Lsn::new(lsn.get()),
                next_leaf,
                entries: right_entries,
            },
        );
        self.insert_parent_separator(route, separator, right_id, lsn)
    }

    fn insert_parent_separator(
        &mut self,
        mut route: Vec<PageId>,
        mut separator: Vec<u8>,
        mut right_child: PageId,
        lsn: Revision,
    ) -> Result<()> {
        route.pop();
        while let Some(parent_id) = route.pop() {
            let parent = self.internal(parent_id)?;
            let mut entries = parent.entries;
            let index = entries.partition_point(|entry| entry.key < separator);
            entries.insert(
                index,
                format::InternalEntry {
                    key: separator,
                    right_child,
                },
            );
            if internal_fits(parent.leftmost_child, &entries) {
                self.replace_page(
                    parent_id,
                    PageData::Internal {
                        lsn: Lsn::new(lsn.get()),
                        leftmost_child: parent.leftmost_child,
                        entries,
                    },
                );
                return Ok(());
            }
            let split = choose_internal_split(parent.leftmost_child, &entries)?;
            let promoted = entries[split].key.clone();
            let right_leftmost = entries[split].right_child;
            let right_id = self.allocate_page()?;
            let left_entries = entries[..split].to_vec();
            let right_entries = entries[split + 1..].to_vec();
            if !internal_fits(parent.leftmost_child, &left_entries)
                || !internal_fits(right_leftmost, &right_entries)
            {
                return Err(Error::invalid_input(
                    "internal split produced an oversized page",
                ));
            }
            self.replace_page(
                parent_id,
                PageData::Internal {
                    lsn: Lsn::new(lsn.get()),
                    leftmost_child: parent.leftmost_child,
                    entries: left_entries,
                },
            );
            self.replace_page(
                right_id,
                PageData::Internal {
                    lsn: Lsn::new(lsn.get()),
                    leftmost_child: right_leftmost,
                    entries: right_entries,
                },
            );
            separator = promoted;
            right_child = right_id;
        }

        let new_root_id = self.allocate_page()?;
        self.replace_page(
            new_root_id,
            PageData::Internal {
                lsn: Lsn::new(lsn.get()),
                leftmost_child: self.root_page_id,
                entries: vec![format::InternalEntry {
                    key: separator,
                    right_child,
                }],
            },
        );
        self.root_page_id = new_root_id;
        Ok(())
    }

    fn find_leaf(&mut self, key: &[u8]) -> Result<(PageId, Vec<PageId>)> {
        let mut page_id = self.root_page_id;
        let mut route = Vec::new();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(page_id) {
                return Err(Error::corruption("tree traversal contains a cycle"));
            }
            route.push(page_id);
            match self.page(page_id)? {
                PageData::Leaf { .. } => return Ok((page_id, route)),
                PageData::Internal {
                    leftmost_child,
                    entries,
                    ..
                } => {
                    let index = entries.partition_point(|entry| entry.key.as_slice() <= key);
                    page_id = if index == 0 {
                        leftmost_child
                    } else {
                        entries[index - 1].right_child
                    };
                }
                _ => return Err(Error::corruption("tree child has an impossible page type")),
            }
        }
    }

    fn leftmost_leaf(&mut self) -> Result<PageId> {
        let mut page_id = self.root_page_id;
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(page_id) {
                return Err(Error::corruption("leftmost traversal contains a cycle"));
            }
            match self.page(page_id)? {
                PageData::Leaf { .. } => return Ok(page_id),
                PageData::Internal { leftmost_child, .. } => page_id = leftmost_child,
                _ => return Err(Error::corruption("tree child has an impossible page type")),
            }
        }
    }

    fn leaf(&mut self, page_id: PageId) -> Result<LeafPage> {
        match self.page(page_id)? {
            PageData::Leaf {
                next_leaf, entries, ..
            } => Ok(LeafPage { next_leaf, entries }),
            _ => Err(Error::corruption("expected a leaf page")),
        }
    }

    fn internal(&mut self, page_id: PageId) -> Result<InternalPage> {
        match self.page(page_id)? {
            PageData::Internal {
                leftmost_child,
                entries,
                ..
            } => Ok(InternalPage {
                leftmost_child,
                entries,
            }),
            _ => Err(Error::corruption("expected an internal page")),
        }
    }

    fn page(&mut self, page_id: PageId) -> Result<PageData> {
        self.check_page_id(page_id)?;
        if let Some(page) = self.pages.get(&page_id) {
            return Ok(page.clone());
        }
        if let Some(page) = self.store.cache.get(page_id) {
            self.pages.insert(page_id, page.clone());
            return Ok(page);
        }
        let page = self.store.read_page_from_file(page_id)?;
        self.read_pages.insert(page_id, page.clone());
        self.pages.insert(page_id, page.clone());
        Ok(page)
    }

    fn replace_page(&mut self, page_id: PageId, page: PageData) {
        self.pages.insert(page_id, page);
        self.dirty.insert(page_id);
    }

    fn check_page_id(&self, page_id: PageId) -> Result<()> {
        if page_id.get() < FIRST_DATA_PAGE || page_id > self.high_water_page_id {
            return Err(Error::corruption(format!(
                "page id {} is outside the allocated range",
                page_id.get()
            )));
        }
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        if let Some(page_id) = self.free_list_head {
            let page = self.page(page_id)?;
            let PageData::Free { next, .. } = page else {
                return Err(Error::corruption("free-list head is not a free page"));
            };
            self.free_list_head = next;
            self.allocated.insert(page_id);
            return Ok(page_id);
        }
        let page_id = match self.high_water_page_id.get().checked_add(1) {
            Some(value) => PageId::new(value),
            None => return Err(Error::invariant("page id exhausted")),
        };
        self.high_water_page_id = page_id;
        self.allocated.insert(page_id);
        Ok(page_id)
    }

    fn allocate_revision(&mut self) -> Result<Revision> {
        let revision = self.next_revision;
        self.next_revision = Revision::new(
            revision
                .get()
                .checked_add(1)
                .ok_or_else(|| Error::invariant("storage revision exhausted"))?,
        );
        self.last_lsn = Some(Lsn::new(revision.get()));
        Ok(revision)
    }

    fn validate_value(&self, value: &[u8]) -> Result<()> {
        if value.len() > self.store.config.limits.max_value_size || value.len() > MAX_VALUE_SIZE {
            return Err(Error::invalid_input(format!(
                "value is {} bytes, maximum is {}",
                value.len(),
                self.store.config.limits.max_value_size.min(MAX_VALUE_SIZE)
            )));
        }
        Ok(())
    }

    fn make_value(&mut self, value: &[u8], lsn: Revision) -> Result<ValueRef> {
        if value.len() <= self.store.config.limits.inline_value_limit
            && value.len() <= INLINE_VALUE_LIMIT
        {
            return Ok(ValueRef::Inline(value.to_vec()));
        }
        let ids = value.len().div_ceil(format::OVERFLOW_DATA_SIZE);
        if ids > MAX_OVERFLOW_PAGES {
            return Err(Error::invalid_input(
                "value requires too many overflow pages",
            ));
        }
        let mut page_ids = Vec::with_capacity(ids);
        for _ in 0..ids {
            page_ids.push(self.allocate_page()?);
        }
        for (index, chunk) in value.chunks(format::OVERFLOW_DATA_SIZE).enumerate() {
            self.replace_page(
                page_ids[index],
                PageData::Overflow {
                    lsn: Lsn::new(lsn.get()),
                    next: page_ids.get(index + 1).copied(),
                    total_length: value.len() as u64,
                    chunk: chunk.to_vec(),
                },
            );
        }
        Ok(ValueRef::Overflow {
            head: page_ids[0],
            length: value.len() as u64,
        })
    }

    fn free_value(&mut self, value: Option<ValueRef>, lsn: Revision) -> Result<()> {
        let Some(ValueRef::Overflow { head, length }) = value else {
            return Ok(());
        };
        let mut page_id = Some(head);
        let mut visited = HashSet::new();
        let mut total_length = 0u64;
        let mut count = 0usize;
        while let Some(current) = page_id {
            if !visited.insert(current) || count >= MAX_OVERFLOW_PAGES {
                return Err(Error::corruption("overflow chain is cyclic or too long"));
            }
            let page = self.page(current)?;
            let PageData::Overflow {
                next,
                total_length: page_total,
                chunk,
                ..
            } = page
            else {
                return Err(Error::corruption(
                    "value reference points to a non-overflow page",
                ));
            };
            if page_total != length {
                return Err(Error::corruption(
                    "overflow length metadata disagrees with leaf",
                ));
            }
            total_length = total_length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| Error::corruption("overflow length overflows"))?;
            page_id = next;
            count += 1;
            self.free_page(current, lsn)?;
        }
        if total_length != length || length == 0 {
            return Err(Error::corruption(
                "overflow chain content length is invalid",
            ));
        }
        Ok(())
    }

    fn read_value(&mut self, value: &ValueRef) -> Result<Vec<u8>> {
        match value {
            ValueRef::Inline(value) => Ok(value.clone()),
            ValueRef::Overflow { head, length } => {
                let mut output = Vec::new();
                let mut page_id = Some(*head);
                let mut visited = HashSet::new();
                let mut count = 0usize;
                while let Some(current) = page_id {
                    if !visited.insert(current) || count >= MAX_OVERFLOW_PAGES {
                        return Err(Error::corruption("overflow chain is cyclic or too long"));
                    }
                    let page = self.page(current)?;
                    let PageData::Overflow {
                        next,
                        total_length,
                        chunk,
                        ..
                    } = page
                    else {
                        return Err(Error::corruption(
                            "value reference points to a non-overflow page",
                        ));
                    };
                    if total_length != *length {
                        return Err(Error::corruption(
                            "overflow length metadata disagrees with leaf",
                        ));
                    }
                    output.extend_from_slice(&chunk);
                    page_id = next;
                    count += 1;
                }
                if output.len() as u64 != *length {
                    return Err(Error::corruption("overflow content length is invalid"));
                }
                Ok(output)
            }
        }
    }

    fn free_page(&mut self, page_id: PageId, lsn: Revision) -> Result<()> {
        self.allocated.remove(&page_id);
        self.replace_page(
            page_id,
            PageData::Free {
                lsn: Lsn::new(lsn.get()),
                next: self.free_list_head,
            },
        );
        self.free_list_head = Some(page_id);
        Ok(())
    }
}

struct InternalPage {
    leftmost_child: PageId,
    entries: Vec<format::InternalEntry>,
}

struct LeafPage {
    next_leaf: Option<PageId>,
    entries: Vec<format::LeafEntry>,
}

fn choose_leaf_split(entries: &[format::LeafEntry]) -> Result<usize> {
    for candidate in split_candidates(entries.len()) {
        if leaf_fits(&entries[..candidate]) && leaf_fits(&entries[candidate..]) {
            return Ok(candidate);
        }
    }
    Err(Error::invalid_input(
        "a single leaf entry exceeds page capacity",
    ))
}

fn choose_internal_split(leftmost: PageId, entries: &[format::InternalEntry]) -> Result<usize> {
    if entries.len() < 3 {
        return Err(Error::invalid_input("internal page cannot be split safely"));
    }
    for candidate in split_candidates(entries.len()) {
        let right_leftmost = entries[candidate].right_child;
        if internal_fits(leftmost, &entries[..candidate])
            && internal_fits(right_leftmost, &entries[candidate + 1..])
        {
            return Ok(candidate);
        }
    }
    Err(Error::invalid_input(
        "internal separators exceed page capacity",
    ))
}

fn split_candidates(length: usize) -> impl Iterator<Item = usize> {
    let middle = length / 2;
    let mut candidates: Vec<_> = (1..length).collect();
    candidates.sort_by_key(move |candidate| candidate.abs_diff(middle));
    candidates.into_iter()
}

fn metadata_from_superblock(
    superblock: &Superblock,
    file_length: u64,
) -> Result<(PageId, Option<PageId>, PageId)> {
    if superblock.page_size as usize != PAGE_SIZE {
        return Err(Error::unsupported_format(
            "superblock page size is unsupported",
        ));
    }
    let page_count = file_length / PAGE_SIZE as u64;
    let root = superblock
        .root_page_id
        .ok_or_else(|| Error::corruption("selected superblock has no root page"))?;
    let high_water = superblock
        .high_water_page_id
        .ok_or_else(|| Error::corruption("selected superblock has no high-water page"))?;
    let expected_page_count = high_water
        .get()
        .checked_add(1)
        .ok_or_else(|| Error::corruption("superblock high-water page overflows"))?;
    if high_water.get() < FIRST_DATA_PAGE || expected_page_count != page_count {
        return Err(Error::corruption(
            "superblock high-water page is inconsistent with file length",
        ));
    }
    if root.get() < FIRST_DATA_PAGE || root > high_water {
        return Err(Error::corruption(
            "superblock root page is outside the file",
        ));
    }
    if let Some(free) = superblock.free_list_head
        && (free.get() < FIRST_DATA_PAGE || free > high_water)
    {
        return Err(Error::corruption(
            "superblock free-list head is outside the file",
        ));
    }
    Ok((root, superblock.free_list_head, high_water))
}

fn read_exact_at<F: DurableFile>(file: &mut F, offset: u64, length: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0; length];
    let mut position = 0;
    while position < length {
        let count = file.read_at(offset + position as u64, &mut bytes[position..])?;
        if count == 0 {
            return Err(Error::corruption("unexpected end of database file"));
        }
        position += count;
    }
    Ok(bytes)
}

fn write_all_at<F: DurableFile>(file: &mut F, offset: u64, bytes: &[u8]) -> Result<()> {
    let mut position = 0;
    while position < bytes.len() {
        let count = file.write_at(offset + position as u64, &bytes[position..])?;
        if count == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "database file returned a zero-byte write",
            )));
        }
        position += count;
    }
    Ok(())
}

fn validate_encoded_key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_ENCODED_KEY_SIZE {
        return Err(Error::invalid_input(format!(
            "encoded document key is {} bytes, maximum is {MAX_ENCODED_KEY_SIZE}",
            key.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
