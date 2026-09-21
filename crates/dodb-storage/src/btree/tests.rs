use dodb_core::{DocumentKey, Error, PrimaryKey, Revision, RevisionState};

use super::*;

#[derive(Clone, Debug, Default)]
struct MemoryFile {
    bytes: Vec<u8>,
}

impl DurableFile for MemoryFile {
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        let start = usize::try_from(offset).map_err(|_| Error::invalid_input("offset overflow"))?;
        if start >= self.bytes.len() {
            return Ok(0);
        }
        let count = buffer.len().min(self.bytes.len() - start);
        buffer[..count].copy_from_slice(&self.bytes[start..start + count]);
        Ok(count)
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        let start = usize::try_from(offset).map_err(|_| Error::invalid_input("offset overflow"))?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| Error::invalid_input("write range overflow"))?;
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[start..end].copy_from_slice(bytes);
        Ok(bytes.len())
    }

    fn len(&self) -> Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn set_len(&mut self, length: u64) -> Result<()> {
        let length =
            usize::try_from(length).map_err(|_| Error::invalid_input("length overflow"))?;
        self.bytes.resize(length, 0);
        Ok(())
    }

    fn sync_data(&mut self) -> Result<()> {
        Ok(())
    }

    fn sync_all(&mut self) -> Result<()> {
        Ok(())
    }
}

fn key(pk: impl Into<Vec<u8>>, sk: impl Into<Vec<u8>>) -> DocumentKey {
    DocumentKey::new(pk, sk)
}

fn config(cache_capacity: usize) -> DatabaseConfig {
    DatabaseConfig::default().with_cache_capacity(cache_capacity)
}

#[test]
fn basic_operations_preserve_missing_revisions_and_order() {
    let mut store = BTreeStore::open(MemoryFile::default(), config(16)).unwrap();
    let first = key(vec![0, 1], vec![0]);
    let second = key(vec![0, 1], vec![1]);
    let other = key(vec![0, 2], vec![0]);

    assert_eq!(
        store.get(&first).unwrap(),
        RevisionState::missing(Revision::ZERO)
    );
    assert_eq!(store.delete(first.clone()).unwrap(), Revision::new(1));
    assert_eq!(
        store.get(&first).unwrap(),
        RevisionState::missing(Revision::new(1))
    );
    assert_eq!(
        store.put(first.clone(), b"first").unwrap(),
        Revision::new(2)
    );
    store.put(second.clone(), b"second").unwrap();
    store.put(other.clone(), b"other").unwrap();
    store.delete(second.clone()).unwrap();

    let rows = store.query(&PrimaryKey::new(vec![0, 1]), None, 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, first);
    assert_eq!(rows[0].value, b"first");
    let scan = store.scan(None, 10).unwrap();
    assert_eq!(
        scan.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
        vec![key(vec![0, 1], vec![0]), other]
    );
    store.check_invariants().unwrap();
}

#[test]
fn splits_root_and_internal_pages_and_reopens() {
    let mut store = BTreeStore::open(MemoryFile::default(), config(0)).unwrap();
    for value in 0u16..3000 {
        let key = key(
            (value % 17).to_le_bytes().to_vec(),
            value.to_le_bytes().to_vec(),
        );
        store.put(key, value.to_le_bytes()).unwrap();
    }
    let report = store.check_invariants().unwrap();
    assert!(report.reachable_pages > 50);
    let file = store.into_file();
    let mut reopened = BTreeStore::open(file, config(1)).unwrap();
    assert_eq!(reopened.scan(None, usize::MAX).unwrap().len(), 3000);
    assert_eq!(
        reopened.check_invariants().unwrap().leaked_pages,
        Vec::new()
    );
}

#[test]
fn large_value_replacement_reclaims_overflow_pages() {
    let mut store = BTreeStore::open(MemoryFile::default(), config(4)).unwrap();
    let document_key = key(vec![7], vec![9]);
    let large = vec![0xa5; 10_000];
    store.put(document_key.clone(), large.clone()).unwrap();
    assert_eq!(
        store.get(&document_key).unwrap().value(),
        Some(large.as_slice())
    );
    store.put(document_key.clone(), b"small").unwrap();
    let report = store.check_invariants().unwrap();
    assert!(report.free_pages >= 3);
    assert_eq!(
        store.get(&document_key).unwrap().value(),
        Some(&b"small"[..])
    );
    store.delete(document_key.clone()).unwrap();
    assert!(store.check_invariants().unwrap().free_pages >= 3);
    store.put(document_key, vec![0x11; 9000]).unwrap();
    assert!(store.check_invariants().unwrap().free_pages < 3);
}

#[test]
fn deleting_all_entries_keeps_empty_leaves_searchable() {
    let mut store = BTreeStore::open(MemoryFile::default(), config(1)).unwrap();
    let keys: Vec<_> = (0..240)
        .map(|index| key(vec![0], (index as u16).to_le_bytes().to_vec()))
        .collect();
    for document_key in &keys {
        store.put(document_key.clone(), b"value").unwrap();
    }
    for document_key in keys.iter().rev() {
        store.delete(document_key.clone()).unwrap();
    }
    assert!(store.scan(None, 10).unwrap().is_empty());
    assert!(
        store
            .query(&PrimaryKey::new(vec![0]), None, 10)
            .unwrap()
            .is_empty()
    );
    let report = store.check_invariants().unwrap();
    assert!(report.reachable_pages > 1);
}

#[test]
fn prepared_batch_is_private_until_published() {
    let file = MemoryFile::default();
    let mut store = BTreeStore::open(file, config(8)).unwrap();
    let document_key = key(vec![1], vec![2]);
    let requests = [
        BatchRequest::Put {
            key: document_key.clone(),
            value: b"value".to_vec(),
        },
        BatchRequest::Get {
            key: document_key.clone(),
        },
    ];
    let prepared = store.prepare_batch(&requests).unwrap();
    assert!(!prepared.changed_pages().is_empty());
    assert_eq!(
        store.get(&document_key).unwrap(),
        RevisionState::missing(Revision::ZERO)
    );
    store.publish_prepared(prepared).unwrap();
    assert_eq!(
        store.get(&document_key).unwrap().value(),
        Some(&b"value"[..])
    );
}

#[test]
fn failed_batch_does_not_publish_earlier_requests() {
    let mut store = BTreeStore::open(MemoryFile::default(), config(0)).unwrap();
    let first_key = key(vec![1], vec![1]);
    let second_key = key(vec![1], vec![2]);
    let result = store.apply_batch(&[
        BatchRequest::Put {
            key: first_key.clone(),
            value: b"first".to_vec(),
        },
        BatchRequest::Put {
            key: second_key,
            value: vec![0; MAX_VALUE_SIZE + 1],
        },
    ]);
    assert!(matches!(result, Err(Error::InvalidInput(_))));
    assert_eq!(
        store.get(&first_key).unwrap(),
        RevisionState::missing(Revision::ZERO)
    );
}

#[test]
fn malformed_page_is_corruption_not_a_panic() {
    let mut store = BTreeStore::open(MemoryFile::default(), config(0)).unwrap();
    store.put(key(vec![1], vec![1]), b"value").unwrap();
    let mut file = store.into_file();
    file.bytes[2 * PAGE_SIZE + 80] ^= 1;
    let result = BTreeStore::open(file, config(0));
    assert!(matches!(result, Err(Error::Corruption(_))));
}

#[test]
fn all_cache_sizes_use_the_same_results() {
    for capacity in [0, 1, 8, 64] {
        let mut store = BTreeStore::open(MemoryFile::default(), config(capacity)).unwrap();
        for index in 0..120 {
            store
                .put(key(vec![index % 4], vec![0, index]), vec![index; 10])
                .unwrap();
        }
        for index in 0..120 {
            let state = store.get(&key(vec![index % 4], vec![0, index])).unwrap();
            assert_eq!(state.value(), Some(vec![index; 10].as_slice()));
        }
        assert!(store.cache_len() <= capacity);
        store.check_invariants().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn async_shard_serializes_concurrent_requests_in_queue_order() {
    let store = BTreeStore::open(MemoryFile::default(), config(2)).unwrap();
    let shard = AsyncShard::start(store, 8);
    let first = shard.execute(BatchRequest::Put {
        key: key(vec![1], vec![1]),
        value: b"first".to_vec(),
    });
    let second = shard.execute(BatchRequest::Get {
        key: key(vec![1], vec![1]),
    });
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap(), BatchResponse::Put(Revision::new(1)));
    assert_eq!(
        second.unwrap(),
        BatchResponse::Get(RevisionState::present(b"first", Revision::new(1)))
    );
    shard.close().await.unwrap();
}

#[test]
fn wal_backed_store_reopens_from_committed_page_images() {
    let mut store =
        BTreeStore::open_with_wal(MemoryFile::default(), MemoryFile::default(), config(0)).unwrap();
    let document_key = key(vec![9], vec![9]);
    let revision = store.put(document_key.clone(), b"wal".to_vec()).unwrap();
    assert!(revision.get() > 1);
    let (data, wal) = store.into_files().unwrap();
    let mut reopened = BTreeStore::open_with_wal(data, wal, config(0)).unwrap();
    assert_eq!(
        reopened.get(&document_key).unwrap(),
        RevisionState::present(b"wal", revision)
    );
}
