use dodb_core::{DocumentKey, PrimaryKey, RevisionState, SortKey};
use dodb_storage::{BTreeStore, DatabaseConfig};

use dodb_testkit::{CrashableFile, FaultAction, FaultPlan, FileOperation, ReferenceDb};

fn key_from_rng(mut value: u64) -> DocumentKey {
    let pk_len = (value as usize) % 9;
    value = value.rotate_left(17);
    let sk_len = (value as usize) % 17;
    let mut pk = Vec::with_capacity(pk_len);
    let mut sk = Vec::with_capacity(sk_len);
    for index in 0..pk_len {
        value = value.wrapping_mul(6364136223846793005).wrapping_add(1);
        pk.push((value >> (index % 8)) as u8);
    }
    for index in 0..sk_len {
        value = value
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        sk.push((value >> (index % 8)) as u8);
    }
    DocumentKey::new(pk, sk)
}

fn value_from_rng(mut value: u64) -> Vec<u8> {
    let length = match value % 11 {
        0 => 0,
        1 => 1,
        2 => 512,
        3 => 513,
        4 => 4096,
        5 => 9000,
        _ => (value as usize) % 120,
    };
    let mut output = Vec::with_capacity(length);
    for _ in 0..length {
        value = value.wrapping_mul(6364136223846793005).wrapping_add(1);
        output.push((value >> 32) as u8);
    }
    output
}

fn assert_same_scan(store: &mut BTreeStore<CrashableFile>, reference: &ReferenceDb) {
    let actual = store.scan(None, usize::MAX).unwrap();
    let expected = reference.scan(None, usize::MAX);
    assert_documents_equal(&actual, &expected);
}

fn assert_documents_equal(actual: &[dodb_storage::Document], expected: &[dodb_testkit::Document]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.value, expected.value);
        assert_eq!(actual.revision, expected.revision);
    }
}

#[test]
fn deterministic_differential_sequences_match_reference() {
    // These seeds are part of the reproducible Phase 1 test corpus.
    for seed in [0x5eed_cafe_u64, 0x0123_4567_89ab_cdef, 0xd0db_2026_0001] {
        let cache_capacity = if seed & 1 == 0 { 0 } else { 3 };
        let mut store = BTreeStore::open(
            CrashableFile::new(),
            DatabaseConfig::default().with_cache_capacity(cache_capacity),
        )
        .unwrap();
        let mut reference = ReferenceDb::new();
        let mut rng = seed;

        for operation_index in 0..5000u32 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let document_key = key_from_rng(rng);
            match rng % 7 {
                0 | 1 => {
                    let value = value_from_rng(rng.rotate_left(11));
                    assert_eq!(
                        store
                            .put(document_key.clone(), value.clone())
                            .unwrap()
                            .get(),
                        reference.put(document_key, value).unwrap().get()
                    );
                }
                2 => {
                    assert_eq!(
                        store.delete(document_key.clone()).unwrap().get(),
                        reference.delete(document_key).unwrap().get()
                    );
                }
                3 => {
                    assert_eq!(
                        store.get(&document_key).unwrap(),
                        reference.get(&document_key)
                    );
                }
                4 => {
                    let cursor = if rng & 1 == 0 {
                        None
                    } else {
                        Some(SortKey::new(document_key.sk.as_bytes().to_vec()))
                    };
                    let actual = store
                        .query(
                            &PrimaryKey::new(document_key.pk.as_bytes().to_vec()),
                            cursor.as_ref(),
                            8,
                        )
                        .unwrap();
                    let expected = reference.query(
                        &PrimaryKey::new(document_key.pk.as_bytes().to_vec()),
                        cursor.as_ref(),
                        8,
                    );
                    assert_documents_equal(&actual, &expected);
                }
                5 => {
                    let cursor = if rng & 1 == 0 {
                        None
                    } else {
                        Some(document_key.clone())
                    };
                    let actual = store.scan(cursor.as_ref(), 8).unwrap();
                    let expected = reference.scan(cursor.as_ref(), 8);
                    assert_documents_equal(&actual, &expected);
                }
                _ => {
                    let batch_key = key_from_rng(rng.rotate_right(7));
                    let value = value_from_rng(rng.rotate_right(13));
                    let responses = store
                        .apply_batch(&[
                            dodb_storage::BatchRequest::Get {
                                key: document_key.clone(),
                            },
                            dodb_storage::BatchRequest::Put {
                                key: batch_key.clone(),
                                value: value.clone(),
                            },
                        ])
                        .unwrap();
                    assert_eq!(
                        responses[0],
                        dodb_storage::BatchResponse::Get(reference.get(&document_key))
                    );
                    reference.put(batch_key, value).unwrap();
                }
            }

            if operation_index % 101 == 0 {
                store.check_invariants().unwrap();
                assert_same_scan(&mut store, &reference);
            }
            if operation_index % 733 == 0 && operation_index != 0 {
                let file = store.into_file();
                store = BTreeStore::open(
                    file,
                    DatabaseConfig::default().with_cache_capacity(cache_capacity),
                )
                .unwrap();
                assert_same_scan(&mut store, &reference);
            }
        }
        assert_same_scan(&mut store, &reference);
    }
}

#[test]
fn reference_missing_state_is_retained_after_reopen() {
    let key = DocumentKey::new(vec![0, 0], Vec::new());
    let mut store = BTreeStore::open(CrashableFile::new(), DatabaseConfig::default()).unwrap();
    let mut reference = ReferenceDb::new();
    store.delete(key.clone()).unwrap();
    reference.delete(key.clone()).unwrap();
    let file = store.into_file();
    let mut reopened = BTreeStore::open(file, DatabaseConfig::default()).unwrap();
    assert_eq!(reopened.get(&key).unwrap(), reference.get(&key));
    assert_eq!(
        reopened.get(&key).unwrap(),
        RevisionState::missing(dodb_core::Revision::new(1))
    );
}

#[test]
fn short_file_io_is_retried_by_the_storage_boundary() {
    let write_faults =
        FaultPlan::default().at(0, FileOperation::WriteAt, FaultAction::short_write(1));
    let mut store = BTreeStore::open(
        CrashableFile::new().with_fault_plan(write_faults),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    let key = DocumentKey::new(vec![8], vec![9]);
    store.put(key.clone(), b"short I/O".to_vec()).unwrap();

    let mut file = store.into_file();
    file.set_fault_plan(FaultPlan::default().at(
        0,
        FileOperation::ReadAt,
        FaultAction::short_read(1),
    ));
    let mut reopened =
        BTreeStore::open(file, DatabaseConfig::default().with_cache_capacity(0)).unwrap();
    assert_eq!(reopened.get(&key).unwrap().value(), Some(&b"short I/O"[..]));
}

#[test]
fn wal_commit_survives_a_simulated_process_crash_before_data_flush() {
    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new(),
        CrashableFile::new(),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    let document_key = DocumentKey::new(vec![4], vec![2]);
    let revision = store
        .put(document_key.clone(), b"durable WAL value".to_vec())
        .unwrap();
    let (mut data, mut wal) = store.into_files().unwrap();
    data.crash();
    wal.crash();
    let mut reopened =
        BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
            .unwrap();
    assert_eq!(
        reopened.get(&document_key).unwrap(),
        RevisionState::present(b"durable WAL value", revision)
    );
    reopened.check_invariants().unwrap();
}

#[test]
fn partial_data_page_flush_is_repaired_from_wal_after_crash() {
    let data_faults = FaultPlan::default()
        // Initialization consumes set_len, three writes, and sync_all. The
        // first data page flush write is operation five; fail its retry after a short
        // first write to leave a torn volatile page.
        .at(5, FileOperation::WriteAt, FaultAction::short_write(64))
        .at(
            6,
            FileOperation::WriteAt,
            FaultAction::io(std::io::ErrorKind::Other, "partial data page write"),
        );
    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new().with_fault_plan(data_faults),
        CrashableFile::new(),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    let key = DocumentKey::new(vec![4], vec![3]);
    let revision = store
        .put(key.clone(), b"partial data write".to_vec())
        .unwrap();
    assert!(store.flush().is_err());
    let (mut data, mut wal) = store.into_files().unwrap();
    data.crash();
    wal.crash();
    let mut reopened =
        BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
            .unwrap();
    assert_eq!(
        reopened.get(&key).unwrap(),
        RevisionState::present(b"partial data write", revision)
    );
    reopened.check_invariants().unwrap();
}

#[test]
fn wal_commit_without_a_commit_record_is_ignored_after_crash() {
    let mut wal_faults = FaultPlan::default();
    // WAL initialization uses three writes and one sync. The next seven
    // writes are the page-image frames; fail at the first COMMIT header.
    wal_faults.insert(
        10,
        FileOperation::WriteAt,
        FaultAction::io(std::io::ErrorKind::Other, "crash before commit record"),
    );
    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new(),
        CrashableFile::new().with_fault_plan(wal_faults),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    let key = DocumentKey::new(vec![5], vec![5]);
    assert!(store.put(key.clone(), b"not committed".to_vec()).is_err());
    let (mut data, mut wal) = store.into_files().unwrap();
    data.crash();
    wal.crash();
    let mut reopened =
        BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
            .unwrap();
    assert_eq!(
        reopened.get(&key).unwrap(),
        RevisionState::missing(dodb_core::Revision::ZERO)
    );
}

#[test]
fn wal_write_and_sync_fault_matrix_never_exposes_a_partial_commit() {
    for write_number in 1..=9 {
        let mut store = BTreeStore::open_with_wal(
            CrashableFile::new(),
            CrashableFile::new(),
            DatabaseConfig::default().with_cache_capacity(0),
        )
        .unwrap();
        let (data, mut wal) = store.into_files().unwrap();
        wal.set_fault_plan(FaultPlan::default().on_nth(
            FileOperation::WriteAt,
            write_number,
            FaultAction::io(std::io::ErrorKind::Other, "deterministic WAL write crash"),
        ));
        store =
            BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
                .unwrap();
        let key = DocumentKey::new(vec![6], vec![write_number as u8]);
        assert!(store.put(key.clone(), b"matrix".to_vec()).is_err());
        let (mut data, mut wal) = store.into_files().unwrap();
        data.crash();
        wal.crash();
        let mut reopened =
            BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
                .unwrap();
        assert_eq!(
            reopened.get(&key).unwrap(),
            RevisionState::missing(dodb_core::Revision::ZERO)
        );
        reopened.check_invariants().unwrap();
    }

    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new(),
        CrashableFile::new(),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    let (data, mut wal) = store.into_files().unwrap();
    wal.set_fault_plan(FaultPlan::default().on_next(
        FileOperation::SyncData,
        FaultAction::io(std::io::ErrorKind::Other, "deterministic WAL sync crash"),
    ));
    store = BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
        .unwrap();
    let key = DocumentKey::new(vec![7], vec![7]);
    assert!(store.put(key.clone(), b"sync matrix".to_vec()).is_err());
    let (mut data, mut wal) = store.into_files().unwrap();
    data.crash();
    wal.crash();
    let mut reopened =
        BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
            .unwrap();
    assert_eq!(
        reopened.get(&key).unwrap(),
        RevisionState::missing(dodb_core::Revision::ZERO)
    );
    reopened.check_invariants().unwrap();
}

#[test]
fn wal_recovery_restores_splits_allocator_and_overflow_pages_atomically() {
    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new(),
        CrashableFile::new(),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    for index in 0..320u16 {
        store
            .put(
                DocumentKey::new(
                    (index % 11).to_le_bytes().to_vec(),
                    index.to_le_bytes().to_vec(),
                ),
                vec![index as u8; 180],
            )
            .unwrap();
    }
    let overflow_key = DocumentKey::new(vec![99], vec![1]);
    store.put(overflow_key.clone(), vec![0xa5; 9000]).unwrap();
    store
        .put(overflow_key.clone(), b"inline replacement".to_vec())
        .unwrap();
    store.delete(overflow_key.clone()).unwrap();
    let expected = store.scan(None, usize::MAX).unwrap();
    let (mut data, mut wal) = store.into_files().unwrap();
    data.crash();
    wal.crash();
    let mut reopened =
        BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
            .unwrap();
    assert_eq!(reopened.scan(None, usize::MAX).unwrap(), expected);
    let report = reopened.check_invariants().unwrap();
    assert!(report.leaked_pages.is_empty());
}

#[test]
fn named_crash_injector_covers_the_commit_boundary() {
    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new(),
        CrashableFile::new(),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    store.set_fault_injector(dodb_testkit::CrashInjector::at("before_commit_record", 1));
    let key = DocumentKey::new(vec![8], vec![8]);
    assert!(store.put(key.clone(), b"injected".to_vec()).is_err());
    let (mut data, mut wal) = store.into_files().unwrap();
    data.crash();
    wal.crash();
    let mut reopened =
        BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
            .unwrap();
    assert_eq!(
        reopened.get(&key).unwrap(),
        RevisionState::missing(dodb_core::Revision::ZERO)
    );
}

#[test]
fn deterministic_crash_recovery_differential_sequence_matches_reference_values() {
    let mut store = BTreeStore::open_with_wal(
        CrashableFile::new(),
        CrashableFile::new(),
        DatabaseConfig::default().with_cache_capacity(0),
    )
    .unwrap();
    let mut reference = ReferenceDb::new();
    let mut state = 0xd0db_2026_0002_u64;
    for _ in 0..256 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let document_key = key_from_rng(state);
        if state & 1 == 0 {
            let value = value_from_rng(state.rotate_left(9));
            store.put(document_key.clone(), value.clone()).unwrap();
            reference.put(document_key, value).unwrap();
        } else {
            store.delete(document_key.clone()).unwrap();
            reference.delete(document_key).unwrap();
        }
        let (mut data, mut wal) = store.into_files().unwrap();
        data.crash();
        wal.crash();
        store =
            BTreeStore::open_with_wal(data, wal, DatabaseConfig::default().with_cache_capacity(0))
                .unwrap();
        let actual = store.scan(None, usize::MAX).unwrap();
        let expected = reference.scan(None, usize::MAX);
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
        if state.is_multiple_of(17) {
            store.check_invariants().unwrap();
        }
    }
}
