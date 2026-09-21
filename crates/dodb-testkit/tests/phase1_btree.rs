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
