use std::time::{Duration, Instant};

use dodb_core::DocumentKey;
use dodb_storage::{BTreeStore, DatabaseConfig, ProductionFile};

const ROWS: usize = 2_000;

fn key(index: usize) -> DocumentKey {
    DocumentKey::new(
        (index % 64).to_le_bytes().to_vec(),
        (index as u64).to_le_bytes().to_vec(),
    )
}

fn random_index(state: &mut u64) -> usize {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (*state as usize) % ROWS
}

fn fresh_store(cache_capacity: usize, name: &str) -> BTreeStore<ProductionFile> {
    let path = std::env::temp_dir().join(format!(
        "dodb-phase1-bench-{}-{name}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    BTreeStore::<ProductionFile>::open_path(
        &path,
        DatabaseConfig::default().with_cache_capacity(cache_capacity),
    )
    .expect("benchmark database should open")
}

fn report(name: &str, elapsed: Duration, operations: usize) {
    let seconds = elapsed.as_secs_f64();
    println!(
        "{name:24} {:>8.0} ops/s {:>8.3} ms/op",
        operations as f64 / seconds,
        seconds * 1_000.0 / operations as f64
    );
}

fn main() {
    println!("dodb Phase 1 baseline; rows={ROWS}; direct synchronized publisher");
    for cache_capacity in [0, 16, 256] {
        println!("\ncache_capacity={cache_capacity}");

        let mut store = fresh_store(cache_capacity, "sequential-put");
        let start = Instant::now();
        for index in 0..ROWS {
            store
                .put(key(index), (index as u64).to_le_bytes())
                .expect("sequential put should succeed");
        }
        report("sequential PUT", start.elapsed(), ROWS);

        let mut store = fresh_store(cache_capacity, "random-put");
        let mut state = 0x5eed_cafe_u64;
        let start = Instant::now();
        for _ in 0..ROWS {
            let index = random_index(&mut state);
            store
                .put(key(index), (index as u64).to_le_bytes())
                .expect("random put should succeed");
        }
        report("random PUT", start.elapsed(), ROWS);

        let mut store = fresh_store(cache_capacity, "reads");
        for index in 0..ROWS {
            store
                .put(key(index), (index as u64).to_le_bytes())
                .expect("seed put should succeed");
        }
        let mut state = 0xd0db_2026_u64;
        let start = Instant::now();
        for _ in 0..ROWS {
            let _ = store
                .get(&key(random_index(&mut state)))
                .expect("get should succeed");
        }
        report("random GET", start.elapsed(), ROWS);

        let start = Instant::now();
        for _ in 0..100 {
            let _ = store.scan(None, 100).expect("scan should succeed");
        }
        report("sequential scan/range", start.elapsed(), 100);

        let start = Instant::now();
        for pk in 0..100 {
            let _ = store
                .query(
                    &dodb_core::PrimaryKey::new((pk as u64 % 64).to_le_bytes().to_vec()),
                    None,
                    32,
                )
                .expect("query should succeed");
        }
        report("single-pk query", start.elapsed(), 100);

        let mut state = 0x1234_5678_u64;
        let start = Instant::now();
        for operation in 0..ROWS {
            let index = random_index(&mut state);
            if operation % 2 == 0 {
                let _ = store.get(&key(index)).expect("mixed get should succeed");
            } else {
                store
                    .put(key(index), (operation as u64).to_le_bytes())
                    .expect("mixed put should succeed");
            }
        }
        report("mixed read/write", start.elapsed(), ROWS);
    }
}
