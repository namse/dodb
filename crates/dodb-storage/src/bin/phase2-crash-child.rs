use std::env;
use std::thread;
use std::time::Duration;

use dodb_core::DocumentKey;
use dodb_storage::{BTreeStore, DatabaseConfig, ProductionFile};

fn main() {
    let mut arguments = env::args_os().skip(1);
    let data_path = arguments.next().expect("data path is required");
    let wal_path = arguments.next().expect("WAL path is required");
    let marker_path = arguments.next().expect("marker path is required");

    let mut store = BTreeStore::open_with_wal(
        ProductionFile::open(data_path).expect("data file should open"),
        ProductionFile::open(wal_path).expect("WAL file should open"),
        DatabaseConfig::default(),
    )
    .expect("database should open");
    store
        .put(
            DocumentKey::new(b"subprocess".to_vec(), b"key".to_vec()),
            b"value".to_vec(),
        )
        .expect("WAL-backed put should succeed");
    std::fs::write(marker_path, b"success-returned").expect("marker should be written");

    loop {
        thread::sleep(Duration::from_millis(50));
    }
}
