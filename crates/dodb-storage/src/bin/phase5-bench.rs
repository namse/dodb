use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use dodb_core::DocumentKey;
use dodb_storage::{
    AsyncShard, BTreeStore, BatchRequest, CoordinatorConfig, DatabaseConfig, ProductionFile,
    restore_snapshot, validate_snapshot,
};

fn main() {
    let root = std::env::temp_dir().join(format!("dodb-phase5-bench-{}", std::process::id()));
    fs::create_dir_all(&root).expect("benchmark directory should be created");
    println!(
        "rows,checkpoint_lsn,pages_flushed,bytes_written,wal_bytes_reclaimed,checkpoint_nanos,snapshot_bytes,copy_nanos,validation_nanos,restore_nanos"
    );
    for row_count in [32usize, 512] {
        let database_path = root.join(format!("database-{row_count}"));
        let snapshot_path = root.join(format!("snapshot-{row_count}"));
        let restore_path = root.join(format!("restore-{row_count}"));
        let mut store: BTreeStore<ProductionFile, ProductionFile> =
            BTreeStore::<ProductionFile, ProductionFile>::open_path(
                &database_path,
                DatabaseConfig::default(),
            )
            .expect("database should open");
        for row_index in 0..row_count {
            store
                .put(
                    DocumentKey::new(b"phase5".to_vec(), row_index.to_le_bytes().to_vec()),
                    vec![row_index as u8; 128],
                )
                .expect("benchmark write should succeed");
        }
        let checkpoint = store.checkpoint().expect("checkpoint should succeed");
        let snapshot = store
            .create_snapshot(&snapshot_path)
            .expect("snapshot should succeed");
        validate_snapshot(&snapshot_path).expect("snapshot should validate");
        let restore_started = Instant::now();
        restore_snapshot(&snapshot_path, &restore_path).expect("restore should succeed");
        let restore_nanos = restore_started.elapsed().as_nanos();
        println!(
            "{row_count},{checkpoint_lsn},{pages_flushed},{bytes_written},{wal_bytes_reclaimed},{checkpoint_nanos},{snapshot_bytes},{copy_nanos},{validation_nanos},{restore_nanos}",
            checkpoint_lsn = checkpoint.checkpoint_lsn.get(),
            pages_flushed = checkpoint.pages_flushed,
            bytes_written = checkpoint.bytes_written,
            wal_bytes_reclaimed = checkpoint.wal_bytes_reclaimed,
            checkpoint_nanos = checkpoint.duration_nanos,
            snapshot_bytes = snapshot.bytes_copied,
            copy_nanos = snapshot.copy_duration_nanos,
            validation_nanos = snapshot.validation_duration_nanos,
        );
        drop(store);
    }
    benchmark_concurrent_checkpoint(&root);
    let _ = fs::remove_dir_all(root);
}

fn benchmark_concurrent_checkpoint(root: &Path) {
    let database_path = root.join("concurrent");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should start");
    runtime.block_on(async {
        let store = BTreeStore::<ProductionFile, ProductionFile>::open_path(
            &database_path,
            DatabaseConfig::default(),
        )
        .expect("concurrent benchmark database should open");
        let shard = Arc::new(AsyncShard::start_with_config(
            store,
            CoordinatorConfig {
                queue_capacity: 1024,
                max_group_requests: 64,
                ..CoordinatorConfig::default()
            },
        ));
        let mut tasks = Vec::new();
        for client_id in 0u64..8 {
            let shard = Arc::clone(&shard);
            tasks.push(tokio::spawn(async move {
                for operation_index in 0u64..64 {
                    shard
                        .execute(BatchRequest::Put {
                            key: DocumentKey::new(
                                b"concurrent".to_vec(),
                                [client_id.to_le_bytes(), operation_index.to_le_bytes()]
                                    .concat(),
                            ),
                            value: vec![operation_index as u8; 64],
                        })
                        .await
                        .expect("concurrent benchmark write should succeed");
                }
            }));
        }
        tokio::task::yield_now().await;
        let wait_started = Instant::now();
        let checkpoint = shard
            .checkpoint()
            .await
            .expect("concurrent checkpoint should succeed");
        let total_nanos = wait_started.elapsed().as_nanos();
        for task in tasks {
            task.await.expect("concurrent benchmark task should finish");
        }
        let shard = match Arc::try_unwrap(shard) {
            Ok(shard) => shard,
            Err(_) => panic!("concurrent benchmark retained a shard reference"),
        };
        println!(
            "concurrent_clients=8,operations=512,total_checkpoint_nanos={total_nanos},checkpoint_nanos={},queue_wait_nanos={}",
            checkpoint.duration_nanos,
            total_nanos.saturating_sub(u128::from(checkpoint.duration_nanos)),
        );
        shard.close().await.expect("concurrent shard should close");
    });
}
