use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use dodb_core::{Error, Lsn, Result};

use crate::ProductionFile;
use crate::btree::{BTreeStore, DatabaseConfig, NoWal};
use crate::durable_file::DurableFile;
use crate::fault::FaultInjector;
use crate::page::PAGE_SIZE;
use crate::superblock::{SUPERBLOCK_FORMAT_VERSION, Superblock, choose_superblock};

pub const SNAPSHOT_MAGIC: &str = "DODB-SNAPSHOT";
pub const SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const SNAPSHOT_DATABASE_NAME: &str = "database";
pub const SNAPSHOT_MANIFEST_NAME: &str = "manifest";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    pub database_uuid: [u8; 16],
    pub tenant_id: u64,
    pub shard_id: u64,
    pub shard_epoch: u64,
    pub database_format_version: u16,
    pub page_size: u32,
    pub checkpoint_lsn: Lsn,
    pub database_file_size: u64,
    pub database_checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReport {
    pub manifest: SnapshotManifest,
    pub bytes_copied: u64,
    pub copy_duration_nanos: u64,
    pub validation_duration_nanos: u64,
}

impl SnapshotManifest {
    pub fn from_superblock(
        superblock: &Superblock,
        database_file_size: u64,
        database_checksum: u32,
    ) -> Self {
        Self {
            database_uuid: superblock.database_uuid,
            tenant_id: superblock.tenant_id.get(),
            shard_id: superblock.shard_id.get(),
            shard_epoch: superblock.shard_epoch.get(),
            database_format_version: superblock.format_version,
            page_size: superblock.page_size,
            checkpoint_lsn: superblock.checkpoint_lsn,
            database_file_size,
            database_checksum,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let body = self.body();
        let checksum = crc32c::crc32c(body.as_bytes());
        let mut encoded = body.into_bytes();
        encoded.extend_from_slice(format!("manifest_checksum={checksum:08x}\n").as_bytes());
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| Error::snapshot("snapshot manifest is not UTF-8"))?;
        let checksum_line = text
            .strip_suffix('\n')
            .and_then(|value| value.rsplit_once('\n'))
            .ok_or_else(|| Error::snapshot("snapshot manifest is missing its checksum"))?;
        let (body, checksum_entry) = checksum_line;
        let checksum_text = checksum_entry
            .strip_prefix("manifest_checksum=")
            .ok_or_else(|| Error::snapshot("snapshot manifest has an invalid checksum field"))?;
        let expected_checksum = u32::from_str_radix(checksum_text, 16)
            .map_err(|_| Error::snapshot("snapshot manifest checksum is not hexadecimal"))?;
        let actual_checksum = crc32c::crc32c(format!("{body}\n").as_bytes());
        if expected_checksum != actual_checksum {
            return Err(Error::snapshot("snapshot manifest checksum mismatch"));
        }

        let entries = body.lines().collect::<Vec<_>>();
        if entries.len() != 11 {
            return Err(Error::snapshot(
                "snapshot manifest has an unexpected field count",
            ));
        }
        let expected_keys = [
            "magic",
            "version",
            "database_uuid",
            "tenant_id",
            "shard_id",
            "shard_epoch",
            "database_format_version",
            "page_size",
            "checkpoint_lsn",
            "database_file_size",
            "database_checksum",
        ];
        for (entry, expected_key) in entries.iter().zip(expected_keys) {
            if !entry.starts_with(&format!("{expected_key}=")) {
                return Err(Error::snapshot(
                    "snapshot manifest fields are not canonical",
                ));
            }
        }
        if value(entries[0])? != SNAPSHOT_MAGIC {
            return Err(Error::snapshot("snapshot manifest magic mismatch"));
        }
        let version = parse_u16(value(entries[1])?, "snapshot manifest version")?;
        if version != SNAPSHOT_FORMAT_VERSION {
            return Err(Error::UnsupportedFormat(format!(
                "snapshot version {version}, supported {SNAPSHOT_FORMAT_VERSION}"
            )));
        }
        Ok(Self {
            database_uuid: decode_hex_16(value(entries[2])?)?,
            tenant_id: parse_u64(value(entries[3])?, "tenant ID")?,
            shard_id: parse_u64(value(entries[4])?, "shard ID")?,
            shard_epoch: parse_u64(value(entries[5])?, "shard epoch")?,
            database_format_version: parse_u16(value(entries[6])?, "database format version")?,
            page_size: parse_u32(value(entries[7])?, "page size")?,
            checkpoint_lsn: Lsn::new(parse_u64(value(entries[8])?, "checkpoint LSN")?),
            database_file_size: parse_u64(value(entries[9])?, "database file size")?,
            database_checksum: parse_hex_u32(value(entries[10])?, "database checksum")?,
        })
    }

    fn body(&self) -> String {
        format!(
            "magic={SNAPSHOT_MAGIC}\nversion={SNAPSHOT_FORMAT_VERSION}\ndatabase_uuid={}\ntenant_id={}\nshard_id={}\nshard_epoch={}\ndatabase_format_version={}\npage_size={}\ncheckpoint_lsn={}\ndatabase_file_size={}\ndatabase_checksum={:08x}\n",
            encode_hex(&self.database_uuid),
            self.tenant_id,
            self.shard_id,
            self.shard_epoch,
            self.database_format_version,
            self.page_size,
            self.checkpoint_lsn.get(),
            self.database_file_size,
            self.database_checksum,
        )
    }
}

pub fn validate_snapshot(path: impl AsRef<Path>) -> Result<SnapshotManifest> {
    let path = path.as_ref();
    let manifest_path = path.join(SNAPSHOT_MANIFEST_NAME);
    let database_path = path.join(SNAPSHOT_DATABASE_NAME);
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|error| Error::snapshot(format!("cannot read snapshot manifest: {error}")))?;
    let manifest = SnapshotManifest::decode(&manifest_bytes)?;
    validate_database_file(&database_path, &manifest)?;
    Ok(manifest)
}

pub(crate) fn create_snapshot<F: DurableFile>(
    file: &mut F,
    superblock: &Superblock,
    destination: &Path,
    mut injector: Option<&mut (dyn FaultInjector + Send + '_)>,
) -> Result<SnapshotReport> {
    if destination.exists() {
        return Err(Error::snapshot("snapshot destination already exists"));
    }
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(Error::snapshot(
            "snapshot destination parent is not a directory",
        ));
    }
    let temporary = create_temporary_directory(destination, "snapshot")?;
    let result = create_snapshot_in_directory(file, superblock, &temporary, &mut injector);
    match result {
        Ok(report) => {
            let finalize_result = (|| {
                hit(&mut injector, "before_snapshot_directory_sync")?;
                sync_directory(&temporary)?;
                hit(&mut injector, "after_snapshot_directory_sync")?;
                hit(&mut injector, "before_snapshot_finalize")?;
                fs::rename(&temporary, destination).map_err(|error| {
                    Error::snapshot(format!("cannot finalize snapshot directory: {error}"))
                })?;
                hit(&mut injector, "after_snapshot_finalize")?;
                hit(&mut injector, "before_snapshot_parent_sync")?;
                sync_directory(parent)?;
                hit(&mut injector, "after_snapshot_parent_sync")?;
                Ok(report)
            })();
            if finalize_result.is_err() {
                let _ = fs::remove_dir_all(&temporary);
            }
            finalize_result
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&temporary);
            Err(error)
        }
    }
}

fn create_snapshot_in_directory<F: DurableFile>(
    file: &mut F,
    superblock: &Superblock,
    directory: &Path,
    injector: &mut Option<&mut (dyn FaultInjector + Send + '_)>,
) -> Result<SnapshotReport> {
    let database_file_size = file.len()?;
    let mut source_offset = 0u64;
    let mut database_checksum = 0u32;
    let copy_started = Instant::now();
    let database_path = directory.join(SNAPSHOT_DATABASE_NAME);
    hit(injector, "before_snapshot_file_create")?;
    let mut database = File::create(&database_path).map_err(|error| {
        Error::snapshot(format!("cannot create snapshot database file: {error}"))
    })?;
    hit(injector, "after_snapshot_file_create")?;
    let mut buffer = vec![0u8; PAGE_SIZE * 16];
    while source_offset < database_file_size {
        let remaining = database_file_size - source_offset;
        let read_length = remaining.min(buffer.len() as u64) as usize;
        read_exact_at(file, source_offset, &mut buffer[..read_length])?;
        database_checksum = crc32c::crc32c_append(database_checksum, &buffer[..read_length]);
        hit(injector, "during_snapshot_copy")?;
        database
            .write_all(&buffer[..read_length])
            .map_err(|error| Error::snapshot(format!("snapshot copy failed: {error}")))?;
        source_offset = source_offset
            .checked_add(read_length as u64)
            .ok_or_else(|| Error::snapshot("snapshot copy offset overflow"))?;
    }
    database
        .set_len(database_file_size)
        .map_err(|error| Error::snapshot(format!("snapshot file length update failed: {error}")))?;
    hit(injector, "after_snapshot_copy")?;
    hit(injector, "before_snapshot_sync")?;
    hit(injector, "during_snapshot_sync")?;
    database
        .sync_all()
        .map_err(|error| Error::snapshot(format!("snapshot database sync failed: {error}")))?;
    hit(injector, "after_snapshot_sync")?;
    drop(database);

    let manifest =
        SnapshotManifest::from_superblock(superblock, database_file_size, database_checksum);
    let manifest_path = directory.join(SNAPSHOT_MANIFEST_NAME);
    hit(injector, "before_snapshot_manifest_write")?;
    let mut manifest_file = File::create(&manifest_path)
        .map_err(|error| Error::snapshot(format!("cannot create snapshot manifest: {error}")))?;
    manifest_file
        .write_all(&manifest.encode()?)
        .map_err(|error| Error::snapshot(format!("snapshot manifest write failed: {error}")))?;
    hit(injector, "after_snapshot_manifest_write")?;
    hit(injector, "before_snapshot_manifest_sync")?;
    hit(injector, "during_snapshot_manifest_sync")?;
    manifest_file
        .sync_all()
        .map_err(|error| Error::snapshot(format!("snapshot manifest sync failed: {error}")))?;
    hit(injector, "after_snapshot_manifest_sync")?;
    drop(manifest_file);

    let validation_started = Instant::now();
    validate_snapshot(directory)?;
    Ok(SnapshotReport {
        manifest,
        bytes_copied: database_file_size,
        copy_duration_nanos: copy_started
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX),
        validation_duration_nanos: validation_started
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX),
    })
}

pub fn restore_snapshot(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<SnapshotManifest> {
    restore_snapshot_with_injector(source.as_ref(), destination.as_ref(), None)
}

pub fn restore_snapshot_with_injector(
    source: &Path,
    destination: &Path,
    mut injector: Option<&mut (dyn FaultInjector + Send + '_)>,
) -> Result<SnapshotManifest> {
    let manifest = validate_snapshot(source)?;
    if destination.exists() {
        return Err(Error::snapshot("restore destination already exists"));
    }
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(Error::snapshot(
            "restore destination parent is not a directory",
        ));
    }
    let mut temporary_path = None;
    let result = (|| {
        hit(&mut injector, "before_restore_copy")?;
        let mut source_file = File::open(source.join(SNAPSHOT_DATABASE_NAME))
            .map_err(|error| Error::snapshot(format!("cannot open snapshot database: {error}")))?;
        let (created_path, mut destination_file) = create_temporary_file(destination, "restore")?;
        temporary_path = Some(created_path.clone());
        let mut buffer = vec![0u8; PAGE_SIZE * 16];
        loop {
            let count = source_file
                .read(&mut buffer)
                .map_err(|error| Error::snapshot(format!("restore copy read failed: {error}")))?;
            if count == 0 {
                break;
            }
            hit(&mut injector, "during_restore_copy")?;
            destination_file
                .write_all(&buffer[..count])
                .map_err(|error| Error::snapshot(format!("restore copy write failed: {error}")))?;
        }
        hit(&mut injector, "before_restore_sync")?;
        destination_file
            .sync_all()
            .map_err(|error| Error::snapshot(format!("restore sync failed: {error}")))?;
        drop(destination_file);
        hit(&mut injector, "after_restore_sync")?;
        let temporary = temporary_path
            .as_ref()
            .ok_or_else(|| Error::snapshot("restore temporary path was not created"))?;
        validate_database_file(temporary, &manifest)?;
        hit(&mut injector, "before_restore_finalize")?;
        fs::rename(temporary, destination)
            .map_err(|error| Error::snapshot(format!("restore finalize failed: {error}")))?;
        hit(&mut injector, "after_restore_finalize")?;
        hit(&mut injector, "before_restore_parent_sync")?;
        sync_directory(parent)?;
        hit(&mut injector, "after_restore_parent_sync")?;
        Ok(manifest.clone())
    })();
    if let Some(temporary) = temporary_path
        && result.is_err()
    {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn validate_database_file(path: &Path, manifest: &SnapshotManifest) -> Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| Error::snapshot(format!("cannot stat snapshot database: {error}")))?;
    if metadata.len() != manifest.database_file_size {
        return Err(Error::snapshot(
            "snapshot database length does not match manifest",
        ));
    }
    let mut file = File::open(path)
        .map_err(|error| Error::snapshot(format!("cannot open snapshot database: {error}")))?;
    let mut checksum = 0u32;
    let mut buffer = vec![0u8; PAGE_SIZE * 16];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| Error::snapshot(format!("cannot read snapshot database: {error}")))?;
        if count == 0 {
            break;
        }
        checksum = crc32c::crc32c_append(checksum, &buffer[..count]);
    }
    if checksum != manifest.database_checksum {
        return Err(Error::snapshot("snapshot database checksum mismatch"));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| Error::snapshot(format!("cannot seek snapshot database: {error}")))?;
    let mut superblocks = [0u8; PAGE_SIZE * 2];
    file.read_exact(&mut superblocks).map_err(|error| {
        Error::snapshot(format!(
            "snapshot database is shorter than its superblocks: {error}"
        ))
    })?;
    let selected = choose_superblock(&superblocks[..PAGE_SIZE], &superblocks[PAGE_SIZE..])
        .map_err(|error| Error::snapshot(format!("snapshot superblock is invalid: {error}")))?;
    let superblock = selected.superblock;
    if superblock.database_uuid != manifest.database_uuid
        || superblock.tenant_id.get() != manifest.tenant_id
        || superblock.shard_id.get() != manifest.shard_id
        || superblock.shard_epoch.get() != manifest.shard_epoch
        || superblock.format_version != manifest.database_format_version
        || superblock.page_size != manifest.page_size
        || superblock.checkpoint_lsn != manifest.checkpoint_lsn
    {
        return Err(Error::snapshot(
            "snapshot superblock metadata does not match its manifest",
        ));
    }
    if manifest.database_format_version != SUPERBLOCK_FORMAT_VERSION
        || manifest.page_size as usize != PAGE_SIZE
    {
        return Err(Error::unsupported_format(
            "snapshot database format is unsupported",
        ));
    }
    let config = DatabaseConfig {
        database_uuid: manifest.database_uuid,
        tenant_id: dodb_core::TenantId::new(manifest.tenant_id),
        shard_id: dodb_core::ShardId::new(manifest.shard_id),
        shard_epoch: dodb_core::ShardEpoch::new(manifest.shard_epoch),
        ..DatabaseConfig::default()
    };
    let production_file = ProductionFile::open(path)?;
    let mut store = BTreeStore::<ProductionFile, NoWal>::open(production_file, config)?;
    store
        .check_invariants()
        .map_err(|error| Error::snapshot(format!("snapshot invariant check failed: {error}")))?;
    Ok(())
}

fn create_temporary_directory(path: &Path, suffix: &str) -> Result<PathBuf> {
    for _ in 0..64 {
        let temporary = temporary_path(path, suffix)?;
        match fs::create_dir(&temporary) {
            Ok(()) => return Ok(temporary),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(Error::snapshot(format!(
                    "cannot create temporary snapshot directory: {error}"
                )));
            }
        }
    }
    Err(Error::snapshot(
        "could not allocate a unique temporary snapshot directory",
    ))
}

fn create_temporary_file(path: &Path, suffix: &str) -> Result<(PathBuf, File)> {
    for _ in 0..64 {
        let temporary = temporary_path(path, suffix)?;
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(Error::snapshot(format!(
                    "cannot create restore file: {error}"
                )));
            }
        }
    }
    Err(Error::snapshot(
        "could not allocate a unique temporary restore file",
    ))
}

fn temporary_path(path: &Path, suffix: &str) -> Result<PathBuf> {
    static TEMPORARY_COUNTER: AtomicU64 = AtomicU64::new(0);
    let name = path
        .file_name()
        .ok_or_else(|| Error::snapshot("snapshot path has no final component"))?
        .to_string_lossy();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::snapshot(format!("system clock is before UNIX epoch: {error}")))?
        .as_nanos();
    let counter = TEMPORARY_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(path.with_file_name(format!(
        ".{name}.{suffix}.{}-{timestamp:x}-{counter:x}",
        std::process::id()
    )))
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .map_err(|error| Error::snapshot(format!("cannot open directory for sync: {error}")))?
            .sync_all()
            .map_err(|error| Error::snapshot(format!("directory sync failed: {error}")))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(Error::snapshot(
            "directory synchronization is unsupported on this platform",
        ))
    }
}

fn read_exact_at<F: DurableFile>(file: &mut F, offset: u64, buffer: &mut [u8]) -> Result<()> {
    let mut position = 0usize;
    while position < buffer.len() {
        let count = file.read_at(offset + position as u64, &mut buffer[position..])?;
        if count == 0 {
            return Err(Error::snapshot("database file ended during snapshot copy"));
        }
        position += count;
    }
    Ok(())
}

fn hit(injector: &mut Option<&mut (dyn FaultInjector + Send + '_)>, point: &str) -> Result<()> {
    if let Some(injector) = injector.as_deref_mut() {
        injector.hit(point)?;
    }
    Ok(())
}

fn value(entry: &str) -> Result<&str> {
    entry
        .split_once('=')
        .map(|(_, value)| value)
        .ok_or_else(|| Error::snapshot("snapshot manifest field has no value"))
}

fn parse_u16(value: &str, name: &str) -> Result<u16> {
    value
        .parse()
        .map_err(|_| Error::snapshot(format!("{name} is invalid")))
}

fn parse_u32(value: &str, name: &str) -> Result<u32> {
    value
        .parse()
        .map_err(|_| Error::snapshot(format!("{name} is invalid")))
}

fn parse_u64(value: &str, name: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|_| Error::snapshot(format!("{name} is invalid")))
}

fn parse_hex_u32(value: &str, name: &str) -> Result<u32> {
    u32::from_str_radix(value, 16).map_err(|_| Error::snapshot(format!("{name} is invalid")))
}

fn encode_hex(bytes: &[u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex_16(value: &str) -> Result<[u8; 16]> {
    if value.len() != 32 {
        return Err(Error::snapshot("database UUID has an invalid length"));
    }
    let mut bytes = [0u8; 16];
    for (byte_index, byte) in bytes.iter_mut().enumerate() {
        let start = byte_index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16)
            .map_err(|_| Error::snapshot("database UUID is not hexadecimal"))?;
    }
    Ok(bytes)
}
