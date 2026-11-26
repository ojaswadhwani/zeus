use super::{APPLIED_INDEX, MAX_SNAPSHOT_IMAGE, MEMBERS, stored_index};
use raft::eraftpb::{Entry, HardState, Snapshot};
use raft::storage::MemStorageCore;
use raft::{GetEntriesContext, RaftState, Storage, StorageError, storage::MemStorage};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::RwLockWriteGuard;
use zeus::{BatchOp, MAX_KEY_SIZE, MAX_VALUE_SIZE, Store};

const MAGIC: &[u8] = b"ZEUSSN01";
const REFERENCE: &[u8] = b"ZEUSREF1";
const DISK_CHUNK: usize = 256 * 1024;

fn image_path(path: &Path, image: &Snapshot) -> PathBuf {
    let metadata = image.get_metadata();
    path.with_extension(format!("snapshot-{}-{}", metadata.index, metadata.term))
}

/// Publish a checksummed sidecar before writing its reference into the Raft log.
pub(super) fn save(path: &Path, image: &Snapshot) -> io::Result<Snapshot> {
    let stage = path.with_extension("snapshot-staging");
    remove_if_exists(&stage)?;
    let mut file = Store::open(&stage)?;
    for (index, chunk) in image.data.chunks(DISK_CHUNK).enumerate() {
        file.put(&(index as u64).to_be_bytes(), chunk)?;
    }
    drop(file);
    fs::rename(stage, image_path(path, image))?;
    File::open(path.parent().unwrap())?.sync_all()?;

    let mut reference = Snapshot::default();
    reference.set_metadata(image.get_metadata().clone());
    reference.set_data(
        [REFERENCE, &(image.data.len() as u64).to_be_bytes()]
            .concat()
            .into(),
    );
    Ok(reference)
}

pub(super) fn load(path: &Path, mut record: Snapshot) -> io::Result<Snapshot> {
    let Some(length) = record.data.strip_prefix(REFERENCE) else {
        // Earlier preview nodes kept the complete snapshot inline.
        return Ok(record);
    };
    let length = u64::from_be_bytes(length.try_into().map_err(|_| invalid())?);
    if length == 0 || length > MAX_SNAPSHOT_IMAGE as u64 {
        return Err(invalid());
    }
    let length = length as usize;
    let path = image_path(path, &record);
    fs::metadata(&path)?;
    let file = Store::open(path)?;
    let mut image = Vec::with_capacity(length);
    for (index, offset) in (0..length).step_by(DISK_CHUNK).enumerate() {
        let chunk = file
            .get(&(index as u64).to_be_bytes())?
            .ok_or_else(invalid)?;
        if chunk.len() != DISK_CHUNK.min(length - offset) {
            return Err(invalid());
        }
        image.extend_from_slice(&chunk);
    }
    record.set_data(image.into());
    Ok(record)
}

pub(super) fn prune(path: &Path, image: &Snapshot) -> io::Result<()> {
    let parent = path.parent().unwrap();
    let keep = image_path(path, image);
    let prefix = format!("{}.snapshot-", path.file_stem().unwrap().to_string_lossy());
    let mut removed = false;
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(suffix) = name.to_str().and_then(|name| name.strip_prefix(&prefix)) else {
            continue;
        };
        let image_file = suffix.split_once('-').is_some_and(|(index, term)| {
            index.parse::<u64>().is_ok() && term.parse::<u64>().is_ok()
        });
        if (suffix == "staging" || image_file) && entry.path() != keep {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(crate) struct RaftStorage {
    memory: MemStorage,
    image: Snapshot,
}

impl RaftStorage {
    pub(super) fn new(image: Snapshot, entries: &[Entry], hard_state: HardState) -> Self {
        let memory = MemStorage::new_with_conf_state((MEMBERS.to_vec(), vec![]));
        {
            let mut store = memory.wl();
            if !image.is_empty() {
                store.apply_snapshot(image.clone()).unwrap();
            }
            store.append(entries).unwrap();
            store.set_hardstate(hard_state);
        }
        Self { memory, image }
    }

    pub(super) fn write(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.memory.wl()
    }

    pub(super) fn snapshot_index(&self) -> u64 {
        self.image.get_metadata().index
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.memory.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.memory.entries(low, high, max_size, context)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        self.memory.term(index)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.memory.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.memory.last_index()
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        if self.image.is_empty() || self.snapshot_index() < request_index {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        Ok(self.image.clone())
    }
}

pub(super) fn export(data: &Store, cluster: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let mut image = MAGIC.to_vec();
    append_field(&mut image, cluster);
    // The replicated store reserves these prefixes and keeps the applied index
    // in its own key, so the range includes all state-machine records.
    for entry in data.scan(APPLIED_INDEX, b"retry0") {
        let (key, value) = entry?;
        if image.len() + 8 + key.len() + value.len() > MAX_SNAPSHOT_IMAGE {
            return Ok(None);
        }
        append_field(&mut image, key);
        append_field(&mut image, &value);
    }
    Ok(Some(image))
}

fn append_field(image: &mut Vec<u8>, field: &[u8]) {
    image.extend_from_slice(&(field.len() as u32).to_be_bytes());
    image.extend_from_slice(field);
}

fn field<'a>(bytes: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    let (length, rest) = bytes.split_first_chunk::<4>().ok_or_else(invalid)?;
    let (field, rest) = rest
        .split_at_checked(u32::from_be_bytes(*length) as usize)
        .ok_or_else(invalid)?;
    *bytes = rest;
    Ok(field)
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid Zeus snapshot")
}

pub(super) fn validate<'a>(
    snapshot: &'a Snapshot,
    cluster: &[u8],
) -> io::Result<Vec<(&'a [u8], &'a [u8])>> {
    let metadata = snapshot.get_metadata();
    let conf = metadata.get_conf_state();
    if metadata.index == 0
        || metadata.term == 0
        || conf.voters != MEMBERS
        || !conf.learners.is_empty()
        || !conf.voters_outgoing.is_empty()
        || !conf.learners_next.is_empty()
        || conf.auto_leave
        || snapshot.data.len() > MAX_SNAPSHOT_IMAGE
    {
        return Err(invalid());
    }
    let mut bytes = snapshot.data.strip_prefix(MAGIC).ok_or_else(invalid)?;
    if field(&mut bytes)? != cluster {
        return Err(invalid());
    }
    let mut rows = Vec::new();
    let mut previous: &[u8] = b"";
    while !bytes.is_empty() {
        let key = field(&mut bytes)?;
        let value = field(&mut bytes)?;
        if key <= previous || key.len() > MAX_KEY_SIZE || value.len() > MAX_VALUE_SIZE {
            return Err(invalid());
        }
        if key == APPLIED_INDEX {
            if value != metadata.index.to_be_bytes() {
                return Err(invalid());
            }
        } else if key.starts_with(b"retry/") {
            if key.len() == b"retry/".len() || value.len() <= 8 {
                return Err(invalid());
            }
        } else if !key.starts_with(b"client/") || key.len() == b"client/".len() {
            return Err(invalid());
        }
        previous = key;
        rows.push((key, value));
    }
    if rows.first().map(|(key, _)| *key) != Some(APPLIED_INDEX) {
        return Err(invalid());
    }
    Ok(rows)
}

pub(super) fn activate(path: &Path, rows: &[(&[u8], &[u8])], index: u64) -> io::Result<Store> {
    let stage = path.with_extension("installing");
    remove_if_exists(&stage)?;
    let mut replacement = Store::open(&stage)?;
    let mut remaining = rows;
    while !remaining.is_empty() {
        let mut bytes = 0;
        let count = remaining
            .iter()
            .take(zeus::MAX_BATCH_OPERATIONS)
            .take_while(|(key, value)| {
                bytes += 9 + key.len() + value.len();
                bytes <= zeus::MAX_BATCH_SIZE
            })
            .count();
        let (chunk, rest) = remaining.split_at(count);
        let batch: Vec<_> = chunk
            .iter()
            .map(|(key, value)| BatchOp::Put(key, value))
            .collect();
        replacement.write_batch(&batch)?;
        remaining = rest;
    }
    assert_eq!(stored_index(&replacement, APPLIED_INDEX), index);
    drop(replacement);
    fs::rename(stage, path)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Store::open(path)
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}
