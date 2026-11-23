#![forbid(unsafe_code)]

mod table;
pub use table::SortedTable;

use table::{Index, LIVE_MAGIC, Location, read_value};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Bound::{Excluded, Included};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_KEY_SIZE: usize = 4 * 1024;
pub const MAX_VALUE_SIZE: usize = 1024 * 1024;
pub const MAX_BATCH_OPERATIONS: usize = 1024;
pub const MAX_BATCH_SIZE: usize = 4 * 1024 * 1024;
const OP_HEADER_SIZE: usize = 9;
const HEADER_SIZE: usize = 20;
const PUT_MAGIC: &[u8; 4] = b"ZWL1";
const DELETE_MAGIC: &[u8; 4] = b"ZWD1";
const BATCH_MAGIC: &[u8; 4] = b"ZWB1";

/// An operation in an atomic write batch.
#[derive(Clone, Copy)]
pub enum BatchOp<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
}

/// An embedded store backed by an exclusively locked local log.
pub struct Store {
    path: PathBuf,
    directory: File,
    log: File,
    entries: Index,
    write_failed: bool,
}

impl Store {
    /// Open or create a log inside an existing operator-controlled directory.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "expected a log filename"))?;
        let parent = parent.canonicalize()?;
        let path = parent.join(name);
        let directory = File::open(parent)?;
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)?;
        log.try_lock().map_err(io::Error::from)?;
        verify_log_path(&path, &log)?;

        let mut store = Self {
            path,
            directory,
            log,
            entries: Index::new(),
            write_failed: false,
        };
        store.recover()?;
        store.log.sync_all()?;
        store.directory.sync_all()?;
        Ok(store)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.entries.get(key).map(|location| read_value(&self.log, key, location)).transpose()
    }

    /// Iterate live entries in bytewise order over the half-open interval [start, end).
    /// Equal or reversed bounds yield an empty iterator.
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = io::Result<(&[u8], Vec<u8>)>> {
        (start < end)
            .then(|| self.entries.range::<[u8], _>((Included(start), Excluded(end))))
            .into_iter()
            .flatten()
            .map(|(key, location)| read_value(&self.log, key, location).map(|value| (key.as_slice(), value)))
    }

    /// A successful return means the complete record has been synced to disk.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.ensure_writable()?;
        validate_key(key)?;
        if value.len() > MAX_VALUE_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid value size"));
        }

        let offset = self.append(PUT_MAGIC, key, value)?;
        self.apply(&BatchOp::Put(key, value), offset + (HEADER_SIZE + key.len()) as u64);
        Ok(())
    }

    /// Durably remove a key. Returns false when the key was already absent.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        self.ensure_writable()?;
        validate_key(key)?;
        if !self.entries.contains_key(key) {
            return Ok(false);
        }
        self.append(DELETE_MAGIC, key, &[])?;
        self.entries.remove(key);
        Ok(true)
    }

    /// Apply operations in order and sync them as one record. Empty batches are no-ops.
    pub fn write_batch(&mut self, operations: &[BatchOp<'_>]) -> io::Result<()> {
        self.ensure_writable()?;
        if operations.is_empty() {
            return Ok(());
        }
        if operations.len() > MAX_BATCH_OPERATIONS {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "too many batch operations"));
        }

        let mut body = Vec::new();
        for operation in operations {
            let (kind, key, value) = match *operation {
                BatchOp::Put(key, value) => (1, key, value),
                BatchOp::Delete(key) => (2, key, &[][..]),
            };
            validate_key(key)?;
            if value.len() > MAX_VALUE_SIZE
                || body.len() + OP_HEADER_SIZE + key.len() + value.len() > MAX_BATCH_SIZE
            {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "batch size limit exceeded"));
            }

            body.push(kind);
            body.extend_from_slice(&(key.len() as u32).to_le_bytes());
            body.extend_from_slice(&(value.len() as u32).to_le_bytes());
            body.extend_from_slice(key);
            body.extend_from_slice(value);
        }
        let offset = self.append(BATCH_MAGIC, &[], &body)?;
        self.apply_batch(operations, offset + HEADER_SIZE as u64);
        Ok(())
    }

    /// Publish a read-only checkpoint at a new path in an existing directory.
    pub fn checkpoint(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.ensure_writable()?;
        table::publish(path.as_ref(), &self.log, &self.entries)
    }

    /// Replace the file with a sorted base table and an empty log tail.
    /// Compaction blocks writes and needs temporary space for the replacement log.
    pub fn compact(&mut self) -> io::Result<()> {
        self.ensure_writable()?;
        verify_log_path(&self.path, &self.log)?;
        let (temporary_path, mut replacement) = create_temporary(&self.path, "compact")?;
        let prepare = (|| {
            replacement.try_lock().map_err(io::Error::from)?;
            table::write_prefix(&mut replacement, &self.log, &self.entries, LIVE_MAGIC)?;
            let size = replacement.stream_position()?;
            replacement.sync_all()?;
            replacement.seek(SeekFrom::Start(0))?;
            let (index, end) = table::read_prefix(&replacement, size, LIVE_MAGIC)?;
            replacement.seek(SeekFrom::Start(end))?;
            fs::rename(&temporary_path, &self.path)?;
            Ok::<_, io::Error>(index)
        })();
        let index = match prepare {
            Ok(index) => index,
            Err(error) => {
                let _ = fs::remove_file(&temporary_path);
                return Err(error);
            }
        };

        // Both inodes are locked across rename. New opens verify their inode after locking.
        self.log = replacement;
        self.entries = index;
        let result = self.directory.sync_all();
        self.write_failed = result.is_err();
        result
    }

    fn apply_batch(&mut self, operations: &[BatchOp<'_>], mut offset: u64) {
        for operation in operations {
            offset += OP_HEADER_SIZE as u64;
            match *operation {
                BatchOp::Put(key, value) => {
                    offset += key.len() as u64;
                    self.apply(operation, offset);
                    offset += value.len() as u64;
                }
                BatchOp::Delete(key) => {
                    offset += key.len() as u64;
                    self.apply(operation, offset);
                }
            }
        }
    }

    fn apply(&mut self, operation: &BatchOp<'_>, offset: u64) {
        match *operation {
            BatchOp::Put(key, value) => {
                self.entries.insert(key.to_vec(), Location {
                    offset,
                    size: value.len(),
                    checksum: crc32(&[key, value]),
                });
            }
            BatchOp::Delete(key) => {
                self.entries.remove(key);
            }
        }
    }

    fn ensure_writable(&self) -> io::Result<()> {
        if self.write_failed {
            return Err(io::Error::other("reopen the store after a write failure"));
        }
        Ok(())
    }

    fn append(&mut self, magic: &[u8; 4], key: &[u8], value: &[u8]) -> io::Result<u64> {
        let offset = self.log.stream_position()?;
        let result = write_record(&mut self.log, magic, key, value)
            .and_then(|()| self.log.sync_all());
        self.write_failed = result.is_err();
        result.map(|()| offset)
    }

    fn recover(&mut self) -> io::Result<()> {
        let end = self.log.metadata()?.len();
        let mut offset = 0;
        if end >= 2 {
            let mut magic = [0; LIVE_MAGIC.len()];
            let probe_size = end.min(magic.len() as u64) as usize;
            self.log.read_exact_at(&mut magic[..probe_size], 0)?;
            if magic.starts_with(b"ZE") {
                if &magic != LIVE_MAGIC {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid live table format"));
                }
                (self.entries, offset) = table::read_prefix(&self.log, end, LIVE_MAGIC)?;
                self.log.seek(SeekFrom::Start(offset))?;
            }
        }
        let mut header = [0; HEADER_SIZE];
        let mut body = Vec::new();
        while end - offset >= HEADER_SIZE as u64 {
            self.log.read_exact(&mut header)?;
            let magic = &header[..4];
            let known_magic = magic == PUT_MAGIC || magic == DELETE_MAGIC || magic == BATCH_MAGIC;
            if !known_magic
                || read_u32(&header[16..20]) != crc32(&[&header[..16]])
            {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid WAL header"));
            }

            let key_size = read_u32(&header[4..8]) as usize;
            let value_size = read_u32(&header[8..12]) as usize;
            let valid_sizes = if magic == BATCH_MAGIC {
                key_size == 0 && (OP_HEADER_SIZE + 1..=MAX_BATCH_SIZE).contains(&value_size)
            } else {
                (1..=MAX_KEY_SIZE).contains(&key_size)
                    && value_size <= MAX_VALUE_SIZE
                    && (magic != DELETE_MAGIC || value_size == 0)
            };
            if !valid_sizes {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid WAL record size"));
            }
            let body_size = key_size + value_size;
            let record_size = (HEADER_SIZE + body_size) as u64;
            if end - offset < record_size {
                break;
            }

            body.resize(body_size, 0);
            self.log.read_exact(&mut body)?;
            if read_u32(&header[12..16]) != crc32(&[&body]) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid WAL payload checksum"));
            }
            if magic == BATCH_MAGIC {
                self.apply_batch(&decode_batch(&body)?, offset + HEADER_SIZE as u64);
            } else {
                let (key, value) = body.split_at(key_size);
                let operation = if magic == DELETE_MAGIC {
                    BatchOp::Delete(key)
                } else {
                    BatchOp::Put(key, value)
                };
                self.apply(&operation, offset + (HEADER_SIZE + key_size) as u64);
            }
            offset += record_size;
        }
        if offset != end {
            self.log.set_len(offset)?;
        }
        self.log.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

fn verify_log_path(path: &Path, log: &File) -> io::Result<()> {
    let named = fs::symlink_metadata(path)?;
    if !named.is_file() || named.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the log must be a regular file with one directory entry",
        ));
    }
    let opened = log.metadata()?;
    if (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
        return Err(io::Error::new(io::ErrorKind::WouldBlock, "log was replaced, retry open"));
    }
    Ok(())
}

fn create_temporary(path: &Path, purpose: &str) -> io::Result<(PathBuf, File)> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    loop {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let candidate = path.with_file_name(format!(".zeus-{purpose}-{}-{id}.tmp", std::process::id()));
        let result = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate);
        match result {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

fn write_record(log: &mut impl Write, magic: &[u8; 4], key: &[u8], value: &[u8]) -> io::Result<()> {
    log.write_all(&encode_header(magic, key, value))?;
    log.write_all(key)?;
    log.write_all(value)
}

fn validate_key(key: &[u8]) -> io::Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid key size"));
    }
    Ok(())
}

fn decode_batch(mut body: &[u8]) -> io::Result<Vec<BatchOp<'_>>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid WAL batch operation");
    let mut operations = Vec::new();
    while !body.is_empty() {
        if body.len() < OP_HEADER_SIZE || operations.len() == MAX_BATCH_OPERATIONS {
            return Err(invalid());
        }
        let kind = body[0];
        let key_size = read_u32(&body[1..5]) as usize;
        let value_size = read_u32(&body[5..9]) as usize;
        if !(1..=MAX_KEY_SIZE).contains(&key_size)
            || value_size > MAX_VALUE_SIZE
            || body.len() - OP_HEADER_SIZE < key_size + value_size
        {
            return Err(invalid());
        }

        let (key, rest) = body[OP_HEADER_SIZE..].split_at(key_size);
        let (value, remaining) = rest.split_at(value_size);
        operations.push(match kind {
            1 => BatchOp::Put(key, value),
            2 if value.is_empty() => BatchOp::Delete(key),
            _ => return Err(invalid()),
        });
        body = remaining;
    }
    Ok(operations)
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("WAL fields are four bytes"))
}

fn encode_header(magic: &[u8; 4], key: &[u8], value: &[u8]) -> [u8; HEADER_SIZE] {
    let mut header = [0; HEADER_SIZE];
    header[..4].copy_from_slice(magic);
    header[4..8].copy_from_slice(&(key.len() as u32).to_le_bytes());
    header[8..12].copy_from_slice(&(value.len() as u32).to_le_bytes());
    header[12..16].copy_from_slice(&crc32(&[key, value]).to_le_bytes());
    let header_crc = crc32(&[&header[..16]]);
    header[16..20].copy_from_slice(&header_crc.to_le_bytes());
    header
}

const CRC32_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb8_8320 * (crc & 1));
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = u32::MAX;
    for part in parts {
        for &byte in *part {
            crc = (crc >> 8) ^ CRC32_TABLE[(crc as u8 ^ byte) as usize];
        }
    }
    !crc
}
