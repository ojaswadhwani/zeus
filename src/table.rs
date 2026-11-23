use crate::{
    HEADER_SIZE, MAX_KEY_SIZE, MAX_VALUE_SIZE, PUT_MAGIC, crc32, create_temporary, read_u32,
    write_record,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::ops::Bound::{Excluded, Included};
use std::os::unix::fs::FileExt;
use std::path::Path;

pub(crate) const LIVE_MAGIC: &[u8; 8] = b"ZEUSDB01";
const TABLE_MAGIC: &[u8; 8] = b"ZEUSTBL1";
const TABLE_HEADER_SIZE: usize = 20;

pub(crate) type Index = BTreeMap<Vec<u8>, Location>;

pub(crate) struct Location {
    pub offset: u64,
    pub size: usize,
    pub checksum: u32,
}

/// Read-only checkpoint with an in-memory key index and disk-backed values.
pub struct SortedTable {
    file: File,
    index: Index,
}

impl SortedTable {
    /// Open and validate an immutable checkpoint written by Store::checkpoint.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let end = file.metadata()?.len();
        let (index, offset) = read_prefix(&file, end, TABLE_MAGIC)?;
        if offset != end {
            return Err(invalid("unexpected trailing sorted table data"));
        }
        Ok(Self { file, index })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.index.get(key).map(|location| read_value(&self.file, key, location)).transpose()
    }

    /// Read values on demand for keys in the half-open bytewise interval [start, end).
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = io::Result<(&[u8], Vec<u8>)>> {
        (start < end)
            .then(|| self.index.range::<[u8], _>((Included(start), Excluded(end))))
            .into_iter()
            .flatten()
            .map(|(key, location)| read_value(&self.file, key, location).map(|value| (key.as_slice(), value)))
    }

}

pub(crate) fn read_prefix(file: &File, end: u64, magic: &[u8; 8]) -> io::Result<(Index, u64)> {
    let mut reader = BufReader::new(file);
    if end < TABLE_HEADER_SIZE as u64 {
        return Err(invalid("truncated sorted table"));
    }
    let mut table_header = [0; TABLE_HEADER_SIZE];
    reader.read_exact(&mut table_header)?;
    if &table_header[..8] != magic
        || read_u32(&table_header[16..]) != crc32(&[&table_header[..16]])
    {
        return Err(invalid("invalid sorted table format"));
    }
    let count = u64::from_le_bytes(table_header[8..16].try_into().expect("eight-byte count"));

    let mut offset = TABLE_HEADER_SIZE as u64;
    let mut index = BTreeMap::<Vec<u8>, Location>::new();
    let mut body = Vec::new();
    for _ in 0..count {
        if end - offset < HEADER_SIZE as u64 {
            return Err(invalid("truncated sorted table header"));
        }
        let mut header = [0; HEADER_SIZE];
        reader.read_exact(&mut header)?;
        if &header[..4] != PUT_MAGIC || read_u32(&header[16..20]) != crc32(&[&header[..16]]) {
            return Err(invalid("invalid sorted table record header"));
        }

        let key_size = read_u32(&header[4..8]) as usize;
        let size = read_u32(&header[8..12]) as usize;
        if !(1..=MAX_KEY_SIZE).contains(&key_size) || size > MAX_VALUE_SIZE {
            return Err(invalid("invalid sorted table record size"));
        }
        let body_size = key_size + size;
        if end - offset - (HEADER_SIZE as u64) < body_size as u64 {
            return Err(invalid("truncated sorted table payload"));
        }

        body.resize(body_size, 0);
        reader.read_exact(&mut body)?;
        let checksum = read_u32(&header[12..16]);
        if checksum != crc32(&[&body]) {
            return Err(invalid("invalid sorted table payload checksum"));
        }
        let key = &body[..key_size];
        if index.last_key_value().is_some_and(|(previous, _)| previous.as_slice() >= key) {
            return Err(invalid("sorted table keys are out of order or duplicated"));
        }

        let value_offset = offset + (HEADER_SIZE + key_size) as u64;
        index.insert(key.to_vec(), Location { offset: value_offset, size, checksum });
        offset = value_offset + size as u64;
    }
    Ok((index, offset))
}

pub(crate) fn read_value(file: &File, key: &[u8], location: &Location) -> io::Result<Vec<u8>> {
    let mut value = vec![0; location.size];
    file.read_exact_at(&mut value, location.offset)?;
    if crc32(&[key, &value]) != location.checksum {
        return Err(invalid("invalid sorted table payload checksum"));
    }
    Ok(value)
}

pub(crate) fn publish(path: &Path, source: &File, entries: &Index) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "expected a checkpoint filename"))?;
    let parent = parent.canonicalize()?;
    let path = parent.join(name);
    let directory = File::open(parent)?;
    let (temporary_path, mut file) = create_temporary(&path, "checkpoint")?;

    let publication = (|| {
        write_prefix(&mut file, source, entries, TABLE_MAGIC)?;
        file.sync_all()?;
        fs::hard_link(&temporary_path, &path)
    })();

    let cleanup = fs::remove_file(&temporary_path);
    publication?;
    let sync = directory.sync_all();
    sync?;
    cleanup
}

pub(crate) fn write_prefix(
    destination: &mut File,
    source: &File,
    entries: &Index,
    magic: &[u8; 8],
) -> io::Result<()> {
    let mut writer = BufWriter::new(destination);
    let mut header = [0; TABLE_HEADER_SIZE];
    header[..8].copy_from_slice(magic);
    header[8..16].copy_from_slice(&(entries.len() as u64).to_le_bytes());
    let checksum = crc32(&[&header[..16]]);
    header[16..].copy_from_slice(&checksum.to_le_bytes());
    writer.write_all(&header)?;
    for (key, location) in entries {
        let value = read_value(source, key, location)?;
        write_record(&mut writer, PUT_MAGIC, key, &value)?;
    }
    writer.flush()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
