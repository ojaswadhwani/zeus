use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use zeus::Store;

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> io::Result<Self> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "zeus-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(name);
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// Produced independently using IEEE CRC-32 and the documented ZWL1 layout.
const RECORD: &[u8] = b"ZWL1\x03\x00\x00\x00\x05\x00\x00\x00\xf7\x78\xc1\x22\xca\xec\xc6\x0dkeysaved";

#[test]
fn reads_and_writes_the_documented_log_format() -> io::Result<()> {
    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    fs::write(&path, RECORD)?;
    let store = Store::open(&path)?;
    assert_eq!(store.get(b"key")?, Some(b"saved".to_vec()));
    assert_eq!(Store::open(&path).err().unwrap().kind(), io::ErrorKind::WouldBlock);
    drop(store);

    let second = dir.0.join("new.wal");
    Store::open(&second)?.put(b"key", b"saved")?;
    assert_eq!(fs::read(second)?, RECORD);
    Ok(())
}

#[test]
fn truncates_an_incomplete_tail_before_the_next_append() -> io::Result<()> {
    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    for tail in [&RECORD[..5], &RECORD[..21]] {
        fs::write(&path, RECORD)?;
        OpenOptions::new().append(true).open(&path)?.write_all(tail)?;
        let mut store = Store::open(&path)?;
        assert_eq!(store.get(b"key")?, Some(b"saved".to_vec()));
        assert_eq!(fs::metadata(&path)?.len(), RECORD.len() as u64);
        store.put(b"key", b"new")?;
        drop(store);
        assert_eq!(Store::open(&path)?.get(b"key")?, Some(b"new".to_vec()));
    }
    Ok(())
}

#[test]
fn rejects_corrupt_complete_records_without_truncating() -> io::Result<()> {
    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    for index in [4, RECORD.len() - 1] {
        let mut damaged = RECORD.to_vec();
        damaged[index] ^= 1;
        fs::write(&path, &damaged)?;
        assert_eq!(Store::open(&path).err().unwrap().kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&path)?, damaged);
    }
    Ok(())
}

#[test]
fn acknowledged_write_survives_sigkill() -> io::Result<()> {
    const CHILD_LOG: &str = "ZEUS_TEST_CHILD_LOG";
    if let Some(path) = std::env::var_os(CHILD_LOG) {
        let mut store = Store::open(path)?;
        store.put(b"checkpoint", b"saved")?;
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "acknowledged")?;
        stdout.flush()?;
        loop {
            std::thread::park();
        }
    }

    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", "acknowledged_write_survives_sigkill", "--nocapture"])
        .env(CHILD_LOG, &path)
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = BufReader::new(child.stdout.take().expect("child stdout was piped"));
    let acknowledged = stdout.lines().find_map(|line| match line {
        Ok(line) if line.trim() == "acknowledged" => Some(Ok(())),
        Ok(_) => None,
        Err(error) => Some(Err(error)),
    });
    let killed = child.kill();
    let status = child.wait()?;
    acknowledged.expect("child exited before acknowledging")?;
    killed?;
    assert_eq!(status.signal(), Some(9));
    assert_eq!(Store::open(&path)?.get(b"checkpoint")?, Some(b"saved".to_vec()));
    Ok(())
}

#[test]
fn ordered_scans_and_deletes_survive_replay() -> io::Result<()> {
    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    fs::write(&path, RECORD)?;
    {
        let mut store = Store::open(&path)?;
        store.put(b"z", b"upper bound")?;
        store.put(b"b", b"second")?;
        store.put(b"a", b"")?;
        assert!(store.delete(b"key")?);
        assert!(!store.delete(b"key")?);
        assert!(store.get(b"key")?.is_none());
    }
    let mut store = Store::open(&path)?;
    assert!(store.get(b"key")?.is_none());
    assert_eq!(
        store.scan(b"a", b"z").collect::<io::Result<Vec<_>>>()?,
        vec![(b"a".as_slice(), Vec::new()), (b"b".as_slice(), b"second".to_vec())]
    );
    assert_eq!(store.scan(b"z", b"a").count(), 0);
    assert_eq!(store.scan(b"a", b"a").count(), 0);
    store.put(b"key", b"restored")?;
    drop(store);
    assert_eq!(Store::open(path)?.get(b"key")?, Some(b"restored".to_vec()));
    Ok(())
}

#[test]
fn batch_recovery_applies_every_operation_or_none() -> io::Result<()> {
    use zeus::BatchOp::{Delete, Put};

    let dir = TestDir::new()?;
    let full_path = dir.0.join("complete.wal");
    fs::write(&full_path, RECORD)?;
    {
        let mut store = Store::open(&full_path)?;
        store.write_batch(&[Put(b"new", b"first"), Delete(b"key"), Put(b"new", b"last")])?;
        assert!(store.get(b"key")?.is_none());
        assert_eq!(store.get(b"new")?, Some(b"last".to_vec()));
    }

    let log = fs::read(&full_path)?;
    let path = dir.0.join("torn.wal");
    for end in RECORD.len()..log.len() {
        fs::write(&path, &log[..end])?;
        let store = Store::open(&path)?;
        assert_eq!(store.get(b"key")?, Some(b"saved".to_vec()));
        assert!(store.get(b"new")?.is_none());
        assert_eq!(fs::metadata(&path)?.len(), RECORD.len() as u64);
    }

    let store = Store::open(&full_path)?;
    assert!(store.get(b"key")?.is_none());
    assert_eq!(store.get(b"new")?, Some(b"last".to_vec()));
    Ok(())
}

#[test]
fn rejects_invalid_batches_before_mutating_the_log() -> io::Result<()> {
    use zeus::BatchOp::{Delete, Put};

    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    fs::write(&path, RECORD)?;
    let mut store = Store::open(&path)?;
    let error = store.write_batch(&[Delete(b"key"), Put(b"", b"bad")]).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(store.get(b"key")?, Some(b"saved".to_vec()));
    assert_eq!(fs::read(&path)?, RECORD);

    store.write_batch(&[])?;
    assert_eq!(fs::read(&path)?, RECORD);
    Ok(())
}

#[test]
fn compaction_keeps_live_data_and_allows_further_writes() -> io::Result<()> {
    use zeus::BatchOp::{Delete, Put};

    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    fs::write(&path, RECORD)?;
    {
        let mut store = Store::open(&path)?;
        store.put(b"old", b"deleted")?;
        store.write_batch(&[Put(b"key", b"overwritten"), Delete(b"old")])?;
        store.put(b"key", b"saved")?;
        assert!(fs::metadata(&path)?.len() > RECORD.len() as u64);

        store.compact()?;
        let compacted = fs::read(&path)?;
        assert_eq!(&compacted[..8], b"ZEUSDB01");
        assert_eq!(&compacted[20..], RECORD);
        assert_eq!(
            Store::open(&path).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        store.put(b"new", b"later")?;
    }
    let mut store = Store::open(&path)?;
    assert_eq!(store.get(b"key")?, Some(b"saved".to_vec()));
    assert_eq!(store.get(b"new")?, Some(b"later".to_vec()));
    assert!(store.get(b"old")?.is_none());
    store.delete(b"key")?;
    store.delete(b"new")?;
    store.compact()?;
    assert_eq!(fs::metadata(&path)?.len(), 20);
    drop(store);
    assert!(Store::open(&path)?.get(b"key")?.is_none());
    Ok(())
}

#[test]
fn checkpoint_is_sorted_independent_and_never_overwrites() -> io::Result<()> {
    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    let destination = dir.0.join("checkpoint.table");
    let mut store = Store::open(&path)?;
    store.put(b"z", b"outside")?;
    store.put(b"b", b"disk")?;
    store.put(b"a", b"")?;
    store.put(b"old", b"discarded")?;
    store.delete(b"old")?;
    store.checkpoint(&destination)?;
    store.put(b"b", b"new")?;
    assert_eq!(
        store.checkpoint(&destination).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(store.checkpoint(&path).unwrap_err().kind(), io::ErrorKind::AlreadyExists);

    let table = zeus::SortedTable::open(&destination)?;
    assert_eq!(table.get(b"b")?, Some(b"disk".to_vec()));
    assert_eq!(table.get(b"old")?, None);
    assert_eq!(
        table.scan(b"a", b"z").collect::<io::Result<Vec<_>>>()?,
        vec![(b"a".as_slice(), Vec::new()), (b"b".as_slice(), b"disk".to_vec())]
    );
    assert_eq!(table.scan(b"z", b"a").count(), 0);
    Ok(())
}

#[test]
fn checkpoint_rejects_a_truncated_record() -> io::Result<()> {
    let dir = TestDir::new()?;
    let mut store = Store::open(dir.0.join("zeus.wal"))?;
    let destination = dir.0.join("checkpoint.table");
    store.put(b"key", b"saved")?;
    store.checkpoint(&destination)?;
    let length = fs::metadata(&destination)?.len();
    let file = OpenOptions::new().write(true).open(&destination)?;
    for cutoff in [length - 1, length - RECORD.len() as u64] {
        file.set_len(cutoff)?;
        assert_eq!(
            zeus::SortedTable::open(&destination).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
    }
    Ok(())
}

#[test]
fn compacted_base_and_wal_tail_recover_independently() -> io::Result<()> {
    use zeus::BatchOp::{Delete, Put};

    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    let base_size;
    {
        let mut store = Store::open(&path)?;
        store.put(b"base", b"original")?;
        store.compact()?;
        base_size = fs::metadata(&path)?.len();
        store.write_batch(&[Delete(b"base"), Put(b"tail", b"updated")])?;
        assert_eq!(store.get(b"tail")?, Some(b"updated".to_vec()));
    }
    let file = OpenOptions::new().write(true).open(&path)?;
    let full_size = file.metadata()?.len();
    assert_eq!(Store::open(&path)?.get(b"tail")?, Some(b"updated".to_vec()));
    file.set_len(full_size - 1)?;
    {
        let store = Store::open(&path)?;
        assert_eq!(store.get(b"base")?, Some(b"original".to_vec()));
        assert_eq!(store.get(b"tail")?, None);
        assert_eq!(fs::metadata(&path)?.len(), base_size);
    }
    file.set_len(base_size - 1)?;
    assert_eq!(Store::open(&path).err().unwrap().kind(), io::ErrorKind::InvalidData);
    assert_eq!(fs::metadata(&path)?.len(), base_size - 1);
    Ok(())
}

#[test]
fn failed_append_and_compaction_preserve_acknowledged_data() -> io::Result<()> {
    let dir = TestDir::new()?;
    let path = dir.0.join("zeus.wal");
    fs::write(&path, RECORD)?;
    let cli = env!("CARGO_BIN_EXE_zeus-local");
    // Linux RLIMIT_FSIZE forces a short write followed by EFBIG. Ignore SIGXFSZ
    // so the kernel error reaches the engine and CLI instead of killing the process.
    let large_path = dir.0.join("large.wal");
    let value = vec![b'x'; 1500];
    Store::open(&large_path)?.put(b"key", &value)?;
    let original = fs::read(&large_path)?;
    let out = Command::new("bash")
        .args(["-c", "trap '' XFSZ; ulimit -f 1; exec \"$@\"", "zeus-test", cli])
        .arg(&large_path)
        .arg("compact")
        .output()?;
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(fs::read(&large_path)?, original);

    let out = Command::new("bash")
        .args(["-c", "trap '' XFSZ; ulimit -f 1; exec \"$@\"", "zeus-test", cli])
        .arg(&path)
        .args(["put", "oversized", &"x".repeat(1500)])
        .output()?;
    assert_eq!(out.status.code(), Some(1));
    let store = Store::open(&path)?;
    assert_eq!(store.get(b"key")?, Some(b"saved".to_vec()));
    assert_eq!(store.get(b"oversized")?, None);
    Ok(())
}
