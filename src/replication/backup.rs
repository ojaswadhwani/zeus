use super::{APPLIED_INDEX, HARD_STATE, LAST_INDEX, MEMBERS, SNAPSHOT, snapshot};
use protobuf::Message as _;
use raft::eraftpb::{ConfState, HardState, Snapshot};
use std::fs;
use std::path::{Path, PathBuf};
use zeus::{BatchOp, MAX_KEY_SIZE, SortedTable, Store};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const MAX_CERTIFICATE: usize = 4096;
const ROW_END: [u8; MAX_KEY_SIZE + 1] = [u8::MAX; MAX_KEY_SIZE + 1];

pub(crate) fn cluster_identity(certificates: [&[u8]; 3]) -> Result<Vec<u8>> {
    let [first, second, third] = certificates;
    if first == second || first == third || second == third {
        return Err("configure one distinct leaf certificate per voter".into());
    }
    let mut cluster = Vec::new();
    for certificate in certificates {
        if certificate.len() > MAX_CERTIFICATE {
            return Err("peer leaf certificate exceeds 4096 bytes".into());
        }
        cluster.extend_from_slice(&(certificate.len() as u32).to_be_bytes());
        cluster.extend_from_slice(certificate);
    }
    Ok(cluster)
}

pub(crate) fn create(id: u64, root: &Path, destination: &Path) -> Result<()> {
    let (raft_path, data_path) = voter_paths(id, root)?;
    // Match the server's lock order and keep both stores locked until publication.
    raft_path.metadata()?;
    let _log = Store::open(raft_path)?;
    data_path.metadata()?;
    let data = Store::open(data_path)?;
    // Reserve space for any valid set of destination voter certificates.
    let cluster = [0; MEMBERS.len() * (4 + MAX_CERTIFICATE)];
    let index = applied_index(data.get(APPLIED_INDEX)?)?;
    make_snapshot(data.scan(b"", &ROW_END), &cluster, index)?;
    data.checkpoint(destination)?;
    Ok(())
}

pub(crate) fn restore(id: u64, root: &Path, source: &Path, cluster: &[u8]) -> Result<()> {
    let (raft_path, data_path) = voter_paths(id, root)?;
    let table = SortedTable::open(source)?;
    let index = applied_index(table.get(APPLIED_INDEX)?)?;
    let image = make_snapshot(table.scan(b"", &ROW_END), cluster, index)?;
    if fs::read_dir(root)?.next().is_some() {
        return Err("restore requires a fresh voter directory".into());
    }
    let mut log = Store::open(raft_path)?;
    if data_path.symlink_metadata().is_ok() || log.scan(b"", &ROW_END).next().is_some() {
        return Err("restore requires a fresh voter directory".into());
    }

    // The normal startup path installs the state machine from this durable seed.
    let reference = snapshot::save(&data_path, &image)?.write_to_bytes()?;
    let state = HardState {
        term: 1,
        commit: index,
        ..HardState::default()
    }
    .write_to_bytes()?;
    log.write_batch(&[
        BatchOp::Put(SNAPSHOT, &reference),
        BatchOp::Put(HARD_STATE, &state),
        BatchOp::Put(LAST_INDEX, &index.to_be_bytes()),
    ])?;
    Ok(())
}

fn voter_paths(id: u64, root: &Path) -> Result<(PathBuf, PathBuf)> {
    if !MEMBERS.contains(&id) {
        return Err("unknown voter ID".into());
    }
    Ok((
        root.join(format!("{id}.raft")),
        root.join(format!("{id}.data")),
    ))
}

fn applied_index(value: Option<Vec<u8>>) -> Result<u64> {
    let value = value.ok_or("missing backup applied index")?;
    let index = u64::from_be_bytes(value.as_slice().try_into()?);
    if index == 0 || index == u64::MAX {
        return Err("invalid backup applied index".into());
    }
    Ok(index)
}

fn make_snapshot<'a>(
    rows: impl Iterator<Item = std::io::Result<(&'a [u8], Vec<u8>)>>,
    cluster: &[u8],
    index: u64,
) -> Result<Snapshot> {
    let data =
        snapshot::encode(rows, cluster)?.ok_or("backup exceeds the 16 MiB snapshot limit")?;
    let mut image = Snapshot::default();
    image.set_data(data.into());
    let metadata = image.mut_metadata();
    metadata.index = index;
    metadata.term = 1;
    let mut conf = ConfState::default();
    conf.set_voters(MEMBERS.to_vec());
    metadata.set_conf_state(conf);
    snapshot::validate(&image, cluster)?;
    Ok(image)
}
