use protobuf::Message as _;
use raft::eraftpb::{Entry, HardState, Message, Snapshot};
use raft::{Config, RawNode, Ready, StateRole, Storage};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use zeus::{BatchOp, Store};

#[path = "replication/snapshot.rs"]
mod snapshot;
use snapshot::RaftStorage;

#[cfg_attr(test, allow(dead_code))]
#[path = "replication/backup.rs"]
pub(crate) mod backup;

pub(crate) const MEMBERS: [u64; 3] = [1, 2, 3];
pub(crate) const LAST_INDEX: &[u8] = b"last-index";
pub(crate) const APPLIED_INDEX: &[u8] = b"applied-index";
const HARD_STATE: &[u8] = b"hard-state";
const SNAPSHOT: &[u8] = b"snapshot-image";
const BATCH_MAGIC: &[u8] = b"ZB01";
pub(crate) const MAX_SNAPSHOT_IMAGE: usize = 16 * 1024 * 1024;

pub(crate) struct Node {
    pub(crate) raft: RawNode<RaftStorage>,
    pub(crate) log: Store,
    pub(crate) data: Store,
    pub(crate) replies: Vec<(u64, WriteReply)>,
    pub(crate) unapplied: Vec<Entry>,
    pub(crate) pause_application: bool,
    next_read: u64,
    pub(crate) read: Option<PendingRead>,
    data_path: PathBuf,
    cluster: Vec<u8>,
    snapshot_attempt: u64,
    pub(crate) snapshot_limit_blocked: bool,
}

pub(crate) struct PendingRead {
    term: u64,
    query: ReadQuery,
    context: [u8; 8],
    sent: bool,
    pub(crate) barrier: Option<u64>,
}

enum ReadQuery {
    Barrier,
    Point(Vec<u8>),
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        max_bytes: usize,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReadReply {
    Pending,
    NotLeader,
    Confirmed,
    Value(Option<Vec<u8>>),
    Rows {
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        more: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteReply {
    Applied,
    CommandMismatch,
    OutOfSequence,
}

impl Node {
    pub(crate) fn open_with_config(root: &Path, mut config: Config, cluster: Vec<u8>) -> Self {
        let id = config.id;
        let log = Store::open(root.join(format!("{id}.raft"))).unwrap();
        let data_path = root.join(format!("{id}.data"));
        let mut data = Store::open(&data_path).unwrap();
        let image = log
            .get(SNAPSHOT)
            .unwrap()
            .map_or_else(Snapshot::default, |bytes| {
                let record = Snapshot::parse_from_bytes(&bytes).unwrap();
                snapshot::load(&data_path, record).unwrap()
            });
        let index = image.get_metadata().index;
        if !image.is_empty() {
            let rows = snapshot::validate(&image, &cluster).unwrap();
            if stored_index(&data, APPLIED_INDEX) < index {
                data = snapshot::activate(&data_path, &rows, index).unwrap();
            }
        }
        snapshot::prune(&data_path, &image).unwrap();
        let storage = RaftStorage::new(image, &read_entries(&log, index), read_hard_state(&log));
        config.applied = stored_index(&data, APPLIED_INDEX);
        config.read_only_option = raft::ReadOnlyOption::Safe;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raft = RawNode::new(&config, storage, &logger).unwrap();
        Self {
            raft,
            log,
            data,
            replies: Vec::new(),
            unapplied: Vec::new(),
            pause_application: false,
            next_read: 0,
            read: None,
            data_path,
            cluster,
            snapshot_attempt: index,
            snapshot_limit_blocked: false,
        }
    }

    pub(crate) fn persist_ready(&mut self, ready: &Ready) {
        let mut records = Vec::new();
        let image = ready.snapshot();
        if !image.is_empty() {
            snapshot::validate(image, &self.cluster).unwrap();
            let reference = snapshot::save(&self.data_path, image).unwrap();
            records.push((SNAPSHOT.to_vec(), reference.write_to_bytes().unwrap()));
        }
        if let Some(state) = ready.hs() {
            records.push((HARD_STATE.to_vec(), state.write_to_bytes().unwrap()));
        }
        if let Some(last) = ready.entries().last() {
            records.push((LAST_INDEX.to_vec(), last.index.to_be_bytes().to_vec()));
        } else if !image.is_empty() {
            records.push((
                LAST_INDEX.to_vec(),
                image.get_metadata().index.to_be_bytes().to_vec(),
            ));
        }
        // LAST_INDEX hides any old suffix when Raft replaces it with a shorter one.
        for entry in ready.entries() {
            records.push((
                entry.index.to_be_bytes().to_vec(),
                entry.write_to_bytes().unwrap(),
            ));
        }
        let batch: Vec<_> = records
            .iter()
            .map(|(key, value)| BatchOp::Put(key, value))
            .collect();
        self.log.write_batch(&batch).unwrap();
    }

    pub(crate) fn finish(&mut self, mut ready: Ready, outbound: &mut VecDeque<Message>) {
        self.persist_ready(&ready);
        if ready.snapshot().is_empty() {
            let mut memory = self.raft.mut_store().write();
            memory.append(ready.entries()).unwrap();
            if let Some(state) = ready.hs() {
                memory.set_hardstate(state.clone());
            }
        } else {
            let image = ready.snapshot();
            let index = image.get_metadata().index;
            let rows = snapshot::validate(image, &self.cluster).unwrap();
            self.data = snapshot::activate(&self.data_path, &rows, index).unwrap();
            snapshot::prune(&self.data_path, image).unwrap();
            self.unapplied.clear();
            self.snapshot_attempt = index;
            self.snapshot_limit_blocked = false;
            *self.raft.mut_store() =
                RaftStorage::new(image.clone(), ready.entries(), read_hard_state(&self.log));
            self.trim_log(index);
        }
        outbound.extend(ready.take_messages());
        outbound.extend(ready.take_persisted_messages());
        for confirmed in ready.take_read_states() {
            if let Some(read) = &mut self.read {
                if confirmed.request_ctx == read.context {
                    read.barrier = Some(confirmed.index);
                }
            }
        }
        self.unapplied.extend(ready.take_committed_entries());
        let mut light = self.raft.advance_append(ready);
        if let Some(index) = light.commit_index() {
            let mut state = read_hard_state(&self.log);
            state.commit = index;
            self.log
                .put(HARD_STATE, &state.write_to_bytes().unwrap())
                .unwrap();
            self.raft.mut_store().write().set_hardstate(state);
        }
        outbound.extend(light.take_messages());
        self.unapplied.extend(light.take_committed_entries());
        if !self.pause_application {
            self.apply_pending();
        }
        self.send_read_index();
    }

    pub(crate) fn apply_pending(&mut self) {
        for entry in std::mem::take(&mut self.unapplied) {
            let index = entry.index.to_be_bytes();
            if entry.data.is_empty() {
                self.data.put(APPLIED_INDEX, &index).unwrap();
                continue;
            }
            let (request, rest) = entry.context.split_first_chunk::<8>().unwrap();
            let (sequence, client) = rest.split_first_chunk::<8>().unwrap();
            let request = u64::from_be_bytes(*request);
            let sequence = u64::from_be_bytes(*sequence);
            let reply = Self::apply_mutation(&mut self.data, client, sequence, &entry.data, &index);
            if self.raft.raft.state == StateRole::Leader && entry.term == self.raft.raft.term {
                self.replies.push((request, reply));
            }
        }
        self.raft
            .advance_apply_to(stored_index(&self.data, APPLIED_INDEX));
    }

    pub(crate) fn maybe_snapshot(&mut self, interval: u64) {
        let index = stored_index(&self.data, APPLIED_INDEX);
        if index.saturating_sub(self.snapshot_attempt) < interval {
            return;
        }
        self.snapshot_attempt = index;
        let Some(data) = snapshot::export(&self.data, &self.cluster).unwrap() else {
            self.snapshot_limit_blocked = true;
            return;
        };
        let storage = self.raft.store();
        let mut image = Snapshot::default();
        image.set_data(data.into());
        let metadata = image.mut_metadata();
        metadata.index = index;
        metadata.term = storage.term(index).unwrap();
        metadata.set_conf_state(storage.initial_state().unwrap().conf_state);
        snapshot::validate(&image, &self.cluster).unwrap();
        let reference = snapshot::save(&self.data_path, &image).unwrap();
        self.log
            .put(SNAPSHOT, &reference.write_to_bytes().unwrap())
            .unwrap();
        snapshot::prune(&self.data_path, &image).unwrap();
        let entries = read_entries(&self.log, index);
        *self.raft.mut_store() = RaftStorage::new(image, &entries, read_hard_state(&self.log));
        self.trim_log(index);
        self.snapshot_limit_blocked = false;
    }

    pub(crate) fn valid_snapshot(&self, image: &Snapshot) -> bool {
        snapshot::validate(image, &self.cluster).is_ok()
    }

    fn trim_log(&mut self, index: u64) {
        let last = stored_index(&self.log, LAST_INDEX);
        let obsolete: Vec<Vec<u8>> = self
            .log
            .scan(b"", &[0xff; 9])
            .filter_map(|record| {
                let (key, _) = record.unwrap();
                let encoded: [u8; 8] = key.try_into().ok()?;
                let entry = u64::from_be_bytes(encoded);
                (entry <= index || entry > last).then(|| key.to_vec())
            })
            .collect();
        for chunk in obsolete.chunks(zeus::MAX_BATCH_OPERATIONS) {
            let batch: Vec<_> = chunk.iter().map(|key| BatchOp::Delete(key)).collect();
            self.log.write_batch(&batch).unwrap();
        }
        self.log.compact().unwrap();
    }

    pub(crate) fn apply_mutation(
        data: &mut Store,
        client: &[u8],
        sequence: u64,
        command: &[u8],
        index: &[u8; 8],
    ) -> WriteReply {
        let retry_key = retry_key(client);
        let previous = data.get(&retry_key).unwrap();
        let previous_sequence = previous.as_ref().map_or(0, |record| {
            u64::from_be_bytes(record[..8].try_into().unwrap())
        });
        if previous_sequence.checked_add(1) == Some(sequence) {
            // Retaining the exact command makes retries compare byte for byte.
            let retry_record = [sequence.to_be_bytes().as_slice(), command].concat();
            let mut batch = decode_batch(command);
            batch.push(BatchOp::Put(&retry_key, &retry_record));
            batch.push(BatchOp::Put(APPLIED_INDEX, index));
            data.write_batch(&batch).unwrap();
            return WriteReply::Applied;
        }
        data.put(APPLIED_INDEX, index).unwrap();
        match previous {
            Some(record) if sequence == previous_sequence => {
                if &record[8..] == command {
                    WriteReply::Applied
                } else {
                    WriteReply::CommandMismatch
                }
            }
            _ => WriteReply::OutOfSequence,
        }
    }

    pub(crate) fn submit_authenticated(
        &mut self,
        request: u64,
        client: &[u8],
        sequence: u64,
        operations: &[BatchOp<'_>],
    ) -> raft::Result<()> {
        assert!(!operations.is_empty());
        let mut command = Vec::new();
        let tagged = operations.iter().any(|op| matches!(op, BatchOp::Delete(_)));
        if tagged {
            command.extend_from_slice(BATCH_MAGIC);
        }
        for operation in operations {
            let (kind, key, value) = match *operation {
                BatchOp::Put(key, value) => (1, key, Some(value)),
                BatchOp::Delete(key) => (2, key, None),
            };
            assert!(key.starts_with(b"client/"));
            if tagged {
                command.push(kind);
            }
            for bytes in std::iter::once(key).chain(value) {
                command.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_be_bytes());
                command.extend_from_slice(bytes);
            }
        }
        let context = [
            request.to_be_bytes().as_slice(),
            &sequence.to_be_bytes(),
            client,
        ]
        .concat();
        self.raft.propose(context, command)
    }

    pub(crate) fn start_read(&mut self, key: &[u8]) -> ReadReply {
        assert!(key.starts_with(b"client/"));
        self.start_query(ReadQuery::Point(key.to_vec()))
    }

    pub(crate) fn start_read_barrier(&mut self) -> ReadReply {
        self.start_query(ReadQuery::Barrier)
    }

    pub(crate) fn start_scan(
        &mut self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
        max_bytes: usize,
    ) -> ReadReply {
        assert!(start.starts_with(b"client/") && end.starts_with(b"client/"));
        self.start_query(ReadQuery::Scan {
            start,
            end,
            limit,
            max_bytes,
        })
    }

    fn start_query(&mut self, query: ReadQuery) -> ReadReply {
        assert!(self.read.is_none());
        if self.raft.raft.state != StateRole::Leader {
            return ReadReply::NotLeader;
        }
        // raft-rs drops ReadIndex requests before a current-term entry commits.
        // Retain the request locally so it can start when that condition is met.
        self.next_read += 1;
        self.read = Some(PendingRead {
            term: self.raft.raft.term,
            query,
            context: self.next_read.to_be_bytes(),
            sent: false,
            barrier: None,
        });
        self.send_read_index();
        ReadReply::Pending
    }

    fn send_read_index(&mut self) {
        if let Some(read) = &mut self.read {
            if self.raft.raft.state == StateRole::Leader
                && read.term == self.raft.raft.term
                && !read.sent
                && self.raft.raft.commit_to_current_term()
            {
                read.sent = true;
                self.raft.read_index(read.context.to_vec());
            }
        }
    }

    pub(crate) fn poll_read(&mut self) -> ReadReply {
        let read = self.read.as_ref().expect("read requested");
        if self.raft.raft.state != StateRole::Leader || read.term != self.raft.raft.term {
            self.cancel_read();
            return ReadReply::NotLeader;
        }
        if read
            .barrier
            .is_some_and(|index| stored_index(&self.data, APPLIED_INDEX) >= index)
        {
            let read = self.read.take().unwrap();
            return match read.query {
                ReadQuery::Barrier => ReadReply::Confirmed,
                ReadQuery::Point(key) => ReadReply::Value(self.data.get(&key).unwrap()),
                ReadQuery::Scan {
                    start,
                    end,
                    limit,
                    max_bytes,
                } => {
                    let mut rows = Vec::new();
                    let mut bytes = 0;
                    for row in self.data.scan(&start, &end) {
                        let (key, value) = row.unwrap();
                        let key = key.strip_prefix(b"client/").unwrap();
                        bytes += 4 + key.len() + value.len();
                        if rows.len() == limit || bytes > max_bytes {
                            return ReadReply::Rows { rows, more: true };
                        }
                        rows.push((key.to_vec(), value));
                    }
                    ReadReply::Rows { rows, more: false }
                }
            };
        }
        ReadReply::Pending
    }

    pub(crate) fn cancel_read(&mut self) {
        self.read = None;
    }
}

fn read_entries(log: &Store, after: u64) -> Vec<Entry> {
    (after + 1..=stored_index(log, LAST_INDEX))
        .map(|index| {
            let record = log.get(&index.to_be_bytes()).unwrap().unwrap();
            Entry::parse_from_bytes(&record).unwrap()
        })
        .collect()
}

pub(crate) fn retry_key(client: &[u8]) -> Vec<u8> {
    [b"retry/".as_slice(), client].concat()
}

fn decode_batch(mut command: &[u8]) -> Vec<BatchOp<'_>> {
    fn take_field<'a>(command: &mut &'a [u8]) -> &'a [u8] {
        let (length, remainder) = command.split_first_chunk::<4>().unwrap();
        let (field, remainder) = remainder.split_at(u32::from_be_bytes(*length) as usize);
        *command = remainder;
        field
    }

    let tagged = if let Some(rest) = command.strip_prefix(BATCH_MAGIC) {
        command = rest;
        true
    } else {
        false
    };
    let mut operations = Vec::new();
    while !command.is_empty() {
        let kind = if tagged {
            let (&kind, rest) = command.split_first().unwrap();
            command = rest;
            kind
        } else {
            1
        };
        let key = take_field(&mut command);
        operations.push(match kind {
            1 => BatchOp::Put(key, take_field(&mut command)),
            2 => BatchOp::Delete(key),
            _ => panic!("invalid replicated batch operation"),
        });
    }
    operations
}

pub(crate) fn stored_index(store: &Store, key: &[u8]) -> u64 {
    store
        .get(key)
        .unwrap()
        .map_or(0, |bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
}

pub(crate) fn read_hard_state(store: &Store) -> HardState {
    store
        .get(HARD_STATE)
        .unwrap()
        .map_or_else(HardState::default, |bytes| {
            HardState::parse_from_bytes(&bytes).unwrap()
        })
}
