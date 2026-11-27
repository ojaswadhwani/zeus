use raft::eraftpb::{Message, MessageType};
use raft::{Config, Ready, StateRole};
use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use zeus::{BatchOp, Store};

#[path = "replication/process.rs"]
mod process;

#[path = "../src/replication.rs"]
mod replication;
use replication::{
    APPLIED_INDEX, LAST_INDEX, MEMBERS, Node, ReadReply, WriteReply, read_hard_state, retry_key,
    stored_index,
};

const KEY: &[u8] = b"client/key";
const OTHER_KEY: &[u8] = b"client/other";

impl Node {
    fn open(root: &Path, id: u64) -> Self {
        Self::open_with_config(
            root,
            Config {
                id,
                ..Config::default()
            },
            Vec::new(),
        )
    }

    fn submit(&mut self, request: u64, client: u64, sequence: u64, puts: &[(&[u8], &[u8])]) {
        let operations: Vec<_> = puts
            .iter()
            .map(|(key, value)| BatchOp::Put(key, value))
            .collect();
        self.submit_authenticated(request, &client.to_be_bytes(), sequence, &operations)
            .unwrap();
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new(scenario: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "zeus-replication-{}-{scenario}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Cluster {
    nodes: [Option<Node>; 3],
    outbound: VecDeque<Message>,
    isolated: Option<u64>,
}

impl Cluster {
    fn node(&mut self, id: u64) -> &mut Node {
        self.nodes[(id - 1) as usize].as_mut().unwrap()
    }

    fn read(&mut self, id: u64, key: &[u8]) -> ReadReply {
        assert_eq!(self.node(id).start_read(key), ReadReply::Pending);
        assert!(self.drive(None).is_none());
        self.node(id).poll_read()
    }

    fn drive(&mut self, pause: Option<u64>) -> Option<Ready> {
        for _ in 0..1000 {
            for id in MEMBERS {
                if let Some(node) = &mut self.nodes[(id - 1) as usize] {
                    while node.raft.has_ready() {
                        let ready = node.raft.ready();
                        if pause == Some(id)
                            && (!ready.entries().is_empty() || !ready.snapshot().is_empty())
                        {
                            return Some(ready);
                        }
                        node.finish(ready, &mut self.outbound);
                    }
                }
            }
            let Some(message) = self.outbound.pop_front() else {
                return None;
            };
            if let Some(id) = self.isolated {
                if (message.from == id) != (message.to == id) {
                    continue;
                }
            }
            if self.nodes[(message.from - 1) as usize].is_none() {
                continue;
            }
            if let Some(node) = &mut self.nodes[(message.to - 1) as usize] {
                node.raft.step(message).unwrap();
            }
        }
        panic!("Raft message queue did not settle");
    }
}

#[test]
fn acknowledged_write_survives_leader_loss() {
    let dir = TestDir::new("durability");
    let mut cluster = Cluster {
        nodes: [
            Some(Node::open(&dir.0, 1)),
            Some(Node::open(&dir.0, 2)),
            None,
        ],
        outbound: VecDeque::new(),
        isolated: None,
    };
    // Give the disconnected voter its own empty durable state.
    drop(Node::open(&dir.0, 3));
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.node(1).raft.raft.state, StateRole::Leader);
    let previous_index = stored_index(&cluster.node(2).log, LAST_INDEX);

    let request = 42;
    cluster.node(1).submit(request, 1, 1, &[(KEY, b"saved")]);
    let pending = cluster
        .drive(Some(2))
        .expect("follower append ready for persistence");
    let index = pending.entries().last().unwrap().index;
    assert_eq!(stored_index(&cluster.node(1).log, LAST_INDEX), index);
    assert_eq!(
        stored_index(&cluster.node(2).log, LAST_INDEX),
        previous_index
    );
    assert!(cluster.node(1).replies.is_empty());
    assert_eq!(cluster.node(1).data.get(KEY).unwrap(), None);
    assert!(cluster.outbound.is_empty());

    cluster.nodes[1]
        .as_mut()
        .unwrap()
        .finish(pending, &mut cluster.outbound);
    // Deliver the durable append acknowledgement, then stop the leader before
    // its new commit notification reaches either follower.
    let ack = cluster.outbound.pop_front().unwrap();
    assert_eq!((ack.from, ack.to), (2, 1));
    assert_eq!(ack.get_msg_type(), MessageType::MsgAppendResponse);
    assert!(!ack.reject);
    assert_eq!(ack.index, index);
    let leader = cluster.node(1);
    leader.raft.step(ack).unwrap();
    let ready = leader.raft.ready();
    leader.finish(ready, &mut VecDeque::new());
    assert_eq!(leader.replies, [(request, WriteReply::Applied)]);
    assert_eq!(
        leader.data.get(KEY).unwrap().as_deref(),
        Some(&b"saved"[..])
    );
    assert!(read_hard_state(&cluster.node(2).log).commit < index);

    cluster.nodes[0] = None;
    cluster.nodes[1] = None;
    cluster.nodes[1] = Some(Node::open(&dir.0, 2));
    cluster.nodes[2] = Some(Node::open(&dir.0, 3));
    assert_eq!(cluster.node(2).data.get(KEY).unwrap(), None);
    cluster.node(2).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    let replacement = cluster.node(2);
    assert_eq!(replacement.raft.raft.state, StateRole::Leader);
    assert!(stored_index(&replacement.data, APPLIED_INDEX) > index);
    assert!(replacement.replies.is_empty());
    assert_eq!(
        cluster.read(2, KEY),
        ReadReply::Value(Some(b"saved".to_vec()))
    );
}

#[test]
fn linearizable_read_requires_fresh_quorum_and_local_application() {
    let dir = TestDir::new("read-index");
    let mut cluster = Cluster {
        nodes: MEMBERS.map(|id| Some(Node::open(&dir.0, id))),
        outbound: VecDeque::new(),
        isolated: None,
    };
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(1).submit(1, 1, 1, &[(KEY, b"old")]);
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.node(1).start_read(KEY), ReadReply::Pending);
    assert!(cluster.drive(None).is_none());
    assert_eq!(
        cluster.node(1).poll_read(),
        ReadReply::Value(Some(b"old".to_vec()))
    );
    assert_eq!(cluster.node(1).start_read_barrier(), ReadReply::Pending);
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.node(1).poll_read(), ReadReply::Confirmed);

    cluster.isolated = Some(1);
    cluster.node(2).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(2).pause_application = true;
    cluster.node(2).submit(2, 2, 1, &[(KEY, b"new")]);
    assert!(cluster.drive(None).is_none());
    let committed = read_hard_state(&cluster.node(2).log).commit;
    assert!(read_hard_state(&cluster.node(3).log).commit >= committed);
    assert_eq!(
        cluster
            .node(2)
            .start_scan(KEY.to_vec(), b"client/z".to_vec(), 8, 1024),
        ReadReply::Pending
    );
    assert!(cluster.drive(None).is_none());
    let leader = cluster.node(2);
    assert_eq!(leader.read.as_ref().unwrap().barrier, Some(committed));
    assert!(stored_index(&leader.data, APPLIED_INDEX) < committed);
    assert_eq!(leader.poll_read(), ReadReply::Pending);
    leader.pause_application = false;
    leader.apply_pending();
    assert_eq!(leader.replies, [(2, WriteReply::Applied)]);
    assert_eq!(
        leader.poll_read(),
        ReadReply::Rows {
            rows: vec![(b"key".to_vec(), b"new".to_vec())],
            more: false
        }
    );

    // No ticks or higher-term messages have reached the old leader. It still
    // believes it is leader and has a previously confirmed read.
    assert_eq!(cluster.node(1).raft.raft.state, StateRole::Leader);
    assert_eq!(cluster.node(1).start_read_barrier(), ReadReply::Pending);
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.node(1).read.as_ref().unwrap().barrier, None);
    assert_eq!(cluster.node(1).poll_read(), ReadReply::Pending);

    cluster.isolated = None;
    cluster.node(2).raft.ping();
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.node(1).poll_read(), ReadReply::NotLeader);
    assert_eq!(cluster.node(1).start_read(KEY), ReadReply::NotLeader);
}

#[test]
fn snapshot_limit_signal_clears_when_compaction_resumes() {
    let dir = TestDir::new("snapshot-limit");
    let mut cluster = Cluster {
        nodes: MEMBERS.map(|id| Some(Node::open(&dir.0, id))),
        outbound: VecDeque::new(),
        isolated: None,
    };
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(1).maybe_snapshot(1);
    let first_index = cluster.node(1).raft.store().snapshot_index();
    assert!(first_index > 0);

    // Populate local state directly to exercise the snapshot size limit.
    let value = vec![b'x'; zeus::MAX_VALUE_SIZE];
    for row in 0..(replication::MAX_SNAPSHOT_IMAGE / value.len()) {
        let key = format!("client/bulk-{row}");
        cluster.node(1).data.put(key.as_bytes(), &value).unwrap();
    }
    cluster.node(1).submit(1, 1, 1, &[(KEY, b"after")]);
    assert!(cluster.drive(None).is_none());
    let leader = cluster.node(1);
    leader.maybe_snapshot(1);
    assert!(leader.snapshot_limit_blocked);
    assert_eq!(leader.raft.store().snapshot_index(), first_index);

    leader.data.delete(b"client/bulk-0").unwrap();
    leader.submit(2, 1, 2, &[(KEY, b"smaller")]);
    assert!(cluster.drive(None).is_none());
    let leader = cluster.node(1);
    leader.maybe_snapshot(1);
    assert!(!leader.snapshot_limit_blocked);
    assert!(leader.raft.store().snapshot_index() > first_index);
}

#[test]
fn retry_after_lost_reply_and_restart_preserves_later_write() {
    let dir = TestDir::new("retry");
    let mut cluster = Cluster {
        nodes: MEMBERS.map(|id| Some(Node::open(&dir.0, id))),
        outbound: VecDeque::new(),
        isolated: None,
    };
    let original = [(KEY, b"first".as_slice()), (OTHER_KEY, b"saved".as_slice())];
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(1).submit(1, 7, 1, &original);
    assert!(cluster.drive(None).is_none());
    // The leader produced a reply, but the client never received it.
    assert_eq!(cluster.node(1).replies, [(1, WriteReply::Applied)]);
    cluster.nodes = [None, None, None];
    for id in [2, 3] {
        cluster.nodes[(id - 1) as usize] = Some(Node::open(&dir.0, id));
    }

    cluster.node(2).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(2).submit(2, 8, 1, &[(KEY, b"later")]);
    assert!(cluster.drive(None).is_none());
    let original_index = stored_index(&cluster.node(2).data, APPLIED_INDEX);
    cluster.node(2).submit(3, 7, 1, &original);
    assert!(cluster.drive(None).is_none());
    assert_eq!(
        cluster.node(2).replies.last(),
        Some(&(3, WriteReply::Applied))
    );
    assert_eq!(
        cluster.read(2, KEY),
        ReadReply::Value(Some(b"later".to_vec()))
    );
    assert_eq!(
        cluster.read(2, OTHER_KEY),
        ReadReply::Value(Some(b"saved".to_vec()))
    );
    cluster.node(2).submit(4, 7, 1, &[(KEY, b"changed")]);
    assert!(cluster.drive(None).is_none());
    cluster.node(2).submit(5, 7, 3, &[(KEY, b"skipped")]);
    assert!(cluster.drive(None).is_none());
    cluster.node(2).submit(6, 7, 2, &[(OTHER_KEY, b"next")]);
    assert!(cluster.drive(None).is_none());
    cluster.node(2).submit(7, 7, 1, &original);
    assert!(cluster.drive(None).is_none());
    assert_eq!(
        cluster.node(2).replies,
        [
            (2, WriteReply::Applied),
            (3, WriteReply::Applied),
            (4, WriteReply::CommandMismatch),
            (5, WriteReply::OutOfSequence),
            (6, WriteReply::Applied),
            (7, WriteReply::OutOfSequence),
        ]
    );
    assert_eq!(
        stored_index(&cluster.node(2).data, APPLIED_INDEX),
        original_index + 5
    );
    for (key, expected) in [(KEY, b"later".as_slice()), (OTHER_KEY, b"next".as_slice())] {
        assert_eq!(
            cluster.read(2, key),
            ReadReply::Value(Some(expected.to_vec()))
        );
    }
}

#[test]
fn restart_after_commit_before_application() {
    restart_during_application("before-apply", false);
}

#[test]
fn restart_with_torn_application_batch() {
    restart_during_application("torn-apply", true);
}

fn restart_during_application(scenario: &str, tear_batch: bool) {
    let dir = TestDir::new(scenario);
    let mut cluster = Cluster {
        nodes: MEMBERS.map(|id| Some(Node::open(&dir.0, id))),
        outbound: VecDeque::new(),
        isolated: None,
    };
    let client = 7;
    let puts = [(KEY, b"new".as_slice()), (OTHER_KEY, b"saved".as_slice())];
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(1).submit(1, 8, 1, &[(KEY, b"old")]);
    assert!(cluster.drive(None).is_none());
    let applied = stored_index(&cluster.node(2).data, APPLIED_INDEX);

    cluster.node(2).pause_application = true;
    cluster.node(1).submit(2, client, 1, &puts);
    assert!(cluster.drive(None).is_none());
    assert_eq!(
        cluster.node(1).replies.last(),
        Some(&(2, WriteReply::Applied))
    );
    let interrupted = cluster.nodes[1].take().unwrap();
    let committed = read_hard_state(&interrupted.log).commit;
    assert_eq!(committed, applied + 1);
    assert_eq!(stored_index(&interrupted.data, APPLIED_INDEX), applied);
    assert_eq!(interrupted.unapplied.len(), 1);
    let entry = interrupted.unapplied.into_iter().next().unwrap();
    assert_eq!(entry.index, committed);
    drop(interrupted.data);
    drop(interrupted.log);
    drop(cluster.nodes[0].take());

    let data_path = dir.0.join("2.data");
    let durable_len = fs::metadata(&data_path).unwrap().len();
    if tear_batch {
        // Use the real batch encoder to produce the next record on a copy.
        // Persist all but its final byte on the interrupted replica.
        let scratch = dir.0.join("complete.data");
        fs::copy(&data_path, &scratch).unwrap();
        let mut data = Store::open(&scratch).unwrap();
        assert_eq!(
            Node::apply_mutation(
                &mut data,
                &client.to_be_bytes(),
                1,
                &entry.data,
                &entry.index.to_be_bytes()
            ),
            WriteReply::Applied
        );
        drop(data);
        let bytes = fs::read(scratch).unwrap();
        let mut file = OpenOptions::new().append(true).open(&data_path).unwrap();
        file.write_all(&bytes[durable_len as usize..bytes.len() - 1])
            .unwrap();
        file.sync_all().unwrap();
        assert!(file.metadata().unwrap().len() > durable_len);
    }

    let mut reopened = Node::open(&dir.0, 2);
    assert_eq!(fs::metadata(&data_path).unwrap().len(), durable_len);
    assert_eq!(stored_index(&reopened.data, APPLIED_INDEX), applied);
    assert_eq!(read_hard_state(&reopened.log).commit, committed);
    assert_eq!(
        reopened.data.get(KEY).unwrap().as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(reopened.data.get(OTHER_KEY).unwrap(), None);
    assert_eq!(
        reopened
            .data
            .get(&retry_key(&client.to_be_bytes()))
            .unwrap(),
        None
    );
    reopened.pause_application = true;
    cluster.nodes[1] = Some(reopened);
    cluster.node(2).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.read(2, KEY), ReadReply::Pending);
    let leader = cluster.node(2);
    assert!(leader.read.as_ref().unwrap().barrier.unwrap() >= committed);
    leader.pause_application = false;
    leader.apply_pending();
    assert_eq!(leader.poll_read(), ReadReply::Value(Some(b"new".to_vec())));
    assert_eq!(
        cluster.read(2, OTHER_KEY),
        ReadReply::Value(Some(b"saved".to_vec()))
    );

    cluster.node(2).submit(3, 8, 2, &[(KEY, b"later")]);
    assert!(cluster.drive(None).is_none());
    cluster.node(2).submit(4, client, 1, &puts);
    assert!(cluster.drive(None).is_none());
    assert_eq!(
        cluster.node(2).replies,
        [(3, WriteReply::Applied), (4, WriteReply::Applied)]
    );
    assert_eq!(
        cluster.read(2, KEY),
        ReadReply::Value(Some(b"later".to_vec()))
    );
}

#[test]
fn interrupted_snapshot_recovers_with_retry_state_and_log_suffix() {
    let dir = TestDir::new("snapshot");
    let mut cluster = Cluster {
        nodes: MEMBERS.map(|id| Some(Node::open(&dir.0, id))),
        outbound: VecDeque::new(),
        isolated: None,
    };
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(1).submit(1, 5, 1, &[(KEY, b"before")]);
    assert!(cluster.drive(None).is_none());
    cluster.isolated = Some(3);
    // The installation spans multiple embedded batches as well as sidecar chunks.
    let bulk = vec![b'x'; 768 * 1024];
    for sequence in 1..=6 {
        let key = format!("client/bulk-{sequence}");
        cluster
            .node(1)
            .submit(10 + sequence, 9, sequence, &[(key.as_bytes(), &bulk)]);
        assert!(cluster.drive(None).is_none());
    }
    cluster.node(1).submit(2, 7, 1, &[(KEY, b"snapshot")]);
    assert!(cluster.drive(None).is_none());
    let snapshot_index = stored_index(&cluster.node(1).data, APPLIED_INDEX);
    cluster.node(1).maybe_snapshot(1);
    assert_eq!(
        cluster
            .node(1)
            .log
            .get(&snapshot_index.to_be_bytes())
            .unwrap(),
        None
    );
    cluster.nodes[0] = None;
    cluster.nodes[0] = Some(Node::open(&dir.0, 1));
    cluster.node(1).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster
        .node(1)
        .submit(3, 5, 2, &[(KEY, b"suffix"), (OTHER_KEY, b"tail")]);
    assert!(cluster.drive(None).is_none());

    cluster.isolated = None;
    cluster.node(1).raft.ping();
    let ready = cluster
        .drive(Some(3))
        .expect("offline voter receives snapshot");
    assert_eq!(ready.snapshot().get_metadata().index, snapshot_index);
    assert!(cluster.node(3).valid_snapshot(ready.snapshot()));
    let mut invalid = ready.snapshot().clone();
    invalid.set_data(b"invalid".to_vec().into());
    assert!(!cluster.node(3).valid_snapshot(&invalid));
    let follower = cluster.node(3);
    follower.persist_ready(&ready);
    assert_eq!(
        follower.data.get(KEY).unwrap().as_deref(),
        Some(b"before".as_slice())
    );

    // Interrupt after the snapshot becomes durable in the Raft log but
    // before replacing the state-machine file or sending any Raft response.
    drop(cluster.nodes[2].take());
    fs::write(dir.0.join("3.installing"), b"incomplete staging file").unwrap();
    cluster.nodes[2] = Some(Node::open(&dir.0, 3));
    let follower = cluster.node(3);
    assert_eq!(stored_index(&follower.data, APPLIED_INDEX), snapshot_index);
    assert_eq!(
        follower.data.get(KEY).unwrap().as_deref(),
        Some(b"snapshot".as_slice())
    );
    assert_eq!(follower.data.get(OTHER_KEY).unwrap(), None);
    assert_eq!(
        follower.data.get(b"client/bulk-6").unwrap().as_deref(),
        Some(bulk.as_slice())
    );
    cluster
        .node(1)
        .raft
        .report_snapshot(3, raft::SnapshotStatus::Finish);
    cluster.node(1).raft.ping();
    assert!(cluster.drive(None).is_none());
    let follower = cluster.node(3);
    assert_eq!(
        follower.data.get(OTHER_KEY).unwrap().as_deref(),
        Some(b"tail".as_slice())
    );

    drop(cluster.nodes[0].take());
    cluster.node(3).raft.campaign().unwrap();
    assert!(cluster.drive(None).is_none());
    cluster.node(3).submit(4, 7, 1, &[(KEY, b"snapshot")]);
    assert!(cluster.drive(None).is_none());
    assert_eq!(cluster.node(3).replies, [(4, WriteReply::Applied)]);
    assert_eq!(
        cluster.read(3, KEY),
        ReadReply::Value(Some(b"suffix".to_vec()))
    );
}
