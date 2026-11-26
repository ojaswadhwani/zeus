use super::{KEY, MEMBERS, Node, ReadReply, TestDir, WriteReply};
use protobuf::Message as _;
use raft::StateRole;
use raft::eraftpb::Message;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAME_BYTES: usize = 64 * 1024;
const WORKER_DIR: &str = "ZEUS_PROCESS_TEST_DIR";
const WORKER_ID: &str = "ZEUS_PROCESS_TEST_ID";
const WORKER_SOCKET: &str = "ZEUS_PROCESS_TEST_SOCKET";

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> io::Result<()> {
    assert!(bytes.len() <= MAX_FRAME_BYTES);
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)
}

fn read_frame(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized test frame",
        ));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

struct Voter {
    child: Child,
    stream: Option<UnixStream>,
}

impl Voter {
    fn spawn(root: &Path, sockets: &Path, id: u64) -> Self {
        let socket = sockets.join(format!("{id}.sock"));
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "process::worker"])
            .env(WORKER_DIR, root)
            .env(WORKER_ID, id.to_string())
            .env(WORKER_SOCKET, &socket)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut voter = Self {
            child,
            stream: None,
        };
        let deadline = Instant::now() + TIMEOUT;
        let stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(
                        voter.child.try_wait().unwrap().is_none(),
                        "voter {id} exited"
                    );
                    assert!(Instant::now() < deadline, "voter {id} did not connect");
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("cannot accept voter {id}: {error}"),
            }
        };
        // The path only serves startup. The connection stays open after unlinking.
        std::fs::remove_file(socket).unwrap();
        stream.set_read_timeout(Some(TIMEOUT)).unwrap();
        stream.set_write_timeout(Some(TIMEOUT)).unwrap();
        voter.stream = Some(stream);
        voter
    }

    fn exchange(&mut self, action: u8, body: &[u8], outbound: &mut VecDeque<Message>) -> Vec<u8> {
        let stream = self.stream.as_mut().unwrap();
        write_frame(stream, &[&[action], body].concat()).unwrap();
        let reply = read_frame(stream).unwrap();
        loop {
            let frame = read_frame(stream).unwrap();
            if frame.is_empty() {
                return reply;
            }
            outbound.push_back(Message::parse_from_bytes(&frame).unwrap());
        }
    }

    fn kill(mut self) {
        self.child.kill().unwrap();
        assert_eq!(self.child.wait().unwrap().signal(), Some(9));
    }
}

impl Drop for Voter {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// The test runner is also a worker so this test exercises the same node
// implementation as the deterministic in-process scenarios.
#[test]
#[ignore = "started by three_process_failover"]
fn worker() -> io::Result<()> {
    let Some(root) = std::env::var_os(WORKER_DIR) else {
        return Ok(());
    };
    let id: u64 = std::env::var(WORKER_ID).unwrap().parse().unwrap();
    let root = Path::new(&root);
    let mut node = Node::open(root, id);
    let mut stream = UnixStream::connect(std::env::var_os(WORKER_SOCKET).unwrap())?;
    let mut outbound = VecDeque::new();
    loop {
        let command = match read_frame(&mut stream) {
            Ok(command) => command,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        let (&action, body) = command.split_first().expect("worker action");
        let mut reply = Vec::new();
        match action {
            b'C' => node.raft.campaign().unwrap(),
            b'M' => node
                .raft
                .step(Message::parse_from_bytes(body).unwrap())
                .unwrap(),
            b'L' => reply.push(u8::from(node.raft.raft.state == StateRole::Leader)),
            b'W' => node.submit(1, 1, 1, &[(KEY, body)]),
            b'A' => reply.push(u8::from(node.replies.contains(&(1, WriteReply::Applied)))),
            b'R' => assert_eq!(node.start_read(KEY), ReadReply::Pending),
            b'P' => match node.poll_read() {
                ReadReply::Pending => reply.push(0),
                ReadReply::NotLeader => reply.push(1),
                ReadReply::Value(None) => reply.push(2),
                ReadReply::Rows { .. } => panic!("worker requested a point read"),
                ReadReply::Value(Some(value)) => {
                    reply.push(3);
                    reply.extend(value);
                }
            },
            _ => panic!("unknown worker action: {action}"),
        }
        while node.raft.has_ready() {
            let ready = node.raft.ready();
            node.finish(ready, &mut outbound);
        }
        write_frame(&mut stream, &reply)?;
        for message in outbound.drain(..) {
            write_frame(&mut stream, &message.write_to_bytes().unwrap())?;
        }
        write_frame(&mut stream, &[])?;
    }
}

struct ProcessCluster {
    voters: [Option<Voter>; 3],
    outbound: VecDeque<Message>,
}

impl ProcessCluster {
    fn exchange(&mut self, id: u64, action: u8, bytes: &[u8]) -> Vec<u8> {
        self.voters[(id - 1) as usize]
            .as_mut()
            .unwrap()
            .exchange(action, bytes, &mut self.outbound)
    }

    fn deliver(&mut self) {
        for _ in 0..1000 {
            let Some(message) = self.outbound.pop_front() else {
                return;
            };
            let from = self.voters[(message.from - 1) as usize].is_some();
            let to = self.voters[(message.to - 1) as usize].is_some();
            if from && to {
                let bytes = message.write_to_bytes().unwrap();
                assert!(self.exchange(message.to, b'M', &bytes).is_empty());
            }
        }
        panic!("process Raft message queue did not settle");
    }
}

#[test]
fn three_process_failover() {
    let dir = TestDir::new("processes");
    // Keep durable stores under TMPDIR while staying within sockaddr_un's
    // path limit even if the selected filesystem has a long mount path.
    let sockets = Path::new("/tmp").join(format!("zeus-sockets-{}", std::process::id()));
    std::fs::create_dir(&sockets).unwrap();
    let sockets = TestDir(sockets);
    let mut cluster = ProcessCluster {
        voters: MEMBERS.map(|id| Some(Voter::spawn(&dir.0, &sockets.0, id))),
        outbound: VecDeque::new(),
    };

    cluster.exchange(1, b'C', &[]);
    cluster.deliver();
    assert_eq!(cluster.exchange(1, b'L', &[]), [1]);

    cluster.exchange(1, b'W', b"process-durable");
    assert_eq!(cluster.exchange(1, b'A', &[]), [0]);
    cluster.deliver();
    assert_eq!(cluster.exchange(1, b'A', &[]), [1]);
    cluster.voters[0].take().unwrap().kill();

    cluster.exchange(2, b'C', &[]);
    cluster.deliver();
    assert_eq!(cluster.exchange(2, b'L', &[]), [1]);
    cluster.exchange(2, b'R', &[]);
    assert_eq!(cluster.exchange(2, b'P', &[]), [0]);
    cluster.deliver();
    assert_eq!(cluster.exchange(2, b'P', &[]), b"\x03process-durable");
}
