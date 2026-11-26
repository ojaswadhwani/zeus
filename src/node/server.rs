use super::client_wire::{
    MAX_BATCH_BYTES, MAX_BATCH_OPERATIONS, MAX_KEY, MAX_SCAN_BYTES, MAX_SCAN_ROWS, MAX_VALUE,
    push_field, take_field,
};
use super::replication::{MAX_SNAPSHOT_IMAGE, MEMBERS, Node, ReadReply, WriteReply};
use super::transport::{
    NETWORK_TIMEOUT, PEER_TIMEOUT, Peer, Result, connect, read_frame, read_snapshot, write_frame,
    write_snapshot,
};
use protobuf::Message as _;
use raft::eraftpb::{Message, MessageType};
use raft::{Config, SnapshotStatus, StateRole};
use rustls::{ClientConfig, ServerConfig, ServerConnection, StreamOwned};
use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::{Duration, Instant};
use zeus::BatchOp;

const TICK: Duration = Duration::from_millis(100);
const CLIENT_DEADLINE: Duration = Duration::from_secs(2);
const MAX_CONNECTIONS: usize = 32;
const SNAPSHOT_INTERVAL: u64 = 32;
const MAX_SNAPSHOT_MESSAGE: usize = MAX_SNAPSHOT_IMAGE + 1024;

enum Event {
    Peer(Message),
    SnapshotDelivery {
        peer: u64,
        term: u64,
        index: u64,
        delivered: bool,
    },
    Client {
        request: Vec<u8>,
        principal: Vec<u8>,
        expires: Instant,
        reply: SyncSender<Vec<u8>>,
    },
}

enum Pending {
    Read,
    Write(u64),
}

struct Client {
    pending: Pending,
    term: u64,
    expires: Instant,
    reply: SyncSender<Vec<u8>>,
}

pub(crate) fn serve(
    id: u64,
    root: &Path,
    address: SocketAddr,
    mut peers: Vec<Peer>,
    server_tls: ServerConfig,
    client_tls: ClientConfig,
) -> Result<()> {
    peers.sort_unstable_by_key(|peer| peer.id);
    let mut cluster = Vec::new();
    for peer in &peers {
        let certificate = peer.certificate.as_ref();
        if certificate.len() > 4096 {
            return Err("peer leaf certificate exceeds 4096 bytes".into());
        }
        cluster.extend_from_slice(&(certificate.len() as u32).to_be_bytes());
        cluster.extend_from_slice(certificate);
    }
    let mut node = Node::open_with_config(
        root,
        Config {
            id,
            election_tick: 12,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 2048,
            max_inflight_msgs: 16,
            max_uncommitted_size: 128 * 1024,
            ..Config::default()
        },
        cluster,
    );
    let listener = TcpListener::bind(address)?;
    let (events, incoming) = mpsc::sync_channel(64);
    let peers = Arc::new(peers);
    let outgoing = peer_senders(id, &peers, Arc::new(client_tls), events.clone());
    let listener_thread =
        thread::spawn(move || accept(listener, id, peers, Arc::new(server_tls), events));
    let mut outbound = VecDeque::new();
    let mut next_tick = Instant::now() + TICK;
    let mut active: Option<Client> = None;
    let mut request_id = 0_u64;

    loop {
        if listener_thread.is_finished() {
            return listener_thread.join().map_err(|_| "listener panicked")?;
        }
        let now = Instant::now();
        if now >= next_tick {
            node.raft.tick();
            next_tick = now + TICK;
        }
        let event = incoming.recv_timeout(next_tick.saturating_duration_since(Instant::now()));
        match event {
            Ok(Event::Peer(message)) => {
                if message.get_msg_type() != MessageType::MsgSnapshot
                    || node.valid_snapshot(message.get_snapshot())
                {
                    let _ = node.raft.step(message);
                }
            }
            Ok(Event::SnapshotDelivery {
                peer,
                term,
                index,
                delivered,
            }) => {
                report_snapshot(&mut node, peer, term, index, delivered);
            }
            Ok(Event::Client {
                request,
                principal,
                expires,
                reply,
            }) => {
                if Instant::now() >= expires {
                    respond(reply, b"TIMEOUT".to_vec());
                } else if node.raft.raft.state != StateRole::Leader {
                    respond(reply, leader_hint(&node));
                } else if active.is_some() {
                    respond(reply, b"BUSY".to_vec());
                } else {
                    request_id = request_id
                        .checked_add(1)
                        .ok_or("request counter exhausted")?;
                    match start(&mut node, request_id, &principal, &request) {
                        Ok(pending) => {
                            active = Some(Client {
                                pending,
                                term: node.raft.raft.term,
                                expires,
                                reply,
                            });
                        }
                        Err(error) => respond(reply, error.to_vec()),
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return listener_thread.join().map_err(|_| "listener panicked")?;
            }
        }
        while node.raft.has_ready() {
            let ready = node.raft.ready();
            node.finish(ready, &mut outbound);
            node.maybe_snapshot(SNAPSHOT_INTERVAL);
        }
        for message in outbound.drain(..) {
            if let Some(sender) = outgoing
                .get((message.to.saturating_sub(1)) as usize)
                .and_then(Option::as_ref)
            {
                // Raft retransmits if a peer is down or its bounded queue is full.
                if let Err(error) = sender.try_send(message) {
                    let message = match error {
                        mpsc::TrySendError::Full(message)
                        | mpsc::TrySendError::Disconnected(message) => message,
                    };
                    if message.get_msg_type() == MessageType::MsgSnapshot {
                        report_snapshot(
                            &mut node,
                            message.to,
                            message.term,
                            message.get_snapshot().get_metadata().index,
                            false,
                        );
                    }
                }
            }
        }
        if let Some(client) = &active {
            if let Some(reply) = complete(&mut node, client) {
                respond(active.take().unwrap().reply, reply);
            }
        }
        node.replies.clear();
    }
}

fn report_snapshot(node: &mut Node, peer: u64, term: u64, index: u64, delivered: bool) {
    if node.raft.raft.state == StateRole::Leader
        && node.raft.raft.term == term
        && node
            .raft
            .raft
            .prs()
            .get(peer)
            .is_some_and(|progress| progress.pending_snapshot == index)
    {
        let status = if delivered {
            SnapshotStatus::Finish
        } else {
            SnapshotStatus::Failure
        };
        node.raft.report_snapshot(peer, status);
    }
}

fn start(
    node: &mut Node,
    request_id: u64,
    principal: &[u8],
    request: &[u8],
) -> std::result::Result<Pending, &'static [u8]> {
    const INVALID: &[u8] = b"INVALID";
    let (&action, mut body) = request.split_first().ok_or(INVALID)?;
    let sequence = if matches!(action, b'P' | b'B') {
        let (sequence, rest) = body.split_first_chunk::<8>().ok_or(INVALID)?;
        body = rest;
        let sequence = u64::from_be_bytes(*sequence);
        if sequence == 0 {
            return Err(INVALID);
        }
        sequence
    } else {
        0
    };

    let result = match action {
        b'G' => {
            let key = read_key(&mut body).ok_or(INVALID)?;
            if !body.is_empty() {
                return Err(INVALID);
            }
            assert_eq!(node.start_read(&key), ReadReply::Pending);
            return Ok(Pending::Read);
        }
        b'S' => {
            let start = read_key(&mut body).ok_or(INVALID)?;
            let end = read_key(&mut body).ok_or(INVALID)?;
            let count = u16::from_be_bytes(body.try_into().map_err(|_| INVALID)?) as usize;
            if !(1..=MAX_SCAN_ROWS).contains(&count) {
                return Err(INVALID);
            }
            assert_eq!(
                node.start_scan(start, end, count, MAX_SCAN_BYTES),
                ReadReply::Pending
            );
            return Ok(Pending::Read);
        }
        b'P' => {
            let key = read_key(&mut body).ok_or(INVALID)?;
            if body.len() > MAX_VALUE {
                return Err(INVALID);
            }
            node.submit_authenticated(request_id, principal, sequence, &[BatchOp::Put(&key, body)])
        }
        b'B' => {
            let batch = read_batch(body).ok_or(INVALID)?;
            let operations: Vec<_> = batch
                .iter()
                .map(|operation| match operation {
                    ClientOperation::Put(key, value) => BatchOp::Put(key, value),
                    ClientOperation::Delete(key) => BatchOp::Delete(key),
                })
                .collect();
            node.submit_authenticated(request_id, principal, sequence, &operations)
        }
        _ => return Err(INVALID),
    };
    result.map_err(|_| b"RETRY".as_slice())?;
    Ok(Pending::Write(request_id))
}

enum ClientOperation<'a> {
    Put(Vec<u8>, &'a [u8]),
    Delete(Vec<u8>),
}

fn read_batch(mut body: &[u8]) -> Option<Vec<ClientOperation<'_>>> {
    if body.is_empty() || body.len() > MAX_BATCH_BYTES {
        return None;
    }
    let mut operations = Vec::new();
    while !body.is_empty() {
        if operations.len() == MAX_BATCH_OPERATIONS {
            return None;
        }
        let (&kind, rest) = body.split_first()?;
        body = rest;
        let key = read_key(&mut body)?;
        operations.push(match kind {
            b'P' => {
                let value = take_field(&mut body)?;
                if value.len() > MAX_VALUE {
                    return None;
                }
                ClientOperation::Put(key, value)
            }
            b'D' => ClientOperation::Delete(key),
            _ => return None,
        });
    }
    Some(operations)
}

fn read_key(body: &mut &[u8]) -> Option<Vec<u8>> {
    let key = take_field(body)?;
    if key.is_empty() || key.len() > MAX_KEY {
        return None;
    }
    Some([b"client/".as_slice(), key].concat())
}

fn complete(node: &mut Node, client: &Client) -> Option<Vec<u8>> {
    if Instant::now() >= client.expires {
        node.cancel_read();
        return Some(b"TIMEOUT".to_vec());
    }
    if node.raft.raft.state != StateRole::Leader || node.raft.raft.term != client.term {
        node.cancel_read();
        return Some(leader_hint(node));
    }
    match client.pending {
        Pending::Read => match node.poll_read() {
            ReadReply::Pending => None,
            ReadReply::NotLeader => Some(leader_hint(node)),
            ReadReply::Value(None) => Some(b"MISSING".to_vec()),
            ReadReply::Value(Some(value)) => Some([b"VALUE ".as_slice(), &value].concat()),
            ReadReply::Rows { rows, more } => {
                let mut response = b"SCAN ".to_vec();
                response.push(u8::from(more));
                for (key, value) in rows {
                    push_field(&mut response, &key);
                    push_field(&mut response, &value);
                }
                Some(response)
            }
        },
        Pending::Write(id) => node.replies.iter().find(|(request, _)| *request == id).map(
            |(_, result)| match result {
                WriteReply::Applied => b"OK".to_vec(),
                WriteReply::CommandMismatch => b"COMMAND_MISMATCH".to_vec(),
                WriteReply::OutOfSequence => b"OUT_OF_SEQUENCE".to_vec(),
            },
        ),
    }
}

fn leader_hint(node: &Node) -> Vec<u8> {
    format!("NOT_LEADER {}", node.raft.raft.leader_id).into_bytes()
}

fn respond(channel: SyncSender<Vec<u8>>, reply: Vec<u8>) {
    let _ = channel.try_send(reply);
}

fn accept(
    listener: TcpListener,
    id: u64,
    peers: Arc<Vec<Peer>>,
    tls: Arc<ServerConfig>,
    events: SyncSender<Event>,
) -> Result<()> {
    let connections = Arc::new(AtomicUsize::new(0));
    for tcp in listener.incoming() {
        let tcp = tcp?;
        if connections.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
            continue;
        }
        connections.fetch_add(1, Ordering::Relaxed);
        let connections = connections.clone();
        let peers = peers.clone();
        let tls = tls.clone();
        let events = events.clone();
        thread::spawn(move || {
            let _ = receive(tcp, id, &peers, tls, &events);
            connections.fetch_sub(1, Ordering::Relaxed);
        });
    }
    Ok(())
}

fn receive(
    mut tcp: TcpStream,
    id: u64,
    peers: &[Peer],
    tls: Arc<ServerConfig>,
    events: &SyncSender<Event>,
) -> Result<()> {
    tcp.set_read_timeout(Some(NETWORK_TIMEOUT))?;
    tcp.set_write_timeout(Some(NETWORK_TIMEOUT))?;
    let mut connection = ServerConnection::new(tls)?;
    while connection.is_handshaking() {
        connection.complete_io(&mut tcp)?;
    }
    let certificate = connection
        .peer_certificates()
        .and_then(|chain| chain.first())
        .ok_or("missing authenticated certificate")?
        .clone();
    let peer = peers.iter().find(|peer| peer.certificate == certificate);
    let mut stream = StreamOwned::new(connection, tcp);
    let frame = read_frame(&mut stream)?;
    let (&kind, request) = frame.split_first().ok_or("empty frame")?;
    match (kind, peer) {
        (b'M' | b'S', Some(peer)) if peer.id != id => {
            let image;
            let request = if kind == b'S' {
                image = read_snapshot(&mut stream, request, MAX_SNAPSHOT_MESSAGE)?;
                &image
            } else {
                request
            };
            let message = Message::parse_from_bytes(request)?;
            if message.from != peer.id
                || message.to != id
                || (kind == b'S' && message.get_msg_type() != MessageType::MsgSnapshot)
            {
                return Err("Raft frame identity does not match the authenticated peer".into());
            }
            events.try_send(Event::Peer(message))?;
            // This confirms transport admission. Raft sends its durable
            // acknowledgement separately, after processing the event.
            write_frame(&mut stream, b"RECEIVED")?;
        }
        (b'C', None) if certificate.len() <= zeus::MAX_KEY_SIZE - b"retry/".len() => {
            let (reply, response) = mpsc::sync_channel(1);
            let expires = Instant::now() + CLIENT_DEADLINE;
            let result = if events
                .try_send(Event::Client {
                    request: request.to_vec(),
                    principal: certificate.as_ref().to_vec(),
                    expires,
                    reply,
                })
                .is_ok()
            {
                response
                    .recv_timeout(CLIENT_DEADLINE)
                    .unwrap_or_else(|_| b"TIMEOUT".to_vec())
            } else {
                b"BUSY".to_vec()
            };
            write_frame(&mut stream, &result)?;
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "certificate role mismatch",
            )
            .into());
        }
    }
    Ok(())
}

fn peer_senders(
    local_id: u64,
    peers: &[Peer],
    tls: Arc<ClientConfig>,
    events: SyncSender<Event>,
) -> [Option<SyncSender<Message>>; 3] {
    let mut outgoing = MEMBERS.map(|_| None);
    for peer in peers.iter().filter(|peer| peer.id != local_id).cloned() {
        let id = peer.id;
        let tls = tls.clone();
        let events = events.clone();
        let (sender, queue): (SyncSender<Message>, Receiver<Message>) = mpsc::sync_channel(16);
        outgoing[(id - 1) as usize] = Some(sender);
        thread::spawn(move || {
            while let Ok(message) = queue.recv() {
                let send = || -> Result<bool> {
                    let mut stream = connect(
                        peer.address,
                        tls.clone(),
                        PEER_TIMEOUT,
                        Some(&peer.certificate),
                    )?;
                    let bytes = message.write_to_bytes()?;
                    if message.get_msg_type() == MessageType::MsgSnapshot {
                        write_snapshot(&mut stream, &bytes)?;
                    } else {
                        write_frame(&mut stream, &[b"M".as_slice(), &bytes].concat())?;
                    }
                    Ok(read_frame(&mut stream)? == b"RECEIVED")
                };
                let delivered = send().unwrap_or(false);
                if message.get_msg_type() == MessageType::MsgSnapshot {
                    let _ = events.send(Event::SnapshotDelivery {
                        peer: id,
                        term: message.term,
                        index: message.get_snapshot().get_metadata().index,
                        delivered,
                    });
                }
            }
        });
    }
    outgoing
}
