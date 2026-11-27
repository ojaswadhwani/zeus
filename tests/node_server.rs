#![cfg(feature = "replication")]

use protobuf::Message as _;
use raft::eraftpb::{Entry, Message, MessageType};
use std::fs;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "../src/node/transport.rs"]
mod transport;

#[allow(dead_code)]
#[path = "../src/node/client_wire.rs"]
mod client_wire;

const BIN: &str = env!("CARGO_BIN_EXE_zeus-node");

struct TestDir(PathBuf);

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Voter(Child);

impl Drop for Voter {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn openssl(root: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "openssl {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn issue_credentials(root: &Path) {
    openssl(
        root,
        &[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=Zeus Test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        ],
    );
    for (name, usages) in [
        ("node1", "serverAuth,clientAuth"),
        ("node2", "serverAuth,clientAuth"),
        ("node3", "serverAuth,clientAuth"),
        ("client", "clientAuth"),
        ("other-client", "clientAuth"),
    ] {
        let key = format!("{name}.key");
        let csr = format!("{name}.csr");
        let cert = format!("{name}.pem");
        openssl(
            root,
            &[
                "req",
                "-new",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-subj",
                &format!("/CN={name}"),
                "-keyout",
                &key,
                "-out",
                &csr,
            ],
        );
        fs::write(root.join("extensions"), format!(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage={usages}\nsubjectAltName=DNS:zeus.local\n"
        )).unwrap();
        openssl(
            root,
            &[
                "x509",
                "-req",
                "-days",
                "1",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-in",
                &csr,
                "-out",
                &cert,
                "-extfile",
                "extensions",
            ],
        );
    }
}

fn call(root: &Path, address: &str, operation: &str, args: &[&str]) -> Output {
    call_as(root, address, "client", operation, args)
}

fn call_as(root: &Path, address: &str, name: &str, operation: &str, args: &[&str]) -> Output {
    client_command(root, address, name, operation, args)
        .output()
        .unwrap()
}

fn client_command(
    root: &Path,
    address: &str,
    name: &str,
    operation: &str,
    args: &[&str],
) -> Command {
    let mut command = Command::new(BIN);
    command
        .current_dir(root)
        .args([
            operation,
            address,
            "ca.pem",
            &format!("{name}.pem"),
            &format!("{name}.key"),
        ])
        .args(args);
    command
}

fn submit_batch_without_reading_reply(root: &Path, address: &str) {
    let path = |name: &str| root.join(name).to_str().unwrap().to_owned();
    let (roots, chain, key) =
        transport::credentials(&path("ca.pem"), &path("client.pem"), &path("client.key")).unwrap();
    let tls = Arc::new(transport::client_config(roots, chain, key).unwrap());
    let mut stream = transport::connect(
        address.parse().unwrap(),
        tls,
        transport::NETWORK_TIMEOUT,
        None,
    )
    .unwrap();
    let mut request = b"CB".to_vec();
    request.extend_from_slice(&35_u64.to_be_bytes());
    request.push(b'P');
    client_wire::push_field(&mut request, b"batch-a");
    client_wire::push_field(&mut request, b"first");
    request.push(b'D');
    client_wire::push_field(&mut request, b"bulk-11");
    transport::write_frame(&mut stream, &request).unwrap();
}

fn find_leader(root: &Path, addresses: &[String; 3], voters: &mut [Option<Voter>; 3]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        for (index, voter) in voters.iter_mut().enumerate() {
            let Some(voter) = voter else { continue };
            assert!(
                voter.0.try_wait().unwrap().is_none(),
                "voter {} exited",
                index + 1
            );
            let output = call(root, &addresses[index], "get", &["election-probe"]);
            // MISSING comes from a successful quorum-confirmed read.
            if output.stderr == b"MISSING\n" {
                return index;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no voter completed a quorum read"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn assert_peer_frame_rejected(root: &Path, address: &str, certificate: &str, from: u64, to: u64) {
    let path = |name: &str| root.join(name).to_str().unwrap().to_owned();
    let (roots, chain, key) = transport::credentials(
        &path("ca.pem"),
        &path(&format!("{certificate}.pem")),
        &path(&format!("{certificate}.key")),
    )
    .unwrap();
    let tls = Arc::new(transport::client_config(roots, chain, key).unwrap());
    let mut connection = transport::connect(
        address.parse().unwrap(),
        tls,
        transport::NETWORK_TIMEOUT,
        None,
    )
    .unwrap();
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHeartbeat);
    message.from = from;
    message.to = to;
    message.term = 100;
    let request = [b"M".as_slice(), &message.write_to_bytes().unwrap()].concat();
    transport::write_frame(&mut connection, &request).unwrap();
    let error = transport::read_frame(&mut connection).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

fn spawn_voter(root: &Path, addresses: &[String; 3], index: usize) -> Voter {
    spawn_with_routes(root, &addresses[index], addresses, index)
}

fn spawn_with_routes(root: &Path, listen: &str, routes: &[String; 3], index: usize) -> Voter {
    spawn_node(Path::new(BIN), root, listen, routes, index)
}

fn spawn_node(
    binary: &Path,
    root: &Path,
    listen: &str,
    routes: &[String; 3],
    index: usize,
) -> Voter {
    let id = index + 1;
    let name = format!("node{id}");
    let mut command = Command::new(binary);
    command.current_dir(root).args([
        "serve",
        &id.to_string(),
        &name,
        listen,
        "ca.pem",
        &format!("{name}.pem"),
        &format!("{name}.key"),
    ]);
    for (peer, address) in routes.iter().enumerate() {
        if peer != index {
            let peer_id = peer + 1;
            command.arg(format!("{peer_id}={address},node{peer_id}.pem"));
        }
    }
    Voter(command.stdin(Stdio::null()).spawn().unwrap())
}

#[test]
#[ignore = "requires ZEUS_PREVIEW_BIN pointing to the v0.2.0-preview zeus-node binary"]
fn offline_preview_upgrade_preserves_state_and_allows_prewrite_rollback() {
    let preview =
        PathBuf::from(std::env::var_os("ZEUS_PREVIEW_BIN").expect("set ZEUS_PREVIEW_BIN"))
            .canonicalize()
            .unwrap();
    let version = Command::new(&preview).arg("--version").output().unwrap();
    assert!(version.status.success());
    assert_eq!(version.stdout, b"zeus-node 0.2.0-preview\n");
    let candidate =
        PathBuf::from(std::env::var_os("ZEUS_CANDIDATE_BIN").unwrap_or_else(|| BIN.into()))
            .canonicalize()
            .unwrap();
    let root = std::env::temp_dir().join(format!("zeus-upgrade-test-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let dir = TestDir(root);
    issue_credentials(&dir.0);
    for id in 1..=3 {
        fs::create_dir(dir.0.join(format!("node{id}"))).unwrap();
    }
    let reserved = [(); 3].map(|()| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = reserved
        .each_ref()
        .map(|socket| socket.local_addr().unwrap().to_string());
    drop(reserved);
    let start = |binary: &Path, root: &Path| {
        [0, 1, 2].map(|index| {
            Some(spawn_node(
                binary,
                root,
                &addresses[index],
                &addresses,
                index,
            ))
        })
    };
    let mut voters = start(&preview, &dir.0);
    let leader = find_leader(&dir.0, &addresses, &mut voters);
    let address = &addresses[leader];
    for sequence in 1..=33 {
        let write = call(
            &dir.0,
            address,
            "put",
            &[&sequence.to_string(), "deleted", "old"],
        );
        assert_eq!(write.stdout, b"OK\n", "{:?}", write.stderr);
    }
    let batch = ["34", "delete", "deleted", "put", "kept", "original"];
    assert_eq!(call(&dir.0, address, "batch", &batch).stdout, b"OK\n");
    let write = call_as(
        &dir.0,
        address,
        "other-client",
        "put",
        &["1", "kept", "after-batch"],
    );
    assert_eq!(write.stdout, b"OK\n");
    for id in 1..=3 {
        let voter = dir.0.join(format!("node{id}"));
        wait_for_file_bytes(&voter.join(format!("{id}.data")), b"after-batch");
        wait_for_file_bytes(&voter.join(format!("{id}.raft")), b"snapshot-image");
    }
    drop(voters);

    let rollback = dir.0.join("rollback");
    fs::create_dir(&rollback).unwrap();
    for name in ["ca.pem", "client.pem", "client.key"] {
        fs::copy(dir.0.join(name), rollback.join(name)).unwrap();
    }
    for id in 1..=3 {
        for extension in ["pem", "key"] {
            let name = format!("node{id}.{extension}");
            fs::copy(dir.0.join(&name), rollback.join(&name)).unwrap();
        }
        let name = format!("node{id}");
        let copy = rollback.join(&name);
        fs::create_dir(&copy).unwrap();
        for file in fs::read_dir(dir.0.join(name)).unwrap() {
            let file = file.unwrap();
            fs::copy(file.path(), copy.join(file.file_name())).unwrap();
        }
    }
    let verify_source_state = |root: &Path, address: &str| {
        assert_eq!(
            call(root, address, "get", &["kept"]).stdout,
            b"after-batch\n"
        );
        assert_eq!(
            call(root, address, "get", &["deleted"]).stderr,
            b"MISSING\n"
        );
    };

    let mut voters = start(&candidate, &dir.0);
    let leader = find_leader(&dir.0, &addresses, &mut voters);
    assert_eq!(
        call(&dir.0, &addresses[leader], "ready", &[]).stdout,
        b"OK\n"
    );
    verify_source_state(&dir.0, &addresses[leader]);
    drop(voters);

    let mut voters = start(&preview, &rollback);
    let leader = find_leader(&rollback, &addresses, &mut voters);
    verify_source_state(&rollback, &addresses[leader]);
    drop(voters);

    let mut voters = start(&candidate, &dir.0);
    let leader = find_leader(&dir.0, &addresses, &mut voters);
    let address = &addresses[leader];
    assert_eq!(call(&dir.0, address, "batch", &batch).stdout, b"OK\n");
    verify_source_state(&dir.0, address);
    let write = call(&dir.0, address, "put", &["35", "kept", "after-upgrade"]);
    assert_eq!(write.stdout, b"OK\n");
    assert_eq!(
        call(&dir.0, address, "get", &["kept"]).stdout,
        b"after-upgrade\n"
    );
}

fn interrupt_snapshot(root: &Path, address: &str, leader: usize, follower: usize) {
    let node = format!("node{}", leader + 1);
    let prefix = format!("{}.snapshot-", leader + 1);
    let (path, index, term) = fs::read_dir(root.join(&node))
        .unwrap()
        .find_map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_str()?.strip_prefix(&prefix)?;
            let (index, term) = name.split_once('-')?;
            Some((
                entry.path(),
                index.parse::<u64>().ok()?,
                term.parse::<u64>().ok()?,
            ))
        })
        .expect("leader published a snapshot sidecar");
    let file = zeus::Store::open(path).unwrap();
    let image: Vec<u8> = file
        .scan(&0_u64.to_be_bytes(), &[0xff; 8])
        .flat_map(|row| row.unwrap().1)
        .collect();
    assert!(
        image.len() > 256 * 1024,
        "snapshot spans multiple disk chunks"
    );
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = (leader + 1) as u64;
    message.to = (follower + 1) as u64;
    message.term = term;
    let snapshot = message.mut_snapshot();
    snapshot.set_data(image.into());
    let metadata = snapshot.mut_metadata();
    metadata.index = index;
    metadata.term = term;
    metadata.mut_conf_state().voters = vec![1, 2, 3];
    let bytes = message.write_to_bytes().unwrap();

    let path = |name: &str| root.join(name).to_str().unwrap().to_owned();
    let (roots, chain, key) = transport::credentials(
        &path("ca.pem"),
        &path(&format!("{node}.pem")),
        &path(&format!("{node}.key")),
    )
    .unwrap();
    let tls = Arc::new(transport::client_config(roots, chain, key).unwrap());
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut connection = loop {
        match transport::connect(
            address.parse().unwrap(),
            tls.clone(),
            transport::NETWORK_TIMEOUT,
            None,
        ) {
            Ok(connection) => break connection,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "waiting for lagging voter: {error}"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
    };
    let header = [b"S".as_slice(), &(bytes.len() as u32).to_be_bytes()].concat();
    transport::write_frame(&mut connection, &header).unwrap();
    transport::write_frame(&mut connection, &bytes[..transport::SNAPSHOT_CHUNK]).unwrap();
    connection.sock.shutdown(Shutdown::Write).unwrap();
    let error = transport::read_frame(&mut connection).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}

fn signal(voter: &Voter, name: &str) {
    assert!(
        Command::new("kill")
            .args([name, &voter.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn offline_backup_restores_a_fresh_three_node_cluster() {
    let root = std::env::temp_dir().join(format!("zeus-backup-test-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let dir = TestDir(root);
    issue_credentials(&dir.0);
    let reserved = [(); 3].map(|()| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = reserved
        .each_ref()
        .map(|socket| socket.local_addr().unwrap().to_string());
    drop(reserved);
    let cli = |args: &[&str]| {
        Command::new(BIN)
            .current_dir(&dir.0)
            .args(args)
            .output()
            .unwrap()
    };
    let ok = |output: Output| {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    let mut voters = [0, 1, 2].map(|index| {
        fs::create_dir(dir.0.join(format!("node{}", index + 1))).unwrap();
        Some(spawn_voter(&dir.0, &addresses, index))
    });
    let leader = find_leader(&dir.0, &addresses, &mut voters);
    let batch = ["1", "put", "kept", "original", "put", "removed", "value"];
    ok(call(&dir.0, &addresses[leader], "batch", &batch));
    let batch = ["2", "delete", "removed", "put", "kept", "saved"];
    ok(call(&dir.0, &addresses[leader], "batch", &batch));
    let id = (leader + 1).to_string();
    let name = format!("node{id}");
    let backup = ["backup", &id, &name, "backup.table"];
    assert!(!cli(&backup).status.success());
    drop(voters);
    ok(cli(&backup));
    assert!(!cli(&backup).status.success());

    let mut corrupt = fs::read(dir.0.join("backup.table")).unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(dir.0.join("corrupt.table"), corrupt).unwrap();
    for index in 0..3 {
        let id = (index + 1).to_string();
        let name = format!("node{id}");
        fs::rename(dir.0.join(&name), dir.0.join(format!("original-{name}"))).unwrap();
        fs::create_dir(dir.0.join(&name)).unwrap();
        let mut restore = [
            "restore",
            &id,
            &name,
            "corrupt.table",
            "node1.pem",
            "node2.pem",
            "node3.pem",
        ];
        assert!(!cli(&restore).status.success());
        assert_eq!(fs::read_dir(dir.0.join(&name)).unwrap().count(), 0);
        restore[3] = "backup.table";
        ok(cli(&restore));
        assert!(!cli(&restore).status.success());
    }
    let mut voters = [0, 1, 2].map(|index| Some(spawn_voter(&dir.0, &addresses, index)));
    let leader = find_leader(&dir.0, &addresses, &mut voters);
    let address = &addresses[leader];
    let saved = call(&dir.0, address, "get", &["kept"]);
    assert_eq!(saved.stdout, b"saved\n");
    let removed = call(&dir.0, address, "get", &["removed"]);
    assert_eq!(removed.stderr, b"MISSING\n");
    ok(call(&dir.0, address, "batch", &batch));
    ok(call(&dir.0, address, "put", &["3", "kept", "new"]));
    let new = call(&dir.0, address, "get", &["kept"]);
    assert_eq!(new.stdout, b"new\n");
}

#[test]
fn offline_rotation_replaces_the_ca_and_voter_credentials() {
    let root = std::env::temp_dir().join(format!("zeus-rotation-test-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let dir = TestDir(root);
    let old = dir.0.join("old");
    let replacement = dir.0.join("replacement");
    for root in [&old, &replacement] {
        fs::create_dir(root).unwrap();
        issue_credentials(root);
        for id in 1..=3 {
            fs::create_dir(root.join(format!("node{id}"))).unwrap();
        }
    }
    let reserved = [(); 3].map(|()| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = reserved
        .each_ref()
        .map(|socket| socket.local_addr().unwrap().to_string());
    drop(reserved);
    let mut voters = [0, 1, 2].map(|index| Some(spawn_voter(&old, &addresses, index)));
    let leader = find_leader(&old, &addresses, &mut voters);
    let address = &addresses[leader];
    let seed = call(
        &old,
        address,
        "batch",
        &["1", "put", "kept", "saved", "put", "removed", "old"],
    );
    assert_eq!(seed.stdout, b"OK\n");
    let delete = call(&old, address, "batch", &["2", "delete", "removed"]);
    assert_eq!(delete.stdout, b"OK\n");
    let saved = call(&old, address, "get", &["kept"]);
    assert_eq!(saved.stdout, b"saved\n");
    drop(voters);

    let id = (leader + 1).to_string();
    let backup = replacement.join("backup.table");
    let output = Command::new(BIN)
        .current_dir(&old)
        .args(["backup", &id, &format!("node{id}")])
        .arg(&backup)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for id in 1..=3 {
        let output = Command::new(BIN)
            .current_dir(&replacement)
            .args([
                "restore",
                &id.to_string(),
                &format!("node{id}"),
                "backup.table",
                "node1.pem",
                "node2.pem",
                "node3.pem",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut voters = [0, 1, 2].map(|index| Some(spawn_voter(&replacement, &addresses, index)));
    let leader = find_leader(&replacement, &addresses, &mut voters);
    let address = &addresses[leader];
    assert_eq!(call(&replacement, address, "ready", &[]).stdout, b"OK\n");
    assert_eq!(
        call(&replacement, address, "get", &["kept"]).stdout,
        b"saved\n"
    );
    assert_eq!(
        call(&replacement, address, "get", &["removed"]).stderr,
        b"MISSING\n"
    );

    for retired in ["node1", "client"] {
        // Trust the new server to check rejection by the server's client verifier.
        let output = Command::new(BIN)
            .current_dir(&replacement)
            .args(["live", address, "ca.pem"])
            .arg(old.join(format!("{retired}.pem")))
            .arg(old.join(format!("{retired}.key")))
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let error = String::from_utf8_lossy(&output.stderr);
        // The server can close the socket before the client reads the TLS alert.
        assert!(
            error.starts_with("received fatal alert:")
                || error.starts_with("Broken pipe")
                || error.starts_with("Connection reset by peer"),
            "{error}"
        );
        assert!(output.stdout.is_empty());
    }
    let write = call(&replacement, address, "put", &["1", "kept", "new"]);
    assert_eq!(write.stdout, b"OK\n");
    assert_eq!(
        call(&replacement, address, "get", &["kept"]).stdout,
        b"new\n"
    );
}

#[test]
fn authenticated_three_node_cluster_elects_replacement_after_sigkill() {
    let root = std::env::temp_dir().join(format!("zeus-node-test-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let dir = TestDir(root);
    issue_credentials(&dir.0);

    let reserved = [(); 3].map(|()| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = reserved
        .each_ref()
        .map(|socket| socket.local_addr().unwrap().to_string());
    drop(reserved);
    let mut voters = [0, 1, 2].map(|index| {
        fs::create_dir(dir.0.join(format!("node{}", index + 1))).unwrap();
        Some(spawn_voter(&dir.0, &addresses, index))
    });
    let leader = find_leader(&dir.0, &addresses, &mut voters);
    let follower = (leader + 1) % 3;
    let hint = call(&dir.0, &addresses[follower], "get", &["key"]);
    assert_eq!(
        hint.stderr,
        format!("NOT_LEADER {}\n", leader + 1).as_bytes()
    );

    let wrong_role = Command::new(BIN)
        .current_dir(&dir.0)
        .args([
            "get",
            &addresses[leader],
            "ca.pem",
            &format!("node{}.pem", follower + 1),
            &format!("node{}.key", follower + 1),
            "key",
        ])
        .output()
        .unwrap();
    assert_eq!(wrong_role.status.code(), Some(1));
    let claimed = (leader + 2) % 3 + 1;
    assert_peer_frame_rejected(
        &dir.0,
        &addresses[leader],
        &format!("node{}", follower + 1),
        claimed as u64,
        (leader + 1) as u64,
    );
    assert_peer_frame_rejected(
        &dir.0,
        &addresses[leader],
        "client",
        claimed as u64,
        (leader + 1) as u64,
    );

    let write = call(&dir.0, &addresses[leader], "put", &["1", "key", "saved"]);
    assert_eq!(
        write.stdout,
        b"OK\n",
        "{}",
        String::from_utf8_lossy(&write.stderr)
    );
    let lagging_data = dir.0.join(format!("node{0}/{0}.data", follower + 1));
    drop(voters[follower].take());
    let old_inode = fs::metadata(&lagging_data).unwrap().ino();
    let bulk = "x".repeat(12 * 1024);
    for sequence in 2..=34 {
        let (key, value) = if sequence == 34 {
            ("suffix".to_owned(), "snapshot-catch-up-tail")
        } else {
            (format!("bulk-{sequence}"), bulk.as_str())
        };
        let write = call(
            &dir.0,
            &addresses[leader],
            "put",
            &[&sequence.to_string(), &key, value],
        );
        assert_eq!(
            write.stdout,
            b"OK\n",
            "{}",
            String::from_utf8_lossy(&write.stderr)
        );
    }
    let metrics = call(&dir.0, &addresses[leader], "metrics", &[]);
    assert!(metrics.status.success());
    let metrics = String::from_utf8(metrics.stdout).unwrap();
    assert!(metrics.lines().any(|line| line == "snapshot_index=32"));
    assert!(
        metrics
            .lines()
            .any(|line| line == "snapshot_limit_blocked=0")
    );
    signal(voters[leader].as_ref().unwrap(), "-STOP");
    voters[follower] = Some(spawn_voter(&dir.0, &addresses, follower));
    interrupt_snapshot(&dir.0, &addresses[follower], leader, follower);
    assert_eq!(fs::metadata(&lagging_data).unwrap().ino(), old_inode);
    let catch_up_start = Instant::now();
    signal(voters[leader].as_ref().unwrap(), "-CONT");
    let deadline = Instant::now() + Duration::from_secs(12);
    while fs::metadata(&lagging_data).unwrap().ino() == old_inode {
        assert!(
            Instant::now() < deadline,
            "offline voter did not install a snapshot"
        );
        assert!(
            voters[follower]
                .as_mut()
                .unwrap()
                .0
                .try_wait()
                .unwrap()
                .is_none()
        );
        thread::sleep(Duration::from_millis(50));
    }
    wait_for_file_bytes(&lagging_data, b"snapshot-catch-up-tail");
    eprintln!(
        "snapshot_catch_up_ms={}",
        catch_up_start.elapsed().as_millis()
    );

    submit_batch_without_reading_reply(&dir.0, &addresses[leader]);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let scan = call(
            &dir.0,
            &addresses[leader],
            "scan",
            &["batch-", "bulk-12", "8"],
        );
        if scan.status.success() && scan.stdout.starts_with(b"batch-a\tfirst\n") {
            assert!(
                !scan
                    .stdout
                    .windows(b"bulk-11\t".len())
                    .any(|window| window == b"bulk-11\t")
            );
            break;
        }
        assert!(Instant::now() < deadline, "batch did not become visible");
        thread::sleep(Duration::from_millis(25));
    }

    let mut killed = voters[leader].take().unwrap();
    let failover_start = Instant::now();
    killed.0.kill().unwrap();
    assert_eq!(killed.0.wait().unwrap().signal(), Some(9));

    let replacement = find_leader(&dir.0, &addresses, &mut voters);
    eprintln!("failover_ms={}", failover_start.elapsed().as_millis());
    let old_log_path = dir.0.join(format!("node{0}/{0}.raft", leader + 1));
    let old_log = zeus::Store::open(old_log_path).unwrap();
    assert!(old_log.get(b"snapshot-image").unwrap().is_some());
    assert_eq!(old_log.get(&20_u64.to_be_bytes()).unwrap(), None);
    assert_ne!(replacement, leader);
    let read = call(&dir.0, &addresses[replacement], "get", &["key"]);
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    assert_eq!(read.stdout, b"saved\n");
    let suffix = call(&dir.0, &addresses[replacement], "get", &["suffix"]);
    assert_eq!(suffix.stdout, b"snapshot-catch-up-tail\n");
    let copied = call(&dir.0, &addresses[replacement], "get", &["bulk-10"]);
    assert_eq!(copied.stdout.strip_suffix(b"\n"), Some(bulk.as_bytes()));
    let overwrite = call_as(
        &dir.0,
        &addresses[replacement],
        "other-client",
        "batch",
        &["1", "put", "batch-a", "later", "put", "bulk-11", "restored"],
    );
    assert_eq!(
        overwrite.stdout,
        b"OK\n",
        "{}",
        String::from_utf8_lossy(&overwrite.stderr)
    );
    let retry = call(
        &dir.0,
        &addresses[replacement],
        "batch",
        &["35", "put", "batch-a", "first", "delete", "bulk-11"],
    );
    assert_eq!(
        retry.stdout,
        b"OK\n",
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let first_page = call(
        &dir.0,
        &addresses[replacement],
        "scan",
        &["batch-", "z", "1"],
    );
    assert_eq!(first_page.stdout, b"batch-a\tlater\n");
    assert_eq!(first_page.stderr, b"MORE\n");
    let restored = call(
        &dir.0,
        &addresses[replacement],
        "scan",
        &["bulk-11", "bulk-12", "8"],
    );
    assert!(restored.status.success());
    assert_eq!(restored.stdout, b"bulk-11\trestored\n");
    assert!(restored.stderr.is_empty());
}

/// Routes each directed peer connection through a gate. Client connections go
/// directly to the listeners, and TLS still runs end to end between the voters.
struct PeerRoutes {
    gate: Arc<AtomicUsize>,
    addresses: [[String; 3]; 3],
    workers: Vec<thread::JoinHandle<()>>,
}

impl PeerRoutes {
    fn new(voters: &[String; 3]) -> Self {
        let gate = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        let addresses = std::array::from_fn(|from| {
            std::array::from_fn(|to| {
                if from == to {
                    return voters[to].clone();
                }
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                listener.set_nonblocking(true).unwrap();
                let address = listener.local_addr().unwrap().to_string();
                let target = voters[to].parse::<SocketAddr>().unwrap();
                let gate = gate.clone();
                workers.push(thread::spawn(move || {
                    while gate.load(Ordering::SeqCst) != usize::MAX {
                        match listener.accept() {
                            Ok((client, _)) => {
                                let isolated = gate.load(Ordering::SeqCst);
                                if isolated != from + 1 && isolated != to + 1 {
                                    relay(client, target);
                                }
                            }
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(2));
                            }
                            Err(error) => panic!("peer relay failed: {error}"),
                        }
                    }
                }));
                address
            })
        });
        Self {
            gate,
            addresses,
            workers,
        }
    }

    fn isolate(&self, voter: usize) {
        self.gate.store(voter + 1, Ordering::SeqCst);
    }

    fn heal(&self) {
        self.gate.store(0, Ordering::SeqCst);
    }
}

impl Drop for PeerRoutes {
    fn drop(&mut self) {
        self.gate.store(usize::MAX, Ordering::SeqCst);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn relay(client: TcpStream, target: SocketAddr) {
    let timeout = Duration::from_secs(2);
    let Ok(server) = TcpStream::connect_timeout(&target, timeout) else {
        return;
    };
    for connection in [&client, &server] {
        connection.set_read_timeout(Some(timeout)).unwrap();
        connection.set_write_timeout(Some(timeout)).unwrap();
    }
    thread::scope(|scope| {
        scope.spawn(|| {
            let _ = io::copy(&mut &client, &mut &server);
            let _ = server.shutdown(Shutdown::Write);
        });
        let _ = io::copy(&mut &server, &mut &client);
        let _ = client.shutdown(Shutdown::Write);
    });
}

fn wait_for_file_bytes(path: &Path, marker: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let bytes = fs::read(path).unwrap();
        if bytes.windows(marker.len()).any(|window| window == marker) {
            return;
        }
        assert!(Instant::now() < deadline, "marker never reached {path:?}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_minority_rejected(result: Output) {
    assert_eq!(result.status.code(), Some(2));
    assert!(result.stdout.is_empty());
    assert!(
        result.stderr.starts_with(b"NOT_LEADER ") || result.stderr == b"TIMEOUT\n",
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn assert_minority_request_rejected(root: &Path, address: &str, operation: &str, args: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = call(root, address, operation, args);
        if result.stderr != b"BUSY\n" {
            assert_minority_rejected(result);
            return;
        }
        // A previous client can time out before the node clears its request.
        assert!(result.stdout.is_empty());
        assert!(Instant::now() < deadline, "isolated voter stayed busy");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn partitioned_cluster_rejects_minority_and_repairs_conflicting_suffix() {
    let root = std::env::temp_dir().join(format!("zeus-partition-test-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let dir = TestDir(root);
    issue_credentials(&dir.0);
    let reserved = [(); 3].map(|()| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = reserved
        .each_ref()
        .map(|socket| socket.local_addr().unwrap().to_string());
    let routes = PeerRoutes::new(&addresses);
    drop(reserved);
    let mut voters = std::array::from_fn(|index| {
        fs::create_dir(dir.0.join(format!("node{}", index + 1))).unwrap();
        Some(spawn_with_routes(
            &dir.0,
            &addresses[index],
            &routes.addresses[index],
            index,
        ))
    });

    let old = find_leader(&dir.0, &addresses, &mut voters);
    let old_log = dir.0.join(format!("node{0}/{0}.raft", old + 1));
    let old_data = dir.0.join(format!("node{0}/{0}.data", old + 1));
    let initial = call(
        &dir.0,
        &addresses[old],
        "put",
        &["1", "partition-key", "before-partition"],
    );
    assert_eq!(initial.stdout, b"OK\n");

    routes.isolate(old);
    let minority_value = "minority-uncommitted-value";
    let request = client_command(
        &dir.0,
        &addresses[old],
        "client",
        "put",
        &["2", "partition-key", minority_value],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    // Inspect the append-only file while its owner is running. We need proof
    // the old leader actually appended an entry before it lost leadership.
    wait_for_file_bytes(&old_log, minority_value.as_bytes());
    assert_minority_rejected(request.wait_with_output().unwrap());

    let isolated = voters[old].take().unwrap();
    let replacement = find_leader(&dir.0, &addresses, &mut voters);
    let ready = call(&dir.0, &addresses[replacement], "ready", &[]);
    assert!(ready.status.success());
    assert_eq!(ready.stdout, b"OK\n");
    let live = call(&dir.0, &addresses[old], "live", &[]);
    assert!(live.status.success());
    assert_eq!(live.stdout, b"OK\n");
    assert_minority_request_rejected(&dir.0, &addresses[old], "ready", &[]);
    let majority_value = "majority-committed-value";
    let committed = call_as(
        &dir.0,
        &addresses[replacement],
        "other-client",
        "put",
        &["1", "partition-key", majority_value],
    );
    assert_eq!(
        committed.stdout,
        b"OK\n",
        "{}",
        String::from_utf8_lossy(&committed.stderr)
    );
    let read = call(&dir.0, &addresses[replacement], "get", &["partition-key"]);
    assert_eq!(
        read.stdout.strip_suffix(b"\n"),
        Some(majority_value.as_bytes())
    );
    let index = |voter: usize, field: &str| {
        let output = call(&dir.0, &addresses[voter], "metrics", &[]);
        assert!(output.status.success());
        let metrics = String::from_utf8(output.stdout).unwrap();
        assert!(
            metrics
                .lines()
                .any(|line| line == format!("voter_id={}", voter + 1))
        );
        metrics
            .lines()
            .filter_map(|line| line.split_once('='))
            .find(|(key, _)| *key == field)
            .unwrap()
            .1
            .parse::<u64>()
            .unwrap()
    };
    let applied = index(replacement, "applied_index");
    assert_eq!(index(replacement, "commit_index"), applied);
    assert!(index(old, "applied_index") < applied);
    assert_minority_request_rejected(&dir.0, &addresses[old], "get", &["partition-key"]);
    assert_minority_request_rejected(
        &dir.0,
        &addresses[old],
        "put",
        &["2", "partition-key", minority_value],
    );
    let old_state = fs::read(&old_data).unwrap();
    assert!(
        old_state
            .windows(b"before-partition".len())
            .any(|bytes| bytes == b"before-partition")
    );
    assert!(
        !old_state
            .windows(minority_value.len())
            .any(|bytes| bytes == minority_value.as_bytes())
    );

    routes.heal();
    wait_for_file_bytes(&old_data, majority_value.as_bytes());
    drop(isolated);
    let log = zeus::Store::open(old_log).unwrap();
    let mut found_majority = false;
    for entry in log.scan(&1_u64.to_be_bytes(), b"\x01") {
        let (key, value) = entry.unwrap();
        assert_eq!(key.len(), 8);
        let entry = Entry::parse_from_bytes(&value).unwrap();
        assert!(
            !entry
                .data
                .windows(minority_value.len())
                .any(|bytes| bytes == minority_value.as_bytes())
        );
        found_majority |= entry
            .data
            .windows(majority_value.len())
            .any(|bytes| bytes == majority_value.as_bytes());
    }
    assert!(
        found_majority,
        "former leader now has the majority's log entry"
    );
    let data = zeus::Store::open(old_data).unwrap();
    assert_eq!(
        data.get(b"client/partition-key").unwrap().as_deref(),
        Some(majority_value.as_bytes())
    );
}
