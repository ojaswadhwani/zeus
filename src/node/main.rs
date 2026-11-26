#![forbid(unsafe_code)]

mod client_wire;
#[path = "../replication.rs"]
mod replication;
mod server;
mod transport;

use client_wire::{
    MAX_BATCH_BYTES, MAX_BATCH_OPERATIONS, MAX_KEY, MAX_SCAN_ROWS, MAX_VALUE, push_field,
    take_field,
};
use replication::MEMBERS;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use transport::{
    NETWORK_TIMEOUT, Peer, Result, certificates, client_config, connect, credentials, read_frame,
    server_config, write_frame,
};

const USAGE: &str = "zeus-node serve ID DATA_DIR LISTEN CA CERT KEY PEER PEER
zeus-node get ADDRESS CA CERT KEY NAME
zeus-node put ADDRESS CA CERT KEY SEQUENCE NAME VALUE
zeus-node batch ADDRESS CA CERT KEY SEQUENCE {put NAME VALUE | delete NAME}...
zeus-node scan ADDRESS CA CERT KEY START END LIMIT

Each PEER is ID=ADDRESS,CERT. Use voter IDs 1, 2 and 3.";

fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<bool> {
    let args: Vec<_> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["serve", id, data, listen, ca, cert, key, first, second] => {
            let id = id.parse()?;
            let address = listen.parse()?;
            let (roots, chain, key) = credentials(ca, cert, key)?;
            let mut peers = vec![parse_peer(first)?, parse_peer(second)?];
            if !MEMBERS.contains(&id)
                || peers.iter().any(|peer| {
                    peer.id == id || peer.address == address || peer.certificate == chain[0]
                })
                || peers[0].id == peers[1].id
                || peers[0].address == peers[1].address
                || peers[0].certificate == peers[1].certificate
            {
                return Err("configure one distinct address and leaf certificate per voter".into());
            }
            peers.push(Peer {
                id,
                address,
                certificate: chain[0].clone(),
            });
            let client = client_config(roots.clone(), chain.clone(), key.clone_key())?;
            let server = server_config(roots, chain, key)?;
            server::serve(id, Path::new(data), address, peers, server, client)?;
            Ok(true)
        }
        ["get", address, ca, cert, key, name] => {
            let mut request = vec![b'C', b'G'];
            encode_key(&mut request, name)?;
            call(address, ca, cert, key, &request)
        }
        ["put", address, ca, cert, key, sequence, name, value] => {
            let sequence: u64 = sequence.parse()?;
            if sequence == 0 || value.len() > MAX_VALUE {
                return Err("sequence must be positive and value must fit in 16 KiB".into());
            }
            let mut request = vec![b'C', b'P'];
            request.extend_from_slice(&sequence.to_be_bytes());
            encode_key(&mut request, name)?;
            request.extend_from_slice(value.as_bytes());
            call(address, ca, cert, key, &request)
        }
        ["batch", address, ca, cert, key, sequence, operations @ ..] => {
            let sequence: u64 = sequence.parse()?;
            if sequence == 0 {
                return Err("sequence must be positive".into());
            }
            let mut request = vec![b'C', b'B'];
            request.extend_from_slice(&sequence.to_be_bytes());
            encode_batch(&mut request, operations)?;
            call(address, ca, cert, key, &request)
        }
        ["scan", address, ca, cert, key, start, end, count] => {
            let count: usize = count.parse()?;
            if !(1..=MAX_SCAN_ROWS).contains(&count) {
                return Err(format!("scan limit must be between 1 and {MAX_SCAN_ROWS}").into());
            }
            let mut request = vec![b'C', b'S'];
            encode_key(&mut request, start)?;
            encode_key(&mut request, end)?;
            request.extend_from_slice(&(count as u16).to_be_bytes());
            call(address, ca, cert, key, &request)
        }
        ["--help" | "-h"] => {
            println!("{USAGE}");
            Ok(true)
        }
        ["--version"] => {
            println!("zeus-node {}", env!("CARGO_PKG_VERSION"));
            Ok(true)
        }
        _ => Err(USAGE.into()),
    }
}

fn parse_peer(spec: &str) -> Result<Peer> {
    let (id, endpoint) = spec.split_once('=').ok_or("expected ID=ADDRESS,CERT")?;
    let (address, certificate) = endpoint
        .split_once(',')
        .ok_or("expected peer certificate")?;
    let id = id.parse()?;
    if !MEMBERS.contains(&id) {
        return Err("unknown voter ID".into());
    }
    let certificate = certificates(certificate)?.remove(0);
    Ok(Peer {
        id,
        address: address.parse()?,
        certificate,
    })
}

fn encode_key(request: &mut Vec<u8>, name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_KEY {
        return Err("key must have 1 to 1024 bytes".into());
    }
    push_field(request, name.as_bytes());
    Ok(())
}

fn encode_batch(request: &mut Vec<u8>, mut args: &[&str]) -> Result<()> {
    let start = request.len();
    let mut operations = 0;
    while !args.is_empty() {
        args = match args {
            ["put", name, value, rest @ ..] if value.len() <= MAX_VALUE => {
                request.push(b'P');
                encode_key(request, name)?;
                push_field(request, value.as_bytes());
                rest
            }
            ["delete", name, rest @ ..] => {
                request.push(b'D');
                encode_key(request, name)?;
                rest
            }
            _ => return Err("expected put NAME VALUE or delete NAME".into()),
        };
        operations += 1;
        if operations > MAX_BATCH_OPERATIONS || request.len() - start > MAX_BATCH_BYTES {
            return Err("batch exceeds 128 operations or 64 KiB".into());
        }
    }
    if operations == 0 {
        return Err("batch must contain at least one operation".into());
    }
    Ok(())
}

fn call(address: &str, ca: &str, cert: &str, key: &str, request: &[u8]) -> Result<bool> {
    let (roots, chain, key) = credentials(ca, cert, key)?;
    let config = client_config(roots, chain, key)?;
    let mut stream = connect(address.parse()?, Arc::new(config), NETWORK_TIMEOUT, None)?;
    write_frame(&mut stream, request)?;
    let reply = read_frame(&mut stream)?;
    if let Some(value) = reply.strip_prefix(b"VALUE ") {
        io::stdout().write_all(value)?;
        io::stdout().write_all(b"\n")?;
        return Ok(true);
    }
    if let Some(page) = reply.strip_prefix(b"SCAN ") {
        let (&more, mut fields) = page.split_first().ok_or("invalid scan response")?;
        if more > 1 {
            return Err("invalid scan response".into());
        }
        let mut stdout = io::stdout().lock();
        while !fields.is_empty() {
            let name = take_field(&mut fields).ok_or("invalid scan key")?;
            let value = take_field(&mut fields).ok_or("invalid scan value")?;
            writeln!(stdout, "{}\t{}", name.escape_ascii(), value.escape_ascii())?;
        }
        if more == 1 {
            eprintln!("MORE");
        }
        return Ok(true);
    }
    if reply == b"OK" {
        println!("OK");
        return Ok(true);
    }
    eprintln!("{}", String::from_utf8_lossy(&reply));
    Ok(false)
}
