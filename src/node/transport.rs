use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, StreamOwned};
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

const SERVER_NAME: &str = "zeus.local";
const MAX_FRAME: usize = 128 * 1024;
pub(crate) const SNAPSHOT_CHUNK: usize = 64 * 1024;
pub(crate) const NETWORK_TIMEOUT: Duration = Duration::from_secs(4);
pub(crate) const PEER_TIMEOUT: Duration = Duration::from_millis(400);
pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub(crate) struct Peer {
    pub id: u64,
    pub address: SocketAddr,
    pub certificate: CertificateDer<'static>,
}

pub(crate) fn certificates(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut file = BufReader::new(File::open(path)?);
    let certificates = rustls_pemfile::certs(&mut file).collect::<io::Result<Vec<_>>>()?;
    if certificates.is_empty() {
        return Err(format!("no certificates in {path}").into());
    }
    Ok(certificates)
}

pub(crate) fn credentials(
    ca: &str,
    certificate: &str,
    key: &str,
) -> Result<(
    RootCertStore,
    Vec<CertificateDer<'static>>,
    PrivateKeyDer<'static>,
)> {
    let mut roots = RootCertStore::empty();
    for cert in certificates(ca)? {
        roots.add(cert)?;
    }
    let chain = certificates(certificate)?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key)?))?
        .ok_or("no private key in PEM file")?;
    Ok((roots, chain, key))
}

pub(crate) fn server_config(
    roots: RootCertStore,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<ServerConfig> {
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
    Ok(ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)?)
}

pub(crate) fn client_config(
    roots: RootCertStore,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<ClientConfig> {
    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)?)
}

pub(crate) fn connect(
    address: SocketAddr,
    tls: Arc<ClientConfig>,
    timeout: Duration,
    pinned: Option<&CertificateDer<'static>>,
) -> Result<StreamOwned<ClientConnection, TcpStream>> {
    let mut tcp = TcpStream::connect_timeout(&address, timeout)?;
    tcp.set_read_timeout(Some(timeout))?;
    tcp.set_write_timeout(Some(timeout))?;
    let mut connection = ClientConnection::new(tls, ServerName::try_from(SERVER_NAME)?)?;
    while connection.is_handshaking() {
        connection.complete_io(&mut tcp)?;
    }
    if let Some(cert) = pinned {
        if connection
            .peer_certificates()
            .and_then(|chain| chain.first())
            != Some(cert)
        {
            return Err("outbound peer certificate differs from configured identity".into());
        }
    }
    Ok(StreamOwned::new(connection, tcp))
}

pub(crate) fn write_frame(stream: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

pub(crate) fn read_frame(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub(crate) fn write_snapshot(stream: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "snapshot too large"))?;
    write_frame(stream, &[b"S".as_slice(), &length.to_be_bytes()].concat())?;
    for chunk in bytes.chunks(SNAPSHOT_CHUNK) {
        write_frame(stream, chunk)?;
    }
    Ok(())
}

pub(crate) fn read_snapshot(
    stream: &mut impl Read,
    header: &[u8],
    max_length: usize,
) -> io::Result<Vec<u8>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid snapshot transfer");
    let length = u32::from_be_bytes(header.try_into().map_err(|_| invalid())?) as usize;
    if length == 0 || length > max_length {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    while bytes.len() < length {
        let chunk = read_frame(stream)?;
        if chunk.len() != SNAPSHOT_CHUNK.min(length - bytes.len()) {
            return Err(invalid());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
